import datetime as dt

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from auth import allowed_data_teams, get_current_user, resolve_resource_access
from log_setup import audit_log
from nav_utils import register_nav_globals
from .cache import forbrug_site, ryd_cache, varsel
from .kunde import kunde_detalje
from .outcomes import (AABNE_UDFALD, ARR_KILDE_BEKRAEFTET, ARR_KILDE_DELING,
                       ARR_KILDER, KANALER, KONTAKT_OPNAAET, KONTAKTRESULTATER,
                       UDFALD, registrer_samtale, valider_registrering)
from .prioritering import prioriteringsdata
from .queries import (account_churn_rate, db_monthly_active_counts,
                      db_monthly_churn_pr_site)
from .usage import latest_complete_month

templates = Jinja2Templates(directory="templates")
register_nav_globals(templates)

router = APIRouter()


MIN_ROLLE = "sales_operations"

# Marketing (rang 4) og management (rang 5) rangerer HØJERE end sales_operations
# (rang 3) og ville ellers slippe ind gennem rangen alene. Besluttet 2026-08-10:
# retention er præcis Sales Operations + admin. admin bypasser altid
# exclude_roles i auth.resolve_resource_access, så adgangen kan ikke låses ude.
EKSKLUDEREDE_ROLLER = ["marketing", "management"]

# Ressource-id'erne er de samme som nav-items i nav_utils.CATEGORIES. Det er
# ikke kosmetik: resolve_resource_access slår op på præcis dette id, så en
# admin kan åbne én side for én bruger via UserResourceAccess uden kodeændring
# — og menu og endpoint bruger så garanteret samme nøgle.
RES_OVERBLIK = "retention-overview"
# "Opkald og risiko" (sammenlagt 2026-08-27 af Dagens opkald og Churn-risiko).
# Modulets INDGANG: arbejdsgangen begynder her, og derfor øverst i
# nav_utils' items-liste. Id'et hedder fortsat retention-risk og ikke fx
# retention-opkald — ændres det, mister enhver bruger med et
# UserResourceAccess-override på den gamle nøgle sin adgang.
RES_RISIKO = "retention-risk"
# Kunde-detaljen står IKKE i nav_utils.CATEGORIES: den er en gennemklikning,
# ikke et sted man går hen — Kundeside nås fra risikolisten og senere fra
# prioriteringssiden. Id'et findes alligevel, så adgangen kan styres på samme
# måde som siderne, hvis nogen får brug for det.
RES_KUNDE = "retention-kunde"

_AFVIST = "Retention er forbeholdt Sales Operations"


def _kraev_adgang(user: dict, resource_id: str) -> None:
    """403 medmindre brugeren må se den pågældende retention-side.

    Bruger resolve_resource_access og ikke has_access, fordi has_access kalder
    videre med resource_id="" og uden exclude_roles — den kan hverken se
    override-rækkerne eller holde marketing og management ude.
    """
    if resolve_resource_access(user, resource_id, MIN_ROLLE,
                               exclude_roles=EKSKLUDEREDE_ROLLER) == "none":
        raise HTTPException(403, _AFVIST)


def _resolve_filters(user: dict, resource_id: str) -> tuple[str | None, list | None]:
    """Oversæt brugerens rolle til (owner_name, team) for retention-queryen.

    Modulet er lukket for alt under Sales Operations (besluttet 2026-08-10).
    Retention-specialisten ER en Sales Operations-bruger og skal se hele
    firmaets churn-billede, så der findes ikke længere en egen-bog-visning:
    en sælger har ingen adgang overhovedet.

    Derfor er `owner_name` altid None — der er ingen rolle tilbage, der skal
    afgrænses til én persons bog. De tidligere salesperson- og
    sales_manager-grene er FJERNET frem for ladt stå: en uåbnelig gren i
    adgangskontrol er farlig, fordi den ser ud til at virke, hvis nogen senere
    sænker vagten.

    Teams kan stadig være begrænset: har admin sat HubUserTeamAccess på
    brugeren, bliver "hele firmaet" de tilladte teams — samme regel som
    _effective_team i perf-modulet. Ubegrænset giver (None, None) = firmabredt,
    inkl. de 16% kunder uden tilskrevet ejer i PipeDrive.
    """
    _kraev_adgang(user, resource_id)
    return None, allowed_data_teams(user)


@router.get("/retention/opkald_data")
def get_opkaldsdata(user=Depends(get_current_user)):
    """Alt "Opkald og risiko" skal vise: dagens to lister, det fulde
    risikobillede og månedens tre tal.

    Erstatter de to tidligere endepunkter /retention/prioritering_data og
    /retention/risk (fjernet 2026-08-27, da Dagens opkald og Churn-risiko blev
    lagt sammen til én side). De to sider læste tidligere risikobilledet
    hver for sig — /retention/risk kaldte `abonnementer_i_risiko()` direkte og
    UKACHET (3,6 sekunder), mens denne side allerede læste det samme billede
    gennem `cache.risiko()`. Med begge lister på én side ville to fetch-kald
    have udført samme beregning to gange; ét kald løser det.

    KLOKKEN LÆSES ÉN GANG, her. `prioriteringsdata` har med vilje ingen default
    på `i_dag`: kaldes `date.today()` to gange under samme sideopslag, kan de to
    kald ligge på hver sin side af midnat, og siden ville beregne opfølgninger
    mod i dag og KPI'er mod i morgen. Ét argument gør fejlen umulig.

    `abo_maaned` sendes bevidst IKKE videre: produktionsvisningen er altid
    indeværende måned, og parameteren findes kun til kontrolkørsler.

    Siden er en SKAL plus dette kald og ikke en server-renderet side. Et koldt
    kald tager 7,5 sekunder, fordi forbruget aggregeres fra grunden (varmt: 0,08
    s), og de sekunder ville ellers være en hvid skærm uden forklaring. Samme
    mønster som modulets øvrige sider.
    """
    _, teams = _resolve_filters(user, RES_RISIKO)
    return prioriteringsdata(dt.date.today(), teams=teams)


@router.get("/retention/prioritering")
async def retention_prioritering_redirect(user=Depends(get_current_user)):
    """Gammel URL for Dagens opkald. Siden findes ikke længere som egen
    side — den blev lagt sammen med Churn-risiko 2026-08-27 til "Opkald og
    risiko". Redirectet holder eksisterende links og bogmærker i live.

    `get_current_user` afhænger stadig med, så et kald uden session først
    rammer login-redirectet og ikke denne — samme opførsel som før."""
    return RedirectResponse("/retention/risk_overview", status_code=302)


@router.get("/retention/monthly_active_counts")
def get_monthly_active_counts(user=Depends(get_current_user)):
    owner_name, teams = _resolve_filters(user, RES_OVERBLIK)
    return db_monthly_active_counts(owner_name=owner_name, teams=teams)


@router.get("/retention/churn_pr_site")
def get_churn_pr_site(maaneder: str, user=Depends(get_current_user)):
    """"Måned mod måned pr. site"-panelet: opsigelser og aktive pr. site for
    de valgte måneder, plus raten rullet op pr. account.

    `maaneder` er kommasepareret ('2026-06-01,2026-07-01'), ISO-datoer som i
    /retention/monthly_active_counts' egne rækker — panelet bygger sine
    månedsvælgere af DEN liste og sender valget tilbage hertil, så der aldrig
    opstår et format begge sider skal blive enige om hver for sig.

    Ingen rate pr. site i svaret — churn-rate-kan-ikke-maales-pr-site: kun 2
    af 35 danske sites har grundlag over MIN_AKTIVE_FOR_RATE. `account_churn_rate`
    rummer den rate, `sites`-rækkerne rummer kun absolutte tal.
    """
    owner_name, teams = _resolve_filters(user, RES_OVERBLIK)
    maaned_liste = [m.strip() for m in maaneder.split(",") if m.strip()]
    rows = db_monthly_churn_pr_site(maaned_liste, owner_name=owner_name, teams=teams)
    return {"sites": rows, "accounts": account_churn_rate(rows, maaned_liste)}


@router.get("/retention/forbrug_pr_site")
def get_forbrug_pr_site(user=Depends(get_current_user)):
    """"Side- og artikelvisninger pr. site"-panelet.

    INGEN team-afgrænsning på selve dataene: forbrugsfilen har ingen
    ejer-kolonne, og der findes ingen vej fra et site til et team (se
    usage.forbrug_pr_site). `_resolve_filters` kaldes alligevel — den er
    stedet der håndhæver ADGANGEN til RES_OVERBLIK, uafhængigt af om `teams`
    bruges bagefter.

    ASYMMETRIEN VISES, DEN SKJULES IKKE: er brugeren selv team-afgrænset
    (`teams is not None`), viser dette endpoints data alligevel hele
    porteføljen, mens churn-panelet ovenfor ER afgrænset. To paneler der
    tavst er uenige om omfang er præcis den slags man opdager på et
    ledermøde, så `bruger_har_team_begraensning` sendes med, og panelet
    skriver en linje når den er sand. I praksis sjældent: retention er
    lukket for alt under Sales Operations, og en ubegrænset bruger får
    `teams=None`.
    """
    _, teams = _resolve_filters(user, RES_OVERBLIK)
    data = forbrug_site()
    return {
        "pr_site": data["pr_site"],
        "maaneder": data["maaneder"],
        "referencemaaned": latest_complete_month(data["maaneder"]),
        "meta": data["meta"],
        "bruger_har_team_begraensning": teams is not None,
    }


@router.get("/retention/opsigelser_varsel")
def get_opsigelser_varsel(user=Depends(get_current_user)):
    """"Opsigelser i varsel"-panelet: abonnementer med en gældende opsigelse,
    hvis ophør endnu ikke er indtruffet.

    TEAM-AFGRÆNSET, i modsætning til forbrugspanelet lige ovenfor:
    varsel.opsigelser_i_varsel bygger på abonnementer_med_ejer, som allerede
    understøtter `teams`. Flaget sendes alligevel med, af samme grund som i
    get_forbrug_pr_site: to paneler der tavst er uenige om omfang er den
    slags man opdager på et ledermøde.
    """
    owner_name, teams = _resolve_filters(user, RES_OVERBLIK)
    # Måneden regnes HER og ikke inde i cache.varsel, så den indgår i
    # cache-nøglen: uden den ville et månedsskift blive ved med at ramme
    # forrige måneds cache-post i op til CACHE_SEKUNDER efter midnat.
    maaned = dt.date.today().strftime("%Y-%m")
    data = varsel(teams, maaned)
    data["meta"]["bruger_har_team_begraensning"] = teams is not None
    return data


@router.get("/retention/overview", response_class=HTMLResponse)
async def retention_overview(request: Request, user=Depends(get_current_user)):
    # Selve dataene hentes client-side og er beskyttet af _resolve_filters, men
    # skallen skal heller ikke kunne åbnes — en bruger uden adgang har intet at
    # gøre på siden, og et tomt panel med en fejlbesked er en dårlig afvisning.
    _kraev_adgang(user, RES_OVERBLIK)
    return templates.TemplateResponse(request, "retention_overview.html", {"user": user})


@router.get("/retention/risk_overview", response_class=HTMLResponse)
async def retention_risk_overview(request: Request, user=Depends(get_current_user)):
    # Samme adgangsvagt som de øvrige retention-sider: dataene er beskyttet af
    # _resolve_filters, men skallen skal heller ikke kunne åbnes uden adgang.
    #
    # "Opkald og risiko" (sammenlagt 2026-08-27 af Dagens opkald og
    # Churn-risiko). URL'en er UÆNDRET, kun skabelonen og navnet i nav_utils er
    # nye — se RES_RISIKO's kommentar for hvorfor id'et heller ikke ændres.
    _kraev_adgang(user, RES_RISIKO)
    return templates.TemplateResponse(request, "retention_opkald.html", {"user": user})


@router.get("/retention/kunde_data/{account}/{org_id}")
def get_kunde_detalje(account: str, org_id: str, user=Depends(get_current_user)):
    """Alt om én kunde (Kundeside).

    `org_id` tages som STRENG og ikke int: risikolaget bærer det som tekst, og
    en int i stien ville give en nøgle der aldrig matcher. outcomes.py
    konverterer selv, når databasen skal have den.
    """
    _, teams = _resolve_filters(user, RES_KUNDE)
    return kunde_detalje(account, org_id, teams=teams)


@router.get("/retention/kunde/{account}/{org_id}", response_class=HTMLResponse)
async def retention_kunde(request: Request, account: str, org_id: str,
                          user=Depends(get_current_user)):
    # Siden skal kunne åbnes for ENHVER kunde, ikke kun dem på risikolisten
    # (Kundeside) — ellers kan et udfald ikke registreres på et sundt
    # abonnement. Derfor slås kunden ikke op her: findes hun ikke, viser siden
    # sin egen tomme tilstand frem for en 404.
    _kraev_adgang(user, RES_KUNDE)
    # Vokabularet kommer FRA outcomes.py og ikke fra skabelonen. Værdierne skal
    # matche databasens CHECK-constraints præcis, og en formular der tilbyder et
    # valg databasen afviser, fejler først efter opkaldet er slut.
    return templates.TemplateResponse(request, "retention_kunde.html",
                                      {"user": user, "account": account,
                                       "org_id": org_id,
                                       "kanaler": KANALER,
                                       "kontaktresultater": KONTAKTRESULTATER,
                                       # LISTE af par, ikke en dict: Jinja2's
                                       # `tojson` har sort_keys=True som standard,
                                       # så en dict ville komme ud alfabetisk i
                                       # JS'en — «Allerede opsagt» først i
                                       # dropdownen frem for «Fornyet». Kanal og
                                       # kontaktresultat rammes ikke, fordi de
                                       # renderes med et {% for %} i skabelonen.
                                       "udfaldstyper": list(UDFALD.items()),
                                       "arr_kilder": ARR_KILDER,
                                       # Formularen skal vide hvilke udfald der
                                       # kræver en opfølgningsdato, og hvad de to
                                       # ARR-kilder heder. Sendes med frem for at
                                       # blive gentaget i JS, hvor de kunne drive.
                                       "aabne_udfald": list(AABNE_UDFALD),
                                       "kontakt_opnaaet": KONTAKT_OPNAAET,
                                       "arr_kilde_deling": ARR_KILDE_DELING,
                                       "arr_kilde_bekraeftet": ARR_KILDE_BEKRAEFTET})


def _tal(vaerdi):
    """Tom → None, ellers float. Kaster 400 på noget, der ikke er et tal."""
    if vaerdi is None or (isinstance(vaerdi, str) and not vaerdi.strip()):
        return None
    try:
        return float(vaerdi)
    except (TypeError, ValueError):
        raise HTTPException(400, f"«{vaerdi}» er ikke et tal")


def _dato(vaerdi):
    """'2026-09-01' → date. Tom → None.

    Konverteres HER og ikke i outcomes.py, fordi det er HTTP-kanten der leverer
    strenge. Databasekolonnen er `date`, og pymssql binder en streng uden at
    klage — men så ville en tastefejl som '2026-9-1' først dukke op som en
    uklar serverfejl.
    """
    if not vaerdi:
        return None
    try:
        return dt.date.fromisoformat(str(vaerdi)[:10])
    except ValueError:
        raise HTTPException(400, f"«{vaerdi}» er ikke en dato (ÅÅÅÅ-MM-DD)")


def _tidspunkt(vaerdi):
    """'2026-08-11T14:30' → datetime. Tom → None (valideringen fanger det)."""
    if not vaerdi:
        return None
    try:
        return dt.datetime.fromisoformat(str(vaerdi)[:26])
    except ValueError:
        raise HTTPException(400, f"«{vaerdi}» er ikke et tidspunkt")


def _kraev_kunde_i_raekkevidde(account: str, org_id: str, teams) -> None:
    """Skrivesiden skal have samme dataafgrænsning som læsesiden.

    Uden dette kunne en team-begrænset bruger POSTe et udfald på en kunde, hun
    ikke må se — læsesiden filtrerer på teams, men en POST rammer databasen
    direkte.

    ÆRLIGT FORBEHOLD: risikobilledet kan ikke skelne "uden for dine teams" fra
    "ikke længere kunde" fra "uden for modulets geografiske afgrænsning"
    (watch_no/se/de, se queries.UDENLANDSKE_ACCOUNTS). En team-begrænset bruger
    kan derfor ikke registrere på en netop opsagt ELLER en udenlandsk kunde,
    selv om Kundeside ellers tillader det. Det er valgt som den forsigtige fejl
    — den nægter at skrive frem for at skrive uden for grænsen. I dag er
    `allowed_data_teams` None for Sales Operations, så den rammer ingen; bliver
    en bruger begrænset, skal reglen tages op igen.
    """
    if teams is None:
        return
    if kunde_detalje(account, org_id, teams=teams)["ingen_aktive"]:
        raise HTTPException(403, "Kunden ligger uden for din dataadgang")


@router.post("/retention/kunde/{account}/{org_id}/samtale")
async def post_registrer_samtale(account: str, org_id: str, request: Request,
                                 user=Depends(get_current_user)):
    """Registrér én samtale og de udfald den gav (Kundeside).

    Den ENESTE rute i modulet der skriver til produktion. Derfor:
    valideringen kaldes server-side, selv om formularen validerer i forvejen —
    browseren er ikke en sikkerhedsgrænse — og registreringen auditeres som de
    øvrige skrivninger i hubben.

    `account` og `org_id` tages fra STIEN og ikke fra body'en. Ligger de to
    steder, kan de være uenige, og så ville et udfald kunne skrives på en anden
    kunde end den, adgangen blev tjekket for.
    """
    _, teams = _resolve_filters(user, RES_KUNDE)
    _kraev_kunde_i_raekkevidde(account, org_id, teams)

    body = await request.json()

    samtale = {
        "account":      account,
        "org_id":       org_id,
        "contacted_at": _tidspunkt(body.get("contacted_at")),
        "channel":      body.get("channel"),
        "summary":      (str(body.get("summary") or "").strip() or None),
        "created_by":   user["name"],
    }

    udfald = []
    for u in body.get("udfald") or []:
        udfald.append({
            "site":               (str(u.get("site") or "").strip() or None),
            "contact_result":     u.get("contact_result"),
            "outcome":            (u.get("outcome") or None),
            "arr_before_dkk":     _tal(u.get("arr_before_dkk")),
            "arr_before_kilde":   (u.get("arr_before_kilde") or None),
            "arr_after_local":    _tal(u.get("arr_after_local")),
            "arr_after_currency": (str(u.get("arr_after_currency") or "").strip().upper() or None),
            "fx_rate":            _tal(u.get("fx_rate")),
            "renewal_date":       _dato(u.get("renewal_date")),
            "expiry_date":        _dato(u.get("expiry_date")),
            "followup_date":      _dato(u.get("followup_date")),
            "note":               (str(u.get("note") or "").strip()[:4000] or None),
        })

    fejl = valider_registrering(samtale, udfald)
    if fejl:
        # 422 og ikke 400: body'en var velformet JSON, men indholdet holder ikke.
        # Fejlene sendes som en LISTE, så formularen kan vise dem alle på én
        # gang — ikke én ad gangen, hvor specialisten skal gætte resten.
        raise HTTPException(422, fejl)

    conversation_id = registrer_samtale(samtale, udfald)
    if conversation_id is None:
        # registrer_samtale har rullet tilbage og logget undtagelsen. Intet er
        # skrevet — det er vigtigt at sige, så samtalen bliver registreret igen
        # frem for at blive betragtet som gemt.
        raise HTTPException(500, "Registreringen kunne ikke gemmes. "
                                 "Intet blev skrevet — prøv igen.")

    # Uden dette viser siden de gamle tal i op til cache.CACHE_SEKUNDER, og
    # specialisten tror registreringen ikke gik igennem. Ét kald rydder BEGGE
    # siders data, fordi der kun er én cache — se cache.py.
    ryd_cache()

    audit_log("retention_samtale_registreret", user=user, request=request,
              account=account, org_id=org_id,
              conversation_id=conversation_id, udfald=len(udfald))

    return {"ok": True, "conversation_id": conversation_id, "udfald": len(udfald)}
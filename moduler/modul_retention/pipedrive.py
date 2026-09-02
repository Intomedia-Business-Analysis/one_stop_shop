"""Pipedrive-siden af udfaldsregistreringen: opkaldet skrives tilbage til CRM'et.

`outcomes.registrer_samtale` gemmer hvad der skete i VORES database. Den her
lægger det samme opkald i Pipedrive som en afsluttet aktivitet på kundens
organisation, så sælgeren kan se på sin egen kunde, at retention har ringet,
uden at skulle åbne One Stop Shop.

REGISTRERINGEN ER CHEFEN. Databasen skrives først og committes, og først
derefter kaldes Pipedrive. Fejler Pipedrive, er udfaldet stadig gemt, og
routeren svarer "gemt, men ikke sendt til Pipedrive". Det omvendte, hvor en
API-timeout kunne kaste et registreret opkald væk, ville koste specialisten
arbejde hun ikke kan lave om: hun har lagt på, og kunden svarer ikke igen.
Derfor kaster INGEN funktion herinde ud til kalderen, se send_opkalds_aktivitet.

KUN HVOR KONTAKT BLEV OPNÅET (besluttet 2026-09-02). `ingen_kontakt` og
`ikke_kontaktbar` fylder mest i registreringerne og siger intet om kunden, kun
om telefonen. Havde de fulgt med, ville Pipedrive-tidslinjen på en kunde blive
en liste over ubesvarede opkald, og den ene registrering der betyder noget ville
drukne.

TRE TING ER VERIFICERET MOD DEN RIGTIGE KONTO 2026-09-02 (watchmedier), fordi
et gæt her først fejler når specialisten har lagt på:

1. `dbo.retention.org_id` ER Pipedrives organisations-id på watch_medier-kontoen.
   Fem stikprøver (org_id 240, 242, 244, 246, 248), navnet matcher 5 af 5. Det
   er grunden til at vi ikke behøver et navneopslag, og det holder KUN inden for
   én account, jf. queries.py: samme org_id er to fremmede firmaer i to accounts.

2. Aktivitetstyperne findes og er aktive: `call` ("Opkald"), `email` ("Email"),
   `meeting` ("Møde"). De tre rammer præcis outcomes.KANALER, så kanalen kan
   oversættes uden at gætte. Kontoen har 14 typer i alt, bl.a. fire Klenty-typer,
   som vi ikke rører.

3. `note` er HTML og ikke ren tekst: 39 af 89 eksisterende noter indeholder
   tags, typisk `<br>`. Derfor bygges noten som HTML, og ALT der kommer fra
   specialisten køres gennem html.escape.

`outcome`-feltet på v2-aktiviteten står tomt i alle 100 stikprøver på kontoen og
bruges altså ikke i huset. Vi sætter det ikke: et felt ingen læser, men som en
rapport måske filtrerer på i morgen, skal ikke fyldes op med vores vokabular.

EGEN HTTP-KLIENT OG IKKE modul_portfolio_alignment.pipedrive_api'S. De to
moduler taler med samme API, men alignment-modulet er færdigt, og dets
hjælpefunktioner er private (`_api_post`, `_get_token`). At importere dem ville
binde retention til en fil, der ikke må ændres for retentions skyld. Prisen er
40 linjers duplikeret POST/GET; gevinsten er at de to kan udvikle sig hver for
sig.
"""
from __future__ import annotations

import datetime as dt
import html
import logging
import os
from zoneinfo import ZoneInfo

import requests
from env import load_env
from os_trust import session

from .outcomes import KANALER, KONTAKT_OPNAAET, UDFALD

logger = logging.getLogger(__name__)

load_env()

BASE_URL = "https://api.pipedrive.com/api/v2"
# /users/me har intet v2-modstykke. Bruges kun til company_domain, så
# aktiviteten kan linkes tilbage til specialisten.
BASE_URL_V1 = "https://api.pipedrive.com/v1"
TIMEOUT = 20

# account → env-variabel med API-token. Navnene er de samme som
# modul_portfolio_alignment bruger, fordi det er DE SAMME tokens i .env.
#
# monitor og marketwire er udkommenteret 2026-09-02 sammen med resten af
# sporet, se queries.DEAKTIVEREDE_ACCOUNTS. En registrering på dem kan ikke
# opstå, mens de er slået fra (kunden kan ikke åbnes), men listen holdes i
# takt med resten, så en genaktivering ikke efterlader ét sted bagud.
# marketwire har i øvrigt aldrig haft et token, jf. .env.
ACCOUNT_TOKEN_ENV: dict[str, str] = {
    "watch_medier": "PD_TOKEN_WATCH_DK_FINANS",
    # "monitor":      "PD_TOKEN_MONITOR",
    # "marketwire":   None,
}

# outcomes.KANALER → Pipedrives key_string på aktivitetstypen. Verificeret
# aktiv på kontoen 2026-09-02, se modulets docstring punkt 2.
KANAL_TIL_AKTIVITETSTYPE: dict[str, str] = {
    "telefon": "call",
    "mail":    "email",
    "moede":   "meeting",
}

# Falder kanalen uden for de tre, bliver aktiviteten en 'call' frem for at
# fejle. En registreret samtale skal i Pipedrive selv om nogen tilføjer en
# fjerde kanal i outcomes.KANALER uden at åbne denne fil.
STANDARD_AKTIVITETSTYPE = "call"

# HVEM AKTIVITETEN TILHØRER. To lovlige værdier:
#
#   "token_bruger" den bruger API-tokenet hører til (System Admin, id 11913480
#                  på watchmedier). `owner_id` udelades af payloaden, og
#                  Pipedrive sætter den selv. Aktiviteten står på
#                  organisationens tidslinje, hvor sælgeren ser den når han
#                  åbner kunden.
#   "org_ejer"     organisationens ejer, altså sælgeren. Aktiviteten lander
#                  desuden i HANS aktivitetsliste.
#
# VALGT: "token_bruger" (besluttet 2026-09-02, efter at have kørt begge dele
# forbi virkeligheden). Tre grunde, i vægtrækkefølge:
#
# 1. EJERSKAB ER ET ANSVARSFELT, ikke et notifikationsfelt. "org_ejer" ville
#    bruge det til at opnå synlighed, og regningen ville lande hos en sælger,
#    der hverken har valgt det eller kan se hvorfor hans tal flyttede sig.
#
# 2. GEVINSTEN VED "org_ejer" ER MINDRE END DEN LYDER. Aktiviteten er `done`,
#    og en afsluttet aktivitet lander ikke i nogens to-do-liste, kun i hans
#    historik. Han ser den ALLIGEVEL på kundens tidslinje, netop når den er
#    relevant: lige inden han selv skal tale med kunden.
#
# 3. ASYMMETRIEN I HVEM DER BÆRER FEJLEN. Er "token_bruger" det forkerte valg,
#    skal sælgeren åbne kunden for at se opkaldet: irriterende, synligt, nemt
#    at rette. Er "org_ejer" det forkerte valg, er en sælgers aktivitetstal
#    forkerte i Pipedrives EGNE rapporter, og den fejl er stille og rammer en
#    anden end den der traf valget.
#
# MÅLT 2026-09-02, så det ikke skal måles igen: INTET andet sted i
# one_stop_shop læser Pipedrive-aktiviteter, så vores egne rapporter er
# upåvirkede uanset valget. Det siger derimod intet om Pipedrives indbyggede
# rapporter og mål, som ikke kan ses fra koden. Viser det sig at ingen bruger
# aktivitetstal DÉR, og at sælgerne vil have opkaldet i deres eget feed, er
# "org_ejer" det rigtige, og det er ét ord at skifte.
#
# ADVARSEL MOD ET FALSK ARGUMENT: 37,5 % af kontoens eksisterende aktiviteter
# har creator_user_id != owner_id, og det ligner et hus, der allerede
# tilskriver andre folks aktiviteter. Det gør det ikke. Slår man brugerne op,
# er samtlige hyppige par mellem INAKTIVE brugere (Rune Lippert -> Søren Lund,
# Hüseyin Øzkan, Christoffer Lesner, Jesper Paulsen), altså ejerskab overdraget
# da folk stoppede. Tallet må ikke bruges til at retfærdiggøre "org_ejer".
AKTIVITET_EJER = "token_bruger"

# PIPEDRIVE REGNER I UTC. `due_time` sendes og gemmes som UTC og vises i den
# enkelte brugers tidszone — det er IKKE dokumenteret i vores ende, og det
# fejler ikke, det viser bare et forkert klokkeslæt.
#
# MÅLT 2026-09-02 paa aktivitet 91519: samtalen blev registreret 12:50 dansk
# tid, vi sendte due_time '12:50', Pipedrive gemte '12:50' og viste 14:50.
# Samme kald svarede add_time '2026-09-02T11:06:49Z', altsaa 13:06 dansk —
# kontoen er utvetydigt UTC-baseret. Uden konverteringen stod hver eneste
# aktivitet to timer for sent om sommeren og én om vinteren.
#
# NAVNGIVEN ZONE og ikke datetime.astimezone(): sidstnaevnte foelger SERVERENS
# tidszone, og saa ville en flytning af appen til en UTC-maskine forskyde alle
# klokkeslaet uden at nogen roerte koden. Specialisterne sidder i Danmark, og
# `contacted_at` er naiv dansk tid hele vejen op gennem modulet.
#
# tzdata er sikret gennem pandas (requirements.txt), og paa Linux laeses
# systemets egen zoneinfo. Ingen ny afhaengighed.
LOKAL_TZ = ZoneInfo("Europe/Copenhagen")

# Cache pr. token. Domænet skifter ikke i en proceslevetid, og opslaget koster
# et rundtur pr. registrering hvis det ikke caches.
_DOMAENE_CACHE: dict[str, str | None] = {}


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def _token(account: str) -> str | None:
    """API-token for kontoen, eller None hvis der ikke er noget at sende med.

    None og ikke en undtagelse: mangler tokenet, skal registreringen stadig
    gemmes, og svaret skal sige at intet blev sendt. Det gør samtidig .env til
    afbryderen, uden at der skal bygges et flag: fjernes tokenet, holder
    integrationen op med at skrive, og alt andet virker.
    """
    env_navn = ACCOUNT_TOKEN_ENV.get(account)
    if not env_navn:
        return None
    return os.getenv(env_navn) or None


def _hoved(token: str) -> dict:
    return {"x-api-token": token}


def _get(token: str, sti: str, params: dict | None = None) -> dict | list | None:
    """GET mod v2. None ved enhver fejl, aldrig en undtagelse.

    Kalderne bruger opslag der er NICE TO HAVE (organisationens ejer,
    kontodomænet). Et fejlet opslag må koste et felt, ikke aktiviteten.
    """
    try:
        r = session().get(BASE_URL + sti, headers=_hoved(token),
                          params=params or {}, timeout=TIMEOUT)
        if r.status_code >= 400:
            return None
        krop = r.json()
        if not krop.get("success"):
            return None
        return krop.get("data")
    except (requests.RequestException, ValueError):
        return None


def _post(token: str, sti: str, payload: dict) -> dict:
    """POST mod v2. Kaster RuntimeError med Pipedrives egen fejltekst.

    Denne ENE kaster, fordi kalderen (send_opkalds_aktivitet) skal kunne
    fortælle specialisten HVORFOR det ikke gik igennem. Undtagelsen når aldrig
    ud af modulet.
    """
    r = session().post(BASE_URL + sti, headers=_hoved(token),
                       json=payload, timeout=TIMEOUT)
    if r.status_code >= 400:
        try:
            krop = r.json()
            fejl = krop.get("error") or krop.get("error_info") or krop
        except ValueError:
            fejl = r.text[:300]
        raise RuntimeError(f"Pipedrive POST {sti} {r.status_code}: {fejl}")
    krop = r.json()
    if not krop.get("success"):
        raise RuntimeError(f"Pipedrive POST {sti} fejl: {krop.get('error', krop)}")
    return krop.get("data") or {}


def _org_ejer(token: str, org_id: int) -> int | None:
    """Pipedrive-brugeren der ejer organisationen, eller None.

    None betyder at `owner_id` udelades af payloaden, og så sætter Pipedrive
    selv tokenets bruger. Aktiviteten kommer altså i CRM'et uanset hvad, den
    lander bare et andet sted i sælgerens billede.
    """
    data = _get(token, f"/organizations/{int(org_id)}")
    if isinstance(data, dict):
        ejer = data.get("owner_id")
        return int(ejer) if ejer else None
    return None


def _kontodomaene(token: str) -> str | None:
    """Kontodomænet til aktivitets-links. Caches, se _DOMAENE_CACHE."""
    if token in _DOMAENE_CACHE:
        return _DOMAENE_CACHE[token]
    domaene = None
    try:
        r = session().get(f"{BASE_URL_V1}/users/me", headers=_hoved(token),
                          timeout=TIMEOUT)
        if r.status_code < 400:
            krop = r.json()
            if krop.get("success"):
                domaene = (krop.get("data") or {}).get("company_domain")
    except (requests.RequestException, ValueError):
        pass
    _DOMAENE_CACHE[token] = domaene
    return domaene


# ---------------------------------------------------------------------------
# Payload (rene funktioner, ingen netværk)
# ---------------------------------------------------------------------------

def kontakt_udfald(udfald: list) -> list:
    """De udfald der skal til Pipedrive: kun dem hvor der var nogen i røret.

    Se modulets docstring. Bemærk at `outcome` er garanteret sat netop her:
    databasens CK_RetOut_outcome_kraever_kontakt er en biimplikation, så
    kontakt_opnaaet uden udfald kan ikke findes i en gemt registrering.
    """
    return [u for u in (udfald or [])
            if u.get("contact_result") == KONTAKT_OPNAAET]


def _kr(beloeb) -> str | None:
    """12500.0 → '12.500 kr.'. None ved tomt, så kalderen kan udelade feltet."""
    if beloeb is None:
        return None
    try:
        hel = int(round(float(beloeb)))
    except (TypeError, ValueError):
        return None
    return f"{hel:,}".replace(",", ".") + " kr."


def _dato_tekst(vaerdi) -> str | None:
    """date/datetime/streng → 'YYYY-MM-DD'. None ved tomt."""
    if not vaerdi:
        return None
    if isinstance(vaerdi, (dt.date, dt.datetime)):
        return vaerdi.strftime("%Y-%m-%d")
    return str(vaerdi)[:10]


def byg_emne(udfald_til_pd: list) -> str:
    """Aktivitetens emne. Det er ALT sælgeren ser i sin liste uden at klikke.

    Derfor står udfaldet i emnet og ikke kun i noten: en linje der hedder
    "Retention: Opsagt (ShippingWatch DK)" kan handles på med det samme, mens
    "Retention-opkald" kræver et klik for at sige noget som helst.

    Længden holdes under 200 tegn. En kunde med ni sites ville ellers give et
    emne, der er klippet midt i et sitenavn af Pipedrives egen visning.
    """
    labels = [UDFALD.get(u.get("outcome"), u.get("outcome") or "Udfald")
              for u in udfald_til_pd]
    if len(udfald_til_pd) == 1:
        site = udfald_til_pd[0].get("site")
        return f"Retention: {labels[0]}" + (f" ({site})" if site else "")
    emne = f"Retention: {len(udfald_til_pd)} udfald (" + ", ".join(labels) + ")"
    if len(emne) > 200:
        # 195 + 5 tegn i halen = praecis 200.
        emne = emne[:195].rstrip(", ") + " ...)"
    return emne


def byg_note(samtale: dict, udfald_til_pd: list) -> str:
    """Noten som HTML. ALT udefra escapes, se modulets docstring punkt 3.

    Noten skal kunne læses af en sælger der ikke kender modulet, så den siger
    hvor den kommer fra og hvem der ringede. Uden det ligner en aktivitet med
    ukendt afsender noget, der er kommet ind ved en fejl, og så bliver den
    slettet.
    """
    e = html.escape
    linjer = [
        "<b>Registreret i One Stop Shop (retention).</b>",
        "Kanal: " + e(KANALER.get(samtale.get("channel"), samtale.get("channel") or "?"))
        + ". Kontakt opnået.",
        "Ringet af: " + e(str(samtale.get("created_by") or "ukendt")) + ".",
        "",
    ]

    for u in udfald_til_pd:
        site = e(str(u.get("site") or "uden site"))
        label = e(UDFALD.get(u.get("outcome"), u.get("outcome") or "udfald"))
        linjer.append(f"<b>{site}</b>: {label}.")

        # ARR vises kun når der ER et tal. En linje der siger "ARR: ingen" er
        # støj, og et 0 ville blive læst som en måling.
        foer, efter = _kr(u.get("arr_before_dkk")), _kr(u.get("arr_after_dkk"))
        if efter is None:
            efter = _kr(u.get("arr_after_local"))
        if foer or efter:
            dele = []
            if foer:
                dele.append("før " + e(foer))
            if efter:
                dele.append("efter " + e(efter))
            # Uden et ekstra punktum: _kr slutter selv paa "kr.", og et
            # punktum mere gav "12.500 kr.." (set i roegtesten 2026-09-02).
            linjer.append("ARR: " + ", ".join(dele))

        for felt, tekst in (("renewal_date", "Fornyelse"),
                            ("expiry_date", "Ophør"),
                            ("followup_date", "Opfølgning")):
            d = _dato_tekst(u.get(felt))
            if d:
                linjer.append(f"{tekst}: {e(d)}.")

        if u.get("note"):
            linjer.append("Note: " + e(str(u["note"])))
        linjer.append("")

    if samtale.get("summary"):
        linjer.append("<b>Opsummering:</b> " + e(str(samtale["summary"])))

    return "<br>".join(linjer).strip()


def byg_aktivitet(samtale: dict, udfald: list,
                  ejer_id: int | None = None) -> dict | None:
    """Payload til POST /activities, eller None hvis der intet er at sende.

    REN FUNKTION uden netværk, så både preview og den rigtige afsendelse bygger
    NØJAGTIG den samme payload, og en røgtest kan bevise indholdet uden et
    Pipedrive-token. `ejer_id` slås op udenfor, af samme grund.

    None (og ikke en tom dict) når ingen af udfaldene har kontakt: kalderen skal
    kunne skelne "der var intet at sende" fra "det gik galt".
    """
    til_pd = kontakt_udfald(udfald)
    if not til_pd:
        return None

    tidspunkt = samtale.get("contacted_at")
    if isinstance(tidspunkt, dt.datetime):
        # Naiv = dansk tid. Er den allerede tidszone-bevidst, respekteres den.
        if tidspunkt.tzinfo is None:
            tidspunkt = tidspunkt.replace(tzinfo=LOKAL_TZ)
        # DATO OG TID KONVERTERES SAMMEN, ikke hver for sig: et opkald kl.
        # 00:30 dansk tid ligger 22:30 UTC DAGEN FØR, og en lokal dato med et
        # UTC-klokkeslæt ville placere aktiviteten et døgn galt.
        utc = tidspunkt.astimezone(dt.timezone.utc)
        due_date = utc.strftime("%Y-%m-%d")
        # 'HH:MM' og ikke 'HH:MM:SS': verificeret mod 122 eksisterende
        # aktiviteter på kontoen 2026-09-02, som alle har minut-opløsning.
        due_time = utc.strftime("%H:%M")
    else:
        # Ren dato uden klokkeslæt: INGEN konvertering. Uden due_time bliver
        # aktiviteten heldagsagtig, og så er den lokale dato den rigtige —
        # en UTC-omregning kunne kun flytte den en dag væk fra den dag,
        # samtalen faktisk fandt sted.
        due_date = _dato_tekst(tidspunkt) or dt.date.today().strftime("%Y-%m-%d")
        due_time = None

    payload: dict = {
        "subject":  byg_emne(til_pd),
        "type":     KANAL_TIL_AKTIVITETSTYPE.get(samtale.get("channel"),
                                                 STANDARD_AKTIVITETSTYPE),
        # Opkaldet ER sket. En uafsluttet aktivitet ville lægge sig i sælgerens
        # to-do-liste som noget HAN skal gøre, og det er præcis omvendt.
        "done":     True,
        "due_date": due_date,
        "org_id":   int(samtale["org_id"]),
        "note":     byg_note(samtale, til_pd),
    }
    if due_time:
        payload["due_time"] = due_time
    if ejer_id:
        payload["owner_id"] = int(ejer_id)
    return payload


# ---------------------------------------------------------------------------
# Public
# ---------------------------------------------------------------------------

def preview_opkalds_aktivitet(samtale: dict, udfald: list) -> dict:
    """Byg payloaden og vis den, uden at skrive noget i Pipedrive.

    Samme kontrakt som send_opkalds_aktivitet, så UI'et kan vise det ene svar
    med den anden kode. Slår ejeren op, hvis der er et token, så previewet også
    afslører HVEM aktiviteten ville lande hos.
    """
    account = samtale.get("account")
    token = _token(account)
    ejer_id = None
    if token and AKTIVITET_EJER == "org_ejer":
        ejer_id = _org_ejer(token, samtale["org_id"])

    payload = byg_aktivitet(samtale, udfald, ejer_id)
    if payload is None:
        return {"ok": True, "dry_run": True, "sendt": False,
                "aarsag": "ingen_kontakt",
                "besked": "Ingen af udfaldene har kontakt opnået, "
                          "så der sendes intet til Pipedrive."}
    return {
        "ok": True, "dry_run": True, "sendt": False,
        "aarsag": None if token else "mangler_token",
        "besked": ("Klar til at sende." if token else
                   f"Der er ingen API-token for {account!r} i .env, "
                   f"så intet ville blive sendt."),
        "payload": payload,
        "ejer_id": ejer_id,
        "antal_udfald": len(kontakt_udfald(udfald)),
    }


def send_opkalds_aktivitet(samtale: dict, udfald: list,
                           dry_run: bool = False) -> dict:
    """Læg samtalen i Pipedrive som en afsluttet aktivitet. KASTER ALDRIG.

    Returnerer altid en dict med `ok` og `sendt`. `ok` er False KUN når
    Pipedrive blev forsøgt og svarede fejl; at der intet var at sende
    (`ingen_kontakt`) eller ingen adgang (`mangler_token`) er ikke en fejl,
    fordi registreringen i begge tilfælde er lige så gyldig.

    Kaldes EFTER at outcomes.registrer_samtale har committet. Se modulets
    docstring for hvorfor rækkefølgen ikke må byttes om.
    """
    account = samtale.get("account")
    try:
        if dry_run:
            return preview_opkalds_aktivitet(samtale, udfald)

        token = _token(account)
        if not token:
            return {"ok": True, "sendt": False, "aarsag": "mangler_token",
                    "besked": f"Ingen Pipedrive-adgang for {account!r}. "
                              f"Udfaldet er gemt."}

        ejer_id = _org_ejer(token, samtale["org_id"]) \
            if AKTIVITET_EJER == "org_ejer" else None
        payload = byg_aktivitet(samtale, udfald, ejer_id)
        if payload is None:
            return {"ok": True, "sendt": False, "aarsag": "ingen_kontakt",
                    "besked": "Intet sendt til Pipedrive: der var ikke "
                              "kontakt på nogen af udfaldene."}

        data = _post(token, "/activities", payload)
        aktivitet_id = data.get("id")
        domaene = _kontodomaene(token)
        # ORGANISATIONEN og ikke aktivitetslisten. Linket pegede indtil
        # 2026-09-02 paa /activities/list/user/everyone, altsaa hele husets
        # aktiviteter - man skulle selv finde sin egen deri. Organisationens
        # side viser aktiviteten oeverst i History og er den side man alligevel
        # skal have fat i. Formen er verificeret i drift samme dag.
        url = (f"https://{domaene}.pipedrive.com/organization/{int(samtale['org_id'])}"
               if domaene else None)
        return {
            "ok": True, "sendt": True, "aarsag": None,
            "aktivitet_id": aktivitet_id,
            "aktivitet_url": url,
            "ejer_id": ejer_id,
            "antal_udfald": len(kontakt_udfald(udfald)),
            "besked": "Lagt i Pipedrive som afsluttet aktivitet.",
        }
    except Exception as exc:
        # Bredt og med vilje. Alt fra en DNS-fejl til en ændret payload-kontrakt
        # skal ende her, fordi alternativet er en 500 på en registrering, der
        # ALLEREDE er gemt: specialisten ville registrere den igen, og så stod
        # der to opkald i basen for ét opkald i virkeligheden.
        logger.exception("Pipedrive-aktivitet fejlede (account=%s, org_id=%s)",
                         account, samtale.get("org_id"))
        return {"ok": False, "sendt": False, "aarsag": "api_fejl",
                "fejl": str(exc)[:300],
                "besked": "Udfaldet er gemt, men kunne ikke sendes til "
                          "Pipedrive."}

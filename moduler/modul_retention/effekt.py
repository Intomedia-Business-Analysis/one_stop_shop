"""Performance og effekt: hvad de registrerede samtaler gav, og om vi naaede dem.

Datalaget bag de to paneler paa /retention/overview#performance. Fanen bar
indtil 2026-09-02 kun churn-grafen plus en tom tilstand, fordi
dbo.RetentionConversations og dbo.RetentionOutcomes havde nul raekker.

TO SPOERGSMAAL, TO RENE FUNKTIONER:

  saml_effekt          Hvad giver arbejdet? Kroner reddet mod tabt, samtaler,
                       kontaktrate, udfaldsfordeling.
  saml_traefsikkerhed  Naaede vi dem i tide? Af de kunder der sagde op, hvor
                       mange havde vi talt med foerst.

DET ER IKKE MAALINGSIDENS FORUDSIGELSESRATE. outcomes.py's indledning siger at
de to tabeller findes for at kunne kalibrere de syv zonevaegte, og det var
ogsaa hensigten. Men zonemodellen blev maalt tre gange og afskaffet som
praediktor 2026-08-26: faldende forbrug forudsiger ikke opsigelse, og halvdelen
af opsigelserne ligger i den sunde zone. Denne fane maaler derfor OPERATIONENS
effekt og ikke modellens traefsikkerhed. Bygges forudsigelsesraten senere, skal
den bygges paa et signal der virker, og ikke paa zonerne.

REN FUNKTION PLUS TYND SKAL, samme disciplin som varsel.py. Prisen er at
reglerne skal bevises et sted, og det sted er roegtest_effekt.py, som koerer
uden en databaseforbindelse.

REGLERNE FOR KRONER GENIMPLEMENTERES IKKE. "Kroner reddet" og "kroner tabt"
kommer fra prioritering.maanedens_kpier, den samme funktion Dagens opkald
bruger. To udgaver af det tal ville drive fra hinanden, og den ene ville have
`reddet_uden_beloeb`-forbeholdet og den anden ikke. Derfor importen nedenfor.

HVORFOR CACHE-WRAPPEREN LIGGER HER OG IKKE I cache.py: cache.py importerer
`varsel`, og prioritering.py importerer `cache`. Laa wrapperen i cache.py,
skulle cache.py importere dette modul, og saa er ringen lukket:
cache -> effekt -> prioritering -> cache. Invarianten i cache.py's docstring
holder alligevel, fordi cachet() skriver i det DELTE _cache -- ryd_cache()
rammer altsaa ogsaa denne post, og specialistens egen registrering forsvinder
ikke fra fanen i ti minutter.
"""
import logging
from datetime import date

from . import cache
from .outcomes import (AABNE_UDFALD, FORTSAT_KUNDE, KONTAKT_OPNAAET,
                       INTET_SITE, LUKKEDE_UDFALD, UDFALD)
from .prioritering import maanedens_kpier
from .queries import (abonnementer_med_ejer, db_opsigelsesdatoer,
                      er_aktiv_account)
from .usage import customer_key

logger = logging.getLogger(__name__)

# Raekkefoelgen i udfaldstabellen. Fast og ikke sorteret paa antal: tabellen
# laeses maaned efter maaned, og en raekke der flytter sig, fordi et tal
# aendrede sig, er svaer at foelge over tid. Grupperet efter hvad udfaldet
# BETYDER -- stadig kunde, sagen aaben, kunden vaek -- saa de tre blokke kan
# laeses uden at kende vokabularet.
UDFALD_RAEKKEFOELGE = list(FORTSAT_KUNDE) + list(AABNE_UDFALD) + list(LUKKEDE_UDFALD)


def _maaned_af(raekke: dict) -> str:
    """'YYYY-MM' ud af raekkens `contacted_at`.

    Reglen staar HER og ikke i SQL'en, saa den kun findes eet sted: maaneden
    gaar paa `contacted_at` og aldrig paa `created_at`. Et opkald taget 31.
    juli og tastet ind 1. august hoerer i juli.

    `contacted_at` er en datetime efter outcomes._normaliser. isoformat()[:7]
    frem for strftime, saa en date ogsaa ville virke -- en haandindsat raekke
    kan have en ren dato.
    """
    return raekke["contacted_at"].isoformat()[:7]


def talt_foerste(udfald: list) -> dict:
    """{(account, org_id, site): 'YYYY-MM-DD'} — FOERSTE samtale pr. abonnement.

    Ren funktion over db_alle_udfald()'s raekker, og med vilje adskilt fra
    saml_traefsikkerhed, saa den kan bevises for sig.

    FOERSTE og ikke seneste. Spoergsmaalet er "naaede vi dem inden opsigelsen
    var en kendsgerning", og det er sandt hvis BARE EEN samtale laa foer. Var
    det den seneste vi maalte, ville en kunde vi ringede til i marts og igen i
    juli staa som "for sent", fordi det sidste opkald laa efter.

    NOEGLEN GAAR GENNEM customer_key, fordi org_id kommer som INT herfra
    (kolonnen er int) mens abonnement-siden baerer den som STRENG. To noegler
    der ser ens ud og ikke er det, rammer aldrig hinanden, og opslaget ville
    tavst svare "aldrig talt med" om hver eneste kunde.
    """
    ud: dict = {}
    for r in udfald:
        kunde = customer_key(r["account"], r["org_id"])
        noegle = (kunde[0], kunde[1], r["site"])
        dato = r["contacted_at"].date().isoformat()
        # min(), fordi raekkefoelgen ikke maa vaere en forudsaetning. SQL'en
        # sorterer paa contacted_at i dag, men en ORDER BY der forsvinder er
        # praecis den slags aendring ingen opdager.
        if noegle not in ud or dato < ud[noegle]:
            ud[noegle] = dato
    return ud


def saml_effekt(udfald: list, tilladte: set | None, maaned: str) -> dict:
    """Hvad arbejdet gav. Ren funktion over db_alle_udfald()'s raekker.

    `tilladte` er de kunde-noegler brugeren maa se, eller None for ingen
    afgraensning -- praecis samme kontrakt som maanedens_kpier, der faar den
    videre uaendret.

    `maaned` er referencemaaneden ('YYYY-MM'), altsaa den maaned NOEGLETALLENE
    gaelder. Graf og tabel daekker alle maaneder. Det er samme opdeling som
    varsel-panellet bruger, og hvert element paa fanen skal selv sige hvad det
    daekker.

    NOEGLETALLENE ER SIDSTE HELE MAANED. Routeren regner den, se dens
    kommentar. En igangvaerende maaned paa et kort ville se endelig ud, og
    Portefoeljens egen regel er at et forloebigt tal aldrig maa vises som om
    det var endeligt.
    """
    # FILTRERET FOERST, saa grupperet. Raekkefoelgen er ikke ligegyldig:
    # grupperes de raa raekker, faar en maaned hvor ALLE raekker tilhoerer et
    # andet team stadig sin egen post i serien, med nuller. Grafen ville vise
    # en soejle paa nul, og det laeses som "vi arbejdede og fik ingenting" i
    # stedet for "dette team gjorde ingenting den maaned". Fanget af
    # roegtest_effekt.py, tilfaelde 5.
    if tilladte is not None:
        alle = [r for r in udfald
                if customer_key(r["account"], r["org_id"]) in tilladte]
    else:
        alle = list(udfald)

    pr_maaned: dict = {}
    for r in alle:
        pr_maaned.setdefault(_maaned_af(r), []).append(r)

    # `tilladte` sendes MED alligevel, selvom `alle` allerede er filtreret.
    # Det er ikke dobbeltarbejde der betyder noget (en haandfuld raekker), og
    # det er med vilje: maanedens_kpier EJER reglen for hvem der maa taelles,
    # og den maa ikke komme til at afhaenge af at dens kalder huskede at
    # filtrere. Fjernes argumentet her, virker koden indtil nogen flytter
    # filtreringen ovenfor.
    #
    # Kaldt pr. maaned og ikke een gang, fordi "antal samtaler" er DISTINKTE
    # conversation_id: summen af distinkte pr. maaned er ikke distinkt i alt.
    serie = []
    for m in sorted(pr_maaned):
        k = maanedens_kpier(pr_maaned[m], tilladte)
        serie.append({"maaned": m, **k})

    fordeling = []
    for navn in UDFALD_RAEKKEFOELGE:
        raekker = [r for r in alle if r["outcome"] == navn]
        if not raekker:
            continue
        fordeling.append({
            "outcome": navn,
            "label": UDFALD[navn],
            "antal": len(raekker),
            # arr_before_dkk for ALLE udfaldstyper, og det er et VALG: det er
            # beloebet der var paa spil da opkaldet blev taget, og det er
            # sammenligneligt paa tvaers af raekkerne. Reddet og tabt hoerer i
            # noegletallene, hvor maanedens_kpier ejer reglen -- en anden
            # kroneregel her ville give to tal der begge hedder "kroner".
            "arr_paa_spil": sum(float(r["arr_before_dkk"]) for r in raekker
                                if r["arr_before_dkk"] is not None),
            "uden_beloeb": sum(1 for r in raekker
                               if r["arr_before_dkk"] is None),
        })

    # Udfald UDEN outcome er ikke en fejl: biimplikationen i
    # CK_RetOut_outcome_kraever_kontakt siger at outcome er NULL praecis naar
    # der ikke var kontakt. De hoerer derfor i kontaktraten og ikke i tabellen.
    kontakt = sum(1 for r in alle if r["contact_result"] == KONTAKT_OPNAAET)

    denne = next((s for s in serie if s["maaned"] == maaned), None)
    return {
        "maaned": maaned,
        # None og ikke nuller, saa skabelonen kan skelne "ingen registreringer
        # i referencemaaneden" fra "nul kroner reddet". De to skal ikke se ens
        # ud paa et kort.
        "maanedens": denne,
        "serie": serie,
        "fordeling": fordeling,
        "kontakt_opnaaet": kontakt,
        "udfald_i_alt": len(alle),
        "samtaler_i_alt": len({r["conversation_id"] for r in alle}),
    }


def saml_traefsikkerhed(abonnementer: list, opsigelsesdatoer: dict,
                        foerste_samtale: dict, maaned: str) -> dict:
    """Naaede vi dem, foer opsigelsen var en kendsgerning hos os?

    `abonnementer` er raekker fra queries.abonnementer_med_ejer, altsaa PRAECIS
    risikolistens population: dansk afgraensning, B2B-filter, team-filter og
    ARR pr. abonnement ligger allerede inde i den. `opsigelsesdatoer` er
    queries.db_opsigelsesdatoer, `foerste_samtale` er talt_foerste() ovenfor,
    og `maaned` er OPHOERSMAANEDEN ('YYYY-MM').

    >>> HER BOR MIN-REGLEN, OG DEN SKAL BLIVE HER. <<<

    Populationen afgraenses paa `ophoer` (service_activation_date), fordi det
    er den dato hele huset regner paa, og fordi panelet saa maaler paa samme
    maaned som churn-taellingen.

    Men i tide/for sent kan IKKE afgoeres paa ophoeret. Varslet er median 34
    dage og p90 97 dage, saa et opkald midt i varslet ville taelle som "i
    tide", selvom kunden allerede havde sagt op. Panelet ville fortaelle
    specialisten at hun rammer fint, mens hun kun naaede folk der var gaaet.

    Og `besluttet` (won_time) alene kan det heller ikke: maalt 2026-09-02 er
    den registreret EFTER ophoeret i 7,5 % af alle opsigelser, op til 160 dage.

    kendsgerning = MIN(besluttet, ophoer) kan derfor aldrig smigre os. Den
    giver won_time i det normale tilfaelde og ophoeret i de bagudregistrerede.

    DEN MAALER VORES EGET SYSTEM, ikke kundens hoved. Vi kan ikke vide hvornaar
    hun besluttede sig, kun hvornaar det blev en kendsgerning hos os. Kald den
    derfor aldrig "foer kunden besluttede sig" i en overskrift.

    TO SITE-VOKABULARER, og de maa ikke blandes: opslaget i `opsigelsesdatoer`
    bruger den RAA `sites` (marketwire har None), mens `foerste_samtale` er
    noeglet med INTET_SITE, fordi RetentionOutcomes.site er NOT NULL og bruger
    sentinellen. Oversaettes den forkerte side, holder marketwire op med at
    kunne slaas op, uden at noget fejler. Samme faelde som kunde.py's note.
    """
    i_tide, for_sent, aldrig = [], [], 0

    for a in abonnementer:
        datoer = opsigelsesdatoer.get((a["account"], a["org_id"], a["sites"]))
        if not datoer or datoer["ophoer"][:7] != maaned:
            continue

        # Tekstsammenligning. 'YYYY-MM-DD' sorterer leksikografisk som den
        # sorterer kronologisk, og resten af modulet goer det samme.
        kendsgerning = min(datoer["besluttet"], datoer["ophoer"])

        talt = foerste_samtale.get(
            (a["account"], a["org_id"], a["sites"] or INTET_SITE))
        if talt is None:
            aldrig += 1
            continue

        arr = a.get("arr_pr_abonnement")
        raekke = {
            "account": a["account"],
            "org_id": a["org_id"],
            "org_name": a.get("org_name"),
            "site": a["sites"] or INTET_SITE,
            "owner_name": a.get("owner_name"),
            "arr_dkk": float(arr) if arr is not None else None,
            "talt": talt,
            "kendsgerning": kendsgerning,
            "ophoer": datoer["ophoer"],
            # Negativ naar vi naaede dem, positiv naar vi kom for sent. Regnet
            # her og ikke i skabelonen, saa fortegnet kun defineres eet sted.
            "dage": (date.fromisoformat(talt)
                     - date.fromisoformat(kendsgerning)).days,
        }
        (i_tide if talt <= kendsgerning else for_sent).append(raekke)

    i_alt = len(i_tide) + len(for_sent) + aldrig
    viste = i_tide + for_sent
    return {
        "maaned": maaned,
        "i_alt": i_alt,
        "i_tide": len(i_tide),
        "for_sent": len(for_sent),
        "aldrig": aldrig,
        # NUL ER IKKE UKENDT. arr_pr_abonnement er None, naar ACV ikke har et
        # beloeb for netop det site, og de raekker maa ikke taelle som 0 kr. --
        # saa ville summen se komplet ud. De taelles her i stedet, og panelet
        # skriver at summen er et minimum. Samme regel som varsel.saml_varsel.
        "arr_for_sent": sum(r["arr_dkk"] for r in for_sent
                            if r["arr_dkk"] is not None),
        "uden_arr": sum(1 for r in viste if r["arr_dkk"] is None),
        # Naaede foerst, saa de for sene. Inden for hver gruppe faldende paa
        # kroner: er der noget at laere af en raekke, er det den dyre.
        "raekker": (sorted(i_tide, key=lambda r: -(r["arr_dkk"] or 0))
                    + sorted(for_sent, key=lambda r: -(r["arr_dkk"] or 0))),
    }


def effektdata(teams: list | None = None, maaned: str | None = None) -> dict:
    """Hent opslagene og aggregér dem. Tynd skal om de to rene funktioner.

    `maaned` er referencemaaneden, 'YYYY-MM', og skal vaere SIDSTE HELE maaned.
    Routeren regner den, saa den indgaar i cache-noeglen.

    INGEN try/except. db_alle_udfald og db_opsigelsesdatoer svarer selv tomt
    ved fejl, og et panel der siger "0 samtaler registreret" er en LOEGN og
    ikke en tom tilstand. Fejler abonnementer_med_ejer, skal den boble op og
    blive til en 500, som panelet viser som en fejlbesked. Samme valg som
    varsel.opsigelser_i_varsel: et halvt panel er vaerre end et fejlet.
    """
    # Importeret inde i funktionen, ikke i modulhovedet: outcomes importeres
    # allerede for konstanterne, og db_alle_udfald hoerer paa skalsiden. Det er
    # ikke en cyklus-omgaaelse, kun laesbarhed -- de rene funktioner ovenfor
    # roerer aldrig databasen.
    from .outcomes import db_alle_udfald

    maaned = maaned or date.today().strftime("%Y-%m")
    abonnementer = abonnementer_med_ejer(maaned, teams=teams)
    udfald = db_alle_udfald()
    # Brand-afgraensningen. `tilladte` nedenfor daekker allerede alle TALLENE
    # i panelet, fordi den bygges af abonnementer_med_ejer, som gaar gennem
    # SQL-filtret. Men `meta.udfald_i_alt` taelles paa den RAA liste og bruges
    # af skabelonen til "for lidt data"-advarslen (retention_overview.html):
    # uden den her stod der 22 udfald, mens panelet regnede paa 16 (maalt
    # 2026-09-02, de seks er 5 monitor + 1 marketwire). Se
    # queries.er_aktiv_account.
    udfald = [u for u in udfald if er_aktiv_account(u["account"])]

    # Samme afgraensning som listerne paa fanen, altsaa kundens ejer og team og
    # ikke `created_by`. Er de to forskellige, beskriver noegletallene en anden
    # gruppe kunder end raekkerne nedenunder, og fanen modsiger sig selv uden
    # at nogen kan se hvorfor. None ville betyde "ingen afgraensning", saa den
    # bygges altid -- ogsaa firmabredt, hvor den bare rummer hele bogen.
    tilladte = {(a["account"], a["org_id"]) for a in abonnementer}

    effekt = saml_effekt(udfald, tilladte, maaned)
    traef = saml_traefsikkerhed(abonnementer, db_opsigelsesdatoer(),
                                talt_foerste(udfald), maaned)
    return {
        "effekt": effekt,
        "traefsikkerhed": traef,
        "meta": {
            "maaned": maaned,
            "abonnementer_i_bogen": len(abonnementer),
            # Grundlaget SKAL med ud. Er der 17 registrerede samtaler, er
            # "96 % aldrig talt med" ikke et fund om operationen, det er at
            # registreringen lige er begyndt. Panelet kan ikke skrive det
            # forbehold uden dette tal.
            "udfald_i_alt": len(udfald),
        },
    }


def effekt_cachet(teams: list | None, maaned: str) -> dict:
    """effektdata(), cachet i det DELTE cache fra cache.py.

    abonnementer_med_ejer alene tager omkring 2 sekunder, og db_opsigelsesdatoer
    scanner hele PipedriveDeals. Samme team-noegle-moenster som cache.varsel:
    teams er en liste og kan ikke vaere noegle, saa den sorteres til en tuple,
    og to kald med samme teams i forskellig raekkefoelge rammer samme post.

    `maaned` er MED i noeglen, saa et maanedsskift ikke serverer forrige
    maaneds tal i op til CACHE_SEKUNDER efter midnat.
    """
    noegle = ("effekt", maaned, tuple(sorted(teams)) if teams else None)
    return cache.cachet(noegle,
                        lambda: effektdata(teams=teams, maaned=maaned))

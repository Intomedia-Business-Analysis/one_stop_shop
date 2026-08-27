"""Prioritering: specialistens startside (Dagens opkald).

To lister i fast rækkefølge — først de aftaler der er lovet i dag, derefter nye
risici. Rækkefølgen er ikke til diskussion: et brudt løfte til en kunde, der
allerede overvejede at gå, er værre end en overset risiko.

FOLDNINGEN HØRER HER, ikke i risiko.py. Risikolaget måler abonnementer, fordi
det er dér signalet findes (Zonemodellen: en kunde med syv sites kan være stoppet på
ét og aktiv på seks). Prioriteringen tæller opkald, og man kan ikke ringe til
et abonnement. Blandes de to enheder i samme modul, begynder tallene at betyde
noget forskelligt fra linje til linje.

TO KILDER, TO FOLD: liste 1 folder udfalds-rækker fra outcomes.opfoelgninger, liste 2
folder risiko-rækker fra abonnementer_i_risiko. Det de deler er NØGLEN, ikke
funktionen. Se kunde_noegle."""

import logging
from datetime import date, timedelta

from . import cache
from .outcomes import (
    AABNE_UDFALD,
    FORTSAT_KUNDE,
    INTET_SITE,
    LUKKEDE_UDFALD,
    db_maanedens_udfald,
    db_seneste_udfald,
    naeste_hverdag,
    opfoelgninger,
    tilbage_paa_listen,
)
from .usage import customer_key
from .zones import zone_alvor

logger = logging.getLogger(__name__)

# Arbejdsgangens loft: hvor mange sager der må ligge åbne samtidig. Tælles PR KUNDE,
# ikke pr. abonnement — folder man ikke først, tæller Novo Nordisk som fire
# sager, og loftet binder cirka en tredjedel for tidligt (målt 2026-08-11:
# 7.044 kandidat-abonnementer fordelt på 5.320 kunder, faktor 1,32).
#
# UKALIBRERET, og det er vigtigere end tallet. 40 er læst ud af en ødelagt
# tabelrække i Arbejdsgang og kan ikke efterprøves, før der findes rigtige
# udfald. Tallet indebærer en påstand: ved Prioriteringsmodellens 10 kunder om
# dagen svarer 40 åbne sager til en gennemsnitlig sagslevetid på fire hverdage,
# og da Arbejdsgang giver en sag op til 3 forsøg over 10 hverdage, kræver det
# at mindst to ud af tre samtaler lukker ved første kontakt. Holder det ikke,
# binder loftet permanent, og siden holder op med at vise nye risici.
# Kalibreres af Målingsidens andel lukket på første kontakt — indtil da skal
# hver gang loftet binder logges, ellers opdager ingen det.
MAKS_AABNE_SAGER = 40

# Prioriteringsmodellen: listen er ti KUNDER lang, minus dagens opfølgninger.
# Ti rækker skal være ti opkald.
LISTE_LAENGDE = 10


def kunde_noegle(raekke: dict) -> tuple:
    """`(account, org_id)` med org_id som STRENG. Regler og Guardrails, regel 7.

    Delegerer til usage.customer_key, som er pakkens kanoniske nøglefunktion —
    den siger om sig selv "Brug ALTID denne". Den str()'er OG strip()'er begge
    led, mens outcomes._noegle_org_id kun str()'er org_id. To nøglefunktioner i
    samme pakke er den fejl der aldrig kaster: et opslag på (account, '6779') i
    en ordbog nøglet med (account, ' 6779') rammer ikke.

    ADVARSEL, og det er hele grunden til at helperen findes: de to kilder bærer
    org_id i FORSKELLIG type. Risikolagets rækker har den som streng ('2084'),
    mens udfalds-rækkerne kommer råt fra en INT-kolonne (2084). Nøgles de hver
    for sig, rammer liste 2's udelukkelse af liste 1's kunder ALDRIG — og den
    fejl kaster ingenting, den viser blot samme kunde to gange på samme side."""

    return customer_key(raekke["account"], raekke["org_id"])


def fold_opfoelgninger(raekker: list, i_dag: date, navne: dict) -> list:
    """Åbne opfølgninger foldet til én post pr. kunde, ældste dato først.

    `raekker` er outcomes.opfoelgninger's output — én pr. ABONNEMENT.
    `navne` er {(account, org_id): org_name}; se HVORFOR nedenfor.
    Ud: én post pr. kunde, altså ét opkald (Prioriteringsmodellen).

    `i_dag` skal være en `date`, ikke en `datetime`. `followup_date` er en
    rigtig `date` (outcomes._normaliser), og de to typer kan ikke sammenlignes
    — en `datetime` her rejser TypeError første gang der FINDES en opfølgning,
    altså i produktion og aldrig hos dig.

    SORTERINGEN er ældste opfølgning først. Har en kunde både en overskreden og
    en dagsaktuel aftale, bedømmes rækken på den overskredne: det er det brudte
    løfte der skal ringes op, og det lægger sig dermed øverst af sig selv.
    `overskredet` er en visningsmarkering (Dagens opkald), ikke et filter —
    outcomes.opfoelgninger har allerede taget alt til og med i dag.

    HVORFOR NAVNET KOMMER UDEFRA: dbo.RetentionOutcomes har ingen org_name-
    kolonne, kun (account, org_id, site). Og navnet kan ikke uden videre hentes
    fra risiko-rækkerne, for de indeholder kun abonnementer der er AKTIVE I
    MÅNEDEN — mens en kunde man har lovet at ringe tilbage til ofte er præcis
    en, hvis abonnement er ophørt. Så ville den vigtigste række på siden stå
    uden navn. `account` kan ikke bruges som nødløsning: det er Pipedrives
    forretningsenhed ('monitor', 'watch'), ikke et firmanavn. Se queries.py."""

    pr_kunde: dict = {}
    for r in raekker:
        noegle = kunde_noegle(r)
        post = pr_kunde.get(noegle)
        if post is None:
            post = {
                "account":             noegle[0],
                "org_id":              noegle[1],
                "org_name":            navne.get(noegle),
                "aeldste_opfoelgning": r["followup_date"],
                "abonnementer":        [],
            }
            pr_kunde[noegle] = post
        if r["followup_date"] < post["aeldste_opfoelgning"]:
            post["aeldste_opfoelgning"] = r["followup_date"]
        post["abonnementer"].append({
            "site":          r["site"],
            "outcome":       r["outcome"],
            "followup_date": r["followup_date"],
        })

    poster = list(pr_kunde.values())
    for p in poster:
        p["overskredet"] = p["aeldste_opfoelgning"] < i_dag
        p["abonnementer"].sort(key=lambda a: a["followup_date"])
    # Navnet bryder uafgjort, så to kunder med samme dato ikke bytter plads
    # mellem to sideindlæsninger. En liste der flytter sig af sig selv koster
    # tillid, som en vilkårlig men FAST rækkefølge ikke gør.
    poster.sort(key=lambda p: (p["aeldste_opfoelgning"], p["org_name"] or ""))
    return poster


def antal_aabne_sager(seneste: dict) -> int:
    """Antal KUNDER med et uindfriet løfte. Arbejdsgangens "bunke".

    Tælles pr. kunde og ikke pr. abonnement, fordi loftet er et loft over
    OPKALD. Uden foldningen tæller Novo Nordisk som fire sager, og loftet binder
    cirka en tredjedel for tidligt — målt 2026-08-11: 7.044
    kandidat-abonnementer fordelt på 5.320 kunder.

    KUN `AABNE_UDFALD` tæller, altså `forskudt` og `tilbud_sendt`. En `fornyet`
    kunde med 180 dages udsættelse er IKKE en åben sag: hun skylder ingen noget,
    hun hviler. Talte udsættelser med, ville tallet vokse med ti om dagen og
    først falde et halvt år senere — loftet ville lukke for nye risici permanent
    inden for en uge, netop fordi specialisten havde gjort sit arbejde godt.
    """
    return len({kunde_noegle(r) for r in seneste.values()
                if r["outcome"] in AABNE_UDFALD})


def fold_risici(raekker: list, seneste: dict, i_dag: date,
                kun_opkaldsklare: bool = True) -> list:
    """Nye risici foldet til én post pr. kunde, vigtigste først.

    Ind: `abonnementer_i_risiko()`s rækker — én pr. ABONNEMENT — og
    `db_seneste_udfald()`s ordbog. Ud: én post pr. kunde, altså ét opkald.

    INGEN POLITIK HER. Funktionen kender hverken LISTE_LAENGDE eller
    MAKS_AABNE_SAGER og svarer kun på "hvem er vigtigst, i hvilken rækkefølge".
    Afkortningen ligger i afkort_nye_risici, fordi den er ukalibreret og skal
    kunne læses og ændres uden at man rører rangeringen.

    Prioriteringsmodellens FEM FILTRE er tre tjek her. Filter 1 (ikke aktivt i
    indeværende måned) er allerede klaret af risikolaget, som kun returnerer
    månedens abonnementer. Filter 3 og 4 er blevet den SAMME sammenligning:
    tilbage_paa_listen svarer altid, så "åben opfølgning i fremtiden" og
    "uudløbet udsættelse" er ét udtryk. Se Fristmodellen.

    NØGLEN TIL `seneste` ER IKKE kunde_noegle. Den er tre led og bruger
    site-sentinelen: `dbo.retention.sites` er NULL for marketwires rækker, og en
    nøgle med NULL i kan aldrig slås op igen — NULL = NULL er ukendt, ikke sandt.
    Glemmes `or INTET_SITE`, finder marketwire aldrig sit eget udfald, filter 3
    og 4 holder op med at virke for dem, og de dukker op på listen igen dagen
    efter et opkald, uden at noget fejler. Samme opslag som kunde.py's
    detaljeside.

    `kun_opkaldsklare` (default True) er den oprindelige og eneste adfærd fra
    før parameteren fandtes, og den ændres ikke: et abonnement der rammer et af
    de tre filtre udelukkes helt. Sat til False udelukkes intet. Hvert
    abonnement får i stedet et felt `spaerre`, som er `None` når det er
    opkaldsklart, og ellers en af `"fast_laeser"`, `"mikrokunde"`, `"opsagt"`
    eller `"udsat"`, i den rækkefølge filtrene rammer. Kundens `score` summerer
    stadig KUN de opkaldsklare abonnementer, så rangeringen af de kaldbare
    kunder er den samme i begge tilstande. Feltet `opkaldsklar` på kunden er
    sandt, hvis mindst ét af hendes abonnementer er det, ellers ligger hun i
    bunden af listen med score 0, synlig men urørt.

    De to tilstande findes side om side, fordi opkaldslistens rangering er
    kalibreret (se MAKS_AABNE_SAGER) og skal kunne bevises uændret, mens den
    samlede liste på "Opkald og risiko" har brug for at vise ALT, inklusive de
    fem afgrænsninger, som slåbare valg i stedet for skjulte fravalg.
    """
    pr_kunde: dict = {}
    for r in raekker:
        # Landeafgraensningen laa her indtil 2026-08-25. Den ligger nu i
        # queries._KUN_DANSKE og gaelder hele modulet, saa raekkerne der naar
        # hertil er allerede danske. Se queries.py.
        #
        # De fem filtre er nu FIRE MULIGE AARSAGER plus "ingen". Raekkefoelgen
        # er den samme som de tre continue-saetninger havde foer parameteren
        # kun_opkaldsklare fandtes, saa en raekke der rammer flere aarsager
        # faar den SAMME ene aarsag som tidligere, uanset hvilken tilstand
        # funktionen koeres i.
        if r["zone"] == "fast_laeser":
            spaerre = "fast_laeser"
        elif r["mikrokunde"]:
            spaerre = "mikrokunde"
        elif r["opsagt_dato"]:
            # OPSAGTE HOERER IKKE PAA OPKALDSLISTEN. Et abonnement med en
            # gaeldende opsigelse er ikke en risiko, det er et faktum, og et
            # opkald der rangerer efter risiko kan ikke redde det. Det staar
            # i stedet paa kundesiden med sin ophoersdato.
            #
            # Maalt 2026-08-19: Deloitte laa nummer to med 163.300. Syv af
            # deres abonnementer er opsagt med ophoer fra 13-09 til
            # 03-11-2026, og de faldt derfor til 123.700 og plads fire. De
            # BLIVER paa listen med de tre der stadig loeber, og det er
            # netop derfor filteret er pr. abonnement og ikke pr. kunde.
            spaerre = "opsagt"
        else:
            # Filter 3 og 4. Findes der intet udfald, er der intet at
            # udsaette paa.
            u = seneste.get((r["account"], r["org_id"], r["site"] or INTET_SITE))
            spaerre = ("udsat" if u is not None and tilbage_paa_listen(u) > i_dag
                       else None)

        if spaerre is not None and kun_opkaldsklare:
            continue

        # Kopi, ikke den delte raekke fra risiko_data["rows"]: samme liste
        # sendes ind i BEGGE tilstande fra prioriteringsdata, og skrev vi
        # "spaerre" direkte paa r, ville det andet kald overskrive det foerste
        # kalds resultat paa de samme objekter.
        abo = dict(r)
        abo["spaerre"] = spaerre

        noegle = kunde_noegle(r)
        post = pr_kunde.get(noegle)
        if post is None:
            post = {
                "account":       noegle[0],
                "org_id":        noegle[1],
                "org_name":      r["org_name"],
                "kunde_arr_dkk": r["kunde_arr_dkk"],
                "score":         0.0,
                "vaerste_zone":  r["zone"],
                "opkaldsklar":   False,
                "abonnementer":  [],
            }
            pr_kunde[noegle] = post

        if spaerre is None:
            post["score"] += r["score"] or 0.0
            post["opkaldsklar"] = True
        if zone_alvor(r["zone"]) < zone_alvor(post["vaerste_zone"]):
            post["vaerste_zone"] = r["zone"]
        post["abonnementer"].append(abo)

    poster = list(pr_kunde.values())
    for p in poster:
        # TO TAL og ikke én `uopgjort`-boolean: en boolean skal vælge en
        # tærskel, og "mindst ét mangler ARR" flager Novo Nordisk med 1 ud af 30
        # lige så højt som en kunde med 1 ud af 1. En advarsel der fyrer på de
        # fleste rækker bliver ignoreret, og det er værre end ingen. To tal er
        # den rå kendsgerning; siden kan så tie når de er ens og sige "scoren
        # dækker 3 af 4 abonnementer" når de ikke er. Tærsklen bliver dermed en
        # skabelonændring frem for en ændring i risikolaget.
        p["antal_abonnementer"] = len(p["abonnementer"])
        p["abonnementer_med_arr"] = sum(1 for a in p["abonnementer"]
                                        if a["arr_dkk"] is not None)
        # Samme rækkefølge som Kundesidens detaljeside, så den udfoldede række og
        # kundesiden ikke viser kundens abonnementer i to forskellige ordner.
        p["abonnementer"].sort(key=lambda a: (a["score"] is None,
                                              -(a["score"] or 0),
                                              zone_alvor(a["zone"])))

    # Prioriteringsmodellen: score faldende, derefter zonens alvor, derefter kundens ARR
    # faldende. Nøglen er KORTERE end kunde.py:146, hvor `score is None` skal
    # forrest — her er scoren en SUM og bliver aldrig None. En kunde uden kendt
    # ARR summerer til 0,0 og lander i bunden af sig selv.
    #
    # Der skelnes bevidst IKKE mellem "score 0 fordi vægten er 0" og "score 0
    # fordi ARR er ukendt", selv om de betyder noget forskelligt, af samme
    # grund som resten af funktionen: politik hoerer i skabelonen, ikke her.
    # Dagen kom 2026-08-27, da den samlede liste fik paging: skabelonen
    # (retention_opkald.html) grupperer nu kunder uden kendt aarsvaerdi i en
    # egen bundgruppe nederst, beregnet over hendes SYNLIGE abonnementer, ikke
    # over denne sortering. Denne funktion aendrer sig ikke, den kender stadig
    # ikke forskellen.
    poster.sort(key=lambda p: (-p["score"],
                               zone_alvor(p["vaerste_zone"]),
                               -(p["kunde_arr_dkk"] or 0)))
    return poster

def afkort_nye_risici(poster: list, antal_opfoelgninger: int,
                      aabne_sager: int, afgraensning_tom: bool = False) -> dict:
    """Hvor mange af posterne der vises — og hvorfor der ikke er flere.

    Ud: `{"poster", "aabne_sager", "plads", "aarsag"}`. `aarsag` er `None`,
    `"afgraensning_tom"`, `"loft"`, `"opfoelgninger_fylder"` eller
    `"tom_bunke"`.

    `afgraensning_tom` betyder, at brugerens team-afgrænsning ikke matcher ÉN
    eneste kunde — altså en HubUserTeamAccess, der peger på et teamnavn, som
    ikke findes. Den kommer først, før loftet, fordi den gør alle de andre svar
    usande: uden den ville siden se 0 kandidater og 10 pladser og svare
    "tom_bunke", altså "du er igennem bunken". Det er den mest beroligende
    besked siden kan give oven på den mest ødelagte tilstand den kan være i —
    målt: teams=["Watch DK"] giver 0 kunder, fordi navnene i ACV hedder "Team
    Watch DK". Specialisten ville gå hjem, mens 5.033 kunder står urørt.

    ÅRSAGEN ER EN NØGLE, ikke en sætning. Dansk tekst hører i skabelonen, samme
    regel som outcomes.py skriver om sit vokabular: værdien er kontrakten,
    labelen er til mennesker. En sætning her ville før eller siden blive
    sammenlignet med == inde i en skabelon og kunne ikke rettes uden at knække
    noget.

    Og de tre årsager må ikke se ens ud på skærmen. Alle tre giver en tom liste,
    men `"loft"` er en INSTRUKS — ryd op — mens de to andre er tilstande. Ser
    den ud som de andre, læses den som "der er ikke mere at lave".

    `aabne_sager` returneres ALTID, ikke kun når loftet binder. Det er tallet,
    der skal kalibrere MAKS_AABNE_SAGER: findes det kun i det øjeblik væggen
    rammes, får målingen et binært signal, bandt eller bandt ikke, og man kan
    aldrig se, om det rigtige loft var 25 eller 90. Lå tallet typisk på 12, er
    40 dekoration; lå det på 38 hver dag, kvæler loftet arbejdet, uden at nogen
    har besluttet det. Siden viser det som en stille tæller ved de tre KPI'er.

    RÆKKEFØLGEN er Arbejdsgangens: loftet er en HÅRD port og går forud for
    pladsregnestykket — "over det lukkes nye risici, indtil bunken er nede".
    """
    if afgraensning_tom:
        logger.warning("Team-afgraensningen matcher ingen kunder. Tjek "
                       "HubUserTeamAccess — navnene i ACV har praefikset "
                       "'Team ', fx 'Team Watch DK'.")
        return {"poster": [], "aabne_sager": aabne_sager, "plads": 0,
                "aarsag": "afgraensning_tom"}

    if aabne_sager >= MAKS_AABNE_SAGER:
        logger.warning("Loftet binder: %s aabne sager, graensen er %s. "
                       "Ingen nye risici vises.",
                       aabne_sager, MAKS_AABNE_SAGER)
        return {"poster": [], "aabne_sager": aabne_sager, "plads": 0,
                "aarsag": "loft"}

    plads = max(0, LISTE_LAENGDE - antal_opfoelgninger)
    if plads == 0:
        return {"poster": [], "aabne_sager": aabne_sager, "plads": 0,
                "aarsag": "opfoelgninger_fylder"}

    valgte = poster[:plads]
    return {"poster": valgte, "aabne_sager": aabne_sager, "plads": plads,
            "aarsag": "tom_bunke" if len(valgte) < plads else None}


def maanedens_kpier(raekker: list, tilladte: set | None) -> dict:
    """Målingsidens tre tal for måneden. Ren funktion over db_maanedens_udfald.

    `tilladte` er de kunde-nøgler, brugeren må se — eller `None` for ingen
    afgrænsning. Det er SAMME afgrænsning som listerne nedenunder på siden,
    altså kundens ejer og team fra routerens _resolve_filters, og ikke
    `created_by`. Er de to forskellige, beskriver tallene øverst en anden gruppe
    kunder end rækkerne nedenunder, og siden modsiger sig selv uden at nogen kan
    se hvorfor. `None` giver hele firmaet, som _resolve_filters selv gør for en
    leder eller admin: én kode, to rigtige svar.

    "Antal samtaler" er DISTINKTE `conversation_id`. Én samtale om syv
    abonnementer er ét opkald, ikke syv.

    `reddet_uden_beloeb` er med af samme grund som de to tal i fold_risici: uden
    det lyver "kroner reddet". `arr_after_dkk` gemmes kun, når BÅDE beløb og
    kurs blev udfyldt (se registrer_samtale), så en fornyelse uden beløb tæller
    som 0 kr. reddet. To tal frem for et gennemsnit, så siden kan sige "12
    fornyelser, heraf 3 uden beløb" i stedet for at påstå et tal.

    KENDT BLINDHED, Hvad Specialisten kan registrere — og det er IKKE en
    undervurdering af beløbet: `opgraderet` findes ikke i CK_RetOut_outcome, så
    en fornyelse med prisstigning registreres som `fornyet`. Men
    `arr_after_dkk` er den NYE pris, så stigningen tælles fuldt med i "kroner
    reddet". Det der mangler, er evnen til at SKELNE en opgradering fra en flad
    fornyelse — Målingside kan derfor ikke rapportere vækst fra opgraderinger
    for sig.

    Formuleringen er rettet 2026-08-12: docstringen sagde tidligere at tallet var
    "systematisk for lav", hvilket modsagde dens egen næste sætning om at
    stigningen bliver målt. Den ENESTE grund til at beløbet er for lavt er
    `reddet_uden_beloeb` — en fornyelse uden beløb tæller som 0 kr.
    """
    if tilladte is not None:
        raekker = [r for r in raekker if kunde_noegle(r) in tilladte]

    reddet = sum(float(r["arr_after_dkk"]) for r in raekker
                 if r["outcome"] in FORTSAT_KUNDE
                 and r["arr_after_dkk"] is not None)
    tabt = sum(float(r["arr_before_dkk"]) for r in raekker
               if r["outcome"] in LUKKEDE_UDFALD
               and r["arr_before_dkk"] is not None)
    return {
        "reddet":             reddet,
        "tabt":               tabt,
        "samtaler":           len({r["conversation_id"] for r in raekker}),
        "reddet_uden_beloeb": sum(1 for r in raekker
                                  if r["outcome"] in FORTSAT_KUNDE
                                  and r["arr_after_dkk"] is None),
    }


def naeste_udtraek(i_dag: date) -> date:
    """Datoen for det naeste forbrugsudtraek: den foerste HVERDAG i en maaned.

    Kadencen er den foerste hverdag og ikke den 1., fordi den 1. kan falde paa en
    loerdag eller en helligdag, og en frist der ikke kan efterleves er ingen
    frist. naeste_hverdag() kender baade weekender og helligdage, saa kalenderen
    staar ET sted (outcomes.HELLIGDAGE, 2020-2040).

    Ligger denne maaneds frist forude, er det den. Er den passeret, er det naeste
    maaneds. Dagen SELV taeller som forude: falder udtraekket i dag, er svaret i
    dag, og siden skriver "i dag" frem for at pege paa naeste maaned.
    """
    def foerste_hverdag(aar: int, maaned: int) -> date:
        # naeste_hverdag giver den foerste hverdag EFTER sin dato, saa der spoerges
        # fra dagen FOER den 1. Rammer den 1. en hverdag, er svaret den 1.
        return naeste_hverdag(date(aar, maaned, 1) - timedelta(days=1))

    denne = foerste_hverdag(i_dag.year, i_dag.month)
    if i_dag <= denne:
        return denne
    aar, maaned = ((i_dag.year + 1, 1) if i_dag.month == 12
                   else (i_dag.year, i_dag.month + 1))
    return foerste_hverdag(aar, maaned)

def prioriteringsdata(i_dag: date, teams: list | None = None,
                      abo_maaned: str | None = None) -> dict:
    """Alt hvad prioriteringssiden skal vise. Dagens opkald.

    `i_dag` er KRÆVET og har ingen default. Klokken læses ÉN gang, i ruten:
    kaldes `date.today()` to gange under samme sideopslag, kan de to kald ligge
    på hver sin side af midnat, og siden ville beregne opfølgninger mod i dag og
    KPI'er mod i morgen. Sandsynligheden er lille, men prisen for at gøre fejlen
    umulig er ét argument — samme valg som i tilbage_paa_listen.

    INGEN `owner_name`. `_resolve_filters` returnerer altid None for den:
    retention er lukket for alt under Sales Operations (besluttet 2026-08-10),
    og specialisten skal se hele firmaets churn-billede — en sælger har ingen
    adgang overhovedet. Team-afgrænsning kan stadig forekomme, hvis en admin har
    sat HubUserTeamAccess; ubegrænset giver hele firmaet, inkl. de 16% kunder
    uden tilskrevet ejer i Pipedrive.

    `abo_maaned` findes for at kunne genskabe en verificeret måned i en
    kontrolkørsel, som i risiko.abonnementer_i_risiko. Ruten sender den aldrig.

    `db_seneste_udfald()` kaldes ÉN gang og afgrænses ÉN gang. Tre ting læser
    den — liste 1, loftets tæller, og Prioriteringsmodellens filter 3 og 4.
    Afgrænses den her frem for i hver af de tre, kan afgrænsningen ikke glemmes
    i én af dem, og de tre tal kan ikke blive uenige om noget, der ændrer sig
    mens siden bygges.

    TO MÅNEDER, og det er ikke en fejl: `maaned` er indeværende og gælder
    KPI'erne (Dagens opkald: "tre tal for indeværende måned"), mens
    `reference_maaned` er sidste HELE måned og gælder zonerne (Zonemodellen).
    Begge sendes med, fordi siden SKAL skrive dem. Gør den ikke det, spørger
    nogen hver måned, hvorfor "kroner reddet" er lille, mens risikolisten er
    lang — og tror, at det ene modsiger det andet.

    `risiko`-nøglen bærer HELE risikobilledet videre (alle rækker, zonerne,
    rækkefølgen), ikke kun de folder-rester `fold_risici` selv har brug for.
    Den findes, fordi "Opkald og risiko" (sammenlagt 2026-08-27 af Dagens
    opkald og Churn-risiko) skal kunne tegne zonekortene og den fulde tabel af
    ét og samme kald — kaldte siden `/retention/risk` derved selv, ville
    `abonnementer_i_risiko()` (3,6 sekunder ukachet) køre igen oven på det
    `cache.risiko()` allerede lige har regnet.

    `nye_risici` og `risikoliste` er to LAG af samme fold, ikke to uafhaengige
    beregninger. `nye_risici` er dagens arbejde: kaldbar-kun, liste 1
    udelukket, afkortet til pladsen efter MAKS_AABNE_SAGER og LISTE_LAENGDE.
    `risikoliste` er totalen bag "Opkald og risiko"s samlede liste (sammenlagt
    af Nye risici og Abonnementer i risiko, 2026-08-27): ALLE kunder, ALLE
    abonnementer, ingen af de fem afgraensninger fjernet, kun markeret via
    `spaerre` (se fold_risici) og via `aftale_i_dag` og `dagens_plads`
    herunder. `fold_risici` kaldes derfor TO gange, én gang i hver tilstand, i
    stedet for at udlede den ene af den anden: `vaerste_zone` og
    `antal_abonnementer` regnes over FORSKELLIGE maengder abonnementer i de to
    tilstande (kun opkaldsklare mod alle), og en udledning ville aendre
    `vaerste_zone` for `nye_risici`s poster.

    `dagens_plads` paa en `risikoliste`-post er sandt for netop de
    kundenoegler, som `nye_risici["poster"]` valgte ud. Den bruges IKKE til at
    afkorte `risikoliste`, kun til at maerke de samme raekker med et tag i
    skabelonen, saa "dagens arbejde" er synligt inde i den fulde liste i
    stedet for at vaere en separat, kortere liste.
    """
    risiko_data = cache.risiko(teams, abo_maaned)
    navne = cache.navne()
    # Ingen team-afgrænsning betyder INGEN begrænsning, ikke "kunder med en
    # ACV-række". Samme semantik som queries.abonnementer_med_ejer: uden filter
    # beholdes kunder uden ACV-række, med filter droppes de — og risiko-rækkerne
    # er afgrænset på præcis samme måde, så de to kan ikke komme i utakt.
    tilladte = set(cache.ejere(teams)) if teams else None

    seneste = db_seneste_udfald()
    if tilladte is not None:
        seneste = {n: r for n, r in seneste.items()
                   if kunde_noegle(r) in tilladte}

    liste1 = fold_opfoelgninger(opfoelgninger(seneste, i_dag), i_dag, navne)

    # En kunde på liste 1 udelukkes HELT fra liste 2. Prioriteringsmodellens
    # filter 3 fjerner kun abonnementer med opfølgning i FREMTIDEN, mens liste
    # 1 viser alt til og med i dag — så en opfølgning der forfalder netop i dag
    # er i ingen af dem, og kunden ville stå på begge lister. Så ville "ti
    # minus opfølgninger" tælle hende to gange: ti rækker, otte opkald.
    # Specialisten ringer én gang og taler om hele kunden.
    paa_liste1 = {(p["account"], p["org_id"]) for p in liste1}
    rangeret = [p for p in fold_risici(risiko_data["rows"], seneste, i_dag)
                if (p["account"], p["org_id"]) not in paa_liste1]

    nye_risici = afkort_nye_risici(
        rangeret, len(liste1), antal_aabne_sager(seneste),
        # Sat men tom = afgrænsningen matchede ingen kunder. Se
        # afkort_nye_risici for hvorfor det ikke må ligne en tom bunke.
        afgraensning_tom=tilladte is not None and not tilladte)

    # Den samlede liste til "Opkald og risiko". Se docstringen ovenfor for
    # forskellen til nye_risici.
    risikoliste = fold_risici(risiko_data["rows"], seneste, i_dag,
                              kun_opkaldsklare=False)
    dagens_plads_noegler = {(p["account"], p["org_id"])
                            for p in nye_risici["poster"]}
    for p in risikoliste:
        noegle = (p["account"], p["org_id"])
        p["aftale_i_dag"] = noegle in paa_liste1
        p["dagens_plads"] = noegle in dagens_plads_noegler

    maaned = i_dag.strftime("%Y-%m")
    udtraek = naeste_udtraek(i_dag)
    return {
        "maaned":           maaned,
                # Loftet skal MED i svaret og ikke skrives af i skabelonen: staar 40 to
        # steder, driver de fra hinanden, og kortet ville paastaa en graense der
        # ikke gaelder.
        "maks_aabne_sager":  MAKS_AABNE_SAGER,
        "naeste_udtraek":    udtraek.isoformat(),
        "dage_til_udtraek":  (udtraek - i_dag).days,"reference_maaned": risiko_data["meta"]["reference_maaned"],
        "kpier":            maanedens_kpier(db_maanedens_udfald(maaned),
                                            tilladte),
        "opfoelgninger":    liste1,
        "nye_risici":       nye_risici,
        "risikoliste":      risikoliste,
        # Bæres med, så siden kan vise forbeholdet fra Churn-risiko: mangler
        # forbrugsfilen, står ALLE abonnementer som "intet signal", og en liste
        # der ser tom for risiko ud er den farligste visning siden kan lave.
        "meta":             risiko_data["meta"],
        # Det UFOLDEDE risikobillede, pr. abonnement, til zonekortene på
        # "Opkald og risiko". `rows` her er ALLE abonnementer; `risikoliste`
        # ovenfor er den kundefoldede udgave af de samme rækker.
        "risiko": {
            "rows":         risiko_data["rows"],
            "zones":        risiko_data["zones"],
            "zone_order":   risiko_data["zone_order"],
            "gruppe_order": risiko_data["gruppe_order"],
        },
    }

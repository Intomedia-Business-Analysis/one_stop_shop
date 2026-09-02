"""Skrivesiden: registrerede samtaler og udfald.

Læsesiden (risiko.py) siger hvem der BØR ringes til. Den her siger hvad der
FAKTISK skete. Uden den kan Målingsidens forudsigelsesrate aldrig beregnes, og så
forbliver alle syv zonevægte skøn.

TO TABELLER, ÉN TRANSAKTION: en samtale uden udfald er en tom række, og et
udfald uden samtale er umuligt (fremmednøglen er NOT NULL). Derfor skriver
registrer_samtale() begge dele under ét — går udfaldet galt, forsvinder
samtalen med det.

INTET OPDATERES, jf. Regler og Guardrails punkt 5: der indsættes altid en ny
række. Ringer man igen om samme abonnement, er det et nyt udfald og ikke en
rettelse af det gamle.
Det er derfor "seneste udfald" er et opslag og ikke bare en kolonne.
"""
import datetime as dt
import logging

from db import get_conn

logger = logging.getLogger(__name__)

# Samme sentinel som churn-beregningen i queries.py. dbo.retention.sites er
# NULL for marketwires 35 rækker, og en nøgle med NULL i kan aldrig slås op
# igen — NULL = NULL er ukendt, ikke sandt.
#
# STADIG I BRUG efter at marketwire blev deaktiveret 2026-09-02
# (queries.DEAKTIVEREDE_ACCOUNTS). Marketwire var den oprindelige grund, men
# watch_medier har SELV rækker uden site — 2 aktive abonnementer, målt
# 2026-09-02 på august. Sentinellen er altså ikke blevet død kode og må ikke
# ryddes op sammen med resten af marketwire-sporet.
INTET_SITE = "(intet site)"

# Udfald der holder sagen åben. Hvad Specialisten kan registrere: de kræver
# followup_date, hvilket databasen håndhæver i CK_RetOut_followup_paa_aabne.
AABNE_UDFALD = ("forskudt", "tilbud_sendt")

# De to andre grupper Fristmodellen behandler ens. Grupperne står som konstanter og
# ikke som strenge nede i logikken, af samme grund som AABNE_UDFALD: skal en
# gruppe udvides, sker det ét sted, og læseren kan se HVORFOR to udfald deler
# regel.
LUKKEDE_UDFALD = ("opsagt", "allerede_opsagt")   # abonnementet findes ikke mere
FORTSAT_KUNDE = ("fornyet", "nedgraderet")       # stadig kunde, ny frist

# Hvor `arr_before_dkk` kom fra. ARR pr. abonnement er kundens ARR divideret
# med antal sites (queries.py: "lige deling er et VALG, ikke en måling"), fordi
# ACV's og retentions site-vokabularer ikke kan brolægges endnu. Registreres et
# udfald på det tal, arver Målingsidens "kroner reddet" divisionen — og et gæt kan
# ikke skelnes fra et målt beløb i en decimal-kolonne bagefter.
#
# Specialisten har den rigtige pris foran sig under opkaldet. Formularen
# forudfylder med delingen (`lige_deling`) og skifter til `bekraeftet`, så snart
# feltet redigeres. Databasen håndhæver værdierne i CK_RetOut_arr_kilde.
ARR_KILDE_DELING = "lige_deling"
ARR_KILDE_BEKRAEFTET = "bekraeftet"

# DATABASENS VOKABULAR, ikke UI-tekst. Nøglerne SKAL være præcis de værdier
# CK_RetConv_channel, CK_RetOut_contact_result og CK_RetOut_outcome tillader.
# De står her og ikke i skabelonen, fordi en formular med et valg databasen
# afviser først fejler EFTER opkaldet, når specialisten har lagt på og ikke kan
# spørge igen. Værdien er kontrakten; labelen er til mennesker.
KANALER = {"telefon": "Telefon", "mail": "Mail", "moede": "Møde"}

# Det ene resultat der kræver et udfald. CK_RetOut_outcome_kraever_kontakt er en
# BIIMPLIKATION: kontakt opnået ⇒ udfald skal være sat, alt andet ⇒ udfald skal
# være NULL. Begge retninger håndhæves, så et udfald kan ikke smugles ind på en
# samtale hvor der ikke var nogen i røret.
KONTAKT_OPNAAET = "kontakt_opnaaet"
# De to andre er navngivet af samme grund som KONTAKT_OPNAAET: de bruges i
# logikken i tilbage_paa_listen, og en streng skrevet i hånden to steder bliver
# uenig med sig selv før eller siden.
INGEN_KONTAKT = "ingen_kontakt"
IKKE_KONTAKTBAR = "ikke_kontaktbar"
KONTAKTRESULTATER = {
    KONTAKT_OPNAAET: "Kontakt opnået",
    INGEN_KONTAKT:   "Ingen kontakt",
    IKKE_KONTAKTBAR: "Ikke kontaktbar",
}

# SEKS udfald. `opgraderet` findes IKKE — verificeret mod CK_RetOut_outcome
# 2026-08-11. Hvad Specialisten kan registreres hul er altså reelt: en
# fornyelse med prisstigning kan
# kun registreres som `fornyet`, hvor specifikationen siger arr_after = før.
#
# Konsekvensen for validering: der må IKKE være regler på ARR pr. udfaldstype.
# En `fornyet` med højere arr_after end arr_before er lovlig og forekommer, og
# en validering der "rettede" den ville skjule stigningen i stedet for at måle
# den. Røgtestens tilfælde 1 fastholder netop det.
UDFALD = {
    "fornyet":         "Fornyet",
    "nedgraderet":     "Nedgraderet",
    "opsagt":          "Opsagt",
    "allerede_opsagt": "Allerede opsagt",
    "forskudt":        "Forskudt",
    "tilbud_sendt":    "Tilbud sendt",
}

ARR_KILDER = {
    ARR_KILDE_DELING:    "Skøn (lige deling)",
    ARR_KILDE_BEKRAEFTET: "Bekræftet hos kunden",
}

# Fristmodellens to poler. Begge er DATOER og ikke None, fordi reglen er total —
# der findes altid et svar. None ville læses som "ingen udsættelse", altså det
# stik modsatte af ALDRIG, og en opsagt kunde ville stå på opkaldslisten.
#
# date.max/min frem for årstal skrevet i hånden: så kan der ikke opstå en dato,
# der ved et uheld ligger uden for polerne.
ALDRIG = dt.date.max    # abonnementet kommer ikke tilbage
STRAKS = dt.date.min    # ingen udsættelse — vis rækken nu

def _paaskedag(aar: int) -> dt.date:
    """Påskedag i `aar`. Meeus/Jones/Butcher, gregoriansk.

    REGNET og ikke slået op i en tabel. Der findes ingen helligdagskalender i
    huset (søgt 2026-08-11 — det eneste er ugegrænse-aritmetik i modul_rotation
    og modul_perf), og en håndskrevet liste over påskedatoer udløber i tavshed:
    den dag listen slutter, holder helligdagene op med at virke, uden at noget
    fejler. Algoritmen gælder for alle årstal i den gregorianske kalender.

    Syv af de danske helligdage hænger på denne ene dato, så den er hele
    grundlaget for kalenderen nedenfor.
    """
    a = aar % 19
    b, c = divmod(aar, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    m = (32 + 2 * e + 2 * i - h - k) % 7
    n = (a + 11 * h + 22 * m) // 451
    maaned, dag = divmod(h + m - 7 * n + 114, 31)
    return dt.date(aar, maaned, dag + 1)


def danske_helligdage(aar: int) -> frozenset:
    """Dage i `aar` hvor ingen tager telefonen. Se HELLIGDAGE.

    STORE BEDEDAG ER MED TIL OG MED 2023. Den blev afskaffet som helligdag ved
    lov fra 2024, og en kalender der stadig holdt den lukket ville give en
    opfølgning et døgns forsinkelse hver forår. Grænsen står her frem for i en
    kommentar, fordi tilbageblik-kørsler (kalibrering.py) godt kan ramme 2023.

    JULEAFTENSDAG OG NYTÅRSAFTENSDAG er formelt ikke helligdage, men er lukkede
    i praksis. De er med, fordi funktionens spørgsmål er "tager nogen telefonen",
    ikke "hvad siger ferieloven".

    GRUNDLOVSDAG er bevidst IKKE med: den er en halv eller hel fridag afhængigt
    af arbejdsplads, og vi ved ikke hvad der gælder her. Skal den med, er det
    linjen `dt.date(aar, 6, 5)`.
    """
    paaske = _paaskedag(aar)
    dage = {
        dt.date(aar, 1, 1),                        # nytårsdag
        paaske - dt.timedelta(days=3),             # skærtorsdag
        paaske - dt.timedelta(days=2),             # langfredag
        paaske,                                    # påskedag
        paaske + dt.timedelta(days=1),             # 2. påskedag
        paaske + dt.timedelta(days=39),            # Kristi himmelfartsdag
        paaske + dt.timedelta(days=49),            # pinsedag
        paaske + dt.timedelta(days=50),            # 2. pinsedag
        dt.date(aar, 12, 24),                      # juleaftensdag
        dt.date(aar, 12, 25),                      # juledag
        dt.date(aar, 12, 26),                      # 2. juledag
        dt.date(aar, 12, 31),                      # nytårsaftensdag
    }
    if aar <= 2023:
        dage.add(paaske + dt.timedelta(days=26))   # store bededag
    return frozenset(dage)


# Dage hvor ingen tager telefonen, ud over lørdag og søndag. naeste_hverdag
# springer over det, der ligger her.
#
# ÅRSSPÆNDET er skrevet ud og ikke regnet fra `date.today()`. Et rullende vindue
# ville blive frosset ved procesopstart, og en server der har kørt siden nytår
# ville miste næste års helligdage uden at nogen kunne se det. Et fast spænd kan
# derimod læses: slutter det, holder helligdagene op med at virke — så det er
# sat langt nok ude, at det ikke sker i dette systems levetid.
HELLIGDAGE_FOERSTE_AAR, HELLIGDAGE_SIDSTE_AAR = 2020, 2040
HELLIGDAGE: frozenset = frozenset().union(
    *(danske_helligdage(a)
      for a in range(HELLIGDAGE_FOERSTE_AAR, HELLIGDAGE_SIDSTE_AAR + 1)))

# Fristmodellens frister i dage. Tallene står her frem for inde i regnestykkerne,
# så de kan læses uden at læse logikken.
FORNYET_DAGE_FOER = 45      # fornyelsesdato MINUS dette
FORNYET_UDEN_DATO = 180     # ingen fornyelsesdato kendt
IKKE_KONTAKTBAR_DAGE = 90

# db.py forbinder med tds_version="7.0", og TDS 7.0 kender ikke `date` og
# `datetime2` — de kom i 7.3. SQL Server sender dem derfor som STRENGE
# ('2026-08-14' og '2026-08-14 15:04:05.1234567'), mens det gamle `datetime`
# kommer tilbage som et rigtigt Python-objekt. Uden normalisering her sprang
# opfoelgninger på `str <= date`, og Dagens opkald kan ikke markere en overskredet
# opfølgning uden at kunne regne på datoen. Rettes ved kanten, én gang, i
# stedet for i hver enkelt kalder.
_DATO_FELTER = ("renewal_date", "expiry_date", "followup_date")
_TIDSPUNKT_FELTER = ("contacted_at", "created_at")


def _som_dato(vaerdi):
    """'2026-08-14' → date(2026, 8, 14). None og date'er slipper uændret igennem."""
    if isinstance(vaerdi, str):
        return dt.date.fromisoformat(vaerdi[:10])
    if isinstance(vaerdi, dt.datetime):
        return vaerdi.date()
    return vaerdi


def _som_tidspunkt(vaerdi):
    """datetime2(7) → datetime. Afskæres til 6 cifres brøkdel.

    fromisoformat kan ikke tage syv cifre. Afkortningen er bevidst og ikke
    afrunding: mikrosekunder på et opkaldstidspunkt er alligevel støj, og en
    afrunding kunne skubbe tidspunktet et sekund frem.
    """
    if isinstance(vaerdi, str):
        return dt.datetime.fromisoformat(vaerdi[:26])
    return vaerdi


# To vokabularer for det samme tal. `dbo.retention.org_id` er INT, og
# RetentionOutcomes følger den — men risikolaget bærer org_id som STRENG
# ('6779'), fordi det kommer fra ACV/Pipedrive-verdenen, hvor id'er er tekst.
#
# Konsekvensen er tavs og alvorlig: et opslag på (account, '6779', site) i en
# ordbog nøglet med (account, 6779, site) rammer ALDRIG. Ingen fejl, bare et
# tomt "seneste udfald" på hver kunde. Derfor ejer dette modul konverteringen:
# databasen får altid int, ordbogsnøgler er altid str.
def _db_org_id(vaerdi) -> int:
    return int(vaerdi)


def _noegle_org_id(vaerdi) -> str:
    return str(vaerdi)


def _normaliser(raekke: dict) -> dict:
    for felt in _DATO_FELTER:
        if felt in raekke:
            raekke[felt] = _som_dato(raekke[felt])
    for felt in _TIDSPUNKT_FELTER:
        if felt in raekke:
            raekke[felt] = _som_tidspunkt(raekke[felt])
    return raekke


def _er_tal(vaerdi) -> bool:
    """Er værdien et brugbart tal? Tom streng og None er IKKE — de betyder 'udfyldt ikke'."""
    if vaerdi is None or (isinstance(vaerdi, str) and not vaerdi.strip()):
        return False
    try:
        float(vaerdi)
        return True
    except (TypeError, ValueError):
        return False


def naeste_hverdag(dag: dt.date) -> dt.date:
    """Den første hverdag EFTER `dag`. Weekend og HELLIGDAGE springes over.

    En løkke og ikke et regnestykke, fordi antallet af dage der skal springes
    ikke er kendt på forhånd: fra en torsdag er det én dag, fra en fredag tre.
    `weekday()` giver 0 for mandag, så 5 og 6 er lørdag og søndag.
    """
    naeste = dag + dt.timedelta(days=1)
    while naeste.weekday() >= 5 or naeste in HELLIGDAGE:
        naeste += dt.timedelta(days=1)
    return naeste


def tilbage_paa_listen(raekke: dict) -> dt.date:
    """Hvornår abonnementet igen må stå som ny risiko. Fristmodellen.

    Ind: én række som db_seneste_udfald leverer den. Ud: ALTID en dato — ALDRIG
    når abonnementet ikke skal tilbage, STRAKS når der intet er at udsætte på.
    Kalderen har derfor én sammenligning og ingen særtilfælde:
    `tilbage_paa_listen(u) > i_dag` betyder "udelad".

    INGEN `i_dag`-PARAMETER, og det er med vilje. Alle frister regnes fra
    rækkens egen `created_at`, aldrig fra kaldstidspunktet: regnes de fra i
    dag, skubbes datoen længere ud ved hvert sideopslag, og kunden kommer
    aldrig tilbage — uden at noget fejler. Uden adgang til "i dag" kan
    funktionen ikke lave den fejl. Samme princip som De to tabellers frosne
    kurs og Regler og Guardrails regel 4: historikken må ikke ændre sig, fordi
    man ser på den igen.

    `contacted_at` ville være marginalt mere korrekt end `created_at` — "da
    opkaldet skete" mod "da det blev tastet ind" — men den ligger på
    RetentionConversations og koster et join. Forskellen er under et døgn på en
    frist der måles i måneder.

    RÆKKEFØLGEN AF TJEK er her for læserens skyld, ikke for rigtighedens — i
    modsætning til zones.bestem_zone, hvor den bærer den.
    CK_RetOut_outcome_kraever_kontakt er en biimplikation, så "kontakt opnået"
    og "har et udfald" følges altid, og de to grene kan ikke overlappe.

    UKENDTE VÆRDIER giver STRAKS og ikke ALDRIG. Fejlen skal pege mod at vise
    for meget frem for for lidt, samme valg som zones.bestem_zone's
    `har_aktiv_konto=True` og `har_zuora_kobling=True`. ALDRIG ville lade en
    kunde forsvinde lydløst; STRAKS lader den dukke op for tidligt, og det
    bliver set.
    """
    grundlag = raekke.get("created_at")
    # created_at er en datetime (se _TIDSPUNKT_FELTER), mens resten af regningen
    # er ren date. De to typer kan ikke sammenlignes. Ordnes ÉN gang her frem
    # for i hver enkelt gren.
    if isinstance(grundlag, dt.datetime):
        grundlag = grundlag.date()
    if grundlag is None:
        # Kan ikke opstå gennem registrer_samtale — kolonnen har en default —
        # men en række skrevet ad anden vej har ingen frist vi kan regne.
        logger.warning("tilbage_paa_listen: række uden created_at (outcome_id "
                       "%s), vises straks", raekke.get("outcome_id"))
        return STRAKS

    resultat = raekke.get("contact_result")
    if resultat != KONTAKT_OPNAAET:
        # Ingen kontakt opnået ⇒ outcome er NULL (biimplikationen), så udfaldets
        # regler kan ikke bruges her.
        if resultat == IKKE_KONTAKTBAR:
            return grundlag + dt.timedelta(days=IKKE_KONTAKTBAR_DAGE)
        if resultat != INGEN_KONTAKT:
            logger.warning("tilbage_paa_listen: ukendt kontaktresultat %r",
                           resultat)
        return naeste_hverdag(grundlag)

    udfald = raekke.get("outcome")
    if udfald in LUKKEDE_UDFALD:
        return ALDRIG
    if udfald in AABNE_UDFALD:
        # followup_date er påkrævet på et åbent udfald, håndhævet af
        # CK_RetOut_followup_paa_aabne — så `or STRAKS` er reelt uopnåelig.
        # Den står der alligevel, fordi alternativet er en TypeError på en
        # side, hvis eneste opgave er at vise, hvem der skal ringes til.
        return raekke.get("followup_date") or STRAKS
    if udfald in FORTSAT_KUNDE:
        fornyelse = raekke.get("renewal_date")
        if fornyelse:
            return fornyelse - dt.timedelta(days=FORNYET_DAGE_FOER)
        return grundlag + dt.timedelta(days=FORNYET_UDEN_DATO)

    logger.warning("tilbage_paa_listen: ukendt udfald %r, vises straks", udfald)
    return STRAKS


def valider_registrering(samtale: dict, udfald: list) -> list[str]:
    """Fejlbeskeder på dansk. Tom liste betyder: kan skrives.

    Databasens fem CHECK-constraints er den EGENTLIGE regel, og de bliver
    håndhævet uanset hvad der står her. Men en overtrådt constraint kommer
    tilbage som en pymssql-fejl på engelsk med et constraint-navn i — og først
    når `registrer_samtale` allerede er kaldt, dvs. efter opkaldet er slut.
    Specialisten kan ikke ringe igen og spørge om det, hun manglede at skrive.
    Derfor siges reglerne her, før noget sendes, i et sprog hun kan handle på.

    Funktionen er IKKE en erstatning for constraints og må ikke blive det:
    browseren kan omgås, og routeren kalder den her, netop fordi klientens
    validering ikke er en sikkerhedsgrænse.
    """
    fejl = []

    if not str(samtale.get("account") or "").strip():
        fejl.append("Samtalen mangler en konto.")
    try:
        _db_org_id(samtale.get("org_id"))
    except (TypeError, ValueError):
        fejl.append("Samtalen mangler et gyldigt organisations-id.")
    if not samtale.get("contacted_at"):
        fejl.append("Angiv hvornår samtalen fandt sted.")
    if samtale.get("channel") not in KANALER:
        fejl.append("Vælg en kanal: " + ", ".join(KANALER.values()) + ".")
    if not str(samtale.get("created_by") or "").strip():
        fejl.append("Samtalen mangler en registrerende bruger.")

    if not udfald:
        fejl.append("Registrér mindst ét udfald — en samtale uden udfald er en tom række.")

    sete = set()
    for u in udfald:
        navn = u.get("site") or INTET_SITE
        hvor = f"«{navn}»"

        resultat = u.get("contact_result")
        if resultat not in KONTAKTRESULTATER:
            fejl.append(f"{hvor}: vælg et kontaktresultat.")

        # Biimplikationen fra CK_RetOut_outcome_kraever_kontakt, begge veje.
        vejen_ud = u.get("outcome") or None
        if resultat == KONTAKT_OPNAAET:
            if not vejen_ud:
                fejl.append(f"{hvor}: kontakt opnået kræver et udfald.")
            elif vejen_ud not in UDFALD:
                fejl.append(f"{hvor}: ukendt udfald «{vejen_ud}».")
        elif vejen_ud:
            fejl.append(f"{hvor}: der kan ikke registreres et udfald, "
                        "når der ikke var kontakt.")

        if vejen_ud in AABNE_UDFALD and not u.get("followup_date"):
            fejl.append(f"{hvor}: «{UDFALD[vejen_ud]}» holder sagen åben og "
                        "kræver en opfølgningsdato.")

        kilde = u.get("arr_before_kilde") or None
        if kilde is not None and kilde not in ARR_KILDER:
            fejl.append(f"{hvor}: ukendt kilde til årsværdien før samtalen.")
        # Et beløb uden kilde er værdiløst bagefter: "kroner reddet" på
        # Målingsiden kan
        # ikke skelne et bekræftet tal fra den lige deling, og så arver
        # forudsigelsesraten en division, ingen kan se.
        if _er_tal(u.get("arr_before_dkk")) and kilde is None:
            fejl.append(f"{hvor}: angiv om årsværdien før samtalen er "
                        "bekræftet eller et skøn.")

        # arr_after_dkk beregnes af registrer_samtale KUN når både beløb og kurs
        # er sat, ellers gemmes NULL uden en lyd, og kronerne er tabt for
        # Målingsiden.
        # Derfor spærres den halve udfyldning her frem for at lade den passere.
        lokal, kurs = u.get("arr_after_local"), u.get("fx_rate")
        valuta = str(u.get("arr_after_currency") or "").strip()
        if _er_tal(lokal):
            if not valuta:
                fejl.append(f"{hvor}: årsværdi efter samtalen mangler en valuta.")
            elif len(valuta) != 3 or not valuta.isalpha():
                fejl.append(f"{hvor}: valutaen skal være en trebogstavskode, fx DKK.")
            if not _er_tal(kurs):
                fejl.append(f"{hvor}: årsværdi efter samtalen mangler en kurs — "
                            "uden den bliver beløbet ikke gemt i kroner.")
        elif _er_tal(kurs) or valuta:
            fejl.append(f"{hvor}: der er angivet valuta eller kurs, "
                        "men intet beløb efter samtalen.")

        # To udfald på det samme abonnement i én samtale. ROW_NUMBER i
        # db_seneste_udfald bryder uafgjort på outcome_id, så det ville ikke
        # være tvetydigt — men det ville være noget, ingen mente at skrive.
        if navn in sete:
            fejl.append(f"{hvor}: det samme abonnement er registreret "
                        "to gange på den samme samtale.")
        sete.add(navn)

    return fejl


def registrer_samtale(samtale: dict, udfald: list) -> int | None:
    """Skriv én samtale og de udfald den gav. Returnerer conversation_id.

    `samtale` skal have account, org_id, contacted_at, channel, created_by og
    valgfrit summary. Hvert element i `udfald` skal have site og
    contact_result, og derudover de felter De to tabeller tillader.

    Returnerer None hvis noget gik galt — og rulles der tilbage, er INTET
    skrevet. En delvist registreret samtale ville være værre end ingen:
    specialisten ville tro udfaldet var gemt.
    """
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()

        # OUTPUT INSERTED frem for SCOPE_IDENTITY(): id'et kommer tilbage fra
        # selve indsættelsen, så der ikke er et vindue mellem skriv og opslag,
        # og så er der ikke tvivl om hvilket scope tælleren blev læst i.
        cur.execute(
            """INSERT INTO dbo.RetentionConversations
                   (account, org_id, contacted_at, channel, summary, created_by)
               OUTPUT INSERTED.conversation_id
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (samtale["account"], _db_org_id(samtale["org_id"]),
             samtale["contacted_at"], samtale["channel"],
             samtale.get("summary"), samtale["created_by"]),
        )
        conversation_id = cur.fetchone()[0]

        for u in udfald:
            # arr_after_dkk beregnes HER og gemmes som tal. De to tabeller: kursen
            # fryses, ellers ændrer historiske "kroner reddet" sig hver gang
            # valutaen bevæger sig. Derfor ikke en computed column.
            lokal, kurs = u.get("arr_after_local"), u.get("fx_rate")
            arr_after_dkk = None
            if lokal is not None and kurs is not None:
                arr_after_dkk = round(float(lokal) * float(kurs), 2)

            cur.execute(
                """INSERT INTO dbo.RetentionOutcomes
                       (account, org_id, site, conversation_id,
                        contact_result, outcome,
                        arr_before_dkk, arr_before_kilde,
                        arr_after_local, arr_after_currency,
                        fx_rate, arr_after_dkk,
                        renewal_date, expiry_date, followup_date,
                        note, created_by)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                           %s, %s, %s, %s, %s, %s)""",
                (samtale["account"], _db_org_id(samtale["org_id"]),
                 u.get("site") or INTET_SITE, conversation_id,
                 u["contact_result"], u.get("outcome"),
                 u.get("arr_before_dkk"), u.get("arr_before_kilde"),
                 lokal, u.get("arr_after_currency"),
                 kurs, arr_after_dkk,
                 u.get("renewal_date"), u.get("expiry_date"),
                 u.get("followup_date"), u.get("note"), samtale["created_by"]),
            )

        conn.commit()
        return conversation_id
    except Exception:
        # Rul eksplicit tilbage. Uden det ville en fejl efter den første
        # indsættelse efterlade en samtale uden udfald, og pymssql lukker ikke
        # nødvendigvis forbindelsen med det samme.
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                logger.exception("rollback fejlede efter registrer_samtale")
        logger.exception("registrer_samtale fejlede")
        return None
    finally:
        if conn is not None:
            conn.close()


def gem_pipedrive_aktivitet(conversation_id: int, aktivitet_id) -> bool:
    """Skriv Pipedrive-aktivitetens id tilbage på samtalen. KASTER ALDRIG.

    EN SELVSTÆNDIG UPDATE og ikke en del af registrer_samtale, fordi id'et
    ikke findes endnu når samtalen skrives: Pipedrive kaldes FØRST efter at
    databasen har committet (se pipedrive.py om hvorfor rækkefølgen ikke må
    byttes om). Der er derfor et vindue hvor rækken står uden id.

    False betyder at aktiviteten ER oprettet i Pipedrive, men at vi ikke fik
    skrevet nummeret ned. Det er en degraderet tilstand, ikke en fejl:
    registreringen er gemt, aktiviteten er i CRM'et, og det eneste tabte er
    sporet mellem dem. Præcis den tilstand modulet var i HELE tiden før
    kolonnen fandtes (tilføjet 2026-09-02), så den må ikke vælte noget.

    HVORFOR KOLONNEN FINDES: uden den kan man ikke gå den anden vej. Da en
    testregistrering skulle ryddes op 2026-09-02, fandtes koblingen mellem
    samtale 109 og aktivitet 91519 kun i audit-loggen og i en chatbesked.

    GRAINEN ER SAMTALEN og ikke udfaldet: der oprettes ÉN aktivitet pr.
    samtale, uanset hvor mange abonnementer den dækkede.
    """
    if not conversation_id or not aktivitet_id:
        return False
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """UPDATE dbo.RetentionConversations
                  SET pipedrive_activity_id = %s
                WHERE conversation_id = %s""",
            (int(aktivitet_id), int(conversation_id)),
        )
        conn.commit()
        return True
    except Exception:
        logger.exception("gem_pipedrive_aktivitet fejlede "
                         "(conversation_id=%s, aktivitet_id=%s)",
                         conversation_id, aktivitet_id)
        return False
    finally:
        if conn is not None:
            conn.close()


def db_seneste_udfald() -> dict:
    """Seneste udfald pr. abonnement: {(account, org_id, site): række}.

    Det er opslaget Dagens opkald hviler på — et abonnement ryddes af listen af sit
    seneste udfald, ikke af data (Fristmodellen). Hentes ufiltreret og filtreres i
    Python, fordi prioriteringslisten alligevel har alle abonnementer i hånden.

    NØGLENS org_id ER EN STRENG, også selv om kolonnen er INT: risikolaget
    bærer org_id som tekst, og et opslag med den forkerte type rammer aldrig
    uden at fejle. Se `_noegle_org_id`.

    Dato- og tidsfelter kommer ud som rigtige `date`/`datetime` — se
    `_normaliser`. Kalderen skal ikke parse noget.

    ROW_NUMBER er korrekt HER, i modsætning til `PipeDrive_ACV`-opslagene hvor
    RANK er det rigtige: der kan to rækker have samme `updated_at` og dermed
    være lige gyldige, mens `outcome_id` er unik og altid bryder uafgjort.
    """
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(
            """WITH rangeret AS (
                   SELECT o.*,
                          ROW_NUMBER() OVER (
                              PARTITION BY o.account, o.org_id, o.site
                              ORDER BY o.created_at DESC, o.outcome_id DESC
                          ) AS rn
                   FROM dbo.RetentionOutcomes o
               )
               SELECT * FROM rangeret WHERE rn = 1;"""
        )
        rows = cur.fetchall()
        conn.close()
        # Nøglens org_id er STRENG, så opslag fra risikolaget rammer. Se
        # _noegle_org_id.
        return {(r["account"], _noegle_org_id(r["org_id"]), r["site"]):
                _normaliser(r) for r in rows}
    except Exception:
        logger.exception("db_seneste_udfald fejlede")
        return {}


def db_historik(account: str, org_id: int) -> list:
    """Alle samtaler for én kunde, nyeste først, hver med sine udfald.

    Kundeside: "Tidligere udfald og samtaler, nyeste først". Grupperet på
    SAMTALEN og ikke på udfaldet, fordi ét opkald kan have dækket fem
    abonnementer — fem løsrevne rækker ville læses som fem opkald.

    Nøglen er kunden `(account, org_id)` og ikke abonnementet: siden viser hele
    kundens historik, også udfald på sites hun ikke længere har. Et opsagt
    abonnement er netop det, man har brug for at kende før man ringer.

    Returnerer en liste af samtaler med `udfald` som liste. Tom liste hvis der
    intet er — og tom liste ved FEJL, hvilket er en bevidst svaghed: siden må
    ikke gå ned, fordi historikken ikke kan hentes, men en tom historik ser ud
    som "vi har aldrig talt med dem". Derfor logges fejlen.
    """
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(
            """SELECT c.conversation_id, c.contacted_at, c.channel, c.summary,
                      c.created_by, c.created_at, c.pipedrive_activity_id,
                      o.outcome_id, o.site, o.contact_result, o.outcome,
                      o.arr_before_dkk, o.arr_before_kilde,
                      o.arr_after_dkk, o.arr_after_local,
                      o.arr_after_currency, o.fx_rate,
                      o.renewal_date, o.expiry_date, o.followup_date, o.note
               FROM dbo.RetentionConversations c
               LEFT JOIN dbo.RetentionOutcomes o
                      ON o.conversation_id = c.conversation_id
               WHERE c.account = %s AND c.org_id = %s
               ORDER BY c.contacted_at DESC, c.conversation_id DESC,
                        o.outcome_id ASC;""",
            (account, _db_org_id(org_id)),
        )
        rows = cur.fetchall()
        conn.close()
    except Exception:
        logger.exception("db_historik fejlede (account=%s, org_id=%s)", account, org_id)
        return []

    # LEFT JOIN: en samtale uden udfald kan ikke opstå gennem
    # registrer_samtale(), men kan gennem en manuel indsættelse. Den skal vises
    # som en samtale uden udfald, ikke skjules.
    samtaler: dict = {}
    for r in rows:
        cid = r["conversation_id"]
        s = samtaler.get(cid)
        if s is None:
            s = {"conversation_id": cid,
                 "contacted_at": _som_tidspunkt(r["contacted_at"]),
                 "channel": r["channel"], "summary": r["summary"],
                 "created_by": r["created_by"],
                 "created_at": _som_tidspunkt(r["created_at"]),
                 # None for alt der er registreret før 2026-09-02, og for
                 # samtaler hvor Pipedrive-kaldet ikke gik igennem.
                 "pipedrive_activity_id": r["pipedrive_activity_id"],
                 "udfald": []}
            samtaler[cid] = s
        if r["outcome_id"] is not None:
            s["udfald"].append(_normaliser({
                k: r[k] for k in
                ("outcome_id", "site", "contact_result", "outcome",
                 "arr_before_dkk", "arr_before_kilde",
                 "arr_after_dkk", "arr_after_local",
                 "arr_after_currency", "fx_rate",
                 "renewal_date", "expiry_date", "followup_date", "note")
            }))
    # dict bevarer indsættelsesrækkefølgen, og queryen er allerede sorteret
    # nyeste først — derfor ingen ny sortering her.
    return list(samtaler.values())


def opfoelgninger(seneste: dict, til_og_med) -> list:
    """Åbne sager med opfølgning senest `til_og_med`. Dagens opkald, liste 1.

    INTET `db_`-PRÆFIKS, og det er ikke kosmetik: funktionen rører ikke
    databasen længere, og præfikset betyder konsekvent det modsatte i dette
    modul (db_seneste_udfald, db_historik). `seneste` er db_seneste_udfald()'s
    ordbog, og den kommer UDEFRA.

    Hvorfor den kommer udefra: prioriteringen har brug for præcis det samme
    opslag tre gange — til denne liste, til Arbejdsgangens loft (som tæller
    ALLE åbne sager, ikke kun dagens), og til Prioriteringsmodellens filter 3
    og 4. Tre kald mod samme tabel er tre chancer for, at de tre tal bliver
    uenige om noget, der ændrer sig, mens siden bygges.

    Bivirkningen er, at den nu kan bevises på håndlavede rækker uden
    forbindelse — det var den ikke før.

    Prioriteringsmodellen: listens længde er 10 kunder MINUS dagens
    opfølgninger, så det her tal styrer hvor mange nye navne specialisten
    får. `<=` og ikke `=`, fordi
    en opfølgning der blev overset i går ikke må forsvinde i morgen.

    Kun det SENESTE udfald pr. abonnement tæller, og det er `seneste`s eget
    grain. Et abonnement der først blev 'tilbud_sendt' og siden 'fornyet' har
    stadig den gamle followup_date liggende på den gamle række, og den skal
    ikke kalde nogen til handling.

    `til_og_med` skal være en `date`. `followup_date` er en `date` efter
    _normaliser, og en `datetime` her rejser TypeError — første gang der FINDES
    en opfølgning, altså i produktion og aldrig under udvikling.
    """
    return [r for r in seneste.values()
            if r["outcome"] in AABNE_UDFALD
            and r["followup_date"] is not None
            and r["followup_date"] <= til_og_med]


def db_maanedens_udfald(maaned: str) -> list:
    """Alle udfald registreret i `maaned` ('2026-08'), med samtalens dato.

    Grundlaget for Målingsidens tre tal. Hentes UAFGRÆNSET — afgrænsningen på ejer
    og team sker i Python, se prioritering.maanedens_kpier. Grunden er den
    samme som i queries.abonnementer_med_ejer: ACV's ejer-opslag er en query
    for sig med sine egne fælder, og den hører ikke i skrivesiden. Rækkerne er
    desuden få — en håndfuld om dagen — så der er intet at spare ved at
    filtrere i SQL.

    MÅNEDEN GÅR PÅ `contacted_at`, ikke på `created_at`. Et opkald taget 31.
    juli og tastet ind 1. august hører i juli. Det er en ANDEN beslutning end i
    tilbage_paa_listen, hvor `created_at` er den rigtige: en udsættelse har brug
    for en stabil egenskab ved rækken, mens en månedsopgørelse har brug for
    datoen, hvor forretningshændelsen skete. Joinet til RetentionConversations
    skal der være alligevel for at kunne tælle samtaler.

    `conversation_id` bæres med, fordi Målingsidens "antal samtaler" er DISTINKTE
    samtaler og ikke udfald: én samtale kan dække syv abonnementer, og det er
    stadig ét opkald.
    """
    aar, md = int(maaned[:4]), int(maaned[5:7])
    fra = dt.date(aar, md, 1)
    # Første i næste måned. December ruller til januar året efter.
    til = dt.date(aar + (md == 12), md % 12 + 1, 1)
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(
            """SELECT o.account, o.org_id, o.conversation_id, o.outcome,
                      o.contact_result, o.arr_before_dkk, o.arr_before_kilde,
                      o.arr_after_dkk, c.contacted_at
               FROM dbo.RetentionOutcomes o
               JOIN dbo.RetentionConversations c
                    ON c.conversation_id = o.conversation_id
               WHERE c.contacted_at >= %s AND c.contacted_at < %s;""",
            (fra, til),
        )
        rows = cur.fetchall()
        conn.close()
        return [_normaliser(r) for r in rows]
    except Exception:
        logger.exception("db_maanedens_udfald(%s) fejlede", maaned)
        return []


def db_alle_udfald() -> list:
    """ALLE udfald med samtalens dato. Grundlaget for Performance-fanen.

    HVORFOR IKKE db_maanedens_udfald I EN LOEKKE: Performance-fanen viser hver
    maaned der findes en registrering i, og et kald pr. maaned er et
    databaseopslag pr. maaned. Den anden grund vejer tungere: den funktion
    returnerer ikke `site`, og traefsikkerheden SKAL noegles paa abonnementet
    (account, org_id, site) for at kunne slaas op mod en opsigelse.

    UFILTRERET, samme valg som db_seneste_udfald: afgraensningen paa team sker
    i Python, fordi ACV's ejer-opslag er en query for sig med sine egne
    faelder, og den hoerer ikke i skrivesiden. Raekkerne er faa -- en haandfuld
    om dagen -- saa der er intet at spare ved at filtrere i SQL.

    MAANEDEN GAAR PAA `contacted_at`, ikke `created_at`. Samme beslutning og
    samme begrundelse som db_maanedens_udfald: et opkald taget 31. juli og
    tastet ind 1. august hoerer i juli. Kalderen laeser maaneden af
    `contacted_at` selv, saa reglen kun findes eet sted.

    `conversation_id` baeres med, fordi "antal samtaler" er DISTINKTE samtaler
    og ikke udfald: een samtale kan daekke syv abonnementer og er stadig eet
    opkald.

    Tom liste ved fejl, og det er en bevidst svaghed af samme slags som
    db_historik's: fanen maa ikke gaa ned, fordi tallene ikke kan hentes. Men
    en tom liste ser ud som "der er aldrig registreret noget", saa panelet skal
    skelne de to paa `meta` -- derfor logges fejlen her.
    """
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(
            """SELECT o.account, o.org_id, o.site, o.conversation_id,
                      o.outcome, o.contact_result,
                      o.arr_before_dkk, o.arr_before_kilde, o.arr_after_dkk,
                      o.followup_date, c.contacted_at
               FROM dbo.RetentionOutcomes o
               JOIN dbo.RetentionConversations c
                    ON c.conversation_id = o.conversation_id
               ORDER BY c.contacted_at, o.outcome_id;"""
        )
        rows = cur.fetchall()
        conn.close()
        return [_normaliser(r) for r in rows]
    except Exception:
        logger.exception("db_alle_udfald fejlede")
        return []

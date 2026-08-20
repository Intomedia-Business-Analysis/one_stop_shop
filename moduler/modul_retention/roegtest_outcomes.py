"""Røgtest af skrivesiden (outcomes.py) mod den RIGTIGE database.

    .venv/Scripts/python.exe moduler/modul_retention/roegtest_outcomes.py

Hedder ikke `test_*.py` med vilje: `tests/conftest.py` peger DB_SERVER på et
hostnavn der ikke findes, fordi pytest-suiten skal køre uden database. Blev den
samlet op af pytest, ville hver enkelt assertion fejle på en connect-fejl.

SKRIVER ALDRIG TIL PRODUKTION. commit() er gjort uvirksom, og til sidst rulles
der eksplicit tilbage — samme disciplin som constraint-testen. Fordi outcomes.py
henter sin forbindelse gennem get_conn(), får skriv og læs den SAMME
forbindelse, så db_seneste_udfald() læser de ucommittede rækker inden for
transaktionen, præcis som den ville læse rigtige.

Hvad testen IKKE dækker: at commit() faktisk virker. Det kan kun vises ved at
skrive for alvor, og det er en beslutning for et menneske.

Identitets-tællerne løber ved hver kørsel. Det er ventet: SQL Server ruller
IDENTITY tilbage sammen med rækkerne, men nulstiller den ikke.
"""
import datetime as dt
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
# db.py's load_dotenv() finder .env ved at gå opad fra cwd — uden dette virker
# scriptet kun hvis det køres fra repo-roden.
os.chdir(REPO_ROOT)

import db  # noqa: E402
from moduler.modul_retention import outcomes  # noqa: E402

# Et rigtigt abonnement fra dbo.retention. Behøver ikke være rigtigt for at
# tabellerne accepterer det (der er ingen fremmednøgle mod et VIEW), men et
# opdigtet ville skjule en typefejl på account/site-længderne.
ACCOUNT, ORG_ID = "monitor", 790
# Risikolaget bærer org_id som STRENG. Der skrives med int og slås op med str,
# netop for at bevise at normaliseringen i outcomes.py holder begge veje — et
# opslag med forkert type rammer aldrig og fejler ikke.
ORG_ID_STR = "790"
SITE_A, SITE_B = "Klimamonitor", "Byrummonitor"
BRUGER = "roegtest@intomedia.dk"
I_DAG = dt.date(2026, 8, 10)

_fejl = []


def tjek(navn, betingelse, detalje=""):
    print(("  OK   " if betingelse else "  FEJL ") + navn + ("  " + detalje if detalje else ""))
    if not betingelse:
        _fejl.append(navn)


class NoCommitConn:
    """Forbindelse der nægter at committe og nægter at lukke sig selv.

    outcomes.py kalder commit() og close() på hver forbindelse den låner.
    Begge skal være uvirksomme her: commit ville lægge testrækkerne i
    produktion, og close ville aflevere forbindelsen til poolen midt i testen
    — og PooledConnection.close() ruller tilbage ved aflevering, så de
    efterfølgende opslag ville se en tom tabel.
    """

    def __init__(self, raw):
        self._raw = raw
        self.commits = 0

    def __getattr__(self, name):
        return getattr(self._raw, name)

    def commit(self):
        self.commits += 1  # tælles, så vi kan vise at koden FORSØGTE at committe

    def close(self):
        pass


def main() -> int:
    raw = db.get_conn()
    delt = NoCommitConn(raw)
    outcomes.get_conn = lambda: delt

    def antal(tabel):
        cur = raw.cursor()
        cur.execute("SELECT COUNT(*) FROM dbo." + tabel)
        return cur.fetchone()[0]

    try:
        print("--- udgangspunkt ---")
        tjek("begge tabeller tomme før testen",
             (antal("RetentionConversations"), antal("RetentionOutcomes")) == (0, 0))

        print("--- 1: to abonnementer på én samtale ---")
        cid = outcomes.registrer_samtale(
            {"account": ACCOUNT, "org_id": ORG_ID,
             "contacted_at": dt.datetime(2026, 8, 10, 10, 30),
             "channel": "telefon", "summary": "Talte om fornyelse",
             "created_by": BRUGER},
            [
                # Fornyet MED prisstigning: 60.000 SEK * 0,72 = 43.200 DKK mod
                # 40.000 før. Bevidst, fordi hullet i udfaldsmodellen betyder at en
                # stigning kun kan registreres som 'fornyet'. Testen fastholder
                # at koden ikke spærrer for det — den må ikke "rette" tallet.
                {"site": SITE_A, "contact_result": "kontakt_opnaaet",
                 "outcome": "fornyet", "arr_before_dkk": 40000,
                 "arr_before_kilde": outcomes.ARR_KILDE_BEKRAEFTET,
                 "arr_after_local": 60000, "arr_after_currency": "SEK",
                 "fx_rate": "0.72", "renewal_date": dt.date(2026, 9, 1)},
                # Åbent udfald: kræver followup_date, ellers brænder
                # CK_RetOut_followup_paa_aabne.
                {"site": SITE_B, "contact_result": "kontakt_opnaaet",
                 "outcome": "tilbud_sendt", "arr_before_dkk": 25000,
                 "arr_before_kilde": outcomes.ARR_KILDE_DELING,
                 "followup_date": dt.date(2026, 8, 14),
                 "note": "Sender tilbud mandag"},
            ],
        )
        tjek("returnerer et conversation_id", isinstance(cid, int) and cid > 0, "cid=" + repr(cid))
        tjek("én samtale, to udfald",
             (antal("RetentionConversations"), antal("RetentionOutcomes")) == (1, 2))
        tjek("commit blev forsøgt", delt.commits == 1, "commits=" + str(delt.commits))

        print("--- 2: arr_after_dkk beregnet og frosset ---")
        seneste = outcomes.db_seneste_udfald()
        a = seneste.get((ACCOUNT, ORG_ID_STR, SITE_A))
        b = seneste.get((ACCOUNT, ORG_ID_STR, SITE_B))
        tjek("opslag med org_id som STRENG rammer (som risikolaget gør)",
             a is not None)
        tjek("opslag med int rammer IKKE — nøglen er str",
             seneste.get((ACCOUNT, ORG_ID, SITE_A)) is None)
        if a:
            tjek("arr_after_dkk = 60000 * 0,72 = 43200",
                 float(a["arr_after_dkk"]) == 43200.0, "faktisk=" + str(a["arr_after_dkk"]))
            tjek("fx_rate gemt uændret", float(a["fx_rate"]) == 0.72)
            tjek("conversation_id peger tilbage", a["conversation_id"] == cid)
            # Uden denne kolonne kan et bekræftet beløb ikke skelnes fra den
            # lige deling, når Målingsidens forudsigelsesrate skal beregnes.
            tjek("arr_before_kilde gemt som bekraeftet",
                 a["arr_before_kilde"] == outcomes.ARR_KILDE_BEKRAEFTET,
                 repr(a["arr_before_kilde"]))
        if b:
            tjek("arr_before_kilde gemt som lige_deling",
                 b["arr_before_kilde"] == outcomes.ARR_KILDE_DELING,
                 repr(b["arr_before_kilde"]))

        print("--- 3: datoer er datoer, ikke strenge ---")
        # TDS 7.0 kender ikke date/datetime2 og sender dem som tekst. Uden
        # normaliseringen i outcomes.py sprang opfoelgninger på
        # 'str <= date', og Dagens opkald kunne ikke afgøre om en aftale
        # var overskredet.
        if a:
            tjek("renewal_date er en date", type(a["renewal_date"]) is dt.date,
                 repr(a["renewal_date"]))
            tjek("created_at er en datetime", isinstance(a["created_at"], dt.datetime),
                 repr(a["created_at"]))
            tjek("tom dato er stadig None", a["expiry_date"] is None)
        if b:
            tjek("followup_date kan regnes på", (b["followup_date"] - I_DAG).days == 4,
                 repr(b["followup_date"]))

        print("--- 4: opfølgninger ---")
        # `seneste` er hentet ovenfor. Opfølgningerne læses nu af den SAMME
        # ordbog i stedet for at hente sin egen — det er hele pointen med at
        # opfoelgninger() tager den ind: ét opslag, ingen utakt.
        tjek("followup 14/8 kalder ikke i dag",
             len(outcomes.opfoelgninger(seneste, I_DAG)) == 0)
        frem = outcomes.opfoelgninger(seneste, dt.date(2026, 8, 14))
        tjek("den dukker op på datoen", len(frem) == 1 and frem[0]["site"] == SITE_B)

        print("--- 5: NULL site bliver sentinel ---")
        # dbo.retention.sites er NULL for marketwires rækker. En nøgle med NULL
        # kan aldrig slås op igen, fordi NULL = NULL er ukendt, ikke sandt.
        cid2 = outcomes.registrer_samtale(
            {"account": "marketwire", "org_id": 1,
             "contacted_at": dt.datetime(2026, 8, 10, 11, 0),
             "channel": "mail", "created_by": BRUGER},
            [{"site": None, "contact_result": "ingen_kontakt"}],
        )
        tjek("samtale uden site accepteres", isinstance(cid2, int))
        seneste = outcomes.db_seneste_udfald()
        noegle = ("marketwire", "1", outcomes.INTET_SITE)
        tjek("nøglen er sentinel, ikke NULL", noegle in seneste)
        tjek("outcome er NULL når der ikke var kontakt",
             seneste.get(noegle, {}).get("outcome") is None)

        print("--- 6: seneste udfald vinder og lukker den gamle aftale ---")
        # SITE_B står som 'tilbud_sendt' med followup 14/8. Fornyes den, må den
        # gamle followup_date IKKE længere kalde nogen til handling. Det er hele
        # grunden til at opfoelgninger filtrerer på SENESTE udfald og ikke
        # bare på alle rækker med en dato.
        outcomes.registrer_samtale(
            {"account": ACCOUNT, "org_id": ORG_ID,
             "contacted_at": dt.datetime(2026, 8, 10, 15, 0),
             "channel": "telefon", "created_by": BRUGER},
            [{"site": SITE_B, "contact_result": "kontakt_opnaaet", "outcome": "fornyet",
              "arr_before_dkk": 25000, "arr_after_local": 25000,
              "arr_after_currency": "DKK", "fx_rate": "1.0"}],
        )
        seneste = outcomes.db_seneste_udfald()
        tjek("seneste udfald for SITE_B er nu fornyet",
             seneste.get((ACCOUNT, ORG_ID_STR, SITE_B), {}).get("outcome") == "fornyet")
        tjek("fire udfald i alt (2 + sentinel + nyt), intet opdateret",
             antal("RetentionOutcomes") == 4, "faktisk=" + str(antal("RetentionOutcomes")))
        # `seneste` er genhentet lige ovenfor, EFTER fornyelsen. Det er dét, der
        # gør kontrollen skarp: filteret læser den friske ordbog, ikke en gammel.
        tjek("den gamle followup 14/8 kalder ikke længere",
             len(outcomes.opfoelgninger(seneste, dt.date(2026, 8, 14))) == 0)

        print("--- 6b: historik grupperet paa samtalen ---")
        # Kundeside. Kunden har nu to samtaler: den foerste med TO udfald, den
        # anden med ET. Grupperingen er hele pointen — fem loesrevne raekker
        # ville laeses som fem opkald.
        # Str ind, samme resultat: db_historik konverterer selv til INT.
        hist = outcomes.db_historik(ACCOUNT, ORG_ID_STR)
        tjek("to samtaler for kunden", len(hist) == 2, "fandt " + str(len(hist)))
        if len(hist) == 2:
            tjek("nyeste foerst", hist[0]["contacted_at"] > hist[1]["contacted_at"],
                 repr(hist[0]["contacted_at"]))
            tjek("nyeste samtale har 1 udfald", len(hist[0]["udfald"]) == 1)
            tjek("aeldste samtale har 2 udfald", len(hist[1]["udfald"]) == 2)
            tjek("datoer normaliseret ogsaa her",
                 all(u["renewal_date"] is None or type(u["renewal_date"]) is dt.date
                     for s in hist for u in s["udfald"]))
            tjek("summary baaret med", hist[1]["summary"] == "Talte om fornyelse")
            tjek("historikken baerer arr_before_kilde med",
                 {u["arr_before_kilde"] for u in hist[1]["udfald"]}
                 == {outcomes.ARR_KILDE_BEKRAEFTET, outcomes.ARR_KILDE_DELING})
        tjek("anden kunde har egen historik",
             len(outcomes.db_historik("marketwire", 1)) == 1)
        tjek("ukendt kunde giver tom liste",
             outcomes.db_historik("findes-ikke", 999) == [])

        print("--- 7: atomicitet ---")
        # Andet udfald har et outcome der ikke findes ('opgraderet' — Hvad
        # Specialisten kan registreres hul). Samtalen og det FØRSTE udfald skal
        # forsvinde med det. Rollback'en inde i registrer_samtale rammer den
        # delte transaktion, så alt ovenstående ryger også. Det er forventet,
        # og er samtidig beviset på at rollback'en er ægte. Logger'en printer
        # en traceback her — den hører til testen.
        foer = antal("RetentionConversations")
        cid3 = outcomes.registrer_samtale(
            {"account": ACCOUNT, "org_id": ORG_ID,
             "contacted_at": dt.datetime(2026, 8, 10, 16, 0),
             "channel": "telefon", "created_by": BRUGER},
            [{"site": SITE_A, "contact_result": "kontakt_opnaaet",
              "outcome": "fornyet", "arr_before_dkk": 1},
             {"site": SITE_B, "contact_result": "kontakt_opnaaet",
              "outcome": "opgraderet"}],
        )
        tjek("returnerer None ved fejl", cid3 is None, "fik " + repr(cid3))
        tjek("intet efterladt: samtaler = 0", antal("RetentionConversations") == 0,
             "der var " + str(foer) + " før fejlen")
        tjek("intet efterladt: udfald = 0", antal("RetentionOutcomes") == 0)
    finally:
        # Uanset hvad: rul tilbage for alvor og aflever forbindelsen.
        try:
            raw.rollback()
        except Exception:
            pass
        raw.close()

    print("--- oprydning ---")
    frisk = db.get_conn()
    cur = frisk.cursor()
    cur.execute("SELECT (SELECT COUNT(*) FROM dbo.RetentionConversations),"
                "       (SELECT COUNT(*) FROM dbo.RetentionOutcomes)")
    c1, c2 = cur.fetchone()
    frisk.close()
    tjek("frisk forbindelse ser tomme tabeller", (c1, c2) == (0, 0), str((c1, c2)))

    print()
    if _fejl:
        print("FEJLEDE: " + ", ".join(_fejl))
        return 1
    print("ALT GRØNT")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Maintenance — hvornår blev data sidst loadet ind, og burde de have været det?

Spørgsmålet dashboardet besvarer er ikke "hvad står der i tabellen", men "kan
jeg stole på tallet, jeg kigger på lige nu". Det kræver to oplysninger, der
ligger hvert sit sted:

    hvornår SKULLE data have været der   → Task Scheduler-planen, som står i
                                            KILDER herunder
    hvornår KOM de faktisk               → dbo.HubDataLoads, som hvert script
                                            skriver en række til pr. kørsel
                                            (se dataloads.py)

Forskellen mellem de to er statussen. Et grønt felt betyder "kørslen er landet
inden for sit vindue", ikke "der står nogle rækker i tabellen".

Hvorfor planen står hårdkodet her
---------------------------------
Task Scheduler ligger på serveren, og appen kan ikke læse den uden at få
adgang til Windows' opgavebibliotek. Planen er derfor skrevet af én gang og
skal rettes HER, hvis nogen flytter en opgave i Task Scheduler. Det er
bevidst valgt frem for at lade være med at vise en forventning: uden en
forventet køretid kan dashboardet kun vise en alder, og en alder alene siger
ikke, om 14 timer er for meget. Kolonnen "Planlagt" i UI'et viser den
forventning, så en uoverensstemmelse med serveren er synlig i stedet for skjult.

Forsinkelsen
------------
`forsinkelse_min` er den tid, der må gå fra opgaven STARTER, til rækkerne er i
databasen: selve kørslen plus lidt luft. Først når den er gået, tæller en
manglende kørsel som forsinket. Sættes den for lavt, lyser dashboardet rødt
hver dag midt i en helt normal kørsel.

Fallback
--------
Indtil et script er opdateret til at skrive til HubDataLoads — og hvis loggen
skulle være utilgængelig — falder dashboardet tilbage på det bedste
tidsstempel, tabellen selv har (`fallback_sql`). Det er svagere, og UI'et siger
det: en fallback kan ikke skelne "kørte, fandt intet nyt" fra "kørte ikke".
Rækker, der bruger fallback, markeres med kilde='fallback'.
"""
import importlib
import logging
import re
from datetime import date, datetime, time, timedelta

logger = logging.getLogger(__name__)

# Fælles pooled DB-forbindelse — se db.py.
from db import get_conn  # noqa: E402

LOG_TABEL = "HubDataLoads"

# En 'running'-række, der er ældre end dette, er ikke en kørsel i gang — det er
# en kørsel, der døde uden at lukke sin række (serveren genstartede, processen
# blev dræbt). Seks timer er rigeligt for den længste af vores kørsler (fuld
# Pipedrive-sync) og kort nok til at en hængende kørsel opdages samme dag.
HAENGER_EFTER_TIMER = 6


# ---------------------------------------------------------------------------
# Katalog over datakilder
# ---------------------------------------------------------------------------
# plan:
#   tider        — faste klokkeslæt (HH:MM), dansk servertid
#   hvert_min    — gentagelsesinterval, sammen med gentag_fra/gentag_til
#   None         — ingen fast plan (manuelt eller eksternt loadet); så bruges
#                  max_alder_dage i stedet for en planlagt køretid
#
# fallback_tz:
#   'lokal'  — DATETIME skrevet med GETDATE() på serveren (dansk tid)
#   'utc'    — DATETIME2 skrevet med SYSUTCDATETIME()
#   'dato'   — kun en DATE; tolkes som midnat og siger noget om DATA, ikke om
#              hvornår de blev skrevet

KILDER = [
    {
        "id":            "pipedrive_sync",
        "titel":         "Pipedrive Deals",
        "tabel":         "PipedriveDeals",
        "kilde":         "pipedrive_sync",
        "repo":          "pipedrive_sync",
        "opgave":        "PipeDrive Sync + PipeDrive Frequent Sync",
        "plan_tekst":    "Hvert 15. minut 06:00–18:00, plus fuld sync 16:00",
        "plan":          {"tider": ["16:00"], "gentag_fra": "06:00",
                          "gentag_til": "18:00", "hvert_min": 15},
        # Den inkrementelle kørsel er hurtig; 25 minutter giver plads til en
        # enkelt overspringelse uden at melde fejl.
        "forsinkelse_min": 25,
        "fallback_sql":  "SELECT MAX(last_run_at) FROM dbo.PipedriveSyncState",
        "fallback_tekst": "MAX(last_run_at) i PipedriveSyncState",
        "fallback_tz":   "lokal",
        "data_sql":      "SELECT MAX(synced_at) FROM dbo.PipedriveDeals",
        "bruges_af":     ["Sælger- og Manager Dashboard", "Forecast",
                          "Medie Benchmark", "Deal Source", "Rotation",
                          "Klippekort", "Monthly Performance Report"],
    },
    {
        "id":            "acv_updater",
        "titel":         "Pipedrive ACV-snapshot",
        "tabel":         "PipeDrive_ACV",
        "kilde":         "acv_updater",
        "repo":          "ACV_updater_pipedrive",
        "opgave":        "PipeDrive ACV Updater",
        "plan_tekst":    "Dagligt kl. 16:00",
        "plan":          {"tider": ["16:00"]},
        "forsinkelse_min": 120,
        # updated_at er SYSUTCDATETIME() i ACV-updateren — derfor 'utc'.
        # ADVARSEL: kørslen skriver kun ÆNDRINGS-rækker, så et døgn uden
        # ændringer flytter ikke tidsstemplet. Fallbacken kan altså se
        # forsinket ud, selv om alt er i orden — kørselsloggen er det rigtige
        # signal for netop denne kilde.
        "fallback_sql":  "SELECT MAX(updated_at) FROM dbo.PipeDrive_ACV",
        "fallback_tekst": "MAX(updated_at) i PipeDrive_ACV (kun ændrings-rækker)",
        "fallback_tz":   "utc",
        "data_sql":      "SELECT MAX(snapshot_date) FROM dbo.PipeDrive_ACV",
        "bruges_af":     ["Sælger Portefølje", "Retention", "Afdelingsleder Dashboard"],
    },
    {
        "id":            "currencycollector",
        "titel":         "Valutakurser",
        "tabel":         "valutakurser",
        "kilde":         "currencycollector",
        "repo":          "currencycollector",
        "opgave":        "Currency Collector",
        "plan_tekst":    "Dagligt kl. 16:45",
        "plan":          {"tider": ["16:45"]},
        "forsinkelse_min": 60,
        # Tabellen har INGEN tidsstempelkolonne — kun kursdatoen. Fallbacken
        # siger derfor kun noget om, hvor nye kurserne er, ikke om scriptet kørte.
        # Nationalbanken offentliggør ikke i weekender og på helligdage, så
        # kursdatoen står stille fredag til mandag helt lovligt: derfor er
        # kun_hverdage sat, så weekenden ikke melder fejl.
        "fallback_sql":  "SELECT MAX([date]) FROM dbo.valutakurser",
        "fallback_tekst": "MAX([date]) — kursdato, ikke loadtidspunkt",
        "fallback_tz":   "dato",
        "fallback_kun_hverdage": True,
        "data_sql":      None,
        "bruges_af":     ["value_dkk på alle Pipedrive-deals (via pipedrive_sync/fx.py)"],
    },
    {
        "id":            "programmatic",
        "titel":         "Programmatic-salg (FINANS)",
        "tabel":         "ProgrammaticSales",
        "kilde":         "programmatic_finans_sales",
        "repo":          "ProgrammaticFinansSales",
        "opgave":        "ProgrammaticFinansSales",
        "plan_tekst":    "Dagligt kl. 05:00",
        "plan":          {"tider": ["05:00"]},
        "forsinkelse_min": 90,
        # Kørslen henter ALTID tal for dagen før: save_rows() sorterer dags dato
        # og fremtidige datoer fra, fordi de tal ikke er komplette endnu. Så er
        # nyeste [Date] = i går det RIGTIGE billede efter en vellykket kørsel —
        # uden denne linje ville rækken stå forsinket hver eneste dag.
        "data_forsinkelse_dage": 1,
        # Som valutakurser: ingen tidsstempelkolonne. [Date] er salgsdatoen, ikke
        # loadtidspunktet — derfor siger denne fallback mindre end de andre, og
        # kørselsloggen er så meget desto vigtigere her.
        "fallback_sql":  "SELECT MAX([Date]) FROM dbo.ProgrammaticSales",
        "fallback_tekst": "MAX([Date]) — salgsdato (altid dagen før kørslen)",
        "fallback_tz":   "dato",
        # Nulkontrol: nyeste dato må ALDRIG stå med beløb 0. Scrapingen kan nå
        # igennem uden fejl og alligevel hente en tom rapport (Relevant Digital
        # har ikke lukket dagen endnu, tabelvisningen skiftede, sessionen udløb),
        # og så skrives et nul, der ser ud som en dag uden omsætning. Det er
        # usynligt i alle rapporter, der summerer — derfor står det her.
        "nulkontrol_sql": """SELECT TOP 1 [Date], SUM([Amount])
                             FROM dbo.ProgrammaticSales
                             GROUP BY [Date]
                             ORDER BY [Date] DESC""",
        "nulkontrol_handling": "Trigger en ny kørsel af ProgrammaticFinansSales.",
        "data_sql":      None,
        "bruges_af":     ["Monthly Performance Report", "Sælger Dashboard",
                          "Afdelingsleder Dashboard", "Rotation"],
    },
    {
        "id":            "zuora_retention",
        "titel":         "Retention-snapshot (Zuora)",
        "tabel":         "retention",
        "kilde":         "zuora_retention",
        "repo":          "—  (loades uden for vores scripts)",
        "opgave":        "Ingen scheduled task på vores server",
        "plan_tekst":    "Månedligt — én række pr. abonnement pr. måned",
        "plan":          None,
        # Snapshottet er månedligt. 40 dage giver plads til, at en måned lander
        # et par dage inde i den næste, uden at det melder fejl.
        "max_alder_dage": 40,
        "fallback_sql":  "SELECT MAX(FirstDayOfMonth) FROM dbo.retention",
        "fallback_tekst": "MAX(FirstDayOfMonth) — snapshottets måned",
        "fallback_tz":   "dato",
        "data_sql":      None,
        "bruges_af":     ["Opkald og risiko", "Operationel og Performance",
                          "Sælger Portefølje"],
    },
    {
        "id":            "zuora_acv_fil",
        "titel":         "Zuora ACV-snapshot (fil)",
        "tabel":         None,                      # ligger ikke i databasen
        "kilde":         "zuora_acv_fil",
        "repo":          "one_stop_shop/modul_portfolio_alignment",
        "opgave":        "Manuelt eksport fra Redshift via DataGrip",
        "plan_tekst":    "Ugentligt — CSV/XLSX lægges i Porteføljer-mappen",
        "plan":          None,
        # Ugentlig kadence; 10 dage giver plads til en ferieuge uden at melde
        # fejl med det samme.
        "max_alder_dage": 10,
        "fil":           True,
        "fallback_tekst": "Nyeste ACV_snapshot_DDMMYYYY-fil i snapshot-mappen",
        "bruges_af":     ["Portfolio Alignment"],
    },
    {
        "id":            "usage_forbrug",
        "titel":         "Retention forbrug (Snowplow)",
        "tabel":         None,                      # ligger ikke i databasen
        "kilde":         "usage_forbrug",
        "repo":          "one_stop_shop/modul_retention",
        "opgave":        "Manuelt eksport fra Redshift via DataGrip",
        "plan_tekst":    "Månedligt — usage_kunde_DDMMYYYY lægges i Retention-mappen",
        "plan":          None,
        # Månedlig kadence. Filens alder er ikke kosmetisk her: signalet er
        # "læste 0 gange i sidste HELE måned", så en forældet fil betyder, at
        # churn-risikoen regnes på en måned, der ikke længere er den seneste.
        # Se modul_retention/usage.py, hvor et tidligere dagsbaseret signal
        # blev fjernet netop fordi filens alder flyttede 77 % af kunderne til
        # en værre zone helt af sig selv.
        "max_alder_dage": 35,
        "fil":           True,
        "fallback_tekst": "Nyeste usage_kunde_DDMMYYYY-fil i Retention-mappen",
        "bruges_af":     ["Opkald og risiko", "Operationel og Performance"],
    },
    {
        "id":            "usage_kobling",
        "titel":         "Retention kontokobling",
        "tabel":         None,
        "kilde":         "usage_kobling",
        "repo":          "one_stop_shop/modul_retention",
        "opgave":        "Manuelt eksport fra Redshift via DataGrip",
        "plan_tekst":    "Månedligt — dm_kobling_DDMMYYYY, samme mappe som forbruget",
        "plan":          None,
        # Eksporteres sammen med forbrugsfilen, men står som sin egen række:
        # bliver kun den ene opdateret, kan forbrug ikke oversættes til kunder,
        # og det ville være usynligt i en fælles række.
        "max_alder_dage": 35,
        "fil":           True,
        "fallback_tekst": "Nyeste dm_kobling_DDMMYYYY-fil i Retention-mappen",
        "bruges_af":     ["Opkald og risiko", "Operationel og Performance"],
    },
    {
        "id":            "budget",
        "titel":         "Budget",
        "tabel":         "BudgetsIntoMedia",
        "kilde":         "budget_upload",
        "repo":          "one_stop_shop/modul_budget",
        "opgave":        "Uploades manuelt i hubben under Sales Operations → Budget",
        "plan_tekst":    "Manuelt — typisk ved budgetlægning og korrektioner",
        "plan":          None,
        "max_alder_dage": None,                     # ingen forventning = ingen alarm
        # Tabellen har ingen tidsstempelkolonne, så uploadtidspunktet kan KUN
        # komme fra kørselsloggen (modul_budget skriver til den). Fallbacken
        # viser i stedet, hvor langt budgettet rækker frem — også nyttigt, men
        # det er et andet spørgsmål.
        "fallback_sql":  "SELECT MAX(BudgetDate) FROM dbo.BudgetsIntoMedia",
        "fallback_tekst": "MAX(BudgetDate) — nyeste budgetperiode, ikke uploadtidspunkt",
        "fallback_tz":   "dato",
        "data_sql":      None,
        "bruges_af":     ["Alle KPI-dashboards", "Forecast", "Rotation",
                          "Monthly Performance Report"],
    },
    {
        "id":            "saelgerbudget",
        "titel":         "Sælgerbudget",
        "tabel":         "SalespersonBudget",
        "kilde":         "saelgerbudget_upload",
        "repo":          "one_stop_shop/modul_budget",
        "opgave":        "Uploades manuelt i hubben under Sales Operations → Budget",
        "plan_tekst":    "Manuelt — typisk ved budgetlægning og korrektioner",
        "plan":          None,
        "max_alder_dage": None,
        "fallback_sql":  "SELECT MAX(BudgetDate) FROM dbo.SalespersonBudget",
        "fallback_tekst": "MAX(BudgetDate) — nyeste budgetperiode, ikke uploadtidspunkt",
        "fallback_tz":   "dato",
        "data_sql":      None,
        "bruges_af":     ["Sælger Dashboard", "Manager Dashboard", "Rotation"],
    },
    {
        "id":            "forecast",
        "titel":         "Forecast",
        "tabel":         "HubForecasts",
        "kilde":         "forecast_upload",
        "repo":          "one_stop_shop/modul_forcast",
        "opgave":        "Udfyldes af sælgere og managers i hubben",
        "plan_tekst":    "Månedligt — brugerne gemmer selv",
        "plan":          None,
        "max_alder_dage": None,
        "fallback_sql":  "SELECT MAX(updated_at) FROM dbo.HubForecasts",
        "fallback_tekst": "MAX(updated_at) i HubForecasts",
        "fallback_tz":   "lokal",
        "data_sql":      None,
        "bruges_af":     ["Forecast", "Manager Dashboard", "Afdelingsleder Dashboard"],
    },
]


# ---------------------------------------------------------------------------
# Grupper
# ---------------------------------------------------------------------------
# Kilderne vises i hver sin tabel, fordi de tre slags ikke kan sammenlignes:
# en scheduled task måles mod et klokkeslæt, en fil mod en kadence, og en
# manuel upload mod ingenting. Blandes de i én tabel, kommer kolonnerne
# "Planlagt" og "Næste" til at stå tomme på halvdelen af rækkerne, og et
# tomt felt ligner en fejl.
#
# Gruppen UDLEDES af kilden i stedet for at stå som endnu et felt: har den en
# plan, er den en scheduled task; er den en fil, er den filbaseret; ellers er
# den manuel. Så kan katalog og gruppering ikke drive fra hinanden.

GRUPPER = [
    {"id": "task",
     "titel": "Scheduled tasks på serveren",
     "beskrivelse": "Kører af sig selv efter en plan i Windows Task Scheduler. "
                    "Status måles mod den plan."},
    {"id": "fil",
     "titel": "Filbaserede eksporter",
     "beskrivelse": "En fil lægges i en mappe på drevet — stierne står i .env. "
                    "Status måles mod den forventede kadence."},
    {"id": "manuel",
     "titel": "Manuelt og eksternt loadet",
     "beskrivelse": "Uploades i hubben eller loades af noget uden for vores "
                    "scripts. Vises til orientering; der er ingen plan at måle mod."},
]


def gruppe_for(kilde: dict) -> str:
    if kilde.get("plan"):
        return "task"
    if kilde.get("fil"):
        return "fil"
    return "manuel"


# ---------------------------------------------------------------------------
# Planen: hvornår burde den seneste kørsel have været i hus?
# ---------------------------------------------------------------------------

def _klokkeslet(s: str) -> time:
    timer, minutter = s.split(":")
    return time(int(timer), int(minutter))


def _tider_paa_dagen(plan: dict) -> list[time]:
    """Alle planlagte klokkeslæt på en dag, sorteret.

    Faste tider og gentagelsesvinduet lægges i et set, så en fast kørsel, der
    falder sammen med gentagelsen (16:00 ligger i 06:00–18:00-vinduet), ikke
    tælles to gange.
    """
    tider = {_klokkeslet(t) for t in plan.get("tider", [])}
    hvert = plan.get("hvert_min")
    if hvert:
        loeb = datetime.combine(date.min, _klokkeslet(plan["gentag_fra"]))
        slut = datetime.combine(date.min, _klokkeslet(plan["gentag_til"]))
        while loeb <= slut:
            tider.add(loeb.time())
            loeb += timedelta(minutes=hvert)
    return sorted(tider)


def _forfaldne_koersler(plan: dict, forsinkelse_min: int, nu: datetime,
                        antal: int = 2) -> list[datetime]:
    """De seneste `antal` planlagte kørsler, der burde være landet nu.

    Grænsen er `nu` minus forsinkelsen: en kørsel, der startede for ti minutter
    siden, er ikke forsinket — den er i gang.
    """
    graense = nu - timedelta(minutes=forsinkelse_min)
    tider = _tider_paa_dagen(plan)
    if not tider:
        return []
    ud: list[datetime] = []
    for dage_tilbage in range(0, 14):
        dag = (graense - timedelta(days=dage_tilbage)).date()
        for t in reversed(tider):
            kandidat = datetime.combine(dag, t)
            if kandidat <= graense:
                ud.append(kandidat)
                if len(ud) >= antal:
                    return ud
    return ud


def _naeste_koersel(plan: dict, nu: datetime) -> datetime | None:
    """Førstkommende planlagte kørsel — vises så man ved, hvornår det retter sig selv."""
    tider = _tider_paa_dagen(plan)
    if not tider:
        return None
    for dage_frem in range(0, 14):
        dag = (nu + timedelta(days=dage_frem)).date()
        for t in tider:
            kandidat = datetime.combine(dag, t)
            if kandidat > nu:
                return kandidat
    return None


# ---------------------------------------------------------------------------
# Kørselsloggen
# ---------------------------------------------------------------------------

def _log_findes(cur) -> bool:
    cur.execute("SELECT 1 FROM sys.tables WHERE name = %s", (LOG_TABEL,))
    return cur.fetchone() is not None


def _seneste_koersler(cur) -> dict:
    """{source: {seneste: række, seneste_ok: række}} — to opslag pr. kilde.

    Begge er nødvendige: den SENESTE kørsel fortæller, om det gik galt lige nu,
    mens den seneste OK-kørsel fortæller, hvor friske data reelt er. En kilde,
    der fejlede i eftermiddags, har stadig gyldige data fra i går, og det skal
    dashboardet kunne sige.
    """
    cur.execute(f"""
        WITH rangeret AS (
            SELECT source, target_table, status, started_at, finished_at,
                   rows_written, data_through, message, host,
                   ROW_NUMBER() OVER (PARTITION BY source
                                      ORDER BY started_at DESC, id DESC) AS rn_alle,
                   ROW_NUMBER() OVER (PARTITION BY source
                                      ORDER BY CASE WHEN status = 'ok' THEN 0 ELSE 1 END,
                                               started_at DESC, id DESC) AS rn_ok
            FROM dbo.{LOG_TABEL}
        )
        SELECT source, target_table, status, started_at, finished_at,
               rows_written, data_through, message, host, rn_alle, rn_ok
        FROM rangeret
        WHERE rn_alle = 1 OR (rn_ok = 1 AND status = 'ok')
    """)
    ud: dict = {}
    for r in cur.fetchall():
        (source, tabel, status, start, slut, raekker, data_through,
         besked, host, rn_alle, rn_ok) = r
        post = ud.setdefault(source, {"seneste": None, "seneste_ok": None})
        raekke = {
            "tabel": tabel, "status": status, "startet": start, "afsluttet": slut,
            "raekker": raekker, "data_through": data_through,
            "besked": besked, "host": host,
        }
        if rn_alle == 1:
            post["seneste"] = raekke
        if rn_ok == 1 and status == "ok":
            post["seneste_ok"] = raekke
    return ud


def _historik(cur, dage: int = 7) -> dict:
    """{source: [kørsler]} for de sidste `dage` — til den lille tidslinje pr. kilde."""
    cur.execute(f"""
        SELECT source, status, started_at, finished_at, rows_written, message
        FROM dbo.{LOG_TABEL}
        WHERE started_at >= DATEADD(day, -%s, GETDATE())
        ORDER BY started_at DESC
    """, (dage,))
    ud: dict = {}
    for source, status, start, slut, raekker, besked in cur.fetchall():
        # Højst 40 pr. kilde: den frekvente Pipedrive-sync alene laver ~340
        # kørsler på en uge, og tidslinjen viser kun de nyeste.
        liste = ud.setdefault(source, [])
        if len(liste) < 40:
            liste.append({"status": status, "startet": start, "afsluttet": slut,
                          "raekker": raekker, "besked": besked})
    return ud


# ---------------------------------------------------------------------------
# Fallback: det bedste tidsstempel tabellen selv har
# ---------------------------------------------------------------------------

def _skalar(cur, sql: str):
    """Kør en skalar-query og returnér værdien — None hvis tabellen mangler.

    En manglende tabel er ikke en fejl her: den er præcis det, dashboardet skal
    kunne vise, og en undtagelse ville tage resten af siden med sig.
    """
    try:
        cur.execute(sql)
        raekke = cur.fetchone()
        return raekke[0] if raekke else None
    except Exception as e:
        logger.warning("maintenance: fallback-query fejlede (%s): %s", sql, e)
        return None


def _raekke(cur, sql: str) -> tuple | None:
    """Som _skalar, men returnerer hele rækken. Til nulkontrollen, der skal
    bruge både dato og beløb."""
    try:
        cur.execute(sql)
        raekke = cur.fetchone()
        return tuple(raekke) if raekke else None
    except Exception as e:
        logger.warning("maintenance: nulkontrol fejlede (%s): %s", sql, e)
        return None


def _som_datetime(vaerdi, tz: str, nu: datetime) -> datetime | None:
    """Normalisér en fallback-værdi til dansk lokaltid.

    UTC-værdier flyttes med serverens aktuelle forskydning i stedet for en fast
    time. Danmark skifter sommertid, og en fast forskydning ville give en times
    fejl halvdelen af året — nok til at et vindue på 60 minutter meldte fejl.
    """
    if vaerdi is None:
        return None
    if isinstance(vaerdi, datetime):
        if tz == "utc":
            forskydning = datetime.now() - datetime.utcnow()
            return vaerdi + forskydning
        return vaerdi
    if isinstance(vaerdi, date):
        return datetime.combine(vaerdi, time(0, 0))
    if isinstance(vaerdi, str):
        # DB_DATE_AS_STRING=1 leverer DATE-kolonner som 'YYYY-MM-DD' (se db.py).
        try:
            return datetime.strptime(vaerdi[:10], "%Y-%m-%d")
        except ValueError:
            return None
    return None


# De filbaserede kilder: hvilket modul der ved, hvor filen ligger, og hvad den
# hedder. Stierne selv står i .env (ZUORA_SNAPSHOT_DIR, USAGE_SNAPSHOT_DIR) og
# gentages IKKE her — modulerne læser dem allerede, og to steder ville drive
# fra hinanden.
_FIL_FINDERE = {
    "zuora_acv_fil": ("moduler.modul_portfolio_alignment.queries",
                      "find_latest_snapshot", "get_snapshot_dir",
                      "ACV_snapshot"),
    "usage_forbrug": ("moduler.modul_retention.usage",
                      "find_latest_usage_file", "get_usage_dir",
                      "usage_kunde"),
    "usage_kobling": ("moduler.modul_retention.usage",
                      "find_latest_kobling_file", "get_usage_dir",
                      "dm_kobling"),
}

# Dato-suffikset DDMMYYYY er fælles for alle eksport-typerne. Mønsteret står
# her frem for at blive importeret, fordi de to moduler har hver sit navn for
# samme regel (SNAPSHOT_FILENAME_RE og _FILE_DATE_RE).
_FIL_DATO_RE = re.compile(r"_(\d{2})(\d{2})(\d{4})(?:$|\D)")


def _fil_status(kilde_id: str) -> dict:
    """Nyeste eksportfil for en filbaseret kilde: dato i filnavnet og mtime.

    Importen ligger inde i funktionen, fordi begge moduler trækker pandas med
    sig. Datastatus-siden skal kunne loade, også hvis den import fejler — det
    er netop en af de ting, den skal kunne fortælle om.
    """
    modulnavn, finder_navn, mappe_navn, praefiks = _FIL_FINDERE[kilde_id]
    try:
        modul = importlib.import_module(modulnavn)
        find = getattr(modul, finder_navn)
        hent_mappe = getattr(modul, mappe_navn)
    except Exception as e:
        return {"fejl": f"Kunne ikke læse {modulnavn}: {e}"}

    try:
        mappe = hent_mappe()
        sti = find()
    except Exception as e:
        return {"fejl": f"Kunne ikke læse eksport-mappen: {e}"}

    if sti is None:
        return {"mappe": str(mappe),
                "fejl": f"Ingen {praefiks}-fil fundet i {mappe}"}

    eksport_dato = None
    m = _FIL_DATO_RE.search(sti.stem)
    if m:
        dd, mm, yyyy = m.groups()
        try:
            eksport_dato = date(int(yyyy), int(mm), int(dd))
        except ValueError:
            eksport_dato = None
    return {
        "mappe":      str(mappe),
        "filnavn":    sti.name,
        "sti":        str(sti),
        "dato":       eksport_dato,
        "aendret":    datetime.fromtimestamp(sti.stat().st_mtime),
        "stoerrelse": sti.stat().st_size,
    }


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

STATUS_RANG = {"fejl": 0, "haenger": 1, "forsinket": 2, "ukendt": 3, "ok": 4, "info": 5}

PLAUSIBEL_MAX = 500   # et loft mellem normalen (3) og en genskrivning (3.782)
NUL_DAGE = 7          # vinduet: højest så mange kørsler kigges der på
MIN_KOERSLER = 5      # tilladelsen: færre kørsler end dette er for lidt til at dømme.


def _vurder_plausibilitet(koersler: list[dict]) -> dict | None:
    """koersler: vellykkede kørsler, nyeste først.
    Returnerer en status, hvis antallet er utroværdigt, ellers None."""
    if not koersler:
        return None

    i_dag = koersler[0]["raekker"]
    if i_dag is not None and i_dag > PLAUSIBEL_MAX:
        return {"status": "fejl",
                "forklaring": f"{i_dag} felter skrevet i én kørsel. "
                              f"Skrivehistorikken bliver sandsynligvis ikke læst."}

    seneste = [k["raekker"] for k in koersler[:NUL_DAGE]]
    if len(seneste) >= MIN_KOERSLER and all(r == 0 for r in seneste):
        return {"status": "forsinket",
                "forklaring": f"0 felter skrevet {len(seneste)} kørsler i træk."}

    return None


def _vurder(kilde: dict, seneste: dict | None, seneste_ok: dict | None,
            fallback: datetime | None, nu: datetime,
            nulkontrol: tuple | None = None,
            historik: list[dict] | None = None) -> dict:
    """Sammenhold det, der skete, med det der burde ske. Returnér status + forklaring.

    Rækkefølgen er bevidst: en kørsel, der HÆNGER eller FEJLEDE lige nu, vises
    som det, også selv om gårsdagens data stadig er inden for vinduet. Det er
    det, nogen skal handle på.
    """
    plan = kilde.get("plan")

    # 1. Hænger kørslen? En 'running'-række, der aldrig blev lukket.
    if seneste and seneste["status"] == "running":
        alder = nu - seneste["startet"]
        if alder > timedelta(hours=HAENGER_EFTER_TIMER):
            return {"status": "haenger",
                    "forklaring": f"Kørsel startet {_dk(seneste['startet'])} er "
                                  f"aldrig afsluttet ({_varighed(alder)} siden). "
                                  f"Processen er sandsynligvis død undervejs."}
        return {"status": "ok",
                "forklaring": f"Kører nu — startet {_dk(seneste['startet'])}."}

    # 2. Fejlede den seneste kørsel?
    if seneste and seneste["status"] == "error":
        forklaring = f"Seneste kørsel {_dk(seneste['startet'])} fejlede."
        if seneste_ok:
            forklaring += (f" Data er fra sidste vellykkede kørsel "
                           f"{_dk(seneste_ok['afsluttet'] or seneste_ok['startet'])}.")
        return {"status": "fejl", "forklaring": forklaring}

    # 3. Står nyeste dato med beløb 0? Se nulkontrol_sql i KILDER.
    #    Tjekket ligger efter fejl/hænger (dér er årsagen kendt og mere
    #    diagnostisk) men FØR friskheden: datoen er helt frisk, og rækken ville
    #    ellers stå grøn oven på et nul, ingen opdager.
    if nulkontrol:
        kontrol_dato, kontrol_vaerdi = nulkontrol
        if kontrol_dato is not None and not kontrol_vaerdi:
            handling = kilde.get("nulkontrol_handling", "")
            return {"status": "fejl",
                    "forklaring": (f"Nyeste dato {_dato(kontrol_dato)} står med "
                                   f"beløb 0. Kørslen nåede igennem, men hentede "
                                   f"ingen tal. {handling}").strip()}

    # 3b. er antallet plausibelt? kun for kilder med "plausibilitet": True
    # samme placering som nulkontrollen og samme grund: kørslen er frisk
    # og 'ok', og friskheden ville ellers melde grønt oven på et utroværdigt tal.
    if kilde.get("plausibilitet") and historik:
        ok_koersler = [k for k in historik if k["status"] == "ok"]
        plaus = _vurder_plausibilitet(ok_koersler)
        if plaus:
            return plaus

    # 4. Hvornår landede data sidst?
    if seneste_ok:
        landet = seneste_ok["afsluttet"] or seneste_ok["startet"]
        log_kilde = "log"
    else:
        landet = fallback
        log_kilde = "fallback"

    if landet is None:
        return {"status": "ukendt",
                "forklaring": "Ingen kørsler logget endnu, og tabellen har intet "
                              "tidsstempel at falde tilbage på."}

    # 5a. Kilder uden plan: kun en alder, og kun en alarm hvis der ER en grænse.
    if not plan:
        graense = kilde.get("max_alder_dage")
        alder = nu - landet
        if graense is None:
            return {"status": "info",
                    "forklaring": f"Sidst opdateret for {_varighed(alder)} siden. "
                                  f"Ingen fast kadence — ingen forventning at måle mod."}
        if alder > timedelta(days=graense):
            return {"status": "forsinket",
                    "forklaring": f"{_varighed(alder)} siden sidste opdatering — "
                                  f"forventet mindst hver {graense}. dag."}
        return {"status": "ok",
                "forklaring": f"Opdateret for {_varighed(alder)} siden "
                              f"(inden for {graense} dage)."}

    # 5b. Planlagte kilder: sammenlign med den seneste kørsel, der burde være i hus.
    forfaldne = _forfaldne_koersler(plan, kilde["forsinkelse_min"], nu, antal=2)
    if not forfaldne:
        return {"status": "ok", "forklaring": "Ingen planlagt kørsel er forfalden endnu."}

    # Fallback på en ren DATE kan aldrig nå et klokkeslæt samme dag — den står
    # på midnat. Derfor sammenlignes den på DATO-niveau, ikke på tidspunkt;
    # ellers ville en kilde med dato-fallback altid se forsinket ud.
    if log_kilde == "fallback" and kilde.get("fallback_tz") == "dato":
        return _vurder_datofallback(kilde, landet, forfaldne, nu)

    if landet >= forfaldne[0]:
        return {"status": "ok",
                "forklaring": f"Kørslen {_dk(forfaldne[0])} er i hus "
                              f"({_dk(landet)})."}
    if len(forfaldne) > 1 and landet >= forfaldne[1]:
        return {"status": "forsinket",
                "forklaring": f"Kørslen {_dk(forfaldne[0])} mangler — sidste data "
                              f"er fra {_dk(landet)}."}
    return {"status": "fejl",
            "forklaring": f"Mindst to kørsler mangler. Sidste data er fra "
                          f"{_dk(landet)} ({_varighed(nu - landet)} siden)."}


def _vurder_datofallback(kilde: dict, landet: datetime,
                         forfaldne: list[datetime], nu: datetime) -> dict:
    """Vurdér en kilde, hvor vi kun har en DATE at gå efter.

    Her måles i hele dage mod den dato, kørslen SKULLE have leveret — ikke mod
    kørslens egen dato. To ting flytter den:

    `data_forsinkelse_dage` — kilden leverer tal for N dage før kørslen.
    Programmatic-salget henter altid dagen før, så nyeste [Date] = i går ER det
    rigtige billede efter morgenens kørsel. Uden forskydningen ville rækken stå
    forsinket hver eneste dag.

    `fallback_kun_hverdage` — Nationalbanken offentliggør ikke i weekenden, så
    en kursdato fra fredag er fuldt korrekt søndag aften og må ikke melde fejl.
    """
    forventet_dato = forfaldne[0].date() - timedelta(
        days=kilde.get("data_forsinkelse_dage", 0))
    if kilde.get("fallback_kun_hverdage"):
        while forventet_dato.weekday() >= 5:           # 5=lørdag, 6=søndag
            forventet_dato -= timedelta(days=1)

    dages_efterslaeb = (forventet_dato - landet.date()).days
    if dages_efterslaeb <= 0:
        return {"status": "ok",
                "forklaring": f"Nyeste data er fra {landet.date().isoformat()} "
                              f"— som forventet."}
    if dages_efterslaeb == 1:
        return {"status": "forsinket",
                "forklaring": f"Nyeste data er fra {landet.date().isoformat()}, "
                              f"forventet {forventet_dato.isoformat()}. "
                              f"Uden kørselslog kan det også betyde, at kørslen "
                              f"gik fint, men intet nyt fandt."}
    return {"status": "fejl",
            "forklaring": f"Nyeste data er fra {landet.date().isoformat()} — "
                          f"{dages_efterslaeb} dage efter det forventede "
                          f"{forventet_dato.isoformat()}."}


# ---------------------------------------------------------------------------
# Formatering
# ---------------------------------------------------------------------------

_UGEDAGE = ["mandag", "tirsdag", "onsdag", "torsdag", "fredag", "lørdag", "søndag"]


def _dk(dt: datetime | None) -> str:
    """Tidspunkt som en kollega ville sige det: 'i dag 16:04', 'i går 16:02'."""
    if dt is None:
        return "—"
    nu = datetime.now()
    dage = (nu.date() - dt.date()).days
    klokken = dt.strftime("%H:%M")
    if dage == 0:
        return f"i dag {klokken}"
    if dage == 1:
        return f"i går {klokken}"
    if 2 <= dage <= 6:
        return f"{_UGEDAGE[dt.weekday()]} {klokken}"
    return dt.strftime("%d-%m-%Y %H:%M")


def _dato(v) -> str:
    """En DATE som 'YYYY-MM-DD', uanset om den kom som date eller streng.

    DB_DATE_AS_STRING=1 (standarden, se db.py) leverer DATE-kolonner som
    strenge, så begge former forekommer i praksis.
    """
    if v is None:
        return "—"
    if isinstance(v, datetime):
        return v.date().isoformat()
    if isinstance(v, date):
        return v.isoformat()
    return str(v)[:10]


def _varighed(delta: timedelta) -> str:
    sekunder = int(delta.total_seconds())
    if sekunder < 0:
        return "0 min."
    if sekunder < 3600:
        return f"{max(1, sekunder // 60)} min."
    if sekunder < 86400:
        timer = sekunder // 3600
        return f"{timer} time" + ("r" if timer != 1 else "")
    dage = sekunder // 86400
    return f"{dage} dag" + ("e" if dage != 1 else "")


# ---------------------------------------------------------------------------
# Indgangen
# ---------------------------------------------------------------------------

def db_maintenance_overblik() -> dict:
    """Én række pr. datakilde med status, sidste kørsel og forventet næste.

    Kaster ikke: en kilde, der ikke kan læses, får status 'ukendt' med
    fejlteksten som forklaring. Et dashboard, hvis formål er at vise, at noget
    er gået i stykker, må ikke selv gå ned, når noget går i stykker.
    """
    nu = datetime.now()
    log: dict = {}
    historik: dict = {}
    log_tilgaengelig = False
    fallbacks: dict = {}
    datadatoer: dict = {}
    nulkontroller: dict = {}
    fejl_besked = None

    try:
        conn = get_conn()
        try:
            cur = conn.cursor()
            log_tilgaengelig = _log_findes(cur)
            if log_tilgaengelig:
                log = _seneste_koersler(cur)
                historik = _historik(cur)
            for kilde in KILDER:
                if kilde.get("fallback_sql"):
                    fallbacks[kilde["id"]] = _som_datetime(
                        _skalar(cur, kilde["fallback_sql"]),
                        kilde.get("fallback_tz", "lokal"), nu)
                if kilde.get("data_sql"):
                    datadatoer[kilde["id"]] = _skalar(cur, kilde["data_sql"])
                if kilde.get("nulkontrol_sql"):
                    nulkontroller[kilde["id"]] = _raekke(cur, kilde["nulkontrol_sql"])
        finally:
            conn.close()
    except Exception as e:
        logger.exception("maintenance: kunne ikke læse databasen")
        fejl_besked = str(e)

    raekker = []
    for kilde in KILDER:
        post = log.get(kilde["kilde"], {})
        seneste = post.get("seneste")
        seneste_ok = post.get("seneste_ok")

        if kilde.get("fil"):
            fil = _fil_status(kilde["id"])
            fallback = None
            if fil.get("dato"):
                fallback = datetime.combine(fil["dato"], time(0, 0))
            elif fil.get("aendret"):
                fallback = fil["aendret"]
        else:
            fil = None
            fallback = fallbacks.get(kilde["id"])

        # En død database gør kun de DB-baserede kilder ubestemmelige.
        # Zuora-snapshottet ligger som en fil på drevet og kan stadig vurderes —
        # og netop når databasen er nede, er det rart at kunne se, at resten af
        # opsætningen er i orden.
        if fejl_besked and not kilde.get("fil"):
            vurdering = {"status": "ukendt",
                         "forklaring": f"Databasen kunne ikke læses: {fejl_besked}"}
        elif fil and fil.get("fejl") and not seneste_ok:
            # Mappen kan ikke læses, eller der ligger ingen snapshot-fil.
            # Den konkrete besked er langt mere brugbar end "ukendt": den siger
            # HVOR der blev kigget, og det er typisk dér, fejlen er.
            vurdering = {"status": "ukendt", "forklaring": fil["fejl"]}
        else:
            vurdering = _vurder(kilde, seneste, seneste_ok, fallback, nu,
                                nulkontroller.get(kilde["id"]), historik.get(kilde["kilde"]))

        landet = None
        if seneste_ok:
            landet = seneste_ok["afsluttet"] or seneste_ok["startet"]
            landet_kilde = "log"
        elif fallback:
            landet = fallback
            landet_kilde = "fallback"
        else:
            landet_kilde = None

        raekker.append({
            **{k: kilde[k] for k in ("id", "titel", "tabel", "opgave",
                                     "plan_tekst", "repo", "bruges_af")},
            "status":         vurdering["status"],
            "forklaring":     vurdering["forklaring"],
            "sidst_loadet":   landet,
            "sidst_loadet_tekst": _dk(landet),
            "alder_tekst":    _varighed(nu - landet) if landet else None,
            "kilde_til_tid":  landet_kilde,
            "fallback_tekst": kilde.get("fallback_tekst"),
            "naeste":         _naeste_koersel(kilde["plan"], nu) if kilde.get("plan") else None,
            "seneste":        seneste,
            "raekker_sidst":  (seneste_ok or {}).get("raekker"),
            "data_through":   (seneste_ok or {}).get("data_through") or datadatoer.get(kilde["id"]),
            "historik":       historik.get(kilde["kilde"], []),
            "fil":            fil,
            "gruppe":         gruppe_for(kilde),
            "nulkontrol":     nulkontroller.get(kilde["id"]),
        })

    raekker.sort(key=lambda r: (STATUS_RANG.get(r["status"], 9), r["titel"]))

    grupper = [
        {**g, "raekker": [r for r in raekker if r["gruppe"] == g["id"]]}
        for g in GRUPPER
    ]

    return {
        "nu":               nu,
        "raekker":          raekker,          # flad liste — bruges af JSON-kaldet
        "grupper":          [g for g in grupper if g["raekker"]],
        "log_tilgaengelig": log_tilgaengelig,
        "fejl":             fejl_besked,
        "antal": {
            "problem": sum(1 for r in raekker if r["status"] in ("fejl", "haenger")),
            "forsinket": sum(1 for r in raekker if r["status"] == "forsinket"),
            "ok":      sum(1 for r in raekker if r["status"] == "ok"),
            "ukendt":  sum(1 for r in raekker if r["status"] == "ukendt"),
        },
        # Sat af scripts og af hubbens egne uploads; vises i UI'et, så det er
        # tydeligt hvilke kilder der allerede har en rigtig kørselslog.
        "logger_der_skriver": sorted(log.keys()),
    }


def db_maintenance_json() -> dict:
    """Samme data, men JSON-venligt: datetimes bliver til ISO-strenge."""
    data = db_maintenance_overblik()

    def rens(v):
        if isinstance(v, datetime):
            return v.isoformat(timespec="seconds")
        if isinstance(v, date):
            return v.isoformat()
        if isinstance(v, dict):
            return {k: rens(x) for k, x in v.items()}
        if isinstance(v, list):
            return [rens(x) for x in v]
        return v

    return rens(data)

"""Medie Benchmark — sammenlign to eller flere medier fra deres egen launchdato.

Spørgsmålet dashboardet besvarer er ikke "hvor mange deals fik vi i juli", men
"hvordan klarede det nye medie sig i sine første X dage sammenlignet med, hvad
det etablerede medie gjorde i SINE første X dage". Derfor er tidsaksen relativ:
dag 0 er hvert medies egen startdato, ikke en fælles kalenderdato.

Konkret case den blev bygget til: DetailWatch DK (launch 8. marts 2022) mod
NordicDefenceWatch (live 11. august 2026) på pipeline 'Company Trial', målt på
[add_time] — altså hvornår deal'en blev OPRETTET.

Datagrundlag
------------
[dbo].[PipedriveDeals] i INTOMEDIA. Et medie identificeres via [sites], som er
en kommasepareret liste (en deal kan dække flere sites), ikke via [account].
Det er bevidst: NordicDefenceWatch sælges på tværs af Norden, så dets deals
ligger spredt over watch_medier, watch_se OG watch_no. En account-baseret
sammenligning ville dele mediet i tre.

En "serie" kan pege på flere sites ad gangen, fordi Pipedrive rummer stavevari-
anter af samme medie ("FødevareWatch DK" / "Fødevare Watch DK", "KForum" /
"Kforum DK"). Uden det ville et medie tælle for lavt uden varsel.

Sammenligningsvinduet
---------------------
window_days er antallet af dage hver serie måles over, regnet fra sin egen
start. Default er det størst mulige vindue, hvor ALLE serier har data (dvs. det
yngste medies alder) — ellers ville man sammenligne DetailWatch' fire år med
NDW's første uge og kalde det en forskel. Brugeren kan overskrive tallet.

Perf-noter
----------
Site-filteret er en EXISTS-subquery mod STRING_SPLIT(d.sites) — samme mønster
som modul_marketing: ingen row-multiplication, ingen DISTINCT nødvendig.
Bucket-inddelingen sker i SQL med heltalsdivision på DATEDIFF, så serveren
returnerer højst ét datapunkt pr. interval frem for én række pr. deal.
"""
import logging
from datetime import date, datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

# Fælles pooled DB-forbindelse — se db.py.
from db import get_conn  # noqa: E402


# Højst så mange serier ad gangen. Fire kurver er, hvad der kan læses i én graf,
# og hver serie koster to queries.
SERIES_MAX = 4

# Hvilken dato-kolonne den relative tidsakse måles på. Whitelist — værdien
# interpoleres direkte ind i SQL, så den må ALDRIG komme ubeskyttet fra klienten.
DATE_BASIS = {
    "add_time":                "d.[add_time]",
    "won_time":                "d.[won_time]",
    "close_time":              "d.[close_time]",
    "service_activation_date": "d.[service_activation_date]",
}
DATE_BASIS_LABELS = {
    "add_time":                "Oprettet (add_time)",
    "won_time":                "Vundet (won_time)",
    "close_time":              "Lukket (close_time)",
    "service_activation_date": "Aktivering (service_activation_date)",
}
DEFAULT_DATE_BASIS = "add_time"

# Intervalbredde i dage. 'Måned' er 30 dage og ikke en kalendermåned med vilje:
# når to medier starter på forskellige datoer i måneden, ville kalendermåneder
# give serierne forskelligt lange første perioder.
BUCKETS = {"day": 1, "week": 7, "month": 30}
BUCKET_LABELS = {"day": "Dag", "week": "Uge", "month": "Måned (30 dage)"}
DEFAULT_BUCKET = "week"

# Loft på antal intervaller i svaret, så 'Dag' + fire års historik ikke bliver
# 1.600 datapunkter pr. serie.
_MAX_BUCKETS = 400

# Loft på rå deals i eksporten — regnearket bygges client-side af SheetJS.
_EXPORT_ROW_CAP = 25000

# Samme udelukkelse som modul_marketing bruger. Den er bevidst IKKE flyttet til
# constants.py: perf, rotation og marketing filtrerer administrative deals
# forskelligt, og en sammenlægning ville ændre eksisterende tal (se constants.py).
_ADM_EXCLUDE = (
    " AND COALESCE(d.[administrativ],'') <> 'ja'"
    " AND UPPER(LTRIM(d.[title])) NOT LIKE 'ADMINISTRATIV%'"
    " AND UPPER(LTRIM(d.[title])) NOT LIKE 'ADM %'"
    " AND COALESCE(d.[deal_type],'') <> 'Rapport'"
)

# status='deleted' er Pipedrives gravsten for en slettet deal (205 i tabellen),
# ikke en forretningstilstand. Uden denne udelukkelse blev de talt med i
# 'deals i vinduet', men ikke i won/open/lost — så kortet gik ikke op:
# NordicDefenceWatch viste 145 deals mod 4+117+23=144. Filteret gælder ALTID,
# også når brugeren har valgt statusser, og 'deleted' er derfor også pillet ud
# af status-dropdownen.
_DELETED_EXCLUDE = " AND COALESCE(d.[status],'') <> 'deleted'"

# Web Sale er self-service-salg fra hjemmesiden: deal'en oprettes og vindes i
# samme øjeblik, den har ingen sælger og ingen deal_source. Den tælles derfor
# SÆRSKILT fra rigtige vundne salg — ellers ser et brand ud til at have vundet
# fire deals, hvor de to er automatiske webkøb. Deal Source-dashboardet
# ekskluderer pipelinen helt; her vises den for sig, så tallene kan forliges.
#
# UPPER() fordi Pipedrive sender både 'Web Sale' og 'Web sale' alt efter konto.
# SQL Server-collationen er i praksis case-insensitiv, men det skal ikke være
# det, der holder tallet rigtigt.
_WEB_SALE_SQL = "UPPER(COALESCE(d.[pipeline_name],'')) = 'WEB SALE'"

# QA- og udviklingsdeals: kontaktpersonen heder noget med 'test'. Der er ~1.190
# af dem i tabellen ('Test Test' alene står for 392), og de er næsten alle
# TABTE — de trækker derfor win rate ned uden at have noget med salg at gøre.
#
# 'test' skal stå i begyndelsen af et ORD, ikke bare være en delstreng. Et naivt
# LIKE '%test%' rammer rigtige nordiske efternavne — Eftestøl, Slettestøl,
# Gautestad, Syftestad, Nattestad, Rustestuen, Hjaltested, Bentestuen, Potestas
# — og dermed 15 rigtige personer, heriblandt Kristine Bentestuen Ludvigsen med
# fire VUNDNE deals. Prisen for ord-grænsen er, at ~8 sammenskrevne testdeals
# ('spirdaxtest', 'Christest1449', 'a-leaf-test') slipper igennem. Byttet er
# bevidst: hellere beholde en håndfuld testdeals end smide rigtigt salg ud.
_TEST_PERSON_SQL = (
    "(COALESCE(d.[person_name],'') LIKE 'test%'"
    " OR COALESCE(d.[person_name],'') LIKE '% test%')"
)

# Kun person_name — ALDRIG org_name. Flere rigtige kunder har 'test' i
# firmanavnet: Skattestyrelsen (19 deals), TestaViva DK ApS (16),
# R&D Test Systems A/S (13), Eurofins Food & Feed Testing Norway, TestHuset A/S,
# Testcenter Danmark. Et org-filter ville fjerne dem alle.
TEST_PERSON_MODES = ("exclude", "only")


def _clean_list(values) -> list[str]:
    """Normalisér en query-param: list/str/None -> list[str] uden tomme."""
    if not values:
        return []
    if isinstance(values, str):
        values = [values]
    return [v.strip() for v in values if v and v.strip()]


def _in_placeholders(n: int) -> str:
    return "(" + ",".join(["%s"] * n) + ")"


def parse_date(value: str, field: str = "dato") -> date:
    """ISO-dato -> date. Kaster ValueError med en besked brugeren kan læse."""
    try:
        return datetime.strptime((value or "").strip(), "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(f"Ugyldig {field}: {value!r} — brug formatet ÅÅÅÅ-MM-DD")


def resolve_date_basis(value: Optional[str]) -> str:
    return value if value in DATE_BASIS else DEFAULT_DATE_BASIS


def resolve_bucket(value: Optional[str]) -> str:
    return value if value in BUCKETS else DEFAULT_BUCKET


def _scope_clause(sites, pipelines, statuses, exclude_adm: bool,
                  test_persons: Optional[str] = "exclude") -> tuple[str, list]:
    """WHERE-fragment for ét medie: sites + pipeline + status (+ adm-filter).

    test_persons: 'exclude' (default) frasorterer QA-deals, 'only' henter KUN
    dem (bruges til at vise hvad filteret fjernede), None slår filteret fra.

    Datofiltrene ligger IKKE her — de er serie-specifikke og bygges af kalderen,
    fordi hver serie har sin egen start.
    """
    if test_persons not in (None,) + TEST_PERSON_MODES:
        raise ValueError(
            f"Ugyldig test_persons: {test_persons!r} — brug 'exclude' eller 'only'")
    sites     = _clean_list(sites)
    pipelines = _clean_list(pipelines)
    statuses  = _clean_list(statuses)
    if not sites:
        raise ValueError("En serie skal have mindst ét site")

    clauses = [
        f"""EXISTS (
            SELECT 1 FROM STRING_SPLIT(d.sites, ',') ss
            WHERE LTRIM(RTRIM(ss.value)) IN {_in_placeholders(len(sites))}
        )"""
    ]
    params: list = list(sites)
    if pipelines:
        clauses.append(f"d.pipeline_name IN {_in_placeholders(len(pipelines))}")
        params.extend(pipelines)
    if statuses:
        clauses.append(f"d.status IN {_in_placeholders(len(statuses))}")
        params.extend(statuses)
    where = " AND " + " AND ".join(clauses) + _DELETED_EXCLUDE
    if exclude_adm:
        where += _ADM_EXCLUDE
    if test_persons == "exclude":
        where += f" AND NOT {_TEST_PERSON_SQL}"
    elif test_persons == "only":
        where += f" AND {_TEST_PERSON_SQL}"
    return where, params


# ── Filter-dropdowns ─────────────────────────────────────────────────────────

def db_filter_options() -> dict:
    """Distinkte sites, pipelines og statusser til dropdownsene."""
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)

        cur.execute("""
            SELECT DISTINCT LTRIM(RTRIM(s.value)) AS v
            FROM [dbo].[PipedriveDeals] d
            CROSS APPLY STRING_SPLIT(d.sites, ',') AS s
            WHERE d.sites IS NOT NULL AND LTRIM(RTRIM(s.value)) <> ''
            ORDER BY v
        """)
        sites = [r["v"] for r in cur.fetchall()]

        cur.execute("""
            SELECT LTRIM(RTRIM(d.pipeline_name)) AS v, COUNT(*) AS c
            FROM [dbo].[PipedriveDeals] d
            WHERE d.pipeline_name IS NOT NULL AND LTRIM(RTRIM(d.pipeline_name)) <> ''
            GROUP BY LTRIM(RTRIM(d.pipeline_name))
            ORDER BY v
        """)
        pipelines = [r["v"] for r in cur.fetchall()]

        # 'deleted' udelades: den udelukkes altid i queryerne (se
        # _DELETED_EXCLUDE), så et valg af den ville give tomt resultat.
        cur.execute("""
            SELECT DISTINCT LTRIM(RTRIM(d.status)) AS v
            FROM [dbo].[PipedriveDeals] d
            WHERE d.status IS NOT NULL AND LTRIM(RTRIM(d.status)) <> ''
              AND LTRIM(RTRIM(d.status)) <> 'deleted'
            ORDER BY v
        """)
        statuses = [r["v"] for r in cur.fetchall()]

        conn.close()
        return {
            "sites":       sites,
            "pipelines":   pipelines,
            "statuses":    statuses,
            "date_basis":  [{"value": k, "label": DATE_BASIS_LABELS[k]} for k in DATE_BASIS],
            "buckets":     [{"value": k, "label": BUCKET_LABELS[k]} for k in BUCKETS],
            "series_max":  SERIES_MAX,
        }
    except Exception:
        logger.exception("db_filter_options fejlede")
        raise


def db_first_activity(
    sites,
    date_basis: Optional[str] = None,
    pipelines=None,
    statuses=None,
    exclude_adm: bool = True,
    exclude_test: bool = True,
) -> dict:
    """Første og sidste dato med aktivitet på et medie — bruges til at auto-
    udfylde launchdatoen, når brugeren vælger et site.

    Det er et FORSLAG, ikke en sandhed: første deal kan ligge før den officielle
    launch (DetailWatch DK har fx Company Trial-deals fra maj 2021, næsten et år
    før 8. marts 2022). Derfor kan feltet altid overskrives i UI'et.
    """
    basis = resolve_date_basis(date_basis)
    col   = DATE_BASIS[basis]
    where, params = _scope_clause(sites, pipelines, statuses, exclude_adm,
                                  "exclude" if exclude_test else None)
    sql = f"""
        SELECT
            CONVERT(NVARCHAR(10), MIN({col}), 23) AS first_date,
            CONVERT(NVARCHAR(10), MAX({col}), 23) AS last_date,
            COUNT({col}) AS deals
        FROM [dbo].[PipedriveDeals] d
        WHERE {col} IS NOT NULL
          {where}
    """
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(sql, tuple(params))
        row = cur.fetchone() or {}
        conn.close()
        return {
            "first_date": row.get("first_date") or "",
            "last_date":  row.get("last_date") or "",
            "deals":      int(row.get("deals") or 0),
            "date_basis": basis,
        }
    except Exception:
        logger.exception("db_first_activity fejlede")
        raise


# ── Sammenligningen ──────────────────────────────────────────────────────────

def _series_totals(cur, col: str, where: str, params: list,
                   start: date, window_end: date) -> dict:
    """Totaler for én serie: både inden for vinduet og for hele levetiden.

    Begge sæt kommer fra ÉN query med conditional aggregation — vinduet er en
    delmængde af "siden start", så en ekstra round-trip ville være spild.
    window_end er EKSKLUSIV: dato-kolonnerne er datetime, så '<= sidste dag'
    ville skære alt efter midnat på den dag væk.
    """
    # 'won' er vundet UDEN Web Sale; webkøbene ligger i egne felter. Summen
    # won + won_web_sale + open + lost giver derfor stadig deals.
    won      = f"{col} < %s AND d.status = 'won' AND NOT {_WEB_SALE_SQL}"
    won_web  = f"{col} < %s AND d.status = 'won' AND {_WEB_SALE_SQL}"
    sql = f"""
        SELECT
            SUM(CASE WHEN {col} < %s THEN 1 ELSE 0 END)                       AS w_deals,
            SUM(CASE WHEN {won}  THEN 1 ELSE 0 END)                           AS w_won,
            SUM(CASE WHEN {won_web} THEN 1 ELSE 0 END)                        AS w_won_web,
            SUM(CASE WHEN {col} < %s AND d.status = 'open' THEN 1 ELSE 0 END) AS w_open,
            SUM(CASE WHEN {col} < %s AND d.status = 'lost' THEN 1 ELSE 0 END) AS w_lost,
            ISNULL(CAST(SUM(CASE WHEN {won}
                                 THEN ISNULL(d.value_dkk, 0) ELSE 0 END) AS BIGINT), 0) AS w_won_value,
            ISNULL(CAST(SUM(CASE WHEN {won_web}
                                 THEN ISNULL(d.value_dkk, 0) ELSE 0 END) AS BIGINT), 0) AS w_won_web_value,
            COUNT(DISTINCT CASE WHEN {col} < %s THEN d.org_id END)            AS w_orgs,
            COUNT(*)                                                          AS all_deals,
            COUNT(DISTINCT d.org_id)                                          AS all_orgs,
            CONVERT(NVARCHAR(10), MIN({col}), 23)                             AS first_date,
            CONVERT(NVARCHAR(10), MAX({col}), 23)                             AS last_date
        FROM [dbo].[PipedriveDeals] d
        WHERE {col} >= %s
          {where}
    """
    end_s = window_end.isoformat()
    cur.execute(sql, (end_s,) * 8 + (start.isoformat(),) + tuple(params))
    r = cur.fetchone() or {}
    w_won, w_lost = int(r.get("w_won") or 0), int(r.get("w_lost") or 0)
    # Win rate regnes UDEN Web Sale: webkøb er altid vundne, så de ville løfte
    # raten kunstigt og gøre den uegnet til at sammenligne salgsindsats.
    closed = w_won + w_lost
    return {
        "deals":             int(r.get("w_deals") or 0),
        "won":               w_won,
        "won_web_sale":      int(r.get("w_won_web") or 0),
        "open":              int(r.get("w_open") or 0),
        "lost":              w_lost,
        "won_value":         int(r.get("w_won_value") or 0),
        "won_web_sale_value": int(r.get("w_won_web_value") or 0),
        "orgs":              int(r.get("w_orgs") or 0),
        # Win rate på AFSLUTTEDE deals (won ÷ (won+lost)) — samme definition som
        # marketing-dashboardets 'Performance pr. Account'. None når intet er lukket.
        "win_rate":          round(w_won / closed * 100, 1) if closed else None,
        "deals_all":         int(r.get("all_deals") or 0),
        "orgs_all":          int(r.get("all_orgs") or 0),
        "first_date":        r.get("first_date") or "",
        "last_date":         r.get("last_date") or "",
    }


def _series_test_deals(cur, col: str, where: str, params: list,
                       start: date, window_end: date) -> dict:
    """Hvor mange QA-deals blev frasorteret i vinduet — og hvad de ville gøre.

    Tallet vises på kortet, så filteret ikke arbejder i det skjulte. Der ER
    grænsetilfælde ('Julie Tester' har 3 vundne deals for 22.996 kr., 'Yvan
    Testu' og 'Cecilie Testern' kan være rigtige efternavne), så man skal kunne
    klikke sig ind og se præcis hvad der røg ud.

    `where` skal være bygget med test_persons='only'.
    """
    sql = f"""
        SELECT
            COUNT(*) AS deals,
            SUM(CASE WHEN d.status = 'won'  THEN 1 ELSE 0 END) AS won,
            SUM(CASE WHEN d.status = 'open' THEN 1 ELSE 0 END) AS open_count,
            SUM(CASE WHEN d.status = 'lost' THEN 1 ELSE 0 END) AS lost
        FROM [dbo].[PipedriveDeals] d
        WHERE {col} >= %s
          AND {col} < %s
          {where}
    """
    cur.execute(sql, (start.isoformat(), window_end.isoformat()) + tuple(params))
    r = cur.fetchone() or {}
    return {
        "deals": int(r.get("deals") or 0),
        "won":   int(r.get("won") or 0),
        "open":  int(r.get("open_count") or 0),
        "lost":  int(r.get("lost") or 0),
    }


def _series_before_start(cur, col: str, where: str, params: list, start: date) -> dict:
    """Deals der ligger FØR seriens dag 1 — altså uden for sammenligningen.

    Uden dette tal ser totalerne forkerte ud, når man holder dem op mod et
    dashboard uden relativ tidsakse. NordicDefenceWatch er et godt eksempel:
    Deal Source viser 149 deals, benchmarken 144, og forskellen er 7 tabte
    deals oprettet 6.–10. august — FØR brandet gik live den 11. Det er korrekt
    at udelade dem i en «siden launch»-sammenligning, men det skal kunne SES,
    ellers ligner det en fejl i tallene.
    """
    sql = f"""
        SELECT
            COUNT(*) AS deals,
            CONVERT(NVARCHAR(10), MIN({col}), 23) AS first_date,
            CONVERT(NVARCHAR(10), MAX({col}), 23) AS last_date
        FROM [dbo].[PipedriveDeals] d
        WHERE {col} < %s
          {where}
    """
    cur.execute(sql, (start.isoformat(),) + tuple(params))
    r = cur.fetchone() or {}
    return {
        "deals":      int(r.get("deals") or 0),
        "first_date": r.get("first_date") or "",
        "last_date":  r.get("last_date") or "",
    }


def _series_currencies(cur, col: str, where: str, params: list,
                       start: date, window_end: date) -> list[dict]:
    """Valutafordeling på seriens vundne deals i vinduet.

    Alle omsætningstal i dashboardet er DKK — de kommer fra [value_dkk], som
    Pipedrive-synken udfylder med dagens kurs ([fx_rate]) for hver deal. Kolonnen
    er udfyldt på samtlige rækker i tabellen, også NOK/SEK/EUR/USD, så der er
    ingen huller at falde i.

    Fordelingen returneres alligevel, så et svensk eller norsk medie kan vises
    med «omregnet fra SEK» og sit lokale beløb ved siden af. Uden det kan man
    ikke se på et DKK-tal, om der HAR været en omregning — og så er der ingen
    måde at afstemme mod en svensk rapport.

    Bemærk: modsat de øvrige dashboards regnes her ALTID i DKK. constants.py's
    deal_value_sql() lader NO/SE/DE-organisationer regne i lokal valuta, fordi
    deres budgetter er lagt i lokal valuta — men her sammenlignes medier på
    tværs af lande, og da skal enheden være den samme for alle.
    """
    sql = f"""
        SELECT
            COALESCE(NULLIF(LTRIM(RTRIM(d.currency)),''), 'DKK') AS currency,
            COUNT(*) AS deals,
            ISNULL(CAST(SUM(ISNULL(d.value, 0))     AS BIGINT), 0) AS value_local,
            ISNULL(CAST(SUM(ISNULL(d.value_dkk, 0)) AS BIGINT), 0) AS value_dkk
        FROM [dbo].[PipedriveDeals] d
        WHERE {col} >= %s
          AND {col} < %s
          AND d.status = 'won'
          {where}
        GROUP BY COALESCE(NULLIF(LTRIM(RTRIM(d.currency)),''), 'DKK')
        ORDER BY value_dkk DESC
    """
    cur.execute(sql, (start.isoformat(), window_end.isoformat()) + tuple(params))
    return [
        {
            "currency":    r["currency"],
            "deals":       int(r["deals"] or 0),
            "value_local": int(r["value_local"] or 0),
            "value_dkk":   int(r["value_dkk"] or 0),
        }
        for r in cur.fetchall()
    ]


def _series_buckets(cur, col: str, where: str, params: list,
                    start: date, end: date, bucket_days: int) -> list[dict]:
    """Deals pr. relativt interval siden `start`. Kun ikke-tomme intervaller —
    kalderen fylder hullerne, så svaret ikke bærer rundt på nuller.

    Heltalsdivisionen sker i SQL, så GROUP BY reducerer til ét datapunkt pr.
    interval frem for at sende én række pr. deal over ledningen.
    """
    sql = f"""
        SELECT
            DATEDIFF(DAY, %s, {col}) / %s                      AS bucket_idx,
            COUNT(*)                                           AS deals,
            SUM(CASE WHEN d.status = 'won' AND NOT {_WEB_SALE_SQL}
                     THEN 1 ELSE 0 END)                        AS won,
            SUM(CASE WHEN d.status = 'won' AND {_WEB_SALE_SQL}
                     THEN 1 ELSE 0 END)                        AS won_web,
            SUM(CASE WHEN d.status = 'open' THEN 1 ELSE 0 END) AS open_count,
            SUM(CASE WHEN d.status = 'lost' THEN 1 ELSE 0 END) AS lost,
            ISNULL(CAST(SUM(CASE WHEN d.status = 'won' AND NOT {_WEB_SALE_SQL}
                                 THEN ISNULL(d.value_dkk, 0) ELSE 0 END) AS BIGINT), 0) AS won_value
        FROM [dbo].[PipedriveDeals] d
        WHERE {col} >= %s
          AND {col} < %s
          {where}
        GROUP BY DATEDIFF(DAY, %s, {col}) / %s
        ORDER BY bucket_idx
    """
    # Parameter-rækkefølgen følger %s'ernes rækkefølge i SQL-teksten, og {where}
    # står MELLEM datofiltrene og GROUP BY — derfor ligger scope-parametrene
    # midt i tuplen og ikke til sidst.
    start_s = start.isoformat()
    cur.execute(sql, (start_s, bucket_days, start_s, end.isoformat())
                + tuple(params) + (start_s, bucket_days))
    return [
        {
            "i":            int(r["bucket_idx"]),
            "deals":        int(r["deals"] or 0),
            "won":          int(r["won"] or 0),
            "won_web_sale": int(r["won_web"] or 0),
            "open":         int(r["open_count"] or 0),
            "lost":         int(r["lost"] or 0),
            "won_value":    int(r["won_value"] or 0),
        }
        for r in cur.fetchall()
    ]


def db_compare(
    series: list[dict],
    date_basis: Optional[str] = None,
    pipelines=None,
    statuses=None,
    bucket: Optional[str] = None,
    window_days: Optional[int] = None,
    exclude_adm: bool = True,
    exclude_test: bool = True,
    full_history: bool = False,
    today: Optional[date] = None,
) -> dict:
    """Kernen: samme tal for hver serie, målt fra dens egen startdato.

    series: [{"label": str|None, "sites": [str, ...], "start_date": "ÅÅÅÅ-MM-DD"}]
    window_days: None -> auto (det yngste medies alder, så alle serier har data
                 i hele vinduet). Sat -> brugerens eget valg.
    full_history: når True dækker kurven hele levetiden pr. serie i stedet for
                 kun vinduet. Totalerne i `totals` følger stadig vinduet, så
                 sammenligningstabellen bliver ikke misvisende af et zoom.
    """
    if not series:
        raise ValueError("Vælg mindst ét medie at sammenligne")
    if len(series) > SERIES_MAX:
        raise ValueError(f"Højst {SERIES_MAX} medier ad gangen")

    basis       = resolve_date_basis(date_basis)
    col         = DATE_BASIS[basis]
    bucket      = resolve_bucket(bucket)
    bucket_days = BUCKETS[bucket]
    today       = today or date.today()

    prepared = []
    for idx, s in enumerate(series):
        sites = _clean_list(s.get("sites"))
        if not sites:
            raise ValueError(f"Serie {idx + 1} mangler et medie")
        start = parse_date(s.get("start_date", ""), f"startdato for serie {idx + 1}")
        if start > today:
            raise ValueError(f"Startdato for serie {idx + 1} ligger i fremtiden")
        where, params = _scope_clause(sites, pipelines, statuses, exclude_adm,
                                      "exclude" if exclude_test else None)
        # Samme scope, men KUN testdeals — grundlag for "hvad blev fjernet".
        test_where, test_params = _scope_clause(sites, pipelines, statuses,
                                                exclude_adm, "only")
        prepared.append({
            "label":  (s.get("label") or "").strip() or ", ".join(sites),
            "sites":  sites,
            "start":  start,
            "where":  where,
            "params": params,
            "test_where":  test_where,
            "test_params": test_params,
            # +1 så startdagen selv tælles med: launcher man i dag, har man 1 dags data.
            "days_available": (today - start).days + 1,
        })

    auto_window = min(p["days_available"] for p in prepared)
    if window_days is None:
        effective_window = auto_window
    else:
        effective_window = max(1, int(window_days))

    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        out = []
        for p in prepared:
            # Eksklusiv øvre grænse — se _series_totals.
            window_end = p["start"] + timedelta(days=effective_window)
            totals = _series_totals(cur, col, p["where"], p["params"], p["start"], window_end)
            currencies = _series_currencies(cur, col, p["where"], p["params"],
                                            p["start"], window_end)
            before = _series_before_start(cur, col, p["where"], p["params"], p["start"])
            test_deals = _series_test_deals(cur, col, p["test_where"], p["test_params"],
                                            p["start"], window_end)

            chart_days = p["days_available"] if full_history else effective_window
            chart_end  = min(p["start"] + timedelta(days=chart_days), today + timedelta(days=1))
            buckets    = _series_buckets(cur, col, p["where"], p["params"],
                                         p["start"], chart_end, bucket_days)
            n_buckets  = -(-chart_days // bucket_days)  # ceil
            out.append({
                "label":          p["label"],
                "sites":          p["sites"],
                "start_date":     p["start"].isoformat(),
                "window_end":     (window_end - timedelta(days=1)).isoformat(),
                "days_available": p["days_available"],
                "chart_days":     chart_days,
                "n_buckets":      min(n_buckets, _MAX_BUCKETS),
                "truncated":      n_buckets > _MAX_BUCKETS,
                "totals":         totals,
                "currencies":     currencies,
                "before_start":   before,
                "test_deals":     test_deals,
                "buckets":        [b for b in buckets if b["i"] < _MAX_BUCKETS],
            })
        conn.close()
        return {
            # Alle beløb i svaret er DKK — se _series_currencies.
            "currency":         "DKK",
            "date_basis":       basis,
            "date_basis_label": DATE_BASIS_LABELS[basis],
            "bucket":           bucket,
            "bucket_label":     BUCKET_LABELS[bucket],
            "bucket_days":      bucket_days,
            "window_days":      effective_window,
            "auto_window_days": auto_window,
            "window_is_auto":   window_days is None,
            "exclude_test":     exclude_test,
            "full_history":     full_history,
            "pipelines":        _clean_list(pipelines),
            "statuses":         _clean_list(statuses),
            "exclude_adm":      exclude_adm,
            "today":            today.isoformat(),
            "series":           out,
        }
    except Exception:
        logger.exception("db_compare fejlede")
        raise


WEB_SALE_MODES = ("only", "exclude")


def db_series_deals(
    sites,
    start_date: str,
    window_days: int,
    date_basis: Optional[str] = None,
    pipelines=None,
    statuses=None,
    exclude_adm: bool = True,
    web_sale: Optional[str] = None,
    test_persons: Optional[str] = "exclude",
    limit: int = _EXPORT_ROW_CAP,
) -> dict:
    """Rå deals for én serie inden for vinduet — til drill-down og Excel.

    web_sale: 'only' -> kun Web Sale-pipelinen, 'exclude' -> alt andet end den,
    None -> begge. Bruges når man klikker på 'Won' eller 'Won (Web Sale)' i
    sammenligningstabellen, så man ser præcis de deals tallet dækker.

    test_persons: 'exclude' (default) matcher sammenligningen, 'only' viser de
    QA-deals filteret har fjernet, None slår filteret fra.

    Returnerer {"rows": [...], "total": n, "truncated": bool}.
    """
    basis  = resolve_date_basis(date_basis)
    col    = DATE_BASIS[basis]
    start  = parse_date(start_date, "startdato")
    end    = start + timedelta(days=max(1, int(window_days)))
    limit  = max(1, min(_EXPORT_ROW_CAP, int(limit or _EXPORT_ROW_CAP)))
    where, params = _scope_clause(sites, pipelines, statuses, exclude_adm, test_persons)
    if web_sale == "only":
        where += f" AND {_WEB_SALE_SQL}"
    elif web_sale == "exclude":
        where += f" AND NOT {_WEB_SALE_SQL}"
    elif web_sale is not None:
        raise ValueError(f"Ugyldig web_sale: {web_sale!r} — brug 'only' eller 'exclude'")

    sql = f"""
        SELECT
            d.pd_deal_id,
            DATEDIFF(DAY, %s, {col})                             AS day_offset,
            CONVERT(NVARCHAR(10), {col}, 23)                     AS basis_date,
            d.status,
            d.account,
            d.sites,
            d.pipeline_name,
            d.stage_name,
            d.deal_source,
            d.deal_type,
            d.org_name,
            d.person_name,
            d.owner_name,
            d.team,
            d.title,
            d.currency,
            d.value,
            d.value_dkk,
            CONVERT(NVARCHAR(10), d.add_time,                23) AS add_date,
            CONVERT(NVARCHAR(10), d.won_time,                23) AS won_date,
            CONVERT(NVARCHAR(10), d.service_activation_date, 23) AS service_activation_date,
            COUNT(*) OVER()                                      AS total_rows
        FROM [dbo].[PipedriveDeals] d
        WHERE {col} >= %s
          AND {col} < %s
          {where}
        ORDER BY {col} ASC, d.pd_deal_id ASC
        OFFSET 0 ROWS FETCH NEXT %s ROWS ONLY
    """
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(sql, (start.isoformat(), start.isoformat(), end.isoformat())
                    + tuple(params) + (limit,))
        rows, total = [], 0
        for r in cur.fetchall():
            total = int(r.get("total_rows") or 0)  # samme værdi i alle rækker
            rows.append({
                "deal_id":                 r.get("pd_deal_id"),
                # +1 så den første dag hedder "dag 1", ikke "dag 0" — det er
                # sådan tallet læses i en launch-sammenligning.
                "day":                     int(r.get("day_offset") or 0) + 1,
                "basis_date":              r.get("basis_date") or "",
                "status":                  r.get("status") or "",
                "account":                 r.get("account") or "",
                "sites":                   r.get("sites") or "",
                "pipeline":                r.get("pipeline_name") or "",
                "stage":                   r.get("stage_name") or "",
                "deal_source":             r.get("deal_source") or "",
                "deal_type":               r.get("deal_type") or "",
                "org_name":                r.get("org_name") or "",
                "person_name":             r.get("person_name") or "",
                "owner_name":              r.get("owner_name") or "",
                "team":                    r.get("team") or "",
                "title":                   r.get("title") or "",
                "currency":                r.get("currency") or "",
                "value":                   float(r.get("value") or 0),
                "value_dkk":               float(r.get("value_dkk") or 0),
                "add_date":                r.get("add_date") or "",
                "won_date":                r.get("won_date") or "",
                "service_activation_date": r.get("service_activation_date") or "",
            })
        conn.close()
        return {"rows": rows, "total": total, "truncated": total > len(rows)}
    except Exception:
        logger.exception("db_series_deals fejlede")
        raise

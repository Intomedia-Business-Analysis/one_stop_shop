import calendar
import os
import traceback
from datetime import date, timedelta

import pymssql
from dotenv import load_dotenv

load_dotenv()

SUBSCRIPTION_BRANDS = [
    "EnergiWatch NO", "MobilityWatch DK", "CleantechWatch DK", "TechWatch NO",
    "AdvokatWatch NO", "Kforum DK", "Seniormonitor", "All Monitor Sites",
    "FinansWatch SE", "Watch Medier DK", "Byrummonitor", "ShippingWatch DK",
    "Idrætsmonitor", "Justitsmonitor", "MatvareWatch NO", "Naturmonitor",
    "Socialmonitor", "FinansWatch DK", "Uddannelsesmonitor", "MedWatch NO",
    "Klimamonitor", "EjendomsWatch DK", "FINANS DK", "DetailWatch DK",
    "FinansWatch NO", "AdvokatWatch DK", "ITWatch DK", "KForum",
    "All Watch Sites DK", "EnergiWatch DK", "Medier24 NO", "AgriWatch DK",
    "Skolemonitor", "EiendomsWatch NO", "Kulturmonitor", "Sundhedsmonitor",
    "MarketWire", "Kom24 NO", "AMWatch DK", "KapitalWatch DK",
    "Policy DK", "HandelsWatch NO", "MedWatch DK", "FødevareWatch DK",
    "Fødevare Watch DK", "All Watch Sites NO", "MediaWatch DK", "Turistmonitor",
    "PolicyWatch DK", "Monitormedier",
]
BRANDS_PLACEHOLDER = "(" + ",".join(["%s"] * len(SUBSCRIPTION_BRANDS)) + ")"

BRAND_GROUPS: dict[str, list[str]] = {
    "watch_dk": [
        "FinansWatch DK", "Watch Medier DK", "ShippingWatch DK", "EjendomsWatch DK",
        "AdvokatWatch DK", "ITWatch DK", "EnergiWatch DK", "AgriWatch DK",
        "AMWatch DK", "KapitalWatch DK", "MedWatch DK", "FødevareWatch DK",
        "Fødevare Watch DK", "MediaWatch DK", "DetailWatch DK", "KForum", "Kforum DK",
        "All Watch Sites DK", "PolicyWatch DK", "Policy DK", "MobilityWatch DK", "CleantechWatch DK",
    ],
    "finans": ["FINANS DK"],
    "watch_no": [
        "EnergiWatch NO", "TechWatch NO", "AdvokatWatch NO", "MatvareWatch NO",
        "MedWatch NO", "FinansWatch NO", "EiendomsWatch NO", "Kom24 NO",
        "HandelsWatch NO", "Medier24 NO", "All Watch Sites NO",
    ],
    "watch_se": ["FinansWatch SE"],
    "monitor": [
        "Seniormonitor", "Byrummonitor", "Idrætsmonitor", "Justitsmonitor",
        "Naturmonitor", "Socialmonitor", "Uddannelsesmonitor", "Klimamonitor",
        "Kulturmonitor", "Sundhedsmonitor", "Skolemonitor", "Turistmonitor",
        "All Monitor Sites", "Monitormedier",
    ],
    "marketwire": ["MarketWire"],
}

BRAND_GROUP_LABELS = {
    "watch_dk":   "Watch DK",
    "finans":     "FINANS DK",
    "watch_no":   "Watch NO",
    "watch_se":   "Watch SE",
    "monitor":    "Monitor",
    "marketwire": "MarketWire",
}

GROUPBY_COLUMNS = {
    "sales_type": "[sales_type]",
    "source":     "[source_name]",
    "basis":      "[deal_basis]",
}

CANCELLATION_PIPELINES = ["Cancellation", "Cancellations", "Opsigelser"]
_CANCEL_PH = "(" + ",".join(["%s"] * len(CANCELLATION_PIPELINES)) + ")"

# Pipelines der indgår i konverteringsraten (sælger-dashboard). Sammenlignes
# case-insensitivt mod UPPER([pipeline_name]). Watch/Monitor bruger de engelske
# navne; MarketWire bruger de danske (Opsigelser udelades — det er churn);
# Banner/Job-sælgere (account jppol_advertising) bruger pipeline 'banner'/'job'.
CONVERSION_PIPELINES_UPPER = [
    "CUSTOMER", "NEWBIZZ", "COMPANY TRIAL",
    "VIRKSOMHEDSPRØVER", "TILBUD", "FORNYELSER",
    "BANNER", "JOB",
]
_CONV_PH = "(" + ",".join(["%s"] * len(CONVERSION_PIPELINES_UPPER)) + ")"

# Ekskluder administrative deals fra alle beregninger
# Bruger dedikeret kolonne + titel-fallback
_ADM_EXCLUDE = "AND (COALESCE([administrativ],'') <> 'ja') AND UPPER(LTRIM([title])) NOT LIKE 'ADMINISTRATIV%' AND UPPER(LTRIM([title])) NOT LIKE 'ADM %' AND COALESCE([deal_type],'') <> 'Rapport'"

DEAL_TYPE_ALIASES: dict[str, list[str]] = {
    "Abonnement":   ["Abonnement", "Subscription"],
    "Subscription": ["Abonnement", "Subscription"],
}
DEAL_TYPE_CANONICAL = {
    "Abonnement":   "Abonnement",
    "Subscription": "Abonnement",
}

MONTH_NAMES_DA = [
    "Januar", "Februar", "Marts", "April", "Maj", "Juni",
    "Juli", "August", "September", "Oktober", "November", "December"
]


def get_conn():
    return pymssql.connect(
        server=os.getenv("DB_SERVER"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME", "INTOMEDIA"),
        tds_version="7.0",
        login_timeout=5,
        timeout=5,
    )


def resolve_brand_list(brand_groups_param: str | None) -> list | None:
    if not brand_groups_param:
        return None
    keys = [k.strip() for k in brand_groups_param.split(",") if k.strip() in BRAND_GROUPS]
    if not keys:
        return None
    combined: list = []
    seen: set = set()
    for k in keys:
        for site in BRAND_GROUPS[k]:
            if site not in seen:
                combined.append(site)
                seen.add(site)
    return combined or None


def date_expr(date_field):
    if date_field == "service_activation_date":
        return "COALESCE([service_activation_date], [won_time])"
    return "[won_time]"


def shift_year_back(from_str: str, to_str: str):
    f = date.fromisoformat(from_str)
    t = date.fromisoformat(to_str)
    try:
        ly_f = f.replace(year=f.year - 1)
    except ValueError:
        ly_f = f.replace(year=f.year - 1, day=28)
    try:
        ly_t = t.replace(year=t.year - 1)
    except ValueError:
        ly_t = t.replace(year=t.year - 1, day=28)
    return ly_f.isoformat(), ly_t.isoformat()


def budget_range(period_from: date, period_to_excl: date):
    incl = period_to_excl - timedelta(days=1)
    start = date(period_from.year, period_from.month, 1)
    if incl.month == 12:
        end = date(incl.year + 1, 1, 1)
    else:
        end = date(incl.year, incl.month + 1, 1)
    return start, end


def build_where(date_field, date_from, date_to, include_web_sale, deal_type, sales_type,
                owner_filter, cancellations_only=False, exclude_cancellations=False,
                source=None, basis=None, brand_list=None):
    d_expr = date_expr(date_field)
    brands = brand_list if brand_list else SUBSCRIPTION_BRANDS
    brands_ph = "(" + ",".join(["%s"] * len(brands)) + ")"
    clauses = [
        "[status] = 'won'",
        f"{d_expr} >= %s",
        f"{d_expr} < %s",
        f"[sites] IN {brands_ph}",
    ]
    params = [date_from, date_to] + list(brands)

    if not include_web_sale:
        clauses.append("[pipeline_name] <> 'Web Sale'")

    if cancellations_only:
        clauses.append(f"[pipeline_name] IN {_CANCEL_PH}")
        params.extend(CANCELLATION_PIPELINES)
    elif exclude_cancellations:
        clauses.append(f"[pipeline_name] NOT IN {_CANCEL_PH}")
        params.extend(CANCELLATION_PIPELINES)

    if owner_filter:
        clauses.append("[owner_name] = %s")
        params.append(owner_filter)

    if deal_type:
        aliases = DEAL_TYPE_ALIASES.get(deal_type, [deal_type])
        alias_ph = "(" + ",".join(["%s"] * len(aliases)) + ")"
        clauses.append(f"[deal_type] IN {alias_ph}")
        params.extend(aliases)

    if sales_type:
        clauses.append("[sales_type] = %s")
        params.append(sales_type)

    if source:
        clauses.append("[source_name] = %s")
        params.append(source)

    if basis:
        clauses.append("[deal_basis] = %s")
        params.append(basis)

    return "WHERE " + " AND ".join(clauses), params


def db_get_filters():
    results = {
        "deal_types": [], "sales_types": [], "sources": [], "bases": [],
        "brands": SUBSCRIPTION_BRANDS,
        "brand_groups": [{"value": k, "label": v} for k, v in BRAND_GROUP_LABELS.items()],
    }
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        for col, key in [
            ("[deal_type]",   "deal_types"),
            ("[sales_type]",  "sales_types"),
            ("[source_name]", "sources"),
            ("[deal_basis]",  "bases"),
        ]:
            try:
                cur.execute(f"""
                    SELECT DISTINCT {col} AS val
                    FROM [dbo].[PipedriveDeals]
                    WHERE {col} IS NOT NULL AND {col} <> ''
                      AND [status] = 'won'
                      AND [sites] IN {BRANDS_PLACEHOLDER}
                    ORDER BY val
                """, tuple(SUBSCRIPTION_BRANDS))
                raw = [r["val"] for r in cur.fetchall()]
                if key == "deal_types":
                    seen: set = set()
                    deduped = []
                    for v in raw:
                        canonical = DEAL_TYPE_CANONICAL.get(v, v)
                        if canonical not in seen:
                            seen.add(canonical)
                            deduped.append(canonical)
                    results[key] = sorted(deduped)
                else:
                    results[key] = raw
            except Exception:
                pass
        conn.close()
    except Exception:
        traceback.print_exc()
    return results

#-----------------------------------------------------------------------------------------------------------------------
#                                          DET NYE DASHBOARD FOR MANAGER
#-----------------------------------------------------------------------------------------------------------------------

def db_manager_data(today: date, team: str | None = None,
                     selected_year: int | None = None, selected_month: str | None = None,
                     date_col: str = "won_time", pipeline_filter: str | None = None):
    ref_year = selected_year or today.year

    # ── Periode-parsing: understøtter enkelt måned, multi-måned (komma-sep.) og Q1-Q4 ──
    months_list: list[int] = []
    ref_month: int | None  = None

    if selected_month in ("Q1", "Q2", "Q3", "Q4"):
        q           = int(selected_month[1])
        months_list = list(range((q - 1) * 3 + 1, q * 3 + 1))
        month_label = f"{selected_month} {ref_year}"
    elif selected_month:
        # Understøtter "3" (enkelt) og "1,2,3,4" (multi)
        raw = [p.strip() for p in selected_month.split(",") if p.strip().isdigit()]
        months_list = sorted({int(p) for p in raw if 1 <= int(p) <= 12})
        if len(months_list) == 1:
            ref_month   = months_list[0]
            month_label = f"{MONTH_NAMES_DA[months_list[0] - 1]} {ref_year}"
        elif months_list:
            month_label = ", ".join(MONTH_NAMES_DA[m - 1] for m in months_list) + f" {ref_year}"
        else:
            month_label = f"Hele Året {ref_year}"
    else:
        month_label = f"Hele Året {ref_year}"

    if months_list:
        month_from   = date(ref_year, months_list[0], 1)
        last_m       = months_list[-1]
        month_to     = date(ref_year + (1 if last_m == 12 else 0), last_m % 12 + 1, 1)
        _months_ph   = "(" + ",".join(["%s"] * len(months_list)) + ")"

        def _period(col: str) -> tuple[str, tuple]:
            return f"YEAR({col}) = %s AND MONTH({col}) IN {_months_ph}", (ref_year, *months_list)
    else:
        month_from = date(ref_year, 1, 1)
        month_to   = date(ref_year + 1, 1, 1)

        def _period(col: str) -> tuple[str, tuple]:
            return f"{col} >= %s AND {col} < %s", (month_from.isoformat(), month_to.isoformat())

    brands_ph   = "(" + ",".join(["%s"] * len(SUBSCRIPTION_BRANDS)) + ")"

    _VALID_DATE_COLS = {"won_time", "service_activation_date"}
    if date_col not in _VALID_DATE_COLS:
        date_col = "won_time"
    d_col = f"[{date_col}]"

    # ── Team-parsing: understøtter enkelt team og multi-team (komma-sep.) ────
    teams_list: list[str] = [t.strip() for t in team.split(",") if t.strip()] if team else []
    multi_team = len(teams_list) > 1

    if multi_team:
        # Multi-team: simpel [team] IN (...) filter — ingen HubUsers JOIN
        _teams_ph   = "(" + ",".join(["%s"] * len(teams_list)) + ")"
        team_clause = f"AND [team] IN {_teams_ph}"
        team_params = tuple(teams_list)
        is_finans_team    = False
        is_watch_dk_team  = False
        is_watch_int_team = False
        non_finans_exclude = ""
        sites_filter = f"AND [sites] IN {brands_ph}"
        team = None  # brug non-team leaderboard-gren (GROUP BY)
    elif teams_list:
        team = teams_list[0]
        # Ekskluder FINANS DK site kun for Watch DK teamet
        is_finans_team    = "FINANS" in team.upper()
        is_watch_dk_team  = "WATCH DK" in team.upper()
        is_watch_int_team = "WATCH INT" in team.upper()

        if is_finans_team:
            team_clause = """AND [owner_name] IN (
                SELECT u2.name FROM HubUsers u2
                JOIN TeamMemberships tm2 ON tm2.user_id = u2.id
                JOIN Teams t2 ON t2.id = tm2.team_id
                WHERE t2.name = %s
                AND (tm2.end_date IS NULL OR tm2.end_date >= GETDATE())
            ) AND COALESCE([sites],'') = 'FINANS DK'"""
            team_params = (team,)
        else:
            team_clause = """AND [owner_name] IN (
                SELECT u2.name FROM HubUsers u2
                JOIN TeamMemberships tm2 ON tm2.user_id = u2.id
                JOIN Teams t2 ON t2.id = tm2.team_id
                WHERE t2.name = %s
                AND (TRY_CAST(tm2.end_date AS DATE) IS NULL OR TRY_CAST(tm2.end_date AS DATE) >= CAST(GETDATE() AS DATE))
            ) AND ([team] = %s OR [team] IS NULL)"""
            team_params = (team, team)

        if is_watch_int_team:
            non_finans_exclude = "AND COALESCE([sites],'') NOT LIKE '%FINANS%'"
        elif is_watch_dk_team:
            non_finans_exclude = "AND COALESCE([sites],'') <> 'FINANS DK'"
        else:
            non_finans_exclude = ""
        sites_filter = f"AND ([sites] IN {brands_ph} OR [sites] IS NULL)"
    else:
        team = None
        team_clause = ""
        team_params = ()
        is_finans_team    = False
        is_watch_dk_team  = False
        is_watch_int_team = False
        non_finans_exclude = ""
        sites_filter = f"AND [sites] IN {brands_ph}"

    # ── Pipeline filter (understøtter komma-separerede værdier) ─────────────
    _CANCEL_KEYS = {'Cancellations', 'Cancellation', 'Opsigelser'}
    _sel_pipes   = [p.strip() for p in pipeline_filter.split(',')] if pipeline_filter and pipeline_filter != 'all' else []
    _non_cancel  = [p for p in _sel_pipes if p not in _CANCEL_KEYS]
    _has_cancel  = any(p in _CANCEL_KEYS for p in _sel_pipes)

    if not _sel_pipes:
        # Alle pipelines
        won_where      = f"AND [pipeline_name] NOT IN {_CANCEL_PH}"
        won_wparams    = tuple(CANCELLATION_PIPELINES)
        cancel_where   = f"AND [pipeline_name] IN {_CANCEL_PH}"
        cancel_wparams = tuple(CANCELLATION_PIPELINES)
        won_case       = f"[pipeline_name] NOT IN {_CANCEL_PH}"
        cancel_case    = f"[pipeline_name] IN {_CANCEL_PH}"
        won_cparams    = tuple(CANCELLATION_PIPELINES)
        cancel_cparams = tuple(CANCELLATION_PIPELINES)
    elif _has_cancel and not _non_cancel:
        # Kun opsigelser
        won_where      = f"AND [pipeline_name] IN {_CANCEL_PH}"
        won_wparams    = tuple(CANCELLATION_PIPELINES)
        cancel_where   = "AND 1=0"
        cancel_wparams = ()
        won_case       = f"[pipeline_name] IN {_CANCEL_PH}"
        cancel_case    = "1=0"
        won_cparams    = tuple(CANCELLATION_PIPELINES)
        cancel_cparams = ()
    elif _non_cancel and not _has_cancel:
        # Specifikke ikke-opsigelse pipelines
        _pipe_ph       = "(" + ",".join(["%s"] * len(_non_cancel)) + ")"
        won_where      = f"AND [pipeline_name] IN {_pipe_ph}"
        won_wparams    = tuple(_non_cancel)
        cancel_where   = "AND 1=0"
        cancel_wparams = ()
        won_case       = f"[pipeline_name] IN {_pipe_ph}"
        cancel_case    = "1=0"
        won_cparams    = tuple(_non_cancel)
        cancel_cparams = ()
    else:
        # Mix: specifikke pipelines + opsigelser
        _pipe_ph       = "(" + ",".join(["%s"] * len(_non_cancel)) + ")"
        won_where      = f"AND [pipeline_name] IN {_pipe_ph}"
        won_wparams    = tuple(_non_cancel)
        cancel_where   = f"AND [pipeline_name] IN {_CANCEL_PH}"
        cancel_wparams = tuple(CANCELLATION_PIPELINES)
        won_case       = f"[pipeline_name] IN {_pipe_ph}"
        cancel_case    = f"[pipeline_name] IN {_CANCEL_PH}"
        won_cparams    = tuple(_non_cancel)
        cancel_cparams = tuple(CANCELLATION_PIPELINES)
    # d.-prefixed variants for JOIN leaderboard queries
    won_case_d    = won_case.replace('[pipeline_name]', 'd.[pipeline_name]')
    cancel_case_d = cancel_case.replace('[pipeline_name]', 'd.[pipeline_name]')

    conn = get_conn()
    cur  = conn.cursor(as_dict=True)

    cur.execute(f"""
        SELECT ISNULL(SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2))),0) AS total
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND {d_col} >= %s AND {d_col} < %s
          {sites_filter}
          {won_where}
          {_ADM_EXCLUDE}
          {non_finans_exclude}
          {team_clause}
    """, (today.isoformat(), (today + timedelta(days=1)).isoformat()) + tuple(SUBSCRIPTION_BRANDS) + won_wparams + team_params)
    salg_dag = float((cur.fetchone() or {}).get("total", 0) or 0)

    # Dagens won-deals pr. datokolonne (uafhængigt af globalt date_col-valg).
    # Salg-mode bruger won_time, Tilvækst-mode bruger service_activation_date.
    salg_dag_by_col = {}
    for _col in ("won_time", "service_activation_date"):
        cur.execute(f"""
            SELECT ISNULL(SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2))),0) AS total
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
              AND [{_col}] >= %s AND [{_col}] < %s
              {sites_filter}
              {won_where}
              {_ADM_EXCLUDE}
              {non_finans_exclude}
              {team_clause}
        """, (today.isoformat(), (today + timedelta(days=1)).isoformat()) + tuple(SUBSCRIPTION_BRANDS) + won_wparams + team_params)
        salg_dag_by_col[_col] = float((cur.fetchone() or {}).get("total", 0) or 0)

    # Månedlig teamtotal for valgt periode
    _p_sql, _p_params = _period(d_col)
    cur.execute(f"""
        SELECT ISNULL(SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2))),0) AS total
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND {_p_sql}
          {sites_filter}
          {won_where}
          {_ADM_EXCLUDE}
          {non_finans_exclude}
          {team_clause}
    """, tuple(_p_params) + tuple(SUBSCRIPTION_BRANDS) + won_wparams + team_params)
    salg_maaned = float((cur.fetchone() or {}).get("total", 0) or 0)

    # Team afmeldinger for valgt periode
    cur.execute(f"""
        SELECT ISNULL(SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2))),0) AS total
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND {_p_sql}
          {sites_filter}
          {cancel_where}
          {_ADM_EXCLUDE}
          {non_finans_exclude}
          {team_clause}
    """, tuple(_p_params) + tuple(SUBSCRIPTION_BRANDS) + cancel_wparams + team_params)
    cancel_maaned = abs(float((cur.fetchone() or {}).get("total", 0) or 0))
    netto_maaned  = round(salg_maaned - cancel_maaned, 2)

    # Månedschart: team-total per måned for valgt år
    year_from = date(ref_year, 1, 1)
    year_to   = date(ref_year + 1, 1, 1)
    cur.execute(f"""
        SELECT MONTH({d_col}) AS maaned,
               ISNULL(SUM(CASE WHEN {won_case}
                   THEN CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)) ELSE 0 END),0) AS won,
               ISNULL(SUM(CASE WHEN {cancel_case}
                   THEN CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)) ELSE 0 END),0) AS cancel
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND {d_col} >= %s AND {d_col} < %s
          {sites_filter}
          {_ADM_EXCLUDE}
          {non_finans_exclude}
          {team_clause}
        GROUP BY MONTH({d_col})
        ORDER BY maaned
    """, won_cparams + cancel_cparams + (year_from.isoformat(), year_to.isoformat()) + tuple(SUBSCRIPTION_BRANDS) + team_params)
    maaned_raw = {r["maaned"]: (float(r["won"] or 0), abs(float(r["cancel"] or 0))) for r in cur.fetchall()}

    # Budget pr. måned for valgt år + valgte teams
    # Bygges af SalespersonBudget for almindelige teams + BudgetsIntoMedia for Banner/Marketwire.
    budget_per_month = {m: 0.0 for m in range(1, 13)}
    if team:
        cur.execute("""
            SELECT MONTH([BudgetDate]) AS m, SUM([BudgetAmount]) AS bud
            FROM [dbo].[SalespersonBudget]
            WHERE YEAR([BudgetDate]) = %s AND [Team] = %s
            GROUP BY MONTH([BudgetDate])
        """, (ref_year, team))
    elif multi_team:
        _teams_ph_b = "(" + ",".join(["%s"] * len(teams_list)) + ")"
        cur.execute(f"""
            SELECT MONTH([BudgetDate]) AS m, SUM([BudgetAmount]) AS bud
            FROM [dbo].[SalespersonBudget]
            WHERE YEAR([BudgetDate]) = %s AND [Team] IN {_teams_ph_b}
            GROUP BY MONTH([BudgetDate])
        """, (ref_year,) + tuple(teams_list))
    else:
        cur.execute("""
            SELECT MONTH([BudgetDate]) AS m, SUM([BudgetAmount]) AS bud
            FROM [dbo].[SalespersonBudget]
            WHERE YEAR([BudgetDate]) = %s
            GROUP BY MONTH([BudgetDate])
        """, (ref_year,))
    for r in cur.fetchall():
        if r["m"]:
            budget_per_month[int(r["m"])] += float(r["bud"] or 0)

    # Tilføj Banner/Marketwire budget fra BudgetsIntoMedia (pr. måned) når relevant
    _selected_teams_mo = (
        {team} if team else
        set(teams_list) if multi_team else
        {"Team Banner", "Team Marketwire"}
    )
    if "Team Banner" in _selected_teams_mo:
        cur.execute("""
            SELECT MONTH([BudgetDate]) AS m, SUM([BudgetAmount]) AS bud
            FROM [dbo].[BudgetsIntoMedia]
            WHERE YEAR([BudgetDate]) = %s AND [DealType]='Banner'
            GROUP BY MONTH([BudgetDate])
        """, (ref_year,))
        for r in cur.fetchall():
            if r["m"]:
                budget_per_month[int(r["m"])] += float(r["bud"] or 0)
    if "Team Marketwire" in _selected_teams_mo:
        cur.execute("""
            SELECT MONTH([BudgetDate]) AS m, SUM([BudgetAmount]) AS bud
            FROM [dbo].[BudgetsIntoMedia]
            WHERE YEAR([BudgetDate]) = %s AND [Brand]='marketwire'
            GROUP BY MONTH([BudgetDate])
        """, (ref_year,))
        for r in cur.fetchall():
            if r["m"]:
                budget_per_month[int(r["m"])] += float(r["bud"] or 0)

    # Forecast pr. måned for valgt år fra HubForecasts (level='team')
    forecast_per_month = {m: 0.0 for m in range(1, 13)}
    if team:
        cur.execute("""
            SELECT forecast_month AS m, SUM(forecast_amount) AS fc
            FROM [dbo].[HubForecasts]
            WHERE forecast_year = %s AND level = 'team' AND dimension_key = %s
            GROUP BY forecast_month
        """, (ref_year, team))
    elif multi_team:
        _teams_ph_f = "(" + ",".join(["%s"] * len(teams_list)) + ")"
        cur.execute(f"""
            SELECT forecast_month AS m, SUM(forecast_amount) AS fc
            FROM [dbo].[HubForecasts]
            WHERE forecast_year = %s AND level = 'team' AND dimension_key IN {_teams_ph_f}
            GROUP BY forecast_month
        """, (ref_year,) + tuple(teams_list))
    else:
        cur.execute("""
            SELECT forecast_month AS m, SUM(forecast_amount) AS fc
            FROM [dbo].[HubForecasts]
            WHERE forecast_year = %s AND level = 'team'
            GROUP BY forecast_month
        """, (ref_year,))
    for r in cur.fetchall():
        if r["m"]:
            forecast_per_month[int(r["m"])] += float(r["fc"] or 0)

    # ── Programmatic sales (kun naar Team Banner er i valgte teams) ───────
    # ProgrammaticSales er site-tagget (FINANS DK m.fl.); ingen pipeline,
    # ingen opsigelser. Indgaar i totalen for Banner-relaterede views.
    programmatic_dag           = 0.0
    programmatic_dag_won       = 0.0
    programmatic_dag_act       = 0.0
    programmatic_maaned        = 0.0
    programmatic_budget_maaned = 0.0  # delmaengde af team_budget — Salestype='Programmatic'
    programmatic_per_month: dict[int, float] = {m: 0.0 for m in range(1, 13)}
    _progr_teams = (
        {team} if team else
        set(teams_list) if multi_team else
        {"Team Banner", "Team Marketwire"}  # alle teams -> medregn Banner
    )
    _include_progr = "Team Banner" in _progr_teams and not _sel_pipes
    if _include_progr:
        # Dagens programmatic — bemaerk at data lander dagen efter kl 16
        cur.execute("""
            SELECT ISNULL(SUM([Amount]),0) AS total
            FROM [dbo].[ProgrammaticSales]
            WHERE [Date] >= %s AND [Date] < %s
        """, (today.isoformat(), (today + timedelta(days=1)).isoformat()))
        programmatic_dag = float((cur.fetchone() or {}).get("total", 0) or 0)
        # Begge varianter af "I dag" bruger samme Date-kolonne for programmatic
        programmatic_dag_won = programmatic_dag
        programmatic_dag_act = programmatic_dag

        # Maaneds-total for valgt periode (samme periode som salg_maaned)
        cur.execute(f"""
            SELECT ISNULL(SUM([Amount]),0) AS total
            FROM [dbo].[ProgrammaticSales]
            WHERE {_period('[Date]')[0]}
        """, tuple(_period('[Date]')[1]))
        programmatic_maaned = float((cur.fetchone() or {}).get("total", 0) or 0)

        # Pr. maaned for hele aaret (til chart)
        cur.execute("""
            SELECT MONTH([Date]) AS m, ISNULL(SUM([Amount]),0) AS total
            FROM [dbo].[ProgrammaticSales]
            WHERE YEAR([Date]) = %s
            GROUP BY MONTH([Date])
        """, (ref_year,))
        for r in cur.fetchall():
            if r["m"]:
                programmatic_per_month[int(r["m"])] = float(r["total"] or 0)

        # Programmatic-specifik budget (delmaengde af Banner-budgettet)
        cur.execute(f"""
            SELECT ISNULL(SUM([BudgetAmount]),0) AS total
            FROM [dbo].[BudgetsIntoMedia]
            WHERE [DealType]='Banner' AND [Salestype]='Programmatic' AND {_period('[BudgetDate]')[0]}
        """, tuple(_period('[BudgetDate]')[1]))
        programmatic_budget_maaned = float((cur.fetchone() or {}).get("total", 0) or 0)

    # Tilfoej programmatic til top-line totals
    salg_dag      += programmatic_dag
    salg_dag_by_col["won_time"]                += programmatic_dag_won
    salg_dag_by_col["service_activation_date"] += programmatic_dag_act
    salg_maaned   += programmatic_maaned
    netto_maaned  = round(salg_maaned - cancel_maaned, 2)  # genberegnet

    maaned_chart = [{"maaned":      MONTH_NAMES_DA[m - 1][:3],
                     "won":         round(maaned_raw.get(m, (0,0))[0] + programmatic_per_month.get(m, 0), 2),
                     "won_pipe":    round(maaned_raw.get(m, (0,0))[0], 2),
                     "programmatic":round(programmatic_per_month.get(m, 0), 2),
                     "cancel":      round(maaned_raw.get(m, (0,0))[1], 2),
                     "netto":       round(maaned_raw.get(m, (0,0))[0] + programmatic_per_month.get(m, 0) - maaned_raw.get(m, (0,0))[1], 2),
                     "budget":      round(budget_per_month.get(m, 0), 2),
                     "forecast":    round(forecast_per_month.get(m, 0), 2)}
                    for m in range(1, 13)]
    sparkline = maaned_chart  # bagudkompatibilitet

    cur.execute(f"""
        SELECT
            COUNT(CASE WHEN [status]='won' AND {won_case}
                THEN 1 END) AS won_count,
            COUNT(CASE WHEN [status]='lost'
                OR ([status]='won' AND {cancel_case})
                THEN 1 END) AS lost_count
        FROM [dbo].[PipedriveDeals]
        WHERE ([status]='won' OR [status]='lost')
          AND [pipeline_name]<>'Web Sale'
          AND {_p_sql}
          {sites_filter}
          {_ADM_EXCLUDE}
          {non_finans_exclude}
          {team_clause}
    """, won_cparams + cancel_cparams + tuple(_p_params) + tuple(SUBSCRIPTION_BRANDS) + team_params)
    conv_row   = cur.fetchone() or {}
    won_count  = int(conv_row.get("won_count", 0) or 0)
    lost_count = int(conv_row.get("lost_count", 0) or 0)
    total_deals = won_count + lost_count
    conv_rate  = round((won_count / total_deals * 100), 1) if total_deals > 0 else 0.0

    if team:
        # Enkelt team: vis ALLE teammedlemmer (også 0-salg) via HubUsers JOIN
        _lp_sql, _lp_params = _period(f"d.{d_col}")
        cur.execute(f"""
            SELECT
                u.name AS owner_name,
                ISNULL(SUM(CASE WHEN {won_case_d}
                    THEN CAST(COALESCE(CASE WHEN d.[currency] IN ('NOK','SEK') THEN d.[value] ELSE d.[value_dkk] END,d.[value]) AS DECIMAL(18,2)) ELSE 0 END), 0) AS won_amount,
                COUNT(CASE WHEN {won_case_d}
                    THEN 1 END) AS won_count,
                ISNULL(SUM(CASE WHEN {cancel_case_d}
                    THEN CAST(COALESCE(CASE WHEN d.[currency] IN ('NOK','SEK') THEN d.[value] ELSE d.[value_dkk] END,d.[value]) AS DECIMAL(18,2)) ELSE 0 END), 0) AS cancel_amount
            FROM HubUsers u
            JOIN TeamMemberships tm ON tm.user_id = u.id
            JOIN Teams t ON t.id = tm.team_id
            LEFT JOIN [dbo].[PipedriveDeals] d
                ON d.[owner_name] = u.name
                AND d.[status] = 'won'
                AND d.[pipeline_name] <> 'Web Sale'
                AND {_lp_sql}
                {"AND COALESCE(d.[sites],'') = 'FINANS DK'" if is_finans_team else "AND (d.[team] = %s OR d.[team] IS NULL)"}
                AND (COALESCE(d.[administrativ],'') <> 'ja')
                AND UPPER(LTRIM(d.[title])) NOT LIKE 'ADMINISTRATIV%'
                AND UPPER(LTRIM(d.[title])) NOT LIKE 'ADM %'
                AND COALESCE(d.[deal_type],'') <> 'Rapport'
                {"AND COALESCE(d.[sites],'') NOT LIKE '%FINANS%'" if is_watch_int_team else ("AND COALESCE(d.[sites],'') <> 'FINANS DK'" if is_watch_dk_team else "")}
            WHERE t.name = %s
              AND (TRY_CAST(tm.end_date AS DATE) IS NULL OR TRY_CAST(tm.end_date AS DATE) >= CAST(GETDATE() AS DATE))
            GROUP BY u.name
            ORDER BY won_amount DESC
        """, won_cparams + won_cparams + cancel_cparams + tuple(_lp_params) + (() if is_finans_team else (team,)) + (team,))
    else:
        # Alle teams eller multi-team: GROUP BY owner
        cur.execute(f"""
            SELECT
                COALESCE([owner_name], 'Ukendt') AS owner_name,
                ISNULL(SUM(CASE WHEN {won_case}
                    THEN CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)) ELSE 0 END), 0) AS won_amount,
                COUNT(CASE WHEN {won_case}
                    THEN 1 END) AS won_count,
                ISNULL(SUM(CASE WHEN {cancel_case}
                    THEN CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)) ELSE 0 END), 0) AS cancel_amount
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
              AND {_p_sql}
              AND [sites] IN {brands_ph}
              {_ADM_EXCLUDE}
              {non_finans_exclude}
              {team_clause}
            GROUP BY [owner_name]
            ORDER BY won_amount DESC
        """, won_cparams + won_cparams + cancel_cparams + tuple(_p_params) + tuple(SUBSCRIPTION_BRANDS) + team_params)

    leaderboard = []
    for r in cur.fetchall():
        won    = float(r["won_amount"]    or 0)
        cancel = abs(float(r["cancel_amount"] or 0))
        leaderboard.append({
            "owner_name":    r["owner_name"],
            "won_amount":    round(won,    2),
            "won_count":     int(r["won_count"] or 0),
            "cancel_amount": round(cancel, 2),
            "netto_amount":  round(won - cancel, 2),
        })

    _bp_sql, _bp_params = _period("[BudgetDate]")
    if team:
        cur.execute(f"""
            SELECT [Owner] AS owner_name, SUM([BudgetAmount]) AS budget
            FROM [dbo].[SalespersonBudget]
            WHERE {_bp_sql} AND [Team] = %s
            GROUP BY [Owner]
        """, tuple(_bp_params) + (team,))
    elif multi_team:
        _teams_ph_b = "(" + ",".join(["%s"] * len(teams_list)) + ")"
        cur.execute(f"""
            SELECT [Owner] AS owner_name, SUM([BudgetAmount]) AS budget
            FROM [dbo].[SalespersonBudget]
            WHERE {_bp_sql} AND [Team] IN {_teams_ph_b}
            GROUP BY [Owner]
        """, tuple(_bp_params) + tuple(teams_list))
    else:
        cur.execute(f"""
            SELECT [Owner] AS owner_name, SUM([BudgetAmount]) AS budget
            FROM [dbo].[SalespersonBudget]
            WHERE {_bp_sql}
            GROUP BY [Owner]
        """, tuple(_bp_params))
    budget_map  = {r["owner_name"]: float(r["budget"] or 0) for r in cur.fetchall()}

    # Banner og Marketwire har ikke per-sælger budget — kun team-budget i BudgetsIntoMedia.
    # Tilføj til total budget når de relevante teams er valgt (single, multi eller alle).
    _team_budget_extra = 0.0
    _selected_team_names = (
        {team} if team else
        set(teams_list) if multi_team else
        {"Team Banner", "Team Marketwire"}
    )
    if "Team Banner" in _selected_team_names:
        cur.execute(f"""
            SELECT ISNULL(SUM([BudgetAmount]),0) AS budget
            FROM [dbo].[BudgetsIntoMedia]
            WHERE [DealType]='Banner' AND {_bp_sql}
        """, tuple(_bp_params))
        _team_budget_extra += float((cur.fetchone() or {}).get("budget", 0) or 0)
    if "Team Marketwire" in _selected_team_names:
        cur.execute(f"""
            SELECT ISNULL(SUM([BudgetAmount]),0) AS budget
            FROM [dbo].[BudgetsIntoMedia]
            WHERE [Brand]='marketwire' AND {_bp_sql}
        """, tuple(_bp_params))
        _team_budget_extra += float((cur.fetchone() or {}).get("budget", 0) or 0)

    team_budget = round(sum(budget_map.values()) + _team_budget_extra, 2)
    netto_vs_budget_pct = round(netto_maaned / team_budget * 100, 1) if team_budget > 0 else None

    for row in leaderboard:
        row["budget"] = budget_map.get(row["owner_name"], 0.0)
        row["vs_budget_pct"] = round(row["netto_amount"] / row["budget"] * 100, 1) if row["budget"] > 0 else None

    # Sorter og rank efter netto (won - afmeldinger)
    leaderboard.sort(key=lambda x: -x["netto_amount"])
    for i, row in enumerate(leaderboard):
        row["rank"] = i + 1

    cur.execute("""
        SELECT DISTINCT t.name
        FROM Teams t
        JOIN TeamMemberships tm ON tm.team_id = t.id
        WHERE t.name IS NOT NULL
          AND (tm.end_date IS NULL OR TRY_CAST(tm.end_date AS DATE) >= CAST(GETDATE() AS DATE))
        ORDER BY t.name
    """)
    teams = [r["name"] for r in cur.fetchall()]

    # ── Ugentlig rapport ────────────────────────────────────────────────────
    week_start    = today - timedelta(days=today.weekday())   # Mandag
    week_end      = week_start + timedelta(days=7)             # Næste mandag (eksklusiv)
    week_num      = today.isocalendar()[1]
    days_in_month = calendar.monthrange(today.year, today.month)[1]
    week_factor   = 7 / days_in_month
    cur_m_start   = date(today.year, today.month, 1)
    cur_m_end     = date(today.year + (1 if today.month == 12 else 0), today.month % 12 + 1, 1)

    _MO = ["jan","feb","mar","apr","maj","jun","jul","aug","sep","okt","nov","dec"]
    we  = week_end - timedelta(days=1)
    week_label_str = (f"Uge {week_num} "
                      f"({week_start.day}. {_MO[week_start.month-1]} "
                      f"– {we.day}. {_MO[we.month-1]})")

    # Team ugeomsætning (d.[team]-baseret)
    cur.execute(f"""
        SELECT
            t.name AS team,
            ISNULL(SUM(CASE WHEN d.[pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
                THEN CAST(COALESCE(CASE WHEN d.[currency] IN ('NOK','SEK') THEN d.[value] ELSE d.[value_dkk] END,d.[value]) AS DECIMAL(18,2)) ELSE 0 END), 0) AS won,
            ABS(ISNULL(SUM(CASE WHEN d.[pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
                THEN CAST(COALESCE(CASE WHEN d.[currency] IN ('NOK','SEK') THEN d.[value] ELSE d.[value_dkk] END,d.[value]) AS DECIMAL(18,2)) ELSE 0 END), 0)) AS cancel
        FROM Teams t
        LEFT JOIN [dbo].[PipedriveDeals] d
            ON d.[team] = t.name
            AND d.[status] = 'won'
            AND d.[pipeline_name] <> 'Web Sale'
            AND d.{d_col} >= %s AND d.{d_col} < %s
            AND COALESCE(d.[administrativ],'') <> 'ja'
            AND UPPER(LTRIM(d.[title])) NOT LIKE 'ADMINISTRATIV%'
            AND UPPER(LTRIM(d.[title])) NOT LIKE 'ADM %'
            AND COALESCE(d.[deal_type],'') <> 'Rapport'
        WHERE t.name IS NOT NULL AND t.name <> ''
        GROUP BY t.name
    """, (week_start.isoformat(), week_end.isoformat()))
    week_team_sales = {}
    for r in cur.fetchall():
        won    = float(r["won"]    or 0)
        cancel = float(r["cancel"] or 0)
        week_team_sales[r["team"]] = round(won - cancel, 2)

    # Team månedbudget (til proration) — samme logik som afdelingsleder
    cur.execute("""
        SELECT [Team] AS team, SUM([BudgetAmount]) AS budget
        FROM [dbo].[SalespersonBudget]
        WHERE [BudgetDate] >= %s AND [BudgetDate] < %s
          AND [Team] IS NOT NULL AND [Team] <> ''
        GROUP BY [Team]
    """, (cur_m_start.isoformat(), cur_m_end.isoformat()))
    team_m_budget = {r["team"]: float(r["budget"] or 0) for r in cur.fetchall()}

    cur.execute("""
        SELECT ISNULL(SUM([BudgetAmount]),0) AS budget FROM [dbo].[BudgetsIntoMedia]
        WHERE [DealType]='Banner' AND [BudgetDate] >= %s AND [BudgetDate] < %s
    """, (cur_m_start.isoformat(), cur_m_end.isoformat()))
    b = float((cur.fetchone() or {}).get("budget", 0) or 0)
    if b: team_m_budget["Team Banner"] = b

    cur.execute("""
        SELECT ISNULL(SUM([BudgetAmount]),0) AS budget FROM [dbo].[BudgetsIntoMedia]
        WHERE [Brand]='marketwire' AND [BudgetDate] >= %s AND [BudgetDate] < %s
    """, (cur_m_start.isoformat(), cur_m_end.isoformat()))
    b = float((cur.fetchone() or {}).get("budget", 0) or 0)
    if b: team_m_budget["Team Marketwire"] = b

    week_teams = []
    for tname, netto in sorted(week_team_sales.items(), key=lambda x: -x[1]):
        m_bud   = team_m_budget.get(tname, 0)
        w_bud   = round(m_bud * week_factor, 2) if m_bud else 0
        week_teams.append({
            "team":        tname,
            "netto":       netto,
            "budget_week": w_bud,
            "vs_pct":      round(netto / w_bud * 100, 1) if w_bud > 0 else None,
        })

    # Sælger ugeomsætning — bruger d.[team] direkte (inkluderer teamledere)
    if team:
        cur.execute(f"""
            SELECT COALESCE([owner_name],'Ukendt') AS owner_name,
                ISNULL(SUM(CASE WHEN [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
                    THEN CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)) ELSE 0 END), 0) AS won,
                ABS(ISNULL(SUM(CASE WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
                    THEN CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)) ELSE 0 END), 0)) AS cancel
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
              AND {d_col} >= %s AND {d_col} < %s
              AND [team] = %s
              AND COALESCE([administrativ],'') <> 'ja'
              AND UPPER(LTRIM([title])) NOT LIKE 'ADMINISTRATIV%'
              AND UPPER(LTRIM([title])) NOT LIKE 'ADM %'
              AND COALESCE([deal_type],'') <> 'Rapport'
            GROUP BY [owner_name] ORDER BY won DESC
        """, (week_start.isoformat(), week_end.isoformat(), team))
    else:
        cur.execute(f"""
            SELECT COALESCE([owner_name],'Ukendt') AS owner_name,
                ISNULL(SUM(CASE WHEN [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
                    THEN CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)) ELSE 0 END), 0) AS won,
                ABS(ISNULL(SUM(CASE WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
                    THEN CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)) ELSE 0 END), 0)) AS cancel
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
              AND {d_col} >= %s AND {d_col} < %s
              AND [sites] IN {brands_ph}
              {_ADM_EXCLUDE}
            GROUP BY [owner_name] ORDER BY won DESC
        """, (week_start.isoformat(), week_end.isoformat()) + tuple(SUBSCRIPTION_BRANDS))
    week_saelger_rows = cur.fetchall()

    # Sælger månedbudget
    if team:
        cur.execute("""
            SELECT [Owner] AS owner_name, SUM([BudgetAmount]) AS budget
            FROM [dbo].[SalespersonBudget]
            WHERE [BudgetDate] >= %s AND [BudgetDate] < %s AND [Team] = %s
            GROUP BY [Owner]
        """, (cur_m_start.isoformat(), cur_m_end.isoformat(), team))
    elif multi_team:
        _teams_ph_w = "(" + ",".join(["%s"] * len(teams_list)) + ")"
        cur.execute(f"""
            SELECT [Owner] AS owner_name, SUM([BudgetAmount]) AS budget
            FROM [dbo].[SalespersonBudget]
            WHERE [BudgetDate] >= %s AND [BudgetDate] < %s AND [Team] IN {_teams_ph_w}
            GROUP BY [Owner]
        """, (cur_m_start.isoformat(), cur_m_end.isoformat()) + tuple(teams_list))
    else:
        cur.execute("""
            SELECT [Owner] AS owner_name, SUM([BudgetAmount]) AS budget
            FROM [dbo].[SalespersonBudget]
            WHERE [BudgetDate] >= %s AND [BudgetDate] < %s
            GROUP BY [Owner]
        """, (cur_m_start.isoformat(), cur_m_end.isoformat()))
    saelger_m_budget = {r["owner_name"]: float(r["budget"] or 0) for r in cur.fetchall()}

    week_saelgere = []
    for r in week_saelger_rows:
        won    = float(r["won"]    or 0)
        cancel = float(r["cancel"] or 0)
        netto  = round(won - cancel, 2)
        m_bud  = saelger_m_budget.get(r["owner_name"], 0)
        w_bud  = round(m_bud * week_factor, 2) if m_bud else 0
        week_saelgere.append({
            "owner_name":  r["owner_name"],
            "netto":       netto,
            "budget_week": w_bud,
            "vs_pct":      round(netto / w_bud * 100, 1) if w_bud > 0 else None,
        })

    conn.close()
    return {
        "salg_dag":           round(salg_dag, 2),
        "salg_dag_won":       round(salg_dag_by_col["won_time"], 2),
        "salg_dag_act":       round(salg_dag_by_col["service_activation_date"], 2),
        "salg_maaned":        round(salg_maaned, 2),
        "cancel_maaned":      round(cancel_maaned, 2),
        "netto_maaned":       round(netto_maaned, 2),
        "programmatic_dag":   round(programmatic_dag, 2),
        "programmatic_maaned": round(programmatic_maaned, 2),
        "programmatic_budget_maaned": round(programmatic_budget_maaned, 2),
        "team_budget":        team_budget,
        "netto_vs_budget_pct": netto_vs_budget_pct,
        "maaned_chart":       maaned_chart,
        "sparkline":    sparkline,
        "conv_rate":    conv_rate,
        "won_count":    won_count,
        "lost_count":   lost_count,
        "leaderboard":  leaderboard,
        "teams":        teams,
        "active_team":    team,
        "active_teams":   teams_list,
        "month_label":    month_label,
        "today":          today.isoformat(),
        "cur_month":      today.month,
        "ref_month":      ref_month,
        "ref_months":     months_list,
        "ref_year":       ref_year,
        "week_label":     week_label_str,
        "week_teams":     week_teams,
        "week_saelgere":  week_saelgere,
    }


#-----------------------------------------------------------------------------------------------------------------------
#                                          YoY SAMMENLIGNINGSVÆRKTØJ
#-----------------------------------------------------------------------------------------------------------------------

def db_yoy_data(today: date, team: str | None = None,
                selected_year: int | None = None, compare_year: int | None = None,
                selected_month: str | None = None,
                date_col: str = "won_time", pipeline_filter: str | None = None):
    ref_year  = selected_year or today.year
    prev_year = compare_year if compare_year else ref_year - 1

    # ── Period label + month/quarter SQL clause ──────────────────────────────
    months_list: list[int] = []
    if selected_month in ("Q1", "Q2", "Q3", "Q4"):
        q           = int(selected_month[1])
        months_list = list(range((q - 1) * 3 + 1, q * 3 + 1))
        period_label = f"{selected_month} {ref_year} vs {selected_month} {prev_year}"
    elif selected_month:
        raw = [p.strip() for p in selected_month.split(",") if p.strip().isdigit()]
        months_list = sorted({int(p) for p in raw if 1 <= int(p) <= 12})
        if len(months_list) == 1:
            period_label = (f"{MONTH_NAMES_DA[months_list[0] - 1]} {ref_year} "
                            f"vs {MONTH_NAMES_DA[months_list[0] - 1]} {prev_year}")
        elif months_list:
            lbl = ", ".join(MONTH_NAMES_DA[m - 1] for m in months_list)
            period_label = f"{lbl} {ref_year} vs {prev_year}"
        else:
            period_label = f"Hele {ref_year} vs Hele {prev_year}"
    else:
        period_label = f"Hele {ref_year} vs Hele {prev_year}"

    if months_list:
        _months_ph    = "(" + ",".join(["%s"] * len(months_list)) + ")"
        period_sql    = "AND MONTH({}) IN " + _months_ph
        period_params = tuple(months_list)
    else:
        period_sql    = ""
        period_params = ()

    # Date range spanning both years
    both_from = date(prev_year, 1, 1)
    both_to   = date(ref_year + 1, 1, 1)

    # ── Date column ──────────────────────────────────────────────────────────
    _VALID_DATE_COLS = {"won_time", "service_activation_date"}
    if date_col not in _VALID_DATE_COLS:
        date_col = "won_time"
    d_col = f"[{date_col}]"

    # ── brands placeholder (needed by team block) ─────────────────────────────
    brands_ph = "(" + ",".join(["%s"] * len(SUBSCRIPTION_BRANDS)) + ")"

    # ── Team clause ──────────────────────────────────────────────────────────
    teams_list: list[str] = [t.strip() for t in team.split(",") if t.strip()] if team else []
    multi_team = len(teams_list) > 1

    if multi_team:
        _teams_ph          = "(" + ",".join(["%s"] * len(teams_list)) + ")"
        team_clause        = f"AND [team] IN {_teams_ph}"
        team_params        = tuple(teams_list)
        is_finans_team     = False
        is_watch_dk_team   = False
        is_watch_int_team  = False
        non_finans_exclude = ""
        sites_filter       = f"AND [sites] IN {brands_ph}"
    elif teams_list:
        team = teams_list[0]
        is_finans_team    = "FINANS" in team.upper()
        is_watch_dk_team  = "WATCH DK" in team.upper()
        is_watch_int_team = "WATCH INT" in team.upper()

        if is_finans_team:
            team_clause = """AND [owner_name] IN (
                SELECT u2.name FROM HubUsers u2
                JOIN TeamMemberships tm2 ON tm2.user_id = u2.id
                JOIN Teams t2 ON t2.id = tm2.team_id
                WHERE t2.name = %s
                AND (tm2.end_date IS NULL OR tm2.end_date >= GETDATE())
            ) AND COALESCE([sites],'') = 'FINANS DK'"""
            team_params = (team,)
        else:
            team_clause = """AND [owner_name] IN (
                SELECT u2.name FROM HubUsers u2
                JOIN TeamMemberships tm2 ON tm2.user_id = u2.id
                JOIN Teams t2 ON t2.id = tm2.team_id
                WHERE t2.name = %s
                AND (TRY_CAST(tm2.end_date AS DATE) IS NULL OR TRY_CAST(tm2.end_date AS DATE) >= CAST(GETDATE() AS DATE))
            ) AND ([team] = %s OR [team] IS NULL)"""
            team_params = (team, team)

        if is_watch_int_team:
            non_finans_exclude = "AND COALESCE([sites],'') NOT LIKE '%FINANS%'"
        elif is_watch_dk_team:
            non_finans_exclude = "AND COALESCE([sites],'') <> 'FINANS DK'"
        else:
            non_finans_exclude = ""

        sites_filter = f"AND ([sites] IN {brands_ph} OR [sites] IS NULL)"
    else:
        team               = None
        team_clause        = ""
        team_params        = ()
        is_finans_team     = False
        is_watch_dk_team   = False
        is_watch_int_team  = False
        non_finans_exclude = ""
        sites_filter       = f"AND [sites] IN {brands_ph}"

    # ── Pipeline filter ──────────────────────────────────────────────────────
    _CANCEL_KEYS = {'Cancellations', 'Cancellation', 'Opsigelser'}
    _sel_pipes   = ([p.strip() for p in pipeline_filter.split(',')]
                    if pipeline_filter and pipeline_filter != 'all' else [])
    _non_cancel  = [p for p in _sel_pipes if p not in _CANCEL_KEYS]
    _has_cancel  = any(p in _CANCEL_KEYS for p in _sel_pipes)

    if not _sel_pipes:
        won_case       = f"[pipeline_name] NOT IN {_CANCEL_PH}"
        cancel_case    = f"[pipeline_name] IN {_CANCEL_PH}"
        won_cparams    = tuple(CANCELLATION_PIPELINES)
        cancel_cparams = tuple(CANCELLATION_PIPELINES)
    elif _has_cancel and not _non_cancel:
        won_case       = f"[pipeline_name] IN {_CANCEL_PH}"
        cancel_case    = "1=0"
        won_cparams    = tuple(CANCELLATION_PIPELINES)
        cancel_cparams = ()
    elif _non_cancel and not _has_cancel:
        _pipe_ph       = "(" + ",".join(["%s"] * len(_non_cancel)) + ")"
        won_case       = f"[pipeline_name] IN {_pipe_ph}"
        cancel_case    = "1=0"
        won_cparams    = tuple(_non_cancel)
        cancel_cparams = ()
    else:  # mix: specific pipelines + cancellations
        _pipe_ph       = "(" + ",".join(["%s"] * len(_non_cancel)) + ")"
        won_case       = f"[pipeline_name] IN {_pipe_ph}"
        cancel_case    = f"[pipeline_name] IN {_CANCEL_PH}"
        won_cparams    = tuple(_non_cancel)
        cancel_cparams = tuple(CANCELLATION_PIPELINES)

    conn = get_conn()
    cur  = conn.cursor(as_dict=True)

    # ── KPI summary: GROUP BY YEAR ────────────────────────────────────────────
    # period_sql uses d_col as format arg (filled in below)
    psql = period_sql.format(d_col) if period_sql else ""
    cur.execute(f"""
        SELECT
            YEAR({d_col}) AS aar,
            ISNULL(SUM(CASE WHEN {won_case}
                THEN CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)) ELSE 0 END), 0) AS won,
            ISNULL(ABS(SUM(CASE WHEN {cancel_case}
                THEN CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)) ELSE 0 END)), 0) AS cancel,
            COUNT(CASE WHEN {won_case} THEN 1 END) AS won_count
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND {d_col} >= %s AND {d_col} < %s
          {psql}
          {sites_filter}
          {_ADM_EXCLUDE}
          {non_finans_exclude}
          {team_clause}
        GROUP BY YEAR({d_col})
    """, won_cparams + cancel_cparams + won_cparams + (both_from.isoformat(), both_to.isoformat()) + period_params + tuple(SUBSCRIPTION_BRANDS) + team_params)

    kpi_by_year = {}
    for r in cur.fetchall():
        y   = int(r["aar"])
        won = float(r["won"] or 0)
        can = float(r["cancel"] or 0)
        kpi_by_year[y] = {
            "won":       round(won, 2),
            "cancel":    round(can, 2),
            "netto":     round(won - can, 2),
            "won_count": int(r["won_count"] or 0),
        }

    def _empty_kpi():
        return {"won": 0, "cancel": 0, "netto": 0, "won_count": 0}

    curr_kpi = kpi_by_year.get(ref_year,  _empty_kpi())
    prev_kpi = kpi_by_year.get(prev_year, _empty_kpi())

    def _delta(c, p):
        diff = round(c - p, 2)
        pct  = round(diff / p * 100, 1) if p != 0 else None
        return {"diff": diff, "pct": pct}

    deltas = {
        "won":       _delta(curr_kpi["won"],       prev_kpi["won"]),
        "cancel":    _delta(curr_kpi["cancel"],    prev_kpi["cancel"]),
        "netto":     _delta(curr_kpi["netto"],     prev_kpi["netto"]),
        "won_count": _delta(curr_kpi["won_count"], prev_kpi["won_count"]),
    }

    # ── Monthly chart: all 12 months × 2 years ───────────────────────────────
    cur.execute(f"""
        SELECT
            YEAR({d_col}) AS aar,
            MONTH({d_col}) AS maaned,
            ISNULL(SUM(CASE WHEN {won_case}
                THEN CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)) ELSE 0 END), 0) AS won,
            ISNULL(ABS(SUM(CASE WHEN {cancel_case}
                THEN CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)) ELSE 0 END)), 0) AS cancel
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND {d_col} >= %s AND {d_col} < %s
          {sites_filter}
          {_ADM_EXCLUDE}
          {non_finans_exclude}
          {team_clause}
        GROUP BY YEAR({d_col}), MONTH({d_col})
        ORDER BY aar, maaned
    """, won_cparams + cancel_cparams + (both_from.isoformat(), both_to.isoformat()) + tuple(SUBSCRIPTION_BRANDS) + team_params)

    raw_monthly: dict[tuple, tuple] = {}
    for r in cur.fetchall():
        won = float(r["won"] or 0)
        can = float(r["cancel"] or 0)
        raw_monthly[(int(r["aar"]), int(r["maaned"]))] = (won, can)

    def _month_series(year):
        out = []
        for m in range(1, 13):
            won, can = raw_monthly.get((year, m), (0.0, 0.0))
            out.append({
                "maaned": MONTH_NAMES_DA[m - 1][:3],
                "won":    round(won, 2),
                "cancel": round(can, 2),
                "netto":  round(won - can, 2),
            })
        return out

    monthly_curr = _month_series(ref_year)
    monthly_prev = _month_series(prev_year)

    # ── Salesperson comparison ────────────────────────────────────────────────
    cur.execute(f"""
        SELECT
            COALESCE([owner_name], 'Ukendt') AS owner_name,
            YEAR({d_col}) AS aar,
            ISNULL(SUM(CASE WHEN {won_case}
                THEN CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)) ELSE 0 END), 0) AS won,
            ISNULL(ABS(SUM(CASE WHEN {cancel_case}
                THEN CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)) ELSE 0 END)), 0) AS cancel,
            COUNT(CASE WHEN {won_case} THEN 1 END) AS won_count
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND {d_col} >= %s AND {d_col} < %s
          {psql}
          {sites_filter}
          {_ADM_EXCLUDE}
          {non_finans_exclude}
          {team_clause}
        GROUP BY COALESCE([owner_name], 'Ukendt'), YEAR({d_col})
        ORDER BY owner_name, aar
    """, won_cparams + cancel_cparams + won_cparams + (both_from.isoformat(), both_to.isoformat()) + period_params + tuple(SUBSCRIPTION_BRANDS) + team_params)

    saelger_map: dict[str, dict] = {}
    for r in cur.fetchall():
        name = r["owner_name"]
        y    = int(r["aar"])
        won  = float(r["won"] or 0)
        can  = float(r["cancel"] or 0)
        netto = round(won - can, 2)
        if name not in saelger_map:
            saelger_map[name] = {}
        saelger_map[name][y] = {
            "won":       round(won, 2),
            "cancel":    round(can, 2),
            "netto":     netto,
            "won_count": int(r["won_count"] or 0),
        }

    saelgere = []
    for name, by_year in saelger_map.items():
        cn = by_year.get(ref_year,  {}).get("netto", 0)
        pn = by_year.get(prev_year, {}).get("netto", 0)
        diff = round(cn - pn, 2)
        pct  = round(diff / pn * 100, 1) if pn != 0 else None
        saelgere.append({
            "owner_name":    name,
            "curr_won":      by_year.get(ref_year,  {}).get("won",       0),
            "curr_cancel":   by_year.get(ref_year,  {}).get("cancel",    0),
            "curr_netto":    cn,
            "curr_count":    by_year.get(ref_year,  {}).get("won_count", 0),
            "prev_won":      by_year.get(prev_year, {}).get("won",       0),
            "prev_cancel":   by_year.get(prev_year, {}).get("cancel",    0),
            "prev_netto":    pn,
            "prev_count":    by_year.get(prev_year, {}).get("won_count", 0),
            "delta_netto":   diff,
            "delta_pct":     pct,
        })
    saelgere.sort(key=lambda x: -x["curr_netto"])

    # ── Teams list ────────────────────────────────────────────────────────────
    cur.execute("""
        SELECT DISTINCT t.name
        FROM Teams t
        JOIN TeamMemberships tm ON tm.team_id = t.id
        WHERE t.name IS NOT NULL
          AND (tm.end_date IS NULL OR TRY_CAST(tm.end_date AS DATE) >= CAST(GETDATE() AS DATE))
        ORDER BY t.name
    """)
    teams = [r["name"] for r in cur.fetchall()]

    conn.close()
    return {
        "ref_year":     ref_year,
        "prev_year":    prev_year,
        "period_label": period_label,
        "curr":         curr_kpi,
        "prev":         prev_kpi,
        "deltas":       deltas,
        "monthly_curr": monthly_curr,
        "monthly_prev": monthly_prev,
        "saelgere":     saelgere,
        "teams":        teams,
    }


#-----------------------------------------------------------------------------------------------------------------------
#                                          DET NYE DASHBOARD FOR SÆLGER
#-----------------------------------------------------------------------------------------------------------------------

def db_saelger_data(today: date, owner_name: str, team: str | None = None,
                     selected_year: int | None = None, selected_month: str | None = None,
                     date_col: str = "won_time"):
    # Reference period — brug valgt år hvis angivet, ellers aktuelt år
    ref_year = selected_year or today.year

    # ── Periode-parsing (samme som manager): enkelt måned, multi (komma-sep) eller Q1-Q4 ──
    months_list: list[int] = []
    if selected_month in ("Q1", "Q2", "Q3", "Q4"):
        q = int(selected_month[1])
        months_list = list(range((q - 1) * 3 + 1, q * 3 + 1))
    elif selected_month:
        raw = [p.strip() for p in str(selected_month).split(",") if p.strip().isdigit()]
        months_list = sorted({int(p) for p in raw if 1 <= int(p) <= 12})

    year_from  = date(ref_year, 1, 1)
    year_to    = date(ref_year + 1, 1, 1)

    # _period(col) → WHERE-fragment + params. Ved multi-måned bruges MONTH(col) IN (...),
    # ellers hele året. Bruges af alle periode-afhængige queries herunder.
    if months_list:
        _months_ph = "(" + ",".join(["%s"] * len(months_list)) + ")"
        def _period(col: str):
            return f"YEAR({col}) = %s AND MONTH({col}) IN {_months_ph}", (ref_year, *months_list)
    else:
        def _period(col: str):
            return f"{col} >= %s AND {col} < %s", (year_from.isoformat(), year_to.isoformat())

    # Sparkline/salg i dag bruger altid rigtig today
    week_start = today - timedelta(days=today.weekday())
    week_end   = week_start + timedelta(days=7)

    brands_ph   = "(" + ",".join(["%s"] * len(SUBSCRIPTION_BRANDS)) + ")"
    team_clause = "AND [team] = %s" if team else ""
    team_params = (team,) if team else ()
    # Dato-kolonne: won_time (matcher Pipedrive) eller service_activation_date
    _VALID_DATE_COLS = {"won_time", "service_activation_date"}
    if date_col not in _VALID_DATE_COLS:
        date_col = "won_time"
    d_col = f"[{date_col}]"

    # Forudberegnede periode-fragmenter for de kolonner queries herunder bruger.
    _p_dcol_clause,   _p_dcol_params   = _period(d_col)
    _p_budget_clause, _p_budget_params = _period("[BudgetDate]")
    _p_close_clause,  _p_close_params  = _period("[expected_close_date]")

    # Samme periode sidste år (til YoY-sammenligning).
    if months_list:
        _ly_clause = f"YEAR({d_col}) = %s AND MONTH({d_col}) IN {_months_ph}"
        _ly_params = (ref_year - 1, *months_list)
    else:
        _ly_clause = f"{d_col} >= %s AND {d_col} < %s"
        _ly_params = (date(ref_year - 1, 1, 1).isoformat(), date(ref_year, 1, 1).isoformat())

    # Sites-filter: tillad NULL så Marketwire/Banner-deals (uden site tag)
    # ogsaa kommer med. Matcher modul_perf manager's single-team mønster
    # (linje 339) — uden NULL-fallback'en forsvinder marketwire-deals helt.
    _sites_filter = f"([sites] IN {brands_ph} OR [sites] IS NULL)"

    conn = get_conn()
    cur  = conn.cursor(as_dict=True)

    # Q1: Won amount/count for reference-måneden
    cur.execute(f"""
        SELECT
            SUM((CASE
                WHEN [currency] IN ('NOK','SEK') THEN ABS(CAST([value] AS DECIMAL(18,2)))
                WHEN [value_dkk] IS NOT NULL THEN ABS(CAST([value_dkk] AS DECIMAL(18,2)))
                WHEN [currency] = 'EUR' THEN ABS(CAST([value] * 7.46 AS DECIMAL(18,2)))
                WHEN [currency] = 'SEK' THEN ABS(CAST([value] * 0.65 AS DECIMAL(18,2)))
                WHEN [currency] = 'NOK' THEN ABS(CAST([value] * 0.63 AS DECIMAL(18,2)))
                WHEN [currency] = 'USD' THEN ABS(CAST([value] * 6.90 AS DECIMAL(18,2)))
                ELSE ABS(CAST([value] AS DECIMAL(18,2)))
            END) * (CASE WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser') THEN -1 ELSE 1 END)) AS net_amount,
            COUNT(CASE WHEN [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser') THEN 1 END) AS won_count,
            SUM(CASE WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
                THEN ABS(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2))) ELSE 0 END) AS cancel_amount
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND {_p_dcol_clause}
          AND [owner_name] = %s
          AND {_sites_filter}
          {_ADM_EXCLUDE}
          {team_clause}
    """, _p_dcol_params + (owner_name,) + tuple(SUBSCRIPTION_BRANDS) + team_params)
    res           = cur.fetchone() or {}
    won_amount    = float(res.get("net_amount", 0) or 0)
    won_count     = int(res.get("won_count", 0) or 0)
    cancel_amount = abs(float(res.get("cancel_amount", 0) or 0))

    cur.execute(f"""
        SELECT [Team], ISNULL(SUM([BudgetAmount]),0) AS budget
        FROM [dbo].[SalespersonBudget]
        WHERE {_p_budget_clause}
          AND [Owner] = %s
        GROUP BY [Team]
        ORDER BY [Team]
    """, _p_budget_params + (owner_name,))
    budget_rows = cur.fetchall()
    budget_by_team_raw = {r["Team"]: float(r["budget"] or 0) for r in budget_rows if r["Team"]}
    # Filtrér på valgt team hvis sat
    if team:
        budget_by_team_raw = {k: v for k, v in budget_by_team_raw.items() if k == team}
    budget = sum(budget_by_team_raw.values())

    cur.execute(f"""
        SELECT CAST({d_col} AS DATE) AS dag,
               ISNULL(SUM(CASE
                   WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
                   THEN -ABS(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)))
                   ELSE ABS(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)))
               END),0) AS total
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND {d_col} >= %s AND {d_col} < %s
          AND [owner_name] = %s
          AND {_sites_filter}
          {_ADM_EXCLUDE}
        GROUP BY CAST({d_col} AS DATE)
    """, (week_start.isoformat(), week_end.isoformat(), owner_name) + tuple(SUBSCRIPTION_BRANDS))
    spark_raw = {str(r["dag"]): float(r["total"] or 0) for r in cur.fetchall()}
    sparkline = [{"dag": (week_start + timedelta(days=i)).isoformat(),
                  "total": round(spark_raw.get((week_start + timedelta(days=i)).isoformat(), 0), 2)}
                 for i in range(7)]

    cur.execute(f"""
        SELECT ISNULL(SUM(CASE
            WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
            THEN -ABS(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)))
            ELSE ABS(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)))
        END),0) AS total
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND {d_col} >= %s AND {d_col} < %s
          AND [owner_name] = %s
          AND {_sites_filter}
          {_ADM_EXCLUDE}
    """, (today.isoformat(), (today + timedelta(days=1)).isoformat(), owner_name) + tuple(SUBSCRIPTION_BRANDS))
    salg_dag = float((cur.fetchone() or {}).get("total", 0) or 0)

    cur.execute(f"""
        SELECT ISNULL(SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2))),0) AS pipeline_value,
               COUNT(*) AS pipeline_count
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='open' AND [pipeline_name]<>'Web Sale'
          AND {_p_close_clause}
          AND [owner_name] = %s
          AND [sites] IN {brands_ph}
          AND COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) > 0
          {_ADM_EXCLUDE}
          {team_clause}
    """, _p_close_params + (owner_name,) + tuple(SUBSCRIPTION_BRANDS) + team_params)
    pipe_row       = cur.fetchone() or {}
    pipeline_value = float(pipe_row.get("pipeline_value", 0) or 0)
    pipeline_count = int(pipe_row.get("pipeline_count", 0) or 0)

    cur.execute(f"""
        SELECT [pipeline_name], COUNT(*) AS antal
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='open' AND [pipeline_name]<>'Web Sale'
          AND {_p_close_clause}
          AND [owner_name] = %s
          AND {_sites_filter}
          AND COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) > 0
          {_ADM_EXCLUDE}
          {team_clause}
        GROUP BY [pipeline_name]
        ORDER BY antal DESC
    """, _p_close_params + (owner_name,) + tuple(SUBSCRIPTION_BRANDS) + team_params)
    pipeline_fordeling = [{"navn": r["pipeline_name"] or "Ukendt", "antal": int(r["antal"])} for r in (cur.fetchall() or [])]

    cur.execute(f"""
        SELECT ISNULL(SUM(CASE
            WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
            THEN -ABS(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)))
            ELSE ABS(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)))
        END),0) AS won
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND {_ly_clause}
          AND [owner_name] = %s
          AND {_sites_filter}
          {_ADM_EXCLUDE}
          {team_clause}
    """, _ly_params + (owner_name,) + tuple(SUBSCRIPTION_BRANDS) + team_params)
    ly_won = float((cur.fetchone() or {}).get("won", 0) or 0)

    cur.execute(f"""
        SELECT MONTH({d_col}) AS maaned,
               ISNULL(SUM(CASE
                   WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
                   THEN -ABS(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)))
                   ELSE ABS(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)))
               END),0) AS won
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND {d_col} >= %s AND {d_col} < %s
          AND [owner_name] = %s
          AND {_sites_filter}
          {_ADM_EXCLUDE}
          {team_clause}
        GROUP BY MONTH({d_col})
        ORDER BY maaned
    """, (year_from.isoformat(), year_to.isoformat(), owner_name) + tuple(SUBSCRIPTION_BRANDS) + team_params)
    maaned_raw = {r["maaned"]: float(r["won"] or 0) for r in cur.fetchall()}
    maaned_chart = [{"maaned": MONTH_NAMES_DA[m - 1][:3], "won": round(maaned_raw.get(m, 0), 2)}
                    for m in range(1, 13)]

    cur.execute(f"""
        SELECT [owner_name],
               SUM(CASE WHEN [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
                   THEN CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)) ELSE 0 END) AS won,
               SUM(CASE WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
                   THEN CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)) ELSE 0 END) AS cancel
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND {_p_dcol_clause}
          AND {_sites_filter}
          {team_clause}
        GROUP BY [owner_name]
    """, _p_dcol_params + tuple(SUBSCRIPTION_BRANDS) + team_params)
    leaderboard = sorted([
        {"owner_name": r["owner_name"],
         "won_amount":    float(r["won"]    or 0),
         "cancel_amount": float(r["cancel"] or 0)}
        for r in cur.fetchall()
    ], key=lambda x: -x["won_amount"])

    # Q9: Deals — filtreret på valgt periode + team
    cur.execute(f"""
        SELECT
            [title], [sites], [org_name],
            ABS(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2))) AS value,
            CONVERT(NVARCHAR(10), {d_col}, 23) AS event_date,
            [deal_type],
            [status],
            [pipeline_name]
        FROM [dbo].[PipedriveDeals]
        WHERE [status] = 'won'
          AND [pipeline_name] <> 'Web Sale'
          AND [owner_name] = %s
          AND {_sites_filter}
          AND {_p_dcol_clause}
          {_ADM_EXCLUDE}
          {team_clause}
        ORDER BY {d_col} DESC
    """, (owner_name,) + tuple(SUBSCRIPTION_BRANDS) + _p_dcol_params + team_params)

    seneste_deals = [{
        "title": r["title"] or "(Uden titel)",
        "site": r["sites"] or "—",
        "org_name": r["org_name"] or "—",
        "value": float(r["value"] or 0),
        "dato": r["event_date"] or "—",
        "status": "Vundet",
        "is_cancel": r["pipeline_name"] in ('Cancellation', 'Cancellations', 'Opsigelser')
    } for r in (cur.fetchall() or [])]

    # Won-beløb per team (til budget-breakdown)
    cur.execute(f"""
        SELECT [team],
               ISNULL(SUM((CASE
                   WHEN [currency] IN ('NOK','SEK') THEN ABS(CAST([value] AS DECIMAL(18,2)))
                   WHEN [value_dkk] IS NOT NULL THEN ABS(CAST([value_dkk] AS DECIMAL(18,2)))
                   WHEN [currency] = 'EUR' THEN ABS(CAST([value] * 7.46 AS DECIMAL(18,2)))
                   WHEN [currency] = 'SEK' THEN ABS(CAST([value] * 0.65 AS DECIMAL(18,2)))
                   WHEN [currency] = 'NOK' THEN ABS(CAST([value] * 0.63 AS DECIMAL(18,2)))
                   WHEN [currency] = 'USD' THEN ABS(CAST([value] * 6.90 AS DECIMAL(18,2)))
                   ELSE ABS(CAST([value] AS DECIMAL(18,2)))
               END) * (CASE WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser') THEN -1 ELSE 1 END)),0) AS won
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND {_p_dcol_clause}
          AND [owner_name] = %s
          AND {_sites_filter}
          {_ADM_EXCLUDE}
          {team_clause}
        GROUP BY [team]
    """, _p_dcol_params + (owner_name,) + tuple(SUBSCRIPTION_BRANDS) + team_params)
    won_by_team_raw = {r["team"]: float(r["won"] or 0) for r in cur.fetchall() if r["team"]}

    # ── Widget: Pipeline-fordeling af tilvækst ───────────────────────────────
    # Vundne deals grupperet pr. pipeline (Newbizz, Customer, Company Trial,
    # Cancellation osv.) for valgt periode. Opsigelser tæller negativt så netto
    # tilvækst pr. pipeline matcher resten af dashboardet.
    cur.execute(f"""
        SELECT [pipeline_name],
               ISNULL(SUM(CASE
                   WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
                   THEN -ABS(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)))
                   ELSE ABS(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)))
               END),0) AS total,
               COUNT(*) AS antal
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND {_p_dcol_clause}
          AND [owner_name] = %s
          AND {_sites_filter}
          {_ADM_EXCLUDE}
          {team_clause}
        GROUP BY [pipeline_name]
        ORDER BY total DESC
    """, _p_dcol_params + (owner_name,) + tuple(SUBSCRIPTION_BRANDS) + team_params)
    tilvaekst_fordeling = [{
        "navn":  r["pipeline_name"] or "Ukendt",
        "total": round(float(r["total"] or 0), 2),
        "antal": int(r["antal"] or 0),
    } for r in (cur.fetchall() or [])]

    # ── Widget: Konverteringsrate (won vs. lost) baseret på close_time ────────
    # Andel af lukkede deals (won + lost) i perioden der blev vundet. Bruger
    # close_time som datokolonne. Beregnes KUN ud fra deals i pipelinene
    # Customer, Newbizz og Company Trial (case-insensitivt).
    _p_close_time_clause, _p_close_time_params = _period("[close_time]")
    cur.execute(f"""
        SELECT
            COUNT(CASE WHEN [status]='won' THEN 1 END) AS won_count,
            COUNT(CASE WHEN [status]='lost' THEN 1 END) AS lost_count
        FROM [dbo].[PipedriveDeals]
        WHERE ([status]='won' OR [status]='lost')
          AND [pipeline_name] <> 'Web Sale'
          AND UPPER([pipeline_name]) IN {_CONV_PH}
          AND {_p_close_time_clause}
          AND [owner_name] = %s
          AND {_sites_filter}
          {_ADM_EXCLUDE}
          {team_clause}
    """, tuple(CONVERSION_PIPELINES_UPPER) + _p_close_time_params + (owner_name,) + tuple(SUBSCRIPTION_BRANDS) + team_params)
    conv_row   = cur.fetchone() or {}
    conv_won   = int(conv_row.get("won_count", 0) or 0)
    conv_lost  = int(conv_row.get("lost_count", 0) or 0)
    conv_total = conv_won + conv_lost
    conv_rate  = round(conv_won / conv_total * 100, 1) if conv_total > 0 else None

    # Konverteringsrate fordelt pr. måned for hele ref_year (til månedsbjælker).
    # Samme pipeline-afgrænsning som periode-totalen: Customer, Newbizz, Company Trial.
    cur.execute(f"""
        SELECT MONTH([close_time]) AS maaned,
            COUNT(CASE WHEN [status]='won' THEN 1 END) AS won_count,
            COUNT(CASE WHEN [status]='lost' THEN 1 END) AS lost_count
        FROM [dbo].[PipedriveDeals]
        WHERE ([status]='won' OR [status]='lost')
          AND [pipeline_name] <> 'Web Sale'
          AND UPPER([pipeline_name]) IN {_CONV_PH}
          AND [close_time] >= %s AND [close_time] < %s
          AND [owner_name] = %s
          AND {_sites_filter}
          {_ADM_EXCLUDE}
          {team_clause}
        GROUP BY MONTH([close_time])
        ORDER BY maaned
    """, tuple(CONVERSION_PIPELINES_UPPER) + (year_from.isoformat(), year_to.isoformat(), owner_name) + tuple(SUBSCRIPTION_BRANDS) + team_params)
    conv_raw = {r["maaned"]: r for r in cur.fetchall()}
    conv_chart = []
    for m in range(1, 13):
        r   = conv_raw.get(m, {"won_count": 0, "lost_count": 0})
        w   = int(r["won_count"]  or 0)
        l   = int(r["lost_count"] or 0)
        tot = w + l
        conv_chart.append({
            "maaned":     MONTH_NAMES_DA[m - 1][:3],
            "won_count":  w,
            "lost_count": l,
            "total":      tot,
            "rate":       round(w / tot * 100, 1) if tot > 0 else None,
        })

    # Saml alle kendte teams fra budget + salg
    all_teams = sorted(set(list(budget_by_team_raw.keys()) + list(won_by_team_raw.keys())))
    budget_by_team = [
        {
            "team":    t,
            "budget":  round(budget_by_team_raw.get(t, 0), 2),
            "won":     round(won_by_team_raw.get(t, 0), 2),
            "pct":     round(won_by_team_raw.get(t, 0) / budget_by_team_raw[t] * 100, 1)
                       if budget_by_team_raw.get(t, 0) > 0 else None,
        }
        for t in all_teams
    ]

    conn.close()

    vs_budget_pct = round(won_amount / budget * 100, 1) if budget > 0 else None
    yoy_pct       = round((won_amount - ly_won) / abs(ly_won) * 100, 1) if ly_won else None

    if selected_month in ("Q1", "Q2", "Q3", "Q4"):
        month_label = f"{selected_month} {ref_year}"
    elif len(months_list) == 1:
        month_label = f"{MONTH_NAMES_DA[months_list[0] - 1]} {ref_year}"
    elif months_list:
        month_label = ", ".join(MONTH_NAMES_DA[m - 1] for m in months_list) + f" {ref_year}"
    else:
        month_label = f"Hele Året {ref_year}"

    return {
        "won_amount":      round(won_amount, 2),
        "won_count":       won_count,
        "cancel_amount":   round(cancel_amount, 2),
        "budget":          round(budget, 2),
        "vs_budget_pct":   vs_budget_pct,
        "salg_dag":        round(salg_dag, 2),
        "sparkline":       sparkline,
        "pipeline_value":     round(pipeline_value, 2),
        "pipeline_count":     pipeline_count,
        "pipeline_fordeling": pipeline_fordeling,
        "tilvaekst_fordeling": tilvaekst_fordeling,
        "conv_rate":          conv_rate,
        "conv_won":           conv_won,
        "conv_lost":          conv_lost,
        "conv_total":         conv_total,
        "conv_chart":         conv_chart,
        "ly_won":          round(ly_won, 2),
        "yoy_pct":         yoy_pct,
        "maaned_chart":    maaned_chart,
        "leaderboard":     leaderboard,
        "seneste_deals":   seneste_deals,
        "budget_by_team":  budget_by_team,
        "owner_name":      owner_name,
        "month_label":     month_label,
        "ref_months":      months_list,
        "ref_year":        ref_year,
        "today":           today.isoformat(),
    }


def db_manager_saelger_deals(owner_name: str, year: int, month: int,
                              date_col: str = "won_time",
                              site: str | None = None,
                              pipeline_type: str | None = None):
    """Hent deals for en specifik sælger til manager deal-historik."""
    valid_cols = {"won_time", "close_time", "service_activation_date"}
    if date_col not in valid_cols:
        date_col = "won_time"

    from datetime import date as _date
    month_from = _date(year, month, 1)
    next_month = month % 12 + 1
    next_year  = year + (1 if month == 12 else 0)
    month_to   = _date(next_year, next_month, 1)

    brands_ph = "(" + ",".join(["%s"] * len(SUBSCRIPTION_BRANDS)) + ")"

    site_clause     = "AND [sites] = %s"          if site          else ""
    pipeline_clause = "AND [pipeline_name] = %s"  if pipeline_type else ""
    site_params     = (site,)          if site          else ()
    pipeline_params = (pipeline_type,) if pipeline_type else ()

    try:
        conn = get_conn()
        cur  = conn.cursor(as_dict=True)
        cur.execute(f"""
            SELECT
                [title], [sites], [org_name],
                ABS(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2))) AS value,
                CONVERT(NVARCHAR(10), [{date_col}], 23) AS event_date,
                [pipeline_name],
                CASE
                    WHEN COALESCE([administrativ],'') = 'ja' THEN 1
                    WHEN UPPER(LTRIM([title])) LIKE 'ADMINISTRATIV%' THEN 1
                    WHEN UPPER(LTRIM([title])) LIKE 'ADM %' THEN 1
                    WHEN COALESCE([deal_type],'') = 'Rapport' THEN 1
                    ELSE 0
                END AS is_admin
            FROM [dbo].[PipedriveDeals]
            WHERE [status] = 'won'
              AND [pipeline_name] <> 'Web Sale'
              AND [owner_name] = %s
              AND [sites] IN {brands_ph}
              AND [{date_col}] >= %s AND [{date_col}] < %s
              {site_clause}
              {pipeline_clause}
            ORDER BY [{date_col}] DESC
        """, (owner_name,) + tuple(SUBSCRIPTION_BRANDS)
             + (month_from.isoformat(), month_to.isoformat())
             + site_params + pipeline_params)

        deals = [{
            "title":     r["title"] or "(Uden titel)",
            "site":      r["sites"] or "—",
            "org_name":  r["org_name"] or "—",
            "value":     float(r["value"] or 0),
            "dato":      r["event_date"] or "—",
            "is_cancel": r["pipeline_name"] in ('Cancellation', 'Cancellations', 'Opsigelser'),
            "is_admin":  bool(r["is_admin"]),
        } for r in (cur.fetchall() or [])]
        conn.close()
        return deals
    except Exception:
        traceback.print_exc()
        return []


def db_manager_saelger_filters(owner_name: str):
    """Hent tilgængelige sites og pipeline-typer for en sælger (til filter-dropdowns)."""
    brands_ph = "(" + ",".join(["%s"] * len(SUBSCRIPTION_BRANDS)) + ")"
    try:
        conn = get_conn()
        cur  = conn.cursor(as_dict=True)

        # Sites fra alle deals (vundne + åbne)
        cur.execute(f"""
            SELECT DISTINCT [sites] AS site
            FROM [dbo].[PipedriveDeals]
            WHERE [owner_name] = %s
              AND [sites] IN {brands_ph}
              AND [sites] IS NOT NULL
            ORDER BY [sites]
        """, (owner_name,) + tuple(SUBSCRIPTION_BRANDS))
        sites = [r["site"] for r in (cur.fetchall() or [])]

        # Pipeline-typer fra vundne deals
        cur.execute(f"""
            SELECT DISTINCT [pipeline_name]
            FROM [dbo].[PipedriveDeals]
            WHERE [owner_name] = %s AND [status] = 'won'
              AND [pipeline_name] IS NOT NULL AND [pipeline_name] <> 'Web Sale'
              AND [sites] IN {brands_ph}
            ORDER BY [pipeline_name]
        """, (owner_name,) + tuple(SUBSCRIPTION_BRANDS))
        deal_pipelines = [r["pipeline_name"] for r in (cur.fetchall() or [])]

        # Pipeline-typer fra åbne deals
        cur.execute(f"""
            SELECT DISTINCT [pipeline_name]
            FROM [dbo].[PipedriveDeals]
            WHERE [owner_name] = %s AND [status] = 'open'
              AND [pipeline_name] IS NOT NULL AND [pipeline_name] <> 'Web Sale'
              AND [sites] IN {brands_ph}
            ORDER BY [pipeline_name]
        """, (owner_name,) + tuple(SUBSCRIPTION_BRANDS))
        open_pipelines = [r["pipeline_name"] for r in (cur.fetchall() or [])]

        conn.close()
        return {
            "sites":           sites,
            "deal_pipelines":  sorted(set(deal_pipelines)),
            "open_pipelines":  sorted(set(open_pipelines)),
        }
    except Exception:
        traceback.print_exc()
        return {"sites": [], "deal_pipelines": [], "open_pipelines": []}


def db_manager_saelger_pipeline(owner_name: str, year: int | None = None,
                                 month: str | int | None = None, site: str | None = None,
                                 pipeline_type: str | None = None):
    """Hent åbne pipeline deals for en sælger (til manager-visning).

    month understøtter enkelt måned (3), multi (komma-sep "1,2,3") og Q1-Q4.
    """
    brands_ph = "(" + ",".join(["%s"] * len(SUBSCRIPTION_BRANDS)) + ")"

    # ── Periode-parsing (samme mønster som db_saelger_data) ──
    months_list: list[int] = []
    if month in ("Q1", "Q2", "Q3", "Q4"):
        q = int(str(month)[1])
        months_list = list(range((q - 1) * 3 + 1, q * 3 + 1))
    elif month not in (None, ""):
        raw = [p.strip() for p in str(month).split(",") if p.strip().isdigit()]
        months_list = sorted({int(p) for p in raw if 1 <= int(p) <= 12})

    # Bygger valgfrie klausuler
    extra_clauses = []
    extra_params  = []

    if year and months_list:
        _mph = "(" + ",".join(["%s"] * len(months_list)) + ")"
        extra_clauses.append(f"YEAR([expected_close_date]) = %s AND MONTH([expected_close_date]) IN {_mph}")
        extra_params += [year, *months_list]
    elif year:
        extra_clauses.append("YEAR([expected_close_date]) = %s")
        extra_params.append(year)

    if site:
        extra_clauses.append("[sites] = %s")
        extra_params.append(site)

    if pipeline_type:
        extra_clauses.append("[pipeline_name] = %s")
        extra_params.append(pipeline_type)

    extra_sql = ("AND " + " AND ".join(extra_clauses)) if extra_clauses else ""

    try:
        conn = get_conn()
        cur  = conn.cursor(as_dict=True)
        cur.execute(f"""
            SELECT
                [title], [sites], [org_name],
                ABS(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2))) AS value,
                [pipeline_name],
                CONVERT(NVARCHAR(10), [expected_close_date], 23) AS expected_close
            FROM [dbo].[PipedriveDeals]
            WHERE [status] = 'open'
              AND [pipeline_name] <> 'Web Sale'
              AND [owner_name] = %s
              AND [sites] IN {brands_ph}
              AND COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) > 0
              {_ADM_EXCLUDE}
              {extra_sql}
            ORDER BY CASE WHEN [expected_close_date] IS NULL THEN 1 ELSE 0 END,
                     [expected_close_date] ASC
        """, (owner_name,) + tuple(SUBSCRIPTION_BRANDS) + tuple(extra_params))

        rows = cur.fetchall() or []
        conn.close()
        return [{
            "title":          r["title"] or "(Uden titel)",
            "site":           r["sites"] or "—",
            "org_name":       r["org_name"] or "—",
            "value":          float(r["value"] or 0),
            "pipeline":       r["pipeline_name"] or "—",
            "expected_close": r["expected_close"] or "—",
        } for r in rows]
    except Exception:
        traceback.print_exc()
        return []


def db_saelger_meta(owner_name: str):
    conn = get_conn()
    cur  = conn.cursor(as_dict=True)

    cur.execute("""
        SELECT DISTINCT YEAR([won_time]) AS yr
        FROM [dbo].[PipedriveDeals]
        WHERE [owner_name] = %s AND [won_time] IS NOT NULL
        ORDER BY yr DESC
    """, (owner_name,))
    years = [r["yr"] for r in cur.fetchall()]

    cur.execute("""
        SELECT DISTINCT [team]
        FROM [dbo].[PipedriveDeals]
        WHERE [owner_name] = %s AND [team] IS NOT NULL AND [team] <> ''
        ORDER BY [team]
    """, (owner_name,))
    teams = [r["team"] for r in cur.fetchall()]

    conn.close()
    return {"years": years, "teams": teams}


def db_saelger_available_owners():
    """Returnér aktive brugere fra HubUsers — bruges af admin-vælgeren på
    sælger-dashboardet. PipedriveDeals.owner_name indeholder også
    fratrådte medarbejdere, så vi filtrerer på HubUsers.is_active=1 for kun at
    vise nuværende ansatte."""
    try:
        conn = get_conn()
        cur  = conn.cursor(as_dict=True)
        cur.execute("""
            SELECT [name]
            FROM [dbo].[HubUsers]
            WHERE [is_active] = 1
              AND [name] IS NOT NULL AND [name] <> ''
            ORDER BY [name]
        """)
        owners = [r["name"] for r in (cur.fetchall() or [])]
        conn.close()
        return owners
    except Exception:
        traceback.print_exc()
        return []


#-----------------------------------------------------------------------------------------------------------------------
#                                          DET NYE DASHBOARD FOR LEDER
#-----------------------------------------------------------------------------------------------------------------------

def db_afdelingsleder_data(year: int, month: int | None = None):
    real_today = date.today()

    # Periode: specifik måned eller hele året
    if month:
        month_from = date(year, month, 1)
        next_m     = month % 12 + 1
        next_y     = year + (1 if month == 12 else 0)
        month_to   = date(next_y, next_m, 1)
        month_label = f"{MONTH_NAMES_DA[month - 1]} {year}"
        # LY: samme måned sidste år
        ly_from = date(year - 1, month, 1)
        ly_to   = date(year - 1 + (1 if month == 12 else 0), next_m, 1)
        # Forecast: valgt måned
        fc_year, fc_month = year, month
        # Churn chart: highlight valgt måned, grey future relative til i dag
        chart_ref_month = month
    else:
        month_from  = date(year, 1, 1)
        month_to    = date(year + 1, 1, 1)
        month_label = str(year)
        # LY: for indeværende år → YTD-sammenligning (Jan → nuværende måned)
        # For historiske år → sammenlign fuldt år med fuldt foregående år
        if year == real_today.year:
            ly_from   = date(year - 1, 1, 1)
            ly_next_m = real_today.month % 12 + 1
            ly_next_y = year - 1 + (1 if real_today.month == 12 else 0)
            ly_to     = date(ly_next_y, ly_next_m, 1)
        else:
            ly_from = date(year - 1, 1, 1)
            ly_to   = date(year, 1, 1)
        # Churn chart: highlight nuværende måned (eller dec hvis historisk år)
        chart_ref_month = real_today.month if year == real_today.year else 12

    year_from = date(year, 1, 1)
    year_to   = date(year + 1, 1, 1)

    brands_ph  = "(" + ",".join(["%s"] * len(SUBSCRIPTION_BRANDS)) + ")"
    sub_filter = f"AND [sites] IN {brands_ph}"
    sub_params = tuple(SUBSCRIPTION_BRANDS)

    conn = get_conn()
    cur  = conn.cursor(as_dict=True)

    cur.execute(f"""
        SELECT ISNULL(SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2))),0) AS revenue
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
          AND [service_activation_date] >= %s AND [service_activation_date] < %s
          AND COALESCE([administrativ],'') <> 'ja'
          AND UPPER(LTRIM([title])) NOT LIKE 'ADMINISTRATIV%'
          AND UPPER(LTRIM([title])) NOT LIKE 'ADM %'
          AND COALESCE([deal_type],'') <> 'Rapport'
          {sub_filter}
    """, (month_from.isoformat(), month_to.isoformat()) + sub_params)
    revenue_maaned = float((cur.fetchone() or {}).get("revenue", 0) or 0)

    cur.execute(f"""
        SELECT ISNULL(SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2))),0) AS cancel
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
          AND [service_activation_date] >= %s AND [service_activation_date] < %s
          AND COALESCE([administrativ],'') <> 'ja'
          AND UPPER(LTRIM([title])) NOT LIKE 'ADMINISTRATIV%'
          AND UPPER(LTRIM([title])) NOT LIKE 'ADM %'
          {sub_filter}
    """, (month_from.isoformat(), month_to.isoformat()) + sub_params)
    cancel_maaned = abs(float((cur.fetchone() or {}).get("cancel", 0) or 0))
    netto_maaned  = revenue_maaned - cancel_maaned

    cur.execute("""
        SELECT ISNULL(SUM([BudgetAmount]),0) AS budget
        FROM [dbo].[BudgetsIntoMedia]
        WHERE [BudgetDate] >= %s AND [BudgetDate] < %s
    """, (month_from.isoformat(), month_to.isoformat()))
    budget_maaned = float((cur.fetchone() or {}).get("budget", 0) or 0)

    cur.execute(f"""
        SELECT
            ISNULL(SUM(CASE WHEN [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
                THEN CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)) ELSE 0 END),0) AS won,
            ISNULL(SUM(CASE WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
                THEN CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)) ELSE 0 END),0) AS cancel
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND [service_activation_date] >= %s AND [service_activation_date] < %s
          AND COALESCE([administrativ],'') <> 'ja'
          AND UPPER(LTRIM([title])) NOT LIKE 'ADMINISTRATIV%'
          AND UPPER(LTRIM([title])) NOT LIKE 'ADM %'
          AND COALESCE([deal_type],'') <> 'Rapport'
          {sub_filter}
    """, (ly_from.isoformat(), ly_to.isoformat()) + sub_params)
    ly_row   = cur.fetchone() or {}
    ly_netto = float(ly_row.get("won", 0) or 0) - abs(float(ly_row.get("cancel", 0) or 0))

    cur.execute(f"""
        SELECT
            MONTH([service_activation_date]) AS maaned,
            ISNULL(SUM(CASE WHEN [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
                THEN CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)) ELSE 0 END),0) AS won,
            ISNULL(SUM(CASE WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
                THEN CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)) ELSE 0 END),0) AS cancel,
            COUNT(CASE WHEN [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser') THEN 1 END) AS won_count,
            COUNT(CASE WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser') THEN 1 END) AS cancel_count
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND [service_activation_date] >= %s AND [service_activation_date] < %s
          AND COALESCE([administrativ],'') <> 'ja'
          AND UPPER(LTRIM([title])) NOT LIKE 'ADMINISTRATIV%'
          AND UPPER(LTRIM([title])) NOT LIKE 'ADM %'
          AND COALESCE([deal_type],'') <> 'Rapport'
          {sub_filter}
        GROUP BY MONTH([service_activation_date])
        ORDER BY maaned
    """, (year_from.isoformat(), year_to.isoformat()) + sub_params)
    churn_raw = {r["maaned"]: r for r in cur.fetchall()}
    churn_chart = []
    for m in range(1, 13):
        r          = churn_raw.get(m, {"won": 0, "cancel": 0, "won_count": 0, "cancel_count": 0})
        won        = float(r["won"] or 0)
        can        = abs(float(r["cancel"] or 0))
        won_cnt    = int(r["won_count"]    or 0)
        cancel_cnt = int(r["cancel_count"] or 0)
        churn_chart.append({
            "maaned":       MONTH_NAMES_DA[m - 1][:3],
            "won":          round(won, 2),
            "cancel":       round(can, 2),
            "netto":        round(won - can, 2),
            "won_count":    won_cnt,
            "cancel_count": cancel_cnt,
            "netto_count":  won_cnt - cancel_cnt,
        })

    # Automatisk forecast: vundet netto + vægtet åben pipeline for perioden.
    # weighted_value = Pipedrive's eget sandsynlighedsvægtede felt (value × stage-probability).
    # Vi inkluderer kun deals med positiv value og ekskluderer Rapport og Web Sale.
    cur.execute(f"""
        SELECT
            ISNULL(SUM(CASE
                WHEN [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
                 AND CAST(COALESCE([weighted_value],[value_dkk],[value],0) AS DECIMAL(18,2)) > 0
                THEN CAST(COALESCE([weighted_value],[value_dkk],[value],0) AS DECIMAL(18,2))
                ELSE 0 END), 0) AS pipeline_won,
            ISNULL(SUM(CASE
                WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
                 AND ABS(CAST(COALESCE([weighted_value],[value_dkk],[value],0) AS DECIMAL(18,2))) > 0
                THEN ABS(CAST(COALESCE([weighted_value],[value_dkk],[value],0) AS DECIMAL(18,2)))
                ELSE 0 END), 0) AS pipeline_cancel
        FROM [dbo].[PipedriveDeals]
        WHERE [status] = 'open'
          AND [pipeline_name] <> 'Web Sale'
          AND COALESCE([administrativ],'') <> 'ja'
          AND UPPER(LTRIM([title])) NOT LIKE 'ADMINISTRATIV%'
          AND UPPER(LTRIM([title])) NOT LIKE 'ADM %'
          AND COALESCE([deal_type],'') <> 'Rapport'
          AND [expected_close_date] >= %s AND [expected_close_date] < %s
          {sub_filter}
    """, (month_from.isoformat(), month_to.isoformat()) + sub_params)
    pipe_row       = cur.fetchone() or {}
    pipeline_won    = float(pipe_row.get("pipeline_won",    0) or 0)
    pipeline_cancel = float(pipe_row.get("pipeline_cancel", 0) or 0)
    forecast_netto  = round(netto_maaned + pipeline_won - pipeline_cancel, 2)

    # Per-team netto revenue — joiner direkte på d.[team] for at matche Pipedrive præcist.
    # Deals uden team-tag (NULL) tæller ikke med under noget team.
    cur.execute("""
        SELECT
            t.name AS team,
            ISNULL(SUM(CASE WHEN d.[pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
                THEN CAST(COALESCE(CASE WHEN d.[currency] IN ('NOK','SEK') THEN d.[value] ELSE d.[value_dkk] END,d.[value]) AS DECIMAL(18,2)) ELSE 0 END), 0) AS won,
            ABS(ISNULL(SUM(CASE WHEN d.[pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
                THEN CAST(COALESCE(CASE WHEN d.[currency] IN ('NOK','SEK') THEN d.[value] ELSE d.[value_dkk] END,d.[value]) AS DECIMAL(18,2)) ELSE 0 END), 0)) AS cancel
        FROM Teams t
        LEFT JOIN [dbo].[PipedriveDeals] d
            ON d.[team] = t.name
            AND d.[status] = 'won'
            AND d.[pipeline_name] <> 'Web Sale'
            AND d.[service_activation_date] >= %s AND d.[service_activation_date] < %s
            AND COALESCE(d.[administrativ],'') <> 'ja'
            AND UPPER(LTRIM(d.[title])) NOT LIKE 'ADMINISTRATIV%'
            AND UPPER(LTRIM(d.[title])) NOT LIKE 'ADM %'
            AND COALESCE(d.[deal_type],'') <> 'Rapport'
        WHERE t.name IS NOT NULL AND t.name <> ''
        GROUP BY t.name
    """, (month_from.isoformat(), month_to.isoformat()))
    team_data_map = {}
    for r in cur.fetchall():
        won    = float(r["won"]    or 0)
        cancel = float(r["cancel"] or 0)
        team_data_map[r["team"]] = {
            "won":    round(won,    2),
            "cancel": round(cancel, 2),
            "netto":  round(won - cancel, 2),
        }

    # Per-team budget fra SalespersonBudget (individuelle sælgere)
    cur.execute("""
        SELECT [Team] AS team, SUM([BudgetAmount]) AS budget
        FROM [dbo].[SalespersonBudget]
        WHERE [BudgetDate] >= %s AND [BudgetDate] < %s
          AND [Team] IS NOT NULL AND [Team] <> ''
        GROUP BY [Team]
    """, (month_from.isoformat(), month_to.isoformat()))
    team_budget_map = {r["team"]: float(r["budget"] or 0) for r in cur.fetchall()}

    # Team Banner budget fra BudgetsIntoMedia (ingen per-person budget)
    cur.execute("""
        SELECT ISNULL(SUM([BudgetAmount]), 0) AS budget
        FROM [dbo].[BudgetsIntoMedia]
        WHERE [DealType] = 'Banner'
          AND [BudgetDate] >= %s AND [BudgetDate] < %s
    """, (month_from.isoformat(), month_to.isoformat()))
    banner_budget = float((cur.fetchone() or {}).get("budget", 0) or 0)
    if banner_budget > 0:
        team_budget_map["Team Banner"] = banner_budget

    # Team Marketwire budget fra BudgetsIntoMedia
    cur.execute("""
        SELECT ISNULL(SUM([BudgetAmount]), 0) AS budget
        FROM [dbo].[BudgetsIntoMedia]
        WHERE [Brand] = 'marketwire'
          AND [BudgetDate] >= %s AND [BudgetDate] < %s
    """, (month_from.isoformat(), month_to.isoformat()))
    marketwire_budget = float((cur.fetchone() or {}).get("budget", 0) or 0)
    if marketwire_budget > 0:
        team_budget_map["Team Marketwire"] = marketwire_budget

    # Alle teams (inkl. dem uden data i perioden)
    cur.execute("""
        SELECT name
        FROM Teams
        WHERE name IS NOT NULL AND name <> ''
        ORDER BY name
    """)
    all_team_names = [r["name"] for r in cur.fetchall()]

    # Sammensæt: alle teams med data + budget
    team_chart = []
    for name in all_team_names:
        data = team_data_map.get(name, {"won": 0, "cancel": 0, "netto": 0})
        team_chart.append({
            "team":   name,
            "won":    data["won"],
            "cancel": data["cancel"],
            "netto":  data["netto"],
            "budget": team_budget_map.get(name, 0),
        })
    # Sorter: teams med data øverst, derefter alfabetisk
    team_chart.sort(key=lambda x: (-x["won"], x["team"]))

    conn.close()

    vs_budget     = round(netto_maaned - budget_maaned, 2) if budget_maaned else None
    vs_budget_pct = round(netto_maaned / budget_maaned * 100, 1) if budget_maaned else None
    vs_ly         = round(netto_maaned - ly_netto, 2) if ly_netto else None
    vs_ly_pct     = round((netto_maaned - ly_netto) / abs(ly_netto) * 100, 1) if ly_netto else None

    return {
        "revenue_maaned":  round(revenue_maaned, 2),
        "cancel_maaned":   round(cancel_maaned, 2),
        "netto_maaned":    round(netto_maaned, 2),
        "budget_maaned":   round(budget_maaned, 2),
        "forecast_netto":   forecast_netto,
        "pipeline_won":    round(pipeline_won, 2),
        "pipeline_cancel": round(pipeline_cancel, 2),
        "ly_netto":        round(ly_netto, 2),
        "vs_budget":       vs_budget,
        "vs_budget_pct":   vs_budget_pct,
        "vs_ly":           vs_ly,
        "vs_ly_pct":       vs_ly_pct,
        "churn_chart":     churn_chart,
        "team_chart":      team_chart,
        "month_label":     month_label,
        "today":           date(year, chart_ref_month, 1).isoformat(),
    }
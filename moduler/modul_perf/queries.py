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
                     selected_year: int | None = None, selected_month: int | None = None,
                     date_col: str = "won_time"):
    ref_year  = selected_year  or today.year
    ref_month = selected_month or today.month

    month_from = date(ref_year, ref_month, 1)
    next_m     = ref_month % 12 + 1
    next_y     = ref_year + (1 if ref_month == 12 else 0)
    month_to   = date(next_y, next_m, 1)

    brands_ph   = "(" + ",".join(["%s"] * len(SUBSCRIPTION_BRANDS)) + ")"

    _VALID_DATE_COLS = {"won_time", "service_activation_date"}
    if date_col not in _VALID_DATE_COLS:
        date_col = "won_time"
    d_col = f"[{date_col}]"

    # Ekskluder FINANS DK site kun for Watch DK teamet ved service_activation_date (matcher Pipedrive's logik)
    is_finans_team = team and "FINANS" in team.upper()
    is_watch_dk_team = team and "WATCH DK" in team.upper()
    is_watch_int_team = team and "WATCH INT" in team.upper()

    if is_finans_team and team:
        # FINANS DK: filtrer på team-medlemmer + FINANS sites (matcher Pipedrive's owner+sites logik)
        team_clause = """AND [owner_name] IN (
            SELECT u2.name FROM HubUsers u2
            JOIN TeamMemberships tm2 ON tm2.user_id = u2.id
            JOIN Teams t2 ON t2.id = tm2.team_id
            WHERE t2.name = %s
            AND (tm2.end_date IS NULL OR tm2.end_date >= GETDATE())
        ) AND COALESCE([sites],'') = 'FINANS DK'"""
        team_params = (team,)
    elif team:
        # Alle andre teams: filtrer på owner_name via HubUsers + team-tag skal matche eller være NULL
        team_clause = """AND [owner_name] IN (
            SELECT u2.name FROM HubUsers u2
            JOIN TeamMemberships tm2 ON tm2.user_id = u2.id
            JOIN Teams t2 ON t2.id = tm2.team_id
            WHERE t2.name = %s
            AND (TRY_CAST(tm2.end_date AS DATE) IS NULL OR TRY_CAST(tm2.end_date AS DATE) >= CAST(GETDATE() AS DATE))
        ) AND ([team] = %s OR [team] IS NULL)"""
        team_params = (team, team)
    else:
        team_clause = ""
        team_params = ()
    if is_watch_int_team:
        # Watch Int ekskluderer alle FINANS sites på alle dato-kolonner
        non_finans_exclude = "AND COALESCE([sites],'') NOT LIKE '%FINANS%'"
    elif is_watch_dk_team:
        # Watch DK ekskluderer FINANS DK på alle dato-kolonner
        non_finans_exclude = "AND COALESCE([sites],'') <> 'FINANS DK'"
    else:
        non_finans_exclude = ""
    # Når team er valgt: tillad også NULL sites (fx Marketwire-deals har ingen site-værdi)
    sites_filter = f"AND ([sites] IN {brands_ph} OR [sites] IS NULL)" if team else f"AND [sites] IN {brands_ph}"

    conn = get_conn()
    cur  = conn.cursor(as_dict=True)

    cur.execute(f"""
        SELECT ISNULL(SUM(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2))),0) AS total
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND [won_time] >= %s AND [won_time] < %s
          {sites_filter}
          AND [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
          {_ADM_EXCLUDE}
          {non_finans_exclude}
          {team_clause}
    """, (today.isoformat(), (today + timedelta(days=1)).isoformat()) + tuple(SUBSCRIPTION_BRANDS) + team_params)
    salg_dag = float((cur.fetchone() or {}).get("total", 0) or 0)

    # Månedlig teamtotal for valgt periode
    cur.execute(f"""
        SELECT ISNULL(SUM(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2))),0) AS total
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND {d_col} >= %s AND {d_col} < %s
          {sites_filter}
          AND [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
          {_ADM_EXCLUDE}
          {non_finans_exclude}
          {team_clause}
    """, (month_from.isoformat(), month_to.isoformat()) + tuple(SUBSCRIPTION_BRANDS) + team_params)
    salg_maaned = float((cur.fetchone() or {}).get("total", 0) or 0)

    # Månedschart: team-total per måned for valgt år
    year_from = date(ref_year, 1, 1)
    year_to   = date(ref_year + 1, 1, 1)
    cur.execute(f"""
        SELECT MONTH({d_col}) AS maaned,
               ISNULL(SUM(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2))),0) AS total
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND {d_col} >= %s AND {d_col} < %s
          {sites_filter}
          AND [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
          {_ADM_EXCLUDE}
          {non_finans_exclude}
          {team_clause}
        GROUP BY MONTH({d_col})
        ORDER BY maaned
    """, (year_from.isoformat(), year_to.isoformat()) + tuple(SUBSCRIPTION_BRANDS) + team_params)
    maaned_raw = {r["maaned"]: float(r["total"] or 0) for r in cur.fetchall()}
    maaned_chart = [{"maaned": MONTH_NAMES_DA[m - 1][:3], "won": round(maaned_raw.get(m, 0), 2)}
                    for m in range(1, 13)]
    sparkline = maaned_chart  # bagudkompatibilitet

    cur.execute(f"""
        SELECT
            COUNT(CASE WHEN [status]='won'
                AND [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
                THEN 1 END) AS won_count,
            COUNT(CASE WHEN [status]='lost'
                OR ([status]='won' AND [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser'))
                THEN 1 END) AS lost_count
        FROM [dbo].[PipedriveDeals]
        WHERE ([status]='won' OR [status]='lost')
          AND [pipeline_name]<>'Web Sale'
          AND {d_col} >= %s AND {d_col} < %s
          {sites_filter}
          {_ADM_EXCLUDE}
          {non_finans_exclude}
          {team_clause}
    """, (month_from.isoformat(), month_to.isoformat()) + tuple(SUBSCRIPTION_BRANDS) + team_params)
    conv_row   = cur.fetchone() or {}
    won_count  = int(conv_row.get("won_count", 0) or 0)
    lost_count = int(conv_row.get("lost_count", 0) or 0)
    total_deals = won_count + lost_count
    conv_rate  = round((won_count / total_deals * 100), 1) if total_deals > 0 else 0.0

    if team:
        # Når et team er valgt: vis ALLE teammedlemmer, også dem med 0 salg
        cur.execute(f"""
            SELECT
                u.name AS owner_name,
                ISNULL(SUM(CASE WHEN d.[pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
                    THEN CAST(COALESCE(d.[value_dkk],d.[value]) AS DECIMAL(18,2)) ELSE 0 END), 0) AS won_amount,
                COUNT(CASE WHEN d.[pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
                    THEN 1 END) AS won_count,
                ISNULL(SUM(CASE WHEN d.[pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
                    THEN CAST(COALESCE(d.[value_dkk],d.[value]) AS DECIMAL(18,2)) ELSE 0 END), 0) AS cancel_amount
            FROM HubUsers u
            JOIN TeamMemberships tm ON tm.user_id = u.id
            JOIN Teams t ON t.id = tm.team_id
            LEFT JOIN [dbo].[PipedriveDeals] d
                ON d.[owner_name] = u.name
                AND d.[status] = 'won'
                AND d.[pipeline_name] <> 'Web Sale'
                AND d.{d_col} >= %s AND d.{d_col} < %s
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
        """, (month_from.isoformat(), month_to.isoformat()) + (() if is_finans_team else (team,)) + (team,))
    else:
        cur.execute(f"""
            SELECT
                COALESCE([owner_name], 'Ukendt') AS owner_name,
                ISNULL(SUM(CASE WHEN [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
                    THEN CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)) ELSE 0 END), 0) AS won_amount,
                COUNT(CASE WHEN [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
                    THEN 1 END) AS won_count,
                ISNULL(SUM(CASE WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
                    THEN CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)) ELSE 0 END), 0) AS cancel_amount
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
              AND {d_col} >= %s AND {d_col} < %s
              AND [sites] IN {brands_ph}
              {_ADM_EXCLUDE}
          {non_finans_exclude}
            GROUP BY [owner_name]
            ORDER BY won_amount DESC
        """, (month_from.isoformat(), month_to.isoformat()) + tuple(SUBSCRIPTION_BRANDS))

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

    if team:
        cur.execute("""
            SELECT [Owner] AS owner_name, SUM([BudgetAmount]) AS budget
            FROM [dbo].[SalespersonBudget]
            WHERE [BudgetDate] >= %s AND [BudgetDate] < %s
              AND [Team] = %s
            GROUP BY [Owner]
        """, (month_from.isoformat(), month_to.isoformat(), team))
    else:
        cur.execute("""
            SELECT [Owner] AS owner_name, SUM([BudgetAmount]) AS budget
            FROM [dbo].[SalespersonBudget]
            WHERE [BudgetDate] >= %s AND [BudgetDate] < %s
            GROUP BY [Owner]
        """, (month_from.isoformat(), month_to.isoformat()))
    budget_map = {r["owner_name"]: float(r["budget"] or 0) for r in cur.fetchall()}

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

    conn.close()
    return {
        "salg_dag":     round(salg_dag, 2),
        "salg_maaned":  round(salg_maaned, 2),
        "maaned_chart": maaned_chart,
        "sparkline":    sparkline,
        "conv_rate":    conv_rate,
        "won_count":    won_count,
        "lost_count":   lost_count,
        "leaderboard":  leaderboard,
        "teams":        teams,
        "active_team":  team,
        "month_label":  f"{MONTH_NAMES_DA[ref_month-1]} {ref_year}",
        "today":        today.isoformat(),
        "cur_month":    today.month,
        "ref_month":    ref_month,
        "ref_year":     ref_year,
    }


#-----------------------------------------------------------------------------------------------------------------------
#                                          DET NYE DASHBOARD FOR SÆLGER
#-----------------------------------------------------------------------------------------------------------------------

def db_saelger_data(today: date, owner_name: str, team: str | None = None,
                     selected_year: int | None = None, selected_month: int | None = None,
                     date_col: str = "won_time"):
    # Reference period — brug valgte år/måned hvis angivet, ellers aktuel dato
    ref_year  = selected_year  or today.year
    ref_month = selected_month or today.month

    month_from = date(ref_year, ref_month, 1)
    next_month = ref_month % 12 + 1
    next_year  = ref_year + (1 if ref_month == 12 else 0)
    month_to   = date(next_year, next_month, 1)

    year_from  = date(ref_year, 1, 1)
    year_to    = date(ref_year + 1, 1, 1)

    # Sparkline/salg i dag bruger altid rigtig today
    week_start = today - timedelta(days=today.weekday())
    week_end   = week_start + timedelta(days=7)

    # Deals-tabel: vis kun valgte måned, eller hele året hvis ingen måned valgt
    deals_from = month_from if selected_month else year_from
    deals_to   = month_to   if selected_month else year_to

    brands_ph   = "(" + ",".join(["%s"] * len(SUBSCRIPTION_BRANDS)) + ")"
    team_clause = "AND [team] = %s" if team else ""
    team_params = (team,) if team else ()
    # Dato-kolonne: won_time (matcher Pipedrive) eller service_activation_date
    _VALID_DATE_COLS = {"won_time", "service_activation_date"}
    if date_col not in _VALID_DATE_COLS:
        date_col = "won_time"
    d_col = f"[{date_col}]"

    conn = get_conn()
    cur  = conn.cursor(as_dict=True)

    # Q1: Won amount/count for reference-måneden
    cur.execute(f"""
        SELECT
            SUM((CASE
                WHEN [value_dkk] IS NOT NULL THEN ABS(CAST([value_dkk] AS DECIMAL(18,2)))
                WHEN [currency] = 'EUR' THEN ABS(CAST([value] * 7.46 AS DECIMAL(18,2)))
                WHEN [currency] = 'SEK' THEN ABS(CAST([value] * 0.65 AS DECIMAL(18,2)))
                WHEN [currency] = 'NOK' THEN ABS(CAST([value] * 0.63 AS DECIMAL(18,2)))
                WHEN [currency] = 'USD' THEN ABS(CAST([value] * 6.90 AS DECIMAL(18,2)))
                ELSE ABS(CAST([value] AS DECIMAL(18,2)))
            END) * (CASE WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser') THEN -1 ELSE 1 END)) AS net_amount,
            COUNT(CASE WHEN [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser') THEN 1 END) AS won_count,
            SUM(CASE WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
                THEN ABS(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2))) ELSE 0 END) AS cancel_amount
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND {d_col} >= %s AND {d_col} < %s
          AND [owner_name] = %s
          AND [sites] IN {brands_ph}
          {_ADM_EXCLUDE}
          {team_clause}
    """, (month_from.isoformat(), month_to.isoformat(), owner_name) + tuple(SUBSCRIPTION_BRANDS) + team_params)
    res           = cur.fetchone() or {}
    won_amount    = float(res.get("net_amount", 0) or 0)
    won_count     = int(res.get("won_count", 0) or 0)
    cancel_amount = abs(float(res.get("cancel_amount", 0) or 0))

    cur.execute("""
        SELECT [Team], ISNULL(SUM([BudgetAmount]),0) AS budget
        FROM [dbo].[SalespersonBudget]
        WHERE [BudgetDate] >= %s AND [BudgetDate] < %s
          AND [Owner] = %s
        GROUP BY [Team]
        ORDER BY [Team]
    """, (month_from.isoformat(), month_to.isoformat(), owner_name))
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
                   THEN -ABS(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)))
                   ELSE ABS(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)))
               END),0) AS total
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND {d_col} >= %s AND {d_col} < %s
          AND [owner_name] = %s
          AND [sites] IN {brands_ph}
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
            THEN -ABS(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)))
            ELSE ABS(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)))
        END),0) AS total
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND {d_col} >= %s AND {d_col} < %s
          AND [owner_name] = %s
          AND [sites] IN {brands_ph}
          {_ADM_EXCLUDE}
    """, (today.isoformat(), (today + timedelta(days=1)).isoformat(), owner_name) + tuple(SUBSCRIPTION_BRANDS))
    salg_dag = float((cur.fetchone() or {}).get("total", 0) or 0)

    cur.execute(f"""
        SELECT ISNULL(SUM(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2))),0) AS pipeline_value,
               COUNT(*) AS pipeline_count
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='open' AND [pipeline_name]<>'Web Sale'
          AND [expected_close_date] >= %s AND [expected_close_date] < %s
          AND [owner_name] = %s
          AND [sites] IN {brands_ph}
          AND COALESCE([value_dkk],[value]) > 0
          {_ADM_EXCLUDE}
          {team_clause}
    """, (month_from.isoformat(), month_to.isoformat(), owner_name) + tuple(SUBSCRIPTION_BRANDS) + team_params)
    pipe_row       = cur.fetchone() or {}
    pipeline_value = float(pipe_row.get("pipeline_value", 0) or 0)
    pipeline_count = int(pipe_row.get("pipeline_count", 0) or 0)

    ly_from = date(ref_year - 1, ref_month, 1)
    ly_next = ref_month % 12 + 1
    ly_ny   = ref_year - 1 + (1 if ref_month == 12 else 0)
    ly_to   = date(ly_ny, ly_next, 1)
    cur.execute(f"""
        SELECT ISNULL(SUM(CASE
            WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
            THEN -ABS(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)))
            ELSE ABS(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)))
        END),0) AS won
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND {d_col} >= %s AND {d_col} < %s
          AND [owner_name] = %s
          AND [sites] IN {brands_ph}
          {_ADM_EXCLUDE}
          {team_clause}
    """, (ly_from.isoformat(), ly_to.isoformat(), owner_name) + tuple(SUBSCRIPTION_BRANDS) + team_params)
    ly_won = float((cur.fetchone() or {}).get("won", 0) or 0)

    cur.execute(f"""
        SELECT MONTH({d_col}) AS maaned,
               ISNULL(SUM(CASE
                   WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
                   THEN -ABS(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)))
                   ELSE ABS(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)))
               END),0) AS won
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND {d_col} >= %s AND {d_col} < %s
          AND [owner_name] = %s
          AND [sites] IN {brands_ph}
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
                   THEN CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)) ELSE 0 END) AS won,
               SUM(CASE WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
                   THEN CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)) ELSE 0 END) AS cancel
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND {d_col} >= %s AND {d_col} < %s
          AND [sites] IN {brands_ph}
          {team_clause}
        GROUP BY [owner_name]
    """, (month_from.isoformat(), month_to.isoformat()) + tuple(SUBSCRIPTION_BRANDS) + team_params)
    leaderboard = sorted([
        {"owner_name": r["owner_name"],
         "won_amount":    float(r["won"]    or 0),
         "cancel_amount": float(r["cancel"] or 0)}
        for r in cur.fetchall()
    ], key=lambda x: -x["won_amount"])

    # Q9: Deals — filtreret på valgt periode + team
    cur.execute(f"""
        SELECT
            [title], [sites],
            ABS(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2))) AS value,
            CONVERT(NVARCHAR(10), {d_col}, 23) AS event_date,
            [deal_type],
            [status],
            [pipeline_name]
        FROM [dbo].[PipedriveDeals]
        WHERE [status] = 'won'
          AND [pipeline_name] <> 'Web Sale'
          AND [owner_name] = %s
          AND [sites] IN {brands_ph}
          AND {d_col} >= %s AND {d_col} < %s
          {_ADM_EXCLUDE}
          {team_clause}
        ORDER BY {d_col} DESC
    """, (owner_name,) + tuple(SUBSCRIPTION_BRANDS) + (deals_from.isoformat(), deals_to.isoformat()) + team_params)

    seneste_deals = [{
        "title": r["title"] or "(Uden titel)",
        "site": r["sites"] or "—",
        "value": float(r["value"] or 0),
        "dato": r["event_date"] or "—",
        "deal_type": r["deal_type"] or "—",
        "status": "Vundet",
        "is_cancel": r["pipeline_name"] in ('Cancellation', 'Cancellations', 'Opsigelser')
    } for r in cur.fetchall()]

    # Won-beløb per team (til budget-breakdown)
    cur.execute(f"""
        SELECT [team],
               ISNULL(SUM((CASE
                   WHEN [value_dkk] IS NOT NULL THEN ABS(CAST([value_dkk] AS DECIMAL(18,2)))
                   WHEN [currency] = 'EUR' THEN ABS(CAST([value] * 7.46 AS DECIMAL(18,2)))
                   WHEN [currency] = 'SEK' THEN ABS(CAST([value] * 0.65 AS DECIMAL(18,2)))
                   WHEN [currency] = 'NOK' THEN ABS(CAST([value] * 0.63 AS DECIMAL(18,2)))
                   WHEN [currency] = 'USD' THEN ABS(CAST([value] * 6.90 AS DECIMAL(18,2)))
                   ELSE ABS(CAST([value] AS DECIMAL(18,2)))
               END) * (CASE WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser') THEN -1 ELSE 1 END)),0) AS won
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND {d_col} >= %s AND {d_col} < %s
          AND [owner_name] = %s
          AND [sites] IN {brands_ph}
          {_ADM_EXCLUDE}
          {team_clause}
        GROUP BY [team]
    """, (month_from.isoformat(), month_to.isoformat(), owner_name) + tuple(SUBSCRIPTION_BRANDS) + team_params)
    won_by_team_raw = {r["team"]: float(r["won"] or 0) for r in cur.fetchall() if r["team"]}

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

    if selected_month:
        month_label = f"{MONTH_NAMES_DA[ref_month-1]} {ref_year}"
    else:
        month_label = str(ref_year)

    return {
        "won_amount":      round(won_amount, 2),
        "won_count":       won_count,
        "cancel_amount":   round(cancel_amount, 2),
        "budget":          round(budget, 2),
        "vs_budget_pct":   vs_budget_pct,
        "salg_dag":        round(salg_dag, 2),
        "sparkline":       sparkline,
        "pipeline_value":  round(pipeline_value, 2),
        "pipeline_count":  pipeline_count,
        "ly_won":          round(ly_won, 2),
        "yoy_pct":         yoy_pct,
        "maaned_chart":    maaned_chart,
        "leaderboard":     leaderboard,
        "seneste_deals":   seneste_deals,
        "budget_by_team":  budget_by_team,
        "owner_name":      owner_name,
        "month_label":     month_label,
        "ref_month":       ref_month,
        "ref_year":        ref_year,
        "today":           today.isoformat(),
    }


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
        SELECT ISNULL(SUM(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2))),0) AS revenue
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
          AND [won_time] >= %s AND [won_time] < %s
          AND COALESCE([administrativ],'') <> 'ja'
          AND UPPER(LTRIM([title])) NOT LIKE 'ADMINISTRATIV%'
          AND UPPER(LTRIM([title])) NOT LIKE 'ADM %'
          AND COALESCE([deal_type],'') <> 'Rapport'
          {sub_filter}
    """, (month_from.isoformat(), month_to.isoformat()) + sub_params)
    revenue_maaned = float((cur.fetchone() or {}).get("revenue", 0) or 0)

    cur.execute(f"""
        SELECT ISNULL(SUM(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2))),0) AS cancel
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
          AND [won_time] >= %s AND [won_time] < %s
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
                THEN CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)) ELSE 0 END),0) AS won,
            ISNULL(SUM(CASE WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
                THEN CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)) ELSE 0 END),0) AS cancel
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND [won_time] >= %s AND [won_time] < %s
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
            MONTH([won_time]) AS maaned,
            ISNULL(SUM(CASE WHEN [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
                THEN CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)) ELSE 0 END),0) AS won,
            ISNULL(SUM(CASE WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
                THEN CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)) ELSE 0 END),0) AS cancel,
            COUNT(CASE WHEN [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser') THEN 1 END) AS won_count,
            COUNT(CASE WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser') THEN 1 END) AS cancel_count
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND [won_time] >= %s AND [won_time] < %s
          AND COALESCE([administrativ],'') <> 'ja'
          AND UPPER(LTRIM([title])) NOT LIKE 'ADMINISTRATIV%'
          AND UPPER(LTRIM([title])) NOT LIKE 'ADM %'
          AND COALESCE([deal_type],'') <> 'Rapport'
          {sub_filter}
        GROUP BY MONTH([won_time])
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

    # Per-team netto revenue via TeamMemberships — matcher db_manager_data-logikken:
    # - FINANS-teams: kun deals med sites = 'FINANS DK' (exact match)
    # - Team Watch Int: non-FINANS subscription sites + team-tag match
    # - Watch DK: ekskluderer FINANS DK site + team-tag match
    # - Andre teams: alle subscription brands + NULL sites + team-tag match
    cur.execute(f"""
        SELECT
            t.name AS team,
            ISNULL(SUM(CASE WHEN d.[pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
                THEN CAST(COALESCE(d.[value_dkk],d.[value]) AS DECIMAL(18,2)) ELSE 0 END), 0) AS won,
            ABS(ISNULL(SUM(CASE WHEN d.[pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
                THEN CAST(COALESCE(d.[value_dkk],d.[value]) AS DECIMAL(18,2)) ELSE 0 END), 0)) AS cancel
        FROM Teams t
        JOIN TeamMemberships tm ON tm.team_id = t.id
        JOIN HubUsers u ON u.id = tm.user_id
        LEFT JOIN [dbo].[PipedriveDeals] d
            ON d.[owner_name] = u.name
            AND d.[status] = 'won'
            AND d.[pipeline_name] <> 'Web Sale'
            AND d.[won_time] >= %s AND d.[won_time] < %s
            AND (COALESCE(d.[administrativ],'') <> 'ja')
            AND UPPER(LTRIM(d.[title])) NOT LIKE 'ADMINISTRATIV%'
            AND UPPER(LTRIM(d.[title])) NOT LIKE 'ADM %'
            AND COALESCE(d.[deal_type],'') <> 'Rapport'
            AND (
                -- FINANS-teams: kun FINANS DK site (exact match)
                (UPPER(t.name) LIKE '%FINANS%'
                 AND COALESCE(d.[sites],'') = 'FINANS DK')
                OR
                -- Team Watch Int: non-FINANS subscription sites + team-tag match
                (t.name = 'Team Watch Int'
                 AND COALESCE(d.[sites],'') NOT LIKE '%FINANS%'
                 AND (d.[sites] IN {brands_ph} OR d.[sites] IS NULL)
                 AND (d.[team] = t.name OR d.[team] IS NULL))
                OR
                -- Watch DK: ekskluder FINANS DK site + team-tag match
                (UPPER(t.name) LIKE '%WATCH DK%'
                 AND COALESCE(d.[sites],'') <> 'FINANS DK'
                 AND (d.[sites] IN {brands_ph} OR d.[sites] IS NULL)
                 AND (d.[team] = t.name OR d.[team] IS NULL))
                OR
                -- Andre teams: alle subscription brands + NULL sites + team-tag match
                (UPPER(t.name) NOT LIKE '%FINANS%' AND t.name <> 'Team Watch Int' AND UPPER(t.name) NOT LIKE '%WATCH DK%'
                 AND (d.[sites] IN {brands_ph} OR d.[sites] IS NULL)
                 AND (d.[team] = t.name OR d.[team] IS NULL))
            )
        WHERE (TRY_CAST(tm.end_date AS DATE) IS NULL
               OR TRY_CAST(tm.end_date AS DATE) >= CAST(GETDATE() AS DATE))
        GROUP BY t.name
    """, (month_from.isoformat(), month_to.isoformat()) + sub_params + sub_params + sub_params)
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

    # Alle aktive teams (inkl. dem uden data)
    cur.execute("""
        SELECT DISTINCT t.name
        FROM Teams t
        JOIN TeamMemberships tm ON tm.team_id = t.id
        WHERE t.name IS NOT NULL AND t.name <> ''
          AND (TRY_CAST(tm.end_date AS DATE) IS NULL OR TRY_CAST(tm.end_date AS DATE) >= CAST(GETDATE() AS DATE))
        ORDER BY t.name
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
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


def db_perf_data(date_from, date_to, date_field, include_web_sale,
                 deal_type, sales_type, brand_list, is_manager,
                 owner_filter, brand_groups, budget_from, budget_to_excl, budget_year):
    conn = get_conn()
    cur = conn.cursor(as_dict=True)

    # Q1: Won
    where, params = build_where(
        date_field, date_from, date_to, include_web_sale, deal_type, sales_type,
        owner_filter, exclude_cancellations=True, brand_list=brand_list,
    )
    cur.execute(f"""
        SELECT COALESCE([sites], 'Ukendt') AS brand,
               COALESCE([owner_name], 'Ukendt') AS owner_name,
               SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS won_amount,
               COUNT(*) AS won_count
        FROM [dbo].[PipedriveDeals]
        {where}
        GROUP BY [sites], [owner_name]
    """, tuple(params))
    won_rows = cur.fetchall()

    # Q2: Cancellations
    where_c, params_c = build_where(
        date_field, date_from, date_to, include_web_sale, deal_type, sales_type,
        owner_filter, cancellations_only=True, brand_list=brand_list,
    )
    try:
        cur.execute(f"""
            SELECT COALESCE([sites], 'Ukendt') AS brand,
                   COALESCE([owner_name], 'Ukendt') AS owner_name,
                   ABS(SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2)))) AS cancel_amount,
                   COUNT(*) AS cancel_count
            FROM [dbo].[PipedriveDeals]
            {where_c}
            GROUP BY [sites], [owner_name]
        """, tuple(params_c))
        cancel_rows = cur.fetchall()
    except Exception:
        cancel_rows = []

    # Q3: Last year won
    ly_from, ly_to = shift_year_back(date_from, date_to)
    where_ly, params_ly = build_where(
        date_field, ly_from, ly_to, include_web_sale, deal_type, sales_type,
        owner_filter, exclude_cancellations=True, brand_list=brand_list,
    )
    cur.execute(f"""
        SELECT COALESCE([sites], 'Ukendt') AS brand,
               COALESCE([owner_name], 'Ukendt') AS owner_name,
               SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS won_amount
        FROM [dbo].[PipedriveDeals]
        {where_ly}
        GROUP BY [sites], [owner_name]
    """, tuple(params_ly))
    last_year_rows = cur.fetchall()

    # Q3b: Last year cancellations
    where_lyc, params_lyc = build_where(
        date_field, ly_from, ly_to, include_web_sale, deal_type, sales_type,
        owner_filter, cancellations_only=True, brand_list=brand_list,
    )
    try:
        cur.execute(f"""
            SELECT COALESCE([sites], 'Ukendt') AS brand,
                   COALESCE([owner_name], 'Ukendt') AS owner_name,
                   ABS(SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2)))) AS cancel_amount
            FROM [dbo].[PipedriveDeals]
            {where_lyc}
            GROUP BY [sites], [owner_name]
        """, tuple(params_lyc))
        last_year_cancel_rows = cur.fetchall()
    except Exception:
        last_year_cancel_rows = []

    # Q4: Brand budget
    try:
        cur.execute("""
            SELECT [Site] AS dimension_key, SUM([BudgetAmount]) AS budget
            FROM [dbo].[BudgetsIntoMedia]
            WHERE [BudgetDate] >= %s AND [BudgetDate] < %s
            GROUP BY [Site]
        """, (budget_from.isoformat(), budget_to_excl.isoformat()))
        brand_budget_rows = cur.fetchall()
    except Exception:
        brand_budget_rows = []

    # Q5: Saelger budget
    saelger_budget_rows = []
    if is_manager:
        try:
            brand_keys = (
                [k.strip() for k in brand_groups.split(",") if k.strip() in BRAND_GROUPS]
                if brand_groups else []
            )
            if brand_keys:
                bk_ph = "(" + ",".join(["%s"] * len(brand_keys)) + ")"
                cur.execute(f"""
                    SELECT [Owner] AS dimension_key, SUM([BudgetAmount]) AS budget
                    FROM [dbo].[SalesPersonBudget]
                    WHERE [BudgetDate] >= %s AND [BudgetDate] < %s
                      AND [Brand] IN {bk_ph}
                    GROUP BY [Owner]
                """, (budget_from.isoformat(), budget_to_excl.isoformat(), *brand_keys))
            else:
                cur.execute("""
                    SELECT [Owner] AS dimension_key, SUM([BudgetAmount]) AS budget
                    FROM [dbo].[SalesPersonBudget]
                    WHERE [BudgetDate] >= %s AND [BudgetDate] < %s
                    GROUP BY [Owner]
                """, (budget_from.isoformat(), budget_to_excl.isoformat()))
            saelger_budget_rows = cur.fetchall()
        except Exception:
            saelger_budget_rows = []

    # Q6: Brand forecast
    try:
        cur.execute("""
            SELECT [dimension_key], SUM([forecast_amount]) AS forecast_total
            FROM [dbo].[HubForecasts]
            WHERE [forecast_year] = %s AND [level] = 'medie'
            GROUP BY [dimension_key]
        """, (budget_year,))
        brand_forecast_rows = cur.fetchall()
    except Exception:
        brand_forecast_rows = []

    # Q7: Saelger forecast
    saelger_forecast_rows = []
    if is_manager:
        try:
            cur.execute("""
                SELECT [dimension_key], SUM([forecast_amount]) AS forecast_total
                FROM [dbo].[HubForecasts]
                WHERE [forecast_year] = %s AND [level] = 'saelger'
                GROUP BY [dimension_key]
            """, (budget_year,))
            saelger_forecast_rows = cur.fetchall()
        except Exception:
            saelger_forecast_rows = []

    conn.close()
    return (won_rows, cancel_rows, last_year_rows, last_year_cancel_rows,
            brand_budget_rows, saelger_budget_rows, brand_forecast_rows, saelger_forecast_rows)


def db_breakdown(group_by, date_from, date_to, date_field, include_web_sale,
                 deal_type, sales_type, owner_filter, brand_list):
    ly_from, ly_to = shift_year_back(date_from, date_to)
    col = GROUPBY_COLUMNS[group_by]
    conn = get_conn()
    cur = conn.cursor(as_dict=True)

    where, params = build_where(
        date_field, date_from, date_to, include_web_sale, deal_type, sales_type,
        owner_filter, exclude_cancellations=True, brand_list=brand_list,
    )
    cur.execute(f"""
        SELECT COALESCE({col}, '(Ukendt)') AS dim,
               SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS won_amount,
               COUNT(*) AS won_count
        FROM [dbo].[PipedriveDeals]
        {where}
        GROUP BY {col}
    """, tuple(params))
    won_rows = cur.fetchall()

    where_c, params_c = build_where(
        date_field, date_from, date_to, include_web_sale, deal_type, sales_type,
        owner_filter, cancellations_only=True, brand_list=brand_list,
    )
    try:
        cur.execute(f"""
            SELECT COALESCE({col}, '(Ukendt)') AS dim,
                   ABS(SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2)))) AS cancel_amount,
                   COUNT(*) AS cancel_count
            FROM [dbo].[PipedriveDeals]
            {where_c}
            GROUP BY {col}
        """, tuple(params_c))
        cancel_rows = cur.fetchall()
    except Exception:
        cancel_rows = []

    where_ly, params_ly = build_where(
        date_field, ly_from, ly_to, include_web_sale, deal_type, sales_type,
        owner_filter, exclude_cancellations=True, brand_list=brand_list,
    )
    cur.execute(f"""
        SELECT COALESCE({col}, '(Ukendt)') AS dim,
               SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS won_amount
        FROM [dbo].[PipedriveDeals]
        {where_ly}
        GROUP BY {col}
    """, tuple(params_ly))
    last_year_rows = cur.fetchall()

    where_lyc, params_lyc = build_where(
        date_field, ly_from, ly_to, include_web_sale, deal_type, sales_type,
        owner_filter, cancellations_only=True, brand_list=brand_list,
    )
    try:
        cur.execute(f"""
            SELECT COALESCE({col}, '(Ukendt)') AS dim,
                   ABS(SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2)))) AS cancel_amount
            FROM [dbo].[PipedriveDeals]
            {where_lyc}
            GROUP BY {col}
        """, tuple(params_lyc))
        last_year_cancel_rows = cur.fetchall()
    except Exception:
        last_year_cancel_rows = []

    conn.close()
    return won_rows, cancel_rows, last_year_rows, last_year_cancel_rows


def db_deals(where, params, date_field):
    d_expr = date_expr(date_field)
    conn = get_conn()
    cur = conn.cursor(as_dict=True)
    cur.execute(f"""
        SELECT TOP 500
            [title],
            COALESCE([owner_name], 'Ukendt') AS owner_name,
            COALESCE([sites], 'Ukendt') AS brand,
            CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2)) AS value,
            CONVERT(NVARCHAR(10), {d_expr}, 23) AS deal_date,
            [deal_type], [sales_type], [pipeline_name],
            CASE WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
                 THEN 1 ELSE 0 END AS is_cancellation
        FROM [dbo].[PipedriveDeals]
        {where}
        ORDER BY {d_expr} DESC
    """, tuple(params))
    rows = cur.fetchall()
    conn.close()
    return rows


def db_overview_data(today: date):
    day_from   = today
    day_to     = today + timedelta(days=1)
    wd         = today.weekday()
    week_from  = today - timedelta(days=wd)
    week_to    = week_from + timedelta(days=7)
    m          = today.month
    month_from = date(today.year, m, 1)
    month_to   = date(today.year + (m // 12), m % 12 + 1, 1)
    year_from  = date(today.year, 1, 1)
    year_to    = date(today.year + 1, 1, 1)
    q          = (m - 1) // 3
    q_start    = q * 3 + 1
    qtr_from   = date(today.year, q_start, 1)
    q_end      = q_start + 3
    qtr_to     = date(today.year + (q_end > 12), (q_end - 1) % 12 + 1, 1)

    brands_ph = "(" + ",".join(["%s"] * len(SUBSCRIPTION_BRANDS)) + ")"

    conn = get_conn()
    cur = conn.cursor(as_dict=True)

    def fetch_rev(pfrom, pto):
        cur.execute(f"""
            SELECT ISNULL(SUM(CASE WHEN [cancellation] IS NULL OR [cancellation]=''
                THEN CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2)) ELSE 0 END),0) AS won
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
              AND [won_time]>=%s AND [won_time]<%s
              AND [sites] IN {brands_ph}
        """, (pfrom.isoformat(), pto.isoformat()) + tuple(SUBSCRIPTION_BRANDS))
        won = float((cur.fetchone() or {}).get("won", 0) or 0)

        cur.execute(f"""
            SELECT
                ISNULL(SUM(CASE WHEN [cancellation] IS NULL OR [cancellation]=''
                    THEN CAST([value] AS DECIMAL(18,2)) ELSE 0 END),0) AS won_sad,
                ISNULL(SUM(CASE WHEN [cancellation] IS NOT NULL AND [cancellation]<>''
                    THEN CAST([value] AS DECIMAL(18,2)) ELSE 0 END),0) AS cancel
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
              AND COALESCE([service_activation_date],[won_time])>=%s
              AND COALESCE([service_activation_date],[won_time])<%s
              AND [sites] IN {brands_ph}
        """, (pfrom.isoformat(), pto.isoformat()) + tuple(SUBSCRIPTION_BRANDS))
        r2     = cur.fetchone() or {}
        cancel = float(r2.get("cancel", 0) or 0)
        net    = float(r2.get("won_sad", 0) or 0) - cancel
        return {"won": round(won, 2), "cancel": round(cancel, 2), "net": round(net, 2)}

    def fetch_team_perf(pfrom, pto):
        bud_from, bud_to = budget_range(pfrom, pto)
        cur.execute(f"""
            WITH PersonTeams AS (
                SELECT u.name AS owner_name, t.name AS team_name, t.brand AS team_brand,
                       tm.start_date AS mem_start, tm.end_date AS mem_end
                FROM HubUsers u
                JOIN TeamMemberships tm ON tm.user_id = u.id
                JOIN Teams t ON t.id = tm.team_id
            ),
            SiteTagged AS (
                SELECT pd.owner_name, pd.value, pd.cancellation,
                       COALESCE(pd.service_activation_date, pd.won_time) AS deal_date,
                       CASE
                           WHEN pd.sites LIKE '%onitor%'    THEN 'monitor'
                           WHEN pd.sites = 'FINANS DK'      THEN 'finans'
                           WHEN pd.sites LIKE '%Watch%' AND pd.sites LIKE '% SE' THEN 'watch_se'
                           WHEN pd.sites LIKE '%Watch%' AND pd.sites LIKE '% NO' THEN 'watch_no'
                           WHEN pd.sites LIKE '%Watch%' AND pd.sites LIKE '% DE' THEN 'watch_de'
                           WHEN pd.sites LIKE '%Watch%'     THEN 'watch_dk'
                           WHEN pd.sites LIKE '%FINANS%' OR pd.sites LIKE '%Finans%' THEN 'finans_int'
                           WHEN pd.sites LIKE '%arketWire%' THEN 'marketwire'
                           ELSE NULL
                       END AS site_brand
                FROM [dbo].[PipedriveDeals] pd
                WHERE pd.status='won' AND pd.pipeline_name<>'Web Sale'
                  AND COALESCE(pd.service_activation_date, pd.won_time) >= %s
                  AND COALESCE(pd.service_activation_date, pd.won_time) < %s
                  AND pd.sites IN {brands_ph}
            ),
            DealWithTeam AS (
                SELECT st.value, st.cancellation,
                       COALESCE(pt_match.team_name, pt_catch.team_name, pt_any.team_name) AS team
                FROM SiteTagged st
                OUTER APPLY (
                    SELECT TOP 1 pt.team_name FROM PersonTeams pt
                    WHERE pt.owner_name = st.owner_name AND pt.team_brand = st.site_brand
                      AND (pt.mem_start IS NULL OR pt.mem_start <= st.deal_date)
                      AND (pt.mem_end   IS NULL OR pt.mem_end   >= st.deal_date)
                ) pt_match
                OUTER APPLY (
                    SELECT TOP 1 pt2.team_name FROM PersonTeams pt2
                    WHERE pt2.owner_name = st.owner_name
                      AND (pt2.team_brand IS NULL OR pt2.team_brand = '')
                      AND (pt2.mem_start IS NULL OR pt2.mem_start <= st.deal_date)
                      AND (pt2.mem_end   IS NULL OR pt2.mem_end   >= st.deal_date)
                ) pt_catch
                OUTER APPLY (
                    SELECT TOP 1 pt3.team_name FROM PersonTeams pt3
                    WHERE pt3.owner_name = st.owner_name
                      AND (pt3.mem_start IS NULL OR pt3.mem_start <= st.deal_date)
                      AND (pt3.mem_end   IS NULL OR pt3.mem_end   >= st.deal_date)
                    ORDER BY
                        CASE
                            WHEN st.site_brand LIKE 'watch%'  AND pt3.team_name LIKE '%Watch%'   THEN 0
                            WHEN st.site_brand LIKE 'finans%' AND pt3.team_name LIKE '%Finans%'  THEN 0
                            WHEN st.site_brand = 'monitor'    AND pt3.team_name LIKE '%Monitor%' THEN 0
                            WHEN st.site_brand = 'marketwire' AND pt3.team_name LIKE '%Market%'  THEN 0
                            ELSE 1
                        END, pt3.team_name
                ) pt_any
            )
            SELECT ISNULL(team, 'Ukendt') AS team,
                   ISNULL(SUM(CASE WHEN cancellation IS NULL OR cancellation=''
                       THEN CAST(value AS DECIMAL(18,2)) ELSE 0 END),0) AS won,
                   ISNULL(SUM(CASE WHEN cancellation IS NOT NULL AND cancellation<>''
                       THEN CAST(value AS DECIMAL(18,2)) ELSE 0 END),0) AS cancel
            FROM DealWithTeam
            GROUP BY ISNULL(team, 'Ukendt')
        """, (pfrom.isoformat(), pto.isoformat()) + tuple(SUBSCRIPTION_BRANDS))
        deal_rows = {r["team"]: r for r in cur.fetchall()}

        cur.execute("""
            SELECT Team, SUM(BudgetAmount) AS budget
            FROM [dbo].[SalesPersonBudget]
            WHERE BudgetDate >= %s AND BudgetDate < %s
            GROUP BY Team
        """, (bud_from.isoformat(), bud_to.isoformat()))
        bud_map = {r["Team"]: float(r["budget"] or 0) for r in cur.fetchall()}

        teams = sorted(set(list(deal_rows.keys()) + list(bud_map.keys())))
        result = []
        for t in teams:
            r   = deal_rows.get(t, {"won": 0, "cancel": 0})
            won = float(r["won"] or 0)
            can = float(r["cancel"] or 0)
            bud = bud_map.get(t, 0.0)
            result.append({"team": t, "won": round(won, 2), "cancel": round(can, 2),
                            "net": round(won - can, 2), "budget": round(bud, 2)})
        return result

    rev_dag    = fetch_rev(day_from,   day_to)
    rev_uge    = fetch_rev(week_from,  week_to)
    rev_maaned = fetch_rev(month_from, month_to)
    rev_aar    = fetch_rev(year_from,  year_to)

    team_quarter = fetch_team_perf(qtr_from,  qtr_to)
    team_month   = fetch_team_perf(month_from, month_to)

    cur.execute(f"""
        SELECT owner_name,
            COUNT(CASE WHEN cancellation IS NULL OR cancellation='' THEN 1 END) AS won_count,
            ISNULL(SUM(CASE WHEN cancellation IS NULL OR cancellation=''
                THEN CAST(value AS DECIMAL(18,2)) ELSE 0 END), 0) AS won_amount,
            COUNT(CASE WHEN cancellation IS NOT NULL AND cancellation<>'' THEN 1 END) AS cancel_count
        FROM [dbo].[PipedriveDeals]
        WHERE status='won' AND pipeline_name<>'Web Sale'
          AND won_time>=%s AND won_time<%s
          AND sites IN {brands_ph}
        GROUP BY owner_name
    """, (month_from.isoformat(), month_to.isoformat()) + tuple(SUBSCRIPTION_BRANDS))
    saelger_month = sorted(cur.fetchall(), key=lambda r: -(r["won_count"] or 0))
    saelger_won = [{"name": r["owner_name"], "count": r["won_count"] or 0,
                    "revenue": round(float(r["won_amount"] or 0), 2)}
                   for r in saelger_month]

    try:
        cur.execute(f"""
            SELECT owner_name, COUNT(*) AS cnt
            FROM [dbo].[PipedriveDeals]
            WHERE add_time>=%s AND add_time<%s
              AND pipeline_name<>'Web Sale'
              AND sites IN {brands_ph}
            GROUP BY owner_name
            ORDER BY cnt DESC
        """, (month_from.isoformat(), month_to.isoformat()) + tuple(SUBSCRIPTION_BRANDS))
        saelger_created = [{"name": r["owner_name"], "count": r["cnt"]} for r in cur.fetchall()]
    except Exception:
        saelger_created = []

    try:
        cur.execute(f"""
            SELECT TOP 25
                pd.owner_name,
                ISNULL(pd.org_name,'') AS org_name,
                ISNULL(ot.team_name,'') AS team,
                ISNULL(pd.sites,'') AS sites,
                CAST(pd.value AS DECIMAL(18,2)) AS deal_value,
                CONVERT(NVARCHAR(19), pd.won_time, 120) AS won_dt
            FROM [dbo].[PipedriveDeals] pd
            OUTER APPLY (
                SELECT TOP 1 t.name AS team_name
                FROM HubUsers u
                JOIN TeamMemberships tm ON tm.user_id = u.id
                    AND (tm.start_date IS NULL OR tm.start_date <= pd.won_time)
                    AND (tm.end_date   IS NULL OR tm.end_date   >= pd.won_time)
                JOIN Teams t ON t.id = tm.team_id
                WHERE u.name = pd.owner_name
                ORDER BY
                    CASE
                        WHEN pd.sites LIKE '%Watch%'  AND t.name LIKE '%Watch%'  THEN 0
                        WHEN pd.sites LIKE '%FINANS%' AND t.name LIKE '%Finans%' THEN 0
                        WHEN pd.sites LIKE '%Finans%' AND t.name LIKE '%Finans%' THEN 0
                        WHEN pd.sites LIKE '%onitor%' AND t.name LIKE '%Monitor%' THEN 0
                        ELSE 1
                    END, t.name
            ) ot
            WHERE pd.status='won' AND pd.pipeline_name<>'Web Sale'
              AND (pd.cancellation IS NULL OR pd.cancellation='')
              AND pd.sites IN {brands_ph}
            ORDER BY pd.won_time DESC
        """, tuple(SUBSCRIPTION_BRANDS))
        recent_deals = [{"owner": r["owner_name"] or "", "org": r["org_name"] or "",
                         "team": r["team"] or "", "site": r["sites"] or "",
                         "value": round(float(r["deal_value"] or 0), 2),
                         "won": r["won_dt"] or ""}
                        for r in cur.fetchall()]
    except Exception:
        traceback.print_exc()
        recent_deals = []

    conn.close()
    return {
        "today": today.isoformat(),
        "periods": {
            "qtr_label":   f"Q{(today.month-1)//3+1} {today.year}",
            "month_label": f"{MONTH_NAMES_DA[today.month-1]} {today.year}",
        },
        "revenue":         {"dag": rev_dag, "uge": rev_uge, "maaned": rev_maaned, "aar": rev_aar},
        "team_quarter":    team_quarter,
        "team_month":      team_month,
        "saelger_won":     saelger_won,
        "saelger_created": saelger_created,
        "recent_deals":    recent_deals,
    }


#-----------------------------------------------------------------------------------------------------------------------
#                                          DET NYE DASHBOARD FOR MANAGER
#-----------------------------------------------------------------------------------------------------------------------

def db_manager_data(today: date, team: str | None = None):
    month_from = date(today.year, today.month, 1)
    next_month = today.month % 12 + 1
    next_year  = today.year + (1 if today.month == 12 else 0)
    month_to   = date(next_year, next_month, 1)

    week_start = today - timedelta(days=today.weekday())
    week_end   = week_start + timedelta(days=7)

    brands_ph = "(" + ",".join(["%s"] * len(SUBSCRIPTION_BRANDS)) + ")"

    # Team-filter klausul
    if team:
        team_clause = """
            AND [owner_name] IN (
                SELECT u.name
                FROM HubUsers u
                JOIN TeamMemberships tm ON tm.user_id = u.id
                JOIN Teams t ON t.id = tm.team_id
                WHERE t.name = %s
                  AND (tm.end_date IS NULL OR tm.end_date >= GETDATE())
            )
        """
        team_params = (team,)
    else:
        team_clause = ""
        team_params = ()

    conn = get_conn()
    cur  = conn.cursor(as_dict=True)

    # Q1: Salg i dag
    cur.execute(f"""
        SELECT ISNULL(SUM(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2))),0) AS total
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND [won_time] >= %s AND [won_time] < %s
          AND [sites] IN {brands_ph}
          AND [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
          {team_clause}
    """, (today.isoformat(), (today + timedelta(days=1)).isoformat()) + tuple(SUBSCRIPTION_BRANDS) + team_params)
    salg_dag = float((cur.fetchone() or {}).get("total", 0) or 0)

    # Q2: Sparkline — salg per dag denne uge
    cur.execute(f"""
        SELECT CAST([won_time] AS DATE) AS dag,
               ISNULL(SUM(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2))),0) AS total
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND [won_time] >= %s AND [won_time] < %s
          AND [sites] IN {brands_ph}
          AND [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
          {team_clause}
        GROUP BY CAST([won_time] AS DATE)
        ORDER BY dag
    """, (week_start.isoformat(), week_end.isoformat()) + tuple(SUBSCRIPTION_BRANDS) + team_params)
    sparkline_raw = {str(r["dag"]): float(r["total"] or 0) for r in cur.fetchall()}
    sparkline = []
    for i in range(7):
        d = week_start + timedelta(days=i)
        sparkline.append({"dag": d.isoformat(), "total": sparkline_raw.get(d.isoformat(), 0)})

    # Q3: Konverteringsrate denne måned
    cur.execute(f"""
        SELECT
            COUNT(CASE WHEN [status]='won'
                AND [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
                THEN 1 END) AS won_count,
            COUNT(CASE WHEN [status]='lost' THEN 1 END) AS lost_count
        FROM [dbo].[PipedriveDeals]
        WHERE ([status]='won' OR [status]='lost')
          AND [pipeline_name]<>'Web Sale'
          AND [won_time] >= %s AND [won_time] < %s
          AND [sites] IN {brands_ph}
          {team_clause}
    """, (month_from.isoformat(), month_to.isoformat()) + tuple(SUBSCRIPTION_BRANDS) + team_params)
    conv_row   = cur.fetchone() or {}
    won_count  = int(conv_row.get("won_count", 0) or 0)
    lost_count = int(conv_row.get("lost_count", 0) or 0)
    total_deals = won_count + lost_count
    conv_rate  = round((won_count / total_deals * 100), 1) if total_deals > 0 else 0.0

    # Q4: Hent alle aktive teammedlemmer
    if team:
        cur.execute("""
                    SELECT DISTINCT u.name AS owner_name
                    FROM HubUsers u
                             JOIN TeamMemberships tm ON tm.user_id = u.id
                             JOIN Teams t ON t.id = tm.team_id
                    WHERE t.name = %s
                      AND (tm.end_date IS NULL OR tm.end_date >= GETDATE())
                      AND u.is_active = 1
                    ORDER BY u.name
                    """, (team,))
    else:
        cur.execute("""
                    SELECT DISTINCT u.name AS owner_name
                    FROM HubUsers u
                             JOIN TeamMemberships tm ON tm.user_id = u.id
                    WHERE (tm.end_date IS NULL OR tm.end_date >= GETDATE())
                      AND u.is_active = 1
                    ORDER BY u.name
                    """)
    all_members = [r["owner_name"] for r in cur.fetchall()]

    # Q5: Deals denne måned
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
          AND [won_time] >= %s AND [won_time] < %s
          AND [sites] IN {brands_ph}
          {team_clause}
        GROUP BY [owner_name]
    """, (month_from.isoformat(), month_to.isoformat()) + tuple(SUBSCRIPTION_BRANDS) + team_params)
    deals_map = {r["owner_name"]: r for r in cur.fetchall()}

    # Merge — alle medlemmer vises, også dem med 0 deals
    leaderboard = []
    for name in all_members:
        r = deals_map.get(name, {"won_amount": 0, "won_count": 0, "cancel_amount": 0})
        leaderboard.append({
            "owner_name": name,
            "won_amount": float(r["won_amount"] or 0),
            "won_count": int(r["won_count"] or 0),
            "cancel_amount": abs(float(r["cancel_amount"] or 0)),
        })

    # Sorter efter won_amount — højeste øverst
    leaderboard.sort(key=lambda x: -x["won_amount"])
    for i, row in enumerate(leaderboard):
        row["rank"] = i + 1

    # Q5: Budget denne måned pr. sælger
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
        row["vs_budget_pct"] = round(row["won_amount"] / row["budget"] * 100, 1) if row["budget"] > 0 else None

    # Q6: Hent alle tilgængelige teams til dropdown
    cur.execute("""
        SELECT DISTINCT t.name
        FROM Teams t
        JOIN TeamMemberships tm ON tm.team_id = t.id
        WHERE t.name IS NOT NULL
          AND (tm.end_date IS NULL OR tm.end_date >= GETDATE())
        ORDER BY t.name
    """)
    teams = [r["name"] for r in cur.fetchall()]

    conn.close()
    return {
        "salg_dag":    round(salg_dag, 2),
        "sparkline":   sparkline,
        "conv_rate":   conv_rate,
        "won_count":   won_count,
        "lost_count":  lost_count,
        "leaderboard": leaderboard,
        "teams":       teams,
        "active_team": team,
        "month_label": f"{['Januar','Februar','Marts','April','Maj','Juni','Juli','August','September','Oktober','November','December'][today.month-1]} {today.year}",
        "today":       today.isoformat(),
    }


#-----------------------------------------------------------------------------------------------------------------------
#                                          DET NYE DASHBOARD FOR LEDER
#-----------------------------------------------------------------------------------------------------------------------

def db_afdelingsleder_data(today: date, vis_alle: bool = False):
    month_from = date(today.year, today.month, 1)
    next_month = today.month % 12 + 1
    next_year  = today.year + (1 if today.month == 12 else 0)
    month_to   = date(next_year, next_month, 1)
    year_from  = date(today.year, 1, 1)
    year_to    = date(today.year + 1, 1, 1)

    brands_ph  = "(" + ",".join(["%s"] * len(SUBSCRIPTION_BRANDS)) + ")"
    sub_filter = f"AND [sites] IN {brands_ph}" if not vis_alle else ""
    sub_params = tuple(SUBSCRIPTION_BRANDS) if not vis_alle else ()

    conn = get_conn()
    cur  = conn.cursor(as_dict=True)

    # Q1: Revenue vs Budget denne måned
    cur.execute(f"""
        SELECT ISNULL(SUM(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2))),0) AS revenue
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
          AND [won_time] >= %s AND [won_time] < %s
          {sub_filter}
    """, (month_from.isoformat(), month_to.isoformat()) + sub_params)
    revenue_maaned = float((cur.fetchone() or {}).get("revenue", 0) or 0)

    # Q2: Opsigelser denne måned
    cur.execute(f"""
        SELECT ISNULL(SUM(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2))),0) AS cancel
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
          AND [won_time] >= %s AND [won_time] < %s
          {sub_filter}
    """, (month_from.isoformat(), month_to.isoformat()) + sub_params)
    cancel_maaned = abs(float((cur.fetchone() or {}).get("cancel", 0) or 0))
    netto_maaned  = revenue_maaned - cancel_maaned

    # Q3: Budget denne måned
    if not vis_alle:
        cur.execute("""
            SELECT ISNULL(SUM([BudgetAmount]),0) AS budget
            FROM [dbo].[BudgetsIntoMedia]
            WHERE [BudgetDate] >= %s AND [BudgetDate] < %s
        """, (month_from.isoformat(), month_to.isoformat()))
    else:
        cur.execute("""
            SELECT ISNULL(SUM([BudgetAmount]),0) AS budget
            FROM [dbo].[BudgetsIntoMedia]
            WHERE [BudgetDate] >= %s AND [BudgetDate] < %s
        """, (month_from.isoformat(), month_to.isoformat()))
    budget_maaned = float((cur.fetchone() or {}).get("budget", 0) or 0)

    # Q4: Samme periode sidste år (netto)
    ly_from = date(today.year - 1, today.month, 1)
    ly_next = today.month % 12 + 1
    ly_ny   = today.year - 1 + (1 if today.month == 12 else 0)
    ly_to   = date(ly_ny, ly_next, 1)

    cur.execute(f"""
        SELECT
            ISNULL(SUM(CASE WHEN [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
                THEN CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)) ELSE 0 END),0) AS won,
            ISNULL(SUM(CASE WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
                THEN CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)) ELSE 0 END),0) AS cancel
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND [won_time] >= %s AND [won_time] < %s
          {sub_filter}
    """, (ly_from.isoformat(), ly_to.isoformat()) + sub_params)
    ly_row    = cur.fetchone() or {}
    ly_netto  = float(ly_row.get("won", 0) or 0) - abs(float(ly_row.get("cancel", 0) or 0))

    # Q5: Churn/Netto per måned dette år (til bar chart)
    cur.execute(f"""
        SELECT
            MONTH([won_time]) AS maaned,
            ISNULL(SUM(CASE WHEN [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
                THEN CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)) ELSE 0 END),0) AS won,
            ISNULL(SUM(CASE WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
                THEN CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)) ELSE 0 END),0) AS cancel
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND [won_time] >= %s AND [won_time] < %s
          {sub_filter}
        GROUP BY MONTH([won_time])
        ORDER BY maaned
    """, (year_from.isoformat(), year_to.isoformat()) + sub_params)
    churn_raw = {r["maaned"]: r for r in cur.fetchall()}
    churn_chart = []
    for m in range(1, 13):
        r   = churn_raw.get(m, {"won": 0, "cancel": 0})
        won = float(r["won"] or 0)
        can = abs(float(r["cancel"] or 0))
        churn_chart.append({
            "maaned":  MONTH_NAMES_DA[m - 1][:3],
            "won":     round(won, 2),
            "cancel":  round(can, 2),
            "netto":   round(won - can, 2),
        })

    # Q6: Forecast denne måned
    cur.execute("""
        SELECT ISNULL(SUM([forecast_amount]),0) AS forecast
        FROM [dbo].[HubForecasts]
        WHERE [forecast_year] = %s AND [forecast_month] = %s AND [level] = 'medie'
    """, (today.year, today.month))
    forecast_maaned = float((cur.fetchone() or {}).get("forecast", 0) or 0)

    conn.close()

    vs_budget  = round(netto_maaned - budget_maaned, 2) if budget_maaned else None
    vs_budget_pct = round(netto_maaned / budget_maaned * 100, 1) if budget_maaned else None
    vs_ly      = round(netto_maaned - ly_netto, 2) if ly_netto else None
    vs_ly_pct  = round((netto_maaned - ly_netto) / abs(ly_netto) * 100, 1) if ly_netto else None

    return {
        "revenue_maaned":  round(revenue_maaned, 2),
        "cancel_maaned":   round(cancel_maaned, 2),
        "netto_maaned":    round(netto_maaned, 2),
        "budget_maaned":   round(budget_maaned, 2),
        "forecast_maaned": round(forecast_maaned, 2),
        "ly_netto":        round(ly_netto, 2),
        "vs_budget":       vs_budget,
        "vs_budget_pct":   vs_budget_pct,
        "vs_ly":           vs_ly,
        "vs_ly_pct":       vs_ly_pct,
        "churn_chart":     churn_chart,
        "vis_alle":        vis_alle,
        "month_label":     f"{MONTH_NAMES_DA[today.month-1]} {today.year}",
        "today":           today.isoformat(),
    }

#-----------------------------------------------------------------------------------------------------------------------
#                                          DET NYE DASHBOARD FOR SÆLGER
#-----------------------------------------------------------------------------------------------------------------------

def db_saelger_data(today: date, owner_name: str):
    month_from = date(today.year, today.month, 1)
    next_month = today.month % 12 + 1
    next_year  = today.year + (1 if today.month == 12 else 0)
    month_to   = date(next_year, next_month, 1)
    year_from  = date(today.year, 1, 1)
    year_to    = date(today.year + 1, 1, 1)

    brands_ph = "(" + ",".join(["%s"] * len(SUBSCRIPTION_BRANDS)) + ")"

    conn = get_conn()
    cur  = conn.cursor(as_dict=True)

    # Q1: Salg denne måned + budget
    cur.execute(f"""
        SELECT
            ISNULL(SUM(CASE WHEN [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
                THEN CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)) ELSE 0 END),0) AS won_amount,
            COUNT(CASE WHEN [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
                THEN 1 END) AS won_count,
            ISNULL(SUM(CASE WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
                THEN CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)) ELSE 0 END),0) AS cancel_amount
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND [won_time] >= %s AND [won_time] < %s
          AND [owner_name] = %s
          AND [sites] IN {brands_ph}
    """, (month_from.isoformat(), month_to.isoformat(), owner_name) + tuple(SUBSCRIPTION_BRANDS))
    row = cur.fetchone() or {}
    won_amount    = float(row.get("won_amount", 0) or 0)
    won_count     = int(row.get("won_count", 0) or 0)
    cancel_amount = abs(float(row.get("cancel_amount", 0) or 0))

    # Q2: Budget denne måned
    cur.execute("""
        SELECT ISNULL(SUM([BudgetAmount]),0) AS budget
        FROM [dbo].[SalespersonBudget]
        WHERE [BudgetDate] >= %s AND [BudgetDate] < %s
          AND [Owner] = %s
    """, (month_from.isoformat(), month_to.isoformat(), owner_name))
    budget = float((cur.fetchone() or {}).get("budget", 0) or 0)

    # Q3: Salg per måned dette år (til progress chart)
    cur.execute(f"""
        SELECT
            MONTH([won_time]) AS maaned,
            ISNULL(SUM(CASE WHEN [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
                THEN CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)) ELSE 0 END),0) AS won
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND [won_time] >= %s AND [won_time] < %s
          AND [owner_name] = %s
          AND [sites] IN {brands_ph}
        GROUP BY MONTH([won_time])
        ORDER BY maaned
    """, (year_from.isoformat(), year_to.isoformat(), owner_name) + tuple(SUBSCRIPTION_BRANDS))
    maaned_raw = {r["maaned"]: float(r["won"] or 0) for r in cur.fetchall()}
    maaned_chart = []
    for m in range(1, 13):
        maaned_chart.append({
            "maaned": MONTH_NAMES_DA[m - 1][:3],
            "won":    round(maaned_raw.get(m, 0), 2),
        })

    # Q4: Åben pipeline (expected close denne måned)
    cur.execute(f"""
        SELECT ISNULL(SUM(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2))),0) AS pipeline_value,
               COUNT(*) AS pipeline_count
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='open' AND [pipeline_name]<>'Web Sale'
          AND [expected_close_date] >= %s AND [expected_close_date] < %s
          AND [owner_name] = %s
          AND [sites] IN {brands_ph}
          AND COALESCE([value_dkk],[value]) > 0
    """, (month_from.isoformat(), month_to.isoformat(), owner_name) + tuple(SUBSCRIPTION_BRANDS))
    pipe_row      = cur.fetchone() or {}
    pipeline_value = float(pipe_row.get("pipeline_value", 0) or 0)
    pipeline_count = int(pipe_row.get("pipeline_count", 0) or 0)

    # Q5: Samme måned sidste år
    ly_from = date(today.year - 1, today.month, 1)
    ly_next = today.month % 12 + 1
    ly_ny   = today.year - 1 + (1 if today.month == 12 else 0)
    ly_to   = date(ly_ny, ly_next, 1)
    cur.execute(f"""
        SELECT ISNULL(SUM(CASE WHEN [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
            THEN CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)) ELSE 0 END),0) AS won
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND [won_time] >= %s AND [won_time] < %s
          AND [owner_name] = %s
          AND [sites] IN {brands_ph}
    """, (ly_from.isoformat(), ly_to.isoformat(), owner_name) + tuple(SUBSCRIPTION_BRANDS))
    ly_won = float((cur.fetchone() or {}).get("won", 0) or 0)

    # Q6: Seneste 5 deals
    cur.execute(f"""
        SELECT TOP 5
            [title], [sites],
            CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)) AS value,
            CONVERT(NVARCHAR(10), [won_time], 23) AS won_date,
            [deal_type]
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND [pipeline_name] NOT IN ('Cancellation','Cancellations','Opsigelser')
          AND [owner_name] = %s
          AND [sites] IN {brands_ph}
        ORDER BY [won_time] DESC
    """, (owner_name,) + tuple(SUBSCRIPTION_BRANDS))
    seneste_deals = [{"title": r["title"] or "(Uden titel)", "site": r["sites"] or "—",
                      "value": float(r["value"] or 0), "dato": r["won_date"] or "—",
                      "deal_type": r["deal_type"] or "—"}
                     for r in cur.fetchall()]

    conn.close()

    vs_budget_pct = round(won_amount / budget * 100, 1) if budget > 0 else None
    yoy_pct       = round((won_amount - ly_won) / abs(ly_won) * 100, 1) if ly_won else None

    return {
        "won_amount":      round(won_amount, 2),
        "won_count":       won_count,
        "cancel_amount":   round(cancel_amount, 2),
        "budget":          round(budget, 2),
        "vs_budget_pct":   vs_budget_pct,
        "pipeline_value":  round(pipeline_value, 2),
        "pipeline_count":  pipeline_count,
        "ly_won":          round(ly_won, 2),
        "yoy_pct":         yoy_pct,
        "maaned_chart":    maaned_chart,
        "seneste_deals":   seneste_deals,
        "owner_name":      owner_name,
        "month_label":     f"{MONTH_NAMES_DA[today.month-1]} {today.year}",
        "today":           today.isoformat(),
    }
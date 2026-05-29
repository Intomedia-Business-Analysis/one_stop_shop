import os
import traceback
from datetime import date, timedelta

import pymssql
from dotenv import load_dotenv

load_dotenv()

# ── Genbrugte konstanter fra modul_perf ─────────────────────────────────────

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
_SUB_PH = "(" + ",".join(["%s"] * len(SUBSCRIPTION_BRANDS)) + ")"

# Sales Performance KPI bruger kun disse brands (Power BI ekskluderer MarketWire)
SALES_PERF_BRANDS = [b for b in SUBSCRIPTION_BRANDS if b != "MarketWire"]
_SALES_PERF_PH = "(" + ",".join(["%s"] * len(SALES_PERF_BRANDS)) + ")"

CANCELLATION_PIPELINES = ["Cancellation", "Cancellations", "Opsigelser"]
_CANCEL_PH = "(" + ",".join(["%s"] * len(CANCELLATION_PIPELINES)) + ")"

_ADM_EXCLUDE = (
    "AND ([administrativ] IS NULL OR [administrativ] = '') "
    "AND UPPER(LTRIM([title])) NOT LIKE 'ADMINISTRATIV%' "
    "AND UPPER(LTRIM([title])) NOT LIKE 'ADM %' "
    "AND COALESCE([deal_type],'') <> 'Rapport' "
    "AND COALESCE([owner_name],'') NOT IN ('System Admin','')"
)

# Pipelines der tæller som subscription-salg (matcher Power BI)
SALES_PIPELINES = ['Company Trial', 'Customer', 'Newbizz']
_SALES_PIPELINES_PH = "(" + ",".join(["%s"] * len(SALES_PIPELINES)) + ")"

MONTH_NAMES_DA = [
    "Januar", "Februar", "Marts", "April", "Maj", "Juni",
    "Juli", "August", "September", "Oktober", "November", "December"
]

MONITOR_SITES = [
    "Seniormonitor", "Byrummonitor", "Idrætsmonitor", "Justitsmonitor",
    "Naturmonitor", "Socialmonitor", "Uddannelsesmonitor", "Klimamonitor",
    "Kulturmonitor", "Sundhedsmonitor", "Skolemonitor", "Turistmonitor",
    "All Monitor Sites", "Monitormedier",
]
WATCH_DK_SITES = [
    "FinansWatch DK", "Watch Medier DK", "ShippingWatch DK", "EjendomsWatch DK",
    "AdvokatWatch DK", "ITWatch DK", "EnergiWatch DK", "AgriWatch DK",
    "AMWatch DK", "KapitalWatch DK", "MedWatch DK", "FødevareWatch DK",
    "Fødevare Watch DK", "MediaWatch DK", "DetailWatch DK", "KForum", "Kforum DK",
    "All Watch Sites DK", "PolicyWatch DK", "Policy DK", "MobilityWatch DK", "CleantechWatch DK",
]
WATCH_INT_SITES = [
    "EnergiWatch NO", "TechWatch NO", "AdvokatWatch NO", "MatvareWatch NO",
    "MedWatch NO", "FinansWatch NO", "EiendomsWatch NO", "Kom24 NO",
    "HandelsWatch NO", "Medier24 NO", "All Watch Sites NO", "FinansWatch SE",
]
FINANS_SITES = ["FINANS DK"]
MARKETWIRE_SITES = ["MarketWire"]

MEDIA_BRAND_GROUPS = {
    "FINANS DK": ["FINANS DK"],
    "Monitor":   MONITOR_SITES,
    "Watch DK":  WATCH_DK_SITES,
    "Watch Int": WATCH_INT_SITES,
    "Watch NO": [
        "EnergiWatch NO", "TechWatch NO", "AdvokatWatch NO", "MatvareWatch NO",
        "MedWatch NO", "FinansWatch NO", "EiendomsWatch NO", "Kom24 NO",
        "HandelsWatch NO", "Medier24 NO", "All Watch Sites NO",
    ],
    "Watch SE": ["FinansWatch SE"],
}


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


def _week_range(today: date):
    # Power BI bruger søndag som ugestart (ikke ISO-mandag)
    days_since_sunday = (today.weekday() + 1) % 7
    start = today - timedelta(days=days_since_sunday)
    return start, start + timedelta(days=7)


def _month_range(today: date):
    m_start = date(today.year, today.month, 1)
    m_end = date(today.year + 1, 1, 1) if today.month == 12 else date(today.year, today.month + 1, 1)
    return m_start, m_end


def _year_range(today: date):
    return date(today.year, 1, 1), date(today.year + 1, 1, 1)


def _quarter_range(today: date):
    q = (today.month - 1) // 3
    q_start = date(today.year, q * 3 + 1, 1)
    q_end_month = q * 3 + 4
    q_end = date(today.year + 1, q_end_month - 12, 1) if q_end_month > 12 else date(today.year, q_end_month, 1)
    return q_start, q_end


def _quarter_label(today: date):
    return f"Qtr {(today.month - 1) // 3 + 1}"


# ── Fælles KPI-hjælper ───────────────────────────────────────────────────────

SALES_PERF_TEAMS = [
    "Team Monitor",
    "Team Watch DK", "Team Watch Int", "Team Watch SE",
    "Team FINANS DK", "Team FINANS Int",
]
_SALES_PERF_TEAMS_PH = "(" + ",".join(["%s"] * len(SALES_PERF_TEAMS)) + ")"

# Subquery der begrænser owner_name til aktive medlemmer af de relevante teams.
# Watch DK og FINANS DK deler samme sælgere — ligesom Watch Int og FINANS Int.
_OWNER_IN_TEAMS_SQL = f"""
    AND [owner_name] IN (
        SELECT u.name FROM [dbo].[HubUsers] u
        JOIN [dbo].[TeamMemberships] tm ON tm.user_id = u.id
        JOIN [dbo].[Teams] t ON t.id = tm.team_id
        WHERE t.name IN {_SALES_PERF_TEAMS_PH}
          AND (tm.end_date IS NULL OR TRY_CAST(tm.end_date AS DATE) >= CAST(GETDATE() AS DATE))
    )
"""


def _revenue_kpis(cur, today: date, pipeline_filter: str, date_col: str = "won_time"):
    week_start, week_end = _week_range(today)
    m_start, m_end = _month_range(today)
    y_start, y_end = _year_range(today)

    if pipeline_filter == "subscription":
        # Gross Revenue — kun Monitor/Watch/FINANS teams på subscription sites.
        # dag/uge/måned bruger won_time; år bruger service_activation_date (aktiveringstidspunkt).
        def _sum(date_from, date_to, expr="[won_time]"):
            cur.execute(f"""
                SELECT ISNULL(SUM(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2))),0) AS total
                FROM [dbo].[PipedriveDeals]
                WHERE [status]='won'
                  AND {expr} >= %s
                  AND {expr} < %s
                  AND [pipeline_name] NOT IN {_CANCEL_PH}
                  AND [pipeline_name] NOT IN ('banner','job','Web Sale')
                  AND [sites] IN {_SALES_PERF_PH}
                  AND [team] IN {_SALES_PERF_TEAMS_PH}
                  {_ADM_EXCLUDE}
            """, (date_from.isoformat(), date_to.isoformat())
                + tuple(CANCELLATION_PIPELINES)
                + tuple(SALES_PERF_BRANDS)
                + tuple(SALES_PERF_TEAMS))
            return float((cur.fetchone() or {}).get("total", 0) or 0)
    else:
        if pipeline_filter == "banner":
            pipe_clause = "AND [pipeline_name] = 'banner' AND [account] = 'jppol_advertising'"
        elif pipeline_filter == "job":
            pipe_clause = "AND [pipeline_name] = 'job' AND [account] = 'jppol_advertising'"
        else:  # advertising = banner + job
            pipe_clause = "AND [pipeline_name] IN ('banner','job') AND [account] = 'jppol_advertising'"

        def _sum(date_from, date_to):
            cur.execute(f"""
                SELECT ISNULL(SUM(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2))),0) AS total
                FROM [dbo].[PipedriveDeals]
                WHERE [status]='won'
                  AND [won_time] >= %s AND [won_time] < %s
                  {pipe_clause}
            """, (date_from.isoformat(), date_to.isoformat()))
            return float((cur.fetchone() or {}).get("total", 0) or 0)

    sad_expr = "COALESCE([service_activation_date],[won_time])"
    return {
        "dag":    round(_sum(today, today + timedelta(days=1)), 2),
        "uge":    round(_sum(week_start, week_end), 2),
        "maaned": round(_sum(m_start, m_end), 2),
        "aar":    round(_sum(y_start, y_end, expr=sad_expr), 2),
    }


# ════════════════════════════════════════════════════════════════════════════
#  DASHBOARD 1 — Sales Performance (Monitor, Watch & FINANS)
# ════════════════════════════════════════════════════════════════════════════

def db_sales_performance(today: date, date_col: str = "won_time"):
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)

        kpis = _revenue_kpis(cur, today, "subscription", date_col=date_col)
        q_start, q_end = _quarter_range(today)
        m_start, m_end = _month_range(today)

        def _team_netto_budget(date_from, date_to):
            cur.execute(f"""
                SELECT [team],
                    ISNULL(SUM(CASE WHEN [pipeline_name] NOT IN {_CANCEL_PH}
                        THEN CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)) ELSE 0 END),0) AS won,
                    ABS(ISNULL(SUM(CASE WHEN [pipeline_name] IN {_CANCEL_PH}
                        THEN CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)) ELSE 0 END),0)) AS cancel
                FROM [dbo].[PipedriveDeals]
                WHERE [status]='won' AND [pipeline_name] NOT IN ('banner','job','Web Sale')
                  AND [won_time] >= %s AND [won_time] < %s
                  AND [sites] IN {_SALES_PERF_PH}
                  AND [team] IS NOT NULL AND [team] <> ''
                  {_ADM_EXCLUDE}
                GROUP BY [team]
            """, tuple(CANCELLATION_PIPELINES) * 2 + (date_from.isoformat(), date_to.isoformat()) + tuple(SALES_PERF_BRANDS))
            netto_map = {r["team"]: round(float(r["won"] or 0) - float(r["cancel"] or 0), 2) for r in cur.fetchall()}

            cur.execute("""
                SELECT [Team] AS team, SUM([BudgetAmount]) AS budget
                FROM [dbo].[SalespersonBudget]
                WHERE [BudgetDate] >= %s AND [BudgetDate] < %s
                  AND [Team] IS NOT NULL AND [Team] <> ''
                GROUP BY [Team]
            """, (date_from.isoformat(), date_to.isoformat()))
            budget_map = {r["team"]: float(r["budget"] or 0) for r in cur.fetchall()}

            all_teams = set(list(netto_map.keys()) + list(budget_map.keys()))
            rows = [{"team": t, "netto": netto_map.get(t, 0.0), "budget": round(budget_map.get(t, 0.0), 2)} for t in all_teams]
            rows.sort(key=lambda x: x["team"])
            return rows

        kvartal_chart = _team_netto_budget(q_start, q_end)
        maaned_chart  = _team_netto_budget(m_start, m_end)

        # Deals oprettet — kun sælgere på de relevante teams (ekskl. Norge)
        try:
            cur.execute(f"""
                SELECT COALESCE([owner_name],'Ukendt') AS owner_name, COUNT(*) AS deals
                FROM [dbo].[PipedriveDeals]
                WHERE [add_time] >= %s AND [add_time] < %s
                  AND [pipeline_name] IN {_SALES_PIPELINES_PH}
                  AND [sites] IN {_SALES_PERF_PH}
                  {_OWNER_IN_TEAMS_SQL}
                  {_ADM_EXCLUDE}
                GROUP BY [owner_name] ORDER BY deals DESC
            """, (m_start.isoformat(), m_end.isoformat()) + tuple(SALES_PIPELINES) + tuple(SALES_PERF_BRANDS) + tuple(SALES_PERF_TEAMS))
            deals_oprettet = [{"owner_name": r["owner_name"], "deals": int(r["deals"] or 0)} for r in cur.fetchall()]
        except Exception:
            deals_oprettet = []

        # Deals vundet — kun sælgere på de relevante teams (ekskl. Norge)
        cur.execute(f"""
            SELECT COALESCE([owner_name],'Ukendt') AS owner_name, COUNT(*) AS deals
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won'
              AND [pipeline_name] IN {_SALES_PIPELINES_PH}
              AND [close_time] >= %s AND [close_time] < %s
              AND [sites] IN {_SALES_PERF_PH}
              {_OWNER_IN_TEAMS_SQL}
              {_ADM_EXCLUDE}
            GROUP BY [owner_name] ORDER BY deals DESC
        """, tuple(SALES_PIPELINES) + (m_start.isoformat(), m_end.isoformat()) + tuple(SALES_PERF_BRANDS) + tuple(SALES_PERF_TEAMS))
        deals_vundet = [{"owner_name": r["owner_name"], "deals": int(r["deals"] or 0)} for r in cur.fetchall()]

        # Deals omsætning — kun sælgere på de relevante teams (ekskl. Norge)
        cur.execute(f"""
            SELECT COALESCE([owner_name],'Ukendt') AS owner_name,
                   ISNULL(SUM(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2))),0) AS revenue
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won'
              AND [pipeline_name] IN {_SALES_PIPELINES_PH}
              AND [won_time] >= %s AND [won_time] < %s
              AND [sites] IN {_SALES_PERF_PH}
              {_OWNER_IN_TEAMS_SQL}
              {_ADM_EXCLUDE}
            GROUP BY [owner_name] ORDER BY revenue DESC
        """, tuple(SALES_PIPELINES) + (m_start.isoformat(), m_end.isoformat()) + tuple(SALES_PERF_BRANDS) + tuple(SALES_PERF_TEAMS))
        deals_omsaetning = [{"owner_name": r["owner_name"], "revenue": round(float(r["revenue"] or 0), 2)} for r in cur.fetchall()]

        # Seneste deals vundet — kun subscription teams, ingen opsigelser
        cur.execute(f"""
            SELECT TOP 20
                COALESCE([owner_name],'Ukendt') AS owner_name,
                COALESCE([org_name],'')         AS org_name,
                COALESCE([team],'')             AS team,
                COALESCE([sites],'')            AS sites,
                CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)) AS value,
                CONVERT(VARCHAR(19),[won_time],120) AS won_time
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won'
              AND [pipeline_name] NOT IN {_CANCEL_PH}
              AND [pipeline_name] NOT IN ('banner','job','Web Sale')
              AND [sites] IN {_SALES_PERF_PH}
              AND [team] IN {_SALES_PERF_TEAMS_PH}
              {_ADM_EXCLUDE}
            ORDER BY [won_time] DESC
        """, tuple(CANCELLATION_PIPELINES) + tuple(SALES_PERF_BRANDS) + tuple(SALES_PERF_TEAMS))
        seneste_deals = [{"owner_name": r["owner_name"], "org_name": r["org_name"], "team": r["team"],
                          "sites": r["sites"], "value": float(r["value"] or 0), "won_time": str(r["won_time"] or "")}
                         for r in cur.fetchall()]

        conn.close()
        return {
            "kpis": kpis, "kvartal_chart": kvartal_chart, "maaned_chart": maaned_chart,
            "deals_oprettet": deals_oprettet, "deals_vundet": deals_vundet,
            "deals_omsaetning": deals_omsaetning, "seneste_deals": seneste_deals,
            "maaned_label": MONTH_NAMES_DA[today.month - 1] + " " + str(today.year),
            "kvartal_label": _quarter_label(today) + " " + str(today.year),
            "today": today.isoformat(),
        }
    except Exception:
        traceback.print_exc()
        return {}


# ════════════════════════════════════════════════════════════════════════════
#  DASHBOARD 2 — Department Performance
# ════════════════════════════════════════════════════════════════════════════

def _dept_sub_panel(cur, m_start, m_end, sites, teams):
    sites_ph = "(" + ",".join(["%s"] * len(sites)) + ")"
    teams_ph = "(" + ",".join(["%s"] * len(teams)) + ")"
    cur.execute(f"""
        SELECT [team],
            ISNULL(SUM(CASE WHEN [pipeline_name] NOT IN {_CANCEL_PH}
                THEN CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)) ELSE 0 END),0) AS won,
            ABS(ISNULL(SUM(CASE WHEN [pipeline_name] IN {_CANCEL_PH}
                THEN CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)) ELSE 0 END),0)) AS cancel
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name] NOT IN ('banner','job','Web Sale')
          AND [won_time] >= %s AND [won_time] < %s
          AND [sites] IN {sites_ph}
          AND [team] IN {teams_ph}
          {_ADM_EXCLUDE}
        GROUP BY [team]
    """, tuple(CANCELLATION_PIPELINES) * 2 + (m_start.isoformat(), m_end.isoformat()) + tuple(sites) + tuple(teams))
    netto_map = {r["team"]: round(float(r["won"] or 0) - float(r["cancel"] or 0), 2) for r in cur.fetchall()}

    cur.execute(f"""
        SELECT [Team] AS team, SUM([BudgetAmount]) AS budget
        FROM [dbo].[SalespersonBudget]
        WHERE [BudgetDate] >= %s AND [BudgetDate] < %s AND [Team] IN {teams_ph}
        GROUP BY [Team]
    """, (m_start.isoformat(), m_end.isoformat()) + tuple(teams))
    budget_map = {r["team"]: float(r["budget"] or 0) for r in cur.fetchall()}

    return [{"team": t, "netto": netto_map.get(t, 0.0), "budget": round(budget_map.get(t, 0.0), 2)} for t in teams]


def _dept_adv_panel(cur, m_start, m_end, pipeline, brand_sites_map):
    deal_type = "Banner" if pipeline == "banner" else "Job"
    rows = []
    for label, site_list in brand_sites_map.items():
        sites_ph = "(" + ",".join(["%s"] * len(site_list)) + ")"
        cur.execute(f"""
            SELECT ISNULL(SUM(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2))),0) AS revenue
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won' AND [pipeline_name] = %s AND [account] = 'jppol_advertising'
              AND [won_time] >= %s AND [won_time] < %s AND [sites] IN {sites_ph}
        """, (pipeline, m_start.isoformat(), m_end.isoformat()) + tuple(site_list))
        revenue = float((cur.fetchone() or {}).get("revenue", 0) or 0)

        cur.execute("""
            SELECT ISNULL(SUM([BudgetAmount]),0) AS budget FROM [dbo].[BudgetsIntoMedia]
            WHERE [DealType] = %s AND [Brand] = %s AND [BudgetDate] >= %s AND [BudgetDate] < %s
        """, (deal_type, label, m_start.isoformat(), m_end.isoformat()))
        budget = float((cur.fetchone() or {}).get("budget", 0) or 0)
        rows.append({"brand": label, "revenue": round(revenue, 2), "budget": round(budget, 2)})
    return rows


def db_department_performance(today: date):
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        m_start, m_end = _month_range(today)

        monitor_rows = _dept_sub_panel(cur, m_start, m_end, MONITOR_SITES, ["Team Monitor"])
        watch_rows   = _dept_sub_panel(cur, m_start, m_end, WATCH_DK_SITES + WATCH_INT_SITES,
                                       ["Team Watch DK", "Team Watch Int", "Team Watch SE"])
        finans_rows  = _dept_sub_panel(cur, m_start, m_end, FINANS_SITES, ["Team FINANS DK", "Team FINANS Int"])

        adv_brands = {"Watch DK": ["Watch DK"], "FINANS DK": ["FINANS DK"], "Monitor": ["Monitor"]}
        banner_rows = _dept_adv_panel(cur, m_start, m_end, "banner", adv_brands)
        job_rows    = _dept_adv_panel(cur, m_start, m_end, "job",    adv_brands)

        cur.execute(f"""
            SELECT ISNULL(SUM(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2))),0) AS revenue
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won' AND [pipeline_name] NOT IN {_CANCEL_PH}
              AND [pipeline_name] NOT IN ('banner','job','Web Sale')
              AND [won_time] >= %s AND [won_time] < %s AND [sites] = 'MarketWire'
              {_ADM_EXCLUDE}
        """, tuple(CANCELLATION_PIPELINES) + (m_start.isoformat(), m_end.isoformat()))
        mw_revenue = float((cur.fetchone() or {}).get("revenue", 0) or 0)

        cur.execute("SELECT ISNULL(SUM([BudgetAmount]),0) AS budget FROM [dbo].[BudgetsIntoMedia] WHERE [Brand]='marketwire' AND [BudgetDate] >= %s AND [BudgetDate] < %s",
                    (m_start.isoformat(), m_end.isoformat()))
        mw_budget = float((cur.fetchone() or {}).get("budget", 0) or 0)

        conn.close()
        return {
            "monitor_rows": monitor_rows, "watch_rows": watch_rows, "finans_rows": finans_rows,
            "banner_rows": banner_rows, "job_rows": job_rows,
            "marketwire_rows": [{"brand": "MarketWire", "revenue": round(mw_revenue, 2), "budget": round(mw_budget, 2)}],
            "maaned_label": MONTH_NAMES_DA[today.month - 1] + " " + str(today.year),
            "today": today.isoformat(),
        }
    except Exception:
        traceback.print_exc()
        return {}


# ════════════════════════════════════════════════════════════════════════════
#  Fælles hjælpere til Dashboard 3 + 4 (reklame)
# ════════════════════════════════════════════════════════════════════════════

def _adv_kvartal_by_brand(cur, today, pipeline):
    q_start, q_end = _quarter_range(today)
    pipeline_clause = f"[pipeline_name] IN ('banner','job')" if pipeline == "advertising" else f"[pipeline_name] = '{pipeline}'"
    cur.execute(f"""
        SELECT COALESCE([sites],'Ukendt') AS brand, [pipeline_name] AS pipeline,
               ISNULL(SUM(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2))),0) AS revenue
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND {pipeline_clause} AND [account]='jppol_advertising'
          AND [won_time] >= %s AND [won_time] < %s
          AND [sites] IS NOT NULL AND [sites] <> ''
        GROUP BY [sites], [pipeline_name] ORDER BY revenue DESC
    """, (q_start.isoformat(), q_end.isoformat()))
    return [{"brand": r["brand"], "pipeline": r["pipeline"], "revenue": round(float(r["revenue"] or 0), 2)} for r in cur.fetchall()]


def _adv_deals_oprettet(cur, m_start, m_end, pipeline):
    try:
        pipeline_clause = "[pipeline_name] IN ('banner','job')" if pipeline == "advertising" else f"[pipeline_name] = '{pipeline}'"
        cur.execute(f"""
            SELECT COALESCE([owner_name],'Ukendt') AS owner_name, COUNT(*) AS deals
            FROM [dbo].[PipedriveDeals]
            WHERE [add_time] >= %s AND [add_time] < %s
              AND {pipeline_clause} AND [account]='jppol_advertising'
            GROUP BY [owner_name] ORDER BY deals DESC
        """, (m_start.isoformat(), m_end.isoformat()))
        return [{"owner_name": r["owner_name"], "deals": int(r["deals"] or 0)} for r in cur.fetchall()]
    except Exception:
        return []


def _adv_deals_vundet(cur, m_start, m_end, pipeline):
    pipeline_clause = "[pipeline_name] IN ('banner','job')" if pipeline == "advertising" else f"[pipeline_name] = '{pipeline}'"
    cur.execute(f"""
        SELECT COALESCE([owner_name],'Ukendt') AS owner_name, COUNT(*) AS deals
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND {pipeline_clause} AND [account]='jppol_advertising'
          AND [won_time] >= %s AND [won_time] < %s
        GROUP BY [owner_name] ORDER BY deals DESC
    """, (m_start.isoformat(), m_end.isoformat()))
    return [{"owner_name": r["owner_name"], "deals": int(r["deals"] or 0)} for r in cur.fetchall()]


def _adv_omsaetning_by_owner(cur, m_start, m_end, pipeline):
    pipeline_clause = "[pipeline_name] IN ('banner','job')" if pipeline == "advertising" else f"[pipeline_name] = '{pipeline}'"
    cur.execute(f"""
        SELECT COALESCE([owner_name],'Ukendt') AS owner_name,
               ISNULL(SUM(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2))),0) AS revenue
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND {pipeline_clause} AND [account]='jppol_advertising'
          AND [won_time] >= %s AND [won_time] < %s
        GROUP BY [owner_name] ORDER BY revenue DESC
    """, (m_start.isoformat(), m_end.isoformat()))
    return [{"owner_name": r["owner_name"], "revenue": round(float(r["revenue"] or 0), 2)} for r in cur.fetchall()]


def _adv_budget_by_brand(cur, m_start, m_end, pipeline):
    brands = ["Watch DK", "FINANS DK", "Monitor"]
    deal_type = "Banner" if pipeline == "banner" else "Job"
    rows = []
    for brand in brands:
        cur.execute("""
            SELECT ISNULL(SUM(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2))),0) AS revenue
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won' AND [pipeline_name]=%s AND [account]='jppol_advertising'
              AND [won_time] >= %s AND [won_time] < %s AND [sites]=%s
        """, (pipeline, m_start.isoformat(), m_end.isoformat(), brand))
        revenue = float((cur.fetchone() or {}).get("revenue", 0) or 0)

        cur.execute("""
            SELECT ISNULL(SUM([BudgetAmount]),0) AS budget FROM [dbo].[BudgetsIntoMedia]
            WHERE [DealType]=%s AND [Brand]=%s AND [BudgetDate] >= %s AND [BudgetDate] < %s
        """, (deal_type, brand, m_start.isoformat(), m_end.isoformat()))
        budget = float((cur.fetchone() or {}).get("budget", 0) or 0)
        rows.append({"brand": brand, "revenue": round(revenue, 2), "budget": round(budget, 2)})
    return rows


def _adv_seneste_deals(cur, pipeline):
    pipeline_clause = "[pipeline_name] IN ('banner','job')" if pipeline == "advertising" else f"[pipeline_name] = '{pipeline}'"
    cur.execute(f"""
        SELECT TOP 20
            COALESCE([owner_name],'Ukendt') AS owner_name,
            COALESCE([org_name],'')         AS org_name,
            COALESCE([sites],'')            AS brand,
            CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2)) AS value,
            CONVERT(VARCHAR(19),[won_time],120) AS won_time
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND {pipeline_clause} AND [account]='jppol_advertising'
        ORDER BY [won_time] DESC
    """)
    return [{"owner_name": r["owner_name"], "org_name": r["org_name"], "brand": r["brand"],
             "value": float(r["value"] or 0), "won_time": str(r["won_time"] or "")} for r in cur.fetchall()]


# ════════════════════════════════════════════════════════════════════════════
#  DASHBOARD 3 — Banner Performance
# ════════════════════════════════════════════════════════════════════════════

def db_banner_performance(today: date):
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        m_start, m_end = _month_range(today)

        # Programmatisk salg (forsøg — kolonnen kan variere)
        try:
            cur.execute("""
                SELECT COALESCE([sites],'Ukendt') AS brand,
                       ISNULL(SUM(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2))),0) AS revenue
                FROM [dbo].[PipedriveDeals]
                WHERE [status]='won' AND [pipeline_name]='banner' AND [account]='jppol_advertising'
                  AND [won_time] >= %s AND [won_time] < %s
                  AND LOWER(COALESCE([sales_type],'')) LIKE '%programmatisk%'
                  AND [sites] IS NOT NULL
                GROUP BY [sites] ORDER BY revenue DESC
            """, (m_start.isoformat(), m_end.isoformat()))
            programmatisk = [{"brand": r["brand"], "revenue": round(float(r["revenue"] or 0), 2), "budget": 0.0} for r in cur.fetchall()]
            # Budget for programmatisk
            cur.execute("""
                SELECT ISNULL(SUM([BudgetAmount]),0) AS budget FROM [dbo].[BudgetsIntoMedia]
                WHERE [DealType]='Banner' AND [BudgetDate] >= %s AND [BudgetDate] < %s
                  AND LOWER(COALESCE([SalesType],'')) LIKE '%programmatisk%'
            """, (m_start.isoformat(), m_end.isoformat()))
            prog_budget_total = float((cur.fetchone() or {}).get("budget", 0) or 0)
            if programmatisk:
                programmatisk[0]["budget"] = prog_budget_total
        except Exception:
            programmatisk = []

        result = {
            "kpis":             _revenue_kpis(cur, today, "banner"),
            "kvartal_chart":    _adv_kvartal_by_brand(cur, today, "banner"),
            "deals_oprettet":   _adv_deals_oprettet(cur, m_start, m_end, "banner"),
            "deals_vundet":     _adv_deals_vundet(cur, m_start, m_end, "banner"),
            "deals_omsaetning": _adv_omsaetning_by_owner(cur, m_start, m_end, "banner"),
            "budget_chart":     _adv_budget_by_brand(cur, m_start, m_end, "banner"),
            "programmatisk":    programmatisk,
            "seneste_deals":    _adv_seneste_deals(cur, "banner"),
            "maaned_label":     MONTH_NAMES_DA[today.month - 1] + " " + str(today.year),
            "kvartal_label":    _quarter_label(today) + " " + str(today.year),
            "today":            today.isoformat(),
        }
        conn.close()
        return result
    except Exception:
        traceback.print_exc()
        return {}


# ════════════════════════════════════════════════════════════════════════════
#  DASHBOARD 4 — Advertising Performance (Banner + Job)
# ════════════════════════════════════════════════════════════════════════════

def db_advertising_performance(today: date):
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        m_start, m_end = _month_range(today)

        result = {
            "kpis":               _revenue_kpis(cur, today, "advertising"),
            "kvartal_chart":      _adv_kvartal_by_brand(cur, today, "advertising"),
            "deals_oprettet":     _adv_deals_oprettet(cur, m_start, m_end, "advertising"),
            "deals_vundet":       _adv_deals_vundet(cur, m_start, m_end, "advertising"),
            "deals_omsaetning":   _adv_omsaetning_by_owner(cur, m_start, m_end, "advertising"),
            "banner_budget_chart": _adv_budget_by_brand(cur, m_start, m_end, "banner"),
            "job_budget_chart":   _adv_budget_by_brand(cur, m_start, m_end, "job"),
            "seneste_deals":      _adv_seneste_deals(cur, "advertising"),
            "maaned_label":       MONTH_NAMES_DA[today.month - 1] + " " + str(today.year),
            "kvartal_label":      _quarter_label(today) + " " + str(today.year),
            "today":              today.isoformat(),
        }
        conn.close()
        return result
    except Exception:
        traceback.print_exc()
        return {}


# ════════════════════════════════════════════════════════════════════════════
#  DASHBOARD 5 — Media Performance
# ════════════════════════════════════════════════════════════════════════════

def db_media_performance(selected_brands: list | None = None, selected_years: list | None = None):
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)

        if selected_brands:
            sites: list = []
            seen: set = set()
            for b in selected_brands:
                for s in MEDIA_BRAND_GROUPS.get(b, []):
                    if s not in seen:
                        sites.append(s)
                        seen.add(s)
        else:
            sites = list(SUBSCRIPTION_BRANDS)

        if not sites:
            conn.close()
            return {"rows": [], "total": {}, "available_years": [], "available_brands": list(MEDIA_BRAND_GROUPS.keys())}

        sites_ph = "(" + ",".join(["%s"] * len(sites)) + ")"

        year_clause = ""
        year_params: tuple = ()
        if selected_years:
            year_ph = "(" + ",".join(["%s"] * len(selected_years)) + ")"
            year_clause = f"AND YEAR([won_time]) IN {year_ph}"
            year_params = tuple(int(y) for y in selected_years)

        cur.execute(f"""
            SELECT [sites],
                   ISNULL(SUM(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2))),0) AS gross
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won' AND [pipeline_name] NOT IN {_CANCEL_PH}
              AND [pipeline_name] NOT IN ('banner','job','Web Sale')
              AND [sites] IN {sites_ph} {year_clause} {_ADM_EXCLUDE}
            GROUP BY [sites]
        """, tuple(CANCELLATION_PIPELINES) + tuple(sites) + year_params)
        gross_map = {r["sites"]: float(r["gross"] or 0) for r in cur.fetchall()}

        cur.execute(f"""
            SELECT [sites],
                   ABS(ISNULL(SUM(CAST(COALESCE([value_dkk],[value]) AS DECIMAL(18,2))),0)) AS cancel
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won' AND [pipeline_name] IN {_CANCEL_PH}
              AND [sites] IN {sites_ph} {year_clause}
            GROUP BY [sites]
        """, tuple(CANCELLATION_PIPELINES) + tuple(sites) + year_params)
        cancel_map = {r["sites"]: float(r["cancel"] or 0) for r in cur.fetchall()}

        year_budget_clause = ""
        year_budget_params: tuple = ()
        if selected_years:
            year_ph2 = "(" + ",".join(["%s"] * len(selected_years)) + ")"
            year_budget_clause = f"AND YEAR([BudgetDate]) IN {year_ph2}"
            year_budget_params = tuple(int(y) for y in selected_years)

        cur.execute(f"""
            SELECT [Brand] AS site, ISNULL(SUM([BudgetAmount]),0) AS budget
            FROM [dbo].[BudgetsIntoMedia]
            WHERE [Brand] IN {sites_ph} {year_budget_clause}
            GROUP BY [Brand]
        """, tuple(sites) + year_budget_params)
        budget_map = {r["site"]: float(r["budget"] or 0) for r in cur.fetchall()}

        cur.execute(f"""
            SELECT DISTINCT YEAR([won_time]) AS aar FROM [dbo].[PipedriveDeals]
            WHERE [status]='won' AND [sites] IN {sites_ph} ORDER BY aar DESC
        """, tuple(sites))
        available_years = [int(r["aar"]) for r in cur.fetchall()]

        conn.close()

        rows = []
        for site in sites:
            gross  = round(gross_map.get(site, 0.0), 2)
            cancel = round(cancel_map.get(site, 0.0), 2)
            net    = round(gross - cancel, 2)
            budget = round(budget_map.get(site, 0.0), 2)
            index  = round(net / budget * 100, 2) if budget > 0 else None
            if gross == 0 and cancel == 0 and budget == 0:
                continue
            rows.append({"site": site, "gross": gross, "cancel": cancel, "net": net, "budget": budget, "index": index})

        rows.sort(key=lambda x: -(x["net"] or 0))

        tg = round(sum(r["gross"]  for r in rows), 2)
        tc = round(sum(r["cancel"] for r in rows), 2)
        tn = round(sum(r["net"]    for r in rows), 2)
        tb = round(sum(r["budget"] for r in rows), 2)

        return {
            "rows":             rows,
            "total":            {"gross": tg, "cancel": tc, "net": tn, "budget": tb, "index": round(tn / tb * 100, 2) if tb > 0 else None},
            "available_years":  available_years,
            "available_brands": list(MEDIA_BRAND_GROUPS.keys()),
        }
    except Exception:
        traceback.print_exc()
        return {"rows": [], "total": {}, "available_years": [], "available_brands": list(MEDIA_BRAND_GROUPS.keys())}

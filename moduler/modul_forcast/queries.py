import os
import traceback
from datetime import date

import pymssql
from dotenv import load_dotenv
from fastapi import HTTPException

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
_SUB_PH = "(" + ",".join(["%s"] * len(SUBSCRIPTION_BRANDS)) + ")"

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

BRAND_LABELS: dict[str, str] = {
    "watch_dk":   "Watch DK",
    "finans":     "FINANS DK",
    "watch_no":   "Watch NO",
    "watch_se":   "Watch SE",
    "watch_de":   "Watch DE",
    "finans_int": "FINANS Int",
    "monitor":    "Monitor",
    "marketwire": "MarketWire",
}

_CANCEL_PIPELINES = ["Cancellation", "Cancellations", "Opsigelser"]
_CANCEL_PH = "(" + ",".join(["%s"] * len(_CANCEL_PIPELINES)) + ")"

# Teams der bruger advertising-pipeline i stedet for abonnement-deals
ADVERTISING_TEAMS: dict[str, str] = {
    "Team Job":    "job",
    "Team Banner": "banner",
}


def get_conn():
    return pymssql.connect(
        server=os.getenv("DB_SERVER"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME", "INTOMEDIA"),
        tds_version="7.0",
        login_timeout=5,
        timeout=10,
    )


def ensure_schema():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            IF NOT EXISTS (
                SELECT * FROM sys.columns
                WHERE object_id = OBJECT_ID('HubForecasts') AND name = 'adjustment_pct'
            )
            ALTER TABLE HubForecasts ADD adjustment_pct DECIMAL(5,2) NULL
        """)
        conn.commit()
        conn.close()
    except Exception:
        pass


def build_team_filter(team: str | None, team_brand: str | None):
    if team_brand and team_brand in BRAND_GROUPS:
        site_list = BRAND_GROUPS[team_brand]
    else:
        site_list = SUBSCRIPTION_BRANDS

    sites_ph = "(" + ",".join(["%s"] * len(site_list)) + ")"

    if team:
        owner_clause = f"""
            AND [owner_name] IN (
                SELECT u.name
                FROM   HubUsers u
                JOIN   TeamMemberships tm ON tm.user_id = u.id
                JOIN   Teams t           ON t.id = tm.team_id
                WHERE  t.name = %s
                  AND  (tm.end_date IS NULL OR tm.end_date >= GETDATE())
            )
        """
        owner_params = [team]
    else:
        owner_clause = ""
        owner_params = []

    site_clause = f"AND [sites] IN {sites_ph}"
    return site_clause, site_list, owner_clause, owner_params


def db_get_teams():
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute("""
            SELECT t.name, ISNULL(t.brand, '') AS brand
            FROM   Teams t
            WHERE  t.name IS NOT NULL
            ORDER BY t.name
        """)
        teams = [{"name": r["name"], "brand": r["brand"]} for r in cur.fetchall()]
        conn.close()
        return teams
    except Exception:
        # Fallback
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute("""
            SELECT DISTINCT [Team] AS name, '' AS brand
            FROM [dbo].[SalesPersonBudget]
            WHERE [Team] IS NOT NULL
            ORDER BY [Team]
        """)
        teams = [{"name": r["name"], "brand": ""} for r in cur.fetchall()]
        conn.close()
        return teams


def db_forecast_data(year: int, month: int, level: str, team: str | None, team_brand: str | None):
    year_m1 = year - 1
    year_m2 = year - 2

    # Byg datointerval strings
    import calendar
    last_day = calendar.monthrange(year, month)[1]
    last_day_m1 = calendar.monthrange(year_m1, month)[1]
    last_day_m2 = calendar.monthrange(year_m2, month)[1]

    date_cur  = (f"{year}-{month:02d}-01",    f"{year}-{month:02d}-{last_day}")
    date_m1   = (f"{year_m1}-{month:02d}-01", f"{year_m1}-{month:02d}-{last_day_m1}")
    date_m2   = (f"{year_m2}-{month:02d}-01", f"{year_m2}-{month:02d}-{last_day_m2}")

    site_clause, site_list, owner_clause, owner_params = build_team_filter(team, team_brand)
    base_site_params = tuple(site_list)

    conn = get_conn()
    cur = conn.cursor(as_dict=True)

    # ── Q1: Historisk tilvækst (year-1 og year-2) ──────────────────────────
    if level == "saelger":
        if team and team in ADVERTISING_TEAMS:
            adv_pipeline = ADVERTISING_TEAMS[team]
            cur.execute("""
                SELECT [owner_name] AS dimension_key,
                       YEAR([service_activation_date]) AS data_year,
                       SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS tilvækst
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'won'
                  AND [pipeline_name] = %s
                  AND [team] = %s
                  AND [owner_name] IS NOT NULL
                  AND [service_activation_date] BETWEEN %s AND %s
                GROUP BY [owner_name], YEAR([service_activation_date])
                UNION ALL
                SELECT [owner_name] AS dimension_key,
                       YEAR([service_activation_date]) AS data_year,
                       SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS tilvækst
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'won'
                  AND [pipeline_name] = %s
                  AND [team] = %s
                  AND [owner_name] IS NOT NULL
                  AND [service_activation_date] BETWEEN %s AND %s
                GROUP BY [owner_name], YEAR([service_activation_date])
            """, (adv_pipeline, team, date_m1[0], date_m1[1],
                  adv_pipeline, team, date_m2[0], date_m2[1]))
        else:
            cur.execute(f"""
                SELECT [owner_name] AS dimension_key,
                       YEAR([service_activation_date]) AS data_year,
                       SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS tilvækst
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'won'
                  AND [pipeline_name] <> 'Web sale'
                  AND [deal_type] IN ('Abonnement', 'Subscription')
                  AND [administrativ] IS NULL
                  AND [owner_name] IS NOT NULL
                  AND [service_activation_date] BETWEEN %s AND %s
                  {owner_clause}
                  AND [team] = %s
                GROUP BY [owner_name], YEAR([service_activation_date])
                UNION ALL
                SELECT [owner_name] AS dimension_key,
                       YEAR([service_activation_date]) AS data_year,
                       SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS tilvækst
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'won'
                  AND [pipeline_name] <> 'Web sale'
                  AND [deal_type] IN ('Abonnement', 'Subscription')
                  AND [administrativ] IS NULL
                  AND [owner_name] IS NOT NULL
                  AND [service_activation_date] BETWEEN %s AND %s
                  {owner_clause}
                  AND [team] = %s
                GROUP BY [owner_name], YEAR([service_activation_date])
            """, (
                date_m1[0], date_m1[1], *owner_params, team,
                date_m2[0], date_m2[1], *owner_params, team,
            ))

    elif level == "team":
        if team and team in ADVERTISING_TEAMS:
            adv_pipeline = ADVERTISING_TEAMS[team]
            cur.execute("""
                SELECT [team] AS dimension_key,
                       YEAR([service_activation_date]) AS data_year,
                       SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS tilvækst
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'won'
                  AND [pipeline_name] = %s
                  AND [team] = %s
                  AND [service_activation_date] BETWEEN %s AND %s
                GROUP BY [team], YEAR([service_activation_date])
                UNION ALL
                SELECT [team] AS dimension_key,
                       YEAR([service_activation_date]) AS data_year,
                       SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS tilvækst
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'won'
                  AND [pipeline_name] = %s
                  AND [team] = %s
                  AND [service_activation_date] BETWEEN %s AND %s
                GROUP BY [team], YEAR([service_activation_date])
            """, (adv_pipeline, team, date_m1[0], date_m1[1],
                  adv_pipeline, team, date_m2[0], date_m2[1]))
        elif team:
            cur.execute("""
                SELECT [team] AS dimension_key,
                       YEAR([service_activation_date]) AS data_year,
                       SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS tilvækst
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'won'
                  AND [pipeline_name] <> 'Web sale'
                  AND [deal_type] IN ('Abonnement', 'Subscription')
                  AND [administrativ] IS NULL
                  AND [team] = %s
                  AND [service_activation_date] BETWEEN %s AND %s
                GROUP BY [team], YEAR([service_activation_date])
                UNION ALL
                SELECT [team] AS dimension_key,
                       YEAR([service_activation_date]) AS data_year,
                       SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS tilvækst
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'won'
                  AND [pipeline_name] <> 'Web sale'
                  AND [deal_type] IN ('Abonnement', 'Subscription')
                  AND [administrativ] IS NULL
                  AND [team] = %s
                  AND [service_activation_date] BETWEEN %s AND %s
                GROUP BY [team], YEAR([service_activation_date])
            """, (team, date_m1[0], date_m1[1], team, date_m2[0], date_m2[1]))
        else:
            # Alle teams: subscription-deals + advertising-deals (job/banner)
            cur.execute("""
                SELECT [team] AS dimension_key,
                       YEAR([service_activation_date]) AS data_year,
                       SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS tilvækst
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'won'
                  AND [team] IS NOT NULL
                  AND [service_activation_date] BETWEEN %s AND %s
                  AND (
                        ([deal_type] IN ('Abonnement', 'Subscription') AND [administrativ] IS NULL AND [pipeline_name] <> 'Web sale')
                        OR [pipeline_name] IN ('job', 'banner')
                      )
                GROUP BY [team], YEAR([service_activation_date])
                UNION ALL
                SELECT [team] AS dimension_key,
                       YEAR([service_activation_date]) AS data_year,
                       SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS tilvækst
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'won'
                  AND [team] IS NOT NULL
                  AND [service_activation_date] BETWEEN %s AND %s
                  AND (
                        ([deal_type] IN ('Abonnement', 'Subscription') AND [administrativ] IS NULL AND [pipeline_name] <> 'Web sale')
                        OR [pipeline_name] IN ('job', 'banner')
                      )
                GROUP BY [team], YEAR([service_activation_date])
            """, (date_m1[0], date_m1[1], date_m2[0], date_m2[1]))

    else:  # medie
        cur.execute(f"""
            SELECT [sites] AS dimension_key,
                   YEAR([service_activation_date]) AS data_year,
                   SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS tilvækst
            FROM [dbo].[PipedriveDeals]
            WHERE [status] = 'won'
              AND [pipeline_name] <> 'Web sale'
              AND [deal_type] IN ('Abonnement', 'Subscription')
              AND [administrativ] IS NULL
              AND [sites] IS NOT NULL
              AND [service_activation_date] BETWEEN %s AND %s
              {site_clause}
            GROUP BY [sites], YEAR([service_activation_date])
            UNION ALL
            SELECT [sites] AS dimension_key,
                   YEAR([service_activation_date]) AS data_year,
                   SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS tilvækst
            FROM [dbo].[PipedriveDeals]
            WHERE [status] = 'won'
              AND [pipeline_name] <> 'Web sale'
              AND [deal_type] IN ('Abonnement', 'Subscription')
              AND [administrativ] IS NULL
              AND [sites] IS NOT NULL
              AND [service_activation_date] BETWEEN %s AND %s
              {site_clause}
            GROUP BY [sites], YEAR([service_activation_date])
        """, (date_m1[0], date_m1[1], *base_site_params,
              date_m2[0], date_m2[1], *base_site_params))

    hist_m1: dict[str, float] = {}
    hist_m2: dict[str, float] = {}
    for r in cur.fetchall():
        key = r["dimension_key"]
        if not key:
            continue
        val = float(r["tilvækst"] or 0)
        if r["data_year"] == year_m1:
            hist_m1[key] = val
        else:
            hist_m2[key] = val

    # ── Q2: Service activation date (realiseret i år) ──────────────────────
    if level == "saelger":
        if team and team in ADVERTISING_TEAMS:
            adv_pipeline = ADVERTISING_TEAMS[team]
            cur.execute("""
                SELECT [owner_name] AS dimension_key,
                       SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS activation_amount
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'won'
                  AND [pipeline_name] = %s
                  AND [team] = %s
                  AND [owner_name] IS NOT NULL
                  AND [service_activation_date] BETWEEN %s AND %s
                GROUP BY [owner_name]
            """, (adv_pipeline, team, date_cur[0], date_cur[1]))
        else:
            cur.execute(f"""
                SELECT [owner_name] AS dimension_key,
                       SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS activation_amount
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'won'
                  AND [pipeline_name] <> 'Web sale'
                  AND [deal_type] IN ('Abonnement', 'Subscription')
                  AND [administrativ] IS NULL
                  AND [owner_name] IS NOT NULL
                  AND [service_activation_date] BETWEEN %s AND %s
                  {owner_clause}
                  AND [team] = %s
                GROUP BY [owner_name]
            """, (date_cur[0], date_cur[1], *owner_params, team))

    elif level == "team":
        if team and team in ADVERTISING_TEAMS:
            adv_pipeline = ADVERTISING_TEAMS[team]
            cur.execute("""
                SELECT [team] AS dimension_key,
                       SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS activation_amount
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'won'
                  AND [pipeline_name] = %s
                  AND [team] = %s
                  AND [service_activation_date] BETWEEN %s AND %s
                GROUP BY [team]
            """, (adv_pipeline, team, date_cur[0], date_cur[1]))
        elif team:
            cur.execute("""
                SELECT [team] AS dimension_key,
                       SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS activation_amount
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'won'
                  AND [pipeline_name] <> 'Web sale'
                  AND [deal_type] IN ('Abonnement', 'Subscription')
                  AND [administrativ] IS NULL
                  AND [team] = %s
                  AND [service_activation_date] BETWEEN %s AND %s
                GROUP BY [team]
            """, (team, date_cur[0], date_cur[1]))
        else:
            cur.execute("""
                SELECT [team] AS dimension_key,
                       SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS activation_amount
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'won'
                  AND [team] IS NOT NULL
                  AND [service_activation_date] BETWEEN %s AND %s
                  AND (
                        ([deal_type] IN ('Abonnement', 'Subscription') AND [administrativ] IS NULL AND [pipeline_name] <> 'Web sale')
                        OR [pipeline_name] IN ('job', 'banner')
                      )
                GROUP BY [team]
            """, (date_cur[0], date_cur[1]))

    else:  # medie
        cur.execute(f"""
            SELECT [sites] AS dimension_key,
                   SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS activation_amount
            FROM [dbo].[PipedriveDeals]
            WHERE [status] = 'won'
              AND [pipeline_name] <> 'Web sale'
              AND [deal_type] IN ('Abonnement', 'Subscription')
              AND [administrativ] IS NULL
              AND [sites] IS NOT NULL
              AND [service_activation_date] BETWEEN %s AND %s
              {site_clause}
            GROUP BY [sites]
        """, (date_cur[0], date_cur[1], *base_site_params))

    activation = {r["dimension_key"]: float(r["activation_amount"] or 0)
                  for r in cur.fetchall() if r["dimension_key"]}

    # ── Q3: Åben pipeline (expected close date) ────────────────────────────
    if level == "saelger":
        if team and team in ADVERTISING_TEAMS:
            adv_pipeline = ADVERTISING_TEAMS[team]
            cur.execute("""
                SELECT [owner_name] AS dimension_key,
                       SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS open_pipeline
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'open'
                  AND [pipeline_name] = %s
                  AND [team] = %s
                  AND [owner_name] IS NOT NULL
                  AND [value_dkk] <> '0'
                  AND [expected_close_date] BETWEEN %s AND %s
                GROUP BY [owner_name]
            """, (adv_pipeline, team, date_cur[0], date_cur[1]))
        else:
            cur.execute(f"""
                SELECT [owner_name] AS dimension_key,
                       SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS open_pipeline
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'open'
                  AND [pipeline_name] <> 'Web sale'
                  AND [deal_type] IN ('Abonnement', 'Subscription')
                  AND [administrativ] IS NULL
                  AND [owner_name] IS NOT NULL
                  AND [value_dkk] <> '0'
                  AND [expected_close_date] BETWEEN %s AND %s
                  {owner_clause}
                  AND [team] = %s
                GROUP BY [owner_name]
            """, (date_cur[0], date_cur[1], *owner_params, team))

    elif level == "team":
        if team and team in ADVERTISING_TEAMS:
            adv_pipeline = ADVERTISING_TEAMS[team]
            cur.execute("""
                SELECT [team] AS dimension_key,
                       SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS open_pipeline
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'open'
                  AND [pipeline_name] = %s
                  AND [team] = %s
                  AND [value_dkk] <> '0'
                  AND [expected_close_date] BETWEEN %s AND %s
                GROUP BY [team]
            """, (adv_pipeline, team, date_cur[0], date_cur[1]))
        elif team:
            cur.execute("""
                SELECT [team] AS dimension_key,
                       SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS open_pipeline
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'open'
                  AND [pipeline_name] <> 'Web sale'
                  AND [deal_type] IN ('Abonnement', 'Subscription')
                  AND [administrativ] IS NULL
                  AND [team] = %s
                  AND [value_dkk] <> '0'
                  AND [expected_close_date] BETWEEN %s AND %s
                GROUP BY [team]
            """, (team, date_cur[0], date_cur[1]))
        else:
            cur.execute("""
                SELECT [team] AS dimension_key,
                       SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS open_pipeline
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'open'
                  AND [team] IS NOT NULL
                  AND [value_dkk] <> '0'
                  AND [expected_close_date] BETWEEN %s AND %s
                  AND (
                        ([deal_type] IN ('Abonnement', 'Subscription') AND [administrativ] IS NULL AND [pipeline_name] <> 'Web sale')
                        OR [pipeline_name] IN ('job', 'banner')
                      )
                GROUP BY [team]
            """, (date_cur[0], date_cur[1]))

    else:  # medie
        cur.execute(f"""
            SELECT [sites] AS dimension_key,
                   SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS open_pipeline
            FROM [dbo].[PipedriveDeals]
            WHERE [status] = 'open'
              AND [pipeline_name] <> 'Web sale'
              AND [deal_type] IN ('Abonnement', 'Subscription')
              AND [administrativ] IS NULL
              AND [sites] IS NOT NULL
              AND [value_dkk] <> '0'
              AND [expected_close_date] BETWEEN %s AND %s
              {site_clause}
            GROUP BY [sites]
        """, (date_cur[0], date_cur[1], *base_site_params))

    pipe = {r["dimension_key"]: float(r["open_pipeline"] or 0)
            for r in cur.fetchall() if r["dimension_key"]}

    # ── Q4: Budget ─────────────────────────────────────────────────────────
    budget_month_str = f"{year}-{month:02d}%"

    if level == "medie":
        cur.execute("""
            SELECT [Site] AS dimension_key, SUM([BudgetAmount]) AS budget
            FROM [dbo].[BudgetsIntoMedia]
            WHERE [BudgetDate] LIKE %s
            GROUP BY [Site]
        """, (budget_month_str,))
        budgets = {r["dimension_key"]: float(r["budget"] or 0) for r in cur.fetchall()}

    elif level == "saelger":
        if team:
            cur.execute("""
                SELECT [Owner] AS dimension_key, SUM([BudgetAmount]) AS budget
                FROM [dbo].[SalespersonBudget]
                WHERE [BudgetDate] LIKE %s
                  AND [Team] = %s
                GROUP BY [Owner]
            """, (budget_month_str, team))
        else:
            cur.execute("""
                SELECT [Owner] AS dimension_key, SUM([BudgetAmount]) AS budget
                FROM [dbo].[SalespersonBudget]
                WHERE [BudgetDate] LIKE %s
                GROUP BY [Owner]
            """, (budget_month_str,))
        budgets = {r["dimension_key"]: float(r["budget"] or 0) for r in cur.fetchall()}

    else:  # team
        if team:
            cur.execute("""
                SELECT [Team] AS dimension_key, SUM([BudgetAmount]) AS budget
                FROM [dbo].[SalespersonBudget]
                WHERE [BudgetDate] LIKE %s
                  AND [Team] = %s
                GROUP BY [Team]
            """, (budget_month_str, team))
        else:
            cur.execute("""
                SELECT [Team] AS dimension_key, SUM([BudgetAmount]) AS budget
                FROM [dbo].[SalespersonBudget]
                WHERE [BudgetDate] LIKE %s
                  AND [Team] IS NOT NULL AND [Team] <> ''
                GROUP BY [Team]
            """, (budget_month_str,))
        budgets = {r["dimension_key"]: float(r["budget"] or 0) for r in cur.fetchall()}

    # ── Q5: Gemte forecasts ────────────────────────────────────────────────
    cur.execute("""
        SELECT dimension_key, pipeline_pct, adjustment_pct, manual_amount, forecast_amount
        FROM [dbo].[HubForecasts]
        WHERE forecast_year = %s AND forecast_month = %s AND level = %s
    """, (year, month, level))
    saved = {r["dimension_key"]: r for r in cur.fetchall()}

    conn.close()
    return hist_m1, hist_m2, pipe, activation, budgets, saved


def db_forecast_save(year: int, month: int, level: str, rows: list, created_by: str):
    saved_count = 0
    conn = get_conn()
    cur = conn.cursor()
    for row in rows:
        dim_key        = str(row.get("dimension_key", "")).strip()
        pipeline_pct   = float(row.get("pipeline_pct",   30.0))
        adjustment_pct = float(row.get("adjustment_pct",  0.0))
        manual_amount  = float(row.get("manual_amount",   0.0))
        forecast_amt   = float(row.get("forecast_total",  0.0))

        if not dim_key:
            continue

        cur.execute("""
            DELETE FROM [dbo].[HubForecasts]
            WHERE forecast_year=%s AND forecast_month=%s AND level=%s AND dimension_key=%s
        """, (year, month, level, dim_key))

        cur.execute("""
            INSERT INTO [dbo].[HubForecasts]
                (forecast_year, forecast_month, level, dimension_key,
                 pipeline_pct, adjustment_pct, manual_amount, forecast_amount, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (year, month, level, dim_key,
              pipeline_pct, adjustment_pct, manual_amount, forecast_amt, created_by))
        saved_count += 1

    conn.commit()
    conn.close()
    return saved_count
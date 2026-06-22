import logging
import os
from datetime import date

import pymssql
from dotenv import load_dotenv
from fastapi import HTTPException

logger = logging.getLogger(__name__)

load_dotenv()

# Fælles brand-konstanter — én kilde til sandheden i constants.py.
from constants import SUBSCRIPTION_BRANDS, BRAND_GROUPS  # noqa: E402,F401

_SUB_PH = "(" + ",".join(["%s"] * len(SUBSCRIPTION_BRANDS)) + ")"

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

from constants import CANCELLATION_PIPELINES as _CANCEL_PIPELINES  # noqa: E402
_CANCEL_PH = "(" + ",".join(["%s"] * len(_CANCEL_PIPELINES)) + ")"

# Teams der bruger advertising-pipeline i stedet for abonnement-deals
ADVERTISING_TEAMS: dict[str, str] = {
    "Team Job":    "job",
    "Team Banner": "banner",
}

# Banner/job-deals er autoritative i jppol_advertising-accounten. De gamle
# accounts (monitor, watch_medier) indeholder spejlede kopier af de samme
# deals, så uden account-filter dobbelt-tælles de. Samme konvention som
# modul_perf og modul_banner_job. Watch NO har sit eget advertising-account
# uden spejlinger og skal med i "alle teams"-visningen.
ADVERTISING_ACCOUNT = "jppol_advertising"
ADVERTISING_ACCOUNTS = ("jppol_advertising", "watch_no_advertising")


# Fælles pooled DB-forbindelse — se db.py.
from db import get_conn  # noqa: E402,F401


def ensure_schema():
    """Migrér HubForecasts til den team-bevidste model og opret review-tabellen.

    Hvert statement kører separat og fejler blødt, så en delvis migreret
    database aldrig vælter opstarten — men fejlen skal i loggen.
    """
    statements = [
        # Legacy-kolonne fra tidligere version
        """IF NOT EXISTS (
               SELECT * FROM sys.columns
               WHERE object_id = OBJECT_ID('HubForecasts') AND name = 'adjustment_pct'
           )
           ALTER TABLE HubForecasts ADD adjustment_pct DECIMAL(5,2) NULL""",
        # team-kolonne: del af den unikke nøgle, så et sælger-forecast gemmes
        # pr. team i stedet for at overskrive på tværs af teams
        """IF NOT EXISTS (
               SELECT * FROM sys.columns
               WHERE object_id = OBJECT_ID('HubForecasts') AND name = 'team'
           )
           ALTER TABLE HubForecasts ADD team NVARCHAR(100) NOT NULL
           CONSTRAINT DF_HubForecasts_team DEFAULT ''""",
        # Backfill: på team-niveau ER dimension_key teamnavnet
        """UPDATE HubForecasts SET team = dimension_key
           WHERE level = 'team' AND team = ''""",
        # Udskift den gamle unikke nøgle (uden team) med den team-bevidste
        """IF EXISTS (
               SELECT * FROM sys.indexes
               WHERE name='UQ_HubForecasts_Key' AND object_id = OBJECT_ID('HubForecasts')
           )
           ALTER TABLE HubForecasts DROP CONSTRAINT UQ_HubForecasts_Key""",
        """IF NOT EXISTS (
               SELECT * FROM sys.indexes
               WHERE name='UQ_HubForecasts_TeamKey' AND object_id = OBJECT_ID('HubForecasts')
           )
           ALTER TABLE HubForecasts
           ADD CONSTRAINT UQ_HubForecasts_TeamKey
           UNIQUE (forecast_year, forecast_month, level, dimension_key, team)""",
        # Managerens vurdering af det samlede forecast pr. team
        """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='HubForecastReviews' AND xtype='U')
           CREATE TABLE HubForecastReviews (
               id              INT IDENTITY(1,1) PRIMARY KEY,
               forecast_year   INT            NOT NULL,
               forecast_month  INT            NOT NULL,
               team            NVARCHAR(100)  NOT NULL,
               manager_amount  DECIMAL(18,2)  NOT NULL DEFAULT 0.00,
               comment         NVARCHAR(1000) NULL,
               created_by      NVARCHAR(100)  NOT NULL,
               created_at      DATETIME       DEFAULT GETDATE(),
               updated_at      DATETIME       DEFAULT GETDATE(),
               CONSTRAINT UQ_HubForecastReviews UNIQUE (forecast_year, forecast_month, team)
           )""",
        # Sælgeren indtaster nu selv hele forecastet. pipeline_close_amount er
        # det beløb, sælgeren forventer at lukke fra sin åbne pipeline (valgt i
        # pipeline-modalen); manual_amount bruges fortsat som "ekstra oveni".
        """IF NOT EXISTS (
               SELECT * FROM sys.columns
               WHERE object_id = OBJECT_ID('HubForecasts') AND name = 'pipeline_close_amount'
           )
           ALTER TABLE HubForecasts ADD pipeline_close_amount DECIMAL(18,2) NULL""",
        # Sælgerens deal-niveau valg i pipeline-modalen. Den justerede værdi er
        # KUN lokal til forecastet — den skrives aldrig tilbage til Pipedrive.
        """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='HubForecastPipelineDeals' AND xtype='U')
           CREATE TABLE HubForecastPipelineDeals (
               id              INT IDENTITY(1,1) PRIMARY KEY,
               forecast_year   INT            NOT NULL,
               forecast_month  INT            NOT NULL,
               dimension_key   NVARCHAR(100)  NOT NULL,
               team            NVARCHAR(100)  NOT NULL,
               deal_id         NVARCHAR(50)   NOT NULL,
               amount          DECIMAL(18,2)  NOT NULL DEFAULT 0.00,
               created_at      DATETIME       DEFAULT GETDATE(),
               CONSTRAINT UQ_HubForecastPipelineDeals
                   UNIQUE (forecast_year, forecast_month, dimension_key, team, deal_id)
           )""",
    ]
    try:
        conn = get_conn()
        cur = conn.cursor()
        for sql in statements:
            try:
                cur.execute(sql)
                conn.commit()
            except Exception:
                logger.warning("ensure_schema: statement fejlede:\n%s", sql, exc_info=True)
                conn.rollback()
        conn.close()
    except Exception:
        logger.warning("ensure_schema fejlede — forecast-skemaet kunne ikke sikres", exc_info=True)


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
        # Forventelig degradering: fald tilbage til teams fra SalespersonBudget
        logger.warning("db_get_teams: Teams-tabellen kunne ikke læses — bruger fallback fra SalespersonBudget", exc_info=True)
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
                       SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END, [value]) AS DECIMAL(18,2))) AS tilvækst
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'won'
                  AND [pipeline_name] = %s
                  AND [team] = %s
                  AND [account] = %s
                  AND [owner_name] IS NOT NULL
                  AND [service_activation_date] BETWEEN %s AND %s
                GROUP BY [owner_name], YEAR([service_activation_date])
                UNION ALL
                SELECT [owner_name] AS dimension_key,
                       YEAR([service_activation_date]) AS data_year,
                       SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END, [value]) AS DECIMAL(18,2))) AS tilvækst
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'won'
                  AND [pipeline_name] = %s
                  AND [team] = %s
                  AND [account] = %s
                  AND [owner_name] IS NOT NULL
                  AND [service_activation_date] BETWEEN %s AND %s
                GROUP BY [owner_name], YEAR([service_activation_date])
            """, (adv_pipeline, team, ADVERTISING_ACCOUNT, date_m1[0], date_m1[1],
                  adv_pipeline, team, ADVERTISING_ACCOUNT, date_m2[0], date_m2[1]))
        else:
            cur.execute(f"""
                SELECT [owner_name] AS dimension_key,
                       YEAR([service_activation_date]) AS data_year,
                       SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END, [value]) AS DECIMAL(18,2))) AS tilvækst
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
                       SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END, [value]) AS DECIMAL(18,2))) AS tilvækst
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
                       SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END, [value]) AS DECIMAL(18,2))) AS tilvækst
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'won'
                  AND [pipeline_name] = %s
                  AND [team] = %s
                  AND [account] = %s
                  AND [service_activation_date] BETWEEN %s AND %s
                GROUP BY [team], YEAR([service_activation_date])
                UNION ALL
                SELECT [team] AS dimension_key,
                       YEAR([service_activation_date]) AS data_year,
                       SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END, [value]) AS DECIMAL(18,2))) AS tilvækst
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'won'
                  AND [pipeline_name] = %s
                  AND [team] = %s
                  AND [account] = %s
                  AND [service_activation_date] BETWEEN %s AND %s
                GROUP BY [team], YEAR([service_activation_date])
            """, (adv_pipeline, team, ADVERTISING_ACCOUNT, date_m1[0], date_m1[1],
                  adv_pipeline, team, ADVERTISING_ACCOUNT, date_m2[0], date_m2[1]))
        elif team:
            cur.execute("""
                SELECT [team] AS dimension_key,
                       YEAR([service_activation_date]) AS data_year,
                       SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END, [value]) AS DECIMAL(18,2))) AS tilvækst
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
                       SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END, [value]) AS DECIMAL(18,2))) AS tilvækst
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
                       SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END, [value]) AS DECIMAL(18,2))) AS tilvækst
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'won'
                  AND [team] IS NOT NULL
                  AND [service_activation_date] BETWEEN %s AND %s
                  AND (
                        ([deal_type] IN ('Abonnement', 'Subscription') AND [administrativ] IS NULL AND [pipeline_name] <> 'Web sale')
                        OR ([pipeline_name] IN ('job', 'banner') AND [account] IN ('jppol_advertising', 'watch_no_advertising'))
                      )
                GROUP BY [team], YEAR([service_activation_date])
                UNION ALL
                SELECT [team] AS dimension_key,
                       YEAR([service_activation_date]) AS data_year,
                       SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END, [value]) AS DECIMAL(18,2))) AS tilvækst
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'won'
                  AND [team] IS NOT NULL
                  AND [service_activation_date] BETWEEN %s AND %s
                  AND (
                        ([deal_type] IN ('Abonnement', 'Subscription') AND [administrativ] IS NULL AND [pipeline_name] <> 'Web sale')
                        OR ([pipeline_name] IN ('job', 'banner') AND [account] IN ('jppol_advertising', 'watch_no_advertising'))
                      )
                GROUP BY [team], YEAR([service_activation_date])
            """, (date_m1[0], date_m1[1], date_m2[0], date_m2[1]))

    else:  # medie
        cur.execute(f"""
            SELECT [sites] AS dimension_key,
                   YEAR([service_activation_date]) AS data_year,
                   SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END, [value]) AS DECIMAL(18,2))) AS tilvækst
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
                   SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END, [value]) AS DECIMAL(18,2))) AS tilvækst
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
                       SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END, [value]) AS DECIMAL(18,2))) AS activation_amount
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'won'
                  AND [pipeline_name] = %s
                  AND [team] = %s
                  AND [account] = %s
                  AND [owner_name] IS NOT NULL
                  AND [service_activation_date] BETWEEN %s AND %s
                GROUP BY [owner_name]
            """, (adv_pipeline, team, ADVERTISING_ACCOUNT, date_cur[0], date_cur[1]))
        else:
            cur.execute(f"""
                SELECT [owner_name] AS dimension_key,
                       SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END, [value]) AS DECIMAL(18,2))) AS activation_amount
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
                       SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END, [value]) AS DECIMAL(18,2))) AS activation_amount
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'won'
                  AND [pipeline_name] = %s
                  AND [team] = %s
                  AND [account] = %s
                  AND [service_activation_date] BETWEEN %s AND %s
                GROUP BY [team]
            """, (adv_pipeline, team, ADVERTISING_ACCOUNT, date_cur[0], date_cur[1]))
        elif team:
            cur.execute("""
                SELECT [team] AS dimension_key,
                       SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END, [value]) AS DECIMAL(18,2))) AS activation_amount
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
                       SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END, [value]) AS DECIMAL(18,2))) AS activation_amount
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'won'
                  AND [team] IS NOT NULL
                  AND [service_activation_date] BETWEEN %s AND %s
                  AND (
                        ([deal_type] IN ('Abonnement', 'Subscription') AND [administrativ] IS NULL AND [pipeline_name] <> 'Web sale')
                        OR ([pipeline_name] IN ('job', 'banner') AND [account] IN ('jppol_advertising', 'watch_no_advertising'))
                      )
                GROUP BY [team]
            """, (date_cur[0], date_cur[1]))

    else:  # medie
        cur.execute(f"""
            SELECT [sites] AS dimension_key,
                   SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END, [value]) AS DECIMAL(18,2))) AS activation_amount
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
                       SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END, [value]) AS DECIMAL(18,2))) AS open_pipeline
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'open'
                  AND [pipeline_name] = %s
                  AND [team] = %s
                  AND [account] = %s
                  AND [owner_name] IS NOT NULL
                  AND [value_dkk] <> '0'
                  AND [expected_close_date] BETWEEN %s AND %s
                GROUP BY [owner_name]
            """, (adv_pipeline, team, ADVERTISING_ACCOUNT, date_cur[0], date_cur[1]))
        else:
            cur.execute(f"""
                SELECT [owner_name] AS dimension_key,
                       SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END, [value]) AS DECIMAL(18,2))) AS open_pipeline
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
                       SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END, [value]) AS DECIMAL(18,2))) AS open_pipeline
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'open'
                  AND [pipeline_name] = %s
                  AND [team] = %s
                  AND [account] = %s
                  AND [value_dkk] <> '0'
                  AND [expected_close_date] BETWEEN %s AND %s
                GROUP BY [team]
            """, (adv_pipeline, team, ADVERTISING_ACCOUNT, date_cur[0], date_cur[1]))
        elif team:
            cur.execute("""
                SELECT [team] AS dimension_key,
                       SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END, [value]) AS DECIMAL(18,2))) AS open_pipeline
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
                       SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END, [value]) AS DECIMAL(18,2))) AS open_pipeline
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'open'
                  AND [team] IS NOT NULL
                  AND [value_dkk] <> '0'
                  AND [expected_close_date] BETWEEN %s AND %s
                  AND (
                        ([deal_type] IN ('Abonnement', 'Subscription') AND [administrativ] IS NULL AND [pipeline_name] <> 'Web sale')
                        OR ([pipeline_name] IN ('job', 'banner') AND [account] IN ('jppol_advertising', 'watch_no_advertising'))
                      )
                GROUP BY [team]
            """, (date_cur[0], date_cur[1]))

    else:  # medie
        cur.execute(f"""
            SELECT [sites] AS dimension_key,
                   SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END, [value]) AS DECIMAL(18,2))) AS open_pipeline
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
    if level == "saelger" and team:
        # Team-bevidst: kun forecasts gemt for netop dette team. Legacy-rækker
        # (team='') vises som fallback indtil sælgeren gemmer sit eget.
        cur.execute("""
            SELECT dimension_key, team, pipeline_pct, adjustment_pct, manual_amount,
                   pipeline_close_amount, forecast_amount, created_by, updated_at, created_at
            FROM [dbo].[HubForecasts]
            WHERE forecast_year = %s AND forecast_month = %s AND level = %s
              AND team IN (%s, '')
        """, (year, month, level, team))
        saved = {}
        for r in cur.fetchall():
            key = r["dimension_key"]
            if key not in saved or r["team"] == team:
                saved[key] = r
    else:
        cur.execute("""
            SELECT dimension_key, team, pipeline_pct, adjustment_pct, manual_amount,
                   pipeline_close_amount, forecast_amount, created_by, updated_at, created_at
            FROM [dbo].[HubForecasts]
            WHERE forecast_year = %s AND forecast_month = %s AND level = %s
        """, (year, month, level))
        saved = {r["dimension_key"]: r for r in cur.fetchall()}

    conn.close()
    return hist_m1, hist_m2, pipe, activation, budgets, saved


def db_saelger_forecast_save(year: int, month: int, owner: str, rows: list):
    """Gem sælgerens eget forecast — én række pr. team, så forecasts på tværs
    af teams aldrig overskriver hinanden.

    Sælgeren indtaster nu selv hele forecastet:
        forecast_total = pipeline_close + ekstra (manual_amount)
    hvor pipeline_close er summen af de deals, sælgeren har valgt (med evt.
    justeret beløb) i pipeline-modalen. Beløbene er autoritative server-side:
    pipeline_close og forecast_total genberegnes ud fra de gemte deals, så
    klienten ikke kan sende et tal, der ikke matcher de valgte deals.

    Returnerer (saved_count, updated_teams) hvor updated_teams er de teams,
    hvor et eksisterende forecast blev erstattet.
    """
    saved_count = 0
    updated_teams = []
    conn = get_conn()
    cur = conn.cursor()
    for row in rows:
        team          = str(row.get("team", "")).strip()
        manual_amount = float(row.get("manual_amount", 0.0))
        # "deals" mangler, hvis sælgeren ikke har åbnet pipeline-modalen for
        # denne række — så bevares det tidligere gemte pipeline-valg.
        deals         = row.get("deals")

        if not team:
            continue

        if deals is None:
            # Bevar eksisterende pipeline_close + deal-valg
            cur.execute("""
                SELECT pipeline_close_amount FROM [dbo].[HubForecasts]
                WHERE forecast_year=%s AND forecast_month=%s AND level='saelger'
                  AND dimension_key=%s AND team=%s
            """, (year, month, owner, team))
            existing = cur.fetchone()
            pipeline_close = float(existing[0]) if existing and existing[0] is not None else 0.0
        else:
            # Genberegn pipeline_close server-side fra de valgte deals
            chosen = [d for d in deals if d.get("included")]
            pipeline_close = round(sum(float(d.get("amount") or 0) for d in chosen), 2)

        forecast_amt = round(pipeline_close + manual_amount, 2)

        # Ryd altid et evt. tidligere forecast for teamet først — så et team,
        # sælgeren tømmer (ingen pipeline-valg, ingen ekstra), også fjernes i
        # stedet for at blive stående.
        cur.execute("""
            DELETE FROM [dbo].[HubForecasts]
            WHERE forecast_year=%s AND forecast_month=%s AND level='saelger'
              AND dimension_key=%s AND team=%s
        """, (year, month, owner, team))
        had_existing = bool(cur.rowcount)

        # Deal-valgene erstattes kun, når modalen var i brug for rækken
        if deals is not None:
            cur.execute("""
                DELETE FROM [dbo].[HubForecastPipelineDeals]
                WHERE forecast_year=%s AND forecast_month=%s
                  AND dimension_key=%s AND team=%s
            """, (year, month, owner, team))

        # Gem ingen tom 0-række for et team, sælgeren ikke har indtastet noget
        # for — ellers ville det fejlagtigt tælle som "udfyldt" i overblikket.
        if pipeline_close == 0 and manual_amount == 0:
            if had_existing:
                updated_teams.append(team)
            continue

        if had_existing:
            updated_teams.append(team)

        cur.execute("""
            INSERT INTO [dbo].[HubForecasts]
                (forecast_year, forecast_month, level, dimension_key, team,
                 pipeline_pct, adjustment_pct, manual_amount,
                 pipeline_close_amount, forecast_amount, created_by)
            VALUES (%s, %s, 'saelger', %s, %s, 0, 0, %s, %s, %s, %s)
        """, (year, month, owner, team,
              manual_amount, pipeline_close, forecast_amt, owner))
        saved_count += 1

        if deals is not None:
            for d in chosen:
                deal_id = str(d.get("deal_id") or "").strip()
                if not deal_id:
                    continue
                cur.execute("""
                    INSERT INTO [dbo].[HubForecastPipelineDeals]
                        (forecast_year, forecast_month, dimension_key, team, deal_id, amount)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (year, month, owner, team, deal_id, float(d.get("amount") or 0)))

    conn.commit()
    conn.close()
    return saved_count, updated_teams


def db_open_pipeline_deals(year: int, month: int, owner: str, team: str, team_brand: str | None):
    """Deal-niveau åben pipeline for én sælger på ét team i forecast-måneden.

    Samme WHERE-logik som den aggregerede åbne pipeline (db_forecast_data Q3),
    men på deal-niveau, så sælgeren kan undersøge og vælge enkelte deals.
    Hvert deal flettes med sælgerens evt. tidligere gemte valg:
        included = om sælgeren har taget dealen med i forecastet
        amount   = sælgerens (evt. justerede) forventede beløb — defaulter til
                   dealens Pipedrive-værdi indtil sælgeren ændrer det
    Den justerede værdi er kun lokal og skrives aldrig tilbage til Pipedrive.
    """
    import calendar
    last_day = calendar.monthrange(year, month)[1]
    date_lo  = f"{year}-{month:02d}-01"
    date_hi  = f"{year}-{month:02d}-{last_day}"

    val_expr = ("CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] "
                "ELSE [value_dkk] END, [value]) AS DECIMAL(18,2))")

    conn = get_conn()
    cur = conn.cursor(as_dict=True)

    if team and team in ADVERTISING_TEAMS:
        adv_pipeline = ADVERTISING_TEAMS[team]
        cur.execute(f"""
            SELECT [pd_deal_id] AS deal_id,
                   [title] AS title,
                   COALESCE([org_name], '') AS org_name,
                   COALESCE([stage_name], '') AS stage_name,
                   {val_expr} AS value,
                   CONVERT(NVARCHAR(10), [expected_close_date], 23) AS expected_close
            FROM [dbo].[PipedriveDeals]
            WHERE [status] = 'open'
              AND [pipeline_name] = %s
              AND [team] = %s
              AND [account] = %s
              AND [owner_name] = %s
              AND [value_dkk] <> '0'
              AND [expected_close_date] BETWEEN %s AND %s
            ORDER BY [expected_close_date], {val_expr} DESC
        """, (adv_pipeline, team, ADVERTISING_ACCOUNT, owner, date_lo, date_hi))
    else:
        _, _, owner_clause, owner_params = build_team_filter(team, team_brand)
        cur.execute(f"""
            SELECT [pd_deal_id] AS deal_id,
                   [title] AS title,
                   COALESCE([org_name], '') AS org_name,
                   COALESCE([stage_name], '') AS stage_name,
                   {val_expr} AS value,
                   CONVERT(NVARCHAR(10), [expected_close_date], 23) AS expected_close
            FROM [dbo].[PipedriveDeals]
            WHERE [status] = 'open'
              AND [pipeline_name] <> 'Web sale'
              AND [deal_type] IN ('Abonnement', 'Subscription')
              AND [administrativ] IS NULL
              AND [owner_name] = %s
              AND [value_dkk] <> '0'
              AND [expected_close_date] BETWEEN %s AND %s
              {owner_clause}
              AND [team] = %s
            ORDER BY [expected_close_date], {val_expr} DESC
        """, (owner, date_lo, date_hi, *owner_params, team))

    deals = []
    for r in cur.fetchall():
        deal_id = str(r["deal_id"]) if r["deal_id"] is not None else None
        if not deal_id:
            continue
        deals.append({
            "deal_id":        deal_id,
            "title":          r["title"] or "(uden titel)",
            "org_name":       r["org_name"] or "—",
            "stage_name":     r["stage_name"] or "",
            "value":          float(r["value"] or 0),
            "expected_close": r["expected_close"] or "—",
        })

    # Flet med sælgerens tidligere gemte valg
    cur.execute("""
        SELECT deal_id, amount
        FROM [dbo].[HubForecastPipelineDeals]
        WHERE forecast_year=%s AND forecast_month=%s
          AND dimension_key=%s AND team=%s
    """, (year, month, owner, team))
    saved = {str(r["deal_id"]): float(r["amount"] or 0) for r in cur.fetchall()}
    conn.close()

    for d in deals:
        if d["deal_id"] in saved:
            d["included"] = True
            d["amount"]   = saved[d["deal_id"]]
        else:
            d["included"] = False
            d["amount"]   = d["value"]

    return deals


def db_active_team_members(team_names: list) -> dict:
    """{teamnavn: [sælgernavne]} for aktive holdmedlemskaber i dag."""
    if not team_names:
        return {}
    from datetime import date as _date
    today = _date.today().isoformat()
    ph = "(" + ",".join(["%s"] * len(team_names)) + ")"
    conn = get_conn()
    cur = conn.cursor(as_dict=True)
    cur.execute(f"""
        SELECT t.name AS team, u.name AS member
        FROM   TeamMemberships tm
        JOIN   Teams t    ON t.id = tm.team_id
        JOIN   HubUsers u ON u.id = tm.user_id
        WHERE  t.name IN {ph}
          AND  tm.start_date <= %s
          AND  (tm.end_date IS NULL OR tm.end_date >= %s)
        ORDER BY u.name
    """, tuple(team_names) + (today, today))
    members: dict[str, list] = {}
    for r in cur.fetchall():
        # DISTINCT pr. team: flere aktive medlemskabsrækker må ikke give
        # samme sælger to gange i overblikket
        team_list = members.setdefault(r["team"], [])
        if r["member"] not in team_list:
            team_list.append(r["member"])
    conn.close()
    return members


def db_get_reviews(year: int, month: int, team_names: list) -> dict:
    """{teamnavn: review-række} for managerens gemte vurderinger."""
    if not team_names:
        return {}
    ph = "(" + ",".join(["%s"] * len(team_names)) + ")"
    conn = get_conn()
    cur = conn.cursor(as_dict=True)
    cur.execute(f"""
        SELECT team, manager_amount, comment, created_by, updated_at
        FROM [dbo].[HubForecastReviews]
        WHERE forecast_year = %s AND forecast_month = %s AND team IN {ph}
    """, (year, month) + tuple(team_names))
    reviews = {r["team"]: r for r in cur.fetchall()}
    conn.close()
    return reviews


def db_review_save(year: int, month: int, team: str, amount: float, comment: str, created_by: str):
    """Gem managerens vurdering af team-forecastet.

    Managerens bud er det officielle team-tal: udover review-tabellen
    upsertes level='team'-rækken i HubForecasts, som Afdelingsleder-
    dashboardet (modul_perf) læser fra.
    """
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE [dbo].[HubForecastReviews]
        SET manager_amount=%s, comment=%s, created_by=%s, updated_at=GETDATE()
        WHERE forecast_year=%s AND forecast_month=%s AND team=%s
    """, (amount, comment, created_by, year, month, team))
    if not cur.rowcount:
        cur.execute("""
            INSERT INTO [dbo].[HubForecastReviews]
                (forecast_year, forecast_month, team, manager_amount, comment, created_by)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (year, month, team, amount, comment, created_by))

    cur.execute("""
        DELETE FROM [dbo].[HubForecasts]
        WHERE forecast_year=%s AND forecast_month=%s AND level='team'
          AND dimension_key=%s
    """, (year, month, team))
    cur.execute("""
        INSERT INTO [dbo].[HubForecasts]
            (forecast_year, forecast_month, level, dimension_key, team,
             pipeline_pct, adjustment_pct, manual_amount, forecast_amount, created_by)
        VALUES (%s, %s, 'team', %s, %s, 0, 0, 0, %s, %s)
    """, (year, month, team, team, amount, created_by))

    conn.commit()
    conn.close()


def db_missing_forecast_teams(owner: str, teams: list, year: int, month: int) -> list:
    """Teams hvor sælgeren endnu ikke har gemt forecast for den givne måned."""
    if not teams:
        return []
    ph = "(" + ",".join(["%s"] * len(teams)) + ")"
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT DISTINCT team
        FROM [dbo].[HubForecasts]
        WHERE forecast_year=%s AND forecast_month=%s AND level='saelger'
          AND dimension_key=%s AND team IN {ph}
    """, (year, month, owner) + tuple(teams))
    filled = {r[0] for r in cur.fetchall()}
    conn.close()
    return [t for t in teams if t not in filled]
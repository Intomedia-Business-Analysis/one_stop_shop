import logging
import os
from datetime import date, timedelta

import pymssql
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ── Genbrugte konstanter fra modul_perf ─────────────────────────────────────

# Fælles brand-/pipeline-konstanter — én kilde til sandheden i constants.py.
from constants import SUBSCRIPTION_BRANDS, CANCELLATION_PIPELINES, MONTH_NAMES_DA  # noqa: E402,F401

_SUB_PH = "(" + ",".join(["%s"] * len(SUBSCRIPTION_BRANDS)) + ")"

# Sales Performance KPI — MarketWire er nu inkluderet (vises som eget team)
SALES_PERF_BRANDS = list(SUBSCRIPTION_BRANDS)
_SALES_PERF_PH = "(" + ",".join(["%s"] * len(SALES_PERF_BRANDS)) + ")"

_CANCEL_PH = "(" + ",".join(["%s"] * len(CANCELLATION_PIPELINES)) + ")"

_ADM_EXCLUDE = (
    "AND ([administrativ] IS NULL OR [administrativ] = '') "
    "AND UPPER(LTRIM([title])) NOT LIKE 'ADMINISTRATIV%' "
    "AND UPPER(LTRIM([title])) NOT LIKE 'ADM %' "
    "AND COALESCE([deal_type],'') <> 'Rapport' "
    "AND COALESCE([owner_name],'') NOT IN ('System Admin','')"
)

# Som _ADM_EXCLUDE, men beholder Web Sale-deals (de ejes alle af 'System Admin',
# men er legitim websalgsomsætning vi vil have med i Media Performance).
_ADM_EXCLUDE_ALLOW_WEBSALE = (
    "AND ([administrativ] IS NULL OR [administrativ] = '') "
    "AND UPPER(LTRIM([title])) NOT LIKE 'ADMINISTRATIV%' "
    "AND UPPER(LTRIM([title])) NOT LIKE 'ADM %' "
    "AND COALESCE([deal_type],'') <> 'Rapport' "
    "AND (COALESCE([owner_name],'') NOT IN ('System Admin','') OR [pipeline_name]='Web Sale')"
)

# Pipelines der tæller som subscription-salg (matcher Power BI)
SALES_PIPELINES = ['Company Trial', 'Customer', 'Newbizz']
_SALES_PIPELINES_PH = "(" + ",".join(["%s"] * len(SALES_PIPELINES)) + ")"

# MONTH_NAMES_DA importeres fra constants.py (øverst i filen).

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
    "Energywatch.com",
]
FINANS_SITES = ["FINANS DK"]
MARKETWIRE_SITES = ["MarketWire"]

# Pipeline-varianter for annonce-salg (Media Performance bruger alle varianter).
BANNER_PIPELINES = ["Banner", "Bannerads"]
JOB_PIPELINES = ["Job", "Jobmarked", "Jobads"]

MEDIA_BRAND_GROUPS = {
    "FINANS DK": ["FINANS DK"],
    "Monitor":   MONITOR_SITES,
    "Watch DK":  WATCH_DK_SITES,
    "Watch NO": [
        "EnergiWatch NO", "TechWatch NO", "AdvokatWatch NO", "MatvareWatch NO",
        "MedWatch NO", "FinansWatch NO", "EiendomsWatch NO", "Kom24 NO",
        "HandelsWatch NO", "Medier24 NO", "All Watch Sites NO",
    ],
    "Watch SE": ["FinansWatch SE"],
}

# Media Performance afgrænses pr. account (ikke brand). Rækker vises stadig pr. site.
# Abonnement og annonce (banner/job) ligger i FORSKELLIGE accounts:
#   abonnement → subscription-accounts; banner/job → advertising-accounts.
MEDIA_ACCOUNTS_SUB = ["watch_medier", "monitor", "watch_no", "watch_se", "watch_de"]
MEDIA_ACCOUNTS_ADS = ["jppol_advertising", "watch_no_advertising", "watch_de"]
MEDIA_ACCOUNT_LABELS = {
    "watch_medier":         "Watch Medier",
    "monitor":              "Monitor",
    "watch_no":             "Watch NO",
    "watch_se":             "Watch SE",
    "watch_de":             "Watch DE",
    "jppol_advertising":    "JP/Pol Annoncer",
    "watch_no_advertising": "Watch NO Annoncer",
}


# Fælles pooled DB-forbindelse — se db.py.
from db import get_conn  # noqa: E402,F401


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

# Standard-teams i Sales Performance. Skærm-konfigurationer kan overstyre
# listen pr. skærm (?teams=Team Watch NO,...), så fx Norge-kontoret kan køre
# sin egen visning uden at påvirke de danske skærme.
DEFAULT_SALES_PERF_TEAMS = [
    "Team Watch DK", "Team Watch Int", "Team Watch SE",
    "Team FINANS DK", "Team FINANS Int",
    "Team Marketwire", "Team Monitor",
]


def db_all_team_names() -> list:
    """Alle holdnavne — til team-vælgeren i skærm-konfigurationen."""
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute("SELECT name FROM Teams WHERE name IS NOT NULL ORDER BY name")
        names = [r["name"] for r in cur.fetchall()]
        conn.close()
        return names
    except Exception:
        # Forventelig degradering — team-vælgeren falder tilbage til standard-teams.
        logger.exception("db_all_team_names fejlede — falder tilbage til standard-teams")
        return list(DEFAULT_SALES_PERF_TEAMS)


def _owner_in_teams_sql(teams_ph: str) -> str:
    """Subquery der begrænser owner_name til aktive medlemmer af teams.
    Watch DK og FINANS DK deler samme sælgere — ligesom Watch Int og FINANS Int.
    """
    return f"""
    AND [owner_name] IN (
        SELECT u.name FROM [dbo].[HubUsers] u
        JOIN [dbo].[TeamMemberships] tm ON tm.user_id = u.id
        JOIN [dbo].[Teams] t ON t.id = tm.team_id
        WHERE t.name IN {teams_ph}
          AND (tm.end_date IS NULL OR TRY_CAST(tm.end_date AS DATE) >= CAST(GETDATE() AS DATE))
    )
"""


def _revenue_kpis(cur, today: date, pipeline_filter: str, date_col: str = "won_time",
                  teams: list | None = None):
    week_start, week_end = _week_range(today)
    m_start, m_end = _month_range(today)
    y_start, y_end = _year_range(today)

    if pipeline_filter == "subscription":
        # Omsætnings-KPI — alle teams i dashboardet (inkl. Team Monitor).
        # Filtrerer på team + deal_type (abonnement/subscription eller NULL for
        # MarketWire). KPI-baren (dag/uge/måned/år) kører altid på won_time.
        kpi_teams = teams if teams else list(DEFAULT_SALES_PERF_TEAMS)
        kpi_teams_ph = "(" + ",".join(["%s"] * len(kpi_teams)) + ")"

        def _sum(date_from, date_to):
            cur.execute(f"""
                SELECT ISNULL(SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2))),0) AS total
                FROM [dbo].[PipedriveDeals]
                WHERE [status]='won'
                  AND [won_time] >= %s
                  AND [won_time] < %s
                  AND [pipeline_name] NOT IN ('Web sale','cancellation','Opsigelser','banner','job','Bannerads','Jobads')
                  AND (LOWER([deal_type]) IN ('abonnement','subscription') OR [deal_type] IS NULL)
                  AND [team] IN {kpi_teams_ph}
                  {_ADM_EXCLUDE}
            """, (date_from.isoformat(), date_to.isoformat())
                + tuple(kpi_teams))
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
                SELECT ISNULL(SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2))),0) AS total
                FROM [dbo].[PipedriveDeals]
                WHERE [status]='won'
                  AND [won_time] >= %s AND [won_time] < %s
                  {pipe_clause}
            """, (date_from.isoformat(), date_to.isoformat()))
            return float((cur.fetchone() or {}).get("total", 0) or 0)

    return {
        "dag":    round(_sum(today, today + timedelta(days=1)), 2),
        "uge":    round(_sum(week_start, week_end), 2),
        "maaned": round(_sum(m_start, m_end), 2),
        "aar":    round(_sum(y_start, y_end), 2),
    }


# ════════════════════════════════════════════════════════════════════════════
#  DASHBOARD 1 — Sales Performance (Monitor, Watch & FINANS)
# ════════════════════════════════════════════════════════════════════════════

def db_sales_performance(today: date, date_col: str = "won_time",
                         teams: list | None = None):
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)

        # Team-liste pr. skærm-konfiguration (?teams=...) — bruges i alle
        # team- og sælger-filtre i dette dashboard. Uden valg: standard-teams.
        perf_teams = [t.strip() for t in (teams or []) if t and t.strip()]
        if not perf_teams:
            perf_teams = list(DEFAULT_SALES_PERF_TEAMS)
        teams_ph = "(" + ",".join(["%s"] * len(perf_teams)) + ")"
        owner_in_teams = _owner_in_teams_sql(teams_ph)

        kpis = _revenue_kpis(cur, today, "subscription", date_col=date_col, teams=perf_teams)
        q_start, q_end = _quarter_range(today)
        m_start, m_end = _month_range(today)

        def _team_netto_budget(date_from, date_to):
            cur.execute(f"""
                SELECT [team],
                    ISNULL(SUM(CASE WHEN [pipeline_name] NOT IN {_CANCEL_PH}
                        THEN CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)) ELSE 0 END),0) AS won,
                    ABS(ISNULL(SUM(CASE WHEN [pipeline_name] IN {_CANCEL_PH}
                        THEN CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)) ELSE 0 END),0)) AS cancel
                FROM [dbo].[PipedriveDeals]
                WHERE [status]='won' AND [pipeline_name] NOT IN ('banner','job','Web Sale')
                  AND (LOWER([deal_type]) IN ('abonnement','subscription') OR [team] = 'Team Marketwire')
                  AND [service_activation_date] >= %s AND [service_activation_date] < %s
                  AND ([sites] IN {_SALES_PERF_PH} OR [team] = 'Team Marketwire')
                  AND [team] IN {teams_ph}
                  {_ADM_EXCLUDE}
                GROUP BY [team]
            """, tuple(CANCELLATION_PIPELINES) * 2 + (date_from.isoformat(), date_to.isoformat()) + tuple(SALES_PERF_BRANDS) + tuple(perf_teams))
            netto_map = {r["team"]: round(float(r["won"] or 0) - float(r["cancel"] or 0), 2) for r in cur.fetchall()}

            cur.execute(f"""
                SELECT [Team] AS team, SUM([BudgetAmount]) AS budget
                FROM [dbo].[SalespersonBudget]
                WHERE [BudgetDate] >= %s AND [BudgetDate] < %s
                  AND [Team] IN {teams_ph}
                GROUP BY [Team]
            """, (date_from.isoformat(), date_to.isoformat()) + tuple(perf_teams))
            budget_map = {r["team"]: float(r["budget"] or 0) for r in cur.fetchall()}

            # MarketWire-budgettet ligger i BudgetsIntoMedia (ikke SalespersonBudget)
            # — men kun hvis Team Marketwire er blandt skærmens valgte teams.
            mw_budget = 0.0
            if any(t.lower() == "team marketwire" for t in perf_teams):
                cur.execute("""
                    SELECT ISNULL(SUM([BudgetAmount]),0) AS budget
                    FROM [dbo].[BudgetsIntoMedia]
                    WHERE [Brand] = 'MarketWire' AND [DealType] = 'Subscription'
                      AND [BudgetDate] >= %s AND [BudgetDate] < %s
                """, (date_from.isoformat(), date_to.isoformat()))
                mw_budget = float((cur.fetchone() or {}).get("budget", 0) or 0)
            if mw_budget:
                budget_map["Team Marketwire"] = budget_map.get("Team Marketwire", 0.0) + mw_budget

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
                  AND ([pipeline_name] IN {_SALES_PIPELINES_PH} OR [team] = 'Team Marketwire')
                  AND ([sites] IN {_SALES_PERF_PH} OR [team] = 'Team Marketwire')
                  {owner_in_teams}
                  {_ADM_EXCLUDE}
                GROUP BY [owner_name] ORDER BY deals DESC
            """, (m_start.isoformat(), m_end.isoformat()) + tuple(SALES_PIPELINES) + tuple(SALES_PERF_BRANDS) + tuple(perf_teams))
            deals_oprettet = [{"owner_name": r["owner_name"], "deals": int(r["deals"] or 0)} for r in cur.fetchall()]
        except Exception:
            deals_oprettet = []

        # Deals vundet — kun sælgere på de relevante teams (ekskl. Norge)
        cur.execute(f"""
            SELECT COALESCE([owner_name],'Ukendt') AS owner_name, COUNT(*) AS deals
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won'
              AND ([pipeline_name] IN {_SALES_PIPELINES_PH} OR [team] = 'Team Marketwire')
              AND [close_time] >= %s AND [close_time] < %s
              AND ([sites] IN {_SALES_PERF_PH} OR [team] = 'Team Marketwire')
              {owner_in_teams}
              {_ADM_EXCLUDE}
            GROUP BY [owner_name] ORDER BY deals DESC
        """, tuple(SALES_PIPELINES) + (m_start.isoformat(), m_end.isoformat()) + tuple(SALES_PERF_BRANDS) + tuple(perf_teams))
        deals_vundet = [{"owner_name": r["owner_name"], "deals": int(r["deals"] or 0)} for r in cur.fetchall()]

        # Deals omsætning — kun sælgere på de relevante teams (ekskl. Norge)
        cur.execute(f"""
            SELECT COALESCE([owner_name],'Ukendt') AS owner_name,
                   ISNULL(SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2))),0) AS revenue
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won'
              AND ([pipeline_name] IN {_SALES_PIPELINES_PH} OR [team] = 'Team Marketwire')
              AND [won_time] >= %s AND [won_time] < %s
              AND ([sites] IN {_SALES_PERF_PH} OR [team] = 'Team Marketwire')
              {owner_in_teams}
              {_ADM_EXCLUDE}
            GROUP BY [owner_name] ORDER BY revenue DESC
        """, tuple(SALES_PIPELINES) + (m_start.isoformat(), m_end.isoformat()) + tuple(SALES_PERF_BRANDS) + tuple(perf_teams))
        deals_omsaetning = [{"owner_name": r["owner_name"], "revenue": round(float(r["revenue"] or 0), 2)} for r in cur.fetchall()]

        # Seneste deals vundet — kun subscription teams, ingen opsigelser
        cur.execute(f"""
            SELECT TOP 20
                COALESCE([owner_name],'Ukendt') AS owner_name,
                COALESCE([org_name],'')         AS org_name,
                COALESCE([team],'')             AS team,
                COALESCE([sites],'')            AS sites,
                CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)) AS value,
                CONVERT(VARCHAR(19),[won_time],120) AS won_time
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won'
              AND [pipeline_name] NOT IN {_CANCEL_PH}
              AND [pipeline_name] NOT IN ('banner','job','Web Sale')
              AND ([sites] IN {_SALES_PERF_PH} OR [team] = 'Team Marketwire')
              AND [team] IN {teams_ph}
              {_ADM_EXCLUDE}
            ORDER BY [won_time] DESC
        """, tuple(CANCELLATION_PIPELINES) + tuple(SALES_PERF_BRANDS) + tuple(perf_teams))
        seneste_deals = [{"owner_name": r["owner_name"], "org_name": r["org_name"], "team": r["team"],
                          "sites": r["sites"], "value": float(r["value"] or 0), "won_time": str(r["won_time"] or "")}
                         for r in cur.fetchall()]

        # Index pr. sælger — tilvækst (service_activation_date) vs. samlet budget
        cur.execute(f"""
            SELECT COALESCE([owner_name],'Ukendt') AS owner_name,
                ISNULL(SUM(CASE WHEN [pipeline_name] NOT IN {_CANCEL_PH}
                    THEN CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)) ELSE 0 END),0) AS won,
                ABS(ISNULL(SUM(CASE WHEN [pipeline_name] IN {_CANCEL_PH}
                    THEN CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)) ELSE 0 END),0)) AS cancel
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won' AND [pipeline_name] NOT IN ('banner','job','Web Sale')
              AND (LOWER([deal_type]) IN ('abonnement','subscription') OR [team] = 'Team Marketwire')
              AND [service_activation_date] >= %s AND [service_activation_date] < %s
              AND ([sites] IN {_SALES_PERF_PH} OR [team] = 'Team Marketwire')
              AND [team] IN {teams_ph}
              {_ADM_EXCLUDE}
            GROUP BY [owner_name]
        """, tuple(CANCELLATION_PIPELINES) * 2 + (m_start.isoformat(), m_end.isoformat())
            + tuple(SALES_PERF_BRANDS) + tuple(perf_teams))
        seller_tilvaekst = {r["owner_name"]: round(float(r["won"] or 0) - float(r["cancel"] or 0), 2)
                            for r in cur.fetchall()}

        # Budget pr. sælger — kun for skærmens valgte teams, så sælgere fra
        # andre teams ikke trækkes ind i rosteret med 0-rækker.
        cur.execute(f"""
            SELECT [Owner] AS owner_name, SUM([BudgetAmount]) AS budget
            FROM [dbo].[SalespersonBudget]
            WHERE [BudgetDate] >= %s AND [BudgetDate] < %s
              AND [Owner] IS NOT NULL AND [Owner] <> ''
              AND [Team] IN {teams_ph}
            GROUP BY [Owner]
        """, (m_start.isoformat(), m_end.isoformat()) + tuple(perf_teams))
        seller_budget = {r["owner_name"]: float(r["budget"] or 0) for r in cur.fetchall()}

        # ── Unificeret sælger-roster på tværs af alle 4 widgets ───────────────
        # Union af sælgere fra: deals oprettet, vundet, omsætning, tilvækst og budget.
        # Hver widget vises med samme sæt sælgere, padded med 0 hvor der ingen aktivitet er.
        opr_map = {r["owner_name"]: int(r["deals"] or 0) for r in deals_oprettet}
        vun_map = {r["owner_name"]: int(r["deals"] or 0) for r in deals_vundet}
        oms_map = {r["owner_name"]: float(r["revenue"] or 0) for r in deals_omsaetning}

        master_sellers = set()
        master_sellers.update(opr_map.keys())
        master_sellers.update(vun_map.keys())
        master_sellers.update(oms_map.keys())
        master_sellers.update(seller_tilvaekst.keys())
        master_sellers.update(seller_budget.keys())
        master_sellers.discard("Ukendt")
        master_sellers.discard("")

        # Byg alle 4 lister mod samme roster
        deals_oprettet = [{"owner_name": s, "deals": opr_map.get(s, 0)} for s in master_sellers]
        deals_oprettet.sort(key=lambda x: (-x["deals"], x["owner_name"]))

        deals_vundet = [{"owner_name": s, "deals": vun_map.get(s, 0)} for s in master_sellers]
        deals_vundet.sort(key=lambda x: (-x["deals"], x["owner_name"]))

        deals_omsaetning = [{"owner_name": s, "revenue": round(oms_map.get(s, 0.0), 2)} for s in master_sellers]
        deals_omsaetning.sort(key=lambda x: (-x["revenue"], x["owner_name"]))

        seller_index = []
        for owner in master_sellers:
            tv = seller_tilvaekst.get(owner, 0.0)
            bd = round(seller_budget.get(owner, 0.0), 2)
            idx = round(tv / bd * 100, 1) if bd > 0 else None
            seller_index.append({"owner_name": owner, "tilvaekst": tv, "budget": bd, "index": idx})
        # Sorter efter index (None nederst), så efter tilvækst
        seller_index.sort(key=lambda x: (x["index"] is None, -(x["index"] or 0), -x["tilvaekst"]))

        conn.close()
        return {
            "kpis": kpis, "kvartal_chart": kvartal_chart, "maaned_chart": maaned_chart,
            "deals_oprettet": deals_oprettet, "deals_vundet": deals_vundet,
            "deals_omsaetning": deals_omsaetning, "seneste_deals": seneste_deals,
            "seller_index": seller_index,
            "maaned_label": MONTH_NAMES_DA[today.month - 1] + " " + str(today.year),
            "kvartal_label": _quarter_label(today) + " " + str(today.year),
            "today": today.isoformat(),
        }
    except Exception:
        logger.exception("db_sales_performance fejlede")
        raise


# ════════════════════════════════════════════════════════════════════════════
#  DASHBOARD 2 — Department Performance
# ════════════════════════════════════════════════════════════════════════════

def _dept_sub_panel(cur, m_start, m_end, sites, teams):
    sites_ph = "(" + ",".join(["%s"] * len(sites)) + ")"
    teams_ph = "(" + ",".join(["%s"] * len(teams)) + ")"
    cur.execute(f"""
        SELECT [team],
            ISNULL(SUM(CASE WHEN [pipeline_name] NOT IN {_CANCEL_PH}
                THEN CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)) ELSE 0 END),0) AS won,
            ABS(ISNULL(SUM(CASE WHEN [pipeline_name] IN {_CANCEL_PH}
                THEN CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)) ELSE 0 END),0)) AS cancel
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name] NOT IN ('banner','job','Web Sale')
          AND (LOWER([deal_type]) IN ('abonnement','subscription') OR [team] = 'Team Marketwire')
          AND [service_activation_date] >= %s AND [service_activation_date] < %s
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


def _dept_adv_panel(cur, m_start, m_end, pipeline, brand_sites_map, include_programmatic=False):
    deal_type = "Banner" if pipeline == "banner" else "Job"
    rows = []
    for label, site_list in brand_sites_map.items():
        sites_ph = "(" + ",".join(["%s"] * len(site_list)) + ")"
        cur.execute(f"""
            SELECT ISNULL(SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2))),0) AS revenue
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won' AND [pipeline_name] = %s
              AND [service_activation_date] >= %s AND [service_activation_date] < %s
              AND [sites] IN {sites_ph}
              {_ADM_EXCLUDE}
        """, (pipeline, m_start.isoformat(), m_end.isoformat()) + tuple(site_list))
        revenue = float((cur.fetchone() or {}).get("revenue", 0) or 0)

        # Budget for brand (alle salestypes, dvs. inkl. programmatic-budget).
        cur.execute("""
            SELECT ISNULL(SUM([BudgetAmount]),0) AS budget FROM [dbo].[BudgetsIntoMedia]
            WHERE [DealType] = %s AND [Brand] = %s AND [BudgetDate] >= %s AND [BudgetDate] < %s
        """, (deal_type, label, m_start.isoformat(), m_end.isoformat()))
        budget = float((cur.fetchone() or {}).get("budget", 0) or 0)

        # FINANS programmatisk salg ligger i ProgrammaticSales (ikke i PipedriveDeals).
        prog_rev = prog_budget = 0.0
        if include_programmatic and label == "FINANS DK":
            cur.execute("""
                SELECT ISNULL(SUM([Amount]),0) AS amt FROM [dbo].[ProgrammaticSales]
                WHERE [Site] = 'FINANS DK' AND [Date] >= %s AND [Date] < %s
            """, (m_start.isoformat(), m_end.isoformat()))
            prog_rev = float((cur.fetchone() or {}).get("amt", 0) or 0)

            cur.execute("""
                SELECT ISNULL(SUM([BudgetAmount]),0) AS budget FROM [dbo].[BudgetsIntoMedia]
                WHERE [DealType] = 'Banner' AND [Brand] = 'FINANS DK'
                  AND LOWER(COALESCE([Salestype],'')) = 'programmatic'
                  AND [BudgetDate] >= %s AND [BudgetDate] < %s
            """, (m_start.isoformat(), m_end.isoformat()))
            prog_budget = float((cur.fetchone() or {}).get("budget", 0) or 0)

            # FINANS-totalen er inkl. programmatisk omsætning.
            revenue += prog_rev

        rows.append({"brand": label, "revenue": round(revenue, 2), "budget": round(budget, 2)})

        # Vis programmatisk-andelen som egen række direkte under FINANS DK.
        if include_programmatic and label == "FINANS DK":
            rows.append({"brand": "— heraf programmatisk", "revenue": round(prog_rev, 2), "budget": round(prog_budget, 2)})
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

        # Brand-labels mapper til de faktiske sites (ikke ét literal site-navn).
        adv_brands = {"Watch DK": WATCH_DK_SITES, "FINANS DK": FINANS_SITES, "Monitor": MONITOR_SITES}
        # Banner inkluderer FINANS' programmatiske salg (fra ProgrammaticSales).
        banner_rows = _dept_adv_panel(cur, m_start, m_end, "banner", adv_brands, include_programmatic=True)

        # Job: omsætning for Team Job-deals vs. budget summeret på Team Job-sælgere
        # (SalespersonBudget i stedet for BudgetsIntoMedia).
        cur.execute(f"""
            SELECT ISNULL(SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2))),0) AS revenue
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won' AND [pipeline_name]='job' AND [team]='Team Job'
              AND [service_activation_date] >= %s AND [service_activation_date] < %s
              {_ADM_EXCLUDE}
        """, (m_start.isoformat(), m_end.isoformat()))
        job_revenue = float((cur.fetchone() or {}).get("revenue", 0) or 0)

        cur.execute("""
            SELECT ISNULL(SUM([BudgetAmount]),0) AS budget FROM [dbo].[SalespersonBudget]
            WHERE [Team]='Team Job' AND [BudgetDate] >= %s AND [BudgetDate] < %s
        """, (m_start.isoformat(), m_end.isoformat()))
        job_budget = float((cur.fetchone() or {}).get("budget", 0) or 0)

        job_rows = [{"brand": "Job", "revenue": round(job_revenue, 2), "budget": round(job_budget, 2)}]

        # MarketWire identificeres på team (sites er NULL for disse deals).
        # Subscription-produkt → service_activation_date som de øvrige abonnementspaneler.
        cur.execute(f"""
            SELECT ISNULL(SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2))),0) AS revenue
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won' AND [pipeline_name] NOT IN {_CANCEL_PH}
              AND [pipeline_name] NOT IN ('banner','job','Web Sale')
              AND [service_activation_date] >= %s AND [service_activation_date] < %s
              AND [team] = 'Team Marketwire'
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
        logger.exception("db_department_performance fejlede")
        raise


# ════════════════════════════════════════════════════════════════════════════
#  Fælles hjælpere til Dashboard 3 + 4 (reklame)
# ════════════════════════════════════════════════════════════════════════════

# Site → hovedbrand. Bruges til at samle de mange sites i få, læsbare serier.
# Watch Int-sites lægges under Watch DK (samlet Watch-gruppe).
_SITE_TO_BRAND = {}
for _s in WATCH_DK_SITES:   _SITE_TO_BRAND[_s] = "Watch DK"
for _s in WATCH_INT_SITES:  _SITE_TO_BRAND[_s] = "Watch DK"
for _s in MONITOR_SITES:    _SITE_TO_BRAND[_s] = "Monitor"
for _s in FINANS_SITES:     _SITE_TO_BRAND[_s] = "FINANS DK"
for _s in MARKETWIRE_SITES: _SITE_TO_BRAND[_s] = "Marketwire"

# Fast farve pr. hovedbrand, så samme brand har samme farve i begge charts.
# "Finans programmatisk" stables lige over "FINANS DK" så det tydeligt fremgår,
# hvor meget af Finans banner-salget der er programmatisk vs. alm. salg.
BRAND_ORDER = ["Watch DK", "FINANS DK", "Finans programmatisk", "Monitor", "Marketwire", "Øvrige"]


def _adv_series_by_brand(cur, today, pipeline, granularity, include_programmatic=False):
    """Stablet tidsserie: omsætning pr. hovedbrand fordelt over enten måneder
    (Jan–Dec) eller kvartaler (Q1–Q4) for indeværende år.

    De mange enkelt-sites grupperes i hovedbrands (Watch DK, FINANS DK, Monitor,
    Watch Int, Marketwire) — alt andet havner i 'Øvrige' — så stablen bliver
    læsbar med få serier.

    granularity: 'month' → 12 punkter, 'quarter' → 4 punkter.
    include_programmatic: læg FINANS' programmatiske banner-salg (ProgrammaticSales)
    oven i FINANS DK-serien, så tidsserien matcher det samlede Finans banner-salg.
    Returnerer {labels:[...], series:[{brand, data:[...]}, ...]}.
    """
    y_start, y_end = _year_range(today)
    pipeline_clause = "[pipeline_name] IN ('banner','job')" if pipeline == "advertising" else f"[pipeline_name] = '{pipeline}'"
    # Omsætning dateres på service_activation_date (aktiverings-dato) — samme
    # grundlag som 'Omsætning vs. budget' og brand-panelerne, så tallene matcher
    # på tværs af dashboardet (budgettet er sat pr. aktiveringsmåned).
    period_expr = "MONTH([service_activation_date])" if granularity == "month" else "DATEPART(QUARTER, [service_activation_date])"
    cur.execute(f"""
        SELECT COALESCE([sites],'Ukendt') AS site,
               {period_expr} AS period,
               ISNULL(SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2))),0) AS revenue
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND {pipeline_clause} AND [account]='jppol_advertising'
          AND [service_activation_date] >= %s AND [service_activation_date] < %s
          AND [sites] IS NOT NULL AND [sites] <> ''
        GROUP BY [sites], {period_expr}
    """, (y_start.isoformat(), y_end.isoformat()))

    n = 12 if granularity == "month" else 4
    labels = (["Jan", "Feb", "Mar", "Apr", "Maj", "Jun",
               "Jul", "Aug", "Sep", "Okt", "Nov", "Dec"]
              if granularity == "month" else ["Q1", "Q2", "Q3", "Q4"])

    # hovedbrand -> [0]*n
    by_brand: dict = {}
    for r in cur.fetchall():
        idx = int(r["period"]) - 1
        if idx < 0 or idx >= n:
            continue
        brand = _SITE_TO_BRAND.get(r["site"], "Øvrige")
        rev = round(float(r["revenue"] or 0), 2)
        by_brand.setdefault(brand, [0.0] * n)
        by_brand[brand][idx] = round(by_brand[brand][idx] + rev, 2)

    # FINANS' programmatiske banner-salg ligger i ProgrammaticSales (ikke
    # PipedriveDeals) og dateres på [Date]. Vis det som EGEN serie ("Finans
    # programmatisk") oven på FINANS DK, så det fremgår tydeligt hvor meget af
    # Finans banner-salget der er programmatisk vs. alm. salg.
    if include_programmatic:
        prog_period = "MONTH([Date])" if granularity == "month" else "DATEPART(QUARTER, [Date])"
        cur.execute(f"""
            SELECT {prog_period} AS period,
                   ISNULL(SUM([Amount]),0) AS revenue
            FROM [dbo].[ProgrammaticSales]
            WHERE [Site]='FINANS DK' AND [Date] >= %s AND [Date] < %s
            GROUP BY {prog_period}
        """, (y_start.isoformat(), y_end.isoformat()))
        for r in cur.fetchall():
            idx = int(r["period"]) - 1
            if idx < 0 or idx >= n:
                continue
            rev = round(float(r["revenue"] or 0), 2)
            by_brand.setdefault("Finans programmatisk", [0.0] * n)
            by_brand["Finans programmatisk"][idx] = round(by_brand["Finans programmatisk"][idx] + rev, 2)

    # Fast rækkefølge (kun brands der har data)
    series = [{"brand": b, "data": by_brand[b]} for b in BRAND_ORDER if b in by_brand]
    return {"labels": labels, "series": series}


def _adv_maaned_series_by_brand(cur, today, pipeline, include_programmatic=False):
    return _adv_series_by_brand(cur, today, pipeline, "month", include_programmatic)


def _adv_kvartal_series_by_brand(cur, today, pipeline, include_programmatic=False):
    return _adv_series_by_brand(cur, today, pipeline, "quarter", include_programmatic)


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
               ISNULL(SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2))),0) AS revenue
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND {pipeline_clause} AND [account]='jppol_advertising'
          AND [won_time] >= %s AND [won_time] < %s
        GROUP BY [owner_name] ORDER BY revenue DESC
    """, (m_start.isoformat(), m_end.isoformat()))
    return [{"owner_name": r["owner_name"], "revenue": round(float(r["revenue"] or 0), 2)} for r in cur.fetchall()]


def _adv_budget_by_brand(cur, m_start, m_end, pipeline, include_programmatic=False):
    # Brand-labels mapper til de faktiske sites; omsætning på service_activation_date.
    brand_sites = {"Watch DK": WATCH_DK_SITES, "FINANS DK": FINANS_SITES, "Monitor": MONITOR_SITES}
    deal_type = "Banner" if pipeline == "banner" else "Job"
    rows = []
    for brand, site_list in brand_sites.items():
        sites_ph = "(" + ",".join(["%s"] * len(site_list)) + ")"
        cur.execute(f"""
            SELECT ISNULL(SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2))),0) AS revenue
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won' AND [pipeline_name]=%s
              AND [service_activation_date] >= %s AND [service_activation_date] < %s
              AND [sites] IN {sites_ph}
              {_ADM_EXCLUDE}
        """, (pipeline, m_start.isoformat(), m_end.isoformat()) + tuple(site_list))
        revenue = float((cur.fetchone() or {}).get("revenue", 0) or 0)

        # Budget for brand (alle salestypes, dvs. inkl. programmatic-budget).
        cur.execute("""
            SELECT ISNULL(SUM([BudgetAmount]),0) AS budget FROM [dbo].[BudgetsIntoMedia]
            WHERE [DealType]=%s AND [Brand]=%s AND [BudgetDate] >= %s AND [BudgetDate] < %s
        """, (deal_type, brand, m_start.isoformat(), m_end.isoformat()))
        budget = float((cur.fetchone() or {}).get("budget", 0) or 0)

        # FINANS' banner-omsætning er inkl. programmatisk salg (ProgrammaticSales).
        if include_programmatic and brand == "FINANS DK":
            cur.execute("""
                SELECT ISNULL(SUM([Amount]),0) AS amt FROM [dbo].[ProgrammaticSales]
                WHERE [Site]='FINANS DK' AND [Date] >= %s AND [Date] < %s
            """, (m_start.isoformat(), m_end.isoformat()))
            revenue += float((cur.fetchone() or {}).get("amt", 0) or 0)

        rows.append({"brand": brand, "revenue": round(revenue, 2), "budget": round(budget, 2)})
    return rows


def _adv_seneste_deals(cur, pipeline):
    pipeline_clause = "[pipeline_name] IN ('banner','job')" if pipeline == "advertising" else f"[pipeline_name] = '{pipeline}'"
    cur.execute(f"""
        SELECT TOP 20
            COALESCE([owner_name],'Ukendt') AS owner_name,
            COALESCE([org_name],'')         AS org_name,
            COALESCE([sites],'')            AS brand,
            CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2)) AS value,
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

        # Programmatisk salg (FINANS DK) — omsætning fra ProgrammaticSales,
        # budget fra BudgetsIntoMedia (Salestype = Programmatic).
        try:
            cur.execute("""
                SELECT ISNULL(SUM([Amount]),0) AS revenue FROM [dbo].[ProgrammaticSales]
                WHERE [Site]='FINANS DK' AND [Date] >= %s AND [Date] < %s
            """, (m_start.isoformat(), m_end.isoformat()))
            prog_rev = float((cur.fetchone() or {}).get("revenue", 0) or 0)

            cur.execute("""
                SELECT ISNULL(SUM([BudgetAmount]),0) AS budget FROM [dbo].[BudgetsIntoMedia]
                WHERE [DealType]='Banner' AND [Brand]='FINANS DK'
                  AND LOWER(COALESCE([Salestype],'')) = 'programmatic'
                  AND [BudgetDate] >= %s AND [BudgetDate] < %s
            """, (m_start.isoformat(), m_end.isoformat()))
            prog_budget = float((cur.fetchone() or {}).get("budget", 0) or 0)
            programmatisk = [{"brand": "FINANS DK", "revenue": round(prog_rev, 2), "budget": round(prog_budget, 2)}]
        except Exception:
            programmatisk = []

        result = {
            "kpis":             _revenue_kpis(cur, today, "banner"),
            "maaned_chart":     _adv_maaned_series_by_brand(cur, today, "banner", include_programmatic=True),
            "kvartal_chart":    _adv_kvartal_series_by_brand(cur, today, "banner", include_programmatic=True),
            "deals_oprettet":   _adv_deals_oprettet(cur, m_start, m_end, "banner"),
            "deals_vundet":     _adv_deals_vundet(cur, m_start, m_end, "banner"),
            "deals_omsaetning": _adv_omsaetning_by_owner(cur, m_start, m_end, "banner"),
            "budget_chart":     _adv_budget_by_brand(cur, m_start, m_end, "banner", include_programmatic=True),
            "programmatisk":    programmatisk,
            "seneste_deals":    _adv_seneste_deals(cur, "banner"),
            "maaned_label":     MONTH_NAMES_DA[today.month - 1] + " " + str(today.year),
            "kvartal_label":    _quarter_label(today) + " " + str(today.year),
            "today":            today.isoformat(),
        }
        conn.close()
        return result
    except Exception:
        logger.exception("db_banner_performance fejlede")
        raise


# ════════════════════════════════════════════════════════════════════════════
#  DASHBOARD 4 — Job Performance (kun job-pipeline)
# ════════════════════════════════════════════════════════════════════════════

def db_job_performance(today: date):
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        m_start, m_end = _month_range(today)

        # Omsætning mod budget: samme budget som i department-dashboardet
        # (SalespersonBudget summeret på Team Job) vs. omsætning for indeværende
        # måned (service_activation_date) for Team Job-deals.
        cur.execute(f"""
            SELECT ISNULL(SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2))),0) AS revenue
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won' AND [pipeline_name]='job' AND [team]='Team Job'
              AND [service_activation_date] >= %s AND [service_activation_date] < %s
              {_ADM_EXCLUDE}
        """, (m_start.isoformat(), m_end.isoformat()))
        job_revenue = float((cur.fetchone() or {}).get("revenue", 0) or 0)

        cur.execute("""
            SELECT ISNULL(SUM([BudgetAmount]),0) AS budget FROM [dbo].[SalespersonBudget]
            WHERE [Team]='Team Job' AND [BudgetDate] >= %s AND [BudgetDate] < %s
        """, (m_start.isoformat(), m_end.isoformat()))
        job_budget = float((cur.fetchone() or {}).get("budget", 0) or 0)

        budget_chart = [{"brand": "Job", "revenue": round(job_revenue, 2), "budget": round(job_budget, 2)}]

        result = {
            "kpis":             _revenue_kpis(cur, today, "job"),
            "maaned_chart":     _adv_maaned_series_by_brand(cur, today, "job"),
            "kvartal_chart":    _adv_kvartal_series_by_brand(cur, today, "job"),
            "deals_oprettet":   _adv_deals_oprettet(cur, m_start, m_end, "job"),
            "deals_vundet":     _adv_deals_vundet(cur, m_start, m_end, "job"),
            "deals_omsaetning": _adv_omsaetning_by_owner(cur, m_start, m_end, "job"),
            "budget_chart":     budget_chart,
            "seneste_deals":    _adv_seneste_deals(cur, "job"),
            "maaned_label":     MONTH_NAMES_DA[today.month - 1] + " " + str(today.year),
            "kvartal_label":    _quarter_label(today) + " " + str(today.year),
            "today":            today.isoformat(),
        }
        conn.close()
        return result
    except Exception:
        logger.exception("db_job_performance fejlede")
        raise


# ════════════════════════════════════════════════════════════════════════════
#  DASHBOARD 4b — Advertising Performance NO (job + banner samlet, Norge)
# ════════════════════════════════════════════════════════════════════════════

# Norske annonce-deals ligger i egen Pipedrive-account, adskilt fra
# jppol_advertising som de danske banner/job-dashboards kører på. Pipelines
# hedder 'Job' og 'Banner'. Alle deals er i NOK, så beløbene vises i NOK
# (matcher Watch NO-budgettet i BudgetsIntoMedia).
NO_ADV_ACCOUNT = "watch_no_advertising"

_NO_VAL = "CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2))"


def db_no_advertising_performance(today: date):
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        m_start, m_end = _month_range(today)
        week_start, week_end = _week_range(today)
        y_start, y_end = _year_range(today)

        # KPI-bar (dag/uge/måned/år) — vundne deals på won_time, job + banner samlet.
        def _sum(date_from, date_to):
            cur.execute(f"""
                SELECT ISNULL(SUM({_NO_VAL}),0) AS total
                FROM [dbo].[PipedriveDeals]
                WHERE [status]='won' AND [account]=%s
                  AND [pipeline_name] IN ('Job','Banner')
                  AND [won_time] >= %s AND [won_time] < %s
            """, (NO_ADV_ACCOUNT, date_from.isoformat(), date_to.isoformat()))
            return float((cur.fetchone() or {}).get("total", 0) or 0)

        kpis = {
            "dag":    round(_sum(today, today + timedelta(days=1)), 2),
            "uge":    round(_sum(week_start, week_end), 2),
            "maaned": round(_sum(m_start, m_end), 2),
            "aar":    round(_sum(y_start, y_end), 2),
        }

        # Stablet tidsserie Job vs. Banner for indeværende år. Nøglen hedder
        # 'brand' så frontenden kan genbruge samme stacked-bar-builder som de
        # danske annonce-dashboards.
        def _series(granularity):
            period_expr = "MONTH([won_time])" if granularity == "month" else "DATEPART(QUARTER, [won_time])"
            cur.execute(f"""
                SELECT [pipeline_name] AS pipeline, {period_expr} AS period,
                       ISNULL(SUM({_NO_VAL}),0) AS revenue
                FROM [dbo].[PipedriveDeals]
                WHERE [status]='won' AND [account]=%s
                  AND [pipeline_name] IN ('Job','Banner')
                  AND [won_time] >= %s AND [won_time] < %s
                GROUP BY [pipeline_name], {period_expr}
            """, (NO_ADV_ACCOUNT, y_start.isoformat(), y_end.isoformat()))
            n = 12 if granularity == "month" else 4
            labels = (["Jan", "Feb", "Mar", "Apr", "Maj", "Jun",
                       "Jul", "Aug", "Sep", "Okt", "Nov", "Dec"]
                      if granularity == "month" else ["Q1", "Q2", "Q3", "Q4"])
            by_pipe: dict = {}
            for r in cur.fetchall():
                idx = int(r["period"]) - 1
                if idx < 0 or idx >= n:
                    continue
                name = "Job" if str(r["pipeline"]).strip().lower().startswith("job") else "Banner"
                by_pipe.setdefault(name, [0.0] * n)
                by_pipe[name][idx] = round(by_pipe[name][idx] + float(r["revenue"] or 0), 2)
            series = [{"brand": p, "data": by_pipe[p]} for p in ("Job", "Banner") if p in by_pipe]
            return {"labels": labels, "series": series}

        maaned_chart  = _series("month")
        kvartal_chart = _series("quarter")

        try:
            cur.execute("""
                SELECT COALESCE([owner_name],'Ukendt') AS owner_name, COUNT(*) AS deals
                FROM [dbo].[PipedriveDeals]
                WHERE [add_time] >= %s AND [add_time] < %s
                  AND [account]=%s AND [pipeline_name] IN ('Job','Banner')
                GROUP BY [owner_name] ORDER BY deals DESC
            """, (m_start.isoformat(), m_end.isoformat(), NO_ADV_ACCOUNT))
            deals_oprettet = [{"owner_name": r["owner_name"], "deals": int(r["deals"] or 0)} for r in cur.fetchall()]
        except Exception:
            deals_oprettet = []

        cur.execute("""
            SELECT COALESCE([owner_name],'Ukendt') AS owner_name, COUNT(*) AS deals
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won' AND [account]=%s AND [pipeline_name] IN ('Job','Banner')
              AND [won_time] >= %s AND [won_time] < %s
            GROUP BY [owner_name] ORDER BY deals DESC
        """, (NO_ADV_ACCOUNT, m_start.isoformat(), m_end.isoformat()))
        deals_vundet = [{"owner_name": r["owner_name"], "deals": int(r["deals"] or 0)} for r in cur.fetchall()]

        cur.execute(f"""
            SELECT COALESCE([owner_name],'Ukendt') AS owner_name,
                   ISNULL(SUM({_NO_VAL}),0) AS revenue
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won' AND [account]=%s AND [pipeline_name] IN ('Job','Banner')
              AND [won_time] >= %s AND [won_time] < %s
            GROUP BY [owner_name] ORDER BY revenue DESC
        """, (NO_ADV_ACCOUNT, m_start.isoformat(), m_end.isoformat()))
        deals_omsaetning = [{"owner_name": r["owner_name"], "revenue": round(float(r["revenue"] or 0), 2)} for r in cur.fetchall()]

        # Omsætning mod budget pr. ben (Job/Banner) for indeværende måned.
        # Omsætning på service_activation_date som de danske budget-paneler;
        # budget fra BudgetsIntoMedia, Brand 'Watch NO'.
        budget_chart = []
        for pipeline in ("Job", "Banner"):
            cur.execute(f"""
                SELECT ISNULL(SUM({_NO_VAL}),0) AS revenue
                FROM [dbo].[PipedriveDeals]
                WHERE [status]='won' AND [account]=%s AND [pipeline_name]=%s
                  AND [service_activation_date] >= %s AND [service_activation_date] < %s
                  {_ADM_EXCLUDE}
            """, (NO_ADV_ACCOUNT, pipeline, m_start.isoformat(), m_end.isoformat()))
            revenue = float((cur.fetchone() or {}).get("revenue", 0) or 0)

            cur.execute("""
                SELECT ISNULL(SUM([BudgetAmount]),0) AS budget FROM [dbo].[BudgetsIntoMedia]
                WHERE [DealType]=%s AND [Brand]='Watch NO'
                  AND [BudgetDate] >= %s AND [BudgetDate] < %s
            """, (pipeline, m_start.isoformat(), m_end.isoformat()))
            budget = float((cur.fetchone() or {}).get("budget", 0) or 0)

            budget_chart.append({"brand": pipeline, "revenue": round(revenue, 2), "budget": round(budget, 2)})

        # Mange norske job-deals har intet site angivet, så pipeline (Job/Banner)
        # vises som type-kolonne i seneste-deals-tabellen.
        cur.execute(f"""
            SELECT TOP 20
                COALESCE([owner_name],'Ukendt') AS owner_name,
                COALESCE([org_name],'')         AS org_name,
                COALESCE([sites],'')            AS brand,
                [pipeline_name]                 AS pipeline,
                {_NO_VAL} AS value,
                CONVERT(VARCHAR(19),[won_time],120) AS won_time
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won' AND [account]=%s AND [pipeline_name] IN ('Job','Banner')
            ORDER BY [won_time] DESC
        """, (NO_ADV_ACCOUNT,))
        seneste_deals = [{"owner_name": r["owner_name"], "org_name": r["org_name"], "brand": r["brand"],
                          "pipeline": str(r["pipeline"] or ""), "value": float(r["value"] or 0),
                          "won_time": str(r["won_time"] or "")} for r in cur.fetchall()]

        conn.close()
        return {
            "kpis": kpis,
            "maaned_chart": maaned_chart, "kvartal_chart": kvartal_chart,
            "deals_oprettet": deals_oprettet, "deals_vundet": deals_vundet,
            "deals_omsaetning": deals_omsaetning,
            "budget_chart": budget_chart, "seneste_deals": seneste_deals,
            "maaned_label": MONTH_NAMES_DA[today.month - 1] + " " + str(today.year),
            "kvartal_label": _quarter_label(today) + " " + str(today.year),
            "today": today.isoformat(),
        }
    except Exception:
        logger.exception("db_no_advertising_performance fejlede")
        raise


# ════════════════════════════════════════════════════════════════════════════
#  DASHBOARD 5 — Media Performance
# ════════════════════════════════════════════════════════════════════════════

def db_media_performance(selected_accounts: list | None = None,
                         selected_years: list | None = None,
                         mode: str = "abonnement",
                         selected_months: list | None = None):
    """Media Performance pr. site, afgrænset på account.

    mode:
      "abonnement" — abonnementsomsætning (deal_type abonnement/subscription)
                     INKL. Web Sale. Placeres på året via [service_activation_date]
                     (tilvækst). Viser cancellations, net og 'heraf web salg'.
                     Budget = BudgetsIntoMedia DealType=Subscription (alle salestypes,
                     dvs. inkl. websale-budget) pr. Site.
      "banner"     — kun banner-omsætning (pipeline Banner/Bannerads), [won_time].
                     Budget = BudgetsIntoMedia DealType=Banner pr. Site.
      "job"        — kun job-omsætning (pipeline Job/Jobmarked/Jobads), [won_time].
                     Budget = BudgetsIntoMedia DealType=Job pr. Site.

    selected_accounts: liste af accounts (watch_medier/monitor/watch_no/...).
    Site-universet udledes dynamisk af de valgte accounts. Budget matches på
    [Site] (BudgetsIntoMedia har ingen account-kolonne).
    """
    mode = (mode or "abonnement").lower()
    if mode not in ("abonnement", "banner", "job"):
        mode = "abonnement"
    # Abonnement og annonce ligger i forskellige accounts.
    mode_accounts = MEDIA_ACCOUNTS_SUB if mode == "abonnement" else MEDIA_ACCOUNTS_ADS
    empty = {"mode": mode, "rows": [], "total": {}, "available_years": [],
             "available_accounts": list(mode_accounts),
             "account_labels": dict(MEDIA_ACCOUNT_LABELS)}
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)

        if selected_accounts:
            accounts = [a for a in selected_accounts if a in mode_accounts]
        else:
            accounts = list(mode_accounts)
        if not accounts:
            conn.close()
            return empty
        acc_ph = "(" + ",".join(["%s"] * len(accounts)) + ")"

        # Abonnement = tilvækst → placeres på service_activation_date.
        # Annonce (banner/job) → placeres på won_time.
        date_col = "service_activation_date" if mode == "abonnement" else "won_time"

        year_clause = ""
        year_params: tuple = ()
        if selected_years:
            year_ph = "(" + ",".join(["%s"] * len(selected_years)) + ")"
            year_clause = f"AND YEAR([{date_col}]) IN {year_ph}"
            year_params = tuple(int(y) for y in selected_years)

        year_budget_clause = ""
        year_budget_params: tuple = ()
        if selected_years:
            year_ph2 = "(" + ",".join(["%s"] * len(selected_years)) + ")"
            year_budget_clause = f"AND YEAR([BudgetDate]) IN {year_ph2}"
            year_budget_params = tuple(int(y) for y in selected_years)

        # Måneds-filter (flervalg) — afgrænser både omsætning og budget på måned.
        month_clause = ""
        month_params: tuple = ()
        month_budget_clause = ""
        month_budget_params: tuple = ()
        if selected_months:
            months = [int(m) for m in selected_months if str(m).strip().isdigit()
                      and 1 <= int(m) <= 12]
            if months:
                m_ph = "(" + ",".join(["%s"] * len(months)) + ")"
                month_clause = f"AND MONTH([{date_col}]) IN {m_ph}"
                month_params = tuple(months)
                month_budget_clause = f"AND MONTH([BudgetDate]) IN {m_ph}"
                month_budget_params = tuple(months)

        # Case-insensitiv site-nøgle, så 'idrætsmonitor' matcher 'Idrætsmonitor' osv.
        def _norm(s):
            return (s or "").strip().lower()

        # Site-univers for de valgte accounts (så budget-only sites også kan vises).
        cur.execute(f"""
            SELECT DISTINCT [sites] AS s FROM [dbo].[PipedriveDeals]
            WHERE [account] IN {acc_ph} AND [sites] IS NOT NULL AND [sites] <> ''
        """, tuple(accounts))
        sites = [r["s"] for r in cur.fetchall()]
        if not sites:
            conn.close()
            return empty
        sites_ph = "(" + ",".join(["%s"] * len(sites)) + ")"

        cancel_map: dict = {}
        ws_map: dict = {}

        if mode == "abonnement":
            # Gross = abonnement/subscription INKL. Web Sale (uanset deal_type).
            cur.execute(f"""
                SELECT [sites],
                       ISNULL(SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2))),0) AS gross
                FROM [dbo].[PipedriveDeals]
                WHERE [status]='won'
                  AND [account] IN {acc_ph}
                  AND [pipeline_name] NOT IN {_CANCEL_PH}
                  AND [pipeline_name] NOT IN ('banner','job','Bannerads','Jobads','Jobmarked')
                  AND (LOWER([deal_type]) IN ('abonnement','subscription') OR [pipeline_name]='Web Sale')
                  {year_clause} {month_clause} {_ADM_EXCLUDE_ALLOW_WEBSALE}
                GROUP BY [sites]
            """, tuple(accounts) + tuple(CANCELLATION_PIPELINES) + year_params + month_params)
            gross_map = {_norm(r["sites"]): float(r["gross"] or 0) for r in cur.fetchall()}

            # Heraf web salg (pipeline Web Sale).
            cur.execute(f"""
                SELECT [sites],
                       ISNULL(SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2))),0) AS ws
                FROM [dbo].[PipedriveDeals]
                WHERE [status]='won' AND [pipeline_name]='Web Sale'
                  AND [account] IN {acc_ph} {year_clause} {month_clause} {_ADM_EXCLUDE_ALLOW_WEBSALE}
                GROUP BY [sites]
            """, tuple(accounts) + year_params + month_params)
            ws_map = {_norm(r["sites"]): float(r["ws"] or 0) for r in cur.fetchall()}

            # Cancellations — SAMME ADM/System-Admin-hygiene som gross (symmetrisk),
            # ellers overdrives churn og net bliver kunstigt lavt.
            cur.execute(f"""
                SELECT [sites],
                       ABS(ISNULL(SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2))),0)) AS cancel
                FROM [dbo].[PipedriveDeals]
                WHERE [status]='won' AND [pipeline_name] IN {_CANCEL_PH}
                  AND [account] IN {acc_ph} {year_clause} {month_clause} {_ADM_EXCLUDE}
                GROUP BY [sites]
            """, tuple(CANCELLATION_PIPELINES) + tuple(accounts) + year_params + month_params)
            cancel_map = {_norm(r["sites"]): float(r["cancel"] or 0) for r in cur.fetchall()}

            cur.execute(f"""
                SELECT [Site] AS site, ISNULL(SUM([BudgetAmount]),0) AS budget
                FROM [dbo].[BudgetsIntoMedia]
                WHERE LOWER([DealType])='subscription' AND [Site] IN {sites_ph} {year_budget_clause} {month_budget_clause}
                GROUP BY [Site]
            """, tuple(sites) + year_budget_params + month_budget_params)
            budget_map = {_norm(r["site"]): float(r["budget"] or 0) for r in cur.fetchall()}

        else:
            pipes = BANNER_PIPELINES if mode == "banner" else JOB_PIPELINES
            pipes_ph = "(" + ",".join(["%s"] * len(pipes)) + ")"
            deal_type = "Banner" if mode == "banner" else "Job"

            cur.execute(f"""
                SELECT [sites],
                       ISNULL(SUM(CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2))),0) AS gross
                FROM [dbo].[PipedriveDeals]
                WHERE [status]='won' AND [pipeline_name] IN {pipes_ph}
                  AND [account] IN {acc_ph} {year_clause} {month_clause} {_ADM_EXCLUDE}
                GROUP BY [sites]
            """, tuple(pipes) + tuple(accounts) + year_params + month_params)
            gross_map = {_norm(r["sites"]): float(r["gross"] or 0) for r in cur.fetchall()}

            cur.execute(f"""
                SELECT [Site] AS site, ISNULL(SUM([BudgetAmount]),0) AS budget
                FROM [dbo].[BudgetsIntoMedia]
                WHERE LOWER([DealType])=LOWER(%s) AND [Site] IN {sites_ph} {year_budget_clause} {month_budget_clause}
                GROUP BY [Site]
            """, (deal_type,) + tuple(sites) + year_budget_params + month_budget_params)
            budget_map = {_norm(r["site"]): float(r["budget"] or 0) for r in cur.fetchall()}

        cur.execute(f"""
            SELECT DISTINCT YEAR([{date_col}]) AS aar FROM [dbo].[PipedriveDeals]
            WHERE [status]='won' AND [account] IN {acc_ph} AND [{date_col}] IS NOT NULL
            ORDER BY aar DESC
        """, tuple(accounts))
        available_years = [int(r["aar"]) for r in cur.fetchall() if r["aar"]]

        conn.close()

        is_sub = (mode == "abonnement")
        rows = []
        for site in sites:
            k = _norm(site)
            gross  = round(gross_map.get(k, 0.0), 2)
            cancel = round(cancel_map.get(k, 0.0), 2)
            net    = round(gross - cancel, 2)
            websale = round(ws_map.get(k, 0.0), 2) if is_sub else None
            budget = round(budget_map.get(k, 0.0), 2)
            index  = round(net / budget * 100, 2) if budget > 0 else None
            if gross == 0 and cancel == 0 and budget == 0:
                continue
            rows.append({"site": site, "gross": gross, "cancel": cancel, "net": net,
                         "websale": websale, "budget": budget, "index": index})

        rows.sort(key=lambda x: -(x["net"] or 0))

        tg = round(sum(r["gross"]  for r in rows), 2)
        tc = round(sum(r["cancel"] for r in rows), 2)
        tn = round(sum(r["net"]    for r in rows), 2)
        tw = round(sum((r["websale"] or 0) for r in rows), 2) if is_sub else None
        tb = round(sum(r["budget"] for r in rows), 2)

        return {
            "mode":             mode,
            "rows":             rows,
            "total":            {"gross": tg, "cancel": tc, "net": tn, "websale": tw,
                                 "budget": tb, "index": round(tn / tb * 100, 2) if tb > 0 else None},
            "available_years":   available_years,
            "available_accounts": list(mode_accounts),
            "account_labels":     dict(MEDIA_ACCOUNT_LABELS),
        }
    except Exception:
        logger.exception("db_media_performance fejlede")
        raise

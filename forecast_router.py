import traceback
import os
from datetime import date

from dotenv import load_dotenv
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import pymssql

from auth import ROLE_LABELS, get_current_user, has_access

load_dotenv()

router = APIRouter(prefix="/tools/forecast", tags=["Forecast"])
templates = Jinja2Templates(directory="templates")
templates.env.globals["ROLE_LABELS"] = ROLE_LABELS


# ---------------------------------------------------------------------------
# Shared brand/site constants (same as perf_router.py)
# ---------------------------------------------------------------------------

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

# SQL expression that derives a brand key from a deal's site name
# Mirrors the CASE WHEN in perf_router.py's overview endpoint
_SITE_TO_BRAND_SQL = """
    CASE
        WHEN [sites] LIKE '%onitor%'                          THEN 'monitor'
        WHEN [sites] = 'FINANS DK'                           THEN 'finans'
        WHEN [sites] LIKE '%Watch%' AND [sites] LIKE '% SE'  THEN 'watch_se'
        WHEN [sites] LIKE '%Watch%' AND [sites] LIKE '% NO'  THEN 'watch_no'
        WHEN [sites] LIKE '%Watch%' AND [sites] LIKE '% DE'  THEN 'watch_de'
        WHEN [sites] LIKE '%Watch%'                          THEN 'watch_dk'
        WHEN [sites] LIKE '%FINANS%' OR [sites] LIKE '%Finans%' THEN 'finans_int'
        WHEN [sites] LIKE '%arketWire%'                      THEN 'marketwire'
        ELSE NULL
    END
"""

# Cancellation pipelines (same as perf_router.py)
_CANCEL_PIPELINES = ["Cancellation", "Cancellations", "Opsigelser"]
_CANCEL_PH = "(" + ",".join(["%s"] * len(_CANCEL_PIPELINES)) + ")"


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


def require_forecast_access(user: dict):
    if not has_access(user, "sales_manager"):
        raise HTTPException(status_code=403, detail="Ingen adgang til Forecast Tool")


# ---------------------------------------------------------------------------
# Ensure HubForecasts has adjustment_pct column (run once on startup)
# ---------------------------------------------------------------------------

def _ensure_schema():
    try:
        conn = get_conn()
        cur  = conn.cursor()
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
        pass  # Table may not exist yet — safe to ignore

_ensure_schema()


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------

@router.get("/", response_class=HTMLResponse)
async def forecast_tool(request: Request, user=Depends(get_current_user)):
    require_forecast_access(user)
    today = date.today()
    return templates.TemplateResponse("forecast_tool.html", {
        "request": request,
        "user": user,
        "months": [
            (1, "Januar"), (2, "Februar"), (3, "Marts"), (4, "April"),
            (5, "Maj"), (6, "Juni"), (7, "Juli"), (8, "August"),
            (9, "September"), (10, "Oktober"), (11, "November"), (12, "December"),
        ],
        "years":         list(range(today.year - 2, today.year + 3)),
        "current_year":  today.year,
        "current_month": today.month,
    })


# ---------------------------------------------------------------------------
# Teams API — returnerer teams fra Teams-tabellen (autorisativ kilde)
# ---------------------------------------------------------------------------

@router.get("/teams")
async def forecast_teams(user=Depends(get_current_user)):
    require_forecast_access(user)
    try:
        conn = get_conn()
        cur  = conn.cursor(as_dict=True)
        cur.execute("""
            SELECT t.name, ISNULL(t.brand, '') AS brand
            FROM   Teams t
            WHERE  t.name IS NOT NULL
            ORDER BY t.name
        """)
        teams = [{"name": r["name"], "brand": r["brand"]} for r in cur.fetchall()]
        conn.close()
        return JSONResponse({"teams": teams})
    except Exception:
        # Fallback: læs fra SalesPersonBudget hvis Teams-tabel ikke er tilgængelig
        try:
            conn = get_conn()
            cur  = conn.cursor(as_dict=True)
            cur.execute("""
                SELECT DISTINCT [Team] AS name, '' AS brand
                FROM [dbo].[SalesPersonBudget]
                WHERE [Team] IS NOT NULL
                ORDER BY [Team]
            """)
            teams = [{"name": r["name"], "brand": ""} for r in cur.fetchall()]
            conn.close()
            return JSONResponse({"teams": teams})
        except Exception as e:
            traceback.print_exc()
            raise HTTPException(500, str(e))


# ---------------------------------------------------------------------------
# Helper: build team filter clauses for PipedriveDeals
# ---------------------------------------------------------------------------

def _build_team_filter(team: str | None, team_brand: str | None) -> tuple[str, list, list]:
    """
    Returnerer (where_clause, extra_params_before_main, extra_params_after_site_filter).
    Filteret begrænser:
      - sites til de brands der tilhører teamets brand-gruppe
      - owner_name til de sælgere der er aktive i teamet (via TeamMemberships)

    Returnerer tre elementer:
      - where_clause: SQL-streng der indsættes EFTER basis WHERE-betingelser
      - site_list: liste af sites (eller SUBSCRIPTION_BRANDS som fallback)
      - owner_clause: SQL-streng + params for owner-filter
    """
    # Bestem site-listen baseret på team_brand
    if team_brand and team_brand in BRAND_GROUPS:
        site_list = BRAND_GROUPS[team_brand]
    else:
        site_list = SUBSCRIPTION_BRANDS

    sites_ph = "(" + ",".join(["%s"] * len(site_list)) + ")"

    if team:
        # Filter: sites tilhørende brand OG ejere der er aktive i teamet
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


# ---------------------------------------------------------------------------
# Data API
# ---------------------------------------------------------------------------

@router.get("/data")
async def forecast_data(
    year:       int,
    month:      int,
    level:      str,
    team:       str | None = None,        # Teams.name (f.eks. "Watch DK")
    team_brand: str | None = None,        # Teams.brand (f.eks. "watch_dk")
    user=Depends(get_current_user),
):
    require_forecast_access(user)
    if level not in ("medie", "saelger", "team"):
        raise HTTPException(400, "level skal være 'medie', 'saelger' eller 'team'")

    year_m1 = year - 1
    year_m2 = year - 2

    # ── Bestem dimension-kolonne og budget-kilde ──────────────────────────────
    if level == "medie":
        dim_col = "sites"
    elif level == "saelger":
        dim_col = "owner_name"
    else:
        dim_col = None  # level='team' bruger CASE WHEN i stedet

    # ── Byg team-filter ───────────────────────────────────────────────────────
    site_clause, site_list, owner_clause, owner_params = _build_team_filter(team, team_brand)
    sites_ph = "(" + ",".join(["%s"] * len(site_list)) + ")"

    # Fælles base-filter parametre (sites)
    base_site_params = tuple(site_list)

    try:
        conn = get_conn()
        cur  = conn.cursor(as_dict=True)

        # ── Q1: Historisk tilvækst — to år (year-2 og year-1) ──────────────────
        # Bruger service_activation_date (fallback: won_time) + value_dkk (fallback: value)
        if level in ("medie", "saelger"):
            hist_sql = f"""
                SELECT [{dim_col}] AS dimension_key,
                       YEAR(COALESCE([service_activation_date], [won_time])) AS data_year,
                       SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS tilvækst
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'won'
                  AND [pipeline_name] <> 'Web Sale'
                  AND [{dim_col}] IS NOT NULL
                  AND MONTH(COALESCE([service_activation_date], [won_time])) = %s
                  AND YEAR(COALESCE([service_activation_date], [won_time])) IN (%s, %s)
                  {site_clause}
                  {owner_clause}
                GROUP BY [{dim_col}], YEAR(COALESCE([service_activation_date], [won_time]))
            """
            hist_params = (month, year_m1, year_m2) + base_site_params + tuple(owner_params)
        else:
            # level='team': grupér på [team]-kolonnen (samme som PipedriveDeals.team)
            hist_sql = f"""
                SELECT [team] AS dimension_key,
                       YEAR(COALESCE([service_activation_date], [won_time])) AS data_year,
                       SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS tilvækst
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'won'
                  AND [pipeline_name] <> 'Web Sale'
                  AND [team] IS NOT NULL
                  AND MONTH(COALESCE([service_activation_date], [won_time])) = %s
                  AND YEAR(COALESCE([service_activation_date], [won_time])) IN (%s, %s)
                  AND [sites] IN {_SUB_PH}
                GROUP BY [team], YEAR(COALESCE([service_activation_date], [won_time]))
            """
            hist_params = (month, year_m1, year_m2) + tuple(SUBSCRIPTION_BRANDS)

        cur.execute(hist_sql, hist_params)

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

        # ── Q2: Åben pipeline — åbne deals med expected_close_date i måneden ───
        if level in ("medie", "saelger"):
            pipe_sql = f"""
                SELECT [{dim_col}] AS dimension_key,
                       SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS open_pipeline
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'open'
                  AND [pipeline_name] <> 'Web Sale'
                  AND [{dim_col}] IS NOT NULL
                  AND [expected_close_date] IS NOT NULL
                  AND MONTH([expected_close_date]) = %s
                  AND YEAR([expected_close_date]) = %s
                  {site_clause}
                  {owner_clause}
                GROUP BY [{dim_col}]
            """
            pipe_params = (month, year) + base_site_params + tuple(owner_params)
        else:
            pipe_sql = f"""
                SELECT [team] AS dimension_key,
                       SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS open_pipeline
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'open'
                  AND [pipeline_name] <> 'Web Sale'
                  AND [team] IS NOT NULL
                  AND [expected_close_date] IS NOT NULL
                  AND MONTH([expected_close_date]) = %s
                  AND YEAR([expected_close_date]) = %s
                  AND [sites] IN {_SUB_PH}
                GROUP BY [team]
            """
            pipe_params = (month, year) + tuple(SUBSCRIPTION_BRANDS)

        cur.execute(pipe_sql, pipe_params)
        pipe = {r["dimension_key"]: float(r["open_pipeline"] or 0)
                for r in cur.fetchall() if r["dimension_key"]}

        # ── Q3: Realiseret tilvækst i indeværende år (service_activation_date) ─
        if level in ("medie", "saelger"):
            act_sql = f"""
                SELECT [{dim_col}] AS dimension_key,
                       SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS activation_amount
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'won'
                  AND [pipeline_name] <> 'Web Sale'
                  AND [{dim_col}] IS NOT NULL
                  AND [service_activation_date] IS NOT NULL
                  AND MONTH([service_activation_date]) = %s
                  AND YEAR([service_activation_date]) = %s
                  {site_clause}
                  {owner_clause}
                GROUP BY [{dim_col}]
            """
            act_params = (month, year) + base_site_params + tuple(owner_params)
        else:
            act_sql = f"""
                SELECT [team] AS dimension_key,
                       SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS activation_amount
                FROM [dbo].[PipedriveDeals]
                WHERE [status] = 'won'
                  AND [pipeline_name] <> 'Web Sale'
                  AND [team] IS NOT NULL
                  AND [service_activation_date] IS NOT NULL
                  AND MONTH([service_activation_date]) = %s
                  AND YEAR([service_activation_date]) = %s
                  AND [sites] IN {_SUB_PH}
                GROUP BY [team]
            """
            act_params = (month, year) + tuple(SUBSCRIPTION_BRANDS)

        cur.execute(act_sql, act_params)
        activation = {r["dimension_key"]: float(r["activation_amount"] or 0)
                      for r in cur.fetchall() if r["dimension_key"]}

        # ── Q4: Budget ─────────────────────────────────────────────────────────
        if level == "medie":
            cur.execute("""
                SELECT [Site] AS dimension_key,
                       SUM([BudgetAmount]) AS budget
                FROM [dbo].[BudgetsIntoMedia]
                WHERE YEAR([BudgetDate]) = %s AND MONTH([BudgetDate]) = %s
                GROUP BY [Site]
            """, (year, month))
            budgets = {r["dimension_key"]: float(r["budget"] or 0) for r in cur.fetchall()}

        elif level == "saelger":
            # Budget pr. sælger — filtrér på brand (team_brand) hvis valgt
            if team_brand:
                cur.execute("""
                    SELECT [Owner] AS dimension_key,
                           SUM([BudgetAmount]) AS budget
                    FROM [dbo].[SalesPersonBudget]
                    WHERE YEAR([BudgetDate]) = %s AND MONTH([BudgetDate]) = %s
                      AND [Brand] = %s
                    GROUP BY [Owner]
                """, (year, month, team_brand))
            else:
                cur.execute("""
                    SELECT [Owner] AS dimension_key,
                           SUM([BudgetAmount]) AS budget
                    FROM [dbo].[SalesPersonBudget]
                    WHERE YEAR([BudgetDate]) = %s AND MONTH([BudgetDate]) = %s
                    GROUP BY [Owner]
                """, (year, month))
            budgets = {r["dimension_key"]: float(r["budget"] or 0) for r in cur.fetchall()}

        else:  # level='team' — budget pr. team-navn (matcher PipedriveDeals.team)
            cur.execute("""
                SELECT [Team] AS dimension_key,
                       SUM([BudgetAmount]) AS budget
                FROM [dbo].[SalesPersonBudget]
                WHERE YEAR([BudgetDate]) = %s AND MONTH([BudgetDate]) = %s
                  AND [Team] IS NOT NULL AND [Team] <> ''
                GROUP BY [Team]
            """, (year, month))
            budgets = {r["dimension_key"]: float(r["budget"] or 0) for r in cur.fetchall()}

        # ── Q5: Tidligere gemte forecasts ──────────────────────────────────────
        cur.execute("""
            SELECT dimension_key, pipeline_pct, adjustment_pct, manual_amount, forecast_amount
            FROM [dbo].[HubForecasts]
            WHERE forecast_year = %s AND forecast_month = %s AND level = %s
        """, (year, month, level))
        saved = {r["dimension_key"]: r for r in cur.fetchall()}

        conn.close()

        # ── Byg rækker ─────────────────────────────────────────────────────────
        all_keys = sorted(set(
            list(hist_m1.keys()) + list(hist_m2.keys()) +
            list(pipe.keys()) + list(budgets.keys()) + list(activation.keys())
        ))

        rows = []
        for key in all_keys:
            sv             = saved.get(key, {})
            pipeline_pct   = float(sv.get("pipeline_pct")   or 30.0)
            adjustment_pct = float(sv.get("adjustment_pct") or 0.0)
            manual_amount  = float(sv.get("manual_amount")  or 0.0)
            h1             = hist_m1.get(key, 0.0)
            h2             = hist_m2.get(key, 0.0)

            # Historisk gennemsnit — inkludér kun år med data (positive OR negative tilvækst tæller)
            available  = [v for v in [h1, h2] if v != 0.0]
            hist_avg   = sum(available) / len(available) if available else 0.0

            p  = pipe.get(key, 0.0)
            a  = activation.get(key, 0.0)
            b  = budgets.get(key, 0.0)

            # Forecast-formel:
            # adjusted_hist = hist_avg * (1 + adjustment_pct/100)
            # forecast = adjusted_hist + pipeline_contribution + manual_amount
            adj_factor     = 1.0 + adjustment_pct / 100.0
            adjusted_hist  = hist_avg * adj_factor
            forecast_total = round(adjusted_hist + (p * pipeline_pct / 100) + manual_amount, 2)

            # dimension_key er nu holdnavn for level=team, direkte brugbart som label
            display_label = key

            rows.append({
                "dimension_key":   key,
                "display_label":   display_label,
                "hist_year_m2":    round(h2, 2),
                "hist_year_m1":    round(h1, 2),
                "historical_avg":  round(hist_avg, 2),
                "adjusted_hist":   round(adjusted_hist, 2),
                "activation":      round(a, 2),
                "open_pipeline":   round(p, 2),
                "pipeline_pct":    pipeline_pct,
                "adjustment_pct":  adjustment_pct,
                "manual_amount":   manual_amount,
                "forecast_total":  forecast_total,
                "budget":          round(b, 2),
                "is_saved":        key in saved,
            })

        return JSONResponse({
            "rows":    rows,
            "year":    year,
            "month":   month,
            "level":   level,
            "year_m1": year_m1,
            "year_m2": year_m2,
        })

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


# ---------------------------------------------------------------------------
# Save API
# ---------------------------------------------------------------------------

@router.post("/save")
async def forecast_save(request: Request, user=Depends(get_current_user)):
    require_forecast_access(user)
    body  = await request.json()
    year  = body.get("year")
    month = body.get("month")
    level = body.get("level")
    rows  = body.get("rows", [])

    if not all([year, month, level]) or level not in ("medie", "saelger", "team"):
        raise HTTPException(400, "Ugyldige parametre")

    saved_count = 0
    try:
        conn = get_conn()
        cur  = conn.cursor()

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
                  pipeline_pct, adjustment_pct, manual_amount, forecast_amt, user["name"]))
            saved_count += 1

        conn.commit()
        conn.close()
        return JSONResponse({"status": "ok", "saved": saved_count})

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))

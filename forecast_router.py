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


def require_forecast_access(user: dict):
    if not has_access(user, "sales_manager"):
        raise HTTPException(status_code=403, detail="Ingen adgang til Forecast Tool")


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
        "years": list(range(today.year - 2, today.year + 3)),
        "current_year": today.year,
        "current_month": today.month,
    })


# ---------------------------------------------------------------------------
# Teams API
# ---------------------------------------------------------------------------

@router.get("/teams")
async def forecast_teams(user=Depends(get_current_user)):
    require_forecast_access(user)
    try:
        conn = get_conn()
        cur  = conn.cursor(as_dict=True)
        cur.execute("""
            SELECT DISTINCT [Team]
            FROM [dbo].[SalesPersonBudget]
            WHERE [Team] IS NOT NULL
            ORDER BY [Team]
        """)
        teams = [r["Team"] for r in cur.fetchall()]
        conn.close()
        return JSONResponse({"teams": teams})
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


# ---------------------------------------------------------------------------
# Data API
# ---------------------------------------------------------------------------

@router.get("/data")
async def forecast_data(
    year:  int,
    month: int,
    level: str,
    team:  str | None = None,
    user=Depends(get_current_user),
):
    require_forecast_access(user)
    if level not in ("medie", "saelger"):
        raise HTTPException(400, "level skal være 'medie' eller 'saelger'")

    dim_col      = "sites"            if level == "medie" else "owner_name"
    budget_table = "BudgetsIntoMedia" if level == "medie" else "SalesPersonBudget"
    budget_dim   = "Site"             if level == "medie" else "Owner"
    year_m1 = year - 1
    year_m2 = year - 2

    # ── Team filter på PipedriveDeals (begge levels via owner_name) ──────────
    team_pd_filter = ""
    team_pd_params: tuple = ()
    if team:
        team_pd_filter = "AND [owner_name] IN (SELECT DISTINCT [Owner] FROM [dbo].[SalesPersonBudget] WHERE [Team] = %s)"
        team_pd_params = (team,)

    # ── Team filter på budget ─────────────────────────────────────────────────
    team_budget_filter = ""
    team_budget_params: tuple = ()
    if team and level == "saelger":
        team_budget_filter = "AND [Team] = %s"
        team_budget_params = (team,)

    try:
        conn = get_conn()
        cur  = conn.cursor(as_dict=True)

        # 1. Historisk omsætning — to separate år (year-2 og year-1)
        cur.execute(f"""
            SELECT [{dim_col}] AS dimension_key,
                   YEAR([won_time]) AS won_year,
                   SUM(CAST([value] AS DECIMAL(18,2))) AS revenue
            FROM [dbo].[PipedriveDeals]
            WHERE [status] = 'won'
              AND [pipeline_name] <> 'Web Sale'
              AND [{dim_col}] IS NOT NULL
              AND [won_time] IS NOT NULL
              AND MONTH([won_time]) = %s
              AND YEAR([won_time]) IN (%s, %s)
              {team_pd_filter}
            GROUP BY [{dim_col}], YEAR([won_time])
        """, (month, year_m1, year_m2) + team_pd_params)

        hist_m1: dict[str, float] = {}
        hist_m2: dict[str, float] = {}
        for r in cur.fetchall():
            key = r["dimension_key"]
            if r["won_year"] == year_m1:
                hist_m1[key] = float(r["revenue"] or 0)
            else:
                hist_m2[key] = float(r["revenue"] or 0)

        # 2. Open pipeline — åbne deals med expected_close_date i måneden
        cur.execute(f"""
            SELECT [{dim_col}] AS dimension_key,
                   SUM(CAST([value] AS DECIMAL(18,2))) AS open_pipeline
            FROM [dbo].[PipedriveDeals]
            WHERE [status] = 'open'
              AND [pipeline_name] <> 'Web Sale'
              AND [{dim_col}] IS NOT NULL
              AND [expected_close_date] IS NOT NULL
              AND MONTH([expected_close_date]) = %s
              AND YEAR([expected_close_date]) = %s
              {team_pd_filter}
            GROUP BY [{dim_col}]
        """, (month, year) + team_pd_params)
        pipe = {r["dimension_key"]: float(r["open_pipeline"] or 0) for r in cur.fetchall()}

        # 3. Service activation — won deals med service_activation_date i måneden
        cur.execute(f"""
            SELECT [{dim_col}] AS dimension_key,
                   SUM(CAST([value] AS DECIMAL(18,2))) AS activation_amount
            FROM [dbo].[PipedriveDeals]
            WHERE [status] = 'won'
              AND [pipeline_name] <> 'Web Sale'
              AND [{dim_col}] IS NOT NULL
              AND [service_activation_date] IS NOT NULL
              AND MONTH([service_activation_date]) = %s
              AND YEAR([service_activation_date]) = %s
              {team_pd_filter}
            GROUP BY [{dim_col}]
        """, (month, year) + team_pd_params)
        activation = {r["dimension_key"]: float(r["activation_amount"] or 0) for r in cur.fetchall()}

        # 4. Budget
        cur.execute(f"""
            SELECT [{budget_dim}] AS dimension_key,
                   SUM([BudgetAmount]) AS budget
            FROM [dbo].[{budget_table}]
            WHERE YEAR([BudgetDate]) = %s AND MONTH([BudgetDate]) = %s
              {team_budget_filter}
            GROUP BY [{budget_dim}]
        """, (year, month) + team_budget_params)
        budgets = {r["dimension_key"]: float(r["budget"] or 0) for r in cur.fetchall()}

        # 5. Tidligere gemte forecasts
        cur.execute("""
            SELECT dimension_key, pipeline_pct, manual_amount, forecast_amount
            FROM [dbo].[HubForecasts]
            WHERE forecast_year = %s AND forecast_month = %s AND level = %s
        """, (year, month, level))
        saved = {r["dimension_key"]: r for r in cur.fetchall()}

        conn.close()

        all_keys = sorted(set(
            list(hist_m1.keys()) + list(hist_m2.keys()) +
            list(pipe.keys()) + list(budgets.keys()) + list(activation.keys())
        ))

        rows = []
        for key in all_keys:
            sv            = saved.get(key, {})
            pipeline_pct  = float(sv.get("pipeline_pct") or 30.0)
            manual_amount = float(sv.get("manual_amount") or 0.0)
            h1            = hist_m1.get(key, 0.0)
            h2            = hist_m2.get(key, 0.0)
            available     = [v for v in [h1, h2] if v > 0]
            hist_avg      = sum(available) / len(available) if available else 0.0
            p             = pipe.get(key, 0.0)
            a             = activation.get(key, 0.0)
            b             = budgets.get(key, 0.0)
            forecast_total = round(hist_avg + (p * pipeline_pct / 100) + manual_amount, 2)
            rows.append({
                "dimension_key":  key,
                "hist_year_m2":   round(h2, 2),
                "hist_year_m1":   round(h1, 2),
                "historical_avg": round(hist_avg, 2),
                "activation":     round(a, 2),
                "open_pipeline":  round(p, 2),
                "pipeline_pct":   pipeline_pct,
                "manual_amount":  manual_amount,
                "forecast_total": forecast_total,
                "budget":         round(b, 2),
                "is_saved":       key in saved,
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

    if not all([year, month, level]) or level not in ("medie", "saelger"):
        raise HTTPException(400, "Ugyldige parametre")

    saved_count = 0
    try:
        conn = get_conn()
        cur  = conn.cursor()

        for row in rows:
            dim_key       = str(row.get("dimension_key", "")).strip()
            pipeline_pct  = float(row.get("pipeline_pct", 30.0))
            manual_amount = float(row.get("manual_amount", 0.0))
            forecast_amt  = float(row.get("forecast_total", 0.0))

            if not dim_key:
                continue

            cur.execute("""
                DELETE FROM [dbo].[HubForecasts]
                WHERE forecast_year=%s AND forecast_month=%s AND level=%s AND dimension_key=%s
            """, (year, month, level, dim_key))

            cur.execute("""
                INSERT INTO [dbo].[HubForecasts]
                    (forecast_year, forecast_month, level, dimension_key,
                     pipeline_pct, manual_amount, forecast_amount, created_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (year, month, level, dim_key,
                  pipeline_pct, manual_amount, forecast_amt, user["name"]))
            saved_count += 1

        conn.commit()
        conn.close()
        return JSONResponse({"status": "ok", "saved": saved_count})

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))

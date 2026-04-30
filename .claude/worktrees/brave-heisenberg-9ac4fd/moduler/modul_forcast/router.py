import traceback
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from auth import ROLE_LABELS, get_current_user, has_access
from moduler.modul_forcast.queries import (
    ensure_schema, db_get_teams, db_forecast_data, db_forecast_save,
    BRAND_GROUPS, BRAND_LABELS, SUBSCRIPTION_BRANDS,
)

router = APIRouter(prefix="/tools/forecast", tags=["Forecast"])
templates = Jinja2Templates(directory="templates")
templates.env.globals["ROLE_LABELS"] = ROLE_LABELS

ensure_schema()

MONTHS = [
    (1, "Januar"), (2, "Februar"), (3, "Marts"), (4, "April"),
    (5, "Maj"), (6, "Juni"), (7, "Juli"), (8, "August"),
    (9, "September"), (10, "Oktober"), (11, "November"), (12, "December"),
]


def require_forecast_access(user: dict):
    if not has_access(user, "sales_manager"):
        raise HTTPException(status_code=403, detail="Ingen adgang til Forecast Tool")


@router.get("/", response_class=HTMLResponse)
async def forecast_tool(request: Request, user=Depends(get_current_user)):
    require_forecast_access(user)
    today = date.today()
    return templates.TemplateResponse("forecast_tool.html", {
        "request":       request,
        "user":          user,
        "months":        MONTHS,
        "years":         list(range(today.year - 2, today.year + 3)),
        "current_year":  today.year,
        "current_month": today.month,
    })


@router.get("/teams")
async def forecast_teams(user=Depends(get_current_user)):
    require_forecast_access(user)
    try:
        teams = db_get_teams()
        return JSONResponse({"teams": teams})
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@router.get("/data")
async def forecast_data(
    year:       int,
    month:      int,
    level:      str,
    team:       str | None = None,
    team_brand: str | None = None,
    user=Depends(get_current_user),
):
    require_forecast_access(user)
    if level not in ("saelger", "team"):
        raise HTTPException(400, "level skal være 'saelger' eller 'team'")

    year_m1 = year - 1
    year_m2 = year - 2

    try:
        hist_m1, hist_m2, pipe, activation, budgets, saved = db_forecast_data(
            year, month, level, team, team_brand
        )

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

            available     = [v for v in [h1, h2] if v != 0.0]
            hist_avg      = sum(available) / len(available) if available else 0.0
            p             = pipe.get(key, 0.0)
            a             = activation.get(key, 0.0)
            b             = budgets.get(key, 0.0)

            adj_factor     = 1.0 + adjustment_pct / 100.0
            adjusted_hist  = hist_avg * adj_factor
            forecast_total = round(adjusted_hist + (p * pipeline_pct / 100) + manual_amount, 2)

            rows.append({
                "dimension_key":  key,
                "display_label":  key,
                "hist_year_m2":   round(h2, 2),
                "hist_year_m1":   round(h1, 2),
                "historical_avg": round(hist_avg, 2),
                "adjusted_hist":  round(adjusted_hist, 2),
                "activation":     round(a, 2),
                "open_pipeline":  round(p, 2),
                "pipeline_pct":   pipeline_pct,
                "adjustment_pct": adjustment_pct,
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


@router.post("/save")
async def forecast_save(request: Request, user=Depends(get_current_user)):
    require_forecast_access(user)
    body  = await request.json()
    year  = body.get("year")
    month = body.get("month")
    level = body.get("level")
    rows  = body.get("rows", [])

    if not all([year, month, level]) or level not in ("saelger", "team"):
        raise HTTPException(400, "Ugyldige parametre")

    try:
        saved_count = db_forecast_save(year, month, level, rows, user["name"])
        return JSONResponse({"status": "ok", "saved": saved_count})
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))
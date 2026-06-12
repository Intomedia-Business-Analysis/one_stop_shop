import logging
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from auth import allowed_data_teams, get_current_user, has_access
from moduler.modul_forcast.queries import (
    ensure_schema, db_get_teams, db_forecast_data,
    db_saelger_forecast_save, db_active_team_members,
    db_get_reviews, db_review_save, db_missing_forecast_teams,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tools/forecast", tags=["Forecast"])
templates = Jinja2Templates(directory="templates")
from nav_utils import register_nav_globals
register_nav_globals(templates)

ensure_schema()

MONTHS = [
    (1, "Januar"), (2, "Februar"), (3, "Marts"), (4, "April"),
    (5, "Maj"), (6, "Juni"), (7, "Juli"), (8, "August"),
    (9, "September"), (10, "Oktober"), (11, "November"), (12, "December"),
]
MONTH_NAMES = dict(MONTHS)

# Påmindelsen om manglende forecast for næste måned vises fra denne dag
REMINDER_FROM_DAY = 20


def require_forecast_access(user: dict):
    if not has_access(user, "salesperson"):
        raise HTTPException(status_code=403, detail="Ingen adgang til Forecast Tool")


def require_manager(user: dict):
    if not has_access(user, "sales_manager"):
        raise HTTPException(status_code=403, detail="Kræver Sales Manager-adgang")


def _is_manager(user: dict) -> bool:
    return has_access(user, "sales_manager")


def _next_month(today: date) -> tuple[int, int]:
    if today.month == 12:
        return today.year + 1, 1
    return today.year, today.month + 1


def _team_brand_map() -> dict:
    try:
        return {t["name"]: (t["brand"] or None) for t in db_get_teams()}
    except Exception:
        logger.warning("Kunne ikke hente team/brand-mapping", exc_info=True)
        return {}


def _fmt_dt(value) -> str | None:
    if not value:
        return None
    try:
        return value.strftime("%d.%m.%Y kl. %H:%M")
    except Exception:
        return str(value)


def _empty_row(name: str, team: str) -> dict:
    return {
        "dimension_key":  name,
        "display_label":  name,
        "team":           team,
        "hist_year_m2":   0.0,
        "hist_year_m1":   0.0,
        "historical_avg": 0.0,
        "adjusted_hist":  0.0,
        "activation":     0.0,
        "open_pipeline":  0.0,
        "pipeline_pct":   30.0,
        "adjustment_pct": 0.0,
        "manual_amount":  0.0,
        "forecast_total": 0.0,
        "budget":         0.0,
        "is_saved":       False,
        "saved_amount":   None,
        "saved_at":       None,
        "saved_by":       None,
    }


def _build_rows(year: int, month: int, team: str, team_brand: str | None) -> list:
    """Beregnede forecast-rækker på sælger-niveau for ét team."""
    hist_m1, hist_m2, pipe, activation, budgets, saved = db_forecast_data(
        year, month, "saelger", team, team_brand
    )

    all_keys = sorted(set(
        list(hist_m1.keys()) + list(hist_m2.keys()) +
        list(pipe.keys()) + list(budgets.keys()) + list(activation.keys()) +
        list(saved.keys())
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

        is_saved = key in saved
        rows.append({
            "dimension_key":  key,
            "display_label":  key,
            "team":           team,
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
            "is_saved":       is_saved,
            "saved_amount":   float(sv.get("forecast_amount") or 0) if is_saved else None,
            "saved_at":       _fmt_dt(sv.get("updated_at")) if is_saved else None,
            "saved_by":       sv.get("created_by") if is_saved else None,
        })
    return rows


# ---------------------------------------------------------------------------
# Sider
# ---------------------------------------------------------------------------

@router.get("/", response_class=HTMLResponse)
async def forecast_tool(request: Request, user=Depends(get_current_user)):
    require_forecast_access(user)
    today = date.today()
    # Forecastet laves for den kommende måned — den er forudvalgt
    def_year, def_month = _next_month(today)
    return templates.TemplateResponse(request, "forecast_tool.html", {
        "user":          user,
        "is_manager":    _is_manager(user),
        "months":        MONTHS,
        "years":         list(range(today.year - 2, today.year + 3)),
        "default_year":  def_year,
        "default_month": def_month,
        "my_teams":      user.get("_teams") or [],
    })


# ---------------------------------------------------------------------------
# Sælger: eget forecast pr. team
# ---------------------------------------------------------------------------

@router.get("/my")
async def my_forecast(year: int, month: int, user=Depends(get_current_user)):
    require_forecast_access(user)
    teams = user.get("_teams") or []
    brand_map = _team_brand_map()

    try:
        rows = []
        for team in teams:
            team_rows = _build_rows(year, month, team, brand_map.get(team))
            mine = next((r for r in team_rows if r["dimension_key"] == user["name"]), None)
            rows.append(mine or _empty_row(user["name"], team))

        return JSONResponse({
            "rows":    rows,
            "year":    year,
            "month":   month,
            "year_m1": year - 1,
            "year_m2": year - 2,
        })
    except Exception:
        logger.exception("my_forecast fejlede (year=%s, month=%s, user=%s)", year, month, user.get("name"))
        raise HTTPException(500, "Data kunne ikke hentes")


@router.post("/my/save")
async def my_forecast_save(request: Request, user=Depends(get_current_user)):
    require_forecast_access(user)
    body  = await request.json()
    year  = body.get("year")
    month = body.get("month")
    rows  = body.get("rows", [])

    if not year or not month:
        raise HTTPException(400, "Ugyldige parametre")

    # Sælgeren må kun gemme på teams, vedkommende er aktivt medlem af
    my_teams = set(user.get("_teams") or [])
    rows = [r for r in rows if str(r.get("team", "")).strip() in my_teams]
    if not rows:
        raise HTTPException(400, "Ingen gyldige rækker — du er ikke medlem af et team")

    try:
        saved_count, updated_teams = db_saelger_forecast_save(year, month, user["name"], rows)
        return JSONResponse({
            "status":        "ok",
            "saved":         saved_count,
            "updated_teams": updated_teams,
        })
    except Exception:
        logger.exception("my_forecast_save fejlede (year=%s, month=%s, user=%s)", year, month, user.get("name"))
        raise HTTPException(500, "Forecast kunne ikke gemmes")


# ---------------------------------------------------------------------------
# Manager: overblik + vurdering
# ---------------------------------------------------------------------------

@router.get("/overview")
async def forecast_overview(year: int, month: int, user=Depends(get_current_user)):
    require_manager(user)

    try:
        teams = db_get_teams()
        allowed = allowed_data_teams(user)
        if allowed is not None:
            teams = [t for t in teams if t["name"] in allowed]

        team_names = [t["name"] for t in teams]
        members    = db_active_team_members(team_names)
        reviews    = db_get_reviews(year, month, team_names)

        out = []
        for t in teams:
            name = t["name"]
            team_rows  = _build_rows(year, month, name, t["brand"] or None)
            rows_by_key = {r["dimension_key"]: r for r in team_rows}
            team_members = members.get(name, [])

            # Aktive medlemmer først; behold derudover gemte forecasts fra
            # sælgere der ikke længere er aktive medlemmer (historik)
            rows = [rows_by_key.get(m) or _empty_row(m, name) for m in team_members]
            rows += [r for k, r in rows_by_key.items()
                     if r["is_saved"] and k not in team_members]

            filled  = [r for r in rows if r["is_saved"]]
            missing = [r["dimension_key"] for r in rows if not r["is_saved"]]
            review  = reviews.get(name)

            out.append({
                "team":         name,
                "brand":        t["brand"] or "",
                "rows":         rows,
                "member_count": len(rows),
                "filled_count": len(filled),
                "missing":      missing,
                "sum_saved":    round(sum(r["saved_amount"] or 0 for r in filled), 2),
                "sum_budget":   round(sum(r["budget"] for r in rows), 2),
                "review": {
                    "manager_amount": float(review["manager_amount"] or 0),
                    "comment":        review["comment"] or "",
                    "created_by":     review["created_by"],
                    "updated_at":     _fmt_dt(review["updated_at"]),
                } if review else None,
            })

        return JSONResponse({
            "teams":   out,
            "year":    year,
            "month":   month,
            "year_m1": year - 1,
            "year_m2": year - 2,
        })
    except Exception:
        logger.exception("forecast_overview fejlede (year=%s, month=%s)", year, month)
        raise HTTPException(500, "Data kunne ikke hentes")


@router.post("/review/save")
async def review_save(request: Request, user=Depends(get_current_user)):
    require_manager(user)
    body    = await request.json()
    year    = body.get("year")
    month   = body.get("month")
    team    = str(body.get("team", "")).strip()
    amount  = body.get("manager_amount")
    comment = str(body.get("comment", "")).strip()[:1000]

    if not all([year, month, team]) or amount is None:
        raise HTTPException(400, "Ugyldige parametre")

    allowed = allowed_data_teams(user)
    if allowed is not None and team not in allowed:
        raise HTTPException(403, "Ingen adgang til dette team")

    try:
        db_review_save(year, month, team, float(amount), comment, user["name"])
        return JSONResponse({"status": "ok"})
    except Exception:
        logger.exception("review_save fejlede (year=%s, month=%s, team=%s)", year, month, team)
        raise HTTPException(500, "Vurderingen kunne ikke gemmes")


# ---------------------------------------------------------------------------
# Påmindelse: mangler sælgeren at udfylde næste måned?
# ---------------------------------------------------------------------------

@router.get("/reminder")
async def forecast_reminder(user=Depends(get_current_user)):
    """In-app notifikation til sælgere. Managere og højere roller laver ikke
    eget forecast og får derfor ingen påmindelse."""
    if not has_access(user, "salesperson") or _is_manager(user):
        return JSONResponse({"show": False})

    teams = user.get("_teams") or []
    today = date.today()
    if not teams or today.day < REMINDER_FROM_DAY:
        return JSONResponse({"show": False})

    nxt_year, nxt_month = _next_month(today)
    try:
        missing = db_missing_forecast_teams(user["name"], teams, nxt_year, nxt_month)
    except Exception:
        logger.warning("forecast_reminder fejlede (user=%s)", user.get("name"), exc_info=True)
        return JSONResponse({"show": False})

    return JSONResponse({
        "show":          bool(missing),
        "year":          nxt_year,
        "month":         nxt_month,
        "month_name":    MONTH_NAMES.get(nxt_month, str(nxt_month)),
        "missing_teams": missing,
    })

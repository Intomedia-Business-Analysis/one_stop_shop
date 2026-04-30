import traceback
from datetime import date, timedelta

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from auth import ROLE_LABELS, get_current_user, has_access
from moduler.modul_perf.queries import (
    SUBSCRIPTION_BRANDS, BRAND_GROUPS, BRAND_GROUP_LABELS, GROUPBY_COLUMNS,
    CANCELLATION_PIPELINES, DEAL_TYPE_ALIASES, DEAL_TYPE_CANONICAL, MONTH_NAMES_DA,
    resolve_brand_list, date_expr, shift_year_back, budget_range, build_where,
    db_get_filters, db_manager_data, db_afdelingsleder_data, db_saelger_data, db_saelger_meta,
)

router = APIRouter(prefix="/tools/performance", tags=["Performance"])
templates = Jinja2Templates(directory="templates")
templates.env.globals["ROLE_LABELS"] = ROLE_LABELS

@router.get("/filters")
async def perf_filters(user=Depends(get_current_user)):
    return JSONResponse(db_get_filters())

#----------------------------------------------------------------------------------------------------------------------
#                                        DET NYE DASHBOARD FOR MANAGER
#----------------------------------------------------------------------------------------------------------------------
@router.get("/manager", response_class=HTMLResponse)
async def perf_manager_page(request: Request, user=Depends(get_current_user)):
    if not has_access(user, "sales_manager"):
        raise HTTPException(403, "Kun Sales Managers og derover har adgang")
    today = date.today()
    return templates.TemplateResponse("perf_manager.html", {
        "request":       request,
        "user":          user,
        "current_year":  today.year,
        "current_month": today.month,
        "current_day":   today.day,
    })

@router.get("/manager-data")
async def perf_manager_data(
    team: str | None = None,
    year: int | None = None,
    month: int | None = None,
    date_col: str = "won_time",
    user=Depends(get_current_user)
):
    if not has_access(user, "sales_manager"):
        raise HTTPException(403, "Ingen adgang")
    try:
        return JSONResponse(db_manager_data(
            date.today(), team=team,
            selected_year=year, selected_month=month, date_col=date_col,
        ))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


#----------------------------------------------------------------------------------------------------------------------
#                                        DET NYE DASHBOARD FOR LEDELSE
#----------------------------------------------------------------------------------------------------------------------

@router.get("/afdelingsleder", response_class=HTMLResponse)
async def perf_afdelingsleder_page(request: Request, user=Depends(get_current_user)):
    if not has_access(user, "management"):
        raise HTTPException(403, "Kun Management og derover har adgang")
    today = date.today()
    return templates.TemplateResponse("perf_afdelingsleder.html", {
        "request":       request,
        "user":          user,
        "current_year":  today.year,
        "current_month": today.month,
        "current_day":   today.day,
    })

@router.get("/afdelingsleder-data")
async def perf_afdelingsleder_data(
    vis_alle: bool = False,
    user=Depends(get_current_user)
):
    if not has_access(user, "management"):
        raise HTTPException(403, "Ingen adgang")
    try:
        return JSONResponse(db_afdelingsleder_data(date.today(), vis_alle=vis_alle))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


#----------------------------------------------------------------------------------------------------------------------
#                                        DET NYE DASHBOARD FOR SÆLGER
#----------------------------------------------------------------------------------------------------------------------

@router.get("/saelger", response_class=HTMLResponse)
async def perf_saelger_page(request: Request, user=Depends(get_current_user)):
    return templates.TemplateResponse("perf_saelger.html", {
        "request": request,
        "user":    user,
    })

@router.get("/saelger-meta")
async def perf_saelger_meta(user=Depends(get_current_user)):
    try:
        return JSONResponse(db_saelger_meta(user["name"]))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))

@router.get("/saelger-data")
async def perf_saelger_data(
    team: str | None = None,
    year: int | None = None,
    month: int | None = None,
    date_col: str = "won_time",
    user=Depends(get_current_user)
):
    try:
        return JSONResponse(db_saelger_data(
            date.today(), user["name"],
            team=team, selected_year=year, selected_month=month,
            date_col=date_col,
        ))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))

#-----------------------------------------------------------------------------------------------------------------------
#                                                  DASHBOARDS VÆLGER
#-----------------------------------------------------------------------------------------------------------------------

@router.get("/dashboards", response_class=HTMLResponse)
async def perf_dashboards_page(request: Request, user=Depends(get_current_user)):
    return templates.TemplateResponse("perf_dashboards.html", {
        "request":    request,
        "user":       user,
        "is_manager": has_access(user, "sales_manager"),
        "is_management": has_access(user, "management"),
    })
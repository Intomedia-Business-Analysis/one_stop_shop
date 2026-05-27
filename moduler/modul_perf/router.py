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
    db_get_filters, db_manager_data, db_yoy_data, db_afdelingsleder_data, db_saelger_data, db_saelger_meta,
    db_saelger_available_owners,
    db_manager_saelger_deals, db_manager_saelger_pipeline, db_manager_saelger_filters,
)

router = APIRouter(prefix="/tools/performance", tags=["Performance"])
templates = Jinja2Templates(directory="templates")
from nav_utils import register_nav_globals
register_nav_globals(templates)

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
    return templates.TemplateResponse(request, "perf_manager.html", {
        "user":          user,
        "current_year":  today.year,
        "current_month": today.month,
        "current_day":   today.day,
    })

@router.get("/manager-data")
async def perf_manager_data(
    team: str | None = None,
    year: int | None = None,
    month: str | None = None,
    date_col: str = "won_time",
    pipeline_filter: str | None = None,
    user=Depends(get_current_user)
):
    if not has_access(user, "sales_manager"):
        raise HTTPException(403, "Ingen adgang")
    try:
        return JSONResponse(db_manager_data(
            date.today(), team=team,
            selected_year=year, selected_month=month, date_col=date_col,
            pipeline_filter=pipeline_filter,
        ))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


#----------------------------------------------------------------------------------------------------------------------
#                                        YoY SAMMENLIGNINGSVÆRKTØJ
#----------------------------------------------------------------------------------------------------------------------
@router.get("/yoy", response_class=HTMLResponse)
async def perf_yoy_page(request: Request, user=Depends(get_current_user)):
    if not has_access(user, "sales_manager"):
        raise HTTPException(403, "Kun Sales Managers og derover har adgang")
    today = date.today()
    return templates.TemplateResponse(request, "yoy_tool.html", {
        "user":          user,
        "current_year":  today.year,
        "current_month": today.month,
    })

@router.get("/yoy-data")
async def perf_yoy_data(
    team:            str | None = None,
    year:            int | None = None,
    compare_year:    int | None = None,
    month:           str | None = None,
    date_col:        str = "won_time",
    pipeline_filter: str | None = None,
    user=Depends(get_current_user)
):
    if not has_access(user, "sales_manager"):
        raise HTTPException(403, "Ingen adgang")
    try:
        return JSONResponse(db_yoy_data(
            date.today(), team=team,
            selected_year=year, compare_year=compare_year,
            selected_month=month,
            date_col=date_col, pipeline_filter=pipeline_filter,
        ))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


#----------------------------------------------------------------------------------------------------------------------
#                                        DET NYE DASHBOARD FOR LEDELSE
#----------------------------------------------------------------------------------------------------------------------

@router.get("/manager-saelger", response_class=HTMLResponse)
async def perf_manager_saelger_page(request: Request, user=Depends(get_current_user)):
    if not has_access(user, "sales_manager"):
        raise HTTPException(403, "Kun Sales Managers og derover har adgang")
    return templates.TemplateResponse(request, "perf_manager_saelger.html", {"user": user})

@router.get("/manager-saelger-filters")
async def manager_saelger_filters_endpoint(
    owner_name: str,
    user=Depends(get_current_user)
):
    if not has_access(user, "sales_manager"):
        raise HTTPException(403, "Ingen adgang")
    try:
        return JSONResponse(db_manager_saelger_filters(owner_name))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@router.get("/manager-saelger-pipeline")
async def manager_saelger_pipeline(
    owner_name: str,
    year: int | None = None,
    month: int | None = None,
    site: str | None = None,
    pipeline_type: str | None = None,
    user=Depends(get_current_user)
):
    if not has_access(user, "sales_manager"):
        raise HTTPException(403, "Ingen adgang")
    try:
        return JSONResponse(db_manager_saelger_pipeline(owner_name, year, month, site, pipeline_type))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@router.get("/manager-saelger-deals")
async def manager_saelger_deals(
    owner_name: str,
    year: int,
    month: int,
    date_col: str = "won_time",
    site: str | None = None,
    pipeline_type: str | None = None,
    user=Depends(get_current_user)
):
    if not has_access(user, "sales_manager"):
        raise HTTPException(403, "Ingen adgang")
    try:
        return JSONResponse(db_manager_saelger_deals(owner_name, year, month, date_col, site, pipeline_type))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@router.get("/afdelingsleder", response_class=HTMLResponse)
async def perf_afdelingsleder_page(request: Request, user=Depends(get_current_user)):
    if not has_access(user, "management"):
        raise HTTPException(403, "Kun Management og derover har adgang")
    today = date.today()
    return templates.TemplateResponse(request, "perf_afdelingsleder.html", {
        "user":          user,
        "current_year":  today.year,
        "current_month": today.month,
        "current_day":   today.day,
    })

@router.get("/afdelingsleder-data")
async def perf_afdelingsleder_data(
    year:  int | None = None,
    month: int | None = None,
    user=Depends(get_current_user)
):
    if not has_access(user, "management"):
        raise HTTPException(403, "Ingen adgang")
    try:
        ref_year = year if year else date.today().year
        return JSONResponse(db_afdelingsleder_data(ref_year, month))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


#----------------------------------------------------------------------------------------------------------------------
#                                        DET NYE DASHBOARD FOR SÆLGER
#----------------------------------------------------------------------------------------------------------------------

def _resolve_saelger_owner(user: dict, requested_owner: str | None) -> str:
    """Bestem hvilken sælger der vises på saelger-dashboardet.

    Admin må vælge en anden sælger via ?owner=... Alle andre roller låses til
    deres egen brugerprofil. Senere kan sales_manager udvides til at vælge
    blandt egne teammedlemmer.
    """
    if requested_owner and has_access(user, "admin"):
        return requested_owner
    return user["name"]


@router.get("/saelger", response_class=HTMLResponse)
async def perf_saelger_page(request: Request, user=Depends(get_current_user)):
    return templates.TemplateResponse(request, "perf_saelger.html", {
        "user":    user,
    })

@router.get("/saelger-meta")
async def perf_saelger_meta(
    owner: str | None = None,
    user=Depends(get_current_user),
):
    try:
        target_owner    = _resolve_saelger_owner(user, owner)
        can_pick_seller = has_access(user, "admin")
        meta = db_saelger_meta(target_owner)
        meta["owner_name"]      = target_owner
        meta["can_pick_seller"] = can_pick_seller
        if can_pick_seller:
            meta["available_owners"] = db_saelger_available_owners()
        return JSONResponse(meta)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))

@router.get("/saelger-data")
async def perf_saelger_data(
    team: str | None = None,
    year: int | None = None,
    month: int | None = None,
    date_col: str = "won_time",
    owner: str | None = None,
    user=Depends(get_current_user)
):
    try:
        target_owner = _resolve_saelger_owner(user, owner)
        return JSONResponse(db_saelger_data(
            date.today(), target_owner,
            team=team, selected_year=year, selected_month=month,
            date_col=date_col,
        ))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@router.get("/saelger-pipeline-deals")
async def perf_saelger_pipeline_deals(
    year:  int | None = None,
    month: int | None = None,
    owner: str | None = None,
    user=Depends(get_current_user),
):
    """Detaljerede åbne pipeline-deals for sælgeren — bruges af 'Pipeline'-modalet
    på /tools/performance/saelger. Genbruger db_manager_saelger_pipeline så
    visningen matcher den sales managers ser pr. sælger."""
    try:
        target_owner = _resolve_saelger_owner(user, owner)
        return JSONResponse(db_manager_saelger_pipeline(target_owner, year, month))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))

#-----------------------------------------------------------------------------------------------------------------------
#                                                  DASHBOARDS VÆLGER
#-----------------------------------------------------------------------------------------------------------------------

@router.get("/dashboards", response_class=HTMLResponse)
async def perf_dashboards_page(request: Request, user=Depends(get_current_user)):
    return templates.TemplateResponse(request, "perf_dashboards.html", {
        "user":       user,
        "is_manager": has_access(user, "sales_manager"),
        "is_management": has_access(user, "management"),
    })
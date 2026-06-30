import logging
from datetime import date, timedelta

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from auth import ROLE_LABELS, allowed_data_teams, get_current_user, has_access
from moduler.modul_perf.queries import (
    SUBSCRIPTION_BRANDS, BRAND_GROUPS, BRAND_GROUP_LABELS, GROUPBY_COLUMNS,
    CANCELLATION_PIPELINES, DEAL_TYPE_ALIASES, DEAL_TYPE_CANONICAL, MONTH_NAMES_DA,
    resolve_brand_list, date_expr, shift_year_back, budget_range, build_where,
    db_get_filters, db_manager_data, db_yoy_data, db_saelger_data, db_saelger_meta,
    db_saelger_available_owners, db_saelger_conversion_deals,
    db_manager_saelger_deals, db_manager_saelger_pipeline, db_manager_saelger_filters,
    db_owner_in_teams,
)

from moduler.modul_perf.queries_afdelingsleder import db_brand_overblik, db_afdelingsleder_hero, db_afdelingsleder_churn, db_afdelingsleder_vaekst

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tools/performance", tags=["Performance"])
templates = Jinja2Templates(directory="templates")
from nav_utils import register_nav_globals
register_nav_globals(templates)

@router.get("/filters")
async def perf_filters(user=Depends(get_current_user)):
    return JSONResponse(db_get_filters())


# ---------------------------------------------------------------------------
# Team-dataadgang: admin kan begrænse hvilke teams en bruger ser data for
# (HubUserTeamAccess — sættes på admin-brugersiden). Ingen begrænsning = alt.
# ---------------------------------------------------------------------------

def _effective_team(user: dict, team: str | None) -> str | None:
    """Begræns team-filteret til brugerens tilladte teams.

    Uden begrænsning returneres parametret uændret. Med begrænsning skæres de
    anmodede teams ned til de tilladte — og intet valg ('Alle teams') bliver
    til alle brugerens tilladte teams i stedet for hele firmaet.
    """
    allowed = allowed_data_teams(user)
    if allowed is None:
        return team
    requested = [t.strip() for t in (team or "").split(",") if t.strip()]
    effective = [t for t in requested if t in allowed] or allowed
    return ",".join(effective)


def _filter_team_lists(user: dict, data: dict) -> dict:
    """Skjul ikke-tilladte teams i svaret (dropdown-liste + ugerapport)."""
    allowed = allowed_data_teams(user)
    if allowed is None:
        return data
    if isinstance(data.get("teams"), list):
        data["teams"] = [t for t in data["teams"] if t in allowed]
    if isinstance(data.get("week_teams"), list):
        data["week_teams"] = [w for w in data["week_teams"] if w.get("team") in allowed]
    return data


def _require_owner_access(user: dict, owner_name: str):
    """403 hvis brugeren er team-begrænset og sælgeren ikke er i et tilladt team."""
    allowed = allowed_data_teams(user)
    if allowed is None:
        return
    if not db_owner_in_teams(owner_name, allowed):
        raise HTTPException(403, "Ingen adgang til denne sælgers data")

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
        data = db_manager_data(
            date.today(), team=_effective_team(user, team),
            selected_year=year, selected_month=month, date_col=date_col,
            pipeline_filter=pipeline_filter,
        )
        return JSONResponse(_filter_team_lists(user, data))
    except Exception:
        logger.exception("manager-data fejlede (team=%s, year=%s, month=%s)", team, year, month)
        raise HTTPException(500, "Data kunne ikke hentes")


#----------------------------------------------------------------------------------------------------------------------
#                                        BRAND OVERBLIK-DATA (bruges af Afdelingsleder Dashboard)
#----------------------------------------------------------------------------------------------------------------------
@router.get("/brand-overblik-data")
async def brand_overblik_data(
    date_col: str = "won_time",
    ytd: int = 1,
    user=Depends(get_current_user)
):
    if not has_access(user, "sales_manager"):
        raise HTTPException(403, "Ingen adgang")
    try:
        return JSONResponse(db_brand_overblik(date.today(), date_col=date_col, ytd=bool(ytd)))
    except Exception:
        logger.exception("brand-overblik-data fejlede (date_col=%s, ytd=%s)", date_col, ytd)
        raise HTTPException(500, "Data kunne ikke hentes")


#----------------------------------------------------------------------------------------------------------------------
#                                        AFDELINGSLEDER HERO-DATA (blok 1: budget vs faktisk)
#----------------------------------------------------------------------------------------------------------------------
@router.get("/afdelingsleder-hero-data")
async def afdelingsleder_hero_data(user=Depends(get_current_user)):
    if not has_access(user, "sales_manager"):
        raise HTTPException(403, "Ingen adgang")
    try:
        return JSONResponse(db_afdelingsleder_hero(date.today()))
    except Exception:
        logger.exception("afdelingsleder-hero-data fejlede")
        raise HTTPException(500, "Data kunne ikke hentes")


#----------------------------------------------------------------------------------------------------------------------
#                                        AFDELINGSLEDER CHURN-DATA (blok 3: churn-rate + top-opsigelser)
#----------------------------------------------------------------------------------------------------------------------
@router.get("/afdelingsleder-churn-data")
async def afdelingsleder_churn_data(user=Depends(get_current_user)):
    if not has_access(user, "sales_manager"):
        raise HTTPException(403, "Ingen adgang")
    try:
        return JSONResponse(db_afdelingsleder_churn(date.today()))
    except Exception:
        logger.exception("afdelingsleder-churn-data fejlede")
        raise HTTPException(500, "Data kunne ikke hentes")


#----------------------------------------------------------------------------------------------------------------------
#                                        AFDELINGSLEDER VAEKST-DATA (blok 4: nye vs eksisterende kunder)
#----------------------------------------------------------------------------------------------------------------------
@router.get("/afdelingsleder-vaekst-data")
async def afdelingsleder_vaekst_data(user=Depends(get_current_user)):
    if not has_access(user, "sales_manager"):
        raise HTTPException(403, "Ingen adgang")
    try:
        return JSONResponse(db_afdelingsleder_vaekst(date.today()))
    except Exception:
        logger.exception("afdelingsleder-vaekst-data fejlede")
        raise HTTPException(500, "Data kunne ikke hentes")


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
        data = db_yoy_data(
            date.today(), team=_effective_team(user, team),
            selected_year=year, compare_year=compare_year,
            selected_month=month,
            date_col=date_col, pipeline_filter=pipeline_filter,
        )
        return JSONResponse(_filter_team_lists(user, data))
    except Exception:
        logger.exception("yoy-data fejlede (team=%s, year=%s, compare_year=%s, month=%s)", team, year, compare_year, month)
        raise HTTPException(500, "Data kunne ikke hentes")


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
    _require_owner_access(user, owner_name)
    try:
        return JSONResponse(db_manager_saelger_filters(owner_name))
    except Exception:
        logger.exception("manager-saelger-filters fejlede (owner=%s)", owner_name)
        raise HTTPException(500, "Data kunne ikke hentes")


@router.get("/manager-saelger-pipeline")
async def manager_saelger_pipeline(
    owner_name: str,
    year: int | None = None,
    month: str | None = None,
    site: str | None = None,
    pipeline_type: str | None = None,
    user=Depends(get_current_user)
):
    if not has_access(user, "sales_manager"):
        raise HTTPException(403, "Ingen adgang")
    _require_owner_access(user, owner_name)
    try:
        return JSONResponse(db_manager_saelger_pipeline(owner_name, year, month, site, pipeline_type))
    except Exception:
        logger.exception("manager-saelger-pipeline fejlede (owner=%s, year=%s, month=%s)", owner_name, year, month)
        raise HTTPException(500, "Data kunne ikke hentes")


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
    _require_owner_access(user, owner_name)
    try:
        return JSONResponse(db_manager_saelger_deals(owner_name, year, month, date_col, site, pipeline_type))
    except Exception:
        logger.exception("manager-saelger-deals fejlede (owner=%s, year=%s, month=%s)", owner_name, year, month)
        raise HTTPException(500, "Data kunne ikke hentes")


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


#----------------------------------------------------------------------------------------------------------------------
#                                        DET NYE DASHBOARD FOR SÆLGER
#----------------------------------------------------------------------------------------------------------------------

def _saelger_pickable_owners(user: dict) -> list | None:
    """Sælgere brugeren må vælge på sælger-dashboardet.

    - management og derover (inkl. admin): None (ubegrænset — hele den aktive
      HubUsers-liste). Management er ikke leder af noget team og skal alligevel
      kunne vælge en hvilken som helst sælger.
    - sales_manager: aktive medlemmer af de teams manageren er LEDER for
      (TeamMemberships.role='leader'), evt. snævret af HubUserTeamAccess.
      Manageren selv er altid med, så eget dashboard kan ses.
    - øvrige roller: kun dem selv.
    """
    if has_access(user, "management"):
        return None
    if has_access(user, "sales_manager"):
        from moduler.modul_saelger_portfolio.queries import (
            get_led_teams, get_team_member_owners)
        led_teams = get_led_teams(user["id"]) or user.get("_teams") or []
        allowed = allowed_data_teams(user)
        if allowed is not None:
            led_teams = [t for t in led_teams if t in allowed]
        owners = get_team_member_owners(led_teams)
        if user.get("name") and user["name"] not in owners:
            owners = sorted(owners + [user["name"]])
        return owners
    return [user["name"]]


def _resolve_saelger_owner(user: dict, requested_owner: str | None) -> str:
    """Bestem hvilken sælger der vises på saelger-dashboardet.

    Uden ?owner=... vises brugerens egen profil. Admin må vælge en hvilken som
    helst sælger; en sales_manager må vælge blandt medlemmerne af de teams,
    han/hun er leder for. Et ikke-tilladt valg falder tilbage til egen profil.
    """
    if not requested_owner:
        return user["name"]
    pickable = _saelger_pickable_owners(user)
    if pickable is None or requested_owner in pickable:
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
        target_owner = _resolve_saelger_owner(user, owner)
        pickable     = _saelger_pickable_owners(user)
        # Admin → hele listen; sales_manager → egne teammedlemmer; ellers kun en selv.
        owners = db_saelger_available_owners() if pickable is None else pickable
        meta = db_saelger_meta(target_owner)
        meta["owner_name"]       = target_owner
        meta["can_pick_seller"]  = len(owners) > 1
        meta["available_owners"] = owners if len(owners) > 1 else []
        return JSONResponse(meta)
    except Exception:
        logger.exception("saelger-meta fejlede (owner=%s)", owner)
        raise HTTPException(500, "Data kunne ikke hentes")

@router.get("/saelger-data")
async def perf_saelger_data(
    team: str | None = None,
    year: int | None = None,
    month: str | None = None,
    date_col: str = "won_time",
    owner: str | None = None,
    pipeline: str | None = None,
    user=Depends(get_current_user)
):
    try:
        target_owner = _resolve_saelger_owner(user, owner)
        return JSONResponse(db_saelger_data(
            date.today(), target_owner,
            team=team, selected_year=year, selected_month=month,
            date_col=date_col, pipeline_filter=pipeline,
        ))
    except Exception:
        logger.exception("saelger-data fejlede (owner=%s, team=%s, year=%s, month=%s)", owner, team, year, month)
        raise HTTPException(500, "Data kunne ikke hentes")


@router.get("/saelger-pipeline-deals")
async def perf_saelger_pipeline_deals(
    year:  int | None = None,
    month: str | None = None,
    owner: str | None = None,
    user=Depends(get_current_user),
):
    """Detaljerede åbne pipeline-deals for sælgeren — bruges af 'Pipeline'-modalet
    på /tools/performance/saelger. Genbruger db_manager_saelger_pipeline så
    visningen matcher den sales managers ser pr. sælger."""
    try:
        target_owner = _resolve_saelger_owner(user, owner)
        return JSONResponse(db_manager_saelger_pipeline(target_owner, year, month))
    except Exception:
        logger.exception("saelger-pipeline-deals fejlede (owner=%s, year=%s, month=%s)", owner, year, month)
        raise HTTPException(500, "Data kunne ikke hentes")

@router.get("/saelger-conversion-deals")
async def perf_saelger_conversion_deals(
    year:  int | None = None,
    month: str | None = None,
    team:  str | None = None,
    owner: str | None = None,
    user=Depends(get_current_user),
):
    """Won/lost-deals bag sælgerens konverteringsrate — bruges af
    'Konverteringsrate'-modalet på /tools/performance/saelger."""
    try:
        target_owner = _resolve_saelger_owner(user, owner)
        return JSONResponse(db_saelger_conversion_deals(target_owner, year, month, team))
    except Exception:
        logger.exception("saelger-conversion-deals fejlede (owner=%s, year=%s, month=%s)", owner, year, month)
        raise HTTPException(500, "Data kunne ikke hentes")


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
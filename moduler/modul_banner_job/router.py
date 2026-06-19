import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from auth import ROLE_LABELS, get_current_user, has_access
from moduler.modul_banner_job.queries import (
    db_owners, db_kpi_data, db_top_customers,
    db_salesperson_performance, db_customer_heatmap, db_customer_history,
    db_all_deals,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tools/banner-job", tags=["Banner & Job"])
templates = Jinja2Templates(directory="templates")
from nav_utils import register_nav_globals
register_nav_globals(templates)

VALID_PIPELINES = {"banner", "job"}
VALID_COUNTRIES = {"dk", "no"}


def _check_pipeline(pipeline: str):
    if pipeline not in VALID_PIPELINES:
        raise HTTPException(400, "Ugyldig pipeline — brug 'banner' eller 'job'")


def _check_country(country: str):
    if country not in VALID_COUNTRIES:
        raise HTTPException(400, "Ugyldigt land — brug 'dk' eller 'no'")


@router.get("/", response_class=HTMLResponse)
async def banner_job_page(request: Request, user=Depends(get_current_user)):
    if not has_access(user, "salesperson"):
        raise HTTPException(403, "Ingen adgang")
    return templates.TemplateResponse(request, "banner_job_dashboard.html", {
        "user":    user,
    })


@router.get("/owners")
async def banner_job_owners(pipeline: str = "banner", country: str = "dk", user=Depends(get_current_user)):
    _check_pipeline(pipeline)
    _check_country(country)
    try:
        return JSONResponse({"owners": db_owners(pipeline, country)})
    except Exception:
        logger.exception("banner_job_owners fejlede (pipeline=%s, country=%s)", pipeline, country)
        raise HTTPException(500, "Data kunne ikke hentes")


@router.get("/kpi-data")
async def banner_job_kpi(
    pipeline: str = "banner",
    year: int | None = None,
    month: str | None = None,
    owner: str | None = None,
    country: str = "dk",
    brand: str | None = None,
    user=Depends(get_current_user),
):
    _check_pipeline(pipeline)
    _check_country(country)
    try:
        return JSONResponse(db_kpi_data(pipeline, year, month, owner, country, brand))
    except Exception:
        logger.exception("banner_job_kpi fejlede (pipeline=%s, year=%s, month=%s, owner=%s, country=%s)",
                         pipeline, year, month, owner, country)
        raise HTTPException(500, "Data kunne ikke hentes")


@router.get("/top-customers")
async def banner_job_top_customers(
    pipeline: str = "banner",
    year: int | None = None,
    month: str | None = None,
    owner: str | None = None,
    country: str = "dk",
    brand: str | None = None,
    user=Depends(get_current_user),
):
    _check_pipeline(pipeline)
    _check_country(country)
    try:
        return JSONResponse({"rows": db_top_customers(pipeline, year, month, owner, country, brand)})
    except Exception:
        logger.exception("banner_job_top_customers fejlede (pipeline=%s, year=%s, month=%s, owner=%s, country=%s)",
                         pipeline, year, month, owner, country)
        raise HTTPException(500, "Data kunne ikke hentes")


@router.get("/salesperson-performance")
async def banner_job_salesperson(
    pipeline: str = "banner",
    year: int | None = None,
    month: str | None = None,
    country: str = "dk",
    brand: str | None = None,
    user=Depends(get_current_user),
):
    if not has_access(user, "sales_manager"):
        raise HTTPException(403, "Kun Sales Managers og derover har adgang")
    _check_pipeline(pipeline)
    _check_country(country)
    try:
        return JSONResponse({"rows": db_salesperson_performance(pipeline, year, month, country, brand)})
    except Exception:
        logger.exception("banner_job_salesperson fejlede (pipeline=%s, year=%s, month=%s, country=%s)",
                         pipeline, year, month, country)
        raise HTTPException(500, "Data kunne ikke hentes")


@router.get("/customer-heatmap")
async def banner_job_heatmap(
    pipeline: str = "banner",
    owner: str | None = None,
    country: str = "dk",
    brand: str | None = None,
    user=Depends(get_current_user),
):
    _check_pipeline(pipeline)
    _check_country(country)
    try:
        return JSONResponse({"rows": db_customer_heatmap(pipeline, owner, country, brand)})
    except Exception:
        logger.exception("banner_job_heatmap fejlede (pipeline=%s, owner=%s, country=%s)", pipeline, owner, country)
        raise HTTPException(500, "Data kunne ikke hentes")


@router.get("/kunde", response_class=HTMLResponse)
async def banner_job_kunde_page(
    request: Request,
    pipeline: str = "banner",
    org_id: str = "",
    country: str = "dk",
    user=Depends(get_current_user),
):
    if not has_access(user, "salesperson"):
        raise HTTPException(403, "Ingen adgang")
    _check_pipeline(pipeline)
    _check_country(country)
    if not org_id:
        raise HTTPException(400, "org_id påkrævet")
    return templates.TemplateResponse(request, "banner_job_kunde.html", {
        "user":     user,
        "pipeline": pipeline,
        "org_id":   org_id,
        "country":  country,
    })


@router.get("/all-deals")
async def banner_job_all_deals(
    pipeline: str = "banner",
    owner: str | None = None,
    country: str = "dk",
    user=Depends(get_current_user),
):
    if not has_access(user, "salesperson"):
        raise HTTPException(403, "Ingen adgang")
    _check_pipeline(pipeline)
    _check_country(country)
    try:
        return JSONResponse({"rows": db_all_deals(pipeline, owner, country)})
    except Exception:
        logger.exception("banner_job_all_deals fejlede (pipeline=%s, owner=%s, country=%s)", pipeline, owner, country)
        raise HTTPException(500, "Data kunne ikke hentes")


@router.get("/customer-history")
async def banner_job_customer_history(
    pipeline: str = "banner",
    org_id: str = "",
    country: str = "dk",
    user=Depends(get_current_user),
):
    _check_pipeline(pipeline)
    _check_country(country)
    if not org_id:
        raise HTTPException(400, "org_id påkrævet")
    try:
        return JSONResponse(db_customer_history(pipeline, org_id, country))
    except Exception:
        logger.exception("banner_job_customer_history fejlede (pipeline=%s, org_id=%s, country=%s)",
                         pipeline, org_id, country)
        raise HTTPException(500, "Data kunne ikke hentes")

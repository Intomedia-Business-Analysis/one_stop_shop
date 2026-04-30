import traceback

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from auth import ROLE_LABELS, get_current_user, has_access
from moduler.modul_banner_job.queries import (
    db_owners, db_kpi_data, db_top_customers,
    db_salesperson_performance, db_customer_heatmap, db_customer_history,
)

router = APIRouter(prefix="/tools/banner-job", tags=["Banner & Job"])
templates = Jinja2Templates(directory="templates")
templates.env.globals["ROLE_LABELS"] = ROLE_LABELS

VALID_PIPELINES = {"banner", "job"}


def _check_pipeline(pipeline: str):
    if pipeline not in VALID_PIPELINES:
        raise HTTPException(400, "Ugyldig pipeline — brug 'banner' eller 'job'")


@router.get("/", response_class=HTMLResponse)
async def banner_job_page(request: Request, user=Depends(get_current_user)):
    if not has_access(user, "salesperson"):
        raise HTTPException(403, "Ingen adgang")
    return templates.TemplateResponse("banner_job_dashboard.html", {
        "request": request,
        "user":    user,
    })


@router.get("/owners")
async def banner_job_owners(pipeline: str = "banner", user=Depends(get_current_user)):
    _check_pipeline(pipeline)
    try:
        return JSONResponse({"owners": db_owners(pipeline)})
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@router.get("/kpi-data")
async def banner_job_kpi(
    pipeline: str = "banner",
    year: int | None = None,
    month: str | None = None,
    owner: str | None = None,
    user=Depends(get_current_user),
):
    _check_pipeline(pipeline)
    try:
        return JSONResponse(db_kpi_data(pipeline, year, month, owner))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@router.get("/top-customers")
async def banner_job_top_customers(
    pipeline: str = "banner",
    year: int | None = None,
    month: str | None = None,
    owner: str | None = None,
    user=Depends(get_current_user),
):
    _check_pipeline(pipeline)
    try:
        return JSONResponse({"rows": db_top_customers(pipeline, year, month, owner)})
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@router.get("/salesperson-performance")
async def banner_job_salesperson(
    pipeline: str = "banner",
    year: int | None = None,
    month: str | None = None,
    user=Depends(get_current_user),
):
    if not has_access(user, "sales_manager"):
        raise HTTPException(403, "Kun Sales Managers og derover har adgang")
    _check_pipeline(pipeline)
    try:
        return JSONResponse({"rows": db_salesperson_performance(pipeline, year, month)})
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@router.get("/customer-heatmap")
async def banner_job_heatmap(
    pipeline: str = "banner",
    owner: str | None = None,
    user=Depends(get_current_user),
):
    _check_pipeline(pipeline)
    try:
        return JSONResponse({"rows": db_customer_heatmap(pipeline, owner)})
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@router.get("/kunde", response_class=HTMLResponse)
async def banner_job_kunde_page(
    request: Request,
    pipeline: str = "banner",
    org_id: str = "",
    user=Depends(get_current_user),
):
    if not has_access(user, "salesperson"):
        raise HTTPException(403, "Ingen adgang")
    _check_pipeline(pipeline)
    if not org_id:
        raise HTTPException(400, "org_id påkrævet")
    return templates.TemplateResponse("banner_job_kunde.html", {
        "request":  request,
        "user":     user,
        "pipeline": pipeline,
        "org_id":   org_id,
    })


@router.get("/customer-history")
async def banner_job_customer_history(
    pipeline: str = "banner",
    org_id: str = "",
    user=Depends(get_current_user),
):
    _check_pipeline(pipeline)
    if not org_id:
        raise HTTPException(400, "org_id påkrævet")
    try:
        return JSONResponse(db_customer_history(pipeline, org_id))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))

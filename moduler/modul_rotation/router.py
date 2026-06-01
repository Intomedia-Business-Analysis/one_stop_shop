from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from auth import get_current_user, has_access, RequiresLoginException

from .queries import (
    db_sales_performance,
    db_department_performance,
    db_banner_performance,
    db_job_performance,
    db_media_performance,
)

router = APIRouter()
templates = Jinja2Templates(directory="templates")


def _require(user, min_role: str = "salesperson"):
    if not user:
        raise RequiresLoginException()
    if not has_access(user, min_role):
        raise RequiresLoginException()
    return user


# ════════════════════════════════════════════════════════════════════════════
#  AUTO-ROTATION — alle dashboards i rotation
# ════════════════════════════════════════════════════════════════════════════

@router.get("/tools/rotation/", response_class=HTMLResponse)
async def rotation_autoplay(request: Request, user=Depends(get_current_user)):
    _require(user, "salesperson")
    return templates.TemplateResponse("rotation_autoplay.html", {"request": request, "user": user})


# ════════════════════════════════════════════════════════════════════════════
#  DASHBOARD 1 — Sales Performance
# ════════════════════════════════════════════════════════════════════════════

@router.get("/tools/rotation/sales-performance", response_class=HTMLResponse)
async def sales_performance_page(request: Request, user=Depends(get_current_user)):
    _require(user, "salesperson")
    return templates.TemplateResponse("rotation_sales_performance.html", {"request": request, "user": user})


@router.get("/tools/rotation/sales-performance-data")
async def sales_performance_data(request: Request, user=Depends(get_current_user)):
    _require(user, "salesperson")
    data = db_sales_performance(date.today())
    return JSONResponse(content=data)


# ════════════════════════════════════════════════════════════════════════════
#  DASHBOARD 2 — Department Performance
# ════════════════════════════════════════════════════════════════════════════

@router.get("/tools/rotation/department-performance", response_class=HTMLResponse)
async def dept_performance_page(request: Request, user=Depends(get_current_user)):
    _require(user, "salesperson")
    return templates.TemplateResponse("rotation_dept_performance.html", {"request": request, "user": user})


@router.get("/tools/rotation/department-performance-data")
async def dept_performance_data(request: Request, user=Depends(get_current_user)):
    _require(user, "salesperson")
    data = db_department_performance(date.today())
    return JSONResponse(content=data)


# ════════════════════════════════════════════════════════════════════════════
#  DASHBOARD 3 — Banner Performance
# ════════════════════════════════════════════════════════════════════════════

@router.get("/tools/rotation/banner-performance", response_class=HTMLResponse)
async def banner_performance_page(request: Request, user=Depends(get_current_user)):
    _require(user, "salesperson")
    return templates.TemplateResponse("rotation_banner_performance.html", {"request": request, "user": user})


@router.get("/tools/rotation/banner-performance-data")
async def banner_performance_data(request: Request, user=Depends(get_current_user)):
    _require(user, "salesperson")
    data = db_banner_performance(date.today())
    return JSONResponse(content=data)


# ════════════════════════════════════════════════════════════════════════════
#  DASHBOARD 4 — Job Performance
# ════════════════════════════════════════════════════════════════════════════

@router.get("/tools/rotation/job-performance", response_class=HTMLResponse)
async def job_performance_page(request: Request, user=Depends(get_current_user)):
    _require(user, "salesperson")
    return templates.TemplateResponse("rotation_job_performance.html", {"request": request, "user": user})


@router.get("/tools/rotation/job-performance-data")
async def job_performance_data(request: Request, user=Depends(get_current_user)):
    _require(user, "salesperson")
    data = db_job_performance(date.today())
    return JSONResponse(content=data)


# ════════════════════════════════════════════════════════════════════════════
#  DASHBOARD 5 — Media Performance
# ════════════════════════════════════════════════════════════════════════════

@router.get("/tools/rotation/media-performance", response_class=HTMLResponse)
async def media_performance_page(request: Request, user=Depends(get_current_user)):
    _require(user, "salesperson")
    return templates.TemplateResponse("rotation_media_performance.html", {"request": request, "user": user})


@router.get("/tools/rotation/media-performance-data")
async def media_performance_data(
    request: Request,
    user=Depends(get_current_user),
    brands: Optional[str] = None,
    years: Optional[str] = None,
    mode: Optional[str] = None,
):
    _require(user, "salesperson")
    selected_brands = [b.strip() for b in brands.split(",")] if brands else None
    selected_years  = [y.strip() for y in years.split(",")]  if years  else None
    data = db_media_performance(selected_brands, selected_years, mode or "abonnement")
    return JSONResponse(content=data)

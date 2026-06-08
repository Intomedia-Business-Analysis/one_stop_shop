import json
import os
import uuid
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from auth import get_current_user, resolve_resource_access, RequiresLoginException, ROLE_RANK

# ════════════════════════════════════════════════════════════════════════════
#  SCREEN CONFIG — hjælpefunktioner
# ════════════════════════════════════════════════════════════════════════════

SCREEN_CONFIGS_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "screen_configs.json")

def _load_configs() -> dict:
    if not os.path.exists(SCREEN_CONFIGS_PATH):
        return {"screens": []}
    with open(SCREEN_CONFIGS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def _save_configs(data: dict):
    with open(SCREEN_CONFIGS_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

from .queries import (
    db_sales_performance,
    db_department_performance,
    db_banner_performance,
    db_job_performance,
    db_media_performance,
)

from nav_utils import register_nav_globals

router = APIRouter()
templates = Jinja2Templates(directory="templates")
register_nav_globals(templates)


def _require(user, min_role: str = "salesperson", resource_id: str = "rotation"):
    """Adgangstjek for rotations-ruterne.

    Bruger ressource-baseret access (resource_id='rotation') i stedet for rent
    rang-tjek, så lav-rang 'screen'-brugere kan få adgang til netop rotationen
    via en RoleResourceAccess-override — uden at få adgang til resten af hubben.
    Normale roller (salesperson og opefter) falder igennem til rang-tjekket og
    bevarer uændret adgang.
    """
    if not user:
        raise RequiresLoginException()
    if resolve_resource_access(user, resource_id, min_role) == "none":
        raise RequiresLoginException()
    return user


# ════════════════════════════════════════════════════════════════════════════
#  AUTO-ROTATION — alle dashboards i rotation
# ════════════════════════════════════════════════════════════════════════════

@router.get("/tools/rotation/", response_class=HTMLResponse)
async def rotation_autoplay(request: Request, user=Depends(get_current_user)):
    _require(user, "salesperson")
    user_rank = ROLE_RANK.get(user["role"], 0) if user else 0
    return templates.TemplateResponse(request, "rotation_autoplay.html", {"user": user, "user_rank": user_rank})


# ════════════════════════════════════════════════════════════════════════════
#  DASHBOARD 1 — Sales Performance
# ════════════════════════════════════════════════════════════════════════════

@router.get("/tools/rotation/sales-performance", response_class=HTMLResponse)
async def sales_performance_page(request: Request, user=Depends(get_current_user)):
    _require(user, "salesperson")
    return templates.TemplateResponse(request, "rotation_sales_performance.html", {"user": user})


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
    return templates.TemplateResponse(request, "rotation_dept_performance.html", {"user": user})


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
    return templates.TemplateResponse(request, "rotation_banner_performance.html", {"user": user})


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
    return templates.TemplateResponse(request, "rotation_job_performance.html", {"user": user})


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
    return templates.TemplateResponse(request, "rotation_media_performance.html", {"user": user})


@router.get("/tools/rotation/media-performance-data")
async def media_performance_data(
    request: Request,
    user=Depends(get_current_user),
    accounts: Optional[str] = None,
    years: Optional[str] = None,
    mode: Optional[str] = None,
    months: Optional[str] = None,
):
    _require(user, "salesperson")
    selected_accounts = [a.strip() for a in accounts.split(",")] if accounts else None
    selected_years    = [y.strip() for y in years.split(",")]    if years    else None
    selected_months   = [m.strip() for m in months.split(",")]   if months   else None
    data = db_media_performance(selected_accounts, selected_years,
                                mode or "abonnement", selected_months)
    return JSONResponse(content=data)


# ════════════════════════════════════════════════════════════════════════════
#  SCREEN CONFIG — admin-side
# ════════════════════════════════════════════════════════════════════════════

@router.get("/tools/rotation/screens", response_class=HTMLResponse)
async def screens_admin(request: Request, user=Depends(get_current_user)):
    _require(user, "sales_manager")
    configs = _load_configs()
    return templates.TemplateResponse(request, "rotation_screens.html", {
        "user": user,
        "screens": configs["screens"],
    })


@router.post("/tools/rotation/screens/save")
async def screens_save(request: Request, user=Depends(get_current_user)):
    _require(user, "sales_manager")
    body = await request.json()
    configs = _load_configs()

    screen_id = body.get("id")
    media = {
        "mode":     body.get("media_mode", ""),
        "accounts": body.get("media_accounts", ""),
        "years":    body.get("media_years", ""),
        "months":   body.get("media_months", ""),
    }
    if screen_id:
        # Opdater eksisterende
        for s in configs["screens"]:
            if s["id"] == screen_id:
                s["name"] = body["name"]
                s["dashboards"] = body["dashboards"]
                s["interval"] = body["interval"]
                s["media"] = media
                break
    else:
        # Opret ny
        configs["screens"].append({
            "id": str(uuid.uuid4())[:8],
            "name": body["name"],
            "dashboards": body["dashboards"],
            "interval": body["interval"],
            "media": media,
        })

    _save_configs(configs)
    return JSONResponse({"ok": True})


@router.post("/tools/rotation/screens/delete")
async def screens_delete(request: Request, user=Depends(get_current_user)):
    _require(user, "sales_manager")
    body = await request.json()
    configs = _load_configs()
    configs["screens"] = [s for s in configs["screens"] if s["id"] != body["id"]]
    _save_configs(configs)
    return JSONResponse({"ok": True})


@router.get("/tools/rotation/screen/{screen_id}", response_class=HTMLResponse)
async def screen_player(screen_id: str, request: Request, user=Depends(get_current_user)):
    _require(user, "salesperson")
    configs = _load_configs()
    screen = next((s for s in configs["screens"] if s["id"] == screen_id), None)
    if not screen:
        return HTMLResponse("Skærm ikke fundet", status_code=404)
    dashboards = ",".join(screen["dashboards"])
    interval = screen["interval"]
    url = f"/tools/rotation/?dashboards={dashboards}&interval={interval}"
    media = screen.get("media", {})
    if media.get("mode"):
        url += f"&mode={media['mode']}"
    if media.get("accounts"):
        url += f"&accounts={media['accounts']}"
    if media.get("years"):
        url += f"&years={media['years']}"
    if media.get("months"):
        url += f"&months={media['months']}"
    return RedirectResponse(url)

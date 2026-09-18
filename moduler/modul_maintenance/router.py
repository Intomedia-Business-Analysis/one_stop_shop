"""Maintenance — routes.

Adgang: 'sales_operations' (rang 3) og derover. Kravet står både her og på
nav-item'et i nav_utils.py — sættes det lavere i nav'en end her, får brugeren et
menupunkt, der svarer 403.

Bemærk at rollerne er rangordnede (se _DEFAULT_ROLES i auth.py): marketing
(rang 4) og management (rang 5) ligger OVER sales_operations og kommer derfor
med. Det er med vilje — siden siger kun, hvor friske data er, ikke hvad de
indeholder.
"""
import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from auth import get_current_user, has_access
from moduler.modul_maintenance.queries import (
    db_maintenance_json,
    db_maintenance_overblik,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tools/maintenance", tags=["Maintenance"])
templates = Jinja2Templates(directory="templates")
from nav_utils import register_nav_globals  # noqa: E402
register_nav_globals(templates)

MIN_ROLLE = "sales_operations"


def _require_access(user: dict) -> None:
    if not has_access(user, MIN_ROLLE):
        raise HTTPException(403, "Kræver Sales Operations-adgang")


@router.get("/", response_class=HTMLResponse)
async def maintenance_side(request: Request, user=Depends(get_current_user)):
    _require_access(user)
    # db_maintenance_overblik() kaster ikke — en utilgængelig database bliver
    # til statussen 'ukendt' på hver række. Siden skal kunne vises netop når
    # noget er galt.
    return templates.TemplateResponse(request, "maintenance_dashboard.html", {
        "user": user,
        "data": db_maintenance_overblik(),
    })


@router.get("/data")
async def maintenance_data(user=Depends(get_current_user)):
    """Samme overblik som JSON — bruges af sidens auto-opdatering."""
    _require_access(user)
    try:
        return JSONResponse(db_maintenance_json())
    except Exception:
        logger.exception("maintenance_data fejlede")
        raise HTTPException(500, "Data kunne ikke hentes")

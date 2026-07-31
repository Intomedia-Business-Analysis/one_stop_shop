from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from auth import get_current_user
from nav_utils import register_nav_globals
from .queries import db_monthly_active_counts

templates = Jinja2Templates(directory="templates")
register_nav_globals(templates)

router = APIRouter()


@router.get("/retention/ping")
def ping():
    return {"message": "pong"}


@router.get("/retention/monthly_active_counts")
def get_monthly_active_counts():
    return db_monthly_active_counts()


@router.get("/retention/overview", response_class=HTMLResponse)
async def retention_overview(request: Request, user=Depends(get_current_user)):
    return templates.TemplateResponse(request, "retention_overview.html", {"user": user})
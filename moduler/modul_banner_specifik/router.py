from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from auth import ROLE_LABELS, get_current_user

router = APIRouter(prefix="/tools/banner", tags=["Banner Specifik"])
templates = Jinja2Templates(directory="templates")
templates.env.globals["ROLE_LABELS"] = ROLE_LABELS


@router.get("/", response_class=HTMLResponse)
async def banner_specifik_page(request: Request, user=Depends(get_current_user)):
    return templates.TemplateResponse("banner_specifik.html", {
        "request": request,
        "user":    user,
    })

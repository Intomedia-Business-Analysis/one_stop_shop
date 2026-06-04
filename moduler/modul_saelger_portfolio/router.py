from datetime import date

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from auth import get_current_user, has_access
from nav_utils import register_nav_globals
from moduler.modul_saelger_portfolio.queries import get_available_owners, get_kundeliste

router = APIRouter(prefix="/tools/saelger-portfolio", tags=["Sælger Portefølje"])
templates = Jinja2Templates(directory="templates")
register_nav_globals(templates)


@router.get("/", response_class=HTMLResponse)
async def saelger_portfolio(
    request: Request,
    user=Depends(get_current_user),
    owner: str = None,
):
    is_admin = has_access(user, "admin")

    # Admins kan vælge en anden sælger via ?owner=Navn
    if is_admin and owner:
        target_owner = owner
    else:
        target_owner = user["name"]

    kunder = get_kundeliste(target_owner)
    available_owners = get_available_owners() if is_admin else []

    return templates.TemplateResponse(
        request=request,
        name="saelger_portfolio.html",
        context={
            "dato": date.today().strftime("%d-%m-%Y"),
            "user": user,
            "kunder": kunder,
            "target_owner": target_owner,
            "is_admin": is_admin,
            "available_owners": available_owners,
        }
    )

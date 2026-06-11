from datetime import date

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from auth import allowed_data_teams, get_current_user, has_access
from nav_utils import register_nav_globals
from moduler.modul_saelger_portfolio.queries import (
    get_available_owners,
    get_growth_timeline,
    get_kundeliste,
)

router = APIRouter(prefix="/tools/saelger-portfolio", tags=["Sælger Portefølje"])
templates = Jinja2Templates(directory="templates")
register_nav_globals(templates)


@router.get("/", response_class=HTMLResponse)
async def saelger_portfolio(
    request: Request,
    user=Depends(get_current_user),
    owner: str = None,
):
    is_manager = has_access(user, "sales_manager")
    teams = allowed_data_teams(user)  # None = ubegrænset

    # Managers (og derover) kan vælge en anden sælger via ?owner=Navn —
    # men kun blandt sælgere i deres tilladte teams (HubUserTeamAccess).
    available_owners = get_available_owners(teams) if is_manager else []
    if is_manager and owner and owner in available_owners:
        target_owner = owner
    else:
        target_owner = user["name"]

    kunder = get_kundeliste(target_owner)

    # Porteføljevækst for den viste sælger (vundne deals på eksisterende kunder)
    growth_timeline = get_growth_timeline(target_owner)

    return templates.TemplateResponse(
        request=request,
        name="saelger_portfolio.html",
        context={
            "dato": date.today().strftime("%d-%m-%Y"),
            "user": user,
            "kunder": kunder,
            "target_owner": target_owner,
            "is_manager": is_manager,
            "available_owners": available_owners,
            "growth_timeline": growth_timeline,
            "current_year": date.today().year,
        }
    )

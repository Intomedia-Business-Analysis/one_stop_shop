import logging
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from auth import allowed_data_teams, get_current_user, has_access
from nav_utils import register_nav_globals
from moduler.modul_saelger_portfolio.queries import (
    UNASSIGNED_OWNER,
    get_available_owners,
    get_customer_portfolio,
    get_kundeliste,
    get_led_teams,
    get_org_owners,
    get_team_member_owners,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tools/saelger-portfolio", tags=["Sælger Portefølje"])
templates = Jinja2Templates(directory="templates")
register_nav_globals(templates)


def _resolve_available_owners(user: dict) -> list:
    """Hvem må brugeren se porteføljer for (ud over sig selv)?

    - sales_manager (men under sales_operations): KUN sælgere fra de teams
      manageren er LEDER for (TeamMemberships.role='leader') — Watch DK-
      lederen må fx ikke se Monitor-sælgere. Fallback til egne medlemskaber
      hvis lederrollen ikke er registreret. HubUserTeamAccess snævrer
      yderligere ind, hvis sat.
    - sales_operations og derover: alle aktive brugere (evt. begrænset af
      HubUserTeamAccess som hidtil).
    - admin: desuden de "skjulte" bøger — kunder uden ejer i Pipedrive og
      System Admin-kontoens kunder.
    """
    teams = allowed_data_teams(user)  # None = ubegrænset

    if not has_access(user, "sales_manager"):
        available_owners = []
    elif not has_access(user, "sales_operations"):
        led_teams = get_led_teams(user["id"]) or user.get("_teams") or []
        if teams is not None:
            led_teams = [t for t in led_teams if t in teams]
        available_owners = get_team_member_owners(led_teams)
    else:
        available_owners = get_available_owners(teams)

    if user.get("role") == "admin":
        available_owners = available_owners + ["System Admin", UNASSIGNED_OWNER]
    return available_owners


def _require_org_access(user: dict, org_id: str) -> None:
    """Kunde-historik må kun ses, hvis kunden ligger i en portefølje,
    brugeren har adgang til (egen, eller en fra sælger-dropdownen)."""
    owners = get_org_owners(org_id)
    if not owners:
        raise HTTPException(404, "Kunde ikke fundet")
    if user["name"] in {o for o in owners if o}:
        return
    available = set(_resolve_available_owners(user))
    for o in owners:
        if (o if o is not None else UNASSIGNED_OWNER) in available:
            return
    raise HTTPException(403, "Ingen adgang til denne kundes historik")


@router.get("/", response_class=HTMLResponse)
async def saelger_portfolio(
    request: Request,
    user=Depends(get_current_user),
    owner: str = None,
):
    is_manager = has_access(user, "sales_manager")

    # Managers (og derover) kan vælge en anden sælger via ?owner=Navn —
    # reglerne for hvem ligger i _resolve_available_owners.
    available_owners = _resolve_available_owners(user)
    if is_manager and owner and owner in available_owners:
        target_owner = owner
    else:
        target_owner = user["name"]

    kunder = get_kundeliste(target_owner)

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
        }
    )


@router.get("/kunde", response_class=HTMLResponse)
async def saelger_portfolio_kunde(
    request: Request,
    org_id: str = "",
    owner: str = "",
    user=Depends(get_current_user),
):
    if not org_id:
        raise HTTPException(400, "org_id påkrævet")
    _require_org_access(user, org_id)
    return templates.TemplateResponse(request, "saelger_portfolio_kunde.html", {
        "user":       user,
        "org_id":     org_id,
        # Tilbage-linket skal lande på den portefølje, man kom fra
        "back_owner": owner or "",
    })


@router.get("/customer-portfolio")
async def saelger_portfolio_customer_portfolio(
    org_id: str = "",
    owner: str = "",
    user=Depends(get_current_user),
):
    if not org_id:
        raise HTTPException(400, "org_id påkrævet")
    _require_org_access(user, org_id)
    try:
        return JSONResponse(get_customer_portfolio(org_id, owner))
    except Exception:
        logger.exception("saelger_portfolio_customer_portfolio fejlede (org_id=%s)", org_id)
        raise HTTPException(500, "Data kunne ikke hentes")

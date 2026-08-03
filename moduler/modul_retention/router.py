from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from auth import allowed_data_teams, get_current_user, has_access
from nav_utils import register_nav_globals
from moduler.modul_saelger_portfolio.queries import get_led_teams
from .queries import db_monthly_active_counts

templates = Jinja2Templates(directory="templates")
register_nav_globals(templates)

router = APIRouter()


def _resolve_filters(user: dict) -> tuple[str | None, list | None]:
    """Oversæt brugerens rolle til (owner_name, team) for retention-queryen.
    - screen: ingen adgang - retention er ikke en del af skærmrotation.
    - salesperson; kun egen bog.
    - sales_manager:de teams brugern er LEDER for. Falder tilbage til egne
    medlemskber hvis lederollen ikke er registreret i TeamMemberships
    (Samme løsning som sælger_portfolio). Uden nogen af dem: 403 fren for
    tavst at vise firmatotalen - et uafgrænset tal ser ud som et team-tal.
    - Seles-operations og derover (marketing, management, admin): firmabredt,
    inkl. de 16% kunder unden tilskrevet ejer i PipeDrive.
    """
    if not has_access(user, "salesperson"):
        raise HTTPException(403, "Ingen adgang til retention")

    allowed = allowed_data_teams(user)  # None = ubegrænset

    if has_access(user, "sales_operations"):
        # Ubegrænset → (None, None) = firmabredt. Er brugeren derimod
        # HubUserTeamAccess-begrænset, bliver "hele firmaet" de tilladte teams
        # — samme regel som _effective_team i perf-modulet.
        return None, allowed
    if has_access(user, "sales_manager"):
        teams = get_led_teams(user["id"]) or user.get("_teams") or []
        if not teams:
            raise HTTPException(
                403, "Du er registreret som Sales Manager, men har ingen team-medlemskaber. "
                "Kontakt en systemadministrator for at få det rettet, hvis du mener det er en fejl."
            )
        if allowed is not None:
            teams = [t for t in teams if t in allowed]
            if not teams:
                raise HTTPException(
                    403, "Din data-adgang er begrænset til teams, du ikke er leder for. "
                    "Kontakt en systemadministrator."
                )
        return None, teams
    return user["name"], None


@router.get("/retention/monthly_active_counts")
def get_monthly_active_counts(user=Depends(get_current_user)):
    owner_name, teams = _resolve_filters(user)
    return db_monthly_active_counts(owner_name=owner_name, teams=teams)


@router.get("/retention/overview", response_class=HTMLResponse)
async def retention_overview(request: Request, user=Depends(get_current_user)):
    # Selve dataene hentes client-side og er beskyttet af _resolve_filters, men
    # skallen skal heller ikke kunne åbnes — en screen-bruger har intet at gøre
    # på siden, og et tomt panel med en fejlbesked er en dårlig afvisning.
    if not has_access(user, "salesperson"):
        raise HTTPException(403, "Ingen adgang til retention")
    return templates.TemplateResponse(request, "retention_overview.html", {"user": user})
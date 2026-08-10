from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from auth import allowed_data_teams, get_current_user, has_access
from nav_utils import register_nav_globals
from moduler.modul_saelger_portfolio.queries import get_led_teams
from .queries import db_monthly_active_counts
from .risiko import abonnementer_i_risiko

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


@router.get("/retention/risk")
def get_abonnementer_i_risiko(user=Depends(get_current_user)):
    """Risikolisten pr. ABONNEMENT. Samme rolle-filtrering som trendlinjen.

    Skiftet fra recency-modellen (risk.customers_at_risk) 2026-08-10, jf. PRD §3
    hvor 14/30-dages-tærsklerne udfases: signalet rådnede med filens alder, og
    grainen var kunden, hvilket er den forkerte måleenhed (PRD §2 og §7.2).
    Ruten beholder sit navn, så eksisterende links og bogmærker virker.

    NB: `_resolve_filters` returnerer `owner_name` fra `retention_owner`-verdenen
    (`user["name"]`), mens risikolisten filtrerer på ACV's org-ejer. Det er samme
    personnavn i begge kilder — HubUsers' `name` — så filteret virker; men de to
    visninger kan medtage forskellige kunder for samme sælger, fordi kilderne er
    uenige om ejerskab i 47% af rækkerne. Det skal fremgå af siden.

    `abo_maaned` sendes bevidst IKKE videre: produktionsvisningen skal altid være
    indeværende måned, og parameteren findes kun til kontrolkørsler.
    """
    owner_name, teams = _resolve_filters(user)
    return abonnementer_i_risiko(owner_name=owner_name, teams=teams)


@router.get("/retention/overview", response_class=HTMLResponse)
async def retention_overview(request: Request, user=Depends(get_current_user)):
    # Selve dataene hentes client-side og er beskyttet af _resolve_filters, men
    # skallen skal heller ikke kunne åbnes — en screen-bruger har intet at gøre
    # på siden, og et tomt panel med en fejlbesked er en dårlig afvisning.
    if not has_access(user, "salesperson"):
        raise HTTPException(403, "Ingen adgang til retention")
    return templates.TemplateResponse(request, "retention_overview.html", {"user": user})


@router.get("/retention/risk_overview", response_class=HTMLResponse)
async def retention_risk_overview(request: Request, user=Depends(get_current_user)):
    # Samme adgangsvagt som de øvrige retention-sider: dataene er beskyttet af
    # _resolve_filters, men skallen skal heller ikke kunne åbnes af en screen-bruger.
    if not has_access(user, "salesperson"):
        raise HTTPException(403, "Ingen adgang til retention")
    return templates.TemplateResponse(request, "retention_risk.html", {"user": user})
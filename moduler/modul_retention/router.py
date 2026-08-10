from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from auth import allowed_data_teams, get_current_user, resolve_resource_access
from nav_utils import register_nav_globals
from .queries import db_monthly_active_counts
from .risiko import abonnementer_i_risiko

templates = Jinja2Templates(directory="templates")
register_nav_globals(templates)

router = APIRouter()


MIN_ROLLE = "sales_operations"

# Marketing (rang 4) og management (rang 5) rangerer HØJERE end sales_operations
# (rang 3) og ville ellers slippe ind gennem rangen alene. Besluttet 2026-08-10:
# retention er præcis Sales Operations + admin. admin bypasser altid
# exclude_roles i auth.resolve_resource_access, så adgangen kan ikke låses ude.
EKSKLUDEREDE_ROLLER = ["marketing", "management"]

# Ressource-id'erne er de samme som nav-items i nav_utils.CATEGORIES. Det er
# ikke kosmetik: resolve_resource_access slår op på præcis dette id, så en
# admin kan åbne én side for én bruger via UserResourceAccess uden kodeændring
# — og menu og endpoint bruger så garanteret samme nøgle.
RES_OVERBLIK = "retention-overview"
RES_RISIKO = "retention-risk"

_AFVIST = "Retention er forbeholdt Sales Operations"


def _kraev_adgang(user: dict, resource_id: str) -> None:
    """403 medmindre brugeren må se den pågældende retention-side.

    Bruger resolve_resource_access og ikke has_access, fordi has_access kalder
    videre med resource_id="" og uden exclude_roles — den kan hverken se
    override-rækkerne eller holde marketing og management ude.
    """
    if resolve_resource_access(user, resource_id, MIN_ROLLE,
                               exclude_roles=EKSKLUDEREDE_ROLLER) == "none":
        raise HTTPException(403, _AFVIST)


def _resolve_filters(user: dict, resource_id: str) -> tuple[str | None, list | None]:
    """Oversæt brugerens rolle til (owner_name, team) for retention-queryen.

    Modulet er lukket for alt under Sales Operations (besluttet 2026-08-10).
    Retention-specialisten ER en Sales Operations-bruger og skal se hele
    firmaets churn-billede, så der findes ikke længere en egen-bog-visning:
    en sælger har ingen adgang overhovedet.

    Derfor er `owner_name` altid None — der er ingen rolle tilbage, der skal
    afgrænses til én persons bog. De tidligere salesperson- og
    sales_manager-grene er FJERNET frem for ladt stå: en uåbnelig gren i
    adgangskontrol er farlig, fordi den ser ud til at virke, hvis nogen senere
    sænker vagten.

    Teams kan stadig være begrænset: har admin sat HubUserTeamAccess på
    brugeren, bliver "hele firmaet" de tilladte teams — samme regel som
    _effective_team i perf-modulet. Ubegrænset giver (None, None) = firmabredt,
    inkl. de 16% kunder uden tilskrevet ejer i PipeDrive.
    """
    _kraev_adgang(user, resource_id)
    return None, allowed_data_teams(user)


@router.get("/retention/monthly_active_counts")
def get_monthly_active_counts(user=Depends(get_current_user)):
    owner_name, teams = _resolve_filters(user, RES_OVERBLIK)
    return db_monthly_active_counts(owner_name=owner_name, teams=teams)


@router.get("/retention/risk")
def get_abonnementer_i_risiko(user=Depends(get_current_user)):
    """Risikolisten pr. ABONNEMENT. Samme rolle-filtrering som trendlinjen.

    Skiftet fra recency-modellen (risk.customers_at_risk) 2026-08-10, jf. PRD §3
    hvor 14/30-dages-tærsklerne udfases: signalet rådnede med filens alder, og
    grainen var kunden, hvilket er den forkerte måleenhed (PRD §2 og §7.2).
    Ruten beholder sit navn, så eksisterende links og bogmærker virker.

    `owner_name` er altid None nu, hvor modulet kræver Sales Operations: ingen
    tilbageværende rolle skal se sin egen bog. Den gamle advarsel om at
    `retention_owner` og ACV's org-ejer er uenige om ejerskab i 47% af rækkerne
    gælder derfor ikke længere for ADGANGEN — men den gælder stadig for enhver
    visning der grupperer på ejer, og det skal fremgå af siden.

    `abo_maaned` sendes bevidst IKKE videre: produktionsvisningen skal altid være
    indeværende måned, og parameteren findes kun til kontrolkørsler.
    """
    owner_name, teams = _resolve_filters(user, RES_RISIKO)
    return abonnementer_i_risiko(owner_name=owner_name, teams=teams)


@router.get("/retention/overview", response_class=HTMLResponse)
async def retention_overview(request: Request, user=Depends(get_current_user)):
    # Selve dataene hentes client-side og er beskyttet af _resolve_filters, men
    # skallen skal heller ikke kunne åbnes — en bruger uden adgang har intet at
    # gøre på siden, og et tomt panel med en fejlbesked er en dårlig afvisning.
    _kraev_adgang(user, RES_OVERBLIK)
    return templates.TemplateResponse(request, "retention_overview.html", {"user": user})


@router.get("/retention/risk_overview", response_class=HTMLResponse)
async def retention_risk_overview(request: Request, user=Depends(get_current_user)):
    # Samme adgangsvagt som de øvrige retention-sider: dataene er beskyttet af
    # _resolve_filters, men skallen skal heller ikke kunne åbnes uden adgang.
    _kraev_adgang(user, RES_RISIKO)
    return templates.TemplateResponse(request, "retention_risk.html", {"user": user})
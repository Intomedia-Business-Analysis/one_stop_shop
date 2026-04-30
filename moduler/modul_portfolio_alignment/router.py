import time
import traceback

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from auth import ROLE_LABELS, get_current_user, has_access
from moduler.modul_portfolio_alignment.queries import (
    ACCOUNT_SCOPES,
    compare_portfolios,
    fetch_customer_deals,
    fetch_web_sale_deals,
    list_account_scopes,
)

router = APIRouter(prefix="/tools/portfolio-alignment", tags=["Portfolio Alignment"])
templates = Jinja2Templates(directory="templates")
templates.env.globals["ROLE_LABELS"] = ROLE_LABELS

# Cache pr. scope (Pipedrive- og Zuora-load er ikke billige).
_CACHE: dict[str, dict] = {}
_CACHE_TTL_SEC = 300  # 5 min


def _get_comparison(scope: str, force: bool = False) -> dict:
    now = time.time()
    cached = _CACHE.get(scope)
    if not force and cached and (now - cached["ts"]) < _CACHE_TTL_SEC:
        return cached
    data = compare_portfolios(scope)
    _CACHE[scope] = {"data": data, "ts": now}
    return _CACHE[scope]


def _validate_scope(scope: str) -> str:
    if scope == "all" or scope in ACCOUNT_SCOPES:
        return scope
    raise HTTPException(400, f"Ukendt scope: {scope!r}")


@router.get("/", response_class=HTMLResponse)
async def alignment_page(request: Request, user=Depends(get_current_user)):
    if not has_access(user, "sales_operations"):
        raise HTTPException(403, "Kræver Sales Operations-adgang")
    return templates.TemplateResponse("portfolio_alignment.html", {
        "request": request,
        "user":    user,
    })


@router.get("/accounts")
async def alignment_accounts(user=Depends(get_current_user)):
    if not has_access(user, "sales_operations"):
        raise HTTPException(403, "Kræver Sales Operations-adgang")
    return JSONResponse({
        "scopes": [{"id": "all", "label": "Alle accounts"}] + list_account_scopes(),
    })


@router.get("/comparison")
async def alignment_comparison(
    scope: str = "all",
    refresh: int = 0,
    user=Depends(get_current_user),
):
    if not has_access(user, "sales_operations"):
        raise HTTPException(403, "Kræver Sales Operations-adgang")
    scope = _validate_scope(scope)
    try:
        cached = _get_comparison(scope, force=bool(refresh))
        return JSONResponse({
            **cached["data"],
            "cached_at": cached["ts"],
        })
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@router.get("/web-sale-deals")
async def alignment_web_sale_deals(
    scope: str,
    site: str,
    user=Depends(get_current_user),
):
    if not has_access(user, "sales_operations"):
        raise HTTPException(403, "Kræver Sales Operations-adgang")
    if scope not in ACCOUNT_SCOPES:
        raise HTTPException(400, "Konkret scope kræves (ikke 'all')")
    if not site:
        raise HTTPException(400, "site er påkrævet")
    try:
        return JSONResponse(fetch_web_sale_deals(scope, site))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@router.get("/customer-deals")
async def alignment_customer_deals(
    scope: str,
    org_id: str,
    user=Depends(get_current_user),
):
    if not has_access(user, "sales_operations"):
        raise HTTPException(403, "Kræver Sales Operations-adgang")
    if scope == "all" or scope not in ACCOUNT_SCOPES:
        raise HTTPException(400, "Konkret scope kræves (ikke 'all')")
    if not org_id:
        raise HTTPException(400, "org_id er påkrævet")
    try:
        return JSONResponse(fetch_customer_deals(scope, org_id))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))

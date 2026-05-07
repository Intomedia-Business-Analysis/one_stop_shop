import time
import traceback

from fastapi import APIRouter, Body, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from auth import ROLE_LABELS, get_current_user, has_access
from moduler.modul_portfolio_alignment.queries import (
    ACCOUNT_SCOPES,
    compare_portfolios,
    fetch_customer_deals,
    fetch_web_sale_deals,
    get_handled_states,
    get_note,
    init_portfolio_notes_db,
    list_account_scopes,
    save_note,
    set_handled,
)
from moduler.modul_portfolio_alignment.pipedrive_api import create_alignment_deal

router = APIRouter(prefix="/tools/portfolio-alignment", tags=["Portfolio Alignment"])
templates = Jinja2Templates(directory="templates")
templates.env.globals["ROLE_LABELS"] = ROLE_LABELS
init_portfolio_notes_db()

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
    site: str = "",
    user=Depends(get_current_user),
):
    if not has_access(user, "sales_operations"):
        raise HTTPException(403, "Kræver Sales Operations-adgang")
    if scope == "all" or scope not in ACCOUNT_SCOPES:
        raise HTTPException(400, "Konkret scope kræves (ikke 'all')")
    if not org_id:
        raise HTTPException(400, "org_id er påkrævet")
    try:
        return JSONResponse(fetch_customer_deals(scope, org_id, site=site or None))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@router.get("/handled-states")
async def alignment_handled_states(scope: str = "all", user=Depends(get_current_user)):
    if not has_access(user, "sales_operations"):
        raise HTTPException(403, "Kræver Sales Operations-adgang")
    return JSONResponse(get_handled_states(scope if scope != "all" else None))


@router.post("/handled")
async def alignment_set_handled(request: Request, user=Depends(get_current_user)):
    if not has_access(user, "sales_operations"):
        raise HTTPException(403, "Kræver Sales Operations-adgang")
    body    = await request.json()
    scope   = body.get("scope", "").strip()
    org_id  = body.get("org_id", "").strip()
    site    = body.get("site", "").strip()
    handled = bool(body.get("handled", False))
    if not scope or not org_id or not site:
        raise HTTPException(400, "scope, org_id og site er påkrævet")
    ok = set_handled(scope, org_id, site, handled, user["name"])
    if not ok:
        raise HTTPException(500, "Kunne ikke opdatere status")
    return JSONResponse({"ok": True})


@router.get("/note")
async def alignment_get_note(
    scope: str,
    org_id: str,
    site: str,
    user=Depends(get_current_user),
):
    if not has_access(user, "sales_operations"):
        raise HTTPException(403, "Kræver Sales Operations-adgang")
    note = get_note(scope, org_id, site)
    return JSONResponse(note or {"note": "", "updated_by": "", "updated_at": ""})


@router.post("/note")
async def alignment_save_note(request: Request, user=Depends(get_current_user)):
    if not has_access(user, "sales_operations"):
        raise HTTPException(403, "Kræver Sales Operations-adgang")
    body   = await request.json()
    scope  = body.get("scope", "").strip()
    org_id = body.get("org_id", "").strip()
    site   = body.get("site", "").strip()
    note   = body.get("note", "").strip()
    if not scope or not org_id or not site:
        raise HTTPException(400, "scope, org_id og site er påkrævet")
    ok = save_note(scope, org_id, site, note, user["name"])
    if not ok:
        raise HTTPException(500, "Kunne ikke gemme kommentar")
    return JSONResponse(get_note(scope, org_id, site) or {"note": note, "updated_by": user["name"], "updated_at": ""})


@router.post("/create-deal")
async def alignment_create_deal(
    payload: dict = Body(...),
    user=Depends(get_current_user),
):
    """Opret en Porteføljeafstemning-deal i Pipedrive baseret på diff-fortegnet.

    Body: {scope, org_id, site, diff, dry_run?}
    """
    if not has_access(user, "sales_operations"):
        raise HTTPException(403, "Kræver Sales Operations-adgang")

    scope   = payload.get("scope")
    org_id  = payload.get("org_id")
    site    = payload.get("site")
    diff    = payload.get("diff")
    dry_run = bool(payload.get("dry_run"))

    if not scope or scope not in ACCOUNT_SCOPES:
        raise HTTPException(400, "scope er påkrævet og skal være et gyldigt account-scope")
    if not org_id:
        raise HTTPException(400, "org_id er påkrævet")
    if not site:
        raise HTTPException(400, "site er påkrævet")
    try:
        diff_f = float(diff)
    except (TypeError, ValueError):
        raise HTTPException(400, "diff er påkrævet og skal være et tal")

    try:
        result = create_alignment_deal(
            scope=scope,
            org_id=int(org_id),
            site=site,
            diff=diff_f,
            dry_run=dry_run,
        )
        return JSONResponse(result)
    except (ValueError, RuntimeError) as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))

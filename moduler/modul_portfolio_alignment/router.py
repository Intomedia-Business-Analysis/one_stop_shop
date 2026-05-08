import time
import traceback

from fastapi import APIRouter, Body, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from auth import ROLE_LABELS, get_current_user, has_access
from moduler.modul_portfolio_alignment.queries import (
    ACCOUNT_SCOPES,
    compare_portfolios,
    compute_local_diff,
    fetch_customer_deals,
    fetch_web_sale_deals,
    get_handled_states,
    get_note,
    init_portfolio_notes_db,
    list_account_scopes,
    save_note,
    set_handled,
)
from moduler.modul_portfolio_alignment.pipedrive_api import (
    create_alignment_deal,
    preview_alignment_deal,
)

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


def _resolve_create_args(payload: dict) -> tuple[str, int, str]:
    """Træk og valider scope/org_id/site fra request-body."""
    scope  = (payload.get("scope") or "").strip()
    org_id = payload.get("org_id")
    site   = (payload.get("site")  or "").strip()
    if not scope or scope not in ACCOUNT_SCOPES:
        raise HTTPException(400, "scope er påkrævet og skal være et gyldigt account-scope")
    if not org_id:
        raise HTTPException(400, "org_id er påkrævet")
    if not site:
        raise HTTPException(400, "site er påkrævet")
    try:
        org_id_int = int(org_id)
    except (TypeError, ValueError):
        raise HTTPException(400, "org_id skal være et heltal")
    return scope, org_id_int, site


@router.get("/deal-preview")
async def alignment_deal_preview(
    scope: str,
    org_id: str,
    site: str,
    user=Depends(get_current_user),
):
    """Returnér valuta + value der vil blive brugt ved deal-oprettelse, uden at POSTe."""
    if not has_access(user, "sales_operations"):
        raise HTTPException(403, "Kræver Sales Operations-adgang")
    try:
        scope_v, org_id_v, site_v = _resolve_create_args({"scope": scope, "org_id": org_id, "site": site})
        local = compute_local_diff(scope_v, str(org_id_v), site_v)
        if abs(local["value"]) < 1:
            raise HTTPException(400, "diff er nul (eller for lille) — der er intet at afstemme")
        prev = preview_alignment_deal(
            scope=scope_v,
            org_id=org_id_v,
            site=site_v,
            diff_signed=float(local["value"]),
            currency=local["currency"],
        )
        prev["local"] = local  # currency-breakdown til UI
        return JSONResponse(prev)
    except HTTPException:
        raise
    except (ValueError, RuntimeError) as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@router.post("/create-deal")
async def alignment_create_deal(
    payload: dict = Body(...),
    user=Depends(get_current_user),
):
    """Opret en Porteføljeafstemning-deal i Pipedrive.

    Backend beregner selv currency + value via compute_local_diff:
    foretrukken valuta = single non-DKK hvis hele kunden ligger i én valuta,
    ellers DKK fallback.

    Body: {scope, org_id, site, dry_run?}
    """
    if not has_access(user, "sales_operations"):
        raise HTTPException(403, "Kræver Sales Operations-adgang")
    dry_run = bool(payload.get("dry_run"))
    scope, org_id_int, site = _resolve_create_args(payload)
    try:
        local = compute_local_diff(scope, str(org_id_int), site)
        if abs(local["value"]) < 1:
            raise HTTPException(400, "diff er nul (eller for lille) — der er intet at afstemme")
        result = create_alignment_deal(
            scope=scope,
            org_id=org_id_int,
            site=site,
            diff_signed=float(local["value"]),
            currency=local["currency"],
            dry_run=dry_run,
        )
        result["local"] = local
        return JSONResponse(result)
    except HTTPException:
        raise
    except (ValueError, RuntimeError) as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@router.post("/create-deals-bulk")
async def alignment_create_deals_bulk(
    payload: dict = Body(...),
    user=Depends(get_current_user),
):
    """Opret afstemnings-deals for alle rækker der matcher filteret.

    Body:
      scope:          'all' eller specifik scope-id
      sites:          [normaliserede sites] eller [] for alle
      statuses:       liste af 'mismatch'|'pd_only'|'zuora_only'|'match'
      max_diff_abs:   max |diff_dkk| pr. række (kr.); 0/null = ingen øvre grænse
      min_diff_abs:   min |diff_dkk| pr. række (kr.); standard 1
      include_handled: bool — inkludér håndterede rækker (default false)
      dry_run:        bool

    Returnerer: {created: [...], failed: [...], skipped: [...], dry_run, count_eligible}
    """
    if not has_access(user, "sales_operations"):
        raise HTTPException(403, "Kræver Sales Operations-adgang")

    scope_filter   = (payload.get("scope") or "all").strip() or "all"
    sites          = payload.get("sites") or []
    statuses       = payload.get("statuses") or ["mismatch", "pd_only", "zuora_only"]
    try:
        max_diff_abs = float(payload.get("max_diff_abs") or 0)
    except (TypeError, ValueError):
        max_diff_abs = 0.0
    try:
        min_diff_abs = float(payload.get("min_diff_abs") or 1)
    except (TypeError, ValueError):
        min_diff_abs = 1.0
    include_handled = bool(payload.get("include_handled"))
    dry_run         = bool(payload.get("dry_run"))

    if scope_filter != "all" and scope_filter not in ACCOUNT_SCOPES:
        raise HTTPException(400, f"Ukendt scope: {scope_filter!r}")
    if not isinstance(sites, list) or not isinstance(statuses, list):
        raise HTTPException(400, "sites og statuses skal være lister")
    statuses_set = set(statuses)

    cmp_data = compare_portfolios(scope_filter if scope_filter != "all" else None)
    rows = cmp_data.get("rows", [])
    handled = set()
    if not include_handled:
        for h in get_handled_states(scope_filter if scope_filter != "all" else None):
            handled.add((h["scope"], h["org_id"], h["site"]))

    eligible = []
    for r in rows:
        if not r.get("org_id"):
            continue                                # kun rækker med pipedrive-org
        if r.get("status") not in statuses_set:
            continue
        if scope_filter != "all" and r.get("scope") != scope_filter:
            continue
        if sites and r.get("site") not in sites:
            continue
        if abs(r.get("diff") or 0) < min_diff_abs:
            continue
        if max_diff_abs and abs(r.get("diff") or 0) > max_diff_abs:
            continue
        if not include_handled and (r["scope"], str(r["org_id"]), r["site"]) in handled:
            continue
        eligible.append(r)

    created, failed, skipped = [], [], []
    for r in eligible:
        try:
            local = compute_local_diff(r["scope"], str(r["org_id"]), r["site"])
            if abs(local["value"]) < 1:
                skipped.append({**_row_short(r), "reason": "diff for lille til at afstemme"})
                continue
            res = create_alignment_deal(
                scope=r["scope"],
                org_id=int(r["org_id"]),
                site=r["site"],
                diff_signed=float(local["value"]),
                currency=local["currency"],
                dry_run=dry_run,
            )
            created.append({
                **_row_short(r),
                "currency": res.get("currency"),
                "value":    res.get("value"),
                "deal_id":  res.get("deal_id"),
                "deal_url": res.get("deal_url"),
                "pipeline": (res.get("pipeline") or {}).get("name"),
            })
        except (ValueError, RuntimeError) as e:
            failed.append({**_row_short(r), "error": str(e)})
        except Exception as e:
            traceback.print_exc()
            failed.append({**_row_short(r), "error": str(e)})

    return JSONResponse({
        "dry_run":        dry_run,
        "count_eligible": len(eligible),
        "created":        created,
        "failed":         failed,
        "skipped":        skipped,
    })


def _row_short(r: dict) -> dict:
    """Mindre sub-set af en sammenligningsrække til brug i bulk-resultatet."""
    return {
        "scope":     r.get("scope"),
        "org_id":    r.get("org_id"),
        "org_name":  r.get("org_name"),
        "site":      r.get("site"),
        "diff_dkk":  r.get("diff"),
        "status":    r.get("status"),
    }

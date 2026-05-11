import threading
import time
import traceback
import uuid

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


# ---------------------------------------------------------------------------
# Bulk-job-pattern
# ---------------------------------------------------------------------------
# Bulk-oprettelse kalder Pipedrive's API én gang pr. række (1-3 sek pr. kald),
# så for 100+ rækker kan det tage minutter. Hvis vi gør det synkront i request-
# håndteringen, blokerer event-loopet og hubben fryser for alle brugere.
#
# I stedet starter endpointet en thread og returnerer øjeblikkeligt med et
# job_id. Frontend poller så /bulk-status?job_id=… hvert par sekunder for at
# tegne en progress-bar og resultatet til sidst.
#
# Note: in-memory store — hvis serveren genstartes mens et job kører, mistes
# status (men deals der allerede er oprettet i Pipedrive er der stadig). Det
# er acceptabelt for vores brug.

_BULK_JOBS: dict[str, dict] = {}
_BULK_JOBS_LOCK = threading.Lock()
_BULK_JOB_TTL_SEC = 3600  # behold færdige jobs i 1 time så frontend kan se resultatet


def _gc_old_bulk_jobs() -> None:
    """Ryd op i færdige jobs ældre end TTL — undgår at hukommelsen vokser."""
    now = time.time()
    with _BULK_JOBS_LOCK:
        stale = [
            k for k, v in _BULK_JOBS.items()
            if v.get("status") == "done" and (now - (v.get("finished_at") or 0)) > _BULK_JOB_TTL_SEC
        ]
        for k in stale:
            del _BULK_JOBS[k]


def _bulk_worker(job_id: str, eligible: list, dry_run: bool, user_name: str) -> None:
    """Kører i baggrundstråd — opretter én deal pr. række, opdaterer job-state.

    Bruger row's deal_currency/deal_value (fra compare_portfolios) i stedet for
    at kalde compute_local_diff pr. række — det sparer DB-query + fil-load
    (snapshot.csv) for hver eneste række og er nødvendigt for at bulken kan
    nå at færdiggøres på rimelig tid.
    """
    job = _BULK_JOBS[job_id]
    for r in eligible:
        currency    = r.get("deal_currency") or "DKK"
        diff_signed = r.get("deal_value")
        try:
            if diff_signed is None or abs(diff_signed) < 1:
                with _BULK_JOBS_LOCK:
                    job["skipped"].append({**_row_short(r), "reason": "diff for lille til at afstemne"})
                    job["progress"] += 1
                continue
            res = create_alignment_deal(
                scope=r["scope"],
                org_id=int(r["org_id"]),
                site=r["site"],
                diff_signed=float(diff_signed),
                currency=currency,
                dry_run=dry_run,
            )
            # Markér rækken som håndteret når deal'en faktisk er oprettet —
            # ikke ved dry-run. Det forhindrer kolleger i at håndtere rækken
            # bagefter, og at samme deal oprettes igen i næste bulk-kørsel.
            if not dry_run:
                try:
                    set_handled(r["scope"], str(r["org_id"]), r["site"], True, user_name)
                except Exception:
                    traceback.print_exc()  # ikke kritisk — deal'en er oprettet
            with _BULK_JOBS_LOCK:
                job["created"].append({
                    **_row_short(r),
                    "currency": res.get("currency"),
                    "value":    res.get("value"),
                    "deal_id":  res.get("deal_id"),
                    "deal_url": res.get("deal_url"),
                    "pipeline": (res.get("pipeline") or {}).get("name"),
                })
                job["progress"] += 1
        except (ValueError, RuntimeError) as e:
            with _BULK_JOBS_LOCK:
                job["failed"].append({**_row_short(r), "error": str(e)})
                job["progress"] += 1
        except Exception as e:
            traceback.print_exc()
            with _BULK_JOBS_LOCK:
                job["failed"].append({**_row_short(r), "error": str(e)})
                job["progress"] += 1
    with _BULK_JOBS_LOCK:
        job["status"]      = "done"
        job["finished_at"] = time.time()


@router.post("/create-deals-bulk")
async def alignment_create_deals_bulk(
    payload: dict = Body(...),
    user=Depends(get_current_user),
):
    """Start et bulk-oprettelses-job. Returnerer øjeblikkeligt med job_id.

    Body:
      scope:          'all' eller specifik scope-id
      sites:          [normaliserede sites] eller [] for alle
      statuses:       liste af 'mismatch'|'pd_only'|'zuora_only'|'match'
      max_diff_abs:   max |diff_dkk| pr. række (kr.); 0/null = ingen øvre grænse
      min_diff_abs:   min |diff_dkk| pr. række (kr.); standard 1
      include_handled: bool — inkludér håndterede rækker (default false)
      dry_run:        bool

    Returnerer: {job_id, count_eligible, dry_run}
    Frontend poller /bulk-status?job_id=… for at få fremdrift og slutresultat.
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

    _gc_old_bulk_jobs()
    job_id = uuid.uuid4().hex
    with _BULK_JOBS_LOCK:
        _BULK_JOBS[job_id] = {
            "status":         "running",
            "progress":       0,
            "total":          len(eligible),
            "created":        [],
            "failed":         [],
            "skipped":        [],
            "dry_run":        dry_run,
            "started_at":     time.time(),
            "finished_at":    None,
        }
    # daemon=True så tråden ikke blokerer en evt. server-genstart
    threading.Thread(
        target=_bulk_worker,
        args=(job_id, eligible, dry_run, user.get("name") or "system"),
        daemon=True,
    ).start()

    return JSONResponse({
        "job_id":         job_id,
        "count_eligible": len(eligible),
        "dry_run":        dry_run,
    })


@router.get("/bulk-status")
async def alignment_bulk_status(
    job_id: str,
    user=Depends(get_current_user),
):
    """Returnér status for et igangværende eller netop færdigt bulk-job."""
    if not has_access(user, "sales_operations"):
        raise HTTPException(403, "Kræver Sales Operations-adgang")
    with _BULK_JOBS_LOCK:
        job = _BULK_JOBS.get(job_id)
        if not job:
            raise HTTPException(404, "Ukendt job (eller udløbet)")
        # Returnér en kopi så frontend kan læse uden at vi holder lock i JSON-encoding
        return JSONResponse(dict(job))


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

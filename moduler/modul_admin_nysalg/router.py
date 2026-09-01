"""FastAPI-routes for værktøjet til matchning af administrative nysalg.

Flow: vælg/upload udtræk → match mod administrative PipeDrive-deals → review
(kommentér + override) → direktøren godkender → rapport (Excel/PDF) genereres og
kan downloades.

Adgang: visning/oprettelse kræver sales_operations+; godkendelse og rapport
kræver management+ (direktør-niveau) — admin bypasser begge via rang.
"""
import calendar
import datetime as _dt
import logging
import os
import threading
import time
import uuid

from fastapi import (APIRouter, Body, Depends, File, Form, HTTPException, Request,
                     UploadFile)
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from auth import get_current_user, has_access
from constants import MONTH_NAMES_DA
from log_setup import audit_log
from nav_utils import register_nav_globals
from moduler.modul_admin_nysalg import extract_loader, report, repo
from moduler.modul_admin_nysalg.brands import classify
from moduler.modul_admin_nysalg.extract_loader import ExtractError
from moduler.modul_admin_nysalg.matcher import build_index, match_rows
from moduler.modul_admin_nysalg.pipedrive_source import get_default_source

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tools/admin-nysalg", tags=["Administrative Nysalg"])
templates = Jinja2Templates(directory="templates")
register_nav_globals(templates)

VIEW_MIN_ROLE = "management"           # se/forberede runs
APPROVE_MIN_ROLE = "management"        # godkende + generere rapport
ADMIN_MIN_ROLE = "admin"              # slette runs


def _require_view(user: dict) -> None:
    if not has_access(user, VIEW_MIN_ROLE):
        raise HTTPException(403, "Ingen adgang til administrative nysalg")


def _require_approve(user: dict) -> None:
    if not has_access(user, APPROVE_MIN_ROLE):
        raise HTTPException(403, "Kun direktør/management kan godkende og generere rapport")


def _require_admin(user: dict) -> None:
    if not has_access(user, ADMIN_MIN_ROLE):
        raise HTTPException(403, "Kun administrator kan slette rapporter")


def _default_extract_path() -> str:
    return os.getenv("ADMIN_NYSALG_EXTRACT_PATH", "")


def _get_run_or_404(run_id: int) -> dict:
    run = repo.get_run(run_id)
    if not run:
        raise HTTPException(404, "Run ikke fundet")
    return run


def _parse_range(date_from: str, date_to: str) -> tuple[str | None, str | None, str | None]:
    """Validér Fra/Til (ISO YYYY-MM-DD, begge valgfrie) → (from, to, label).

    Zuora-bevægelser er stemplet på månedens sidste dag (EOMONTH), så perioden
    snappes til HELE måneder: Fra → den 1. i måneden, Til → den sidste dag i
    måneden. Ellers ville en dag-i-måneden-slutdato (fx 15. maj) udelukke hele
    den måneds bevægelser (month_end = 31. maj). label er en månedsbaseret tekst
    til run.period (None = hele udtrækket).
    """
    df = (date_from or "").strip() or None
    dt = (date_to or "").strip() or None
    parsed: dict[str, _dt.date] = {}
    for key, v in (("df", df), ("dt", dt)):
        if v:
            try:
                parsed[key] = _dt.date.fromisoformat(v)
            except ValueError:
                raise ExtractError(f"Ugyldig dato: {v!r} — brug formatet ÅÅÅÅ-MM-DD.")
    if "df" in parsed and "dt" in parsed and parsed["df"] > parsed["dt"]:
        raise ExtractError("Fra-dato skal være før eller lig med Til-dato.")
    if "df" in parsed:
        df = parsed["df"].replace(day=1).isoformat()
    if "dt" in parsed:
        d = parsed["dt"]
        df_last = calendar.monthrange(d.year, d.month)[1]
        dt = d.replace(day=df_last).isoformat()
    # Månedsbaseret label (YYYY-MM), så det afspejler at perioden dækker hele måneder.
    if df and dt:
        fm, tm = df[:7], dt[:7]
        label = fm if fm == tm else f"{fm} – {tm}"
    elif df:
        label = f"fra {df[:7]}"
    elif dt:
        label = f"til {dt[:7]}"
    else:
        label = None
    return df, dt, label


def _month_label(ym: str) -> str:
    """'YYYY-MM' → 'Måned ÅÅÅÅ' (dansk), fallback til ym selv."""
    try:
        y, m = ym.split("-")
        return f"{MONTH_NAMES_DA[int(m) - 1]} {y}"
    except (ValueError, IndexError):
        return ym


def _months_breakdown(matches: list, date_from, date_to, comments: dict,
                      scope: str = "business_media") -> list[dict]:
    """[{ym, label, rows}] pr. måned i intervallet (til review + rapport)."""
    by_month = repo.brand_rows_by_month(matches, date_from, date_to, comments, scope=scope)
    return [{"ym": ym, "label": _month_label(ym), "rows": rows}
            for ym, rows in by_month.items()]


def _scope_extra_rows(scope: str, date_from, date_to, brand_comments: dict,
                      budgets: dict) -> tuple[list[dict], dict | None]:
    """DB-delen af brand-rækkerne: (PipeDrive-annonce-rækker, monitor-site-budgetter).

    Skilt fra selve opsummeringen, så review-handleren kan hente den parallelt
    med de andre tunge opslag (matches, org-navne)."""
    if scope == "monitor":
        ad_rows = repo.monitor_advertising_brand_rows(date_from, date_to,
                                                     brand_comments)
        return ad_rows, repo.monitor_site_budgets(date_from, date_to)
    return repo.pipedrive_brand_rows(date_from, date_to, brand_comments, budgets,
                                     parallel=True), None


def _scope_brand_rows(scope: str, matches: list[dict], date_from, date_to,
                      brand_comments: dict, budgets: dict,
                      prefetched: tuple | None = None) -> list[dict]:
    """Brand-rækkerne for review/rapport efter runnets scope.

    business_media: Zuora-brands + PipeDrive-annonce-rækkerne (som hidtil).
    monitor: én række pr. Monitor-site (site-budgetter) + Monitor Job/Banner.
    prefetched = resultatet af _scope_extra_rows hvis allerede hentet (parallelt).
    """
    extra_rows, norm_budgets = (prefetched if prefetched is not None
                                else _scope_extra_rows(scope, date_from, date_to,
                                                       brand_comments, budgets))
    if scope == "monitor":
        return repo.monitor_brand_rows(matches, norm_budgets or {},
                                       brand_comments, extra_rows=extra_rows)
    return repo.summarize_by_brand(matches, budgets, brand_comments, extra_rows=extra_rows)


def _match_brand(m: dict) -> str:
    """Brand-label for en Zuora-match-række (samme logik som summarize_by_brand)."""
    from moduler.modul_admin_nysalg.brands import classify
    return m.get("brand") or classify(m.get("site"))


def _visible_matches(run: dict) -> list[dict]:
    """Match-rækker for et run, afgrænset til runnets rapport-scope.

    business_media: alt UNDTAGEN de udeladte brands (Monitor) — pilles ud så
    tidligt som muligt, så det forsvinder fra ALT: top-tal, brand-tabel,
    måneds-opdeling OG Excel-detaljefanerne.
    monitor: KUN Monitor-bevægelser, relabel'et med brand = site, så hele
    pipelinen arbejder pr. enkelt site.
    """
    from moduler.modul_admin_nysalg.brands import EXCLUDED_BRANDS
    matches = repo.get_matches(run["run_id"])
    if repo.run_scope(run) == "monitor":
        return repo.monitor_relabel(
            [m for m in matches if _match_brand(m) == "Monitor"])
    return [m for m in matches if _match_brand(m) not in EXCLUDED_BRANDS]


def _click_summary_payload(run: dict) -> dict:
    """Opdaterede topkort-tal + per-brand netto efter et klik i reviewet.

    Beregnes i SQL i ÉN GROUP BY-query (Fase B) i stedet for at hente alle
    match-rækker over netværket og regne i Python — klik-gem er dermed
    uafhængig af runnets størrelse. Topkortene ekskluderer skjulte brands
    (spejler _apply_hidden på review-siden); per-brand-tallene beholder dem,
    da brand-tabellen viser skjulte brands så de kan klikkes tilbage.

    Kun Subscription-brands medtages i brand-map'et: annonce-rækkerne
    (Job/Banner/Norge/Marketwire) kommer fra PipeDrive og påvirkes ikke af
    gross in/out — og en Zuora-bucket for dem ville ellers overskrive den
    rigtige PipeDrive-række i tabellen.
    """
    from moduler.modul_admin_nysalg.brands import brand_geo
    run_id = run["run_id"]
    groups = repo.aggregate_by_brand_sql(run_id, repo.run_scope(run))
    hidden = repo.get_hidden_brands(run_id)
    brands = {}
    for label, g in groups.items():
        if brand_geo(label)[1] != "Subscription":
            continue
        b = {k: round(g[k], 2)
             for k in ("brutto", "adm_nysalg", "opsigelser", "adm_opsigelser")}
        b["netto"] = round((b["brutto"] - b["adm_nysalg"])
                           - (b["opsigelser"] - b["adm_opsigelser"]), 2)
        brands[label] = b
    return {"ok": True,
            "summary": repo.summarize_from_groups(groups, exclude_brands=hidden),
            "brands": brands}


def _brand_movements(matches: list[dict], org_names: dict | None = None) -> list[dict]:
    """Bevægelser (gross in/out) grupperet pr. brand til review-siden.

    Direktøren ser her de Zuora-bevægelser fra udtrækket der indgår i omsætning OG
    opsigelser, og kan medtage/udelukke + rette gross in/out pr. række. Kun rækker
    der rå bidrager (gross in eller out ≠ 0) listes — også udeladte (så de kan slås
    til igen). Kundenavn slås op pr. brands KONTO (org-id er ikke unikke), samme
    opslag som Excel-"Movements"-arket. Returneres i DISPLAY_ORDER.
    org_names kan gives med, hvis kalderen allerede har hentet dem (parallelt).
    """
    from moduler.modul_admin_nysalg.brands import DISPLAY_ORDER
    if org_names is None:
        org_names = repo.pipedrive_org_names()
    groups: dict[str, list[dict]] = {}
    for m in matches:
        gi_raw = m.get("gross_in") or 0
        go_raw = repo._row_opsigelse(m)
        if not gi_raw and not go_raw:
            continue
        label = _match_brand(m)
        pid = str(m.get("pipedrive_id") or "").strip()
        customer = repo.customer_name(dict(m, brand=label), org_names)
        gi_ov, go_ov = m.get("gross_in_override"), m.get("gross_out_override")
        ai_ov, ao_ov = m.get("adm_in_override"), m.get("adm_out_override")
        is_adm_in = repo.effective_is_admin(m)
        is_adm_out = repo.is_admin_opsigelse(m)
        # Effektiv administrativ andel uanset medtag/udeluk (til visning i Adm.-
        # felterne) — automatisk delvis når deal-værdien er mindre end gross.
        mc = dict(m, total_excluded=False)
        adm_in_eff = repo.effective_adm_in(mc)
        adm_out_eff = repo.effective_adm_out(mc)
        gi_eff = float(gi_ov if gi_ov is not None else gi_raw)
        go_eff = float(go_ov if go_ov is not None else go_raw)
        adm_partial = ((is_adm_in and adm_in_eff < gi_eff - 0.5)
                       or (is_adm_out and adm_out_eff < go_eff - 0.5))
        groups.setdefault(label, []).append({
            "match_id": m.get("match_id"),
            "site": m.get("site") or "",
            "customer": customer or "—",
            "account_number": m.get("account_number") or "",
            "pipedrive_id": pid,
            "month_end": m.get("month_end") or "",
            "movement": m.get("movement") or "",
            "currency": m.get("currency") or "DKK",
            # Inputfelterne viser override hvis sat, ellers den rå værdi (heltal).
            "gross_in_input": int(round(gi_ov if gi_ov is not None else gi_raw)),
            "gross_out_input": int(round(go_ov if go_ov is not None else go_raw)),
            # Delvist administrativ: den administrative andel af gross in/out.
            # Blank = automatikken (deal-værdi hvis mindre end gross, ellers alt).
            "adm_in_input": "" if ai_ov is None else int(round(ai_ov)),
            "adm_out_input": "" if ao_ov is None else int(round(ao_ov)),
            # Placeholder: den andel der faktisk trækkes fra, når feltet er blankt.
            "adm_in_auto": int(round(adm_in_eff)) if is_adm_in else "",
            "adm_out_auto": int(round(adm_out_eff)) if is_adm_out else "",
            "adm_split": ai_ov is not None or ao_ov is not None,
            # Kun en del af beløbet er administrativt (auto eller manuelt).
            "adm_partial": adm_partial,
            "edited": gi_ov is not None or go_ov is not None,
            "excluded": bool(m.get("total_excluded")),
            # Administrativ = trækkes allerede fra Actual Sale/Churn (admin-matchet).
            # Markeres i UI, så det ikke ligner at den tæller med i totalen.
            "administrativ": repo.effective_is_admin(m) or repo.is_admin_opsigelse(m),
        })

    def _order(label):
        return DISPLAY_ORDER.index(label) if label in DISPLAY_ORDER else len(DISPLAY_ORDER)

    out = []
    for label in sorted(groups, key=lambda l: (_order(l), l)):
        rows = sorted(groups[label],
                      key=lambda r: max(r["gross_in_input"], r["gross_out_input"]), reverse=True)
        out.append({"brand": label, "rows": rows})
    return out


def _apply_hidden(matches: list, brand_rows: list, months_breakdown: list,
                  hidden: set) -> tuple[dict, list, list]:
    """Fjern skjulte brands fra rapporten: brand-tabel, måneds-opdeling OG top-tal.

    Returnerer (summary, brand_rows, months_breakdown) hvor skjulte brands er
    pillet ud overalt. Topkort-tallene genberegnes fra de tilbageværende Zuora-
    matches (PipeDrive-only-brands indgår alligevel ikke i topkortene).
    """
    if hidden:
        matches = [m for m in matches if _match_brand(m) not in hidden]
        brand_rows = [b for b in brand_rows if b["brand"] not in hidden]
        months_breakdown = [
            {**blk, "rows": [b for b in blk.get("rows", []) if b["brand"] not in hidden]}
            for blk in months_breakdown
        ]
    return repo.summarize(matches), brand_rows, months_breakdown


# ── Forside + nyt run ────────────────────────────────────────────────────────
# Endpoints med DB-arbejde er bevidst sync (`def`, ikke `async def`): FastAPI
# kører dem så i threadpoolen, så de blokerende pymssql-kald ikke fryser hele
# event-loopet (og dermed resten af hubben) mens de kører. POST-bodies læses via
# Body(...) i stedet for `await request.json()` af samme grund.

@router.get("/", response_class=HTMLResponse)
def index(request: Request, user=Depends(get_current_user)):
    _require_view(user)
    runs = repo.list_runs(100)
    return templates.TemplateResponse(request, "admin_nysalg_index.html", {
        "user": user,
        "runs": runs,
        "can_approve": has_access(user, APPROVE_MIN_ROLE),
        "is_admin": has_access(user, ADMIN_MIN_ROLE),
    })


@router.post("/{run_id}/delete")
def delete_run(run_id: int, user=Depends(get_current_user)):
    _require_admin(user)
    if not repo.delete_run(run_id):
        raise HTTPException(404, "Run ikke fundet")
    audit_log("admin_nysalg_slettet", user=user, run_id=run_id)
    return JSONResponse({"ok": True})


@router.get("/new", response_class=HTMLResponse)
def new_run(request: Request, user=Depends(get_current_user)):
    _require_view(user)
    return templates.TemplateResponse(request, "admin_nysalg_new.html", {
        "user": user,
        "default_path": _default_extract_path(),
        "error": None,
    })


# ---------------------------------------------------------------------------
# Matchning som baggrundsjob (med progress-bar)
# ---------------------------------------------------------------------------
# Matchning + indsættelse kan tage tid for store udtræk (én INSERT pr. bevægelses-
# række). Vi kører det derfor i en daemon-tråd og lader frontend polle /run-status
# for en progress-bar, i stedet for at blokere request-håndteringen. In-memory
# store — status mistes ved server-genstart (acceptabelt; et evt. oprettet run
# findes stadig i databasen).

_RUN_JOBS: dict[str, dict] = {}
_RUN_JOBS_LOCK = threading.Lock()
_RUN_JOB_TTL_SEC = 1800   # behold færdige jobs i 30 min


def _gc_old_run_jobs() -> None:
    now = time.time()
    with _RUN_JOBS_LOCK:
        stale = [k for k, v in _RUN_JOBS.items()
                 if v.get("status") in ("done", "error")
                 and (now - (v.get("finished_at") or 0)) > _RUN_JOB_TTL_SEC]
        for k in stale:
            del _RUN_JOBS[k]


def _set_job(job_id: str, **fields) -> None:
    with _RUN_JOBS_LOCK:
        job = _RUN_JOBS.get(job_id)
        if job is not None:
            job.update(fields)


def _run_worker(job_id, user, file_bytes, filename, src_path, src_name,
                date_from, date_to, period_label,
                report_scope="business_media") -> None:
    """Kører matchningen i baggrunden og opdaterer job-state løbende."""
    try:
        _set_job(job_id, phase="Indlæser udtræk…", percent=8)
        if file_bytes is not None:
            rows_all = extract_loader.load_extract(file_bytes=file_bytes, filename=filename)
        else:
            rows_all = extract_loader.load_extract(path=src_path)

        rows = extract_loader.filter_range(rows_all, date_from, date_to)
        if not rows:
            _set_job(job_id, status="error", finished_at=time.time(),
                     error=f"Ingen rækker i udtrækket for perioden {period_label or 'alle'}.")
            return

        _set_job(job_id, phase="Henter PipeDrive-deals…", percent=22)
        source = get_default_source()
        deals = source.fetch_admin_deals(date_from, date_to)

        _set_job(job_id, phase="Matcher mod deals…", percent=38)
        site_map = repo.load_site_map()
        idx, dups = build_index(deals, site_map)
        match_rows(rows, idx, dups, site_map)
        # Brand-gruppér hver række (Watch/Finans/Monitor/Norge/SE/DE/Marketwire).
        for r in rows:
            r.brand = classify(r.site)

        _set_job(job_id, phase="Gemmer resultat…", percent=45)
        run_id = repo.create_run(user.get("name"), src_path, src_name, period_label,
                                 date_from, date_to, report_scope=report_scope)

        def _prog(i, n):
            _set_job(job_id, percent=(45 + int(50 * i / n)) if n else 95)
        repo.insert_matches(run_id, rows, progress_cb=_prog)
        repo.update_status(run_id, "in_review")

        audit_log("admin_nysalg_run", user=user, run_id=run_id,
                  periode=period_label or "alle", raekker=len(rows), deals=len(deals))
        _set_job(job_id, status="done", phase="Færdig", percent=100,
                 run_id=run_id, finished_at=time.time())
    except ExtractError as e:
        _set_job(job_id, status="error", finished_at=time.time(), error=str(e))
    except Exception:
        logger.exception("admin-nysalg matchning fejlede (job=%s)", job_id)
        _set_job(job_id, status="error", finished_at=time.time(),
                 error="Matchningen fejlede — prøv igen eller kontakt support.")


@router.post("/run")
async def run_match(
    request: Request,
    file: UploadFile = File(None),
    source_path: str = Form(""),
    period_from: str = Form(""),
    period_to: str = Form(""),
    report_scope: str = Form("business_media"),
    user=Depends(get_current_user),
):
    """Start matchningen som baggrundsjob. Returnerer {job_id}; frontend poller
    /run-status for progress og redirecter til review når jobbet er færdigt."""
    from moduler.modul_admin_nysalg.brands import VALID_SCOPES
    _require_view(user)
    if report_scope not in VALID_SCOPES:
        return JSONResponse({"error": f"Ugyldig rapport-type: {report_scope!r}"},
                            status_code=400)

    # Validér interval + kildevalg synkront, så brugeren får øjeblikkelig fejl.
    try:
        date_from, date_to, period_label = _parse_range(period_from, period_to)
        file_bytes = filename = src_path = src_name = None
        if file is not None and file.filename:
            file_bytes = await file.read()
            filename = src_name = file.filename
        else:
            path = (source_path or "").strip() or _default_extract_path()
            if not path:
                raise ExtractError("Vælg en fil at uploade, eller angiv en sti til udtrækket.")
            src_path = path
            src_name = os.path.basename(path)
    except ExtractError as e:
        return JSONResponse({"error": str(e)}, status_code=400)

    _gc_old_run_jobs()
    job_id = uuid.uuid4().hex
    with _RUN_JOBS_LOCK:
        _RUN_JOBS[job_id] = {
            "status": "running", "phase": "Starter…", "percent": 2,
            "run_id": None, "error": None,
            "started_at": time.time(), "finished_at": None,
        }
    threading.Thread(
        target=_run_worker,
        args=(job_id, user, file_bytes, filename, src_path, src_name,
              date_from, date_to, period_label, report_scope),
        daemon=True,
    ).start()
    return JSONResponse({"job_id": job_id})


@router.get("/run-status")
async def run_status(job_id: str, user=Depends(get_current_user)):
    """Status for et igangværende eller netop færdigt matchnings-job."""
    _require_view(user)
    with _RUN_JOBS_LOCK:
        job = _RUN_JOBS.get(job_id)
        if not job:
            raise HTTPException(404, "Ukendt job (eller udløbet)")
        return JSONResponse(dict(job))


# ── Review ───────────────────────────────────────────────────────────────────

@router.get("/{run_id}/review", response_class=HTMLResponse)
def review(run_id: int, request: Request, user=Depends(get_current_user)):
    _require_view(user)
    run = _get_run_or_404(run_id)
    scope = repo.run_scope(run)
    date_from, date_to = repo.run_date_range(run)
    budgets = repo.brand_budgets(date_from, date_to) if scope != "monitor" else {}
    brand_comments = repo.get_brand_comments(run_id)
    # De tre tunge, indbyrdes uafhængige opslag (match-rækker, org-navne og
    # PipeDrive-annonce-rækker) hentes parallelt — sekventielt lå de i forlængelse
    # af hinanden og udgjorde det meste af sidens åbnetid.
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=3) as ex:
        f_matches = ex.submit(_visible_matches, run)
        f_orgs = ex.submit(repo.pipedrive_org_names)
        f_extra = ex.submit(_scope_extra_rows, scope, date_from, date_to,
                            brand_comments, budgets)
        matches = f_matches.result()
        org_names = f_orgs.result()
        prefetched = f_extra.result()
    brand_rows = _scope_brand_rows(scope, matches, date_from, date_to,
                                   brand_comments, budgets, prefetched=prefetched)
    # adm_share = det der faktisk trækkes fra Actual Sale (deal-værdien ved
    # delvist administrative rækker, ellers hele gross in).
    admin_rows = [dict(m, adm_share=repo.effective_adm_in(dict(m, total_excluded=False)),
                       kunde=repo.customer_name(m, org_names))
                  for m in matches if repo.effective_is_admin(m)]
    # Brand-tabellen viser ALLE brands (også skjulte, så de kan klikkes tilbage),
    # men topkortene afspejler skjulningen. Måneds-opdelingen (mange per-måned-
    # queries) hentes asynkront via /months-fragment, så siden åbner hurtigt.
    hidden = repo.get_hidden_brands(run_id)
    summary, _, _ = _apply_hidden(matches, brand_rows, [], hidden)
    brand_movements = _brand_movements(matches, org_names)
    return templates.TemplateResponse(request, "admin_nysalg_review.html", {
        "user": user,
        "run": run,
        "report_scope": scope,
        "summary": summary,
        "brand_rows": brand_rows,
        "hidden_brands": sorted(hidden),
        "admin_rows": admin_rows,
        "brand_movements": brand_movements,
        "can_approve": has_access(user, APPROVE_MIN_ROLE),
        "locked": run.get("status") in ("approved", "reported"),
    })


@router.get("/{run_id}/months-fragment", response_class=HTMLResponse)
def months_fragment(run_id: int, request: Request, user=Depends(get_current_user)):
    """Måneds-opdelingen som HTML-fragment — hentes asynkront af review-siden.

    Beregningen laver ~25 PipeDrive-/budget-queries pr. måned og kan tage
    adskillige sekunder for lange perioder; ved at hente den i baggrunden
    blokerer den ikke sideåbningen. Tom body (204) ved én eller ingen måneder.
    """
    _require_view(user)
    run = _get_run_or_404(run_id)
    scope = repo.run_scope(run)
    matches = _visible_matches(run)
    date_from, date_to = repo.run_date_range(run)
    brand_comments = repo.get_brand_comments(run_id)
    months_breakdown = _months_breakdown(matches, date_from, date_to, brand_comments,
                                         scope=scope)
    hidden = repo.get_hidden_brands(run_id)
    if hidden:
        months_breakdown = [
            {**blk, "rows": [b for b in blk.get("rows", []) if b["brand"] not in hidden]}
            for blk in months_breakdown
        ]
    if not months_breakdown or len(months_breakdown) < 2:
        return HTMLResponse("", status_code=204)
    return templates.TemplateResponse(request, "_admin_nysalg_months.html", {
        "months_breakdown": months_breakdown,
        "is_monitor": scope == "monitor",
    })


@router.post("/{run_id}/comment")
def save_comment(run_id: int, body: dict = Body(...), user=Depends(get_current_user)):
    _require_view(user)
    _get_run_or_404(run_id)
    scope = body.get("scope")
    comment = (body.get("comment") or "").strip()
    if scope == "director":
        repo.set_director_comment(run_id, comment)
    elif scope == "brand":
        brand = (body.get("brand") or "").strip()
        if not brand:
            raise HTTPException(400, "brand påkrævet")
        repo.set_brand_comment(run_id, brand, comment)
    elif scope == "row":
        match_id = body.get("match_id")
        if not match_id:
            raise HTTPException(400, "match_id påkrævet")
        repo.set_row_comment(run_id, int(match_id), comment)
    else:
        raise HTTPException(400, "Ukendt scope")
    return JSONResponse({"ok": True})


@router.post("/{run_id}/override")
def set_override(run_id: int, body: dict = Body(...), user=Depends(get_current_user)):
    _require_view(user)
    run = _get_run_or_404(run_id)
    if run.get("status") in ("approved", "reported"):
        raise HTTPException(409, "Run er låst — override kan ikke ændres")
    match_id = body.get("match_id")
    override = body.get("override")
    if override in ("", "default", None):
        override = None
    repo.set_override(run_id, int(match_id), override)
    # Returnér opdaterede topkort-tal + per-brand netto, så frontend kan opdatere uden reload.
    return JSONResponse(_click_summary_payload(run))


def _require_unlocked(run: dict) -> None:
    if run.get("status") in ("approved", "reported"):
        raise HTTPException(409, "Run er låst — bevægelser kan ikke ændres")


@router.post("/{run_id}/row-include")
def row_include(run_id: int, body: dict = Body(...), user=Depends(get_current_user)):
    """Medtag/udeluk en enkelt bevægelse fra rapportens totaler."""
    _require_view(user)
    run = _get_run_or_404(run_id)
    _require_unlocked(run)
    match_id = body.get("match_id")
    if not match_id:
        raise HTTPException(400, "match_id påkrævet")
    repo.set_row_total_excluded(run_id, int(match_id), bool(body.get("excluded")))
    return JSONResponse(_click_summary_payload(run))


@router.post("/{run_id}/row-value")
def row_value(run_id: int, body: dict = Body(...), user=Depends(get_current_user)):
    """Ret gross in/gross out for en bevægelse (None pr. felt = ryd → brug rå værdi)."""
    _require_view(user)
    run = _get_run_or_404(run_id)
    _require_unlocked(run)
    match_id = body.get("match_id")
    if not match_id:
        raise HTTPException(400, "match_id påkrævet")

    def _num(v):
        if v in (None, "", "default"):
            return None
        try:
            return round(float(v), 2)
        except (TypeError, ValueError):
            raise HTTPException(400, f"Ugyldig værdi: {v!r}")

    repo.set_row_value_override(run_id, int(match_id),
                                _num(body.get("gross_in")), _num(body.get("gross_out")))
    return JSONResponse(_click_summary_payload(run))


@router.post("/{run_id}/row-adm")
def row_adm(run_id: int, body: dict = Body(...), user=Depends(get_current_user)):
    """Sæt den administrative andel af gross in/out for en bevægelse (delvist
    administrativ deal — fx hvor kun en del af beløbet er administrativt).
    None/blank pr. felt = ryd → alt-eller-intet efter admin-match/flag."""
    _require_view(user)
    run = _get_run_or_404(run_id)
    _require_unlocked(run)
    match_id = body.get("match_id")
    if not match_id:
        raise HTTPException(400, "match_id påkrævet")

    def _num(v):
        if v in (None, "", "default"):
            return None
        try:
            f = round(float(v), 2)
        except (TypeError, ValueError):
            raise HTTPException(400, f"Ugyldig værdi: {v!r}")
        if f < 0:
            raise HTTPException(400, "Administrativ andel kan ikke være negativ")
        return f

    repo.set_row_adm_split(run_id, int(match_id),
                           _num(body.get("adm_in")), _num(body.get("adm_out")))
    return JSONResponse(_click_summary_payload(run))


@router.post("/{run_id}/brand-visibility")
def brand_visibility(run_id: int, body: dict = Body(...), user=Depends(get_current_user)):
    """Klik et brand til/fra rapporten. Skjulte brands fjernes fra brand-tabel,
    måneds-opdeling og top-tallene (både i review og den genererede rapport)."""
    _require_view(user)
    run = _get_run_or_404(run_id)
    if run.get("status") in ("approved", "reported"):
        raise HTTPException(409, "Run er låst — brands kan ikke skjules")
    brand = (body.get("brand") or "").strip()
    if not brand:
        raise HTTPException(400, "brand påkrævet")
    repo.set_brand_hidden(run_id, brand, bool(body.get("hidden")))
    return JSONResponse({"ok": True})


@router.post("/{run_id}/approve")
def approve(run_id: int, body: dict = Body({}), user=Depends(get_current_user)):
    _require_approve(user)
    run = _get_run_or_404(run_id)
    # Gem evt. samlet kommentar sendt med godkendelsen.
    if "director_comment" in body:
        repo.set_director_comment(run_id, (body.get("director_comment") or "").strip())
    repo.approve_run(run_id, user.get("name"))
    audit_log("admin_nysalg_godkendt", user=user, run_id=run_id)
    return JSONResponse({"ok": True})


# ── Rapport ──────────────────────────────────────────────────────────────────

@router.post("/{run_id}/report")
def make_report(run_id: int, user=Depends(get_current_user)):
    _require_approve(user)
    run = _get_run_or_404(run_id)
    if run.get("status") not in ("approved", "reported"):
        raise HTTPException(409, "Run skal godkendes før rapporten kan genereres")
    from moduler.modul_admin_nysalg.brands import EXCLUDED_BRANDS
    scope = repo.run_scope(run)
    matches = _visible_matches(run)
    brand_comments = repo.get_brand_comments(run_id)
    date_from, date_to = repo.run_date_range(run)
    budgets = repo.brand_budgets(date_from, date_to) if scope != "monitor" else {}
    brand_rows = _scope_brand_rows(scope, matches, date_from, date_to,
                                   brand_comments, budgets)
    months_breakdown = _months_breakdown(matches, date_from, date_to, brand_comments,
                                         scope=scope)
    # Skjulte brands pilles helt ud af rapporten (tabel, måneds-opdeling, top-tal).
    hidden = repo.get_hidden_brands(run_id)
    summary, brand_rows, months_breakdown = _apply_hidden(
        matches, brand_rows, months_breakdown, hidden)
    # Afstemningsfanen (PipeDrive deals) følger scopet: Monitor-rapporten viser
    # kun Monitor-deals; Business Media alt undtagen Monitor.
    if scope == "monitor":
        pd_deals = [d for d in repo.period_pipedrive_deals(date_from, date_to)
                    if d.get("brand") == "Monitor"]
    else:
        pd_deals = [d for d in repo.period_pipedrive_deals(date_from, date_to)
                    if d.get("brand") not in EXCLUDED_BRANDS]
    org_names = repo.pipedrive_org_names()
    # Niche-opdeling (WM DK + WM NO) til Excel-fanen — kun Business Media (Monitor-
    # rapportens hovedtabel ER allerede pr. site) og kun ikke-skjulte brands.
    site_rows = []
    if scope != "monitor":
        site_brands = tuple(b for b in ("Watch DK", "Watch NO") if b not in hidden)
        site_rows = repo.summarize_by_site(matches, site_brands) if site_brands else []
    try:
        xlsx_path = report.generate_excel(run, matches, summary, brand_rows,
                                          pd_deals=pd_deals, org_names=org_names,
                                          months_breakdown=months_breakdown,
                                          site_rows=site_rows)
        try:
            report.generate_pdf(run, matches, summary, brand_rows,
                                 months_breakdown=months_breakdown)
        except Exception:
            logger.exception("PDF-generering fejlede (run %s) — Excel blev gemt", run_id)
    except Exception:
        logger.exception("Rapportgenerering fejlede (run %s)", run_id)
        raise HTTPException(500, "Rapporten kunne ikke genereres")
    repo.set_report_path(run_id, xlsx_path)
    audit_log("admin_nysalg_rapport", user=user, run_id=run_id)
    return JSONResponse({"ok": True})


@router.get("/{run_id}/download")
def download(run_id: int, fmt: str = "xlsx", user=Depends(get_current_user)):
    _require_view(user)
    run = _get_run_or_404(run_id)
    if not run.get("report_path"):
        raise HTTPException(404, "Ingen rapport genereret endnu")
    base, _ = os.path.splitext(run["report_path"])
    fmt = "pdf" if fmt == "pdf" else "xlsx"
    path = base + ("." + fmt)
    if not os.path.exists(path):
        raise HTTPException(404, f"Rapportfilen ({fmt}) findes ikke")
    media = ("application/pdf" if fmt == "pdf"
             else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    return FileResponse(path, media_type=media, filename=os.path.basename(path))

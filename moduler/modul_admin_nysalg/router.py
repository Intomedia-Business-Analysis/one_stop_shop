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

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
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


def _months_breakdown(matches: list, date_from, date_to, comments: dict) -> list[dict]:
    """[{ym, label, rows}] pr. måned i intervallet (til review + rapport)."""
    by_month = repo.brand_rows_by_month(matches, date_from, date_to, comments)
    return [{"ym": ym, "label": _month_label(ym), "rows": rows}
            for ym, rows in by_month.items()]


def _match_brand(m: dict) -> str:
    """Brand-label for en Zuora-match-række (samme logik som summarize_by_brand)."""
    from moduler.modul_admin_nysalg.brands import classify
    return m.get("brand") or classify(m.get("site"))


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

@router.get("/", response_class=HTMLResponse)
async def index(request: Request, user=Depends(get_current_user)):
    _require_view(user)
    runs = repo.list_runs(100)
    return templates.TemplateResponse(request, "admin_nysalg_index.html", {
        "user": user,
        "runs": runs,
        "can_approve": has_access(user, APPROVE_MIN_ROLE),
        "is_admin": has_access(user, ADMIN_MIN_ROLE),
    })


@router.post("/{run_id}/delete")
async def delete_run(run_id: int, user=Depends(get_current_user)):
    _require_admin(user)
    if not repo.delete_run(run_id):
        raise HTTPException(404, "Run ikke fundet")
    audit_log("admin_nysalg_slettet", user=user, run_id=run_id)
    return JSONResponse({"ok": True})


@router.get("/new", response_class=HTMLResponse)
async def new_run(request: Request, user=Depends(get_current_user)):
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
                date_from, date_to, period_label) -> None:
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
                                 date_from, date_to)

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
    user=Depends(get_current_user),
):
    """Start matchningen som baggrundsjob. Returnerer {job_id}; frontend poller
    /run-status for progress og redirecter til review når jobbet er færdigt."""
    _require_view(user)

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
              date_from, date_to, period_label),
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
async def review(run_id: int, request: Request, user=Depends(get_current_user)):
    _require_view(user)
    run = _get_run_or_404(run_id)
    matches = repo.get_matches(run_id)
    date_from, date_to = repo.run_date_range(run)
    budgets = repo.brand_budgets(date_from, date_to)
    brand_comments = repo.get_brand_comments(run_id)
    pd_rows = repo.pipedrive_brand_rows(date_from, date_to, brand_comments, budgets)
    brand_rows = repo.summarize_by_brand(matches, budgets, brand_comments, extra_rows=pd_rows)
    months_breakdown = _months_breakdown(matches, date_from, date_to, brand_comments)
    admin_rows = [m for m in matches if repo.effective_is_admin(m)]
    # Brand-tabellen viser ALLE brands (også skjulte, så de kan klikkes tilbage),
    # men topkort + måneds-opdeling afspejler skjulningen.
    hidden = repo.get_hidden_brands(run_id)
    summary, _, months_breakdown = _apply_hidden(matches, brand_rows, months_breakdown, hidden)
    return templates.TemplateResponse(request, "admin_nysalg_review.html", {
        "user": user,
        "run": run,
        "summary": summary,
        "brand_rows": brand_rows,
        "hidden_brands": sorted(hidden),
        "months_breakdown": months_breakdown,
        "admin_rows": admin_rows,
        "can_approve": has_access(user, APPROVE_MIN_ROLE),
        "locked": run.get("status") in ("approved", "reported"),
    })


@router.post("/{run_id}/comment")
async def save_comment(run_id: int, request: Request, user=Depends(get_current_user)):
    _require_view(user)
    _get_run_or_404(run_id)
    body = await request.json()
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
async def set_override(run_id: int, request: Request, user=Depends(get_current_user)):
    _require_view(user)
    run = _get_run_or_404(run_id)
    if run.get("status") in ("approved", "reported"):
        raise HTTPException(409, "Run er låst — override kan ikke ændres")
    body = await request.json()
    match_id = body.get("match_id")
    override = body.get("override")
    if override in ("", "default", None):
        override = None
    repo.set_override(run_id, int(match_id), override)
    # Returnér opdaterede topkort-tal, så frontend kan opdatere uden reload.
    summary = repo.summarize(repo.get_matches(run_id))
    return JSONResponse({"ok": True, "summary": summary})


@router.post("/{run_id}/brand-visibility")
async def brand_visibility(run_id: int, request: Request, user=Depends(get_current_user)):
    """Klik et brand til/fra rapporten. Skjulte brands fjernes fra brand-tabel,
    måneds-opdeling og top-tallene (både i review og den genererede rapport)."""
    _require_view(user)
    run = _get_run_or_404(run_id)
    if run.get("status") in ("approved", "reported"):
        raise HTTPException(409, "Run er låst — brands kan ikke skjules")
    body = await request.json()
    brand = (body.get("brand") or "").strip()
    if not brand:
        raise HTTPException(400, "brand påkrævet")
    repo.set_brand_hidden(run_id, brand, bool(body.get("hidden")))
    return JSONResponse({"ok": True})


@router.post("/{run_id}/approve")
async def approve(run_id: int, request: Request, user=Depends(get_current_user)):
    _require_approve(user)
    run = _get_run_or_404(run_id)
    # Gem evt. samlet kommentar sendt med godkendelsen.
    try:
        body = await request.json()
    except Exception:
        body = {}
    if "director_comment" in body:
        repo.set_director_comment(run_id, (body.get("director_comment") or "").strip())
    repo.approve_run(run_id, user.get("name"))
    audit_log("admin_nysalg_godkendt", user=user, run_id=run_id)
    return JSONResponse({"ok": True})


# ── Rapport ──────────────────────────────────────────────────────────────────

@router.post("/{run_id}/report")
async def make_report(run_id: int, request: Request, user=Depends(get_current_user)):
    _require_approve(user)
    run = _get_run_or_404(run_id)
    if run.get("status") not in ("approved", "reported"):
        raise HTTPException(409, "Run skal godkendes før rapporten kan genereres")
    matches = repo.get_matches(run_id)
    brand_comments = repo.get_brand_comments(run_id)
    date_from, date_to = repo.run_date_range(run)
    budgets = repo.brand_budgets(date_from, date_to)
    brand_rows = repo.summarize_by_brand(
        matches, budgets, brand_comments,
        extra_rows=repo.pipedrive_brand_rows(date_from, date_to, brand_comments, budgets))
    months_breakdown = _months_breakdown(matches, date_from, date_to, brand_comments)
    # Skjulte brands pilles helt ud af rapporten (tabel, måneds-opdeling, top-tal).
    hidden = repo.get_hidden_brands(run_id)
    summary, brand_rows, months_breakdown = _apply_hidden(
        matches, brand_rows, months_breakdown, hidden)
    pd_deals = repo.period_pipedrive_deals(date_from, date_to)
    org_names = repo.pipedrive_org_names()
    try:
        xlsx_path = report.generate_excel(run, matches, summary, brand_rows,
                                          pd_deals=pd_deals, org_names=org_names,
                                          months_breakdown=months_breakdown)
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
async def download(run_id: int, fmt: str = "xlsx", user=Depends(get_current_user)):
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

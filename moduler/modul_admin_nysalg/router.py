"""FastAPI-routes for værktøjet til matchning af administrative nysalg.

Flow: vælg/upload udtræk → match mod administrative PipeDrive-deals → review
(kommentér + override) → direktøren godkender → rapport (Excel/PDF) genereres og
kan downloades.

Adgang: visning/oprettelse kræver sales_operations+; godkendelse og rapport
kræver management+ (direktør-niveau) — admin bypasser begge via rang.
"""
import logging
import os

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from auth import get_current_user, has_access
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


@router.post("/run")
async def run_match(
    request: Request,
    file: UploadFile = File(None),
    source_path: str = Form(""),
    period: str = Form(""),
    user=Depends(get_current_user),
):
    _require_view(user)
    period = (period or "").strip() or None
    src_path = None
    src_name = None
    try:
        if file is not None and file.filename:
            data = await file.read()
            rows_all = extract_loader.load_extract(file_bytes=data, filename=file.filename)
            src_name = file.filename
        else:
            path = (source_path or "").strip() or _default_extract_path()
            if not path:
                raise ExtractError("Vælg en fil at uploade, eller angiv en sti til udtrækket.")
            rows_all = extract_loader.load_extract(path=path)
            src_path = path
            src_name = os.path.basename(path)
    except ExtractError as e:
        return templates.TemplateResponse(request, "admin_nysalg_new.html", {
            "user": user, "default_path": _default_extract_path(), "error": str(e),
        }, status_code=400)

    rows = extract_loader.filter_period(rows_all, period)
    if not rows:
        return templates.TemplateResponse(request, "admin_nysalg_new.html", {
            "user": user, "default_path": _default_extract_path(),
            "error": f"Ingen rækker i udtrækket for perioden {period}.",
        }, status_code=400)

    # Hent administrative deals og match KUN nysalgssiden (pos-fortegn).
    try:
        source = get_default_source()
        deals = source.fetch_admin_deals(period)
    except Exception:
        logger.exception("Kunne ikke hente administrative deals")
        raise HTTPException(500, "Kunne ikke hente administrative deals fra PipeDrive-kilden")

    site_map = repo.load_site_map()
    idx, dups = build_index(deals, site_map)
    match_rows(rows, idx, dups, site_map)

    # Brand-gruppér hver række (Watch/Finans/Monitor/Norge/SE/DE/Marketwire).
    for r in rows:
        r.brand = classify(r.site)

    run_id = repo.create_run(user.get("name"), src_path, src_name, period)
    repo.insert_matches(run_id, rows)
    repo.update_status(run_id, "in_review")
    audit_log("admin_nysalg_run", user=user, run_id=run_id, periode=period or "alle",
              raekker=len(rows), deals=len(deals))
    return RedirectResponse(f"/tools/admin-nysalg/{run_id}/review", status_code=302)


# ── Review ───────────────────────────────────────────────────────────────────

@router.get("/{run_id}/review", response_class=HTMLResponse)
async def review(run_id: int, request: Request, user=Depends(get_current_user)):
    _require_view(user)
    run = _get_run_or_404(run_id)
    matches = repo.get_matches(run_id)
    summary = repo.summarize(matches)
    budgets = repo.brand_budgets(run.get("period"))
    brand_comments = repo.get_brand_comments(run_id)
    pd_rows = repo.pipedrive_brand_rows(run.get("period"), brand_comments, budgets)
    brand_rows = repo.summarize_by_brand(matches, budgets, brand_comments, extra_rows=pd_rows)
    admin_rows = [m for m in matches if repo.effective_is_admin(m)]
    ambiguous_rows = [m for m in matches if m.get("ambiguous")]
    return templates.TemplateResponse(request, "admin_nysalg_review.html", {
        "user": user,
        "run": run,
        "summary": summary,
        "brand_rows": brand_rows,
        "admin_rows": admin_rows,
        "ambiguous_rows": ambiguous_rows,
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
    summary = repo.summarize(matches)
    brand_comments = repo.get_brand_comments(run_id)
    budgets = repo.brand_budgets(run.get("period"))
    brand_rows = repo.summarize_by_brand(
        matches, budgets, brand_comments,
        extra_rows=repo.pipedrive_brand_rows(run.get("period"), brand_comments, budgets))
    pd_deals = repo.period_pipedrive_deals(run.get("period"))
    try:
        xlsx_path = report.generate_excel(run, matches, summary, brand_rows, pd_deals=pd_deals)
        try:
            report.generate_pdf(run, matches, summary, brand_rows)
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

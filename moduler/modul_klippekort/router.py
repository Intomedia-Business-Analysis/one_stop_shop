import threading
import traceback

from fastapi import APIRouter, Body, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from auth import get_current_user, has_access
from moduler.modul_klippekort.queries import (
    db_forbrug_for_deal,
    db_oekonomi,
    db_org_owner_meta,
    db_overblik,
    db_rediger_job,
    db_registrer_forbrug,
    db_slet_job,
    db_udloebende_jobs,
    get_site_groups,
    init_klippekort_db,
    refresh_org_owners,
)
from moduler.modul_klippekort.pipedrive_api import add_used_clip_cards

router = APIRouter(prefix="/tools/klippekort", tags=["Klippekort"])
templates = Jinja2Templates(directory="templates")
from nav_utils import register_nav_globals
register_nav_globals(templates)
init_klippekort_db()

# Org-ejer cache opdateres i baggrunden (org-ejer er ikke et deal-felt i
# PipedriveDeals). Trigges ved sidevisning hvis cachen er tom/forældet.
_ORG_REFRESH_LOCK = threading.Lock()
_ORG_REFRESHING = {"running": False}
_ORG_STALE_HOURS = 24


def _maybe_refresh_pd_cache():
    meta = db_org_owner_meta()
    if meta["count"] > 0 and meta["alder_timer"] < _ORG_STALE_HOURS:
        return
    with _ORG_REFRESH_LOCK:
        if _ORG_REFRESHING["running"]:
            return
        _ORG_REFRESHING["running"] = True

    def _worker():
        try:
            refresh_org_owners()
        except Exception:
            traceback.print_exc()
        finally:
            _ORG_REFRESHING["running"] = False

    threading.Thread(target=_worker, daemon=True).start()


@router.get("/", response_class=HTMLResponse)
async def klippekort_page(request: Request, user=Depends(get_current_user)):
    if not has_access(user, "salesperson"):
        raise HTTPException(403, "Ingen adgang")
    return templates.TemplateResponse(request, "klippekort_overblik.html", {
        "user": user,
    })


@router.get("/overblik")
async def klippekort_overblik(mine: int = 0, status: str = "aktive", user=Depends(get_current_user)):
    if not has_access(user, "salesperson"):
        raise HTTPException(403, "Ingen adgang")
    if status not in ("aktive", "udloebne"):
        status = "aktive"
    _maybe_refresh_pd_cache()
    try:
        owner = user.get("name") if mine else None
        return JSONResponse({"rows": db_overblik(owner, status)})
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@router.get("/udloebende")
async def klippekort_udloebende(mine: int = 0, status: str = "aktive", user=Depends(get_current_user)):
    """Stillinger med slutdato — opfølgnings-oversigt (aktive eller udløbne)."""
    if not has_access(user, "salesperson"):
        raise HTTPException(403, "Ingen adgang")
    if status not in ("aktive", "udloebne"):
        status = "aktive"
    try:
        owner = user.get("name") if mine else None
        return JSONResponse({"rows": db_udloebende_jobs(owner, status)})
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@router.get("/sites")
async def klippekort_sites(user=Depends(get_current_user)):
    """Site-familier (Watch DK / Monitor) til 'opret job'-dropdownen."""
    if not has_access(user, "salesperson"):
        raise HTTPException(403, "Ingen adgang")
    return JSONResponse(get_site_groups())


@router.get("/oekonomi")
async def klippekort_oekonomi(user=Depends(get_current_user)):
    if not has_access(user, "salesperson"):
        raise HTTPException(403, "Ingen adgang")
    try:
        return JSONResponse({"rows": db_oekonomi()})
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@router.get("/forbrug")
async def klippekort_forbrug(pd_deal_id: int, user=Depends(get_current_user)):
    """Hent alle registrerede jobs/klip-forbrug for én deal."""
    if not has_access(user, "salesperson"):
        raise HTTPException(403, "Ingen adgang")
    try:
        return JSONResponse({"rows": db_forbrug_for_deal(pd_deal_id)})
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@router.post("/slet-job")
async def klippekort_slet_job(payload: dict = Body(...), user=Depends(get_current_user)):
    """Slet et helt job (alle dets sites) og opdatér 'klip brugt' i Pipedrive.

    Body: {job_id}
    """
    if not has_access(user, "salesperson"):
        raise HTTPException(403, "Ingen adgang")
    job_id = (payload.get("job_id") or "").strip()
    if not job_id:
        raise HTTPException(400, "job_id er påkrævet")

    res = db_slet_job(job_id)
    if not res.get("ok"):
        raise HTTPException(400, res.get("error", "Kunne ikke slette job"))

    # Additivt: træk de fjernede klip fra Pipedrives autoritative used_clip_cards
    # (delta er negativ), så vi ikke overskriver klip toolet ikke selv har registreret.
    pd = add_used_clip_cards(res["pd_deal_id"], res["delta"])
    return JSONResponse({"ok": True, "brugt": res["brugt"], "pipedrive": pd})


@router.post("/rediger-job")
async def klippekort_rediger_job(payload: dict = Body(...), user=Depends(get_current_user)):
    """Redigér et aktivt stillingsopslag: forlæng perioden og/eller giv effektgaranti.

    Effektgaranti = aftalt med kunden hvor lang tid ekstra stillingen kører —
    derfor skal der altid en (ny) slutdato med.
    Body: {job_id, slutdato (YYYY-MM-DD), effektgaranti}
    """
    if not has_access(user, "salesperson"):
        raise HTTPException(403, "Ingen adgang")
    job_id = (payload.get("job_id") or "").strip()
    slutdato = (payload.get("slutdato") or "").strip()
    effektgaranti = bool(payload.get("effektgaranti"))
    if not job_id:
        raise HTTPException(400, "job_id er påkrævet")
    if not slutdato:
        raise HTTPException(400, "slutdato er påkrævet")

    res = db_rediger_job(job_id, slutdato, effektgaranti)
    if not res.get("ok"):
        raise HTTPException(400, res.get("error", "Kunne ikke opdatere job"))
    return JSONResponse({"ok": True})


@router.post("/opret-job")
async def klippekort_opret_job(payload: dict = Body(...), user=Depends(get_current_user)):
    """Registrér ét job (kan dække flere sites og koste flere klip) + push til Pipedrive.

    Body: {pd_deal_id, sites: [..], stilling, tidspunkt (YYYY-MM-DD), klip}
    """
    if not has_access(user, "salesperson"):
        raise HTTPException(403, "Ingen adgang")

    try:
        pd_deal_id = int(payload.get("pd_deal_id"))
    except (TypeError, ValueError):
        raise HTTPException(400, "pd_deal_id skal være et heltal")
    sites = payload.get("sites")
    if isinstance(sites, str):
        sites = [sites]
    sites = [s.strip() for s in (sites or []) if s and s.strip()]
    stilling  = (payload.get("stilling") or "").strip()
    tidspunkt = (payload.get("tidspunkt") or "").strip()
    slutdato  = (payload.get("slutdato") or "").strip() or None
    effektgaranti = bool(payload.get("effektgaranti"))
    try:
        klip = int(payload.get("klip", 1))
    except (TypeError, ValueError):
        klip = 1
    if not sites or not stilling or not tidspunkt:
        raise HTTPException(400, "mindst ét site, stilling og tidspunkt er påkrævet")
    if klip < 1:
        raise HTTPException(400, "antal klip skal være mindst 1")

    try:
        res = db_registrer_forbrug(pd_deal_id, sites, stilling, tidspunkt, klip,
                                   user.get("name") or "system", slutdato=slutdato,
                                   effektgaranti=effektgaranti)
        if not res.get("ok"):
            raise HTTPException(400, res.get("error", "Kunne ikke registrere job"))

        # Additivt: læg de nyligt registrerede klip oveni Pipedrives autoritative
        # used_clip_cards (delta = +klip) i stedet for at overskrive med toolets sum.
        pd = add_used_clip_cards(pd_deal_id, res["delta"])
        return JSONResponse({
            "ok":        True,
            "brugt":     res["brugt"],
            "rest":      res["rest"],
            "pipedrive": pd,
        })
    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))

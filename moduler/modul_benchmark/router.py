"""Medie Benchmark — routes.

Adgang: 'management' (rang 5) og derover, dvs. ledelse + admin. Kravet står både
her og på nav-item'et i nav_utils.py — sættes det lavere i nav'en end her, får
brugeren et menupunkt der svarer 403.
"""
import logging

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from auth import get_current_user, has_access
from moduler.modul_benchmark.queries import (
    SERIES_MAX,
    db_compare,
    db_filter_options,
    db_first_activity,
    db_series_deals,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tools/benchmark", tags=["Benchmark"])
templates = Jinja2Templates(directory="templates")
from nav_utils import register_nav_globals  # noqa: E402
register_nav_globals(templates)

MIN_ROLLE = "management"


def _require_access(user: dict) -> None:
    if not has_access(user, MIN_ROLLE):
        raise HTTPException(403, "Kræver Management-adgang")


def _norm(v: str | None) -> str | None:
    if v is None:
        return None
    s = v.strip()
    return s or None


def _parse_series(s_sites: list[str] | None,
                  s_start: list[str] | None,
                  s_label: list[str] | None) -> list[dict]:
    """Sæt serier sammen af tre parallelle query-lister, parret på position.

    En serie kan pege på flere sites, så listerne indeholder komma-joinede
    site-navne (`s_sites=DetailWatch DK,KForum`). Det er sikkert, fordi
    [sites]-kolonnen selv er kommasepareret — et site-navn kan derfor aldrig
    indeholde et komma.

    Parallelle lister frem for JSON i en POST: så kan eksport-endpointet rammes
    med et almindeligt link, og filtrene kan deles som URL.
    """
    sites_raw = s_sites or []
    starts    = s_start or []
    labels    = s_label or []
    if not sites_raw:
        raise ValueError("Vælg mindst ét medie at sammenligne")
    if len(starts) != len(sites_raw):
        raise ValueError("Hver serie skal have en startdato")
    if len(sites_raw) > SERIES_MAX:
        raise ValueError(f"Højst {SERIES_MAX} medier ad gangen")
    return [
        {
            "sites":      [p.strip() for p in sites_raw[i].split(",") if p.strip()],
            "start_date": starts[i],
            "label":      labels[i] if i < len(labels) else None,
        }
        for i in range(len(sites_raw))
    ]


@router.get("/medier", response_class=HTMLResponse)
async def benchmark_medier_page(request: Request, user=Depends(get_current_user)):
    _require_access(user)
    return templates.TemplateResponse(request, "benchmark_medier.html", {
        "user":       user,
        "series_max": SERIES_MAX,
    })


@router.get("/filters")
async def benchmark_filters(user=Depends(get_current_user)):
    _require_access(user)
    try:
        return JSONResponse(db_filter_options())
    except Exception:
        logger.exception("benchmark_filters fejlede")
        raise HTTPException(500, "Data kunne ikke hentes")


@router.get("/first-activity")
async def benchmark_first_activity(
    site: list[str] = Query(...),
    date_basis: str | None = None,
    pipeline: list[str] | None = Query(default=None),
    status: list[str] | None = Query(default=None),
    exclude_adm: bool = True,
    exclude_test: bool = True,
    user=Depends(get_current_user),
):
    """Foreslå en launchdato: første dato med aktivitet på det valgte medie.

    QA-deals frasorteres som standard — ellers kunne forslaget pege på en
    testdeal oprettet længe før mediet gik live.
    """
    _require_access(user)
    try:
        return JSONResponse(db_first_activity(
            sites=site,
            date_basis=_norm(date_basis),
            pipelines=pipeline,
            statuses=status,
            exclude_adm=exclude_adm,
            exclude_test=exclude_test,
        ))
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception:
        logger.exception("benchmark_first_activity fejlede")
        raise HTTPException(500, "Data kunne ikke hentes")


@router.get("/compare")
async def benchmark_compare(
    s_sites: list[str] = Query(...),
    s_start: list[str] = Query(...),
    s_label: list[str] | None = Query(default=None),
    date_basis: str | None = None,
    pipeline: list[str] | None = Query(default=None),
    status: list[str] | None = Query(default=None),
    bucket: str | None = None,
    window_days: int | None = None,
    exclude_adm: bool = True,
    exclude_test: bool = True,
    full_history: bool = False,
    user=Depends(get_current_user),
):
    _require_access(user)
    try:
        return JSONResponse(db_compare(
            series=_parse_series(s_sites, s_start, s_label),
            date_basis=_norm(date_basis),
            pipelines=pipeline,
            statuses=status,
            bucket=_norm(bucket),
            window_days=window_days,
            exclude_adm=exclude_adm,
            exclude_test=exclude_test,
            full_history=full_history,
        ))
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception:
        logger.exception("benchmark_compare fejlede")
        raise HTTPException(500, "Data kunne ikke hentes")


@router.get("/deals")
async def benchmark_deals(
    site: list[str] = Query(...),
    start_date: str = Query(...),
    window_days: int = Query(...),
    date_basis: str | None = None,
    pipeline: list[str] | None = Query(default=None),
    status: list[str] | None = Query(default=None),
    exclude_adm: bool = True,
    web_sale: str | None = None,
    test_persons: str | None = "exclude",
    user=Depends(get_current_user),
):
    """Rå deals for én serie — drill-down-modal og Excel-eksport."""
    _require_access(user)
    try:
        return JSONResponse(db_series_deals(
            sites=site,
            start_date=start_date,
            window_days=window_days,
            date_basis=_norm(date_basis),
            pipelines=pipeline,
            statuses=status,
            exclude_adm=exclude_adm,
            web_sale=_norm(web_sale),
            test_persons=_norm(test_persons),
        ))
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception:
        logger.exception("benchmark_deals fejlede")
        raise HTTPException(500, "Data kunne ikke hentes")

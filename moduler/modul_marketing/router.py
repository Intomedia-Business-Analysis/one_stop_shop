import traceback

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from auth import get_current_user, has_access
from moduler.modul_marketing.queries import (
    db_account_deals,
    db_by_account,
    db_deals,
    db_filter_options,
    db_summary,
)

router = APIRouter(prefix="/tools/marketing", tags=["Marketing"])
templates = Jinja2Templates(directory="templates")
from nav_utils import register_nav_globals
register_nav_globals(templates)


def _require_access(user: dict) -> None:
    if not has_access(user, "marketing"):
        raise HTTPException(403, "Kræver Marketing-adgang")


def _norm(v: str | None) -> str | None:
    if v is None:
        return None
    s = v.strip()
    return s or None


@router.get("/deal-source", response_class=HTMLResponse)
async def marketing_deal_source_page(request: Request, user=Depends(get_current_user)):
    _require_access(user)
    return templates.TemplateResponse("marketing_deal_source.html", {
        "request": request,
        "user":    user,
    })


@router.get("/filters")
async def marketing_filters(user=Depends(get_current_user)):
    _require_access(user)
    try:
        return JSONResponse(db_filter_options())
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@router.get("/summary")
async def marketing_summary(
    account: list[str] | None = Query(default=None),
    site: list[str] | None = Query(default=None),
    deal_source: list[str] | None = Query(default=None),
    owner: list[str] | None = Query(default=None),
    date_from: str | None = None,
    date_to: str | None = None,
    user=Depends(get_current_user),
):
    _require_access(user)
    try:
        return JSONResponse(db_summary(
            accounts=account,
            sites=site,
            deal_sources=deal_source,
            owners=owner,
            date_from=_norm(date_from),
            date_to=_norm(date_to),
        ))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@router.get("/by-account")
async def marketing_by_account(
    site: list[str] | None = Query(default=None),
    deal_source: list[str] | None = Query(default=None),
    owner: list[str] | None = Query(default=None),
    date_from: str | None = None,
    date_to: str | None = None,
    user=Depends(get_current_user),
):
    """Per-account breakdown. account-filter ignoreres bevidst — det er
    pointen at se kilden på tværs af accounts."""
    _require_access(user)
    try:
        return JSONResponse({"rows": db_by_account(
            sites=site,
            deal_sources=deal_source,
            owners=owner,
            date_from=_norm(date_from),
            date_to=_norm(date_to),
        )})
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@router.get("/account-deals")
async def marketing_account_deals(
    account: str,
    status: str,
    site: list[str] | None = Query(default=None),
    deal_source: list[str] | None = Query(default=None),
    owner: list[str] | None = Query(default=None),
    date_from: str | None = None,
    date_to: str | None = None,
    user=Depends(get_current_user),
):
    """Drill-down for én account+status (modal-data)."""
    _require_access(user)
    try:
        return JSONResponse({"rows": db_account_deals(
            account=account,
            status=status,
            sites=site,
            deal_sources=deal_source,
            owners=owner,
            date_from=_norm(date_from),
            date_to=_norm(date_to),
        )})
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@router.get("/deals")
async def marketing_deals(
    account: list[str] | None = Query(default=None),
    site: list[str] | None = Query(default=None),
    deal_source: list[str] | None = Query(default=None),
    owner: list[str] | None = Query(default=None),
    date_from: str | None = None,
    date_to: str | None = None,
    page: int = 1,
    page_size: int = 50,
    sort_by: str = "service_activation_date",
    sort_dir: str = "desc",
    user=Depends(get_current_user),
):
    _require_access(user)
    try:
        return JSONResponse(db_deals(
            accounts=account,
            sites=site,
            deal_sources=deal_source,
            owners=owner,
            date_from=_norm(date_from),
            date_to=_norm(date_to),
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_dir=sort_dir,
        ))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))



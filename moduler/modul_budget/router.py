import json
import io
import logging
from datetime import date

import pandas as pd
from fastapi import APIRouter, Depends, Request, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from auth import ROLE_LABELS, allowed_data_teams, get_current_user, has_access
from log_setup import audit_log
from moduler.modul_budget.queries import (
    db_get_distinct,
    db_medie_upsert_rows, db_medie_upload_df,
    db_saelger_upsert_rows, db_saelger_upload_df,
    db_medie_query, db_saelger_query,
    db_medie_delete, db_medie_update,
    db_budget_scope, db_owners_for_teams, db_medie_get,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tools/budget", tags=["Budget"])
templates = Jinja2Templates(directory="templates")
from nav_utils import register_nav_globals
register_nav_globals(templates)

MONTHS = [
    (1,"Januar"),(2,"Februar"),(3,"Marts"),(4,"April"),
    (5,"Maj"),(6,"Juni"),(7,"Juli"),(8,"August"),
    (9,"September"),(10,"Oktober"),(11,"November"),(12,"December")
]


def require_budget_access(user: dict):
    if not has_access(user, "sales_manager"):
        raise HTTPException(status_code=403, detail="Ingen adgang til Budget Tool")


def _medie_scope(user: dict) -> dict | None:
    """Medie-budget-scope for team-begrænsede brugere. None = ubegrænset."""
    allowed = allowed_data_teams(user)
    if allowed is None:
        return None
    return db_budget_scope(allowed)


def _medie_row_ok(scope: dict | None, brand, dealtype) -> bool:
    if scope is None:
        return True
    return brand in scope["brands"] or dealtype in scope["dealtypes"]


@router.get("/", response_class=HTMLResponse)
async def budget_tool(request: Request, user=Depends(get_current_user)):
    require_budget_access(user)
    brands     = db_get_distinct("BudgetsIntoMedia", "Brand")
    sp_teams   = db_get_distinct("SalespersonBudget", "Team")
    sp_persons = db_get_distinct("SalespersonBudget", "Owner")
    allowed = allowed_data_teams(user)
    if allowed is not None:
        scope = db_budget_scope(allowed)
        brands     = [b for b in brands if b in scope["brands"]]
        sp_teams   = [t for t in sp_teams if t in allowed]
        sp_persons = db_owners_for_teams(allowed)
    return templates.TemplateResponse(request, "budget_tool.html", {
        "user":       user,
        "sites":      db_get_distinct("BudgetsIntoMedia", "Site"),
        "brands":     brands,
        "deal_types": db_get_distinct("BudgetsIntoMedia", "DealType"),
        "salestypes": db_get_distinct("BudgetsIntoMedia", "Salestype"),
        "sp_sites":   db_get_distinct("SalespersonBudget", "Brand"),
        "sp_teams":   sp_teams,
        "sp_persons": sp_persons,
        "months":       MONTHS,
        "years":        list(range(date.today().year - 1, date.today().year + 3)),
        "current_year": date.today().year,
    })


@router.post("/medie/insert")
async def medie_insert(
    site:        str = Form(...),
    brand:       str = Form(...),
    deal_type:   str = Form(...),
    salestype:   str = Form(...),
    year:        int = Form(...),
    months_data: str = Form(...),
    user=Depends(get_current_user),
):
    require_budget_access(user)
    if not _medie_row_ok(_medie_scope(user), brand, deal_type):
        raise HTTPException(403, "Ingen adgang til at redigere budget for dette brand")
    try:
        rows = json.loads(months_data)
        inserted = db_medie_upsert_rows(site, brand, deal_type, salestype, year, rows)
        audit_log("budget_medie_indsat", user=user, site=site, brand=brand,
                  dealtype=deal_type, aar=year, raekker=inserted)
        return JSONResponse({"status": "ok", "inserted": inserted})
    except Exception:
        logger.exception("medie_insert fejlede (brand=%s, year=%s)", brand, year)
        raise HTTPException(status_code=500, detail="Budgettet kunne ikke gemmes")


@router.post("/medie/upload")
async def medie_upload(file: UploadFile = File(...), user=Depends(get_current_user)):
    require_budget_access(user)
    if not file.filename.endswith((".xlsx", ".xls", ".csv")):
        raise HTTPException(400, "Kun .xlsx, .xls eller .csv filer er tilladt")
    content = await file.read()
    try:
        df = pd.read_csv(io.BytesIO(content)) if file.filename.endswith(".csv") else pd.read_excel(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(400, f"Kunne ikke læse filen: {e}")

    missing = {"Site", "Brand", "DealType", "Salestype", "BudgetDate", "BudgetAmount"} - set(df.columns)
    if missing:
        raise HTTPException(400, f"Mangler kolonner: {', '.join(missing)}")

    scope = _medie_scope(user)
    if scope is not None:
        outside = sorted({
            str(r["Brand"]) for _, r in df.iterrows()
            if not _medie_row_ok(scope, str(r["Brand"]), str(r["DealType"]))
        })
        if outside:
            raise HTTPException(403, f"Filen indeholder brands uden for din team-adgang: {', '.join(outside)}")

    try:
        inserted, errors, error_rows = db_medie_upload_df(df)
    except Exception:
        logger.exception("medie_upload fejlede (fil=%s)", file.filename)
        raise HTTPException(500, "Filen kunne ikke importeres")

    audit_log("budget_medie_upload", user=user, fil=file.filename,
              raekker=inserted, fejl=errors)
    return JSONResponse({"status": "ok", "inserted": inserted, "errors": errors, "error_rows": error_rows[:10]})


@router.post("/saelger/insert")
async def saelger_insert(
    salesperson: str = Form(...),
    site:        str = Form(...),
    team:        str = Form(...),
    year:        int = Form(...),
    months_data: str = Form(...),
    user=Depends(get_current_user),
):
    require_budget_access(user)
    allowed = allowed_data_teams(user)
    if allowed is not None and team not in allowed:
        raise HTTPException(403, "Ingen adgang til at redigere dette teams budget")
    try:
        rows = json.loads(months_data)
        inserted = db_saelger_upsert_rows(salesperson, site, team, year, rows)
        audit_log("budget_saelger_indsat", user=user, saelger=salesperson,
                  team=team, aar=year, raekker=inserted)
        return JSONResponse({"status": "ok", "inserted": inserted})
    except Exception:
        logger.exception("saelger_insert fejlede (saelger=%s, year=%s)", salesperson, year)
        raise HTTPException(status_code=500, detail="Budgettet kunne ikke gemmes")


@router.post("/saelger/upload")
async def saelger_upload(file: UploadFile = File(...), user=Depends(get_current_user)):
    require_budget_access(user)
    if not file.filename.endswith((".xlsx", ".xls", ".csv")):
        raise HTTPException(400, "Kun .xlsx, .xls eller .csv filer er tilladt")
    content = await file.read()
    try:
        df = pd.read_csv(io.BytesIO(content)) if file.filename.endswith(".csv") else pd.read_excel(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(400, f"Kunne ikke læse filen: {e}")

    missing = {"Owner", "Brand", "Team", "BudgetDate", "BudgetAmount"} - set(df.columns)
    if missing:
        raise HTTPException(400, f"Mangler kolonner: {', '.join(missing)}")

    allowed = allowed_data_teams(user)
    if allowed is not None:
        outside = sorted({str(t) for t in df["Team"].unique() if str(t) not in allowed})
        if outside:
            raise HTTPException(403, f"Filen indeholder teams uden for din team-adgang: {', '.join(outside)}")

    try:
        inserted, errors, error_rows = db_saelger_upload_df(df)
    except Exception:
        logger.exception("saelger_upload fejlede (fil=%s)", file.filename)
        raise HTTPException(500, "Filen kunne ikke importeres")

    audit_log("budget_saelger_upload", user=user, fil=file.filename,
              raekker=inserted, fejl=errors)

    return JSONResponse({"status": "ok", "inserted": inserted, "errors": errors, "error_rows": error_rows[:10]})


@router.get("/medie/data")
async def medie_data(
    year: int = None, month: int = None, site: str = None,
    brand: str = None, dealtype: str = None, salestype: str = None,
    user=Depends(get_current_user),
):
    require_budget_access(user)
    try:
        rows = db_medie_query(year, month, site, brand, dealtype, salestype)
        scope = _medie_scope(user)
        if scope is not None:
            rows = [r for r in rows if _medie_row_ok(scope, r["Brand"], r["DealType"])]
        monthly = {}
        for r in rows:
            m = int(r["Måned"])
            monthly[m] = monthly.get(m, 0) + float(r["BudgetAmount"] or 0)
        chart = [{"måned": m, "budget": round(monthly.get(m, 0))} for m in range(1, 13)]
        return JSONResponse({"rows": rows, "chart": chart, "total": sum(monthly.values())})
    except Exception:
        logger.exception("medie_data fejlede")
        raise HTTPException(500, "Data kunne ikke hentes")


@router.get("/saelger/data")
async def saelger_data(
    year: int = None, month: int = None, site: str = None,
    team: str = None, salesperson: str = None,
    user=Depends(get_current_user),
):
    require_budget_access(user)
    allowed = allowed_data_teams(user)
    if allowed is not None and team and team not in allowed:
        raise HTTPException(403, "Ingen adgang til dette teams budget")
    try:
        rows = db_saelger_query(year, month, site, team, salesperson)
        if allowed is not None:
            rows = [r for r in rows if r["Team"] in allowed]
        monthly = {}
        for r in rows:
            m = int(r["Måned"])
            monthly[m] = monthly.get(m, 0) + float(r["BudgetAmount"] or 0)
        chart = [{"måned": m, "budget": round(monthly.get(m, 0))} for m in range(1, 13)]
        return JSONResponse({"rows": rows, "chart": chart, "total": sum(monthly.values())})
    except Exception:
        logger.exception("saelger_data fejlede")
        raise HTTPException(500, "Data kunne ikke hentes")


@router.delete("/medie/delete/{row_id}")
async def medie_delete(row_id: int, user=Depends(get_current_user)):
    require_budget_access(user)
    scope = _medie_scope(user)
    if scope is not None:
        row = db_medie_get(row_id)
        if not row or not _medie_row_ok(scope, row["Brand"], row["DealType"]):
            raise HTTPException(403, "Ingen adgang til at slette denne budgetrække")
    try:
        db_medie_delete(row_id)
        audit_log("budget_medie_slettet", user=user, row_id=row_id)
        return JSONResponse({"status": "ok"})
    except Exception:
        logger.exception("medie_delete fejlede (row_id=%s)", row_id)
        raise HTTPException(500, "Budgetrækken kunne ikke slettes")


@router.put("/medie/update/{row_id}")
async def medie_update(
    row_id:    int,
    site:      str   = Form(...),
    brand:     str   = Form(...),
    dealtype:  str   = Form(...),
    salestype: str   = Form(...),
    year:      int   = Form(...),
    month:     int   = Form(...),
    amount:    float = Form(...),
    user=Depends(get_current_user),
):
    require_budget_access(user)
    scope = _medie_scope(user)
    if scope is not None:
        # Både den eksisterende række og de nye værdier skal være i scope
        row = db_medie_get(row_id)
        if not row or not _medie_row_ok(scope, row["Brand"], row["DealType"]) \
                or not _medie_row_ok(scope, brand, dealtype):
            raise HTTPException(403, "Ingen adgang til at redigere denne budgetrække")
    try:
        db_medie_update(row_id, site, brand, dealtype, salestype, year, month, amount)
        audit_log("budget_medie_opdateret", user=user, row_id=row_id, brand=brand,
                  dealtype=dealtype, aar=year, maaned=month, beloeb=amount)
        return JSONResponse({"status": "ok"})
    except Exception:
        logger.exception("medie_update fejlede (row_id=%s)", row_id)
        raise HTTPException(500, "Budgetrækken kunne ikke opdateres")


@router.get("/medie/template")
async def medie_template(user=Depends(get_current_user)):
    require_budget_access(user)
    df = pd.DataFrame(columns=["DealType","Site","BudgetDate","BudgetAmount","Brand","Salestype"])
    df.loc[0] = ["Job","FinansWatch DK","2025-01-01","500000","Watch DK","Business"]
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)
    return StreamingResponse(buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=medie_budget_template.xlsx"})


@router.get("/saelger/template")
async def saelger_template(user=Depends(get_current_user)):
    require_budget_access(user)
    today = date.today()
    year = today.year
    rows = [
        ["Michael Toft", "FINANS DK", "Team FINANS Int", f"{year}-{m:02d}-01", 31000]
        for m in range(1, 13)
    ]
    df = pd.DataFrame(rows, columns=["Owner", "Brand", "Team", "BudgetDate", "BudgetAmount"])
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)
    return StreamingResponse(buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=saelger_budget_template.xlsx"})
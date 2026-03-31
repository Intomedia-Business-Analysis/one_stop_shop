import json
import io
import traceback
from datetime import date

import pandas as pd
from fastapi import APIRouter, Depends, Request, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from auth import ROLE_LABELS, get_current_user
from moduler.modul_budget.queries import (
    db_get_distinct,
    db_medie_upsert_rows, db_medie_upload_df,
    db_saelger_upsert_rows, db_saelger_upload_df,
    db_medie_query, db_saelger_query,
    db_medie_delete, db_medie_update,
)

router = APIRouter(prefix="/tools/budget", tags=["Budget"])
templates = Jinja2Templates(directory="templates")
templates.env.globals["ROLE_LABELS"] = ROLE_LABELS

MONTHS = [
    (1,"Januar"),(2,"Februar"),(3,"Marts"),(4,"April"),
    (5,"Maj"),(6,"Juni"),(7,"Juli"),(8,"August"),
    (9,"September"),(10,"Oktober"),(11,"November"),(12,"December")
]


@router.get("/", response_class=HTMLResponse)
async def budget_tool(request: Request, user=Depends(get_current_user)):
    return templates.TemplateResponse("budget_tool.html", {
        "request":    request,
        "user":       user,
        "sites":      db_get_distinct("BudgetsIntoMedia", "Site"),
        "brands":     db_get_distinct("BudgetsIntoMedia", "Brand"),
        "deal_types": db_get_distinct("BudgetsIntoMedia", "DealType"),
        "salestypes": db_get_distinct("BudgetsIntoMedia", "Salestype"),
        "sp_sites":   db_get_distinct("SalespersonBudget", "Brand"),
        "sp_teams":   db_get_distinct("SalespersonBudget", "Team"),
        "sp_persons": db_get_distinct("SalespersonBudget", "Owner"),
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
):
    try:
        rows = json.loads(months_data)
        inserted = db_medie_upsert_rows(site, brand, deal_type, salestype, year, rows)
        return JSONResponse({"status": "ok", "inserted": inserted})
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/medie/upload")
async def medie_upload(file: UploadFile = File(...)):
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

    try:
        inserted, errors, error_rows = db_medie_upload_df(df)
    except Exception as e:
        raise HTTPException(500, str(e))

    return JSONResponse({"status": "ok", "inserted": inserted, "errors": errors, "error_rows": error_rows[:10]})


@router.post("/saelger/insert")
async def saelger_insert(
    salesperson: str = Form(...),
    site:        str = Form(...),
    team:        str = Form(...),
    year:        int = Form(...),
    months_data: str = Form(...),
):
    try:
        rows = json.loads(months_data)
        inserted = db_saelger_upsert_rows(salesperson, site, team, year, rows)
        return JSONResponse({"status": "ok", "inserted": inserted})
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/saelger/upload")
async def saelger_upload(file: UploadFile = File(...)):
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

    try:
        inserted, errors, error_rows = db_saelger_upload_df(df)
    except Exception as e:
        raise HTTPException(500, str(e))

    return JSONResponse({"status": "ok", "inserted": inserted, "errors": errors, "error_rows": error_rows[:10]})


@router.get("/medie/data")
async def medie_data(
    year: int = None, month: int = None, site: str = None,
    brand: str = None, dealtype: str = None, salestype: str = None,
):
    try:
        rows = db_medie_query(year, month, site, brand, dealtype, salestype)
        monthly = {}
        for r in rows:
            m = int(r["Måned"])
            monthly[m] = monthly.get(m, 0) + float(r["BudgetAmount"] or 0)
        chart = [{"måned": m, "budget": round(monthly.get(m, 0))} for m in range(1, 13)]
        return JSONResponse({"rows": rows, "chart": chart, "total": sum(monthly.values())})
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@router.get("/saelger/data")
async def saelger_data(
    year: int = None, month: int = None, site: str = None,
    team: str = None, salesperson: str = None,
):
    try:
        rows = db_saelger_query(year, month, site, team, salesperson)
        monthly = {}
        for r in rows:
            m = int(r["Måned"])
            monthly[m] = monthly.get(m, 0) + float(r["BudgetAmount"] or 0)
        chart = [{"måned": m, "budget": round(monthly.get(m, 0))} for m in range(1, 13)]
        return JSONResponse({"rows": rows, "chart": chart, "total": sum(monthly.values())})
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@router.delete("/medie/delete/{row_id}")
async def medie_delete(row_id: int):
    try:
        db_medie_delete(row_id)
        return JSONResponse({"status": "ok"})
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


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
):
    try:
        db_medie_update(row_id, site, brand, dealtype, salestype, year, month, amount)
        return JSONResponse({"status": "ok"})
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@router.get("/medie/template")
async def medie_template():
    df = pd.DataFrame(columns=["DealType","Site","BudgetDate","BudgetAmount","Brand","Salestype"])
    df.loc[0] = ["Job","FinansWatch DK","2025-01-01","500000","Watch DK","Business"]
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)
    return StreamingResponse(buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=medie_budget_template.xlsx"})


@router.get("/saelger/template")
async def saelger_template():
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
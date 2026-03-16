from fastapi import APIRouter, Request, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import pymssql
import pandas as pd
import io
import os
from datetime import date
from dotenv import load_dotenv

load_dotenv()

router = APIRouter(prefix="/tools/budget", tags=["Budget"])
templates = Jinja2Templates(directory="templates")

# ── DB connection ────────────────────────────────────────────────
def get_conn():
    return pymssql.connect(
        server=os.getenv('DB_SERVER'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME', 'INTOMEDIA'),
        tds_version='7.0',
        login_timeout=5,
        timeout=5
    )

# ── Lookup helpers ───────────────────────────────────────────────
def get_distinct(table: str, column: str) -> list[str]:
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(f"SELECT DISTINCT [{column}] FROM [dbo].[{table}] WHERE [{column}] IS NOT NULL ORDER BY [{column}]")
        rows = [r[0] for r in cur.fetchall()]
        conn.close()
        return rows
    except Exception:
        return []

# ── PAGES ────────────────────────────────────────────────────────
@router.get("/", response_class=HTMLResponse)
async def budget_tool(request: Request):
    sites       = get_distinct("BudgetsIntoMedia", "Site")
    brands      = get_distinct("BudgetsIntoMedia", "Brand")
    deal_types  = get_distinct("BudgetsIntoMedia", "DealType")
    salestypes  = get_distinct("BudgetsIntoMedia", "Salestype")
    sp_sites    = get_distinct("SalesPersonBudget", "Site")
    sp_teams    = get_distinct("SalesPersonBudget", "Team")
    sp_persons  = get_distinct("SalesPersonBudget", "SalesPersonName")
    return templates.TemplateResponse("budget_tool.html", {
        "request":    request,
        "sites":      sites,
        "brands":     brands,
        "deal_types": deal_types,
        "salestypes": salestypes,
        "sp_sites":   sp_sites,
        "sp_teams":   sp_teams,
        "sp_persons": sp_persons,
        "months": [
            (1,"Januar"),(2,"Februar"),(3,"Marts"),(4,"April"),
            (5,"Maj"),(6,"Juni"),(7,"Juli"),(8,"August"),
            (9,"September"),(10,"Oktober"),(11,"November"),(12,"December")
        ],
        "years": list(range(date.today().year - 1, date.today().year + 3)),
        "current_year": date.today().year,
    })

# ── MEDIE: Manuel insert ─────────────────────────────────────────
@router.post("/medie/insert")
async def medie_insert(
    site:         str = Form(...),
    brand:        str = Form(...),
    deal_type:    str = Form(...),
    salestype:    str = Form(...),
    year:         int = Form(...),
    months_data:  str = Form(...),   # JSON: {"1":"100000","2":"200000",...}
):
    import json
    rows = json.loads(months_data)
    inserted = 0
    try:
        conn = get_conn()
        cur = conn.cursor()
        for month_str, amount_str in rows.items():
            amount = float(amount_str) if amount_str else 0.0
            budget_date = date(year, int(month_str), 1)
            # Upsert: delete existing + insert
            cur.execute("""
                DELETE FROM [dbo].[BudgetsIntoMedia]
                WHERE [Site]=? AND [Brand]=? AND [DealType]=? AND [Salestype]=? AND [BudgetDate]=?
            """, site, brand, deal_type, salestype, budget_date)
            cur.execute("""
                INSERT INTO [dbo].[BudgetsIntoMedia] ([DealType],[Site],[BudgetDate],[BudgetAmount],[Brand],[Salestype])
                VALUES (?,?,?,?,?,?)
            """, deal_type, site, budget_date, amount, brand, salestype)
            inserted += 1
        conn.commit()
        conn.close()
        return JSONResponse({"status": "ok", "inserted": inserted})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ── MEDIE: Excel upload ──────────────────────────────────────────
@router.post("/medie/upload")
async def medie_upload(file: UploadFile = File(...)):
    if not file.filename.endswith((".xlsx", ".xls", ".csv")):
        raise HTTPException(400, "Kun .xlsx, .xls eller .csv filer er tilladt")
    content = await file.read()
    try:
        if file.filename.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(content))
        else:
            df = pd.read_excel(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(400, f"Kunne ikke læse filen: {e}")

    # Validate columns
    required = {"Site", "Brand", "DealType", "Salestype", "BudgetDate", "BudgetAmount"}
    missing = required - set(df.columns)
    if missing:
        raise HTTPException(400, f"Mangler kolonner: {', '.join(missing)}")

    inserted = errors = 0
    error_rows = []
    try:
        conn = get_conn()
        cur = conn.cursor()
        for i, row in df.iterrows():
            try:
                budget_date = pd.to_datetime(row["BudgetDate"]).date()
                budget_date = budget_date.replace(day=1)
                amount = float(row["BudgetAmount"])
                cur.execute("""
                    DELETE FROM [dbo].[BudgetsIntoMedia]
                    WHERE [Site]=? AND [Brand]=? AND [DealType]=? AND [Salestype]=? AND [BudgetDate]=?
                """, str(row["Site"]), str(row["Brand"]), str(row["DealType"]), str(row["Salestype"]), budget_date)
                cur.execute("""
                    INSERT INTO [dbo].[BudgetsIntoMedia] ([DealType],[Site],[BudgetDate],[BudgetAmount],[Brand],[Salestype])
                    VALUES (?,?,?,?,?,?)
                """, str(row["DealType"]), str(row["Site"]), budget_date, amount, str(row["Brand"]), str(row["Salestype"]))
                inserted += 1
            except Exception as e:
                errors += 1
                error_rows.append({"row": i + 2, "error": str(e)})
        conn.commit()
        conn.close()
    except Exception as e:
        raise HTTPException(500, str(e))

    return JSONResponse({"status": "ok", "inserted": inserted, "errors": errors, "error_rows": error_rows[:10]})

# ── SÆLGER: Manuel insert ────────────────────────────────────────
@router.post("/saelger/insert")
async def saelger_insert(
    salesperson: str = Form(...),
    site:        str = Form(...),
    team:        str = Form(...),
    year:        int = Form(...),
    months_data: str = Form(...),
):
    import json
    rows = json.loads(months_data)
    inserted = 0
    try:
        conn = get_conn()
        cur = conn.cursor()
        for month_str, amount_str in rows.items():
            amount = float(amount_str) if amount_str else 0.0
            budget_date = date(year, int(month_str), 1)
            cur.execute("""
                DELETE FROM [dbo].[SalesPersonBudget]
                WHERE [SalesPersonName]=? AND [Site]=? AND [Team]=? AND [BudgetDate]=?
            """, salesperson, site, team, budget_date)
            cur.execute("""
                INSERT INTO [dbo].[SalesPersonBudget] ([SalesPersonName],[Site],[BudgetDate],[BudgetAmount],[Team])
                VALUES (?,?,?,?,?)
            """, salesperson, site, budget_date, amount, team)
            inserted += 1
        conn.commit()
        conn.close()
        return JSONResponse({"status": "ok", "inserted": inserted})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ── SÆLGER: Excel upload ─────────────────────────────────────────
@router.post("/saelger/upload")
async def saelger_upload(file: UploadFile = File(...)):
    if not file.filename.endswith((".xlsx", ".xls", ".csv")):
        raise HTTPException(400, "Kun .xlsx, .xls eller .csv filer er tilladt")
    content = await file.read()
    try:
        if file.filename.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(content))
        else:
            df = pd.read_excel(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(400, f"Kunne ikke læse filen: {e}")

    required = {"SalesPersonName", "Site", "Team", "BudgetDate", "BudgetAmount"}
    missing = required - set(df.columns)
    if missing:
        raise HTTPException(400, f"Mangler kolonner: {', '.join(missing)}")

    inserted = errors = 0
    error_rows = []
    try:
        conn = get_conn()
        cur = conn.cursor()
        for i, row in df.iterrows():
            try:
                budget_date = pd.to_datetime(row["BudgetDate"]).date()
                budget_date = budget_date.replace(day=1)
                amount = float(row["BudgetAmount"])
                cur.execute("""
                    DELETE FROM [dbo].[SalesPersonBudget]
                    WHERE [SalesPersonName]=? AND [Site]=? AND [Team]=? AND [BudgetDate]=?
                """, str(row["SalesPersonName"]), str(row["Site"]), str(row["Team"]), budget_date)
                cur.execute("""
                    INSERT INTO [dbo].[SalesPersonBudget] ([SalesPersonName],[Site],[BudgetDate],[BudgetAmount],[Team])
                    VALUES (?,?,?,?,?)
                """, str(row["SalesPersonName"]), str(row["Site"]), budget_date, amount, str(row["Team"]))
                inserted += 1
            except Exception as e:
                errors += 1
                error_rows.append({"row": i + 2, "error": str(e)})
        conn.commit()
        conn.close()
    except Exception as e:
        raise HTTPException(500, str(e))

    return JSONResponse({"status": "ok", "inserted": inserted, "errors": errors, "error_rows": error_rows[:10]})



# ── OVERBLIK: Medie budget query ─────────────────────────────────
@router.get("/medie/data")
async def medie_data(
    year:      int = None,
    month:     int = None,
    site:      str = None,
    brand:     str = None,
):
    try:
        conn = get_conn()
        cur  = conn.cursor()
        where = ["1=1"]
        params = []
        if year:
            where.append("YEAR([BudgetDate]) = %s")
            params.append(year)
        if month:
            where.append("MONTH([BudgetDate]) = %s")
            params.append(month)
        if site:
            where.append("[Site] = %s")
            params.append(site)
        if brand:
            where.append("[Brand] = %s")
            params.append(brand)
        sql = f"""
            SELECT [Site],[Brand],[DealType],[Salestype],
                   YEAR([BudgetDate]) AS År,
                   MONTH([BudgetDate]) AS Måned,
                   [BudgetAmount]
            FROM [dbo].[BudgetsIntoMedia]
            WHERE {" AND ".join(where)}
            ORDER BY [BudgetDate],[Site],[Brand]
        """
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        # Build monthly chart series: group by month, sum amount
        monthly = {}
        for r in rows:
            m = int(r["Måned"])
            monthly[m] = monthly.get(m, 0) + float(r["BudgetAmount"] or 0)
        chart = [{"måned": m, "budget": round(monthly.get(m, 0)) } for m in range(1, 13)]
        conn.close()
        return JSONResponse({"rows": rows, "chart": chart, "total": sum(monthly.values())})
    except Exception as e:
        raise HTTPException(500, str(e))


# ── OVERBLIK: Sælger budget query ────────────────────────────────
@router.get("/saelger/data")
async def saelger_data(
    year:       int = None,
    month:      int = None,
    site:       str = None,
    team:       str = None,
    salesperson: str = None,
):
    try:
        conn = get_conn()
        cur  = conn.cursor()
        where = ["1=1"]
        params = []
        if year:
            where.append("YEAR([BudgetDate]) = %s")
            params.append(year)
        if month:
            where.append("MONTH([BudgetDate]) = %s")
            params.append(month)
        if site:
            where.append("[Site] = %s")
            params.append(site)
        if team:
            where.append("[Team] = %s")
            params.append(team)
        if salesperson:
            where.append("[SalesPersonName] = %s")
            params.append(salesperson)
        sql = f"""
            SELECT [SalesPersonName],[Site],[Team],
                   YEAR([BudgetDate]) AS År,
                   MONTH([BudgetDate]) AS Måned,
                   [BudgetAmount]
            FROM [dbo].[SalesPersonBudget]
            WHERE {" AND ".join(where)}
            ORDER BY [BudgetDate],[SalesPersonName]
        """
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        monthly = {}
        for r in rows:
            m = int(r["Måned"])
            monthly[m] = monthly.get(m, 0) + float(r["BudgetAmount"] or 0)
        chart = [{"måned": m, "budget": round(monthly.get(m, 0)) } for m in range(1, 13)]
        conn.close()
        return JSONResponse({"rows": rows, "chart": chart, "total": sum(monthly.values())})
    except Exception as e:
        raise HTTPException(500, str(e))

# ── Excel template download ──────────────────────────────────────
@router.get("/medie/template")
async def medie_template():
    from fastapi.responses import StreamingResponse
    df = pd.DataFrame(columns=["DealType","Site","BudgetDate","BudgetAmount","Brand","Salestype"])
    df.loc[0] = ["Job","FinansWatch DK","2025-01-01","500000","Watch DK","Business"]
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)
    return StreamingResponse(buf, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers={"Content-Disposition": "attachment; filename=medie_budget_template.xlsx"})

@router.get("/saelger/template")
async def saelger_template():
    from fastapi.responses import StreamingResponse
    df = pd.DataFrame(columns=["SalesPersonName","Site","BudgetDate","BudgetAmount","Team"])
    df.loc[0] = ["Michael Toft","FINANS DK","2025-01-01","31000","Team FINANS Int"]
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)
    return StreamingResponse(buf, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers={"Content-Disposition": "attachment; filename=saelger_budget_template.xlsx"})
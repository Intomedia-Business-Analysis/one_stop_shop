import traceback
from fastapi import APIRouter, Depends, Request, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import pymssql
import pandas as pd
import io
import os
from datetime import date
from dotenv import load_dotenv

from auth import ROLE_LABELS, get_current_user

load_dotenv()

router = APIRouter(prefix="/tools/budget", tags=["Budget"])
templates = Jinja2Templates(directory="templates")
templates.env.globals["ROLE_LABELS"] = ROLE_LABELS

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
async def budget_tool(request: Request, user=Depends(get_current_user)):
    sites       = get_distinct("BudgetsIntoMedia", "Site")
    brands      = get_distinct("BudgetsIntoMedia", "Brand")
    deal_types  = get_distinct("BudgetsIntoMedia", "DealType")
    salestypes  = get_distinct("BudgetsIntoMedia", "Salestype")
    sp_sites    = get_distinct("SalespersonBudget", "Brand")
    sp_teams    = get_distinct("SalespersonBudget", "Team")
    sp_persons  = get_distinct("SalespersonBudget", "Owner")
    return templates.TemplateResponse("budget_tool.html", {
        "request":    request,
        "user":       user,
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
            amount = round(float(amount_str), 2) if amount_str else 0.0
            budget_date = date(year, int(month_str), 1)
            # Upsert: delete existing + insert
            cur.execute("""
                DELETE FROM [dbo].[BudgetsIntoMedia]
                WHERE [Site]=%s AND [Brand]=%s AND [DealType]=%s AND [Salestype]=%s AND [BudgetDate]=%s
            """, (site, brand, deal_type, salestype, budget_date))

            cur.execute("""
                INSERT INTO [dbo].[BudgetsIntoMedia] ([DealType],[Site],[BudgetDate],[BudgetAmount],[Brand],[Salestype])
                VALUES (%s,%s,%s,%s,%s,%s)
            """, (deal_type, site, budget_date, amount, brand, salestype))
            inserted += 1
        conn.commit()
        conn.close()
        return JSONResponse({"status": "ok", "inserted": inserted})
    except Exception as e:
        traceback.print_exc()
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
                    WHERE [Site]=%s AND [Brand]=%s AND [DealType]=%s AND [Salestype]=%s AND [BudgetDate]=%s
                """, (str(row["Site"]), str(row["Brand"]), str(row["DealType"]), str(row["Salestype"]), budget_date))
                cur.execute("""
                    INSERT INTO [dbo].[BudgetsIntoMedia] ([DealType],[Site],[BudgetDate],[BudgetAmount],[Brand],[Salestype])
                    VALUES (%s,%s,%s,%s,%s,%s)
                """, (str(row["DealType"]), str(row["Site"]), budget_date, amount, str(row["Brand"]), str(row["Salestype"])))
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
                DELETE FROM [dbo].[SalespersonBudget]
                WHERE [Owner]=%s AND [Brand]=%s AND [Team]=%s AND [BudgetDate]=%s
            """, (salesperson, site, team, budget_date))
            cur.execute("""
                INSERT INTO [dbo].[SalespersonBudget] ([Owner],[Brand],[BudgetDate],[BudgetAmount],[Team])
                VALUES (%s,%s,%s,%s,%s)
            """, (salesperson, site, budget_date, amount, team))
            inserted += 1
        conn.commit()
        conn.close()
        return JSONResponse({"status": "ok", "inserted": inserted})
    except Exception as e:
        traceback.print_exc()
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

    required = {"Owner", "Brand", "Team", "BudgetDate", "BudgetAmount"}
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
                    DELETE FROM [dbo].[SalespersonBudget]
                    WHERE [Owner]=%s AND [Brand]=%s AND [Team]=%s AND [BudgetDate]=%s
                """, (str(row["Owner"]), str(row["Brand"]), str(row["Team"]), budget_date))
                cur.execute("""
                    INSERT INTO [dbo].[SalespersonBudget] ([Owner],[Brand],[BudgetDate],[BudgetAmount],[Team])
                    VALUES (%s,%s,%s,%s,%s)
                """, (str(row["Owner"]), str(row["Brand"]), budget_date, amount, str(row["Team"])))
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
    dealtype:  str = None,
    salestype: str = None,
):
    def serialize(v):
        if type(v).__name__ == 'Decimal':
            return float(v)
        return v

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
        if dealtype:
            where.append("[DealType] = %s")
            params.append(dealtype)
        if salestype:
            where.append("[Salestype] = %s")
            params.append(salestype)

        sql = f"""
            SELECT [ID],[Site],[Brand],[DealType],[Salestype],
                   YEAR([BudgetDate]) AS År,
                   MONTH([BudgetDate]) AS Måned,
                   [BudgetAmount]
            FROM [dbo].[BudgetsIntoMedia]
            WHERE {" AND ".join(where)}
            ORDER BY [BudgetDate],[Site],[Brand]
        """
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, [serialize(v) for v in r])) for r in cur.fetchall()]
        monthly = {}
        for r in rows:
            m = int(r["Måned"])
            monthly[m] = monthly.get(m, 0) + float(r["BudgetAmount"] or 0)
        chart = [{"måned": m, "budget": round(monthly.get(m, 0))} for m in range(1, 13)]
        conn.close()
        return JSONResponse({"rows": rows, "chart": chart, "total": sum(monthly.values())})
    except Exception as e:
        traceback.print_exc()
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
    def serialize(v):
        if type(v).__name__ == 'Decimal':
            return float(v)
        return v

    try:
        conn = get_conn()
        cur  = conn.cursor()
        where  = ["1=1"]
        params = []
        if year:
            where.append("YEAR([BudgetDate]) = %s")
            params.append(year)
        if month:
            where.append("MONTH([BudgetDate]) = %s")
            params.append(month)
        if site:
            where.append("[Brand] = %s")
            params.append(site)
        if team:
            where.append("[Team] = %s")
            params.append(team)
        if salesperson:
            where.append("[Owner] = %s")
            params.append(salesperson)

        sql = f"""
            SELECT [Owner] AS SalesPersonName,[Brand] AS Site,[Team],
                   YEAR([BudgetDate]) AS År,
                   MONTH([BudgetDate]) AS Måned,
                   [BudgetAmount]
            FROM [dbo].[SalespersonBudget]
            WHERE {" AND ".join(where)}
            ORDER BY [BudgetDate],[Owner]
        """
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, [serialize(v) for v in r])) for r in cur.fetchall()]
        monthly = {}
        for r in rows:
            m = int(r["Måned"])
            monthly[m] = monthly.get(m, 0) + float(r["BudgetAmount"] or 0)
        chart = [{"måned": m, "budget": round(monthly.get(m, 0))} for m in range(1, 13)]
        conn.close()
        return JSONResponse({"rows": rows, "chart": chart, "total": sum(monthly.values())})
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))

# ---- Medie Slet knap--------------------------------------
@router.delete("/medie/delete/{row_id}")
async def medie_delete(row_id: int):
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(
            "DELETE FROM [dbo].[BudgetsIntoMedia] WHERE [ID] = %s",
            (row_id,)
        )
        conn.commit()
        conn.close()
        return JSONResponse({"status": "ok"})
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))

#--------------Medie Rediger knap------------------------------------
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
        conn = get_conn()
        cur  = conn.cursor()
        budget_date = date(year, month, 1)
        cur.execute("""
            UPDATE [dbo].[BudgetsIntoMedia]
            SET [Site]=%s, [Brand]=%s, [DealType]=%s, [Salestype]=%s,
                [BudgetDate]=%s, [BudgetAmount]=%s
            WHERE [ID]=%s
        """, (site, brand, dealtype, salestype, budget_date, amount, row_id))
        conn.commit()
        conn.close()
        return JSONResponse({"status": "ok"})
    except Exception as e:
        traceback.print_exc()
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
    df = pd.DataFrame(columns=["Owner","Brand","Team","BudgetDate","BudgetAmount"])
    df.loc[0] = ["Michael Toft","FINANS DK","Team FINANS Int","2025-01-01","31000"]
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)
    return StreamingResponse(buf, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers={"Content-Disposition": "attachment; filename=saelger_budget_template.xlsx"})
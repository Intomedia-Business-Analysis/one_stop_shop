import os
import traceback
import io
from datetime import date

import pymssql
import pandas as pd
from fastapi import HTTPException
from dotenv import load_dotenv

load_dotenv()


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


def db_get_distinct(table: str, column: str) -> list:
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(f"SELECT DISTINCT [{column}] FROM [dbo].[{table}] WHERE [{column}] IS NOT NULL ORDER BY [{column}]")
        rows = [r[0] for r in cur.fetchall()]
        conn.close()
        return rows
    except Exception:
        return []


def db_medie_upsert_rows(site, brand, deal_type, salestype, year, rows: dict):
    inserted = 0
    conn = get_conn()
    cur = conn.cursor()
    for month_str, amount_str in rows.items():
        amount = round(float(amount_str), 2) if amount_str else 0.0
        budget_date = date(year, int(month_str), 1)
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
    return inserted


def db_medie_upload_df(df: pd.DataFrame):
    inserted = errors = 0
    error_rows = []
    conn = get_conn()
    cur = conn.cursor()
    for i, row in df.iterrows():
        try:
            budget_date = pd.to_datetime(row["BudgetDate"]).date().replace(day=1)
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
        except Exception:
            errors += 1
            error_rows.append({"row": i + 2, "error": traceback.format_exc()})
    conn.commit()
    conn.close()
    return inserted, errors, error_rows


def db_saelger_upsert_rows(salesperson, site, team, year, rows: dict):
    inserted = 0
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
    return inserted


def db_saelger_upload_df(df: pd.DataFrame):
    inserted = errors = 0
    error_rows = []
    conn = get_conn()
    cur = conn.cursor()
    for i, row in df.iterrows():
        try:
            budget_date = pd.to_datetime(row["BudgetDate"]).date().replace(day=1)
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
        except Exception:
            errors += 1
            error_rows.append({"row": i + 2, "error": traceback.format_exc()})
    conn.commit()
    conn.close()
    return inserted, errors, error_rows


def db_medie_query(year=None, month=None, site=None, brand=None, dealtype=None, salestype=None):
    def serialize(v):
        if type(v).__name__ == 'Decimal':
            return float(v)
        return v

    conn = get_conn()
    cur = conn.cursor()
    where = ["1=1"]
    params = []
    if year:
        where.append("YEAR([BudgetDate]) = %s"); params.append(year)
    if month:
        where.append("MONTH([BudgetDate]) = %s"); params.append(month)
    if site:
        where.append("[Site] = %s"); params.append(site)
    if brand:
        where.append("[Brand] = %s"); params.append(brand)
    if dealtype:
        where.append("[DealType] = %s"); params.append(dealtype)
    if salestype:
        where.append("[Salestype] = %s"); params.append(salestype)

    cur.execute(f"""
        SELECT [ID],[Site],[Brand],[DealType],[Salestype],
               YEAR([BudgetDate]) AS År, MONTH([BudgetDate]) AS Måned, [BudgetAmount]
        FROM [dbo].[BudgetsIntoMedia]
        WHERE {" AND ".join(where)}
        ORDER BY [BudgetDate],[Site],[Brand]
    """, params)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, [serialize(v) for v in r])) for r in cur.fetchall()]
    conn.close()
    return rows


def db_saelger_query(year=None, month=None, site=None, team=None, salesperson=None):
    def serialize(v):
        if type(v).__name__ == 'Decimal':
            return float(v)
        return v

    conn = get_conn()
    cur = conn.cursor()
    where = ["1=1"]
    params = []
    if year:
        where.append("YEAR([BudgetDate]) = %s"); params.append(year)
    if month:
        where.append("MONTH([BudgetDate]) = %s"); params.append(month)
    if site:
        where.append("[Brand] = %s"); params.append(site)
    if team:
        where.append("[Team] = %s"); params.append(team)
    if salesperson:
        where.append("[Owner] = %s"); params.append(salesperson)

    cur.execute(f"""
        SELECT [Owner] AS SalesPersonName, [Brand] AS Site, [Team],
               YEAR([BudgetDate]) AS År, MONTH([BudgetDate]) AS Måned, [BudgetAmount]
        FROM [dbo].[SalespersonBudget]
        WHERE {" AND ".join(where)}
        ORDER BY [BudgetDate],[Owner]
    """, params)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, [serialize(v) for v in r])) for r in cur.fetchall()]
    conn.close()
    return rows


def db_medie_delete(row_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM [dbo].[BudgetsIntoMedia] WHERE [ID] = %s", (row_id,))
    conn.commit()
    conn.close()


def db_medie_update(row_id, site, brand, dealtype, salestype, year, month, amount):
    conn = get_conn()
    cur = conn.cursor()
    budget_date = date(year, month, 1)
    cur.execute("""
        UPDATE [dbo].[BudgetsIntoMedia]
        SET [Site]=%s, [Brand]=%s, [DealType]=%s, [Salestype]=%s,
            [BudgetDate]=%s, [BudgetAmount]=%s
        WHERE [ID]=%s
    """, (site, brand, dealtype, salestype, budget_date, amount, row_id))
    conn.commit()
    conn.close()
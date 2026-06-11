import logging
import os
import io
from datetime import date

import pymssql
import pandas as pd
from fastapi import HTTPException
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


# Fælles pooled DB-forbindelse — se db.py.
from db import get_conn  # noqa: E402,F401


# ── Team-dataadgang ──────────────────────────────────────────────────────────
# Sælgerbudgettet har en [Team]-kolonne og filtreres direkte. Medie-budgettet
# er brand/site-niveau, så en team-begrænsning oversættes til de budget-brands
# teamet dækker: Teams.brand (watch_no, finans, …) → BudgetsIntoMedia.[Brand].
TEAM_BRAND_TO_BUDGET_BRANDS = {
    "watch_dk":   ["Watch DK", "KForum"],
    "watch_int":  ["Watch Int"],
    "watch_no":   ["Watch NO"],
    "watch_se":   ["Watch SE"],
    "watch_de":   ["Watch DE"],
    "finans":     ["FINANS DK"],
    "finans_int": ["FINANS Int"],
    "monitor":    ["Monitor"],
    "marketwire": ["MarketWire"],
}
# Teams uden brand (Banner/Job) genkendes på DealType i medie-budgettet.
TEAM_TO_DEALTYPES = {
    "Team Banner": ["Banner"],
    "Team Job":    ["Job"],
}


def db_budget_scope(team_names: list) -> dict:
    """Oversæt tilladte teams til medie-budget-scope: {brands, dealtypes}.

    En medie-budgetrække er tilladt hvis dens [Brand] ELLER [DealType] er i scope.
    """
    brands: set = set()
    dealtypes: set = set()
    if not team_names:
        return {"brands": brands, "dealtypes": dealtypes}
    ph = "(" + ",".join(["%s"] * len(team_names)) + ")"
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(f"SELECT name, brand FROM Teams WHERE name IN {ph}", tuple(team_names))
        for name, brand in cur.fetchall():
            brands.update(TEAM_BRAND_TO_BUDGET_BRANDS.get(brand or "", []))
            dealtypes.update(TEAM_TO_DEALTYPES.get(name, []))
        conn.close()
    except Exception:
        # Fallback: tomt scope = ingen medie-budgetrækker vises (sikker degradering).
        logger.exception("db_budget_scope fejlede — returnerer tomt scope")
    return {"brands": brands, "dealtypes": dealtypes}


def db_owners_for_teams(team_names: list) -> list:
    """Sælgere (Owner) i sælgerbudgettet for de angivne teams — til dropdowns."""
    if not team_names:
        return []
    ph = "(" + ",".join(["%s"] * len(team_names)) + ")"
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            f"SELECT DISTINCT [Owner] FROM [dbo].[SalespersonBudget] "
            f"WHERE [Team] IN {ph} AND [Owner] IS NOT NULL ORDER BY [Owner]",
            tuple(team_names),
        )
        rows = [r[0] for r in cur.fetchall()]
        conn.close()
        return rows
    except Exception:
        # Fallback: tom dropdown-liste i stedet for fejlside.
        logger.exception("db_owners_for_teams fejlede — returnerer tom liste")
        return []


def db_medie_get(row_id: int) -> dict | None:
    """Hent én medie-budgetrække — til adgangstjek før update/delete."""
    conn = get_conn()
    cur = conn.cursor(as_dict=True)
    cur.execute(
        "SELECT [ID],[Site],[Brand],[DealType],[Salestype] "
        "FROM [dbo].[BudgetsIntoMedia] WHERE [ID] = %s",
        (row_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def db_get_distinct(table: str, column: str) -> list:
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(f"SELECT DISTINCT [{column}] FROM [dbo].[{table}] WHERE [{column}] IS NOT NULL ORDER BY [{column}]")
        rows = [r[0] for r in cur.fetchall()]
        conn.close()
        return rows
    except Exception:
        # Fallback: tom dropdown-liste i stedet for fejlside.
        logger.exception("db_get_distinct fejlede (%s.%s) — returnerer tom liste", table, column)
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
        except Exception as e:
            # Forventelig degradering — dårlige rækker i upload springes over.
            errors += 1
            logger.warning("Medie-upload: række %s kunne ikke importeres: %s", i + 2, e)
            error_rows.append({"row": i + 2, "error": str(e)})
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

    # Detect "day-as-month" encoding: alle datoer i januar med dag 1-12
    # (fx 2026-01-02 = februar, 2026-01-07 = juli osv.)
    parsed_dates = pd.to_datetime(df["BudgetDate"], errors="coerce")
    if (parsed_dates.dt.month == 1).all() and parsed_dates.dt.day.between(1, 12).all() and (parsed_dates.dt.day > 1).any():
        df = df.copy()
        df["BudgetDate"] = parsed_dates.apply(lambda d: d.replace(month=int(d.day), day=1))

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
        except Exception as e:
            # Forventelig degradering — dårlige rækker i upload springes over.
            errors += 1
            logger.warning("Sælger-upload: række %s kunne ikke importeres: %s", i + 2, e)
            error_rows.append({"row": i + 2, "error": str(e)})
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
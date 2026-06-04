import os
import pymssql
from dotenv import load_dotenv

load_dotenv()

def get_conn():
    return pymssql.connect(
        server=os.getenv("DB_SERVER"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME", "INTOMEDIA"),
        tds_version="7.0",
        login_timeout=5,
        timeout=5,
    )

def get_available_owners() -> list:
    conn = get_conn()
    cur = conn.cursor(as_dict=True)
    cur.execute("""
        SELECT [name]
        FROM [dbo].[HubUsers]
        WHERE [is_active] = 1
        ORDER BY [name]
    """)
    rows = [r["name"] for r in cur.fetchall()]
    conn.close()
    return rows

def get_kundeliste(owner_name: str) -> list:
    conn = get_conn()
    cursor = conn.cursor(as_dict=True)
    cursor.execute("""
        SELECT
            a.org_name,
            a.org_id,
            a.site,
            a.acv_value_dkk,
            a.brand,
            a.first_activation,
            a.last_activation,
            (
                SELECT TOP 1 d.team
                FROM [dbo].[PipedriveDeals] d
                WHERE d.org_id = a.org_id
                  AND d.owner_name = %s
                  AND d.team IS NOT NULL
                  AND d.team <> ''
                ORDER BY d.won_time DESC
            ) AS team
        FROM [dbo].[PipeDrive_ACV] a
        WHERE a.owner_name = %s
        ORDER BY a.acv_value_dkk DESC
    """, (owner_name, owner_name))
    rows = cursor.fetchall()
    conn.close()
    for r in rows:
        if r.get("acv_value_dkk") is not None:
            r["acv_value_dkk"] = float(r["acv_value_dkk"])
    return rows

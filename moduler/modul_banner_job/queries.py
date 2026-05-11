import os
import traceback

import pymssql
from dotenv import load_dotenv

load_dotenv()

BASE_YEAR = 2023
HEATMAP_YEARS = [2023, 2024, 2025, 2026]

_BASE_WHERE = """
    account = 'jppol_advertising'
    AND pipeline_name = %s
    AND org_id IS NOT NULL
    AND service_activation_date >= '2023-01-01'
"""


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


def _period_clause(year: int | None, month: str | None) -> tuple[str, list]:
    if not year:
        return "", []
    if not month:
        return "AND YEAR(service_activation_date) = %s", [year]
    if month in ("Q1", "Q2", "Q3", "Q4"):
        q = int(month[1])
        m_from = (q - 1) * 3 + 1
        m_to   = q * 3
        return (
            "AND YEAR(service_activation_date) = %s"
            " AND MONTH(service_activation_date) BETWEEN %s AND %s",
            [year, m_from, m_to],
        )
    return (
        "AND YEAR(service_activation_date) = %s"
        " AND MONTH(service_activation_date) = %s",
        [year, int(month)],
    )


def _year_clause(year: int | None) -> tuple[str, list]:
    if year:
        return "AND YEAR(service_activation_date) = %s", [year]
    return "", []


def _owner_clause(owner_name: str | None) -> tuple[str, list]:
    if owner_name:
        return "AND owner_name = %s", [owner_name]
    return "", []


PIPELINE_TEAM = {
    "banner": "Team Banner",
    "job":    "Team Job",
}


def db_owners(pipeline: str) -> list[str]:
    team_name = PIPELINE_TEAM.get(pipeline)
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        if team_name:
            cur.execute("""
                SELECT u.name AS owner_name
                FROM HubUsers u
                JOIN TeamMemberships tm ON tm.user_id = u.id
                JOIN Teams t ON t.id = tm.team_id
                WHERE t.name = %s
                  AND (TRY_CAST(tm.end_date AS DATE) IS NULL OR TRY_CAST(tm.end_date AS DATE) >= CAST(GETDATE() AS DATE))
                ORDER BY u.name
            """, (team_name,))
        else:
            cur.execute(f"""
                SELECT DISTINCT owner_name
                FROM [dbo].[PipedriveDeals]
                WHERE {_BASE_WHERE}
                  AND owner_name IS NOT NULL
                  AND owner_name <> 'System Admin'
                ORDER BY owner_name
            """, (pipeline,))
        rows = [r["owner_name"] for r in cur.fetchall()]
        conn.close()
        return rows
    except Exception:
        traceback.print_exc()
        return []


def db_kpi_data(pipeline: str, year: int | None = None, month: str | None = None, owner_name: str | None = None) -> dict:
    yc, yp = _period_clause(year, month)
    oc, op = _owner_clause(owner_name)
    params = (pipeline,) + tuple(yp) + tuple(op)
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)

        # Aktive kunder (unikke org_id)
        cur.execute(f"""
            SELECT COUNT(DISTINCT org_id) AS active_customers
            FROM [dbo].[PipedriveDeals]
            WHERE {_BASE_WHERE} {yc} {oc}
              AND status = 'won'
        """, params)
        active_customers = int((cur.fetchone() or {}).get("active_customers", 0) or 0)

        # Total omsætning og antal deals
        cur.execute(f"""
            SELECT
                ISNULL(CAST(SUM(value_dkk) AS BIGINT), 0) AS total_value,
                COUNT(*) AS total_deals
            FROM [dbo].[PipedriveDeals]
            WHERE {_BASE_WHERE} {yc} {oc}
              AND status = 'won'
        """, params)
        row = cur.fetchone() or {}
        total_value = int(row.get("total_value", 0) or 0)
        total_deals = int(row.get("total_deals", 0) or 0)
        avg_deal = round(total_value / total_deals) if total_deals > 0 else 0
        avg_per_customer = round(total_value / active_customers) if active_customers > 0 else 0

        # Tilbagevendende kunder:
        # Hvis år valgt: købt i det valgte år OG mindst ét andet år
        # Hvis intet år: købt i mindst 2 forskellige år
        if year:
            cur.execute(f"""
                SELECT COUNT(*) AS returning_customers
                FROM (
                    SELECT org_id
                    FROM [dbo].[PipedriveDeals]
                    WHERE {_BASE_WHERE}
                      AND status = 'won'
                    GROUP BY org_id
                    HAVING
                        SUM(CASE WHEN YEAR(service_activation_date) = %s THEN 1 ELSE 0 END) > 0
                        AND COUNT(DISTINCT YEAR(service_activation_date)) > 1
                ) t
            """, (pipeline, year))
        else:
            cur.execute(f"""
                SELECT COUNT(*) AS returning_customers
                FROM (
                    SELECT org_id
                    FROM [dbo].[PipedriveDeals]
                    WHERE {_BASE_WHERE}
                      AND status = 'won'
                    GROUP BY org_id
                    HAVING COUNT(DISTINCT YEAR(service_activation_date)) > 1
                ) t
            """, (pipeline,))
        returning_customers = int((cur.fetchone() or {}).get("returning_customers", 0) or 0)

        conn.close()
        return {
            "active_customers": active_customers,
            "total_value": total_value,
            "avg_deal": avg_deal,
            "avg_per_customer": avg_per_customer,
            "total_deals": total_deals,
            "returning_customers": returning_customers,
        }
    except Exception:
        traceback.print_exc()
        return {"active_customers": 0, "total_value": 0, "avg_deal": 0, "avg_per_customer": 0, "total_deals": 0, "returning_customers": 0}


def db_top_customers(pipeline: str, year: int | None = None, month: str | None = None, owner_name: str | None = None) -> list[dict]:
    yc, yp = _period_clause(year, month)
    oc, op = _owner_clause(owner_name)
    params = (pipeline,) + tuple(yp) + tuple(op)
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(f"""
            SELECT TOP 10
                org_name,
                CAST(SUM(value_dkk) AS BIGINT) AS total_value,
                COUNT(*) AS deal_count
            FROM [dbo].[PipedriveDeals]
            WHERE {_BASE_WHERE} {yc} {oc}
              AND status = 'won'
            GROUP BY org_id, org_name
            ORDER BY total_value DESC
        """, params)
        rows = [
            {"org_name": r["org_name"] or "—", "total_value": int(r["total_value"] or 0), "deal_count": int(r["deal_count"] or 0)}
            for r in cur.fetchall()
        ]
        conn.close()
        return rows
    except Exception:
        traceback.print_exc()
        return []


def db_salesperson_performance(pipeline: str, year: int | None = None, month: str | None = None) -> list[dict]:
    yc, yp = _period_clause(year, month)
    params = (pipeline,) + tuple(yp)
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        team_name = PIPELINE_TEAM.get(pipeline)
        team_filter = ""
        if team_name:
            team_filter = """
              AND owner_name IN (
                SELECT u.name
                FROM HubUsers u
                JOIN TeamMemberships tm ON tm.user_id = u.id
                JOIN Teams t ON t.id = tm.team_id
                WHERE t.name = %s
                  AND (TRY_CAST(tm.end_date AS DATE) IS NULL OR TRY_CAST(tm.end_date AS DATE) >= CAST(GETDATE() AS DATE))
              )"""
            params = params + (team_name,)
        cur.execute(f"""
            SELECT
                owner_name,
                YEAR(service_activation_date) AS aar,
                COUNT(DISTINCT org_id) AS antal_kunder,
                COUNT(*) AS antal_deals,
                CAST(SUM(value_dkk) AS BIGINT) AS total_value,
                CAST(SUM(value_dkk) / COUNT(DISTINCT org_id) AS BIGINT) AS value_pr_kunde
            FROM [dbo].[PipedriveDeals]
            WHERE {_BASE_WHERE} {yc}
              AND status = 'won'
              AND owner_name IS NOT NULL
              {team_filter}
            GROUP BY owner_name, YEAR(service_activation_date)
            ORDER BY owner_name, aar
        """, params)
        rows = []
        for r in cur.fetchall():
            rows.append({
                "owner_name":    r["owner_name"],
                "aar":           int(r["aar"]),
                "antal_kunder":  int(r["antal_kunder"] or 0),
                "antal_deals":   int(r["antal_deals"] or 0),
                "total_value":   int(r["total_value"] or 0),
                "value_pr_kunde": int(r["value_pr_kunde"] or 0),
            })
        conn.close()
        return rows
    except Exception:
        traceback.print_exc()
        return []


def db_customer_heatmap(pipeline: str, owner_name: str | None = None) -> list[dict]:
    oc, op = _owner_clause(owner_name)
    params = (pipeline,) + tuple(op)
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(f"""
            SELECT
                org_id,
                org_name,
                SUM(CASE WHEN YEAR(service_activation_date) = 2023 THEN 1 ELSE 0 END) AS antal_2023,
                SUM(CASE WHEN YEAR(service_activation_date) = 2024 THEN 1 ELSE 0 END) AS antal_2024,
                SUM(CASE WHEN YEAR(service_activation_date) = 2025 THEN 1 ELSE 0 END) AS antal_2025,
                SUM(CASE WHEN YEAR(service_activation_date) = 2026 THEN 1 ELSE 0 END) AS antal_2026,
                CAST(SUM(CASE WHEN YEAR(service_activation_date) = 2023 THEN value_dkk ELSE 0 END) AS INT) AS value_2023,
                CAST(SUM(CASE WHEN YEAR(service_activation_date) = 2024 THEN value_dkk ELSE 0 END) AS INT) AS value_2024,
                CAST(SUM(CASE WHEN YEAR(service_activation_date) = 2025 THEN value_dkk ELSE 0 END) AS INT) AS value_2025,
                CAST(SUM(CASE WHEN YEAR(service_activation_date) = 2026 THEN value_dkk ELSE 0 END) AS INT) AS value_2026,
                COUNT(*) AS antal_total,
                CAST(SUM(value_dkk) AS INT) AS total_value
            FROM [dbo].[PipedriveDeals]
            WHERE {_BASE_WHERE} {oc}
              AND status = 'won'
            GROUP BY org_id, org_name
            ORDER BY total_value DESC
        """, params)
        rows = []
        for r in cur.fetchall():
            rows.append({
                "org_id":     r["org_id"],
                "org_name":   r["org_name"] or "—",
                "antal_2023": int(r["antal_2023"] or 0),
                "antal_2024": int(r["antal_2024"] or 0),
                "antal_2025": int(r["antal_2025"] or 0),
                "antal_2026": int(r["antal_2026"] or 0),
                "value_2023": int(r["value_2023"] or 0),
                "value_2024": int(r["value_2024"] or 0),
                "value_2025": int(r["value_2025"] or 0),
                "value_2026": int(r["value_2026"] or 0),
                "antal_total": int(r["antal_total"] or 0),
                "total_value": int(r["total_value"] or 0),
            })
        conn.close()
        return rows
    except Exception:
        traceback.print_exc()
        return []


def db_customer_history(pipeline: str, org_id: str) -> dict:
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)

        cur.execute("""
            SELECT org_name FROM [dbo].[PipedriveDeals]
            WHERE org_id = %s AND account = 'jppol_advertising' AND pipeline_name = %s
              AND org_id IS NOT NULL
            ORDER BY service_activation_date DESC
        """, (org_id, pipeline))
        name_row = cur.fetchone()
        org_name = (name_row or {}).get("org_name", org_id)

        cur.execute("""
            SELECT
                YEAR(service_activation_date) AS aar,
                COUNT(*) AS antal_deals,
                CAST(SUM(value_dkk) AS INT) AS total_value
            FROM [dbo].[PipedriveDeals]
            WHERE org_id = %s
              AND account = 'jppol_advertising'
              AND pipeline_name = %s
              AND org_id IS NOT NULL
              AND service_activation_date >= '2023-01-01'
              AND status = 'won'
            GROUP BY YEAR(service_activation_date)
            ORDER BY aar
        """, (org_id, pipeline))
        by_year = [
            {
                "aar": int(r["aar"]),
                "antal_deals": int(r["antal_deals"] or 0),
                "total_value": int(r["total_value"] or 0),
            }
            for r in cur.fetchall()
        ]

        cur.execute("""
            SELECT
                title,
                CONVERT(NVARCHAR(10), service_activation_date, 23) AS dato,
                CAST(value_dkk AS INT) AS value,
                owner_name,
                YEAR(service_activation_date) AS aar,
                COALESCE([sites], '') AS sites
            FROM [dbo].[PipedriveDeals]
            WHERE org_id = %s
              AND account = 'jppol_advertising'
              AND pipeline_name = %s
              AND org_id IS NOT NULL
              AND service_activation_date >= '2023-01-01'
              AND status = 'won'
            ORDER BY service_activation_date DESC
        """, (org_id, pipeline))
        deals = [
            {
                "title":      r["title"] or "(Uden titel)",
                "dato":       r["dato"] or "—",
                "value":      int(r["value"] or 0),
                "owner_name": r["owner_name"] or "—",
                "aar":        int(r["aar"]),
                "sites":      r["sites"] or "—",
            }
            for r in cur.fetchall()
        ]

        conn.close()
        return {"org_name": org_name, "by_year": by_year, "deals": deals}
    except Exception:
        traceback.print_exc()
        return {"org_name": org_id, "by_year": [], "deals": []}

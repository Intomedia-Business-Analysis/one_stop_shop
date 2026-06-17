import logging
import os

import pymssql
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

BASE_YEAR = 2023
HEATMAP_YEARS = [2023, 2024, 2025, 2026]

# Land → Pipedrive-account. Banner & Job-tool'et kan vises for enten DK eller NO;
# pipelines (Banner/Job) og statusser er ens på tværs af landene, så et skift af
# account er alt der skal til.
COUNTRY_ACCOUNT = {
    "dk": "jppol_advertising",
    "no": "watch_no_advertising",
}


def account_for_country(country: str | None) -> str:
    return COUNTRY_ACCOUNT.get((country or "dk").lower(), COUNTRY_ACCOUNT["dk"])


_BASE_WHERE = """
    account = %s
    AND pipeline_name = %s
    AND org_id IS NOT NULL
    AND service_activation_date >= '2023-01-01'
"""


# Fælles pooled DB-forbindelse — se db.py.
from db import get_conn  # noqa: E402,F401


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


def db_owners(pipeline: str, country: str = "dk") -> list[str]:
    account = account_for_country(country)
    # DK-sælgere kommer fra team-medlemskab (så også medlemmer uden deals vises).
    # NO har ingen tilsvarende team-opsætning, så dér listes de sælgere der
    # faktisk har deals i watch_no_advertising-kontoen.
    team_name = PIPELINE_TEAM.get(pipeline) if country == "dk" else None
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
            """, (account, pipeline))
        rows = [r["owner_name"] for r in cur.fetchall()]
        conn.close()
        return rows
    except Exception:
        logger.exception("db_owners fejlede (pipeline=%s, country=%s)", pipeline, country)
        raise


def db_kpi_data(pipeline: str, year: int | None = None, month: str | None = None, owner_name: str | None = None, country: str = "dk") -> dict:
    account = account_for_country(country)
    yc, yp = _period_clause(year, month)
    oc, op = _owner_clause(owner_name)
    params = (account, pipeline) + tuple(yp) + tuple(op)
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
        # Owner-filteret skal med her ligesom de øvrige KPI'er — ellers viser
        # tallet det samme for en enkelt sælger som for hele afdelingen.
        if year:
            cur.execute(f"""
                SELECT COUNT(*) AS returning_customers
                FROM (
                    SELECT org_id
                    FROM [dbo].[PipedriveDeals]
                    WHERE {_BASE_WHERE} {oc}
                      AND status = 'won'
                    GROUP BY org_id
                    HAVING
                        SUM(CASE WHEN YEAR(service_activation_date) = %s THEN 1 ELSE 0 END) > 0
                        AND COUNT(DISTINCT YEAR(service_activation_date)) > 1
                ) t
            """, (account, pipeline) + tuple(op) + (year,))
        else:
            cur.execute(f"""
                SELECT COUNT(*) AS returning_customers
                FROM (
                    SELECT org_id
                    FROM [dbo].[PipedriveDeals]
                    WHERE {_BASE_WHERE} {oc}
                      AND status = 'won'
                    GROUP BY org_id
                    HAVING COUNT(DISTINCT YEAR(service_activation_date)) > 1
                ) t
            """, (account, pipeline) + tuple(op))
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
        logger.exception("db_kpi_data fejlede (pipeline=%s, year=%s, month=%s, owner=%s)",
                         pipeline, year, month, owner_name)
        raise


def db_top_customers(pipeline: str, year: int | None = None, month: str | None = None, owner_name: str | None = None, country: str = "dk") -> list[dict]:
    account = account_for_country(country)
    yc, yp = _period_clause(year, month)
    oc, op = _owner_clause(owner_name)
    params = (account, pipeline) + tuple(yp) + tuple(op)
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
        logger.exception("db_top_customers fejlede (pipeline=%s, year=%s, month=%s, owner=%s)",
                         pipeline, year, month, owner_name)
        raise


def db_salesperson_performance(pipeline: str, year: int | None = None, month: str | None = None, country: str = "dk") -> list[dict]:
    account = account_for_country(country)
    yc, yp = _period_clause(year, month)
    params = (account, pipeline) + tuple(yp)
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        # Team-afgrænsning gælder kun DK (NO har ingen tilsvarende team-opsætning).
        team_name = PIPELINE_TEAM.get(pipeline) if country == "dk" else None
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
        logger.exception("db_salesperson_performance fejlede (pipeline=%s, year=%s, month=%s)",
                         pipeline, year, month)
        raise


def db_customer_heatmap(pipeline: str, owner_name: str | None = None, country: str = "dk") -> list[dict]:
    account = account_for_country(country)
    oc, op = _owner_clause(owner_name)
    params = (account, pipeline) + tuple(op)
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
        logger.exception("db_customer_heatmap fejlede (pipeline=%s, owner=%s)", pipeline, owner_name)
        raise


def db_customer_history(pipeline: str, org_id: str, country: str = "dk") -> dict:
    account = account_for_country(country)
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)

        cur.execute("""
            SELECT org_name FROM [dbo].[PipedriveDeals]
            WHERE org_id = %s AND account = %s AND pipeline_name = %s
              AND org_id IS NOT NULL
            ORDER BY service_activation_date DESC
        """, (org_id, account, pipeline))
        name_row = cur.fetchone()
        org_name = (name_row or {}).get("org_name", org_id)

        cur.execute("""
            SELECT
                YEAR(service_activation_date) AS aar,
                COUNT(*) AS antal_deals,
                CAST(SUM(value_dkk) AS INT) AS total_value
            FROM [dbo].[PipedriveDeals]
            WHERE org_id = %s
              AND account = %s
              AND pipeline_name = %s
              AND org_id IS NOT NULL
              AND service_activation_date >= '2023-01-01'
              AND status = 'won'
            GROUP BY YEAR(service_activation_date)
            ORDER BY aar
        """, (org_id, account, pipeline))
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
                COALESCE(ad_type, '') AS ad_type,
                CONVERT(NVARCHAR(10), service_activation_date, 23) AS dato,
                CAST(value_dkk AS INT) AS value,
                owner_name,
                YEAR(service_activation_date) AS aar,
                COALESCE([sites], '') AS sites
            FROM [dbo].[PipedriveDeals]
            WHERE org_id = %s
              AND account = %s
              AND pipeline_name = %s
              AND org_id IS NOT NULL
              AND service_activation_date >= '2023-01-01'
              AND status = 'won'
            ORDER BY service_activation_date DESC
        """, (org_id, account, pipeline))
        deals = [
            {
                "title":      r["title"] or "(Uden titel)",
                "ad_type":    r["ad_type"] or "—",
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
        logger.exception("db_customer_history fejlede (pipeline=%s, org_id=%s, country=%s)", pipeline, org_id, country)
        raise


def db_all_deals(pipeline: str, owner_name: str | None = None, country: str = "dk") -> list[dict]:
    """Alle vundne deals på deal-niveau for en pipeline, grupperet pr. kunde.

    Samme deal-felter som db_customer_history, men på tværs af alle kunder, så
    heatmap-widget'en kan trække de enkelte deals under hver kunde til Excel i ét
    udtræk. Spejler heatmap'ens afgrænsning (account/land + pipeline + valgfrit
    owner, alle år).
    """
    account = account_for_country(country)
    oc, op = _owner_clause(owner_name)
    params = (account, pipeline) + tuple(op)
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(f"""
            SELECT
                org_id,
                org_name,
                title,
                COALESCE(ad_type, '') AS ad_type,
                CONVERT(NVARCHAR(10), service_activation_date, 23) AS dato,
                CAST(value_dkk AS INT) AS value,
                owner_name,
                YEAR(service_activation_date) AS aar,
                COALESCE([sites], '') AS sites
            FROM [dbo].[PipedriveDeals]
            WHERE {_BASE_WHERE} {oc}
              AND status = 'won'
            ORDER BY org_name, service_activation_date DESC
        """, params)
        rows = [
            {
                "org_id":     r["org_id"],
                "org_name":   r["org_name"] or "—",
                "title":      r["title"] or "(Uden titel)",
                "ad_type":    r["ad_type"] or "—",
                "dato":       r["dato"] or "—",
                "value":      int(r["value"] or 0),
                "owner_name": r["owner_name"] or "—",
                "aar":        int(r["aar"]),
                "sites":      r["sites"] or "—",
            }
            for r in cur.fetchall()
        ]
        conn.close()
        return rows
    except Exception:
        logger.exception("db_all_deals fejlede (pipeline=%s, owner=%s, country=%s)", pipeline, owner_name, country)
        raise

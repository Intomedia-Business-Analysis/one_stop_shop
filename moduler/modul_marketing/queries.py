"""Marketing — Deal Source dashboard.

Læser PipedriveDeals og giver marketing-afdelingen overblik over hvilke
leads (deal_source) der konverterer til betalende abonnementer pr.
account/site, i en valgt tidsperiode.

Datofilter bruger service_activation_date (samme dato-felt som de øvrige
dashboards), så tallene matcher Banner/Job, Forecast og Performance.

Perf-noter
----------
Site-filteret er en EXISTS-subquery mod STRING_SPLIT(d.sites). Det er hurtigere
end CROSS APPLY i hovedqueryen fordi det ikke multiplicerer rækker — så vi
slipper for DISTINCT på 13 kolonner.
"""
import logging
import os
from typing import Optional

import pymssql
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


# Fælles pooled DB-forbindelse — se db.py.
from db import get_conn  # noqa: E402,F401


# Samme udelukkelse som modul_perf bruger — administrative deals og rapport-
# deals tæller ikke som rigtig konvertering for marketing.
_ADM_EXCLUDE = (
    " AND COALESCE(d.[administrativ],'') <> 'ja'"
    " AND UPPER(LTRIM(d.[title])) NOT LIKE 'ADMINISTRATIV%'"
    " AND UPPER(LTRIM(d.[title])) NOT LIKE 'ADM %'"
    " AND COALESCE(d.[deal_type],'') <> 'Rapport'"
)

# Web Sale-pipelinens deals har ingen deal_source — de bidrager kun til støj
# i en kilde-analyse, så vi ekskluderer dem.
_WEB_SALE_EXCLUDE = " AND COALESCE(d.[pipeline_name],'') <> 'Web Sale'"


def _clean_list(values) -> list[str]:
    """Normalisér en query-param: list/str/None -> list[str] uden tomme."""
    if not values:
        return []
    if isinstance(values, str):
        values = [values]
    return [v.strip() for v in values if v and v.strip()]


def _in_placeholders(n: int) -> str:
    return "(" + ",".join(["%s"] * n) + ")"


def _filter_clause(
    accounts,
    sites,
    deal_sources,
    owners,
    date_from: Optional[str],
    date_to: Optional[str],
) -> tuple[str, list]:
    """Byg WHERE-fragment + parametre. Alle list-filtre er valgfri."""
    accounts     = _clean_list(accounts)
    sites        = _clean_list(sites)
    deal_sources = _clean_list(deal_sources)
    owners       = _clean_list(owners)

    clauses: list[str] = []
    params: list = []

    if accounts:
        clauses.append(f"d.account IN {_in_placeholders(len(accounts))}")
        params.extend(accounts)
    if deal_sources:
        clauses.append(f"d.deal_source IN {_in_placeholders(len(deal_sources))}")
        params.extend(deal_sources)
    if owners:
        clauses.append(f"d.owner_name IN {_in_placeholders(len(owners))}")
        params.extend(owners)
    if sites:
        # EXISTS er hurtigere end CROSS APPLY i hovedqueryen — ingen row-
        # multiplication, ingen DISTINCT nødvendig på outer SELECT.
        clauses.append(f"""EXISTS (
            SELECT 1 FROM STRING_SPLIT(d.sites, ',') ss
            WHERE LTRIM(RTRIM(ss.value)) IN {_in_placeholders(len(sites))}
        )""")
        params.extend(sites)
    if date_from:
        clauses.append("d.service_activation_date >= %s")
        params.append(date_from)
    if date_to:
        clauses.append("d.service_activation_date <= %s")
        params.append(date_to)
    where = (" AND " + " AND ".join(clauses)) if clauses else ""
    return where, params


def db_filter_options(accounts=None) -> dict:
    """Distinkte værdier til filter-dropdowns. Tomme/NULL frasorteres.

    accounts (valgfri): når sat, afgrænses deal_sources til kun de kilder der
    optræder på den/de valgte account(s) — så dropdownen kun viser relevante
    kilder. account-listen selv forbliver global (man kan altid skifte account).
    """
    sel_accounts = _clean_list(accounts)
    acc_clause = ""
    acc_params: list = []
    if sel_accounts:
        acc_clause = f"AND d.account IN {_in_placeholders(len(sel_accounts))}"
        acc_params = list(sel_accounts)
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)

        cur.execute("""
            SELECT DISTINCT LTRIM(RTRIM(account)) AS v
            FROM [dbo].[PipedriveDeals]
            WHERE account IS NOT NULL AND LTRIM(RTRIM(account)) <> ''
            ORDER BY v
        """)
        accounts = [r["v"] for r in cur.fetchall()]

        cur.execute("""
            SELECT DISTINCT LTRIM(RTRIM(s.value)) AS v
            FROM [dbo].[PipedriveDeals] d
            CROSS APPLY STRING_SPLIT(d.sites, ',') AS s
            WHERE d.sites IS NOT NULL AND LTRIM(RTRIM(s.value)) <> ''
            ORDER BY v
        """)
        sites = [r["v"] for r in cur.fetchall()]

        cur.execute(f"""
            SELECT DISTINCT LTRIM(RTRIM(d.deal_source)) AS v
            FROM [dbo].[PipedriveDeals] d
            WHERE d.deal_source IS NOT NULL AND LTRIM(RTRIM(d.deal_source)) <> ''
              {acc_clause}
              {_ADM_EXCLUDE}
              {_WEB_SALE_EXCLUDE}
            ORDER BY v
        """, tuple(acc_params))
        sources = [r["v"] for r in cur.fetchall()]

        cur.execute(f"""
            SELECT DISTINCT LTRIM(RTRIM(d.owner_name)) AS v
            FROM [dbo].[PipedriveDeals] d
            WHERE d.owner_name IS NOT NULL AND LTRIM(RTRIM(d.owner_name)) <> ''
              AND d.owner_name <> 'System Admin'
              {_ADM_EXCLUDE}
              {_WEB_SALE_EXCLUDE}
            ORDER BY v
        """)
        owners = [r["v"] for r in cur.fetchall()]

        conn.close()
        return {"accounts": accounts, "sites": sites, "deal_sources": sources, "owners": owners}
    except Exception:
        logger.exception("db_filter_options fejlede")
        raise


def db_summary(
    accounts=None,
    sites=None,
    deal_sources=None,
    owners=None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> dict:
    """KPI'er + status-counts.

    Én query med conditional aggregation:
      - Revenue/KPIs beregnes kun på status='won' + value_dkk er sat
      - status_counts tæller ALLE matchende deals (won/open/lost) uanset value_dkk
    """
    where, params = _filter_clause(accounts, sites, deal_sources, owners, date_from, date_to)
    sql = f"""
        SELECT
            ISNULL(CAST(SUM(CASE WHEN d.status = 'won' AND d.value_dkk IS NOT NULL
                                 THEN d.value_dkk ELSE 0 END) AS BIGINT), 0) AS total_value,
            SUM(CASE WHEN d.status = 'won' AND d.value_dkk IS NOT NULL THEN 1 ELSE 0 END) AS total_deals,
            COUNT(DISTINCT CASE WHEN d.status = 'won' AND d.value_dkk IS NOT NULL
                                THEN d.org_id END)                                       AS unique_customers,
            SUM(CASE WHEN d.status = 'won'  THEN 1 ELSE 0 END) AS status_won,
            SUM(CASE WHEN d.status = 'open' THEN 1 ELSE 0 END) AS status_open,
            SUM(CASE WHEN d.status = 'lost' THEN 1 ELSE 0 END) AS status_lost
        FROM [dbo].[PipedriveDeals] d
        WHERE 1=1
          {_ADM_EXCLUDE}
          {_WEB_SALE_EXCLUDE}
          {where}
    """
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(sql, tuple(params))
        row = cur.fetchone() or {}
        conn.close()
        total_value      = int(row.get("total_value") or 0)
        total_deals      = int(row.get("total_deals") or 0)
        unique_customers = int(row.get("unique_customers") or 0)
        avg_deal         = round(total_value / total_deals) if total_deals else 0
        return {
            "total_value":      total_value,
            "total_deals":      total_deals,
            "unique_customers": unique_customers,
            "avg_deal":         avg_deal,
            "status_counts": {
                "won":  int(row.get("status_won")  or 0),
                "open": int(row.get("status_open") or 0),
                "lost": int(row.get("status_lost") or 0),
            },
        }
    except Exception:
        logger.exception("db_summary fejlede")
        raise


def db_by_account(
    sites=None,
    deal_sources=None,
    owners=None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> list[dict]:
    """Per-account breakdown for de aktuelle filtre.

    Bemærk: accounts-filteret er bevidst udeladt — meningen er at sammenligne
    HVORDAN en deal source performer PÅ TVÆRS AF accounts. Brugerens
    account-valg bruges kun til at indsnævre KPI'er og deals-tabellen.
    """
    where, params = _filter_clause(None, sites, deal_sources, owners, date_from, date_to)
    sql = f"""
        SELECT
            ISNULL(NULLIF(LTRIM(RTRIM(d.account)),''), '(ingen)') AS account,
            SUM(CASE WHEN d.status = 'won'  THEN 1 ELSE 0 END) AS won_count,
            SUM(CASE WHEN d.status = 'open' THEN 1 ELSE 0 END) AS open_count,
            SUM(CASE WHEN d.status = 'lost' THEN 1 ELSE 0 END) AS lost_count,
            ISNULL(CAST(SUM(CASE WHEN d.status = 'won' AND d.value_dkk IS NOT NULL
                                 THEN d.value_dkk ELSE 0 END) AS BIGINT), 0) AS won_revenue,
            COUNT(DISTINCT CASE WHEN d.status = 'won' AND d.value_dkk IS NOT NULL
                                THEN d.org_id END) AS won_customers,
            COUNT(*) AS total_count
        FROM [dbo].[PipedriveDeals] d
        WHERE 1=1
          {_ADM_EXCLUDE}
          {_WEB_SALE_EXCLUDE}
          {where}
        GROUP BY ISNULL(NULLIF(LTRIM(RTRIM(d.account)),''), '(ingen)')
        ORDER BY won_revenue DESC, total_count DESC
    """
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(sql, tuple(params))
        rows = [
            {
                "account":       r["account"],
                "won":           int(r["won_count"]   or 0),
                "open":          int(r["open_count"]  or 0),
                "lost":          int(r["lost_count"]  or 0),
                "won_revenue":   int(r["won_revenue"] or 0),
                "won_customers": int(r["won_customers"] or 0),
                "total":         int(r["total_count"] or 0),
            }
            for r in cur.fetchall()
        ]
        conn.close()
        return rows
    except Exception:
        logger.exception("db_by_account fejlede")
        raise


_ALLOWED_DRILLDOWN_STATUSES = ("won", "open", "lost")


def db_account_deals(
    account: str,
    status: str,
    sites=None,
    deal_sources=None,
    owners=None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> list[dict]:
    """Drill-down: deals for én account + status, med pipeline + expected close.

    Bruges af modalen der åbnes ved klik på en status-celle i
    'Performance pr. Account'-tabellen.
    """
    status = (status or "").lower()
    if status not in _ALLOWED_DRILLDOWN_STATUSES:
        raise ValueError(f"Ugyldig status: {status!r}")
    if not account:
        raise ValueError("account er påkrævet")

    # Account-filteret håndteres direkte her (én værdi), så vi bypasser den
    # almindelige _filter_clause's IN-liste.
    where, params = _filter_clause(None, sites, deal_sources, owners, date_from, date_to)
    account_clause = "d.account = %s" if account != "(ingen)" else (
        "(d.account IS NULL OR LTRIM(RTRIM(d.account)) = '')"
    )
    account_params = (account,) if account != "(ingen)" else ()

    sql = f"""
        SELECT
            d.pd_deal_id,
            d.title,
            d.org_id,
            d.org_name,
            d.account,
            d.sites,
            d.pipeline_name,
            d.deal_source,
            d.deal_type,
            d.owner_name,
            d.value,
            d.value_dkk,
            d.currency,
            CONVERT(NVARCHAR(10), d.expected_close_date,     23) AS expected_close_date,
            CONVERT(NVARCHAR(10), d.service_activation_date, 23) AS service_activation_date
        FROM [dbo].[PipedriveDeals] d
        WHERE d.status = %s
          AND {account_clause}
          {_ADM_EXCLUDE}
          {_WEB_SALE_EXCLUDE}
          {where}
        ORDER BY
            CASE WHEN d.expected_close_date IS NULL THEN 1 ELSE 0 END,
            d.expected_close_date ASC,
            d.pd_deal_id DESC
    """
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(sql, (status, *account_params, *params))
        rows = []
        for r in cur.fetchall():
            rows.append({
                "deal_id":                 r.get("pd_deal_id"),
                "title":                   r.get("title") or "—",
                "org_id":                  r.get("org_id"),
                "org_name":                r.get("org_name") or "—",
                "account":                 r.get("account") or "—",
                "sites":                   r.get("sites") or "—",
                "pipeline":                r.get("pipeline_name") or "—",
                "deal_source":             r.get("deal_source") or "—",
                "deal_type":               r.get("deal_type") or "—",
                "owner_name":              r.get("owner_name") or "—",
                "value":                   float(r.get("value") or 0),
                "value_dkk":               float(r.get("value_dkk") or 0),
                "currency":                r.get("currency") or "DKK",
                "expected_close_date":     r.get("expected_close_date") or "",
                "service_activation_date": r.get("service_activation_date") or "",
            })
        conn.close()
        return rows
    except Exception:
        logger.exception("db_account_deals fejlede")
        raise


_SORT_COLUMNS = {
    "service_activation_date": "d.service_activation_date",
    "org_name":                "d.org_name",
    "deal_source":             "d.deal_source",
    "owner_name":              "d.owner_name",
    "value_dkk":               "d.value_dkk",
}


def db_deals(
    accounts=None,
    sites=None,
    deal_sources=None,
    owners=None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
    sort_by: str = "service_activation_date",
    sort_dir: str = "desc",
) -> dict:
    """Paginerede deals + total-tæller. SQL Server OFFSET/FETCH NEXT."""
    page      = max(1, int(page or 1))
    page_size = max(1, min(500, int(page_size or 50)))
    offset    = (page - 1) * page_size
    sort_col  = _SORT_COLUMNS.get(sort_by, "d.service_activation_date")
    sort_dir  = "ASC" if str(sort_dir).lower() == "asc" else "DESC"
    where, params = _filter_clause(accounts, sites, deal_sources, owners, date_from, date_to)

    # Én round-trip: COUNT(*) OVER() returnerer total uden separat query.
    sql = f"""
        SELECT
            d.pd_deal_id,
            d.title,
            d.org_id,
            d.org_name,
            d.account,
            d.deal_source,
            d.sites,
            d.deal_type,
            d.owner_name,
            d.value,
            d.value_dkk,
            d.currency,
            CONVERT(NVARCHAR(10), d.service_activation_date, 23) AS service_activation_date,
            COUNT(*) OVER() AS total_rows
        FROM [dbo].[PipedriveDeals] d
        WHERE d.status = 'won'
          AND d.value_dkk IS NOT NULL
          {_ADM_EXCLUDE}
          {_WEB_SALE_EXCLUDE}
          {where}
        ORDER BY {sort_col} {sort_dir}, d.pd_deal_id DESC
        OFFSET %s ROWS FETCH NEXT %s ROWS ONLY
    """
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(sql, tuple(params) + (offset, page_size))
        rows = []
        total = 0
        for r in cur.fetchall():
            total = int(r.get("total_rows") or 0)  # samme værdi i alle rækker
            rows.append({
                "deal_id":                 r.get("pd_deal_id"),
                "title":                   r.get("title") or "—",
                "org_id":                  r.get("org_id"),
                "org_name":                r.get("org_name") or "—",
                "account":                 r.get("account") or "—",
                "deal_source":             r.get("deal_source") or "—",
                "sites":                   r.get("sites") or "—",
                "deal_type":               r.get("deal_type") or "—",
                "owner_name":              r.get("owner_name") or "—",
                "value":                   float(r.get("value") or 0),
                "value_dkk":               float(r.get("value_dkk") or 0),
                "currency":                r.get("currency") or "DKK",
                "service_activation_date": r.get("service_activation_date") or "",
            })
        conn.close()
        return {
            "rows":      rows,
            "total":     total,
            "page":      page,
            "page_size": page_size,
            "sort_by":   sort_by,
            "sort_dir":  sort_dir.lower(),
        }
    except Exception:
        logger.exception("db_deals fejlede")
        raise

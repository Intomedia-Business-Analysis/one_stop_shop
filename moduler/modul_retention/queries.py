from db import get_conn
import logging

logger = logging.getLogger(__name__)


def db_monthly_active_counts(owner_name: str | None = None,
                             teams: list | None = None) -> list:
    """Antal aktive org+site-kombinationer pr. måned — retention-trendlinjen."""
    clause = ''
    params: tuple = ()

    if owner_name:
        clause += ' AND o.owner_name = %s'
        params += (owner_name,)
    if teams:
        ph = ','.join(['%s'] * len(teams))
        clause += f' AND o.team IN ({ph})'
        params += tuple(teams)

    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(
           f"""SELECT FirstDayOfMonth,
             COUNT(*) AS active_count,
             COUNT(o.owner_name) AS attributed_count
            FROM dbo.retention r
            LEFT JOIN dbo.retention_owner o
            ON o.account = r.account
            AND o.org_id = r.org_id
            AND ISNULL(o.sites, '') = ISNULL(r.sites, '')
            WHERE FirstDayOfMonth <= EOMONTH(GETDATE())
                {clause}
            GROUP BY FirstDayOfMonth
            ORDER BY FirstDayOfMonth;""",
            params,
        )
        result = cur.fetchall()
        conn.close()
        return result
    except Exception:
        logger.exception("db_monthly_active_counts fejlede")
        return []
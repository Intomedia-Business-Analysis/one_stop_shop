from db import get_conn
import logging

logger = logging.getLogger(__name__)


def db_monthly_active_counts() -> list:
    """Antal aktive org+site-kombinationer pr. måned — retention-trendlinjen."""
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(
            """SELECT FirstDayOfMonth, COUNT(*) AS active_count
            FROM dbo.retention
            WHERE FirstDayOfMonth <= EOMONTH(GETDATE())
            GROUP BY FirstDayOfMonth
            ORDER BY FirstDayOfMonth;"""
        )
        result = cur.fetchall()
        conn.close()
        return result
    except Exception:
        logger.exception("db_monthly_active_counts fejlede")
        return []
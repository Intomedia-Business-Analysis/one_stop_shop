"""Kilde til administrative PipeDrive-deals.

Tyndt adapter-interface (PipeDriveAdminSource) så kilden kan udskiftes uden at
røre matcheren. Default-implementeringen (SyncedDbAdminSource) læser fra den
allerede synkede PipedriveDeals-tabel — administrative deals er dem hvor
[administrativ] = 'ja'. En live-API-implementering kan tilføjes senere uden
ændringer andre steder.

month_end på en AdminDeal er sidste dag i service_activation_date'ens måned
(EOMONTH), så nøglen ligger på samme måneds-grid som Zuoras month_end.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Optional

from db import get_conn
from moduler.modul_admin_nysalg.matcher import last_day_of_month
from moduler.modul_admin_nysalg.models import AdminDeal

logger = logging.getLogger(__name__)

# Værdien i PipedriveDeals.[administrativ] der markerer en deal som administrativ.
# (Samme markør som perf-/forecast-modulerne filtrerer på.)
ADMIN_FLAG_VALUE = "ja"


class PipeDriveAdminSource(ABC):
    """Adapter-interface: hent administrative deals for en periode."""

    @abstractmethod
    def fetch_admin_deals(self, period: Optional[str] = None) -> list[AdminDeal]:
        """period = 'YYYY-MM' afgrænser til service_activation_date i den måned.
        None = alle administrative deals.
        """
        raise NotImplementedError


class SyncedDbAdminSource(PipeDriveAdminSource):
    """Default: læs administrative deals fra den synkede PipedriveDeals-tabel."""

    def fetch_admin_deals(self, period: Optional[str] = None) -> list[AdminDeal]:
        where = [
            "LOWER(LTRIM(RTRIM(COALESCE([administrativ],'')))) = %s",
            "[service_activation_date] IS NOT NULL",
            "[org_id] IS NOT NULL",
        ]
        params: list = [ADMIN_FLAG_VALUE]
        if period:
            try:
                y, m = period.split("-")
                where.append("YEAR([service_activation_date]) = %s AND MONTH([service_activation_date]) = %s")
                params.extend([int(y), int(m)])
            except (ValueError, AttributeError):
                logger.warning("Ugyldig periode %r — ignorerer periodefilter", period)

        sql = f"""
            SELECT
                [pd_deal_id], [org_id], [org_name], [sites],
                [value], [value_dkk], [currency],
                CONVERT(varchar(10), [service_activation_date], 23) AS sad,
                [pipeline_name], [status]
            FROM [dbo].[PipedriveDeals]
            WHERE {' AND '.join(where)}
        """
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
        conn.close()

        deals: list[AdminDeal] = []
        for r in rows:
            sad = r.get("sad")
            if not sad:
                continue
            deals.append(AdminDeal(
                deal_id=str(r.get("pd_deal_id") or ""),
                org_id=str(r.get("org_id") or "").strip(),
                site=(r.get("sites") or "").strip(),
                month_end=last_day_of_month(sad),
                value=float(r.get("value") or 0),
                pipeline=r.get("pipeline_name") or "",
                status=r.get("status") or "",
                org_name=(r.get("org_name") or "").strip(),
            ))
        return deals


def get_default_source() -> PipeDriveAdminSource:
    """Fabrik — gør det let at skifte default-kilde ét sted."""
    return SyncedDbAdminSource()

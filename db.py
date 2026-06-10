"""Fælles databaseadgang med connection-pool.

Alle moduler henter forbindelser herfra via get_conn(). Tidligere åbnede hvert
query-kald sin egen pymssql-forbindelse (TCP + TLS + login pr. kald) — med
poolen genbruges forbindelserne, hvilket gør dashboard-load mærkbart hurtigere.

Designet er drop-in-kompatibelt med den gamle kode: get_conn() returnerer et
objekt der opfører sig som en pymssql-Connection, og .close() lægger
forbindelsen tilbage i poolen i stedet for at lukke den. Eksisterende
`conn = get_conn() ... conn.close()`-kode virker uændret.

Undtagelse: modul_portfolio_alignment har sin egen forbindelse (længere
timeouts til Pipedrive-sync) og bruger ikke poolen.
"""
import os
import queue
import pymssql
from dotenv import load_dotenv

load_dotenv()

# Maks. antal ledige forbindelser der holdes i live. Flere samtidige brugere
# end dette giver blot friske forbindelser, der lukkes reelt ved aflevering.
_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "10"))

# Query-timeout: 15 s er den højeste værdi, modulerne brugte før poolen
# (marketing). Lavere pr.-modul-værdier (5/10 s) er bevidst løftet hertil —
# det ændrer kun, hvor længe en LANGSOM query må køre, ikke normal drift.
_QUERY_TIMEOUT = int(os.getenv("DB_QUERY_TIMEOUT", "15"))

_pool: "queue.LifoQueue[pymssql.Connection]" = queue.LifoQueue(maxsize=_POOL_SIZE)


def _new_raw_conn():
    return pymssql.connect(
        server=os.getenv("DB_SERVER"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME", "INTOMEDIA"),
        tds_version="7.0",
        login_timeout=5,
        timeout=_QUERY_TIMEOUT,
    )


class PooledConnection:
    """Tynd wrapper om en pymssql-Connection: close() afleverer til poolen."""

    def __init__(self, raw):
        self._raw = raw

    def __getattr__(self, name):
        return getattr(self._raw, name)

    def close(self):
        raw, self._raw = self._raw, None
        if raw is None:  # allerede lukket
            return
        try:
            # Ryd evt. åben transaktion, så næste låner får en ren forbindelse.
            raw.rollback()
        except Exception:
            try:
                raw.close()
            except Exception:
                pass
            return
        try:
            _pool.put_nowait(raw)
        except queue.Full:
            try:
                raw.close()
            except Exception:
                pass

    # Hvis kalderen glemmer close(), afleveres forbindelsen ved garbage collection.
    def __del__(self):
        try:
            self.close()
        except Exception:
            pass


def get_conn() -> PooledConnection:
    """Hent en forbindelse fra poolen (eller opret en ny).

    Genbrugte forbindelser sundhedstjekkes med SELECT 1, så en forbindelse
    SQL Server har droppet i mellemtiden (idle timeout, failover) kasseres
    stille i stedet for at give en fejl midt i et dashboard.
    """
    while True:
        try:
            raw = _pool.get_nowait()
        except queue.Empty:
            return PooledConnection(_new_raw_conn())
        try:
            cur = raw.cursor()
            cur.execute("SELECT 1")
            cur.fetchall()
            cur.close()
            return PooledConnection(raw)
        except Exception:
            try:
                raw.close()
            except Exception:
                pass

"""Fælles databaseadgang med connection-pool.

Alle moduler henter forbindelser herfra via get_conn(). Tidligere åbnede hvert
query-kald sin egen pymssql-forbindelse (TCP + TLS + login pr. kald) — med
poolen genbruges forbindelserne, hvilket gør dashboard-load mærkbart hurtigere.

Designet er drop-in-kompatibelt med den gamle kode: get_conn() returnerer et
objekt der opfører sig som en pymssql-Connection, og .close() lægger
forbindelsen tilbage i poolen i stedet for at lukke den. Eksisterende
`conn = get_conn() ... conn.close()`-kode virker uændret.

Forbindelsen defineres KUN her — også for de moduler der har brug for andre
timeouts end poolens (Portfolio Alignment og Sælger-portefølje). De kalder
new_connection() i stedet for at gentage pymssql.connect() med egne værdier,
så der findes ét sted at rette server, login og TDS-version.

── To ting der ændrede sig ved flytningen til den nye server ────────────────

1. TDS-VERSION. Koden havde tds_version="7.0" hårdkodet fire steder. TDS 7.0
   kan ikke TLS, og servere der kræver kryptering afviser den. Standarden er
   nu 7.4 (DB_TDS_VERSION overstyrer).

2. DATOTYPER, som følge af 1. TDS 7.0 kender ikke `date` og `datetime2` — de
   kom i 7.3 — så SQL Server sendte dem som STRENGE ('2026-08-14'). Fra 7.4
   kommer de tilbage som rigtige Python-objekter.

   Det er en bedre verden, men det ændrer datatypen under fødderne på 16
   moduler på én gang, og en forkert type viser sig som "Data utilgængelig" i
   et dashboard — ikke som en fejl nogen ser. Derfor pakkes cursors ind, så
   DATE-kolonner leveres som 'YYYY-MM-DD'-strenge præcis som før:

       DB_DATE_AS_STRING=1   (standard) — som TDS 7.0. Sikker ved flytningen.
       DB_DATE_AS_STRING=0              — rigtige date-objekter.

   Broen er MENT som midlertidig. Slå den fra når modulerne er gennemgået ét
   for ét; det er kun DATE-kolonner der berøres, og de fleste queries
   konverterer allerede selv med CONVERT(NVARCHAR(10), ...) i SQL'en og er
   derfor upåvirkede uanset indstillingen.

   Bemærk det broen IKKE kan: DATETIME2-kolonner kom også som strenge under
   7.0, men de er umulige at skelne fra almindelige DATETIME-kolonner (begge
   ankommer som datetime), så dem leveres som datetime uanset indstilling.
   Det kendte sted der læser sådanne felter — modul_retention/outcomes.py —
   normaliserer selv og tager begge former.
"""
import datetime as _dt
import os
import queue

import pymssql

from env import load_env

load_env()

# Maks. antal ledige forbindelser der holdes i live. Flere samtidige brugere
# end dette giver blot friske forbindelser, der lukkes reelt ved aflevering.
_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "10"))

# Query-timeout: 15 s er den højeste værdi, modulerne brugte før poolen
# (marketing). Lavere pr.-modul-værdier (5/10 s) er bevidst løftet hertil —
# det ændrer kun, hvor længe en LANGSOM query må køre, ikke normal drift.
_QUERY_TIMEOUT = int(os.getenv("DB_QUERY_TIMEOUT", "15"))

# 7.4 er påkrævet mod servere der kræver kryptering; 7.0 kan ikke TLS.
_TDS_VERSION = os.getenv("DB_TDS_VERSION", "7.4")

_pool: "queue.LifoQueue[pymssql.Connection]" = queue.LifoQueue(maxsize=_POOL_SIZE)


def _flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in ("1", "yes", "y", "true", "ja")


# Se punkt 2 i modul-docstringen.
DATE_AS_STRING = _flag("DB_DATE_AS_STRING", True)


def _required(name: str) -> str:
    """
    Læs en påkrævet miljøvariabel, eller fejl med en brugbar besked.

    Før blev DB_SERVER/DB_USER/DB_PASSWORD sendt videre som None hvis de
    manglede — og fejlen kom først som en kryptisk login-fejl fra driveren,
    typisk oversat til "Data utilgængelig" i et dashboard.
    """
    val = os.getenv(name)
    if not val:
        raise RuntimeError(
            f"{name} mangler. Sæt den i .env eller som miljøvariabel "
            f"(se env.py for hvor .env læses fra). Kør preflight.py for et "
            f"samlet tjek af opsætningen."
        )
    return val


# ── Datokompatibilitet ──────────────────────────────────────────────────────

def _legacy(value):
    """DATE → 'YYYY-MM-DD'. Alt andet slipper uændret igennem.

    datetime er en subklasse af date, så typen tjekkes præcist: kun rene
    DATE-kolonner konverteres, ikke DATETIME/DATETIME2.
    """
    if type(value) is _dt.date:
        return value.isoformat()
    return value


def _legacy_row(row):
    if row is None:
        return None
    if isinstance(row, dict):                      # cursor(as_dict=True)
        return {k: _legacy(v) for k, v in row.items()}
    if isinstance(row, tuple):
        return tuple(_legacy(v) for v in row)
    if isinstance(row, list):
        return [_legacy(v) for v in row]
    return row


class _LegacyDateCursor:
    """Cursor-wrapper der giver DATE-kolonner tilbage som strenge (som TDS 7.0)."""

    def __init__(self, raw):
        self._raw = raw

    def __getattr__(self, name):
        return getattr(self._raw, name)

    def fetchone(self):
        return _legacy_row(self._raw.fetchone())

    def fetchmany(self, *args, **kwargs):
        return [_legacy_row(r) for r in self._raw.fetchmany(*args, **kwargs)]

    def fetchall(self):
        return [_legacy_row(r) for r in self._raw.fetchall()]

    def __iter__(self):
        for row in self._raw:
            yield _legacy_row(row)

    def __enter__(self):
        self._raw.__enter__()
        return self

    def __exit__(self, *exc):
        return self._raw.__exit__(*exc)


class _ConnectionProxy:
    """Fælles wrapper: cursor() pakkes ind, så datotyperne er som aftalt."""

    def __init__(self, raw):
        self._raw = raw

    def __getattr__(self, name):
        return getattr(self._raw, name)

    def cursor(self, *args, **kwargs):
        cur = self._raw.cursor(*args, **kwargs)
        return _LegacyDateCursor(cur) if DATE_AS_STRING else cur

    def close(self):
        if self._raw is not None:
            raw, self._raw = self._raw, None
            raw.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


# ── Forbindelser ────────────────────────────────────────────────────────────

def _new_raw_conn(login_timeout: int = 5, timeout: int | None = None):
    """Rå pymssql-forbindelse. Eneste sted forbindelsen defineres."""
    return pymssql.connect(
        server=_required("DB_SERVER"),
        user=_required("DB_USER"),          # SQL-login, eller DOMÆNE\\bruger til NTLM
        password=_required("DB_PASSWORD"),
        database=os.getenv("DB_NAME", "INTOMEDIA"),
        tds_version=_TDS_VERSION,
        login_timeout=login_timeout,
        timeout=_QUERY_TIMEOUT if timeout is None else timeout,
    )


def new_connection(login_timeout: int = 5, timeout: int | None = None):
    """
    En frisk forbindelse UDEN for poolen, med egne timeouts.

    Til de moduler der bevidst vil have andre grænser end poolens: Portfolio
    Alignment (tunge Pipedrive-sync-queries) og Sælger-portefølje. De havde
    hver sin pymssql.connect() med tds_version="7.0" — nu ligger valget her.
    Kalderen lukker den selv; den afleveres ikke til poolen.
    """
    return _ConnectionProxy(_new_raw_conn(login_timeout=login_timeout, timeout=timeout))


class PooledConnection(_ConnectionProxy):
    """Tynd wrapper om en pymssql-Connection: close() afleverer til poolen."""

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


# ── Opstartstjek ────────────────────────────────────────────────────────────

def check_connection() -> dict:
    """
    Verificér forbindelsen og returnér server, database og login.

    Bruges af preflight.py, så en forkert server eller et mislykket login
    opdages FØR appen startes — i stedet for at hvert dashboard viser
    "Data utilgængelig" uden at sige hvorfor.
    """
    conn = new_connection(login_timeout=5, timeout=10)
    try:
        cur = conn.cursor()
        cur.execute("SELECT SUSER_NAME(), DB_NAME(), @@SERVERNAME")
        login, database, servername = cur.fetchone()
        return {
            "server":         os.getenv("DB_SERVER"),
            "servername":     servername,
            "login":          login,
            "database":       database,
            "tds_version":    _TDS_VERSION,
            "date_as_string": DATE_AS_STRING,
            "pool_size":      _POOL_SIZE,
        }
    finally:
        conn.close()


def table_exists(name: str) -> bool:
    """True hvis tabellen findes i den forbundne database (uden skema-præfiks)."""
    conn = new_connection(login_timeout=5, timeout=10)
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM sys.tables WHERE name = %s", (name,))
        return cur.fetchone() is not None
    finally:
        conn.close()

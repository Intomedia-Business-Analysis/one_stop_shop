"""Kørselslog til maintenance-dashboardet — ORIGINALEN af en spejlkopi.

Hvert af de scripts, Task Scheduler kører, skriver én række pr. kørsel til
dbo.HubDataLoads. Dashboardet på /tools/maintenance/ læser den række og kan
derfor svare på det, ingen datatabel selv kan svare på: kørte scriptet, og gik
det godt — også når kørslen intet nyt fandt.

Uden loggen er det eneste tilgængelige signal "nyeste dato i tabellen", og den
kan ikke skelne tre tilstande, der betyder vidt forskellige ting:

    scriptet kørte og hentede nye rækker        (alt er som det skal være)
    scriptet kørte, men der var intet nyt       (helt normalt i en weekend)
    scriptet kørte slet ikke, eller det fejlede (nogen skal kigge på det)

Brug
----
    from dataloads import record_run

    with record_run("currencycollector", "valutakurser") as run:
        n = hent_og_skriv()
        run.rows(n)
        run.data_through(seneste_kursdato)

Ved en undtagelse skrives status='error' med fejlteksten, og undtagelsen kastes
videre uændret — record_run() ændrer aldrig kørslens forløb.

To ting der er bevidste
-----------------------
1.  LOGNINGEN MÅ ALDRIG VÆLTE KØRSLEN. Alle fejl i selve logningen sluges og
    skrives kun til stdout. En utilgængelig logtabel skal koste et hul i
    dashboardet, ikke en manglende valutakurs.

2.  RÆKKEN SKRIVES TO GANGE: status='running' ved start, og 'ok'/'error' ved
    slut. Det koster en ekstra UPDATE, men det er netop det, der gør en kørsel,
    der blev dræbt undervejs (serveren genstartede, processen hang), synlig —
    den efterlader en 'running'-række, der aldrig blev lukket. Med kun én
    skrivning til sidst ville sådan en kørsel se ud, som om den aldrig var
    begyndt.

SPEJLKOPI
---------
Denne fil ligger identisk i pipedrive_sync, ACV_updater_pipedrive,
currencycollector og ProgrammaticFinansSales. Den importerer kun get_conn (og
valgfrit _P) fra projektets eget db.py, som alle fem har, så kopien kan lægges
uændret over. Rettes den ét sted, skal den rettes alle fem — originalen er
denne fil, og tests/test_dataloads_spejlkopi.py fejler hvis kopierne driver fra
den.
"""
import socket
import traceback
from contextlib import contextmanager
from datetime import datetime

from db import get_conn

try:
    # Sidemapperne understøtter både pymssql og pyodbc og eksporterer derfor
    # deres parameter-pladsholder. one_stop_shop bruger pymssql direkte og har
    # ingen — der er '%s' altid det rigtige.
    from db import _P
except ImportError:
    _P = "%s"

TABLE = "HubDataLoads"

_CREATE_SQL = f"""
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{TABLE}' AND xtype='U')
CREATE TABLE dbo.{TABLE} (
    id            INT IDENTITY(1,1) PRIMARY KEY,
    source        NVARCHAR(100)  NOT NULL,   -- scriptets navn, fx 'pipedrive_sync'
    target_table  NVARCHAR(128)  NOT NULL,   -- tabellen der blev skrevet til
    scope         NVARCHAR(100)  NULL,       -- delmaengde, fx Pipedrive-kontoen
    started_at    DATETIME2(0)   NOT NULL,   -- serverens lokale tid (dansk)
    finished_at   DATETIME2(0)   NULL,       -- NULL = koerslen er i gang (eller doede)
    status        NVARCHAR(20)   NOT NULL,   -- 'running' | 'ok' | 'error'
    rows_written  INT            NULL,
    data_through  DATE           NULL,       -- nyeste datadato koerslen naaede
    message       NVARCHAR(1000) NULL,       -- fejltekst, eller en kort note
    host          NVARCHAR(100)  NULL
);
"""

# Dashboardet slår altid op på "seneste kørsel for denne tabel", derfor er
# (target_table, started_at) den rigtige nøgle — ikke source.
_CREATE_IX_SQL = f"""
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='ix_hubdataloads_target')
CREATE INDEX ix_hubdataloads_target
    ON dbo.{TABLE} (target_table, started_at DESC);
"""


def ensure_table() -> None:
    """Opret logtabel og indeks hvis de mangler. Idempotent.

    Kaldes automatisk af record_run(), så et nyt script ikke kræver et
    setup-trin, nogen kan glemme.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(_CREATE_SQL)
        cur.execute(_CREATE_IX_SQL)
        conn.commit()
    finally:
        conn.close()


class _Run:
    """Håndtaget record_run() giver. Alt herpå er valgfrit at kalde."""

    def __init__(self, source: str, target_table: str, scope=None):
        self.source = source
        self.target_table = target_table
        self.scope = scope
        self.started_at = datetime.now()
        self.rows_written = None
        self.message = None
        self._data_through = None
        self._id = None
        self._fejlet = False

    def rows(self, n) -> None:
        """Antal rækker kørslen skrev (indsat + opdateret)."""
        try:
            self.rows_written = int(n)
        except (TypeError, ValueError):
            self.rows_written = None

    def data_through(self, d) -> None:
        """Nyeste datadato kørslen nåede — fx seneste kursdato eller salgsdato.

        Accepterer date, datetime eller 'YYYY-MM-DD'. Noget uforståeligt
        ignoreres frem for at vælte kørslen.
        """
        if d is None:
            self._data_through = None
        elif isinstance(d, datetime):
            self._data_through = d.date()
        elif isinstance(d, str):
            try:
                self._data_through = datetime.strptime(d[:10], "%Y-%m-%d").date()
            except ValueError:
                self._data_through = None
        else:
            self._data_through = d          # date

    def note(self, text) -> None:
        """Kort note der vises i dashboardet, fx 'fuld sync' eller 'inkrementel'."""
        self.message = (str(text) or "")[:1000] or None

    def fail(self, text) -> None:
        """Markér kørslen som fejlet UDEN at afbryde den.

        Til delvise fejl: en sync hvor én konto af fem fejlede, har skrevet
        halve data. Scriptet skal stadig gøre sit færdige arbejde og afslutte
        med sin sædvanlige exitkode — men dashboardet skal vise rødt, ikke
        grønt. Uden denne ville den eneste måde at melde fejl på være at kaste,
        og det ville ændre, hvad Task Scheduler ser.
        """
        self._fejlet = True
        self.note(text)


def _insert_running(run: _Run) -> None:
    """Skriv 'running'-rækken og husk dens id til afslutningen."""
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""INSERT INTO dbo.{TABLE}
                    (source, target_table, scope, started_at, status, host)
                VALUES ({_P}, {_P}, {_P}, {_P}, 'running', {_P});
                SELECT CAST(SCOPE_IDENTITY() AS INT);""",
            (run.source, run.target_table, run.scope, run.started_at,
             socket.gethostname()[:100]),
        )
        row = cur.fetchone()
        if row:
            run._id = row[0]
        conn.commit()
    finally:
        conn.close()


def _finish(run: _Run, status: str) -> None:
    """Luk rækken.

    Er id'et væk (insert'en fejlede), skrives en hel række i stedet — ellers
    ville netop den kørsel, der havde problemer, forsvinde fra dashboardet.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        if run._id is not None:
            cur.execute(
                f"""UPDATE dbo.{TABLE}
                       SET finished_at = {_P}, status = {_P}, rows_written = {_P},
                           data_through = {_P}, message = {_P}
                     WHERE id = {_P}""",
                (datetime.now(), status, run.rows_written, run._data_through,
                 run.message, run._id),
            )
        else:
            cur.execute(
                f"""INSERT INTO dbo.{TABLE}
                        (source, target_table, scope, started_at, finished_at,
                         status, rows_written, data_through, message, host)
                    VALUES ({_P}, {_P}, {_P}, {_P}, {_P}, {_P}, {_P}, {_P}, {_P}, {_P})""",
                (run.source, run.target_table, run.scope, run.started_at,
                 datetime.now(), status, run.rows_written, run._data_through,
                 run.message, socket.gethostname()[:100]),
            )
        conn.commit()
    finally:
        conn.close()


@contextmanager
def record_run(source: str, target_table: str, scope: str = None):
    """Logér én kørsel. Se modulets docstring for brug.

    Undtagelser fra kroppen logges som status='error' og kastes videre —
    kaldstedets egen fejlhåndtering er uændret.
    """
    run = _Run(source, target_table, scope)
    try:
        ensure_table()
        _insert_running(run)
    except Exception as exc:                  # logningen må ikke vælte kørslen
        print(f"  !  Koerselslog utilgaengelig ({exc}) - koerslen fortsaetter.")

    try:
        yield run
    except BaseException as exc:
        # BaseException, ikke Exception: en kørsel afbrudt med Ctrl+C eller
        # SystemExit skal heller ikke efterlade en åben 'running'-række.
        run.message = (f"{type(exc).__name__}: {exc}\n"
                       f"{traceback.format_exc()}")[:1000]
        try:
            _finish(run, "error")
        except Exception as log_exc:
            print(f"  !  Kunne ikke skrive fejl til koerselsloggen: {log_exc}")
        raise
    else:
        try:
            _finish(run, "error" if run._fejlet else "ok")
        except Exception as log_exc:
            print(f"  !  Kunne ikke afslutte koerselsloggen: {log_exc}")

"""SQL Server-persistens for admin-nysalg-runs og deres matchede rækker.

Et "run" gemmer ALLE behandlede nysalgsrækker (også ikke-matchede), så rapporten
kan vise brutto/administrativt/netto og et run er fuldt reproducerbart, samt kan
genoptages i en senere session.
"""
from __future__ import annotations

import calendar
import logging
import os
from datetime import date
from typing import Optional

from constants import deal_value_sql
from db import get_conn
from moduler.modul_admin_nysalg.models import ExtractRow

logger = logging.getLogger(__name__)

VALID_STATUSES = ("matched", "in_review", "approved", "reported")
VALID_OVERRIDES = ("include", "exclude")


# ── DB-init (idempotent) ─────────────────────────────────────────────────────

INIT_STMTS = [
    """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='admin_nysalg_run' AND xtype='U')
       CREATE TABLE admin_nysalg_run (
           run_id            INT IDENTITY(1,1) PRIMARY KEY,
           created_at        DATETIME       NOT NULL DEFAULT GETDATE(),
           created_by        NVARCHAR(100)  NULL,
           source_path       NVARCHAR(500)  NULL,
           source_filename   NVARCHAR(260)  NULL,
           period            NVARCHAR(20)    NULL,
           status            NVARCHAR(20)   NOT NULL DEFAULT 'matched',
           director_comment  NVARCHAR(MAX)  NULL,
           approved_by       NVARCHAR(100)  NULL,
           approved_at       DATETIME       NULL,
           report_path       NVARCHAR(500)  NULL
       )""",
    """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='admin_nysalg_match' AND xtype='U')
       CREATE TABLE admin_nysalg_match (
           match_id          INT IDENTITY(1,1) PRIMARY KEY,
           run_id            INT            NOT NULL,
           row_index         INT            NULL,
           month_end         NVARCHAR(10)   NULL,
           account_number    NVARCHAR(100)  NULL,
           pipedrive_id      NVARCHAR(50)   NULL,
           contact_companyname NVARCHAR(200) NULL,
           site              NVARCHAR(150)  NULL,
           brands            NVARCHAR(150)  NULL,
           brand             NVARCHAR(50)   NULL,
           movement          NVARCHAR(50)   NULL,
           net_diff          DECIMAL(18,2)  NULL,
           gross_in          DECIMAL(18,2)  NULL,
           gross_out         DECIMAL(18,2)  NULL,
           administrativ     BIT            NOT NULL DEFAULT 0,
           matched_deal_id   NVARCHAR(50)   NULL,
           matched_value     DECIMAL(18,2)  NULL,
           matched_pipeline  NVARCHAR(100)  NULL,
           matched_org_name  NVARCHAR(200)  NULL,
           match_sign        NVARCHAR(5)    NULL,
           is_admin          BIT            NOT NULL DEFAULT 0,
           ambiguous         BIT            NOT NULL DEFAULT 0,
           override          NVARCHAR(20)   NULL,
           row_comment       NVARCHAR(MAX)  NULL
       )""",
    """IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='IX_admin_nysalg_match_run'
                      AND object_id = OBJECT_ID('admin_nysalg_match'))
       CREATE INDEX IX_admin_nysalg_match_run ON admin_nysalg_match (run_id)""",
    # Migration: org_name på matchede deals (så kunden kan ses i review).
    """IF EXISTS (SELECT * FROM sysobjects WHERE name='admin_nysalg_match' AND xtype='U')
       AND NOT EXISTS (SELECT * FROM sys.columns
           WHERE object_id = OBJECT_ID('admin_nysalg_match') AND name = 'matched_org_name')
       ALTER TABLE admin_nysalg_match ADD matched_org_name NVARCHAR(200) NULL""",
    # Migration: kundenavn fra Zuora-udtrækket (primær navnekilde — PipeDrive
    # bruges kun som fallback, og for Prenax-rækker hvor Zuora-navnet er
    # bureauet i stedet for slutkunden. Se customer_name()).
    """IF EXISTS (SELECT * FROM sysobjects WHERE name='admin_nysalg_match' AND xtype='U')
       AND NOT EXISTS (SELECT * FROM sys.columns
           WHERE object_id = OBJECT_ID('admin_nysalg_match') AND name = 'contact_companyname')
       ALTER TABLE admin_nysalg_match ADD contact_companyname NVARCHAR(200) NULL""",
    # Migrationer: brand-gruppe + gross_out + extract-administrativ flag
    # (bruges til per-brand-aggregering: penge ind/ud).
    """IF EXISTS (SELECT * FROM sysobjects WHERE name='admin_nysalg_match' AND xtype='U')
       AND NOT EXISTS (SELECT * FROM sys.columns
           WHERE object_id = OBJECT_ID('admin_nysalg_match') AND name = 'brand')
       ALTER TABLE admin_nysalg_match ADD brand NVARCHAR(50) NULL""",
    """IF EXISTS (SELECT * FROM sysobjects WHERE name='admin_nysalg_match' AND xtype='U')
       AND NOT EXISTS (SELECT * FROM sys.columns
           WHERE object_id = OBJECT_ID('admin_nysalg_match') AND name = 'gross_out')
       ALTER TABLE admin_nysalg_match ADD gross_out DECIMAL(18,2) NULL""",
    """IF EXISTS (SELECT * FROM sysobjects WHERE name='admin_nysalg_match' AND xtype='U')
       AND NOT EXISTS (SELECT * FROM sys.columns
           WHERE object_id = OBJECT_ID('admin_nysalg_match') AND name = 'administrativ')
       ALTER TABLE admin_nysalg_match ADD administrativ BIT NOT NULL DEFAULT 0""",
    # Valuta pr. række (NOK/SEK/DKK) — så NO/SE kan vises i lokal valuta.
    """IF EXISTS (SELECT * FROM sysobjects WHERE name='admin_nysalg_match' AND xtype='U')
       AND NOT EXISTS (SELECT * FROM sys.columns
           WHERE object_id = OBJECT_ID('admin_nysalg_match') AND name = 'currency')
       ALTER TABLE admin_nysalg_match ADD currency NVARCHAR(10) NULL""",
    # Deal-niveau kontrol (direktørens manuelle efterbehandling): udeluk en
    # bevægelse fra totalerne, og/eller ret gross_in/gross_out (fx falsk gross-in
    # ved skift af kontonummer). NULL-override = brug den rå værdi.
    """IF EXISTS (SELECT * FROM sysobjects WHERE name='admin_nysalg_match' AND xtype='U')
       AND NOT EXISTS (SELECT * FROM sys.columns
           WHERE object_id = OBJECT_ID('admin_nysalg_match') AND name = 'total_excluded')
       ALTER TABLE admin_nysalg_match ADD total_excluded BIT NOT NULL DEFAULT 0""",
    """IF EXISTS (SELECT * FROM sysobjects WHERE name='admin_nysalg_match' AND xtype='U')
       AND NOT EXISTS (SELECT * FROM sys.columns
           WHERE object_id = OBJECT_ID('admin_nysalg_match') AND name = 'gross_in_override')
       ALTER TABLE admin_nysalg_match ADD gross_in_override DECIMAL(18,2) NULL""",
    """IF EXISTS (SELECT * FROM sysobjects WHERE name='admin_nysalg_match' AND xtype='U')
       AND NOT EXISTS (SELECT * FROM sys.columns
           WHERE object_id = OBJECT_ID('admin_nysalg_match') AND name = 'gross_out_override')
       ALTER TABLE admin_nysalg_match ADD gross_out_override DECIMAL(18,2) NULL""",
    # Migration: delvist administrative bevægelser — den administrative ANDEL af
    # gross in/out kan sættes pr. række (fx en deal hvor kun en del af beløbet er
    # administrativt). NULL = alt-eller-intet efter admin-match/flag.
    """IF EXISTS (SELECT * FROM sysobjects WHERE name='admin_nysalg_match' AND xtype='U')
       AND NOT EXISTS (SELECT * FROM sys.columns
           WHERE object_id = OBJECT_ID('admin_nysalg_match') AND name = 'adm_in_override')
       ALTER TABLE admin_nysalg_match ADD adm_in_override DECIMAL(18,2) NULL""",
    """IF EXISTS (SELECT * FROM sysobjects WHERE name='admin_nysalg_match' AND xtype='U')
       AND NOT EXISTS (SELECT * FROM sys.columns
           WHERE object_id = OBJECT_ID('admin_nysalg_match') AND name = 'adm_out_override')
       ALTER TABLE admin_nysalg_match ADD adm_out_override DECIMAL(18,2) NULL""",
    # Migration: datointerval (period_from/period_to) — afløser enkelt-måneds-perioden.
    # [period] beholdes som læsbar label (fx '2026-01 – 2026-05'); from/to bærer
    # det faktiske filter.
    """IF EXISTS (SELECT * FROM sysobjects WHERE name='admin_nysalg_run' AND xtype='U')
       AND NOT EXISTS (SELECT * FROM sys.columns
           WHERE object_id = OBJECT_ID('admin_nysalg_run') AND name = 'period_from')
       ALTER TABLE admin_nysalg_run ADD period_from NVARCHAR(10) NULL""",
    """IF EXISTS (SELECT * FROM sysobjects WHERE name='admin_nysalg_run' AND xtype='U')
       AND NOT EXISTS (SELECT * FROM sys.columns
           WHERE object_id = OBJECT_ID('admin_nysalg_run') AND name = 'period_to')
       ALTER TABLE admin_nysalg_run ADD period_to NVARCHAR(10) NULL""",
    # Udvid [period] så den kan rumme en interval-label ('YYYY-MM-DD – YYYY-MM-DD').
    # NVARCHAR(20) => max_length 40 (bytes); NVARCHAR(60) => 120.
    """IF EXISTS (SELECT * FROM sys.columns
           WHERE object_id = OBJECT_ID('admin_nysalg_run') AND name = 'period'
             AND max_length < 120)
       ALTER TABLE admin_nysalg_run ALTER COLUMN period NVARCHAR(60) NULL""",
    # Migration: rapport-scope pr. run — 'business_media' (alt undtagen Monitor,
    # NULL tolkes som denne for gamle runs) eller 'monitor' (kun Monitor, pr. site).
    """IF EXISTS (SELECT * FROM sysobjects WHERE name='admin_nysalg_run' AND xtype='U')
       AND NOT EXISTS (SELECT * FROM sys.columns
           WHERE object_id = OBJECT_ID('admin_nysalg_run') AND name = 'report_scope')
       ALTER TABLE admin_nysalg_run ADD report_scope NVARCHAR(20) NULL""",
    # Pr-brand-kommentar (direktøren kommenterer på den samlede brand-performance).
    """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='admin_nysalg_brand_comment' AND xtype='U')
       CREATE TABLE admin_nysalg_brand_comment (
           id          INT IDENTITY(1,1) PRIMARY KEY,
           run_id      INT            NOT NULL,
           brand       NVARCHAR(50)   NOT NULL,
           comment     NVARCHAR(MAX)  NULL,
           updated_at  DATETIME       DEFAULT GETDATE(),
           CONSTRAINT UQ_admin_nysalg_brand_comment UNIQUE (run_id, brand)
       )""",
    # Skjulte brands pr. run — direktøren kan klikke et brand helt ud af rapporten
    # (fjernes fra brand-tabel, måneds-opdeling OG top-tallene).
    """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='admin_nysalg_brand_hidden' AND xtype='U')
       CREATE TABLE admin_nysalg_brand_hidden (
           id          INT IDENTITY(1,1) PRIMARY KEY,
           run_id      INT            NOT NULL,
           brand       NVARCHAR(50)   NOT NULL,
           CONSTRAINT UQ_admin_nysalg_brand_hidden UNIQUE (run_id, brand)
       )""",
    # Site-mapping for de få afvigelser mellem Zuora og PipeDrive (kan udvides
    # uden kodeændring). Tom tabel => 1:1-matchning.
    """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='admin_nysalg_site_map' AND xtype='U')
       CREATE TABLE admin_nysalg_site_map (
           id             INT IDENTITY(1,1) PRIMARY KEY,
           zuora_site     NVARCHAR(150)  NOT NULL,
           pipedrive_site NVARCHAR(150)  NOT NULL,
           created_at     DATETIME       DEFAULT GETDATE()
       )""",
]


def init_admin_nysalg_db() -> None:
    """Opret modulets tabeller hvis de mangler. Kaldes ved app-opstart."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        for sql in INIT_STMTS:
            cur.execute(sql)
        conn.commit()
        conn.close()
    except Exception:
        logger.exception("init_admin_nysalg_db: kunne ikke initialisere tabeller")


# ── Site-map ─────────────────────────────────────────────────────────────────

def load_site_map() -> dict:
    """{zuora_site: pipedrive_site}. Tom dict hvis tabellen er tom/utilgængelig."""
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute("SELECT zuora_site, pipedrive_site FROM admin_nysalg_site_map")
        rows = cur.fetchall() or []
        conn.close()
        return {r["zuora_site"]: r["pipedrive_site"] for r in rows}
    except Exception:
        return {}


# ── Runs ─────────────────────────────────────────────────────────────────────

def create_run(created_by: str, source_path: Optional[str], source_filename: Optional[str],
               period: Optional[str], period_from: Optional[str] = None,
               period_to: Optional[str] = None,
               report_scope: str = "business_media") -> int:
    """period = læsbar label; period_from/period_to = ISO YYYY-MM-DD-interval (filter).
    report_scope: 'business_media' (alt undtagen Monitor) eller 'monitor' (kun
    Monitor, pr. site)."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO admin_nysalg_run
               (created_by, source_path, source_filename, period, period_from, period_to,
                report_scope, status)
           OUTPUT INSERTED.run_id VALUES (%s, %s, %s, %s, %s, %s, %s, 'matched')""",
        (created_by, source_path, source_filename, period, period_from, period_to,
         report_scope),
    )
    run_id = int(cur.fetchone()[0])
    conn.commit()
    conn.close()
    return run_id


def run_scope(run: dict) -> str:
    """Rapport-scope for et run — gamle runs (NULL) er Business Media."""
    return (run.get("report_scope") or "business_media").strip()


def insert_matches(run_id: int, rows: list[ExtractRow], progress_cb=None) -> None:
    """progress_cb(i, total) kaldes pr. batch så en baggrundsjob-kører kan
    rapportere fremdrift.

    Indsættes i multi-række-batches (én INSERT pr. _INSERT_BATCH rækker) — store
    udtræk har flere hundrede tusinde rækker, og én INSERT pr. række betød én
    netværksrundtur pr. række.
    """
    _INSERT_BATCH = 200
    conn = get_conn()
    cur = conn.cursor()
    total = len(rows)
    row_sql = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    for start in range(0, total, _INSERT_BATCH):
        chunk = rows[start:start + _INSERT_BATCH]
        params: list = []
        for r in chunk:
            m = r.match
            params.extend((
                run_id, r.row_index, r.month_end, r.account_number, r.pipedrive_id, r.site,
                r.brands, r.brand, r.movement, r.net_diff, r.gross_in, r.gross_out,
                1 if r.administrativ else 0, r.currency, r.contact_companyname,
                (m.deal_id if m else None),
                (m.value if m else None),
                (m.pipeline if m else None),
                (m.org_name if m else None),
                r.match_sign,
                1 if r.is_admin_nysalg() else 0,
                1 if r.ambiguous else 0,
            ))
        cur.execute(
            """INSERT INTO admin_nysalg_match
               (run_id, row_index, month_end, account_number, pipedrive_id, site, brands,
                brand, movement, net_diff, gross_in, gross_out, administrativ, currency,
                contact_companyname,
                matched_deal_id, matched_value, matched_pipeline, matched_org_name,
                match_sign, is_admin, ambiguous)
               VALUES """ + ",".join([row_sql] * len(chunk)),
            tuple(params),
        )
        if progress_cb:
            try:
                progress_cb(min(start + _INSERT_BATCH, total), total)
            except Exception:
                pass   # progress-rapportering må aldrig vælte indsættelsen
    conn.commit()
    conn.close()


def get_run(run_id: int) -> Optional[dict]:
    conn = get_conn()
    cur = conn.cursor(as_dict=True)
    cur.execute("SELECT * FROM admin_nysalg_run WHERE run_id = %s", (run_id,))
    row = cur.fetchone()
    conn.close()
    return row


def list_runs(limit: int = 100) -> list[dict]:
    conn = get_conn()
    cur = conn.cursor(as_dict=True)
    cur.execute(
        "SELECT TOP (%s) * FROM admin_nysalg_run ORDER BY run_id DESC", (int(limit),)
    )
    rows = cur.fetchall() or []
    conn.close()
    return rows


def delete_run(run_id: int) -> bool:
    """Slet et run fuldstændigt: match-rækker, brand-kommentarer, selve runnet og
    de genererede rapportfiler på disk. Returnerer True hvis runnet fandtes.
    """
    run = get_run(run_id)
    if not run:
        return False
    # Rapportfiler (base-sti gemt uden/med extension) — fjern både xlsx og pdf.
    rp = run.get("report_path")
    if rp:
        base, _ = os.path.splitext(rp)
        for ext in (".xlsx", ".pdf"):
            try:
                if os.path.exists(base + ext):
                    os.remove(base + ext)
            except OSError:
                logger.warning("Kunne ikke slette rapportfil %s", base + ext)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM admin_nysalg_match WHERE run_id = %s", (run_id,))
    cur.execute("DELETE FROM admin_nysalg_brand_comment WHERE run_id = %s", (run_id,))
    cur.execute("DELETE FROM admin_nysalg_brand_hidden WHERE run_id = %s", (run_id,))
    cur.execute("DELETE FROM admin_nysalg_run WHERE run_id = %s", (run_id,))
    conn.commit()
    conn.close()
    return True


# Rækker der overhovedet kan bidrage til rapporten: bevægelse, admin-markering
# eller manuel efterbehandling. Alt andet bidrager med 0 til ALLE totaler og
# filtreres fra i SQL — deles af get_matches og SQL-aggregaterne, så de to veje
# ser præcis samme rækkesæt.
_CONTRIBUTING_WHERE = """(COALESCE(net_diff, 0) <> 0
                  OR COALESCE(gross_in, 0) <> 0
                  OR COALESCE(gross_out, 0) <> 0
                  OR is_admin = 1 OR administrativ = 1
                  OR matched_deal_id IS NOT NULL
                  OR total_excluded = 1 OR override IS NOT NULL
                  OR gross_in_override IS NOT NULL OR gross_out_override IS NOT NULL
                  OR adm_in_override IS NOT NULL OR adm_out_override IS NOT NULL)"""


def get_matches(run_id: int) -> list[dict]:
    """Match-rækker for et run — kun rækker der kan bidrage til rapporten.

    Rækker uden bevægelse (net_diff/gross_in/gross_out = 0) og uden admin-match,
    flag eller manuel efterbehandling bidrager med 0 til ALLE totaler og vises
    ingen steder — de springes over i SQL i stedet for at blive hentet. Et run
    over en lang periode gemmer let flere hundrede tusinde rækker, og at hente
    dem alle over netværket tog minutter pr. kald.
    """
    conn = get_conn()
    cur = conn.cursor(as_dict=True)
    cur.execute(
        f"""SELECT * FROM admin_nysalg_match
           WHERE run_id = %s
             AND {_CONTRIBUTING_WHERE}
           ORDER BY row_index""", (run_id,)
    )
    rows = cur.fetchall() or []
    conn.close()
    for r in rows:
        for k in ("net_diff", "gross_in", "gross_out", "matched_value",
                  "gross_in_override", "gross_out_override",
                  "adm_in_override", "adm_out_override"):
            r[k] = float(r[k]) if r.get(k) is not None else None
        r["is_admin"] = bool(r["is_admin"])
        r["ambiguous"] = bool(r["ambiguous"])
        r["administrativ"] = bool(r.get("administrativ"))
        r["total_excluded"] = bool(r.get("total_excluded"))
    return rows


def update_status(run_id: int, status: str) -> None:
    if status not in VALID_STATUSES:
        raise ValueError(f"Ugyldig status: {status}")
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE admin_nysalg_run SET status = %s WHERE run_id = %s", (status, run_id))
    conn.commit()
    conn.close()


def set_director_comment(run_id: int, comment: str) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "UPDATE admin_nysalg_run SET director_comment = %s WHERE run_id = %s",
        (comment, run_id),
    )
    conn.commit()
    conn.close()


def set_row_comment(run_id: int, match_id: int, comment: str) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "UPDATE admin_nysalg_match SET row_comment = %s WHERE match_id = %s AND run_id = %s",
        (comment, match_id, run_id),
    )
    conn.commit()
    conn.close()


def set_override(run_id: int, match_id: int, override: Optional[str]) -> None:
    """override: 'include' | 'exclude' | None (= ryd override)."""
    if override is not None and override not in VALID_OVERRIDES:
        raise ValueError(f"Ugyldig override: {override}")
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "UPDATE admin_nysalg_match SET override = %s WHERE match_id = %s AND run_id = %s",
        (override, match_id, run_id),
    )
    conn.commit()
    conn.close()


def set_row_total_excluded(run_id: int, match_id: int, excluded: bool) -> None:
    """Medtag/udeluk en enkelt bevægelse fra rapportens totaler."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "UPDATE admin_nysalg_match SET total_excluded = %s WHERE match_id = %s AND run_id = %s",
        (1 if excluded else 0, match_id, run_id),
    )
    conn.commit()
    conn.close()


def set_row_value_override(run_id: int, match_id: int,
                           gross_in: Optional[float], gross_out: Optional[float]) -> None:
    """Ret gross_in/gross_out for en bevægelse. None pr. felt = ryd (brug rå værdi)."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """UPDATE admin_nysalg_match
           SET gross_in_override = %s, gross_out_override = %s
           WHERE match_id = %s AND run_id = %s""",
        (gross_in, gross_out, match_id, run_id),
    )
    conn.commit()
    conn.close()


def set_row_adm_split(run_id: int, match_id: int,
                      adm_in: Optional[float], adm_out: Optional[float]) -> None:
    """Sæt den administrative ANDEL af gross in/out for en bevægelse.

    Bruges når kun en del af beløbet er administrativt: andelen trækkes fra
    Actual Sale/Churn, resten tæller som almindeligt salg/churn. None pr. felt =
    ryd (tilbage til alt-eller-intet efter admin-match/flag).
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """UPDATE admin_nysalg_match
           SET adm_in_override = %s, adm_out_override = %s
           WHERE match_id = %s AND run_id = %s""",
        (adm_in, adm_out, match_id, run_id),
    )
    conn.commit()
    conn.close()


def approve_run(run_id: int, approved_by: str) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """UPDATE admin_nysalg_run
           SET status='approved', approved_by=%s, approved_at=GETDATE()
           WHERE run_id=%s""",
        (approved_by, run_id),
    )
    conn.commit()
    conn.close()


def set_report_path(run_id: int, report_path: str) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "UPDATE admin_nysalg_run SET report_path=%s, status='reported' WHERE run_id=%s",
        (report_path, run_id),
    )
    conn.commit()
    conn.close()


# ── Afledte tal ──────────────────────────────────────────────────────────────

# ── Pr-brand-kommentarer ─────────────────────────────────────────────────────

def get_brand_comments(run_id: int) -> dict:
    """{brand: comment} for et run."""
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute("SELECT brand, comment FROM admin_nysalg_brand_comment WHERE run_id = %s",
                    (run_id,))
        rows = cur.fetchall() or []
        conn.close()
        return {r["brand"]: r["comment"] for r in rows}
    except Exception:
        return {}


def set_brand_comment(run_id: int, brand: str, comment: str) -> None:
    """Upsert kommentaren for én brand-gruppe på et run."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM admin_nysalg_brand_comment WHERE run_id = %s AND brand = %s",
        (run_id, brand),
    )
    cur.execute(
        "INSERT INTO admin_nysalg_brand_comment (run_id, brand, comment) VALUES (%s, %s, %s)",
        (run_id, brand, comment),
    )
    conn.commit()
    conn.close()


# ── Skjulte brands pr. run ───────────────────────────────────────────────────

def get_hidden_brands(run_id: int) -> set:
    """{brand} der er klikket helt ud af rapporten for et run."""
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute("SELECT brand FROM admin_nysalg_brand_hidden WHERE run_id = %s", (run_id,))
        rows = cur.fetchall() or []
        conn.close()
        return {r["brand"] for r in rows}
    except Exception:
        return set()


def set_brand_hidden(run_id: int, brand: str, hidden: bool) -> None:
    """Skjul/vis ét brand på et run (upsert ved skjul, slet ved vis)."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM admin_nysalg_brand_hidden WHERE run_id = %s AND brand = %s",
        (run_id, brand),
    )
    if hidden:
        cur.execute(
            "INSERT INTO admin_nysalg_brand_hidden (run_id, brand) VALUES (%s, %s)",
            (run_id, brand),
        )
    conn.commit()
    conn.close()


# ── Brand-budgetter (samlet mediebudget pr. brand) ───────────────────────────

def _date_between(col: str, date_from: str | None, date_to: str | None,
                  cast_to_date: bool = False) -> tuple[list[str], list]:
    """(clause-liste, params) for et inklusivt datointerval på en kolonne.

    cast_to_date=True bruges på datetime-kolonner (fx service_activation_date) så
    den øvre grænse også fanger rækker senere på slutdagen.
    """
    clauses: list[str] = []
    params: list = []
    upper = f"CAST({col} AS DATE)" if cast_to_date else col
    if date_from:
        clauses.append(f"{col} >= %s")
        params.append(date_from)
    if date_to:
        clauses.append(f"{upper} <= %s")
        params.append(date_to)
    return clauses, params


def brand_budgets(date_from: str | None, date_to: str | None,
                  subscription_only: bool = True) -> dict:
    """{brand-label: abonnements-mediebudget} fra BudgetsIntoMedia for intervallet.

    Rapporten er en abonnementsvisning, så budgettet afgrænses til DealType
    'Subscription' — ellers tæller fx Banner-/Job-budget med i abonnements-
    brandenes budget. date_from/date_to = ISO YYYY-MM-DD afgrænser på BudgetDate
    (inkl.); begge None summerer alle datoer. subscription_only=False = hele
    budgettet (alle DealTypes).
    """
    from moduler.modul_admin_nysalg.brands import BUDGET_BRANDS
    where = []
    params: list = []
    if subscription_only:
        where.append("LOWER(LTRIM(RTRIM(COALESCE([DealType],'')))) = 'subscription'")
    dclauses, dparams = _date_between("[BudgetDate]", date_from, date_to)
    where += dclauses
    params += dparams
    sql = "SELECT [Brand] AS brand, ISNULL(SUM([BudgetAmount]),0) AS budget FROM [dbo].[BudgetsIntoMedia]"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " GROUP BY [Brand]"
    raw: dict = {}
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(sql, tuple(params))
        for r in cur.fetchall() or []:
            raw[(r["brand"] or "").strip().lower()] = float(r["budget"] or 0)
        conn.close()
    except Exception:
        logger.exception("brand_budgets fejlede")
        return {}
    out: dict = {}
    for label, brand_names in BUDGET_BRANDS.items():
        total = sum(raw.get(bn.strip().lower(), 0.0) for bn in brand_names)
        out[label] = round(total, 2)
    return out


# ── Monitor-rapporten (pr. enkelt site) ──────────────────────────────────────
# report_scope='monitor': kun Monitor-bevægelser, opgjort pr. site (Byrummonitor,
# Klimamonitor, …). Sitet ER rækken — hele den eksisterende brand-pipeline
# genbruges ved at relabel'e matches med brand = site.

def monitor_norm(site: str) -> str:
    """Join-nøgle for et Monitor-site på tværs af kilder.

    Zuora ('Sundhedsmonitor DK' / 'sundhedsmonitor.dk') og BudgetsIntoMedia
    ('Sundhedsmonitor' / 'idrætsmonitor') staver sites forskelligt — normalisér
    via matcherens normalize_site (lowercase, æøå-foldning, domænesuffiks væk)
    og strip et evt. ' dk'-landesuffiks (Monitor er kun dansk).
    """
    from moduler.modul_admin_nysalg.matcher import normalize_site
    n = normalize_site(site)
    if n.endswith(" dk"):
        n = n[:-3].strip()
    return n


def monitor_relabel(matches: list[dict]) -> list[dict]:
    """Kopiér Monitor-matches med brand = sitets navn.

    Derefter arbejder totaler, måneds-opdeling, bevægelsesliste, kommentarer,
    skjul-knapper og Excel/PDF pr. enkelt site — uden ændringer i de delte helpers.
    """
    return [dict(m, brand=(m.get("site") or "").strip() or "Ukendt site")
            for m in matches]


def _monitor_budget_rows(date_from: str | None, date_to: str | None,
                         by_month: bool) -> list[dict]:
    """Rå budgetrækker for Monitor-abonnement pr. site (evt. også pr. måned)."""
    where = ["[Brand] = 'Monitor'",
             "LOWER(LTRIM(RTRIM(COALESCE([DealType],'')))) = 'subscription'"]
    params: list = []
    dclauses, dparams = _date_between("[BudgetDate]", date_from, date_to)
    where += dclauses
    params += dparams
    ym_col = "CONVERT(varchar(7), [BudgetDate], 126) AS ym, " if by_month else ""
    ym_grp = "CONVERT(varchar(7), [BudgetDate], 126), " if by_month else ""
    sql = (f"SELECT {ym_col}[Site] AS site, ISNULL(SUM([BudgetAmount]),0) AS budget "
           f"FROM [dbo].[BudgetsIntoMedia] WHERE {' AND '.join(where)} "
           f"GROUP BY {ym_grp}[Site]")
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(sql, tuple(params))
        rows = cur.fetchall() or []
        conn.close()
        return rows
    except Exception:
        logger.exception("monitor budget-opslag fejlede")
        return []


def _monitor_budget_entry(r: dict) -> tuple[str, tuple[str, float]]:
    site = (r.get("site") or "").strip()
    # Budget-arket skriver nogle sites med småt ('idrætsmonitor') — vis pænt.
    display = site[:1].upper() + site[1:]
    return monitor_norm(site), (display, round(float(r.get("budget") or 0), 2))


def monitor_site_budgets(date_from: str | None, date_to: str | None) -> dict:
    """{monitor_norm(site): (visningsnavn, budget)} for perioden."""
    out: dict = {}
    for r in _monitor_budget_rows(date_from, date_to, by_month=False):
        if (r.get("site") or "").strip():
            k, v = _monitor_budget_entry(r)
            out[k] = v
    return out


def monitor_site_budgets_by_month(date_from: str | None, date_to: str | None) -> dict:
    """{'YYYY-MM': {monitor_norm(site): (visningsnavn, budget)}} — ét kald."""
    out: dict = {}
    for r in _monitor_budget_rows(date_from, date_to, by_month=True):
        if (r.get("site") or "").strip():
            k, v = _monitor_budget_entry(r)
            out.setdefault((r.get("ym") or "")[:7], {})[k] = v
    return out


def monitor_brand_rows(matches: list[dict], norm_budgets: dict,
                       comments: dict | None = None,
                       extra_rows: list[dict] | None = None) -> list[dict]:
    """Rækker til Monitor-rapporten: én pr. site + annonce-rækkerne (Job/Banner).

    matches SKAL være relabel'et (brand = site, jf. monitor_relabel);
    norm_budgets fra monitor_site_budgets(_by_month). Budget-sites uden
    bevægelser i perioden vises med 0-tal, så budget/deviation er komplet.
    Ren beregning (ingen DB) — annonce-rækkerne leveres via extra_rows.
    """
    from moduler.modul_admin_nysalg.brands import brand_geo
    comments = comments or {}
    # Budget pr. visningslabel: brug sitenavnet fra bevægelserne når det findes
    # (så budget og bevægelser lander på SAMME række), ellers budgettets navn.
    label_by_norm: dict[str, str] = {}
    for m in matches:
        lbl = m.get("brand") or ""
        label_by_norm.setdefault(monitor_norm(lbl), lbl)
    budgets: dict[str, float] = {}
    ensure: list[str] = []
    for norm, (display, budget) in norm_budgets.items():
        lbl = label_by_norm.get(norm)
        if lbl is None:
            lbl = display
            ensure.append(lbl)
        budgets[lbl] = budget
    rows = summarize_by_brand(matches, budgets, comments, extra_rows=extra_rows,
                              ensure_labels=ensure, seed_defaults=False)
    # Abonnements-sites først (alfabetisk), annonce-rækkerne (Job/Banner) sidst.
    rows.sort(key=lambda r: (brand_geo(r["brand"])[1] != "Subscription",
                             (r["brand"] or "").lower()))
    return rows


# ── Reklame-rækker (PipeDrive-deals, ikke abonnement) ────────────────────────

def _budget_for_where(cur, frag: str, date_from: str | None, date_to: str | None) -> float:
    """Σ BudgetAmount fra BudgetsIntoMedia for et WHERE-fragment + periodeafgrænsning.

    `frag` er kode-kontrolleret (fra brands.py), så LIKE-mønstre må stå som literaler.
    """
    where = [frag]
    params: list = []
    dclauses, dparams = _date_between("[BudgetDate]", date_from, date_to)
    where += dclauses
    params += dparams
    cur.execute(
        "SELECT ISNULL(SUM([BudgetAmount]),0) AS budget FROM [dbo].[BudgetsIntoMedia] "
        "WHERE " + " AND ".join(where), tuple(params))
    row = cur.fetchone()
    return float((row["budget"] if row else 0) or 0)


def _budget_by_month_for_where(cur, frag: str, date_from: str | None,
                               date_to: str | None) -> dict:
    """{'YYYY-MM': Σ BudgetAmount} for et WHERE-fragment — som _budget_for_where,
    men alle måneder i intervallet i ét GROUP BY-kald (Fase B).

    `frag` er kode-kontrolleret (fra brands.py), så LIKE-mønstre må stå som literaler.
    """
    where = [frag]
    params: list = []
    dclauses, dparams = _date_between("[BudgetDate]", date_from, date_to)
    where += dclauses
    params += dparams
    cur.execute(
        "SELECT CONVERT(varchar(7), [BudgetDate], 126) AS ym, "
        "ISNULL(SUM([BudgetAmount]),0) AS budget FROM [dbo].[BudgetsIntoMedia] "
        "WHERE " + " AND ".join(where) +
        " GROUP BY CONVERT(varchar(7), [BudgetDate], 126)", tuple(params))
    return {(r["ym"] or "")[:7]: float(r["budget"] or 0)
            for r in cur.fetchall() or []}


def _ad_budget(cur, label: str, date_from: str | None, date_to: str | None) -> float:
    """Budget for en reklame-række fra BudgetsIntoMedia (0 ved manglende mapping)."""
    from moduler.modul_admin_nysalg.brands import AD_BUDGET_WHERE
    frag = AD_BUDGET_WHERE.get(label)
    if not frag:
        return 0.0
    return _budget_for_where(cur, frag, date_from, date_to)


def brand_budgets_by_month(date_from: str | None, date_to: str | None,
                           subscription_only: bool = True) -> dict:
    """{'YYYY-MM': {brand-label: budget}} — som brand_budgets, men alle måneder i
    intervallet i ÉT kald (GROUP BY måned) i stedet for én query pr. måned."""
    from moduler.modul_admin_nysalg.brands import BUDGET_BRANDS
    where = []
    params: list = []
    if subscription_only:
        where.append("LOWER(LTRIM(RTRIM(COALESCE([DealType],'')))) = 'subscription'")
    dclauses, dparams = _date_between("[BudgetDate]", date_from, date_to)
    where += dclauses
    params += dparams
    sql = ("SELECT CONVERT(varchar(7), [BudgetDate], 126) AS ym, [Brand] AS brand, "
           "ISNULL(SUM([BudgetAmount]),0) AS budget FROM [dbo].[BudgetsIntoMedia]")
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " GROUP BY CONVERT(varchar(7), [BudgetDate], 126), [Brand]"
    raw: dict[str, dict] = {}
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(sql, tuple(params))
        for r in cur.fetchall() or []:
            ym = (r["ym"] or "")[:7]
            raw.setdefault(ym, {})[(r["brand"] or "").strip().lower()] = float(r["budget"] or 0)
        conn.close()
    except Exception:
        logger.exception("brand_budgets_by_month fejlede")
        return {}
    out: dict[str, dict] = {}
    for ym, brands_raw in raw.items():
        out[ym] = {label: round(sum(brands_raw.get(bn.strip().lower(), 0.0)
                                    for bn in brand_names), 2)
                   for label, brand_names in BUDGET_BRANDS.items()}
    return out


def pipedrive_brand_rows(date_from: str | None, date_to: str | None,
                         comments: dict | None = None,
                         budgets: dict | None = None,
                         specs: list | None = None,
                         dk_split: bool = True,
                         parallel: bool = False) -> list[dict]:
    """Brand-rækker hentet direkte fra PipedriveDeals (findes ikke i Zuora).

    Dækker Job/Banner/Norge advertising og MarketWire (jf. PIPEDRIVE_ROWS).
    Bruttonysalg = Σ won-deals uden for cancellation-pipelines; opsigelser =
    Σ ABS af cancellation-pipelines; netto = brutto − opsigelser. Job/Banner/Norge
    har ingen cancellation-pipelines → opsigelser=0; MarketWire har rigtige
    opsigelser. Værdi i lokal valuta for NOK/SEK, ellers DKK. Filtreres på
    service_activation_date i intervallet (ISO YYYY-MM-DD, inkl.) og status='won'.
    Budget: spec'ens egen 'budget_where' hvis sat, ellers Job/Banner/Norge fra
    AD_BUDGET_WHERE, ellers det normale brand-budget. Returnerer [] ved DB-fejl.

    specs/dk_split: kaldere kan give egne specs og slå DK-annonce-opdelingen fra.
    (Monitor-rapportens annoncerækker bygges for sig — se
    monitor_advertising_brand_rows.)
    parallel=True kører hver spec (+ DK-opdelingen) i egen tråd/forbindelse —
    bruges af de direkte kald (review/rapport). brand_rows_by_month lader den
    stå sekventiel, da den allerede paralleliserer på tværs af måneder.
    """
    from constants import CANCELLATION_PIPELINES
    from moduler.modul_admin_nysalg.brands import (AD_BUDGET_WHERE, PIPEDRIVE_ROWS,
                                                   brand_currency)
    comments = comments or {}
    budgets = budgets or {}
    if specs is None:
        specs_list = PIPEDRIVE_ROWS
    else:
        specs_list = specs
    # Lokal valuta for NO/SE-organisationen (jf. perf-modulet), ellers DKK.
    val = deal_value_sql()
    cancel_ph = "(" + ",".join(["%s"] * len(CANCELLATION_PIPELINES)) + ")"
    cancel_up = [p.upper() for p in CANCELLATION_PIPELINES]

    def _spec_row(cur, spec) -> dict:
        where = [
            "[status] = 'won'",
            "COALESCE([administrativ],'') <> 'ja'",
            "[service_activation_date] IS NOT NULL",
            f"LOWER(LTRIM(RTRIM(COALESCE([{spec['scope_col']}],'')))) = %s",
        ]
        where_params: list = [spec["scope_val"].lower()]
        pipes = spec.get("pipelines")
        if pipes:
            pipe_ph = "(" + ",".join(["%s"] * len(pipes)) + ")"
            where.append(f"LOWER(LTRIM(RTRIM(COALESCE([pipeline_name],'')))) IN {pipe_ph}")
            where_params += [p.lower() for p in pipes]
        dclauses, dparams = _date_between(
            "[service_activation_date]", date_from, date_to, cast_to_date=True)
        where += dclauses
        where_params += dparams
        sql = f"""
            SELECT
              ISNULL(SUM(CASE WHEN UPPER([pipeline_name]) NOT IN {cancel_ph}
                  THEN CAST({val} AS DECIMAL(18,2)) ELSE 0 END), 0) AS won,
              ISNULL(ABS(SUM(CASE WHEN UPPER([pipeline_name]) IN {cancel_ph}
                  THEN CAST({val} AS DECIMAL(18,2)) ELSE 0 END)), 0) AS ops
            FROM [dbo].[PipedriveDeals]
            WHERE {' AND '.join(where)}
        """
        # SELECT-placeholders (to cancel-lister) kommer før WHERE-params i SQL'en.
        cur.execute(sql, tuple(cancel_up) * 2 + tuple(where_params))
        row = cur.fetchone() or {}
        brutto = round(float(row.get("won") or 0), 2)
        ops = round(float(row.get("ops") or 0), 2)
        label = spec["label"]
        if spec.get("budget_where"):
            budget = round(_budget_for_where(cur, spec["budget_where"],
                                             date_from, date_to), 2)
        elif label in AD_BUDGET_WHERE:
            budget = round(_ad_budget(cur, label, date_from, date_to), 2)
        else:
            budget = round(float(budgets.get(label, 0.0) or 0.0), 2)

        # Underrækker (drill-down): samme scope + ét site (eller site-mønster)
        # pr. underrække, hver med sit eget site-budget. Delmængde af brutto/
        # opsigelser — totalen i hovedrækken er uændret, og Σ underrække-budget
        # = hovedrækkens budget. 'site_like' matcher med LIKE (fx alle Watch-
        # sites), 'site' eksakt.
        subrows: list[dict] = []
        for sr in spec.get("subrows", []):
            if sr.get("site_like"):
                # Mønsteret er kode-kontrolleret (brands.py) → literal LIKE.
                site_cond = f"LOWER(COALESCE([sites],'')) LIKE '{sr['site_like']}'"
                site_params: list = []
            else:
                site_cond = "LOWER(LTRIM(RTRIM(COALESCE([sites],'')))) = %s"
                site_params = [sr["site"].lower()]
            sr_where = where + [site_cond]
            sr_params = where_params + site_params
            cur.execute(f"""
                SELECT
                  ISNULL(SUM(CASE WHEN UPPER([pipeline_name]) NOT IN {cancel_ph}
                      THEN CAST({val} AS DECIMAL(18,2)) ELSE 0 END), 0) AS won,
                  ISNULL(ABS(SUM(CASE WHEN UPPER([pipeline_name]) IN {cancel_ph}
                      THEN CAST({val} AS DECIMAL(18,2)) ELSE 0 END)), 0) AS ops
                FROM [dbo].[PipedriveDeals]
                WHERE {' AND '.join(sr_where)}
            """, tuple(cancel_up) * 2 + tuple(sr_params))
            srow = cur.fetchone() or {}
            s_brutto = round(float(srow.get("won") or 0), 2)
            s_ops = round(float(srow.get("ops") or 0), 2)
            s_budget = (round(_budget_for_where(cur, sr["budget_where"], date_from, date_to), 2)
                        if sr.get("budget_where") else None)
            subrows.append({
                "brand": sr["label"], "brutto": s_brutto, "adm_nysalg": 0.0,
                "opsigelser": s_ops, "adm_opsigelser": 0.0,
                "netto": round(s_brutto - s_ops, 2), "budget": s_budget,
                "comment": "", "n_ambiguous": 0, "currency": brand_currency(label),
            })

        return {
            "brand": label, "brutto": brutto, "adm_nysalg": 0.0,
            "opsigelser": ops, "adm_opsigelser": 0.0,
            "netto": round(brutto - ops, 2), "budget": budget,
            "comment": comments.get(label, "") or "", "n_ambiguous": 0,
            "currency": brand_currency(label), "subrows": subrows,
        }

    def _run_spec(spec) -> dict:
        conn = get_conn()
        try:
            return _spec_row(conn.cursor(as_dict=True), spec)
        finally:
            conn.close()

    def _run_dk(cur=None) -> list[dict]:
        # DK Job/Banner med kilde-brand-opdeling (Watch DK / FINANS DK / Finans
        # programmatisk) — spejler banner-/job-performance-dashboardet. Isoleret, så
        # en fejl her (fx manglende ProgrammaticSales) ikke vælter Norge/MarketWire.
        # Kun i Business Media-rapporten (dk_split) — Monitor har sine egne specs.
        conn = None
        try:
            if cur is None:
                conn = get_conn()
                cur_ = conn.cursor(as_dict=True)
            else:
                cur_ = cur
            return _dk_advertising_brand_rows(cur_, date_from, date_to, comments)
        except Exception:
            logger.exception("DK annonce-opdeling (Job/Banner) fejlede")
            return []
        finally:
            if conn is not None:
                conn.close()

    try:
        if parallel:
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=min(4, len(specs_list) + 1)) as ex:
                futs = [ex.submit(_run_spec, s) for s in specs_list]
                fut_dk = ex.submit(_run_dk) if dk_split else None
                rows = [f.result() for f in futs]
                if fut_dk is not None:
                    rows += fut_dk.result()
        else:
            conn = get_conn()
            cur = conn.cursor(as_dict=True)
            rows = [_spec_row(cur, s) for s in specs_list]
            if dk_split:
                rows += _run_dk(cur)
            conn.close()
    except Exception:
        logger.exception("pipedrive_brand_rows fejlede")
        return []
    return rows


def _dk_advertising_brand_rows(cur, date_from: str | None, date_to: str | None,
                               comments: dict) -> list[dict]:
    """DK-annoncerækkerne (Job + Banner) med kilde-brand-opdeling.

    Spejler banner-/job-performance-dashboardet (modul_rotation): omsætningen
    splittes pr. kilde-brand via deals' [sites] → Watch DK og FINANS DK. For Banner
    lægges FINANS' programmatiske salg (ProgrammaticSales) oveni som EGEN underrække
    'Finans programmatisk'. Monitor indgår IKKE (pillet ud af rapporten), så
    hovedrækkens total = Σ underrækker. Genbruger dashboardets site-lister og
    administrative-eksklusion, så tallene matcher dashboardet.

    Beløb i DKK; annoncesalg har ingen cancellation-pipelines → churn=0, net=sale.
    Budget (på hovedrækken) fra AD_BUDGET_WHERE (ekskl. Monitor). Hver underrække
    har sit eget kilde-brand-budget (Watch DK / FINANS DK direkte / FINANS DK
    programmatisk), så Σ underrække-budget = hovedrækkens budget.
    """
    from moduler.modul_admin_nysalg.brands import AD_BUDGET_WHERE
    # Genbrug dashboardets site-lister + administrative-eksklusion (én kilde til
    # sandheden), så rapporten stemmer med banner-/job-dashboardet.
    from moduler.modul_rotation.queries import (WATCH_DK_SITES, FINANS_SITES,
                                                WATCH_INT_SITES, _ADM_EXCLUDE)
    comments = comments or {}
    # Watch DK-serien i dashboardet samler også Watch Int-sites under Watch DK.
    watch_sites = list(WATCH_DK_SITES) + list(WATCH_INT_SITES)
    val = f"CAST({deal_value_sql()} AS DECIMAL(18,2))"

    def _site_revenue(pipeline: str, sites: list[str]) -> float:
        ph = "(" + ",".join(["%s"] * len(sites)) + ")"
        dcl, dpar = _date_between("[service_activation_date]", date_from, date_to,
                                  cast_to_date=True)
        where = ["[status] = 'won'", "[account] = 'jppol_advertising'",
                 "LOWER([pipeline_name]) = %s", f"[sites] IN {ph}"] + dcl
        cur.execute(
            f"SELECT ISNULL(SUM({val}),0) AS rev FROM [dbo].[PipedriveDeals] "
            f"WHERE {' AND '.join(where)} {_ADM_EXCLUDE}",
            (pipeline,) + tuple(sites) + tuple(dpar))
        return round(float((cur.fetchone() or {}).get("rev", 0) or 0), 2)

    def _programmatic_revenue() -> float:
        dcl, dpar = _date_between("[Date]", date_from, date_to, cast_to_date=True)
        where = ["[Site] = 'FINANS DK'"] + dcl
        cur.execute(
            "SELECT ISNULL(SUM([Amount]),0) AS rev FROM [dbo].[ProgrammaticSales] "
            f"WHERE {' AND '.join(where)}", tuple(dpar))
        return round(float((cur.fetchone() or {}).get("rev", 0) or 0), 2)

    def _sub(name: str, sale: float, budget: float | None = None) -> dict:
        return {"brand": name, "brutto": sale, "adm_nysalg": 0.0,
                "opsigelser": 0.0, "adm_opsigelser": 0.0, "netto": sale,
                "budget": budget, "currency": "DKK"}

    out: list[dict] = []
    for pipeline, label in (("job", "Job"), ("banner", "Banner")):
        # Budget pr. kilde-brand: Watch DK og FINANS DK (direkte = Salestype ≠
        # 'Programmatic'). Σ kilde-budget = hovedrækkens AD_BUDGET_WHERE-budget, så
        # subtotalerne stemmer. Programmatisk har sit eget Salestype='Programmatic'-
        # budget (kun Banner).
        watch = _site_revenue(pipeline, watch_sites)
        finans = _site_revenue(pipeline, FINANS_SITES)
        watch_bud = round(_budget_for_where(
            cur, f"[Brand]='Watch DK' AND [DealType]='{label}'", date_from, date_to), 2)
        finans_bud = round(_budget_for_where(
            cur, f"[Brand]='FINANS DK' AND [DealType]='{label}' "
                 "AND COALESCE([Salestype],'') <> 'Programmatic'", date_from, date_to), 2)
        finans_sub = _sub("FINANS DK", finans, finans_bud)
        if pipeline == "job":
            # Finans har intet direkte jobsalg — Finans' andel dækkes af en intern
            # allokering (6,25 % af WM DK's jobsalg), så rækken står som 0 her.
            # Noten vises i rapporten (Excel-kommentar + PDF-fodnote).
            finans_sub["note"] = (
                "Job sales for Finans are shown as 0 because 6.25% of the job "
                "sales are internally allocated from WM DK to Finans.")
        subrows = [_sub("Watch DK", watch, watch_bud), finans_sub]
        total = watch + finans
        if pipeline == "banner":
            prog = _programmatic_revenue()
            prog_bud = round(_budget_for_where(
                cur, "[Brand]='FINANS DK' AND [DealType]='Banner' "
                     "AND [Salestype]='Programmatic'", date_from, date_to), 2)
            subrows.append(_sub("Finans programmatisk", prog, prog_bud))
            total += prog
        budget = round(_ad_budget(cur, label, date_from, date_to), 2)
        out.append({
            "brand": label, "brutto": round(total, 2), "adm_nysalg": 0.0,
            "opsigelser": 0.0, "adm_opsigelser": 0.0, "netto": round(total, 2),
            "budget": budget, "comment": comments.get(label, "") or "",
            "n_ambiguous": 0, "currency": "DKK", "subrows": subrows,
        })
    return out


# ── Monitor-rapportens annoncerækker (Job + Banner) ──────────────────────────
# Monitor-annoncesalget ligger IKKE i monitor-kontoen — den har kun abonnements-
# pipelines (Customer/Newbizz/Company Trial/…). Banner- og jobdeals for monitor-
# sites ligger i den fælles danske annoncekonto 'jppol_advertising' med
# pipeline_name 'Banner'/'Job' og genkendes på [sites]. Business Media-rapportens
# DK-annoncerækker summerer kun Watch DK + FINANS DK-sites, så Monitor-andelen
# hører netop her (de to rapporter tilsammen = hele DK-annoncesalget).
#
# Beløb i DKK. Annoncesalg har ingen cancellation-pipelines → opsigelser = 0.
# Samme administrative-eksklusion som banner-/job-dashboardet og Business Media-
# rapportens DK-rækker (_ADM_EXCLUDE), så tallene er sammenlignelige. Budgettet
# ligger samlet på 'All Monitor Sites', så kun hovedrækken har budget —
# underrækkerne pr. site vises med '—'.

def _monitor_ad_revenue(cur, pipeline: str, date_from: str | None,
                        date_to: str | None, by_month: bool = False) -> dict:
    """{(ym, site): omsætning} for én annonce-pipeline på Monitor-sites.

    ym er '' når by_month er False (hele perioden i én bucket).
    """
    from moduler.modul_rotation.queries import MONITOR_SITES, _ADM_EXCLUDE
    ph = "(" + ",".join(["%s"] * len(MONITOR_SITES)) + ")"
    dcl, dpar = _date_between("[service_activation_date]", date_from, date_to,
                              cast_to_date=True)
    where = ["[status] = 'won'", "[account] = 'jppol_advertising'",
             "LOWER([pipeline_name]) = %s", f"[sites] IN {ph}"] + dcl
    val = f"CAST({deal_value_sql()} AS DECIMAL(18,2))"
    ym_expr = "CONVERT(varchar(7), [service_activation_date], 126)"
    # GROUP BY på en konstant er ikke lovligt i SQL Server — uden måneds-opdeling
    # grupperes der kun på site, og ym-feltet sættes til ''.
    select_ym = ym_expr if by_month else "''"
    group_by = f"{ym_expr}, [sites]" if by_month else "[sites]"
    cur.execute(
        f"SELECT {select_ym} AS ym, [sites] AS site, ISNULL(SUM({val}),0) AS rev "
        f"FROM [dbo].[PipedriveDeals] "
        f"WHERE {' AND '.join(where)} {_ADM_EXCLUDE} GROUP BY {group_by}",
        (pipeline,) + tuple(MONITOR_SITES) + tuple(dpar))
    return {((r["ym"] or "")[:7], (r["site"] or "").strip()):
            round(float(r["rev"] or 0), 2)
            for r in cur.fetchall() or []}


def _monitor_ad_rows(cur, date_from: str | None, date_to: str | None,
                     comments: dict, yms: list[str]) -> dict[str, list[dict]]:
    """{ym: [Job-række, Banner-række]} for Monitor-annoncesalget.

    yms == [''] betyder hele perioden i én bucket (uden måneds-opdeling); ellers
    hentes alle måneder i ét sæt GROUP BY-queries, som resten af Fase B.
    """
    from moduler.modul_admin_nysalg.brands import MONITOR_AD_BUDGET_WHERE
    comments = comments or {}
    by_month = yms != [""]

    def _sub(site: str, sale: float) -> dict:
        # Budget kun på hovedrækken ('All Monitor Sites') → None pr. site.
        return {"brand": site, "brutto": sale, "adm_nysalg": 0.0,
                "opsigelser": 0.0, "adm_opsigelser": 0.0, "netto": sale,
                "budget": None, "comment": "", "n_ambiguous": 0,
                "currency": "DKK"}

    out: dict[str, list[dict]] = {ym: [] for ym in yms}
    for pipeline, label in (("job", "Job"), ("banner", "Banner")):
        rev = _monitor_ad_revenue(cur, pipeline, date_from, date_to, by_month=by_month)
        frag = MONITOR_AD_BUDGET_WHERE[label]
        if by_month:
            bud_by_ym = _budget_by_month_for_where(cur, frag, date_from, date_to)
            total_bud = None
        else:
            bud_by_ym = None
            total_bud = round(_budget_for_where(cur, frag, date_from, date_to), 2)
        for ym in yms:
            # Underrækker pr. site, største først. Sites uden salg i perioden
            # udelades — de har intet site-budget at holdes op mod.
            sites = sorted(((site, v) for (y, site), v in rev.items()
                            if y == ym and v),
                           key=lambda kv: (-kv[1], kv[0]))
            total = round(sum(v for _, v in sites), 2)
            out[ym].append({
                "brand": label, "brutto": total, "adm_nysalg": 0.0,
                "opsigelser": 0.0, "adm_opsigelser": 0.0, "netto": total,
                "budget": (round(bud_by_ym.get(ym, 0.0), 2) if by_month
                           else total_bud),
                "comment": comments.get(label, "") or "", "n_ambiguous": 0,
                "currency": "DKK",
                "subrows": [_sub(site, v) for site, v in sites],
            })
    return out


def monitor_advertising_brand_rows(date_from: str | None, date_to: str | None,
                                   comments: dict | None = None) -> list[dict]:
    """Annoncerækkerne (Job + Banner) til Monitor-rapporten. [] ved DB-fejl.

    Isoleret fejlhåndtering som DK-opdelingen: en fejl her må ikke vælte
    abonnements-rækkerne i rapporten.
    """
    conn = None
    try:
        conn = get_conn()
        return _monitor_ad_rows(conn.cursor(as_dict=True), date_from, date_to,
                                comments or {}, [""])[""]
    except Exception:
        logger.exception("Monitor annonce-rækker (Job/Banner) fejlede")
        return []
    finally:
        if conn is not None:
            conn.close()


def monitor_advertising_by_month(months: list[str], comments: dict | None = None
                                 ) -> dict[str, list[dict]]:
    """{'YYYY-MM': annoncerækker} — by-month-varianten af
    monitor_advertising_brand_rows (alle måneder i ét sæt GROUP BY-queries).

    Returnerer {} ved DB-fejl — kalderen behandler manglende måneder som ingen
    ekstra rækker."""
    months = [ym for ym in months if ym]
    if not months:
        return {}
    q_from = _month_bounds(months[0])[0]
    q_to = _month_bounds(months[-1])[1]
    conn = None
    try:
        conn = get_conn()
        return _monitor_ad_rows(conn.cursor(as_dict=True), q_from, q_to,
                                comments or {}, months)
    except Exception:
        logger.exception("Monitor annonce-rækker pr. måned (Job/Banner) fejlede")
        return {}
    finally:
        if conn is not None:
            conn.close()


def pipedrive_brand_rows_by_month(months: list[str], comments: dict | None = None,
                                  budgets_by_month: dict | None = None,
                                  specs: list | None = None,
                                  dk_split: bool = True) -> dict[str, list[dict]]:
    """{'YYYY-MM': brand-rækker} — by-month-varianten af pipedrive_brand_rows.

    Leverer præcis de samme rækker som pipedrive_brand_rows kaldt én gang pr.
    måned (med månedens grænser), men med GROUP BY måned, så hele perioden koster
    ét fast antal queries i stedet for ~25 pr. måned (Fase B). Alle måneder i
    `months` får en række pr. spec — også måneder uden deals (0-tal + månedens
    budget). budgets_by_month ({'YYYY-MM': {label: budget}}, jf.
    brand_budgets_by_month) bruges for specs uden egen budget_where/AD-mapping.
    Hver spec (+ DK-opdelingen) kører i egen tråd/forbindelse.
    Returnerer {} ved DB-fejl — kalderen behandler manglende måneder som ingen
    ekstra rækker, som pipedrive_brand_rows' [].
    """
    from constants import CANCELLATION_PIPELINES
    from moduler.modul_admin_nysalg.brands import (AD_BUDGET_WHERE, PIPEDRIVE_ROWS,
                                                   brand_currency)
    comments = comments or {}
    budgets_by_month = budgets_by_month or {}
    specs_list = PIPEDRIVE_ROWS if specs is None else specs
    months = [ym for ym in months if ym]
    if not months:
        return {}
    # Hele kalendermåneder — spejler _month_bounds-grænserne i pr-måned-kaldene.
    q_from = _month_bounds(months[0])[0]
    q_to = _month_bounds(months[-1])[1]
    val = deal_value_sql()
    ym_expr = "CONVERT(varchar(7), [service_activation_date], 126)"
    cancel_ph = "(" + ",".join(["%s"] * len(CANCELLATION_PIPELINES)) + ")"
    cancel_up = [p.upper() for p in CANCELLATION_PIPELINES]

    def _grouped(cur, where: list[str], where_params: list) -> dict:
        """{ym: (brutto, opsigelser)} for et WHERE — én query, GROUP BY måned."""
        cur.execute(f"""
            SELECT {ym_expr} AS ym,
              ISNULL(SUM(CASE WHEN UPPER([pipeline_name]) NOT IN {cancel_ph}
                  THEN CAST({val} AS DECIMAL(18,2)) ELSE 0 END), 0) AS won,
              ISNULL(ABS(SUM(CASE WHEN UPPER([pipeline_name]) IN {cancel_ph}
                  THEN CAST({val} AS DECIMAL(18,2)) ELSE 0 END)), 0) AS ops
            FROM [dbo].[PipedriveDeals]
            WHERE {' AND '.join(where)}
            GROUP BY {ym_expr}
        """, tuple(cancel_up) * 2 + tuple(where_params))
        return {(r["ym"] or "")[:7]: (round(float(r["won"] or 0), 2),
                                      round(float(r["ops"] or 0), 2))
                for r in cur.fetchall() or []}

    def _spec_months(spec) -> dict[str, dict]:
        """{ym: hovedrække} for én spec — samme rækkeform som _spec_row."""
        conn = get_conn()
        try:
            cur = conn.cursor(as_dict=True)
            where = [
                "[status] = 'won'",
                "COALESCE([administrativ],'') <> 'ja'",
                "[service_activation_date] IS NOT NULL",
                f"LOWER(LTRIM(RTRIM(COALESCE([{spec['scope_col']}],'')))) = %s",
            ]
            where_params: list = [spec["scope_val"].lower()]
            pipes = spec.get("pipelines")
            if pipes:
                pipe_ph = "(" + ",".join(["%s"] * len(pipes)) + ")"
                where.append(f"LOWER(LTRIM(RTRIM(COALESCE([pipeline_name],'')))) IN {pipe_ph}")
                where_params += [p.lower() for p in pipes]
            dclauses, dparams = _date_between(
                "[service_activation_date]", q_from, q_to, cast_to_date=True)
            where += dclauses
            where_params += dparams

            main = _grouped(cur, where, where_params)
            label = spec["label"]
            if spec.get("budget_where"):
                bud_by_ym = _budget_by_month_for_where(cur, spec["budget_where"], q_from, q_to)
            elif label in AD_BUDGET_WHERE:
                bud_by_ym = _budget_by_month_for_where(cur, AD_BUDGET_WHERE[label], q_from, q_to)
            else:
                bud_by_ym = None   # månedens brand-budget fra budgets_by_month

            sub_data: list[tuple] = []
            for sr in spec.get("subrows", []):
                if sr.get("site_like"):
                    # Mønsteret er kode-kontrolleret (brands.py) → literal LIKE.
                    site_cond = f"LOWER(COALESCE([sites],'')) LIKE '{sr['site_like']}'"
                    site_params: list = []
                else:
                    site_cond = "LOWER(LTRIM(RTRIM(COALESCE([sites],'')))) = %s"
                    site_params = [sr["site"].lower()]
                sr_months = _grouped(cur, where + [site_cond], where_params + site_params)
                sr_bud = (_budget_by_month_for_where(cur, sr["budget_where"], q_from, q_to)
                          if sr.get("budget_where") else None)
                sub_data.append((sr, sr_months, sr_bud))

            out: dict[str, dict] = {}
            for ym in months:
                brutto, ops = main.get(ym, (0.0, 0.0))
                if bud_by_ym is not None:
                    budget = round(bud_by_ym.get(ym, 0.0), 2)
                else:
                    budget = round(float((budgets_by_month.get(ym) or {})
                                         .get(label, 0.0) or 0.0), 2)
                subrows = []
                for sr, sr_months, sr_bud in sub_data:
                    s_brutto, s_ops = sr_months.get(ym, (0.0, 0.0))
                    subrows.append({
                        "brand": sr["label"], "brutto": s_brutto, "adm_nysalg": 0.0,
                        "opsigelser": s_ops, "adm_opsigelser": 0.0,
                        "netto": round(s_brutto - s_ops, 2),
                        "budget": (round(sr_bud.get(ym, 0.0), 2)
                                   if sr_bud is not None else None),
                        "comment": "", "n_ambiguous": 0,
                        "currency": brand_currency(label),
                    })
                out[ym] = {
                    "brand": label, "brutto": brutto, "adm_nysalg": 0.0,
                    "opsigelser": ops, "adm_opsigelser": 0.0,
                    "netto": round(brutto - ops, 2), "budget": budget,
                    "comment": comments.get(label, "") or "", "n_ambiguous": 0,
                    "currency": brand_currency(label), "subrows": subrows,
                }
            return out
        finally:
            conn.close()

    def _run_dk() -> dict[str, list[dict]]:
        # Isoleret som i pipedrive_brand_rows: en fejl her (fx manglende
        # ProgrammaticSales) vælter ikke Norge/MarketWire-rækkerne.
        conn = get_conn()
        try:
            return _dk_advertising_by_month(conn.cursor(as_dict=True),
                                            q_from, q_to, months, comments)
        except Exception:
            logger.exception("DK annonce-opdeling pr. måned (Job/Banner) fejlede")
            return {}
        finally:
            conn.close()

    try:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=min(4, len(specs_list) + 1)) as ex:
            futs = [ex.submit(_spec_months, s) for s in specs_list]
            fut_dk = ex.submit(_run_dk) if dk_split else None
            per_spec = [f.result() for f in futs]
            dk_rows = fut_dk.result() if fut_dk is not None else {}
    except Exception:
        logger.exception("pipedrive_brand_rows_by_month fejlede")
        return {}
    return {ym: [sp[ym] for sp in per_spec if ym in sp] + dk_rows.get(ym, [])
            for ym in months}


def _dk_advertising_by_month(cur, date_from: str | None, date_to: str | None,
                             months: list[str], comments: dict) -> dict[str, list[dict]]:
    """{'YYYY-MM': DK-annoncerækker (Job + Banner)} — by-month-varianten af
    _dk_advertising_brand_rows: samme rækker, men alle måneder i ét sæt
    GROUP BY-queries (Fase B)."""
    from moduler.modul_admin_nysalg.brands import AD_BUDGET_WHERE
    from moduler.modul_rotation.queries import (WATCH_DK_SITES, FINANS_SITES,
                                                WATCH_INT_SITES, _ADM_EXCLUDE)
    comments = comments or {}
    watch_sites = list(WATCH_DK_SITES) + list(WATCH_INT_SITES)
    val = f"CAST({deal_value_sql()} AS DECIMAL(18,2))"
    ym_expr = "CONVERT(varchar(7), [service_activation_date], 126)"

    def _site_revenue(pipeline: str, sites: list[str]) -> dict:
        ph = "(" + ",".join(["%s"] * len(sites)) + ")"
        dcl, dpar = _date_between("[service_activation_date]", date_from, date_to,
                                  cast_to_date=True)
        where = ["[status] = 'won'", "[account] = 'jppol_advertising'",
                 "LOWER([pipeline_name]) = %s", f"[sites] IN {ph}"] + dcl
        cur.execute(
            f"SELECT {ym_expr} AS ym, ISNULL(SUM({val}),0) AS rev "
            f"FROM [dbo].[PipedriveDeals] "
            f"WHERE {' AND '.join(where)} {_ADM_EXCLUDE} GROUP BY {ym_expr}",
            (pipeline,) + tuple(sites) + tuple(dpar))
        return {(r["ym"] or "")[:7]: round(float(r["rev"] or 0), 2)
                for r in cur.fetchall() or []}

    def _programmatic_revenue() -> dict:
        dcl, dpar = _date_between("[Date]", date_from, date_to, cast_to_date=True)
        where = ["[Site] = 'FINANS DK'"] + dcl
        cur.execute(
            "SELECT CONVERT(varchar(7), [Date], 126) AS ym, "
            "ISNULL(SUM([Amount]),0) AS rev FROM [dbo].[ProgrammaticSales] "
            f"WHERE {' AND '.join(where)} GROUP BY CONVERT(varchar(7), [Date], 126)",
            tuple(dpar))
        return {(r["ym"] or "")[:7]: round(float(r["rev"] or 0), 2)
                for r in cur.fetchall() or []}

    def _sub(name: str, sale: float, budget: float | None = None) -> dict:
        return {"brand": name, "brutto": sale, "adm_nysalg": 0.0,
                "opsigelser": 0.0, "adm_opsigelser": 0.0, "netto": sale,
                "budget": budget, "currency": "DKK"}

    prog_by_ym = _programmatic_revenue()
    prog_bud_by_ym = _budget_by_month_for_where(
        cur, "[Brand]='FINANS DK' AND [DealType]='Banner' "
             "AND [Salestype]='Programmatic'", date_from, date_to)
    out: dict[str, list[dict]] = {ym: [] for ym in months}
    for pipeline, label in (("job", "Job"), ("banner", "Banner")):
        watch_by_ym = _site_revenue(pipeline, watch_sites)
        finans_by_ym = _site_revenue(pipeline, FINANS_SITES)
        watch_bud_by_ym = _budget_by_month_for_where(
            cur, f"[Brand]='Watch DK' AND [DealType]='{label}'", date_from, date_to)
        finans_bud_by_ym = _budget_by_month_for_where(
            cur, f"[Brand]='FINANS DK' AND [DealType]='{label}' "
                 "AND COALESCE([Salestype],'') <> 'Programmatic'", date_from, date_to)
        main_bud_by_ym = _budget_by_month_for_where(
            cur, AD_BUDGET_WHERE[label], date_from, date_to)
        for ym in months:
            watch = watch_by_ym.get(ym, 0.0)
            finans = finans_by_ym.get(ym, 0.0)
            finans_sub = _sub("FINANS DK", finans, round(finans_bud_by_ym.get(ym, 0.0), 2))
            if pipeline == "job":
                # Samme fodnote som _dk_advertising_brand_rows (intern allokering).
                finans_sub["note"] = (
                    "Job sales for Finans are shown as 0 because 6.25% of the job "
                    "sales are internally allocated from WM DK to Finans.")
            subrows = [_sub("Watch DK", watch, round(watch_bud_by_ym.get(ym, 0.0), 2)),
                       finans_sub]
            total = watch + finans
            if pipeline == "banner":
                prog = prog_by_ym.get(ym, 0.0)
                subrows.append(_sub("Finans programmatisk", prog,
                                    round(prog_bud_by_ym.get(ym, 0.0), 2)))
                total += prog
            out[ym].append({
                "brand": label, "brutto": round(total, 2), "adm_nysalg": 0.0,
                "opsigelser": 0.0, "adm_opsigelser": 0.0, "netto": round(total, 2),
                "budget": round(main_bud_by_ym.get(ym, 0.0), 2),
                "comment": comments.get(label, "") or "",
                "n_ambiguous": 0, "currency": "DKK", "subrows": subrows,
            })
    return out


# Bureauer der køber abonnementer PÅ VEGNE AF slutkunden. Zuora fakturerer
# bureauet, så contact_companyname er bureauets navn — ikke den kunde tallet
# hører til. For dem springes Zuora-navnet over, og navnet hentes i PipeDrive,
# hvor dealen ligger på den rigtige slutkunde. Matches på præfiks, så
# 'Prenax AB', 'Prenax A/S' m.fl. også fanges.
AGENCY_NAME_PREFIXES = ("prenax",)


def is_agency_name(name: str) -> bool:
    """True hvis navnet er et abonnementsbureau og ikke slutkunden."""
    n = (name or "").strip().lower()
    return bool(n) and n.startswith(AGENCY_NAME_PREFIXES)


def customer_name(m: dict, org_names: dict | None = None) -> str:
    """Kundenavn for en bevægelsesrække — '' hvis intet navn kan findes.

    Kilderækkefølge:
      1. contact_companyname fra Zuora-udtrækket (primær kilde)
      2. org_name på den matchede PipeDrive-deal
      3. opslag på org-id i brandets PipeDrive-KONTO (org-id er ikke unikke på
         tværs af konti, så opslaget SKAL scopes — se brands.BRAND_ACCOUNT)

    Bureaurækker (Prenax) springer trin 1 over: dér er Zuora-navnet bureauets,
    mens PipeDrive-dealen ligger på slutkunden. Ældre runs har ingen
    contact_companyname og falder derfor helt naturligt til trin 2/3.
    """
    from moduler.modul_admin_nysalg.brands import brand_account
    zuora = (m.get("contact_companyname") or "").strip()
    if zuora and not is_agency_name(zuora):
        return zuora
    hit = (m.get("matched_org_name") or "").strip()
    if hit:
        return hit
    acct = brand_account(m.get("brand") or "")
    if acct and org_names:
        pid = str(m.get("pipedrive_id") or "").strip()
        hit = (org_names.get(acct, {}).get(pid, "") or "").strip()
        if hit:
            return hit
    # Bureaunavnet er bedre end ingenting, hvis PipeDrive ikke kender kunden.
    return zuora


def pipedrive_org_names() -> dict:
    """{konto (lower): {org_id (str): org_name}} fra PipedriveDeals.

    Bruges til at sætte virksomhedsnavn på Zuora-bevægelser der ikke matchede en
    administrativ deal (de har kun org-id, ikke navn, fra udtrækket). Org-id er
    IKKE unikke på tværs af konti, så opslaget er scopet pr. konto — opslagssiden
    vælger kontoen ud fra brandet (brands.BRAND_ACCOUNT). Returnerer {} ved DB-fejl.
    """
    out: dict = {}
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute("""
            SELECT [account], [org_id], MAX([org_name]) AS org_name
            FROM [dbo].[PipedriveDeals]
            WHERE [org_id] IS NOT NULL AND [account] IS NOT NULL
              AND LTRIM(RTRIM(COALESCE([org_name],''))) <> ''
            GROUP BY [account], [org_id]
        """)
        for r in cur.fetchall() or []:
            acct = (r["account"] or "").strip().lower()
            out.setdefault(acct, {})[str(r["org_id"]).strip()] = (r["org_name"] or "").strip()
        conn.close()
    except Exception:
        logger.exception("pipedrive_org_names fejlede")
        return {}
    return out


def period_pipedrive_deals(date_from: str | None, date_to: str | None) -> list[dict]:
    """Alle won PipeDrive-deals med service_activation_date i intervallet.

    Bruges til afstemningsfanen i Excel-rapporten, så PipeDrive-kilden kan holdes
    op imod Zuora-bevægelserne. Brandet udledes med samme classify() som
    bevægelserne, så de to faner kan sammenlignes pr. brand. date_from/date_to =
    ISO YYYY-MM-DD (inkl.); begge None = alle datoer. Returnerer [] ved DB-fejl.
    """
    from moduler.modul_admin_nysalg.brands import classify
    where = ["[status] = 'won'", "[service_activation_date] IS NOT NULL"]
    params: list = []
    dclauses, dparams = _date_between(
        "[service_activation_date]", date_from, date_to, cast_to_date=True)
    where += dclauses
    params += dparams
    sql = f"""
        SELECT [pd_deal_id], [org_id], [org_name], [sites],
               [value], [value_dkk], [currency],
               CONVERT(varchar(10), [service_activation_date], 23) AS sad,
               [pipeline_name], [status], [account], [team], [administrativ]
        FROM [dbo].[PipedriveDeals]
        WHERE {' AND '.join(where)}
        ORDER BY [sites], [service_activation_date]
    """
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(sql, tuple(params))
        rows = cur.fetchall() or []
        conn.close()
    except Exception:
        logger.exception("period_pipedrive_deals fejlede")
        return []
    out: list[dict] = []
    for r in rows:
        site = (r.get("sites") or "").strip()
        cur_code = (r.get("currency") or "").strip().upper()
        local = float(r.get("value") or 0)
        dkk = float(r.get("value_dkk") or 0)
        # Lokal valuta for NOK/SEK (jf. perf-modulet), ellers DKK.
        value = local if cur_code in ("NOK", "SEK") else (dkk or local)
        out.append({
            "deal_id": str(r.get("pd_deal_id") or ""),
            "org_id": str(r.get("org_id") or "").strip(),
            "org_name": (r.get("org_name") or "").strip(),
            "site": site,
            "brand": classify(site),
            "value": value,
            "currency": cur_code or "DKK",
            "service_activation_date": r.get("sad") or "",
            "pipeline": (r.get("pipeline_name") or "").strip(),
            "status": (r.get("status") or "").strip(),
            "account": (r.get("account") or "").strip(),
            "team": (r.get("team") or "").strip(),
            "administrativ": str(r.get("administrativ") or "").strip().lower() == "ja",
        })
    return out


def _row_opsigelse(m: dict) -> float:
    """Brutto afgang på en række (gross_out, fallback |net_diff| ved afgang)."""
    go = m.get("gross_out") or 0
    if go:
        return go
    nd = m.get("net_diff") or 0
    return abs(nd) if nd < 0 else 0.0


def effective_gross_in(m: dict) -> float:
    """Gross in der faktisk tæller med — direktørens manuelle efterbehandling.

    Udeladt række (total_excluded) tæller 0; ellers bruges gross_in_override hvis
    sat (fx falsk gross-in rettet til 0 ved skift af kontonummer), ellers den rå
    gross_in fra udtrækket.
    """
    if m.get("total_excluded"):
        return 0.0
    ov = m.get("gross_in_override")
    return float(ov) if ov is not None else (m.get("gross_in") or 0.0)


def effective_gross_out(m: dict) -> float:
    """Gross out (opsigelse) der faktisk tæller med — jf. effective_gross_in."""
    if m.get("total_excluded"):
        return 0.0
    ov = m.get("gross_out_override")
    return float(ov) if ov is not None else _row_opsigelse(m)


# Delvist administrative bevægelser: er den matchede deals værdi mindst så meget
# mindre end gross-beløbet, er kun deal-værdien administrativ — resten tæller som
# almindeligt salg/churn. Mindre afvigelser (afrunding/valutakurs) behandles som
# helt administrative.
ADM_PARTIAL_TOLERANCE = 0.01   # 1 %


def auto_adm_share(gross: float, deal_value: Optional[float]) -> float:
    """Automatisk administrativ andel af et gross-beløb for en admin-række.

    |deal_value| bruges når den er mere end tolerancen mindre end gross (delvist
    administrativ, fx deal 110.000 mod gross in 250.000); ellers hele gross
    (alt-eller-intet som hidtil). Ingen deal-værdi (fx Zuora-flag uden match
    eller manuel include) => hele gross.
    """
    if gross <= 0 or deal_value is None:
        return gross if gross > 0 else 0.0
    dv = abs(float(deal_value))
    return dv if dv < gross * (1 - ADM_PARTIAL_TOLERANCE) else gross


def effective_adm_in(m: dict) -> float:
    """Administrativ andel af gross in — det der trækkes fra Actual Sale.

    Udeladt række tæller 0. adm_in_override (direktøren har skrevet præcis hvor
    meget der er administrativt) bruges hvis sat; ellers, for administrative
    rækker (effective_is_admin), den automatiske andel: deal-værdien når den er
    mindre end gross in (delvist administrativ), ellers hele gross in.
    Ikke-administrativ række => 0.
    """
    if m.get("total_excluded"):
        return 0.0
    ov = m.get("adm_in_override")
    if ov is not None:
        return float(ov)
    if not effective_is_admin(m):
        return 0.0
    deal = m.get("matched_value") if m.get("match_sign") == "pos" else None
    return auto_adm_share(effective_gross_in(m), deal)


def effective_adm_out(m: dict) -> float:
    """Administrativ andel af gross out (opsigelse) — jf. effective_adm_in.

    Deal-værdien (negativ ved administrativ opsigelse) bruges kun når rækken
    matchede en negativ deal; rækker der alene bærer Zuoras administrativ-flag
    har ingen deal-værdi og forbliver alt-eller-intet.
    """
    if m.get("total_excluded"):
        return 0.0
    ov = m.get("adm_out_override")
    if ov is not None:
        return float(ov)
    if not is_admin_opsigelse(m):
        return 0.0
    deal = m.get("matched_value") if m.get("match_sign") == "neg" else None
    return auto_adm_share(effective_gross_out(m), deal)


def summarize_by_brand(matches: list[dict], budgets: dict | None = None,
                       comments: dict | None = None,
                       extra_rows: list[dict] | None = None,
                       ensure_labels: list[str] | None = None,
                       seed_defaults: bool = True) -> list[dict]:
    """Per-brand totaltal med de administrative bevægelser udskilt:

      brutto          = al bruttoomsætning/nysalg (Σ gross_in)
      adm_nysalg      = heraf administrativt nysalg (trækkes fra) — effective_is_admin
      opsigelser      = alle opsigelser (Σ gross_out)
      adm_opsigelser  = heraf administrative opsigelser (trækkes fra) — administrativ-flag
      netto (tilvækst)= (brutto − adm_nysalg) − (opsigelser − adm_opsigelser)
      budget          = samlet mediebudget for brandet

    ensure_labels = labels der altid skal have en (0-)række, ud over dem med
    bevægelser. seed_defaults=False slår Business Media-rapportens faste rækker
    (ALWAYS_SHOWN/BUDGET_BRANDS) fra — bruges af Monitor-rapporten, hvor
    rækkerne er sites, ikke brands.
    """
    from moduler.modul_admin_nysalg.brands import (ALWAYS_SHOWN, DISPLAY_ORDER,
                                                   BUDGET_BRANDS, brand_currency, classify)
    budgets = budgets or {}
    comments = comments or {}

    groups: dict[str, dict] = {}

    def _bucket(label):
        return groups.setdefault(label, {
            "brand": label, "brutto": 0.0, "adm_nysalg": 0.0,
            "opsigelser": 0.0, "adm_opsigelser": 0.0, "netto": 0.0,
            "budget": 0.0, "comment": "", "n_ambiguous": 0,
            "currency": brand_currency(label),
        })

    for m in matches:
        label = m.get("brand") or classify(m.get("site"))
        g = _bucket(label)
        g["brutto"] += effective_gross_in(m)
        g["opsigelser"] += effective_gross_out(m)
        # Administrativ andel — deal-værdien ved delvist administrative rækker
        # (auto eller manuelt sat), ellers alt-eller-intet.
        g["adm_nysalg"] += effective_adm_in(m)
        g["adm_opsigelser"] += effective_adm_out(m)
        if m.get("ambiguous"):
            g["n_ambiguous"] += 1

    # Brands der altid skal vises + brands med budget (også uden bevægelser).
    if seed_defaults:
        for label in list(ALWAYS_SHOWN) + list(BUDGET_BRANDS.keys()):
            if budgets.get(label) or label in ALWAYS_SHOWN:
                _bucket(label)
    for label in ensure_labels or []:
        _bucket(label)

    for label, g in groups.items():
        g["budget"] = round(budgets.get(label, 0.0), 2)
        g["comment"] = comments.get(label, "") or ""
        for k in ("brutto", "adm_nysalg", "opsigelser", "adm_opsigelser"):
            g[k] = round(g[k], 2)
        g["netto"] = round((g["brutto"] - g["adm_nysalg"])
                           - (g["opsigelser"] - g["adm_opsigelser"]), 2)

    # Reklame-rækker er allerede færdigberegnede (eget budget) — læg dem oveni.
    for er in (extra_rows or []):
        groups[er["brand"]] = er

    def _order(label):
        return DISPLAY_ORDER.index(label) if label in DISPLAY_ORDER else len(DISPLAY_ORDER)

    return sorted(groups.values(), key=lambda x: (_order(x["brand"]), x["brand"]))


# ── Niche-opdeling (pr. site) ────────────────────────────────────────────────

def summarize_by_site(matches: list[dict],
                      brands: tuple[str, ...] = ("Watch DK", "Watch NO")) -> list[dict]:
    """Per-niche (site) totaltal for de valgte brand-grupper.

    `site` i udtrækket ER nichen (fx 'FinansWatch DK', 'EnergiWatch NO'), så en
    niche-visning er blot en aggregering af bevægelserne pr. (brand, site). Bruges
    til "Niches"-fanen i Excel-rapporten. Administrative bevægelser trækkes fra, så
    tallene matcher Actual Sale/Churn/Net i hovedrapporten:

      sale  = Σ gross_in  − Σ administrativt nysalg
      churn = Σ gross_out − Σ administrative opsigelser
      net   = sale − churn

    Returnerer rækker sorteret efter (brand-rækkefølge, site), hver med land/valuta.
    """
    from moduler.modul_admin_nysalg.brands import (DISPLAY_ORDER, brand_currency,
                                                   brand_geo, classify)
    wanted = set(brands)
    groups: dict[tuple[str, str], dict] = {}
    for m in matches:
        label = m.get("brand") or classify(m.get("site"))
        if label not in wanted:
            continue
        site = (m.get("site") or "").strip() or "—"
        key = (label, site)
        g = groups.get(key)
        if g is None:
            country, _ = brand_geo(label)
            g = groups[key] = {
                "brand": label, "site": site, "country": country,
                "currency": brand_currency(label),
                "sale": 0.0, "churn": 0.0, "net": 0.0,
            }
        g["sale"] += effective_gross_in(m) - effective_adm_in(m)
        g["churn"] += effective_gross_out(m) - effective_adm_out(m)

    def _order(label):
        return DISPLAY_ORDER.index(label) if label in DISPLAY_ORDER else len(DISPLAY_ORDER)

    out = sorted(groups.values(), key=lambda x: (_order(x["brand"]), x["site"]))
    for g in out:
        g["sale"] = round(g["sale"], 2)
        g["churn"] = round(g["churn"], 2)
        g["net"] = round(g["sale"] - g["churn"], 2)
    return out


# ── Måneds-opdeling ──────────────────────────────────────────────────────────

def months_in_range(date_from: str | None, date_to: str | None) -> list[str]:
    """Kalendermåneder ('YYYY-MM') fra date_from til date_to (inkl.). [] hvis ukendt."""
    if not date_from or not date_to:
        return []
    try:
        f = date.fromisoformat(date_from)
        t = date.fromisoformat(date_to)
    except ValueError:
        return []
    out: list[str] = []
    y, m = f.year, f.month
    while (y, m) <= (t.year, t.month):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return out


def _month_bounds(ym: str) -> tuple[str, str]:
    """('YYYY-MM') -> (første dag, sidste dag) som ISO-strenge."""
    y, m = int(ym[:4]), int(ym[5:7])
    last = calendar.monthrange(y, m)[1]
    return f"{y:04d}-{m:02d}-01", f"{y:04d}-{m:02d}-{last:02d}"


def run_date_range(run: dict) -> tuple[str | None, str | None]:
    """(date_from, date_to) for et run.

    Bruger de nye period_from/period_to-kolonner. For gamle runs (NULL) falder den
    tilbage til den ældre enkelt-måneds 'YYYY-MM'-periode, så historiske runs stadig
    afgrænses korrekt på budget-/PipeDrive-queries.
    """
    df, dt = run.get("period_from"), run.get("period_to")
    if df or dt:
        return df, dt
    period = (run.get("period") or "").strip()
    if (len(period) == 7 and period[4] == "-"
            and period[:4].isdigit() and period[5:].isdigit()):
        return _month_bounds(period)
    return None, None


def brand_rows_by_month(matches: list[dict], date_from: str | None, date_to: str | None,
                        comments: dict | None = None,
                        scope: str = "business_media") -> dict[str, list[dict]]:
    """{'YYYY-MM': [brand_rows]} pr. måned i intervallet.

    Mangler datointervallet udledes månederne af bevægelsernes month_end. For hver
    måned genberegnes brand-rækkerne (Zuora-matches + PipeDrive-only-rækker + budget)
    med de eksisterende helpere, så summen pr. måned matcher den samlede visning.
    scope='monitor': rækker pr. Monitor-site (matches skal være relabel'et) med
    site-budgetter og Monitor Job/Banner som annonce-rækker.

    Budgetter OG PipeDrive-rækkerne hentes for ALLE måneder i ét fast antal
    GROUP BY-kald (Fase B — pipedrive_brand_rows_by_month), så selve
    måneds-opdelingen er ren beregning uafhængig af periodelængden. Tidligere
    kostede hver måned ~25 PipeDrive-/budget-queries.
    """
    comments = comments or {}
    months = months_in_range(date_from, date_to)
    if not months:
        months = sorted({(m.get("month_end") or "")[:7]
                         for m in matches if m.get("month_end")})
    months = [ym for ym in months if ym]
    if not months:
        return {}
    # Uden eksplicit interval (månederne udledt af month_end) afgrænses budget-
    # kaldet til de udledte måneder.
    b_from = date_from or _month_bounds(months[0])[0]
    b_to = date_to or _month_bounds(months[-1])[1]
    matches_by_ym: dict[str, list[dict]] = {}
    for m in matches:
        matches_by_ym.setdefault((m.get("month_end") or "")[:7], []).append(m)

    if scope == "monitor":
        norm_budgets_all = monitor_site_budgets_by_month(b_from, b_to)
        pd_by_month = monitor_advertising_by_month(months, comments)

        def _one_month(ym: str) -> list[dict]:
            return monitor_brand_rows(matches_by_ym.get(ym, []),
                                      norm_budgets_all.get(ym, {}), comments,
                                      extra_rows=pd_by_month.get(ym, []))
    else:
        budgets_all = brand_budgets_by_month(b_from, b_to)
        pd_by_month = pipedrive_brand_rows_by_month(
            months, comments, budgets_by_month=budgets_all)

        def _one_month(ym: str) -> list[dict]:
            return summarize_by_brand(matches_by_ym.get(ym, []),
                                      budgets_all.get(ym, {}), comments,
                                      extra_rows=pd_by_month.get(ym, []))

    return {ym: _one_month(ym) for ym in months}


def effective_is_admin(match_row: dict) -> bool:
    """Endeligt admin-flag for en gemt match-række, justeret af override.

    exclude => tæller som normalt nysalg (ikke admin).
    include => tvinges som administrativt nysalg.
    ellers  => det matchede is_admin.
    """
    ov = match_row.get("override")
    if ov == "exclude":
        return False
    if ov == "include":
        return True
    return bool(match_row.get("is_admin"))


def is_admin_opsigelse(match_row: dict) -> bool:
    """Administrativ opsigelse for en gemt match-række.

    Tæller som administrativ hvis enten Zuoras `administrativ`-flag er sat, ELLER
    rækken matchede en negativ administrativ deal i PipeDrive (match_sign='neg') —
    så en opsigelse uden flaget i Zuora stadig fanges af det administrative match.
    """
    return bool(match_row.get("administrativ")) or match_row.get("match_sign") == "neg"


def summarize(matches: list[dict]) -> dict:
    """Topkort-tal på tværs af alle brands:

      brutto         = al bruttoomsætning/nysalg (Σ gross_in)
      adm_nysalg     = heraf administrativt nysalg (trækkes fra)
      opsigelser     = alle opsigelser (Σ gross_out)
      adm_opsigelser = heraf administrative opsigelser (trækkes fra)
      netto_tilvaekst= (brutto − adm_nysalg) − (opsigelser − adm_opsigelser)
    """
    brutto = adm_nysalg = opsigelser = adm_opsigelser = 0.0
    n_admin = n_ambiguous = 0
    for m in matches:
        brutto += effective_gross_in(m)
        opsigelser += effective_gross_out(m)
        ai = effective_adm_in(m)
        adm_nysalg += ai
        # Tæl både helt administrative rækker og rækker med en delvis adm. andel.
        if effective_is_admin(m) or ai > 0:
            n_admin += 1
        adm_opsigelser += effective_adm_out(m)
        if m.get("ambiguous"):
            n_ambiguous += 1
    netto = (brutto - adm_nysalg) - (opsigelser - adm_opsigelser)
    return {
        "brutto": round(brutto, 2),
        "adm_nysalg": round(adm_nysalg, 2),
        "opsigelser": round(opsigelser, 2),
        "adm_opsigelser": round(adm_opsigelser, 2),
        "netto_tilvaekst": round(netto, 2),
        "n_admin": n_admin,
        "n_ambiguous": n_ambiguous,
    }


# ── SQL-aggregering (Fase B) ─────────────────────────────────────────────────
# Topkort- og per-brand-tallene beregnes i SQL Server som GROUP BY i stedet for
# at hente alle match-rækker over netværket og regne i Python. effective_*-
# funktionerne ovenfor ER den dokumenterede reference — CASE-udtrykkene her skal
# give præcis samme resultat pr. række (paritetstest:
# tests/test_admin_nysalg_sql_parity.py). Afvig ikke fra reglerne i den ene uden
# at rette den anden OG testen.
#
# SQL'en er bevidst dialekt-portabel (kun COALESCE/NULLIF/ABS/LTRIM/RTRIM/CASE
# og afledte kolonner via underforespørgsler — ingen CROSS APPLY), så paritets-
# testen kan køre PRÆCIS samme query-tekst mod SQLite uden database-server.

def _agg_sql(scope: str, placeholder: str = "%s") -> str:
    """GROUP BY-query for et runs effektive tal pr. brand (én parameter: run_id).

    scope='business_media': alt undtagen Monitor, grupperet på brand (NULL/tom
    brand vises som 'Øvrige' — i praksis sætter insert_matches altid brand).
    scope='monitor': kun Monitor, grupperet på site (spejler monitor_relabel).
    placeholder: '%s' (pymssql) eller '?' (sqlite3 i paritetstesten).
    """
    if scope == "monitor":
        label_expr = "COALESCE(NULLIF(LTRIM(RTRIM(site)), ''), 'Ukendt site')"
        scope_where = "brand = 'Monitor'"
    else:
        label_expr = "COALESCE(NULLIF(LTRIM(RTRIM(brand)), ''), 'Øvrige')"
        scope_where = "COALESCE(brand, '') <> 'Monitor'"
    # ADM_PARTIAL_TOLERANCE indlejres som literal, så testen fanger drift.
    tol = f"(1 - {ADM_PARTIAL_TOLERANCE})"
    return f"""
        SELECT label,
               SUM(eff_gross_in)  AS brutto,
               SUM(eff_adm_in)    AS adm_nysalg,
               SUM(eff_gross_out) AS opsigelser,
               SUM(eff_adm_out)   AS adm_opsigelser,
               SUM(CASE WHEN eff_is_admin = 1 OR eff_adm_in > 0 THEN 1 ELSE 0 END) AS n_admin,
               SUM(CASE WHEN ambiguous = 1 THEN 1 ELSE 0 END) AS n_ambiguous
        FROM (
            SELECT x.*,
                   CASE WHEN x.total_excluded = 1 THEN 0
                        WHEN x.adm_in_override IS NOT NULL THEN x.adm_in_override
                        WHEN x.eff_is_admin = 0 THEN 0
                        WHEN x.eff_gross_in <= 0 THEN 0
                        WHEN x.match_sign = 'pos' AND x.matched_value IS NOT NULL
                             AND ABS(x.matched_value) < x.eff_gross_in * {tol}
                            THEN ABS(x.matched_value)
                        ELSE x.eff_gross_in END AS eff_adm_in,
                   CASE WHEN x.total_excluded = 1 THEN 0
                        WHEN x.adm_out_override IS NOT NULL THEN x.adm_out_override
                        WHEN x.is_admin_ops = 0 THEN 0
                        WHEN x.eff_gross_out <= 0 THEN 0
                        WHEN x.match_sign = 'neg' AND x.matched_value IS NOT NULL
                             AND ABS(x.matched_value) < x.eff_gross_out * {tol}
                            THEN ABS(x.matched_value)
                        ELSE x.eff_gross_out END AS eff_adm_out
            FROM (
                SELECT {label_expr} AS label,
                       total_excluded, adm_in_override, adm_out_override,
                       match_sign, matched_value, ambiguous,
                       CASE WHEN total_excluded = 1 THEN 0
                            WHEN gross_in_override IS NOT NULL THEN gross_in_override
                            ELSE COALESCE(gross_in, 0) END AS eff_gross_in,
                       CASE WHEN total_excluded = 1 THEN 0
                            WHEN gross_out_override IS NOT NULL THEN gross_out_override
                            WHEN COALESCE(gross_out, 0) <> 0 THEN gross_out
                            WHEN COALESCE(net_diff, 0) < 0 THEN ABS(net_diff)
                            ELSE 0 END AS eff_gross_out,
                       CASE WHEN override = 'exclude' THEN 0
                            WHEN override = 'include' THEN 1
                            WHEN is_admin = 1 THEN 1 ELSE 0 END AS eff_is_admin,
                       CASE WHEN administrativ = 1 OR match_sign = 'neg' THEN 1
                            ELSE 0 END AS is_admin_ops
                FROM admin_nysalg_match
                WHERE run_id = {placeholder}
                  AND {scope_where}
                  AND {_CONTRIBUTING_WHERE}
            ) x
        ) y
        GROUP BY label
    """


def aggregate_by_brand_sql(run_id: int, scope: str = "business_media") -> dict:
    """{label: {brutto, adm_nysalg, opsigelser, adm_opsigelser, n_admin,
    n_ambiguous}} for et run — samme tal som summarize/summarize_by_brand over
    get_matches, men beregnet i databasen i ÉN query (uafhængig af rækkeantal).

    Værdierne er uafrundede floats — kaldere afrunder som de respektive
    Python-helpers (summarize afrunder totalen, summarize_by_brand pr. brand).
    """
    conn = get_conn()
    cur = conn.cursor(as_dict=True)
    cur.execute(_agg_sql(scope), (run_id,))
    rows = cur.fetchall() or []
    conn.close()
    return {r["label"]: {
        "brutto": float(r["brutto"] or 0),
        "adm_nysalg": float(r["adm_nysalg"] or 0),
        "opsigelser": float(r["opsigelser"] or 0),
        "adm_opsigelser": float(r["adm_opsigelser"] or 0),
        "n_admin": int(r["n_admin"] or 0),
        "n_ambiguous": int(r["n_ambiguous"] or 0),
    } for r in rows}


def summarize_from_groups(groups: dict, exclude_brands=frozenset()) -> dict:
    """Topkort-tal (samme form som summarize) fra aggregate_by_brand_sql-grupper.

    exclude_brands (fx skjulte brands) pilles fra inden summeringen — spejler
    _apply_hidden, hvor topkortene genberegnes uden de skjulte brands.
    Summerer de uafrundede gruppetal og afrunder til sidst, som summarize.
    """
    brutto = adm_nysalg = opsigelser = adm_opsigelser = 0.0
    n_admin = n_ambiguous = 0
    for label, g in groups.items():
        if label in exclude_brands:
            continue
        brutto += g["brutto"]
        adm_nysalg += g["adm_nysalg"]
        opsigelser += g["opsigelser"]
        adm_opsigelser += g["adm_opsigelser"]
        n_admin += g["n_admin"]
        n_ambiguous += g["n_ambiguous"]
    netto = (brutto - adm_nysalg) - (opsigelser - adm_opsigelser)
    return {
        "brutto": round(brutto, 2),
        "adm_nysalg": round(adm_nysalg, 2),
        "opsigelser": round(opsigelser, 2),
        "adm_opsigelser": round(adm_opsigelser, 2),
        "netto_tilvaekst": round(netto, 2),
        "n_admin": n_admin,
        "n_ambiguous": n_ambiguous,
    }


def summarize_sql(run_id: int, scope: str = "business_media",
                  exclude_brands=frozenset()) -> dict:
    """Topkort-tal for et run beregnet i SQL — SQL-udgaven af summarize."""
    return summarize_from_groups(aggregate_by_brand_sql(run_id, scope), exclude_brands)

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
               period_to: Optional[str] = None) -> int:
    """period = læsbar label; period_from/period_to = ISO YYYY-MM-DD-interval (filter)."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO admin_nysalg_run
               (created_by, source_path, source_filename, period, period_from, period_to, status)
           OUTPUT INSERTED.run_id VALUES (%s, %s, %s, %s, %s, %s, 'matched')""",
        (created_by, source_path, source_filename, period, period_from, period_to),
    )
    run_id = int(cur.fetchone()[0])
    conn.commit()
    conn.close()
    return run_id


def insert_matches(run_id: int, rows: list[ExtractRow], progress_cb=None) -> None:
    """progress_cb(i, total) kaldes undervejs (hver 25. række + til sidst) så en
    baggrundsjob-kører kan rapportere fremdrift."""
    conn = get_conn()
    cur = conn.cursor()
    total = len(rows)
    for i, r in enumerate(rows, start=1):
        m = r.match
        cur.execute(
            """INSERT INTO admin_nysalg_match
               (run_id, row_index, month_end, account_number, pipedrive_id, site, brands,
                brand, movement, net_diff, gross_in, gross_out, administrativ, currency,
                matched_deal_id, matched_value, matched_pipeline, matched_org_name,
                match_sign, is_admin, ambiguous)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                run_id, r.row_index, r.month_end, r.account_number, r.pipedrive_id, r.site,
                r.brands, r.brand, r.movement, r.net_diff, r.gross_in, r.gross_out,
                1 if r.administrativ else 0, r.currency,
                (m.deal_id if m else None),
                (m.value if m else None),
                (m.pipeline if m else None),
                (m.org_name if m else None),
                r.match_sign,
                1 if r.is_admin_nysalg() else 0,
                1 if r.ambiguous else 0,
            ),
        )
        if progress_cb and (i % 25 == 0 or i == total):
            try:
                progress_cb(i, total)
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


def get_matches(run_id: int) -> list[dict]:
    conn = get_conn()
    cur = conn.cursor(as_dict=True)
    cur.execute(
        "SELECT * FROM admin_nysalg_match WHERE run_id = %s ORDER BY row_index", (run_id,)
    )
    rows = cur.fetchall() or []
    conn.close()
    for r in rows:
        for k in ("net_diff", "gross_in", "gross_out", "matched_value"):
            r[k] = float(r[k]) if r.get(k) is not None else None
        r["is_admin"] = bool(r["is_admin"])
        r["ambiguous"] = bool(r["ambiguous"])
        r["administrativ"] = bool(r.get("administrativ"))
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


# ── Reklame-rækker (PipeDrive-deals, ikke abonnement) ────────────────────────

def _ad_budget(cur, label: str, date_from: str | None, date_to: str | None) -> float:
    """Budget for en reklame-række fra BudgetsIntoMedia (0 ved manglende mapping)."""
    from moduler.modul_admin_nysalg.brands import AD_BUDGET_WHERE
    frag = AD_BUDGET_WHERE.get(label)
    if not frag:
        return 0.0
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


def pipedrive_brand_rows(date_from: str | None, date_to: str | None,
                         comments: dict | None = None,
                         budgets: dict | None = None) -> list[dict]:
    """Brand-rækker hentet direkte fra PipedriveDeals (findes ikke i Zuora).

    Dækker Job/Banner/Norge advertising og MarketWire (jf. PIPEDRIVE_ROWS).
    Bruttonysalg = Σ won-deals uden for cancellation-pipelines; opsigelser =
    Σ ABS af cancellation-pipelines; netto = brutto − opsigelser. Job/Banner/Norge
    har ingen cancellation-pipelines → opsigelser=0; MarketWire har rigtige
    opsigelser. Værdi i lokal valuta for NOK/SEK, ellers DKK. Filtreres på
    service_activation_date i intervallet (ISO YYYY-MM-DD, inkl.) og status='won'.
    Budget: Job/Banner/Norge fra AD_BUDGET_WHERE, øvrige (MarketWire) fra det
    normale brand-budget. Returnerer [] ved DB-fejl.
    """
    from constants import CANCELLATION_PIPELINES
    from moduler.modul_admin_nysalg.brands import (AD_BUDGET_WHERE, PIPEDRIVE_ROWS,
                                                   brand_currency)
    comments = comments or {}
    budgets = budgets or {}
    # Lokal valuta for NOK/SEK (jf. perf-modulet), ellers DKK.
    val = "CASE WHEN [currency] IN ('NOK','SEK') THEN [value] ELSE [value_dkk] END"
    cancel_ph = "(" + ",".join(["%s"] * len(CANCELLATION_PIPELINES)) + ")"
    cancel_up = [p.upper() for p in CANCELLATION_PIPELINES]
    rows: list[dict] = []
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        for spec in PIPEDRIVE_ROWS:
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
            if label in AD_BUDGET_WHERE:
                budget = round(_ad_budget(cur, label, date_from, date_to), 2)
            else:
                budget = round(float(budgets.get(label, 0.0) or 0.0), 2)

            # Underrækker (drill-down): samme scope + ét site pr. underrække.
            # Delmængde af brutto/opsigelser — totalen i hovedrækken er uændret.
            subrows: list[dict] = []
            for sr in spec.get("subrows", []):
                sr_where = where + ["LOWER(LTRIM(RTRIM(COALESCE([sites],'')))) = %s"]
                sr_params = where_params + [sr["site"].lower()]
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
                subrows.append({
                    "brand": sr["label"], "brutto": s_brutto, "adm_nysalg": 0.0,
                    "opsigelser": s_ops, "adm_opsigelser": 0.0,
                    "netto": round(s_brutto - s_ops, 2), "budget": None,
                    "comment": "", "n_ambiguous": 0, "currency": brand_currency(label),
                })

            rows.append({
                "brand": label, "brutto": brutto, "adm_nysalg": 0.0,
                "opsigelser": ops, "adm_opsigelser": 0.0,
                "netto": round(brutto - ops, 2), "budget": budget,
                "comment": comments.get(label, "") or "", "n_ambiguous": 0,
                "currency": brand_currency(label), "subrows": subrows,
            })
        conn.close()
    except Exception:
        logger.exception("pipedrive_brand_rows fejlede")
        return []
    return rows


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


def summarize_by_brand(matches: list[dict], budgets: dict | None = None,
                       comments: dict | None = None,
                       extra_rows: list[dict] | None = None) -> list[dict]:
    """Per-brand totaltal med de administrative bevægelser udskilt:

      brutto          = al bruttoomsætning/nysalg (Σ gross_in)
      adm_nysalg      = heraf administrativt nysalg (trækkes fra) — effective_is_admin
      opsigelser      = alle opsigelser (Σ gross_out)
      adm_opsigelser  = heraf administrative opsigelser (trækkes fra) — administrativ-flag
      netto (tilvækst)= (brutto − adm_nysalg) − (opsigelser − adm_opsigelser)
      budget          = samlet mediebudget for brandet
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
        g["brutto"] += (m.get("gross_in") or 0)
        g["opsigelser"] += _row_opsigelse(m)
        if effective_is_admin(m):
            g["adm_nysalg"] += (m.get("gross_in") or 0)
        if is_admin_opsigelse(m):
            g["adm_opsigelser"] += _row_opsigelse(m)
        if m.get("ambiguous"):
            g["n_ambiguous"] += 1

    # Brands der altid skal vises + brands med budget (også uden bevægelser).
    for label in list(ALWAYS_SHOWN) + list(BUDGET_BRANDS.keys()):
        if budgets.get(label) or label in ALWAYS_SHOWN:
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
                        comments: dict | None = None) -> dict[str, list[dict]]:
    """{'YYYY-MM': [brand_rows]} pr. måned i intervallet.

    Mangler datointervallet udledes månederne af bevægelsernes month_end. For hver
    måned genberegnes brand-rækkerne (Zuora-matches + PipeDrive-only-rækker + budget)
    med de eksisterende helpere, så summen pr. måned matcher den samlede visning.
    """
    comments = comments or {}
    months = months_in_range(date_from, date_to)
    if not months:
        months = sorted({(m.get("month_end") or "")[:7]
                         for m in matches if m.get("month_end")})
    out: dict[str, list[dict]] = {}
    for ym in months:
        if not ym:
            continue
        m_from, m_to = _month_bounds(ym)
        matches_m = [m for m in matches if (m.get("month_end") or "")[:7] == ym]
        budgets_m = brand_budgets(m_from, m_to)
        pd_rows = pipedrive_brand_rows(m_from, m_to, comments, budgets_m)
        out[ym] = summarize_by_brand(matches_m, budgets_m, comments, extra_rows=pd_rows)
    return out


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
        gi = m.get("gross_in") or 0
        go = _row_opsigelse(m)
        brutto += gi
        opsigelser += go
        if effective_is_admin(m):
            adm_nysalg += gi
            n_admin += 1
        if is_admin_opsigelse(m):
            adm_opsigelser += go
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

"""SQL Server-persistens for admin-nysalg-runs og deres matchede rækker.

Et "run" gemmer ALLE behandlede nysalgsrækker (også ikke-matchede), så rapporten
kan vise brutto/administrativt/netto og et run er fuldt reproducerbart, samt kan
genoptages i en senere session.
"""
from __future__ import annotations

import logging
import os
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
               period: Optional[str]) -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO admin_nysalg_run (created_by, source_path, source_filename, period, status)
           OUTPUT INSERTED.run_id VALUES (%s, %s, %s, %s, 'matched')""",
        (created_by, source_path, source_filename, period),
    )
    run_id = int(cur.fetchone()[0])
    conn.commit()
    conn.close()
    return run_id


def insert_matches(run_id: int, rows: list[ExtractRow]) -> None:
    conn = get_conn()
    cur = conn.cursor()
    for r in rows:
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


# ── Brand-budgetter (samlet mediebudget pr. brand) ───────────────────────────

def brand_budgets(period: str | None, subscription_only: bool = True) -> dict:
    """{brand-label: abonnements-mediebudget} fra BudgetsIntoMedia for perioden.

    Rapporten er en abonnementsvisning, så budgettet afgrænses til DealType
    'Subscription' — ellers tæller fx Banner-/Job-budget med i abonnements-
    brandenes budget. period = 'YYYY-MM' afgrænser på BudgetDate; None summerer
    alle datoer. subscription_only=False = hele budgettet (alle DealTypes).
    """
    from moduler.modul_admin_nysalg.brands import BUDGET_BRANDS
    where = []
    params: list = []
    if subscription_only:
        where.append("LOWER(LTRIM(RTRIM(COALESCE([DealType],'')))) = 'subscription'")
    if period:
        try:
            y, m = period.split("-")
            where.append("YEAR([BudgetDate]) = %s AND MONTH([BudgetDate]) = %s")
            params.extend([int(y), int(m)])
        except (ValueError, AttributeError):
            pass
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

def _ad_budget(cur, label: str, period: str | None) -> float:
    """Budget for en reklame-række fra BudgetsIntoMedia (0 ved manglende mapping)."""
    from moduler.modul_admin_nysalg.brands import AD_BUDGET_WHERE
    frag = AD_BUDGET_WHERE.get(label)
    if not frag:
        return 0.0
    where = [frag]
    params: list = []
    if period:
        try:
            y, m = period.split("-")
            where.append("YEAR([BudgetDate]) = %s AND MONTH([BudgetDate]) = %s")
            params.extend([int(y), int(m)])
        except (ValueError, AttributeError):
            pass
    cur.execute(
        "SELECT ISNULL(SUM([BudgetAmount]),0) AS budget FROM [dbo].[BudgetsIntoMedia] "
        "WHERE " + " AND ".join(where), tuple(params))
    row = cur.fetchone()
    return float((row["budget"] if row else 0) or 0)


def pipedrive_brand_rows(period: str | None, comments: dict | None = None,
                         budgets: dict | None = None) -> list[dict]:
    """Brand-rækker hentet direkte fra PipedriveDeals (findes ikke i Zuora).

    Dækker Job/Banner/Norge advertising og MarketWire (jf. PIPEDRIVE_ROWS).
    Bruttonysalg = Σ won-deals uden for cancellation-pipelines; opsigelser =
    Σ ABS af cancellation-pipelines; netto = brutto − opsigelser. Job/Banner/Norge
    har ingen cancellation-pipelines → opsigelser=0; MarketWire har rigtige
    opsigelser. Værdi i lokal valuta for NOK/SEK, ellers DKK. Filtreres på
    service_activation_date i perioden og status='won'. Budget: Job/Banner/Norge
    fra AD_BUDGET_WHERE, øvrige (MarketWire) fra det normale brand-budget.
    Returnerer [] ved DB-fejl.
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
            if period:
                try:
                    y, m = period.split("-")
                    where.append("YEAR([service_activation_date]) = %s "
                                 "AND MONTH([service_activation_date]) = %s")
                    where_params += [int(y), int(m)]
                except (ValueError, AttributeError):
                    pass
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
                budget = round(_ad_budget(cur, label, period), 2)
            else:
                budget = round(float(budgets.get(label, 0.0) or 0.0), 2)
            rows.append({
                "brand": label, "brutto": brutto, "adm_nysalg": 0.0,
                "opsigelser": ops, "adm_opsigelser": 0.0,
                "netto": round(brutto - ops, 2), "budget": budget,
                "comment": comments.get(label, "") or "", "n_ambiguous": 0,
                "currency": brand_currency(label),
            })
        conn.close()
    except Exception:
        logger.exception("pipedrive_brand_rows fejlede")
        return []
    return rows


def period_pipedrive_deals(period: str | None) -> list[dict]:
    """Alle won PipeDrive-deals med service_activation_date i perioden.

    Bruges til afstemningsfanen i Excel-rapporten, så PipeDrive-kilden kan holdes
    op imod Zuora-bevægelserne. Brandet udledes med samme classify() som
    bevægelserne, så de to faner kan sammenlignes pr. brand. period = 'YYYY-MM';
    None = alle datoer. Returnerer [] ved DB-fejl.
    """
    from moduler.modul_admin_nysalg.brands import classify
    where = ["[status] = 'won'", "[service_activation_date] IS NOT NULL"]
    params: list = []
    if period:
        try:
            y, m = period.split("-")
            where.append("YEAR([service_activation_date]) = %s "
                         "AND MONTH([service_activation_date]) = %s")
            params.extend([int(y), int(m)])
        except (ValueError, AttributeError):
            pass
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
        if m.get("administrativ"):
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
        if m.get("administrativ"):
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

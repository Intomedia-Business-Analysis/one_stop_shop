"""Klippekort Overblik — DB-lag.

To datakilder:
  - PipedriveDeals (synces FRA Pipedrive): hvilke kunder har et aktivt
    job-klippekort, hvor mange klip er købt (clip_card_size) og hvad er
    annonceringsperioden.
  - KlippekortForbrug (lokal app-tabel, oprettes her): hvert klip-forbrug
    sælgeren registrerer i toolet (stilling, site, tidspunkt). Denne tabel er
    kilden til "klip brugt" fremadrettet og driver klippekort-økonomien.

'Klip brugt' pushes desuden til Pipedrive-feltet used_clip_cards via
pipedrive_api.update_used_clip_cards, så det kommer ned i PipedriveDeals ved
næste sync.
"""
import os
import traceback
import uuid

import pymssql
from dotenv import load_dotenv

load_dotenv()


def get_site_groups() -> dict:
    """Site-familier til 'opret job'-dropdownen — trukket dynamisk fra de sites
    der faktisk bruges på job-deals (jppol_advertising), så listen vedligeholder
    sig selv. Monitor-sites genkendes på 'monitor' i navnet; resten er Watch-familien.
    """
    watch, monitor = [], []
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute("""
            SELECT DISTINCT LTRIM(RTRIM(s.value)) AS site
            FROM [dbo].[PipedriveDeals] d
            CROSS APPLY STRING_SPLIT(d.sites, ',') AS s
            WHERE d.account = 'jppol_advertising'
              AND d.pipeline_name = 'job'
              AND LTRIM(RTRIM(s.value)) <> ''
        """)
        for r in cur.fetchall():
            site = r["site"]
            (monitor if "monitor" in site.lower() else watch).append(site)
        conn.close()
    except Exception:
        traceback.print_exc()
    watch.sort(key=str.lower)
    monitor.sort(key=str.lower)
    return {"watch_dk": watch, "monitor": monitor}


def get_conn():
    return pymssql.connect(
        server=os.getenv("DB_SERVER"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME", "INTOMEDIA"),
        tds_version="7.0",
        login_timeout=5,
        timeout=10,
    )


def init_klippekort_db():
    """Opret/migrér KlippekortForbrug (kaldes ved router-import).

    Hvert job kan dække flere sites og koste et antal klip. Vi gemmer én række
    pr. (job, site) hvor 'klip' er den andel klip der falder på det site
    (= total_klip / antal_sites). job_id grupperer et jobs site-rækker.
    """
    stmts = [
        """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='KlippekortForbrug' AND xtype='U')
        CREATE TABLE KlippekortForbrug (
            id             INT IDENTITY(1,1) PRIMARY KEY,
            job_id         NVARCHAR(40)   NULL,
            pd_deal_id     INT            NOT NULL,
            org_id         INT            NULL,
            org_name       NVARCHAR(255)  NULL,
            site           NVARCHAR(100)  NOT NULL,
            stilling       NVARCHAR(255)  NOT NULL,
            tidspunkt      DATE           NOT NULL,
            klip           DECIMAL(9,4)   NOT NULL DEFAULT 1,
            pris_pr_klip   DECIMAL(18,2)  NOT NULL DEFAULT 0,
            clip_card_size INT            NULL,
            created_by     NVARCHAR(100)  NOT NULL,
            created_at     DATETIME       DEFAULT GETDATE()
        )""",
        # Migration: tilføj nye kolonner til eksisterende tabel
        """IF COL_LENGTH('KlippekortForbrug','job_id') IS NULL
           ALTER TABLE KlippekortForbrug ADD job_id NVARCHAR(40) NULL""",
        """IF COL_LENGTH('KlippekortForbrug','klip') IS NULL
           ALTER TABLE KlippekortForbrug ADD klip DECIMAL(9,4) NOT NULL DEFAULT 1""",
        """IF COL_LENGTH('KlippekortForbrug','slutdato') IS NULL
           ALTER TABLE KlippekortForbrug ADD slutdato DATE NULL""",
        """IF COL_LENGTH('KlippekortForbrug','effektgaranti') IS NULL
           ALTER TABLE KlippekortForbrug ADD effektgaranti BIT NOT NULL DEFAULT 0""",
        # Lokal cache af organisationens ejer (hentes fra Pipedrive i baggrunden,
        # da org-owner ikke synces til PipedriveDeals).
        """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='KlippekortOrgOwner' AND xtype='U')
        CREATE TABLE KlippekortOrgOwner (
            org_id      INT            NOT NULL PRIMARY KEY,
            owner_name  NVARCHAR(150)  NULL,
            owner_email NVARCHAR(150)  NULL,
            updated_at  DATETIME       DEFAULT GETDATE()
        )""",
    ]
    try:
        conn = get_conn()
        cur = conn.cursor()
        for s in stmts:
            cur.execute(s)
        conn.commit()
        conn.close()
    except Exception:
        traceback.print_exc()


def db_overblik(only_owner_name: str | None = None, status: str = "aktive") -> list[dict]:
    """Job-klippekort med købt/brugt/rest og dage til udløb.

    'brugt' tælles fra KlippekortForbrug (toolets egen registrering) — ikke fra
    Pipedrives used_clip_cards, som kun vises som reference (brugt_pipedrive).
    only_owner_name filtrerer til den indloggede sælger ("kun mine") — på
    ORGANISATIONENS ejer (oo.owner_name), ikke deal-owneren.
    status: 'aktive' (i annonceringsperioden netop nu) eller 'udloebne'
            (periode-slut ligger før i dag).
    """
    owner_clause = ""
    params: list = []
    if only_owner_name:
        owner_clause = "AND oo.owner_name = %s"
        params.append(only_owner_name)
    if status == "udloebne":
        period_clause = "AND d.advertising_periode_end < CAST(GETDATE() AS date)"
        order_clause = "ORDER BY dage_til_udloeb DESC"  # senest udløbne først
    else:
        period_clause = ("AND CAST(GETDATE() AS date)"
                         " BETWEEN d.advertising_periode_start AND d.advertising_periode_end")
        order_clause = "ORDER BY dage_til_udloeb ASC"   # snart-udløbne først
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(f"""
            SELECT
                d.pd_deal_id,
                d.org_id,
                d.org_name,
                d.title,
                d.owner_name,
                oo.owner_name AS org_owner,
                d.sites,
                TRY_CAST(d.clip_card_size AS INT)  AS clip_card_size,
                ISNULL(TRY_CAST(d.transfered_clip_cards AS INT), 0) AS clips_previous,
                TRY_CAST(d.used_clip_cards AS INT) AS brugt_pipedrive,
                CAST(d.value_dkk AS BIGINT)        AS value_dkk,
                CONVERT(NVARCHAR(10), d.advertising_periode_start, 23) AS periode_start,
                CONVERT(NVARCHAR(10), d.advertising_periode_end, 23)  AS periode_slut,
                DATEDIFF(day, CAST(GETDATE() AS date), d.advertising_periode_end) AS dage_til_udloeb,
                (SELECT ISNULL(SUM(f.klip), 0) FROM KlippekortForbrug f WHERE f.pd_deal_id = d.pd_deal_id) AS brugt
            FROM [dbo].[PipedriveDeals] d
            LEFT JOIN KlippekortOrgOwner oo ON oo.org_id = d.org_id
            WHERE d.pipeline_name = 'job'
              AND d.account = 'jppol_advertising'
              AND d.status = 'won'
              AND TRY_CAST(d.clip_card_size AS INT) > 0
              {period_clause}
              {owner_clause}
            {order_clause}
        """, tuple(params))
        rows = []
        for r in cur.fetchall():
            size = int(r["clip_card_size"] or 0)
            tidligere = int(r["clips_previous"] or 0)
            kob = size + tidligere   # samlet antal klip = kortets størrelse + klip fra tidligere aftale
            brugt = round(float(r["brugt"] or 0))
            rows.append({
                "pd_deal_id":      r["pd_deal_id"],
                "org_id":          r["org_id"],
                "org_name":        r["org_name"] or "—",
                "title":           r["title"] or "(Uden titel)",
                "owner_name":      r["owner_name"] or "—",
                "org_owner":       r["org_owner"] or "—",
                "sites":           r["sites"] or "",
                "clip_card_size":  kob,
                "clip_card_base":  size,
                "klip_fra_tidligere": tidligere,
                "brugt":           brugt,
                "rest":            kob - brugt,
                "brugt_pipedrive": int(r["brugt_pipedrive"] or 0),
                "value_dkk":       int(r["value_dkk"] or 0),
                "periode_start":   r["periode_start"] or "—",
                "periode_slut":    r["periode_slut"] or "—",
                "dage_til_udloeb": int(r["dage_til_udloeb"] if r["dage_til_udloeb"] is not None else 0),
            })
        conn.close()
        return rows
    except Exception:
        traceback.print_exc()
        return []


def _deal_info(cur, pd_deal_id: int) -> dict | None:
    """Hent clip_card_size, value_dkk, org og sites for én deal."""
    cur.execute("""
        SELECT TOP 1
            org_id,
            org_name,
            TRY_CAST(clip_card_size AS INT) AS clip_card_size,
            CAST(value_dkk AS DECIMAL(18,2)) AS value_dkk
        FROM [dbo].[PipedriveDeals]
        WHERE pd_deal_id = %s
          AND account = 'jppol_advertising'
          AND pipeline_name = 'job'
    """, (pd_deal_id,))
    return cur.fetchone()


def _brugt_for_deal(cur, pd_deal_id: int) -> int:
    cur.execute("SELECT ISNULL(SUM(klip), 0) AS n FROM KlippekortForbrug WHERE pd_deal_id = %s",
                (pd_deal_id,))
    return round(float((cur.fetchone() or {}).get("n", 0) or 0))


def db_registrer_forbrug(pd_deal_id: int, sites: list[str], stilling: str,
                         tidspunkt: str, klip: int, created_by: str,
                         slutdato: str | None = None,
                         effektgaranti: bool = False) -> dict:
    """Log ét job (kan dække flere sites og koste flere klip).

    Jobbet koster `klip` klip i alt, fordelt ligeligt over `sites` (én DB-række
    pr. site med klip-andel = klip / antal_sites). Økonomi pr. site bliver dermed
    (klip × pris_pr_klip) / antal_sites. pris_pr_klip = value_dkk / clip_card_size.
    slutdato = hvornår stillingen udløber (til opfølgnings-oversigten).
    Returnerer {ok, brugt, rest, clip_card_size, pris_pr_klip} eller {ok: False, error}.
    """
    sites = [s.strip() for s in (sites or []) if s and s.strip()]
    if not sites:
        return {"ok": False, "error": "Mindst ét site skal vælges"}
    try:
        klip = int(klip)
    except (TypeError, ValueError):
        klip = 1
    if klip < 1:
        return {"ok": False, "error": "Antal klip skal være mindst 1"}
    slutdato = (slutdato or "").strip() or None

    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        deal = _deal_info(cur, pd_deal_id)
        if not deal:
            conn.close()
            return {"ok": False, "error": f"Deal {pd_deal_id} blev ikke fundet (job/jppol_advertising)"}
        size = int(deal["clip_card_size"] or 0)
        value = float(deal["value_dkk"] or 0)
        pris_pr_klip = round(value / size, 2) if size > 0 else 0.0

        job_id = uuid.uuid4().hex
        klip_per_site = round(klip / len(sites), 4)
        eff = 1 if effektgaranti else 0
        for site in sites:
            cur.execute("""
                INSERT INTO KlippekortForbrug
                    (job_id, pd_deal_id, org_id, org_name, site, stilling, tidspunkt,
                     slutdato, effektgaranti, klip, pris_pr_klip, clip_card_size, created_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                job_id, pd_deal_id, deal["org_id"], deal["org_name"], site, stilling,
                tidspunkt, slutdato, eff, klip_per_site, pris_pr_klip, size, created_by,
            ))

        brugt = _brugt_for_deal(cur, pd_deal_id)
        conn.commit()
        conn.close()
        return {
            "ok": True,
            "brugt": brugt,
            "rest": size - brugt,
            "clip_card_size": size,
            "pris_pr_klip": pris_pr_klip,
        }
    except Exception as e:
        traceback.print_exc()
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        return {"ok": False, "error": str(e)}


def db_forbrug_for_deal(pd_deal_id: int) -> list[dict]:
    """Registrerede jobs for én deal — grupperet pr. job (nyeste først).

    Hvert job kan dække flere sites; vi samler site-rækkerne til ét job med
    sites-liste, samlet antal klip og samlet beløb.
    """
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute("""
            SELECT
                id, job_id, site, stilling,
                CONVERT(NVARCHAR(10), tidspunkt, 23)   AS tidspunkt,
                CONVERT(NVARCHAR(10), slutdato, 23)    AS slutdato,
                effektgaranti,
                CAST(klip AS DECIMAL(9,4))             AS klip,
                CAST(pris_pr_klip AS DECIMAL(18,2))    AS pris_pr_klip,
                created_by,
                CONVERT(NVARCHAR(16), created_at, 120) AS created_at
            FROM KlippekortForbrug
            WHERE pd_deal_id = %s
            ORDER BY created_at DESC, id DESC
        """, (pd_deal_id,))
        groups: dict = {}
        order: list = []
        for r in cur.fetchall():
            key = r["job_id"] or f"row-{r['id']}"
            g = groups.get(key)
            if not g:
                g = {
                    "job_id":     key,
                    "stilling":   r["stilling"] or "—",
                    "tidspunkt":  r["tidspunkt"] or "—",
                    "slutdato":   r["slutdato"] or "",
                    "effektgaranti": bool(r["effektgaranti"]),
                    "created_by": r["created_by"] or "—",
                    "created_at": r["created_at"] or "—",
                    "sites":      [],
                    "klip":       0.0,
                    "belob":      0.0,
                }
                groups[key] = g
                order.append(key)
            klip = float(r["klip"] or 0)
            pris = float(r["pris_pr_klip"] or 0)
            if r["site"]:
                g["sites"].append(r["site"])
            g["klip"] += klip
            g["belob"] += klip * pris
        out = []
        for key in order:
            g = groups[key]
            g["klip"] = round(g["klip"], 2)
            g["belob"] = round(g["belob"], 2)
            out.append(g)
        conn.close()
        return out
    except Exception:
        traceback.print_exc()
        return []


def db_slet_job(job_id: str) -> dict:
    """Slet et helt job (alle dets site-rækker) og returnér deal'ens nye brugt-total."""
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute("SELECT TOP 1 pd_deal_id FROM KlippekortForbrug WHERE job_id = %s", (job_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            return {"ok": False, "error": f"Job {job_id} blev ikke fundet"}
        pd_deal_id = int(row["pd_deal_id"])

        cur.execute("DELETE FROM KlippekortForbrug WHERE job_id = %s", (job_id,))
        brugt = _brugt_for_deal(cur, pd_deal_id)
        conn.commit()
        conn.close()
        return {"ok": True, "pd_deal_id": pd_deal_id, "brugt": brugt}
    except Exception as e:
        traceback.print_exc()
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        return {"ok": False, "error": str(e)}


def db_oekonomi() -> list[dict]:
    """Omsætning fordelt på de sites hvor klip er blevet brugt.

    Et job på flere sites fordeler sit beløb ligeligt: hvert site bidrager med
    (job-klip / antal-sites) × pris_pr_klip — gemt som klip-andel pr. site-række.
    """
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute("""
            SELECT
                site,
                SUM(klip)                AS antal_klip,
                SUM(klip * pris_pr_klip) AS omsaetning
            FROM KlippekortForbrug
            GROUP BY site
            ORDER BY omsaetning DESC
        """)
        rows = [
            {
                "site":       r["site"] or "—",
                "antal_klip": round(float(r["antal_klip"] or 0), 2),
                "omsaetning": float(r["omsaetning"] or 0),
            }
            for r in cur.fetchall()
        ]
        conn.close()
        return rows
    except Exception:
        traceback.print_exc()
        return []


def db_udloebende_jobs(only_owner_name: str | None = None, status: str = "aktive") -> list[dict]:
    """Stillinger med en slutdato — til opfølgning.

    status='aktive'   → stillinger der stadig kører (slutdato >= i dag, eller
                        effektgaranti) — snart-udløbne øverst.
    status='udloebne' → stillinger hvis slutdato er passeret (og ikke effektgaranti)
                        — senest udløbne øverst.
    only_owner_name filtrerer på organisationens ejer.
    """
    if status == "udloebne":
        status_clause = "AND f.slutdato < CAST(GETDATE() AS date) AND f.effektgaranti = 0"
        order_clause = "ORDER BY f.slutdato DESC, f.job_id"
    else:
        status_clause = "AND (f.slutdato >= CAST(GETDATE() AS date) OR f.effektgaranti = 1)"
        order_clause = "ORDER BY f.slutdato ASC, f.job_id"
    owner_clause = ""
    params: list = []
    if only_owner_name:
        owner_clause = "AND oo.owner_name = %s"
        params.append(only_owner_name)
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(f"""
            SELECT
                f.job_id, f.pd_deal_id, f.org_id, f.org_name, f.stilling, f.site,
                CONVERT(NVARCHAR(10), f.slutdato, 23) AS slutdato,
                DATEDIFF(day, CAST(GETDATE() AS date), f.slutdato) AS dage_til_slut,
                f.effektgaranti,
                d.owner_name AS deal_owner,
                oo.owner_name AS org_owner
            FROM KlippekortForbrug f
            LEFT JOIN [dbo].[PipedriveDeals] d
                   ON d.pd_deal_id = f.pd_deal_id
                  AND d.account = 'jppol_advertising'
                  AND d.pipeline_name = 'job'
                  AND d.status = 'won'
            LEFT JOIN KlippekortOrgOwner oo ON oo.org_id = f.org_id
            WHERE f.slutdato IS NOT NULL
              {status_clause}
              {owner_clause}
            {order_clause}
        """, tuple(params))
        groups: dict = {}
        order: list = []
        for r in cur.fetchall():
            key = r["job_id"] or f"deal-{r['pd_deal_id']}-{r['slutdato']}"
            g = groups.get(key)
            if not g:
                g = {
                    "job_id":        key,
                    "pd_deal_id":    r["pd_deal_id"],
                    "org_name":      r["org_name"] or "—",
                    "stilling":      r["stilling"] or "—",
                    "slutdato":      r["slutdato"] or "—",
                    "dage_til_slut": int(r["dage_til_slut"] if r["dage_til_slut"] is not None else 0),
                    "effektgaranti": bool(r["effektgaranti"]),
                    "deal_owner":    r["deal_owner"] or "—",
                    "org_owner":     r["org_owner"] or "—",
                    "sites":         [],
                }
                groups[key] = g
                order.append(key)
            if r["site"]:
                g["sites"].append(r["site"])
        conn.close()
        return [groups[k] for k in order]
    except Exception:
        traceback.print_exc()
        return []


# ---------------------------------------------------------------------------
# Org-owner cache (organisationens ejer hentes fra Pipedrive i baggrunden)
# ---------------------------------------------------------------------------

def needed_org_ids() -> list[int]:
    """Distinkte org_id der optræder på job-klippekort-deals."""
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute("""
            SELECT DISTINCT org_id
            FROM [dbo].[PipedriveDeals]
            WHERE pipeline_name='job' AND account='jppol_advertising' AND status='won'
              AND TRY_CAST(clip_card_size AS INT) > 0 AND org_id IS NOT NULL
        """)
        ids = [int(r["org_id"]) for r in cur.fetchall()]
        conn.close()
        return ids
    except Exception:
        traceback.print_exc()
        return []


def db_org_owner_meta() -> dict:
    """Antal cachede ejere + alder på ældste række (til staleness-tjek)."""
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute("""
            SELECT COUNT(*) AS n,
                   DATEDIFF(hour, MIN(updated_at), GETDATE()) AS alder_timer
            FROM KlippekortOrgOwner
        """)
        r = cur.fetchone() or {}
        conn.close()
        return {"count": int(r.get("n", 0) or 0),
                "alder_timer": int(r.get("alder_timer") or 0)}
    except Exception:
        traceback.print_exc()
        return {"count": 0, "alder_timer": 0}


def db_upsert_org_owners(items: list[tuple]) -> int:
    """items = [(org_id, owner_name, owner_email), ...] — upsert i cache-tabellen."""
    if not items:
        return 0
    try:
        conn = get_conn()
        cur = conn.cursor()
        for org_id, name, email in items:
            cur.execute("""
                MERGE KlippekortOrgOwner AS t
                USING (SELECT %s AS org_id) AS s ON t.org_id = s.org_id
                WHEN MATCHED THEN UPDATE SET owner_name=%s, owner_email=%s, updated_at=GETDATE()
                WHEN NOT MATCHED THEN INSERT (org_id, owner_name, owner_email)
                                     VALUES (%s, %s, %s);
            """, (org_id, name, email, org_id, name, email))
        conn.commit()
        conn.close()
        return len(items)
    except Exception:
        traceback.print_exc()
        return 0


def refresh_org_owners() -> int:
    """Hent organisationernes ejere fra Pipedrive og opdatér cache-tabellen.

    Kører typisk i en baggrunds-tråd (kan tage et minut). Returnerer antal opdaterede.
    """
    from moduler.modul_klippekort import pipedrive_api as pda
    ids = needed_org_ids()
    if not ids:
        return 0
    owners = pda.fetch_org_owners(ids)
    if not owners:
        return 0
    return db_upsert_org_owners([(oid, nm, em) for oid, (nm, em) in owners.items()])

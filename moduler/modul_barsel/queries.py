import json
import logging

from auth import get_conn

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DB-initialisering
# ---------------------------------------------------------------------------

INIT_STMTS = [
    # Virksomhedens globale barselsindstillinger
    """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='BarselSettings' AND xtype='U')
       CREATE TABLE BarselSettings (
           id              INT IDENTITY(1,1) PRIMARY KEY,
           grav_uger       INT NOT NULL DEFAULT 4,
           mor_uger        INT NOT NULL DEFAULT 26,
           faed_uger       INT NOT NULL DEFAULT 2,
           forl_uger       INT NOT NULL DEFAULT 17,
           notify_emails   NVARCHAR(MAX) NULL,
           updated_by      INT NOT NULL DEFAULT 0,
           updated_at      DATETIME DEFAULT GETDATE()
       )""",
    # Migration: distributionsliste til notifikationer (kan tilføjes efter
    # godkendt barselsplan). Mailafsendelsen selv implementeres senere.
    """IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id=OBJECT_ID('BarselSettings') AND name='notify_emails')
       ALTER TABLE BarselSettings ADD notify_emails NVARCHAR(MAX) NULL""",
    # Barselsager pr. medarbejder
    """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='BarselCases' AND xtype='U')
       CREATE TABLE BarselCases (
           id                    INT IDENTITY(1,1) PRIMARY KEY,
           hub_user_id           INT           NULL,
           mor_navn              NVARCHAR(100) NULL,
           far_navn              NVARCHAR(100) NULL,
           mor_ansat             BIT           NOT NULL DEFAULT 0,
           far_ansat             BIT           NOT NULL DEFAULT 0,
           termin                NVARCHAR(10)  NULL,
           foedsel_dato          NVARCHAR(10)  NULL,
           mor_uger              INT           NULL,
           faed_uger             INT           NULL,
           forl_uger             INT           NULL,
           mor_ferie_optjent     INT           NOT NULL DEFAULT 0,
           mor_ferie_selvbetalt  INT           NOT NULL DEFAULT 0,
           far_ferie_optjent     INT           NOT NULL DEFAULT 0,
           far_ferie_selvbetalt  INT           NOT NULL DEFAULT 0,
           faed_start            NVARCHAR(10)  NULL,
           forl_start            NVARCHAR(10)  NULL,
           approval_status       NVARCHAR(15)  NOT NULL DEFAULT 'draft',
           approved_by           INT           NULL,
           approved_at           DATETIME      NULL,
           created_by            INT           NOT NULL,
           created_at            DATETIME      DEFAULT GETDATE(),
           updated_at            DATETIME      DEFAULT GETDATE()
       )""",
    # Migrations på eksisterende tabel
    """IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id=OBJECT_ID('BarselCases') AND name='mor_uger')
       ALTER TABLE BarselCases ADD mor_uger INT NULL""",
    """IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id=OBJECT_ID('BarselCases') AND name='faed_uger')
       ALTER TABLE BarselCases ADD faed_uger INT NULL""",
    """IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id=OBJECT_ID('BarselCases') AND name='forl_uger')
       ALTER TABLE BarselCases ADD forl_uger INT NULL""",
    """IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id=OBJECT_ID('BarselCases') AND name='hub_user_id')
       ALTER TABLE BarselCases ADD hub_user_id INT NULL""",
    """IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id=OBJECT_ID('BarselCases') AND name='mor_ansat')
       ALTER TABLE BarselCases ADD mor_ansat BIT NOT NULL DEFAULT 0""",
    """IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id=OBJECT_ID('BarselCases') AND name='far_ansat')
       ALTER TABLE BarselCases ADD far_ansat BIT NOT NULL DEFAULT 0""",
    """IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id=OBJECT_ID('BarselCases') AND name='mor_ferie_optjent')
       ALTER TABLE BarselCases ADD mor_ferie_optjent INT NOT NULL DEFAULT 0""",
    """IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id=OBJECT_ID('BarselCases') AND name='mor_ferie_selvbetalt')
       ALTER TABLE BarselCases ADD mor_ferie_selvbetalt INT NOT NULL DEFAULT 0""",
    """IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id=OBJECT_ID('BarselCases') AND name='far_ferie_optjent')
       ALTER TABLE BarselCases ADD far_ferie_optjent INT NOT NULL DEFAULT 0""",
    """IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id=OBJECT_ID('BarselCases') AND name='far_ferie_selvbetalt')
       ALTER TABLE BarselCases ADD far_ferie_selvbetalt INT NOT NULL DEFAULT 0""",
    """IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id=OBJECT_ID('BarselCases') AND name='approval_status')
       ALTER TABLE BarselCases ADD approval_status NVARCHAR(15) NOT NULL DEFAULT 'draft'""",
    """IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id=OBJECT_ID('BarselCases') AND name='approved_by')
       ALTER TABLE BarselCases ADD approved_by INT NULL""",
    """IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id=OBJECT_ID('BarselCases') AND name='approved_at')
       ALTER TABLE BarselCases ADD approved_at DATETIME NULL""",
    # Multi-periode orlovsplan (mor/far → liste af perioder). Gemmes som JSON,
    # så en sag kan have et vilkårligt antal orlovs- og ferieperioder (fx delt
    # forældreorlov). De gamle scalar-kolonner (mor_uger/faed_uger/forl_uger,
    # ferie-felter) bevares for bagudkompatibilitet, men detaljerne lever her.
    """IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id=OBJECT_ID('BarselCases') AND name='plan_json')
       ALTER TABLE BarselCases ADD plan_json NVARCHAR(MAX) NULL""",
    # Migration: backfill ferie-split fra gammelt mor_ferie/far_ferie hvis felterne stadig findes
    # (bruger sp_executesql så batch-parseren ikke fejler ved fravær af kolonnen)
    """IF EXISTS (SELECT * FROM sys.columns WHERE object_id=OBJECT_ID('BarselCases') AND name='mor_ferie')
       EXEC sp_executesql N'UPDATE BarselCases SET mor_ferie_optjent = ISNULL(mor_ferie,0)
                            WHERE mor_ferie IS NOT NULL AND mor_ferie_optjent = 0 AND mor_ferie_selvbetalt = 0'""",
    """IF EXISTS (SELECT * FROM sys.columns WHERE object_id=OBJECT_ID('BarselCases') AND name='far_ferie')
       EXEC sp_executesql N'UPDATE BarselCases SET far_ferie_optjent = ISNULL(far_ferie,0)
                            WHERE far_ferie IS NOT NULL AND far_ferie_optjent = 0 AND far_ferie_selvbetalt = 0'""",
    # HubUsers: manager-relation
    """IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id=OBJECT_ID('HubUsers') AND name='manager_id')
       ALTER TABLE HubUsers ADD manager_id INT NULL""",
]


def init_barsel_db():
    """Opret/migrer barseltabeller ved opstart. Idempotent."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        for sql in INIT_STMTS:
            cur.execute(sql)
        conn.commit()
        conn.close()
    except Exception:
        # Må ikke vælte app-opstart — migreringen forsøges igen ved næste genstart
        logger.exception("init_barsel_db fejlede")


# ---------------------------------------------------------------------------
# Hjælpefunktioner til felt-mapping (DB <-> frontend)
# ---------------------------------------------------------------------------

_EMPTY_PLAN = {"mor": [], "far": []}


def _parse_plan(raw) -> dict:
    """Læs plan_json → {mor:[...], far:[...]}. Tåler NULL/ugyldig JSON."""
    if not raw:
        return {"mor": [], "far": []}
    try:
        data = json.loads(raw)
    except (ValueError, TypeError):
        return {"mor": [], "far": []}
    if not isinstance(data, dict):
        return {"mor": [], "far": []}
    return {
        "mor": data.get("mor") if isinstance(data.get("mor"), list) else [],
        "far": data.get("far") if isinstance(data.get("far"), list) else [],
    }


def _serialize_plan(plan) -> str | None:
    """Frontend-plan → JSON-string til DB. None hvis tom/ugyldig."""
    if not isinstance(plan, dict):
        return None
    out = {
        "mor": plan.get("mor") if isinstance(plan.get("mor"), list) else [],
        "far": plan.get("far") if isinstance(plan.get("far"), list) else [],
    }
    if not out["mor"] and not out["far"]:
        return None
    return json.dumps(out, ensure_ascii=False)


def _row_to_front(r: dict) -> dict:
    """Konverterer DB-rækkens snake_case-nøgler til camelCase til frontend."""
    return {
        "id":                 r["id"],
        "hubUserId":          r.get("hub_user_id"),
        "hubUserName":        r.get("hub_user_name") or "",
        "hubUserManagerId":   r.get("hub_user_manager_id"),
        "morNavn":            r["mor_navn"] or "",
        "farNavn":            r["far_navn"] or "",
        "morAnsat":           bool(r.get("mor_ansat") or 0),
        "farAnsat":           bool(r.get("far_ansat") or 0),
        "termin":             r["termin"] or "",
        "foedselDato":        r["foedsel_dato"] or "",
        "morUger":            r["mor_uger"],
        "faedUger":           r["faed_uger"],
        "forlUger":           r["forl_uger"],
        "morFerieOptjent":    r.get("mor_ferie_optjent") or 0,
        "morFerieSelvbetalt": r.get("mor_ferie_selvbetalt") or 0,
        "farFerieOptjent":    r.get("far_ferie_optjent") or 0,
        "farFerieSelvbetalt": r.get("far_ferie_selvbetalt") or 0,
        "faedStart":          r["faed_start"] or "",
        "forlStart":          r["forl_start"] or "",
        "plan":               _parse_plan(r.get("plan_json")),
        "approvalStatus":     r.get("approval_status") or "draft",
        "approvedBy":         r.get("approved_by"),
        "approvedByName":     r.get("approved_by_name") or "",
        "approvedAt":         (r["approved_at"].isoformat() if r.get("approved_at") else ""),
        "createdBy":          r["created_by"],
        "createdByName":      r.get("created_by_name") or "",
    }


def _nullable_int(val):
    if val is None or val == "":
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _front_to_db(data: dict) -> dict:
    """Konverterer frontend camelCase til DB snake_case."""
    return {
        "hub_user_id":          _nullable_int(data.get("hubUserId")),
        "mor_navn":             (data.get("morNavn") or "")[:100],
        "far_navn":             (data.get("farNavn") or "")[:100],
        "mor_ansat":            1 if data.get("morAnsat") else 0,
        "far_ansat":            1 if data.get("farAnsat") else 0,
        "termin":               (data.get("termin") or "")[:10],
        "foedsel_dato":         (data.get("foedselDato") or "")[:10],
        "mor_uger":             _nullable_int(data.get("morUger")),
        "faed_uger":            _nullable_int(data.get("faedUger")),
        "forl_uger":            _nullable_int(data.get("forlUger")),
        "mor_ferie_optjent":    max(0, int(data.get("morFerieOptjent") or 0)),
        "mor_ferie_selvbetalt": max(0, int(data.get("morFerieSelvbetalt") or 0)),
        "far_ferie_optjent":    max(0, int(data.get("farFerieOptjent") or 0)),
        "far_ferie_selvbetalt": max(0, int(data.get("farFerieSelvbetalt") or 0)),
        "faed_start":           (data.get("faedStart") or "")[:10],
        "forl_start":           (data.get("forlStart") or "")[:10],
        "plan_json":            _serialize_plan(data.get("plan")),
    }


def _settings_to_front(s: dict) -> dict:
    return {
        "gravUger":     s["grav_uger"],
        "morUger":      s["mor_uger"],
        "faedUger":     s["faed_uger"],
        "forlUger":     s["forl_uger"],
        "notifyEmails": s.get("notify_emails") or "",
    }


def _front_to_settings(data: dict) -> dict:
    # Uge-felterne bevares i skemaet, men er ikke længere forudfyldte
    # standardværdier i UI'et (fjernet: mor 26 / far 17). Orlovsstrukturen
    # styres nu pr. sag i den enkelte plan (plan_json).
    return {
        "grav_uger":     max(0, int(data.get("gravUger", 0))),
        "mor_uger":      max(0, int(data.get("morUger",  0))),
        "faed_uger":     max(0, int(data.get("faedUger", 0))),
        "forl_uger":     max(0, int(data.get("forlUger", 0))),
        "notify_emails": (data.get("notifyEmails") or "").strip()[:1000],
    }


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

_DEFAULT_SETTINGS = {
    "grav_uger": 0, "mor_uger": 0, "faed_uger": 0,
    "forl_uger": 0, "notify_emails": "",
}


def get_settings() -> dict:
    """Hent virksomhedens barselsindstillinger (senest opdaterede)."""
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute("SELECT TOP 1 * FROM BarselSettings ORDER BY updated_at DESC")
        row = cur.fetchone()
        conn.close()
        if row:
            return _settings_to_front(row)
    except Exception:
        # Fallback til standardindstillingerne — siden skal kunne vises alligevel
        logger.exception("get_settings fejlede — bruger standardindstillinger")
    return _settings_to_front(_DEFAULT_SETTINGS)


def upsert_settings(data: dict, user_id: int):
    """Gem (opret eller opdater) virksomhedens barselsindstillinger."""
    s = _front_to_settings(data)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM BarselSettings")
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute(
            """INSERT INTO BarselSettings
               (grav_uger, mor_uger, faed_uger, forl_uger, notify_emails, updated_by)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (s["grav_uger"], s["mor_uger"], s["faed_uger"],
             s["forl_uger"], s["notify_emails"], user_id),
        )
    else:
        cur.execute(
            """UPDATE BarselSettings SET
               grav_uger=%s, mor_uger=%s, faed_uger=%s,
               forl_uger=%s, notify_emails=%s,
               updated_by=%s, updated_at=GETDATE()""",
            (s["grav_uger"], s["mor_uger"], s["faed_uger"],
             s["forl_uger"], s["notify_emails"], user_id),
        )
    conn.commit()
    conn.close()

# ---------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------

_BASE_SELECT = """
    SELECT c.*,
           cu.name       AS created_by_name,
           hu.name       AS hub_user_name,
           hu.manager_id AS hub_user_manager_id,
           au.name       AS approved_by_name
    FROM   BarselCases c
    LEFT JOIN HubUsers cu ON c.created_by   = cu.id
    LEFT JOIN HubUsers hu ON c.hub_user_id  = hu.id
    LEFT JOIN HubUsers au ON c.approved_by  = au.id
"""


def get_cases(user_id: int, see_all: bool) -> list:
    """Hent barselsager.

    Admin/management ser alt. Andre ser sager hvor de selv har oprettet
    sagen, OR de er angivet som hub_user_id (medarbejderen), OR de er
    leder for medarbejderen (HubUsers.manager_id = user_id).
    """
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        if see_all:
            cur.execute(_BASE_SELECT + " ORDER BY c.created_at DESC")
        else:
            cur.execute(
                _BASE_SELECT +
                """ WHERE c.created_by = %s
                       OR c.hub_user_id = %s
                       OR c.hub_user_id IN (SELECT id FROM HubUsers WHERE manager_id = %s)
                    ORDER BY c.created_at DESC""",
                (user_id, user_id, user_id),
            )
        rows = cur.fetchall()
        conn.close()
        return [_row_to_front(r) for r in rows]
    except Exception:
        logger.exception("get_cases fejlede")
        raise


def get_case(case_id: int) -> dict | None:
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(_BASE_SELECT + " WHERE c.id = %s", (case_id,))
        row = cur.fetchone()
        conn.close()
        return _row_to_front(row) if row else None
    except Exception:
        # Re-raise: None betyder "sag findes ikke" — en DB-fejl må ikke ligne en 404
        logger.exception("get_case fejlede")
        raise


def user_can_access_case(user: dict, case_id: int) -> bool:
    """True hvis brugeren må læse/skrive sagen."""
    if user["role"] in ("admin", "management"):
        return True
    case = get_case(case_id)
    if not case:
        return False
    if case["createdBy"] == user["id"]:
        return True
    if case["hubUserId"] == user["id"]:
        return True
    # Tjek om brugeren er leder for medarbejderen på sagen
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT manager_id FROM HubUsers WHERE id = %s",
            (case["hubUserId"] or 0,),
        )
        row = cur.fetchone()
        conn.close()
        return bool(row and row[0] == user["id"])
    except Exception:
        # Fail-closed: ved fejl nægtes adgang
        logger.exception("user_can_access_case: manager-opslag fejlede")
        return False


def create_case(data: dict, user_id: int) -> int:
    """Opret ny barselsag. Returnerer det nye ID."""
    d = _front_to_db(data)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO BarselCases
           (hub_user_id, mor_navn, far_navn, mor_ansat, far_ansat,
            termin, foedsel_dato,
            mor_uger, faed_uger, forl_uger,
            mor_ferie_optjent, mor_ferie_selvbetalt,
            far_ferie_optjent, far_ferie_selvbetalt,
            faed_start, forl_start, plan_json, created_by)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (d["hub_user_id"], d["mor_navn"], d["far_navn"],
         d["mor_ansat"], d["far_ansat"],
         d["termin"], d["foedsel_dato"],
         d["mor_uger"], d["faed_uger"], d["forl_uger"],
         d["mor_ferie_optjent"], d["mor_ferie_selvbetalt"],
         d["far_ferie_optjent"], d["far_ferie_selvbetalt"],
         d["faed_start"], d["forl_start"], d["plan_json"], user_id),
    )
    cur.execute("SELECT SCOPE_IDENTITY()")
    new_id = int(cur.fetchone()[0])
    conn.commit()
    conn.close()
    return new_id


def update_case(case_id: int, data: dict, user: dict):
    """Opdater eksisterende barselsag. Adgangstjek allerede udført."""
    d = _front_to_db(data)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """UPDATE BarselCases SET
           hub_user_id=%s, mor_navn=%s, far_navn=%s,
           mor_ansat=%s, far_ansat=%s,
           termin=%s, foedsel_dato=%s,
           mor_uger=%s, faed_uger=%s, forl_uger=%s,
           mor_ferie_optjent=%s, mor_ferie_selvbetalt=%s,
           far_ferie_optjent=%s, far_ferie_selvbetalt=%s,
           faed_start=%s, forl_start=%s, plan_json=%s, updated_at=GETDATE()
           WHERE id=%s""",
        (d["hub_user_id"], d["mor_navn"], d["far_navn"],
         d["mor_ansat"], d["far_ansat"],
         d["termin"], d["foedsel_dato"],
         d["mor_uger"], d["faed_uger"], d["forl_uger"],
         d["mor_ferie_optjent"], d["mor_ferie_selvbetalt"],
         d["far_ferie_optjent"], d["far_ferie_selvbetalt"],
         d["faed_start"], d["forl_start"], d["plan_json"], case_id),
    )
    conn.commit()
    conn.close()


def delete_case(case_id: int):
    """Slet barselsag. Adgangstjek allerede udført."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM BarselCases WHERE id=%s", (case_id,))
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Godkendelses-flow
# ---------------------------------------------------------------------------

def set_approval_status(case_id: int, status: str, approver_id: int | None):
    """Opdaterer godkendelsesstatus. Sætter approved_by/approved_at hvis approved."""
    conn = get_conn()
    cur = conn.cursor()
    if status == "approved":
        cur.execute(
            """UPDATE BarselCases SET
               approval_status='approved',
               approved_by=%s, approved_at=GETDATE(),
               updated_at=GETDATE()
               WHERE id=%s""",
            (approver_id, case_id),
        )
    else:
        cur.execute(
            """UPDATE BarselCases SET
               approval_status=%s,
               approved_by=NULL, approved_at=NULL,
               updated_at=GETDATE()
               WHERE id=%s""",
            (status, case_id),
        )
    conn.commit()
    conn.close()


def user_can_approve_case(user: dict, case_id: int) -> bool:
    """Admin/management kan godkende. Ellers skal brugeren være manager for
    sagens medarbejder (HubUsers.manager_id = user.id for hub_user_id)."""
    if user["role"] in ("admin", "management"):
        return True
    case = get_case(case_id)
    if not case or not case.get("hubUserId"):
        return False
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT manager_id FROM HubUsers WHERE id = %s",
            (case["hubUserId"],),
        )
        row = cur.fetchone()
        conn.close()
        return bool(row and row[0] == user["id"])
    except Exception:
        # Fail-closed: ved fejl nægtes godkendelse
        logger.exception("user_can_approve_case: manager-opslag fejlede")
        return False


# ---------------------------------------------------------------------------
# HubUser-liste til dropdown (kun navn + id)
# ---------------------------------------------------------------------------

def list_hub_users() -> list:
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(
            "SELECT id, name FROM HubUsers WHERE is_active = 1 ORDER BY name"
        )
        rows = cur.fetchall() or []
        conn.close()
        return [{"id": r["id"], "name": r["name"]} for r in rows]
    except Exception:
        # Dropdown-hjælper — tom liste er et fornuftigt fallback
        logger.exception("list_hub_users fejlede")
        return []

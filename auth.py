import os
import datetime
import logging
import pymssql
from passlib.context import CryptContext
from dotenv import load_dotenv
from fastapi import Request

load_dotenv()

logger = logging.getLogger(__name__)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Fallback-værdier — bruges hvis DB ikke kan kontaktes. De er også seed-værdier
# der bliver indlæst i HubRoles ved første init_db. Alle 6 markeres is_system=1
# så de ikke kan slettes via admin-UI.
_DEFAULT_ROLES = [
    # Kontorskærm-bruger — laveste rang. Har KUN adgang til rotationen via en
    # RoleResourceAccess-override (seedes i init_db), ikke resten af hubben.
    {"name": "screen",           "label": "Skærm",             "rank": 0, "is_system": True},
    {"name": "salesperson",      "label": "Sælger",            "rank": 1, "is_system": True},
    {"name": "sales_manager",    "label": "Sales Manager",     "rank": 2, "is_system": True},
    {"name": "sales_operations", "label": "Sales Operations",  "rank": 3, "is_system": True},
    {"name": "marketing",        "label": "Marketing",         "rank": 4, "is_system": True},
    {"name": "management",       "label": "Management",        "rank": 5, "is_system": True},
    {"name": "admin",            "label": "Admin",             "rank": 6, "is_system": True},
]

# Disse dicts opdateres af reload_roles_cache() ved opstart og når admin ændrer
# roller. Default-værdier sat så modulet er funktionelt selv før DB svarer.
ROLE_RANK   = {r["name"]: r["rank"]  for r in _DEFAULT_ROLES}
ROLE_LABELS = {r["name"]: r["label"] for r in _DEFAULT_ROLES}
ROLES_META  = {r["name"]: dict(r)    for r in _DEFAULT_ROLES}


class RequiresLoginException(Exception):
    pass


# Fælles pooled DB-forbindelse — se db.py. Navnet genexporteres herfra, fordi
# modul_barsel og usage_tracking importerer get_conn fra auth.
from db import get_conn  # noqa: E402,F401


# ---------------------------------------------------------------------------
# DB initialisation
# ---------------------------------------------------------------------------

def init_db():
    """Opret hub-tabeller hvis de ikke allerede eksisterer. Idempotent."""
    stmts = [
        # Brugertabel
        """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='HubUsers' AND xtype='U')
           CREATE TABLE HubUsers (
               id            INT IDENTITY(1,1) PRIMARY KEY,
               username      NVARCHAR(50)  NOT NULL,
               password_hash NVARCHAR(255) NOT NULL,
               name          NVARCHAR(100) NOT NULL,
               initials      NVARCHAR(10)  NOT NULL,
               role          NVARCHAR(30)  NOT NULL DEFAULT 'salesperson',
               brand         NVARCHAR(50)  NULL,
               is_active     BIT           NOT NULL DEFAULT 1,
               created_at    DATETIME      DEFAULT GETDATE()
           )""",
        # Roller (dynamisk — admin kan oprette egne)
        """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='HubRoles' AND xtype='U')
           CREATE TABLE HubRoles (
               name        NVARCHAR(50)  NOT NULL PRIMARY KEY,
               label       NVARCHAR(100) NOT NULL,
               rank        INT           NOT NULL,
               is_system   BIT           NOT NULL DEFAULT 0,
               created_at  DATETIME      DEFAULT GETDATE()
           )""",
        # Per-rolle, per-ressource adgangsstyring (kollektiv override)
        """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='RoleResourceAccess' AND xtype='U')
           CREATE TABLE RoleResourceAccess (
               id          INT IDENTITY(1,1) PRIMARY KEY,
               role        NVARCHAR(50)  NOT NULL,
               resource_id NVARCHAR(100) NOT NULL,
               access      NVARCHAR(10)  NOT NULL,
               created_at  DATETIME      DEFAULT GETDATE(),
               CONSTRAINT UQ_RoleResourceAccess UNIQUE (role, resource_id)
           )""",
        # Per-bruger, per-ressource adgangsstyring
        """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='UserResourceAccess' AND xtype='U')
           CREATE TABLE UserResourceAccess (
               id          INT IDENTITY(1,1) PRIMARY KEY,
               user_id     INT           NOT NULL,
               resource_id NVARCHAR(100) NOT NULL,
               access      NVARCHAR(10)  NOT NULL,
               created_at  DATETIME      DEFAULT GETDATE()
           )""",
        # Hold
        """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Teams' AND xtype='U')
           CREATE TABLE Teams (
               id          INT IDENTITY(1,1) PRIMARY KEY,
               name        NVARCHAR(100) NOT NULL,
               brand       NVARCHAR(50)  NULL,
               description NVARCHAR(500) NULL,
               created_at  DATETIME      DEFAULT GETDATE()
           )""",
        # Holdmedlemskaber (tidsbaserede)
        """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='TeamMemberships' AND xtype='U')
           CREATE TABLE TeamMemberships (
               id          INT IDENTITY(1,1) PRIMARY KEY,
               user_id     INT           NOT NULL,
               team_id     INT           NOT NULL,
               role        NVARCHAR(20)  NOT NULL DEFAULT 'member',
               start_date  NVARCHAR(10)  NOT NULL,
               end_date    NVARCHAR(10)  NULL,
               notes       NVARCHAR(500) NULL,
               created_at  DATETIME      DEFAULT GETDATE()
           )""",
        # Per-bruger team-dataadgang: hvilke teams brugeren må se data for i
        # performance-dashboards. Ingen rækker = ubegrænset (ser alle teams).
        """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='HubUserTeamAccess' AND xtype='U')
           CREATE TABLE HubUserTeamAccess (
               id          INT IDENTITY(1,1) PRIMARY KEY,
               user_id     INT NOT NULL,
               team_id     INT NOT NULL,
               created_at  DATETIME DEFAULT GETDATE(),
               CONSTRAINT UQ_HubUserTeamAccess UNIQUE (user_id, team_id)
           )""",
        # Forecast-gemte prognoser. team indgår i den unikke nøgle, så en
        # sælger kan have ét forecast pr. team uden at de overskriver hinanden.
        """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='HubForecasts' AND xtype='U')
           CREATE TABLE HubForecasts (
               id               INT IDENTITY(1,1) PRIMARY KEY,
               forecast_year    INT           NOT NULL,
               forecast_month   INT           NOT NULL,
               level            NVARCHAR(10)  NOT NULL,
               dimension_key    NVARCHAR(200) NOT NULL,
               team             NVARCHAR(100) NOT NULL DEFAULT '',
               pipeline_pct     DECIMAL(5,2)  NOT NULL DEFAULT 30.00,
               manual_amount    DECIMAL(18,2) NOT NULL DEFAULT 0.00,
               forecast_amount  DECIMAL(18,2) NOT NULL DEFAULT 0.00,
               created_by       NVARCHAR(100) NOT NULL,
               created_at       DATETIME      DEFAULT GETDATE(),
               updated_at       DATETIME      DEFAULT GETDATE()
           )""",
        """IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('HubForecasts') AND name = 'team')
           AND NOT EXISTS (
               SELECT * FROM sys.indexes
               WHERE name='UQ_HubForecasts_TeamKey' AND object_id = OBJECT_ID('HubForecasts')
           )
           ALTER TABLE HubForecasts
           ADD CONSTRAINT UQ_HubForecasts_TeamKey
           UNIQUE (forecast_year, forecast_month, level, dimension_key, team)""",
        # Sales managerens vurdering af det samlede sælger-forecast pr. team —
        # gemmes separat så den aldrig rører sælgernes egne tal.
        """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='HubForecastReviews' AND xtype='U')
           CREATE TABLE HubForecastReviews (
               id              INT IDENTITY(1,1) PRIMARY KEY,
               forecast_year   INT            NOT NULL,
               forecast_month  INT            NOT NULL,
               team            NVARCHAR(100)  NOT NULL,
               manager_amount  DECIMAL(18,2)  NOT NULL DEFAULT 0.00,
               comment         NVARCHAR(1000) NULL,
               created_by      NVARCHAR(100)  NOT NULL,
               created_at      DATETIME       DEFAULT GETDATE(),
               updated_at      DATETIME       DEFAULT GETDATE(),
               CONSTRAINT UQ_HubForecastReviews UNIQUE (forecast_year, forecast_month, team)
           )""",
        # Tilføj service_activation_date til PipedriveDeals hvis kolonnen mangler
        """IF EXISTS (SELECT * FROM sysobjects WHERE name='PipedriveDeals' AND xtype='U')
           AND NOT EXISTS (
               SELECT * FROM sys.columns
               WHERE object_id = OBJECT_ID('PipedriveDeals')
                 AND name = 'service_activation_date'
           )
           ALTER TABLE PipedriveDeals ADD service_activation_date DATETIME NULL""",
        # Usage-tracking: én række pr. sidevisning
        """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='HubUsageLog' AND xtype='U')
           CREATE TABLE HubUsageLog (
               id             INT IDENTITY(1,1) PRIMARY KEY,
               user_id        INT           NULL,
               path           NVARCHAR(400) NOT NULL,
               resource_label NVARCHAR(150) NULL,
               method         NVARCHAR(10)  NOT NULL DEFAULT 'GET',
               status_code    INT           NULL,
               duration_ms    INT           NULL,
               created_at     DATETIME      NOT NULL DEFAULT GETDATE()
           )""",
        """IF NOT EXISTS (
               SELECT * FROM sys.indexes
               WHERE name='IX_HubUsageLog_created' AND object_id = OBJECT_ID('HubUsageLog')
           )
           CREATE INDEX IX_HubUsageLog_created ON HubUsageLog (created_at)""",
        """IF NOT EXISTS (
               SELECT * FROM sys.indexes
               WHERE name='IX_HubUsageLog_user' AND object_id = OBJECT_ID('HubUsageLog')
           )
           CREATE INDEX IX_HubUsageLog_user ON HubUsageLog (user_id, created_at)""",
    ]
    try:
        conn = get_conn()
        cur = conn.cursor()
        for sql in stmts:
            cur.execute(sql)
        conn.commit()
        # Seed system-roller hvis HubRoles er tom (idempotent)
        cur.execute("SELECT COUNT(*) FROM HubRoles")
        if (cur.fetchone() or [0])[0] == 0:
            for r in _DEFAULT_ROLES:
                cur.execute(
                    "INSERT INTO HubRoles (name, label, rank, is_system) VALUES (%s, %s, %s, %s)",
                    (r["name"], r["label"], r["rank"], 1 if r["is_system"] else 0),
                )
            conn.commit()

        # Sikr at 'screen'-rollen findes også på eksisterende installationer
        # (seedet ovenfor kører kun når HubRoles er helt tom). Idempotent.
        cur.execute(
            "IF NOT EXISTS (SELECT 1 FROM HubRoles WHERE name='screen') "
            "INSERT INTO HubRoles (name, label, rank, is_system) "
            "VALUES ('screen', 'Skærm', 0, 1)"
        )
        # 'screen' må KUN se rotationen: åbn rotations-ruterne ('rotation'),
        # selve rotations-kategorien og autoplay-menupunktet — alt andet spærres
        # af rang-tjekket (rank 0). Idempotent.
        for rid in ("rotation", "rotation-dashboards", "rotation-autoplay"):
            cur.execute(
                "IF NOT EXISTS (SELECT 1 FROM RoleResourceAccess "
                "WHERE role='screen' AND resource_id=%s) "
                "INSERT INTO RoleResourceAccess (role, resource_id, access) "
                "VALUES ('screen', %s, 'read')",
                (rid, rid),
            )
        conn.commit()
        conn.close()
    except Exception:
        logger.exception("init_db: kunne ikke initialisere tabeller")

    # Indlæs rolle-tabel i memory så ROLE_RANK / ROLE_LABELS afspejler DB
    reload_roles_cache()


# ---------------------------------------------------------------------------
# Role management
# ---------------------------------------------------------------------------

def reload_roles_cache():
    """Genopfrisk in-memory ROLE_RANK / ROLE_LABELS / ROLES_META fra DB.
    Kaldes ved opstart og efter admin opretter/ændrer/sletter en rolle.
    """
    global ROLE_RANK, ROLE_LABELS, ROLES_META
    try:
        conn = get_conn()
        cur  = conn.cursor(as_dict=True)
        cur.execute("SELECT name, label, rank, is_system FROM HubRoles ORDER BY rank")
        rows = cur.fetchall() or []
        conn.close()
        if rows:
            ROLE_RANK.clear()
            ROLE_LABELS.clear()
            ROLES_META.clear()
            for r in rows:
                ROLE_RANK[r["name"]]   = int(r["rank"])
                ROLE_LABELS[r["name"]] = r["label"]
                ROLES_META[r["name"]]  = {
                    "name": r["name"],
                    "label": r["label"],
                    "rank": int(r["rank"]),
                    "is_system": bool(r["is_system"]),
                }
    except Exception as e:
        # DB nede — behold default-værdier
        logger.warning("reload_roles_cache: DB utilgængelig, beholder defaults: %s", e)


def list_roles() -> list:
    """Returnér roller sorteret efter rang (lavest først)."""
    return sorted(ROLES_META.values(), key=lambda r: r["rank"])


def get_role_resource_access(role: str) -> dict:
    """Returnér {resource_id: access} for en rolles eksplicitte overrides."""
    try:
        conn = get_conn()
        cur  = conn.cursor(as_dict=True)
        cur.execute(
            "SELECT resource_id, access FROM RoleResourceAccess WHERE role = %s",
            (role,),
        )
        rows = cur.fetchall() or []
        conn.close()
        return {r["resource_id"]: r["access"] for r in rows}
    except Exception:
        return {}


def set_role_resource_access(role: str, resource_id: str, access: str | None) -> None:
    """Opret/opdater/slet en rolle-ressource-override.
    access=None (eller 'default') sletter rækken så standard min_role gælder.
    """
    try:
        conn = get_conn()
        cur  = conn.cursor()
        if not access or access == "default":
            cur.execute(
                "DELETE FROM RoleResourceAccess WHERE role = %s AND resource_id = %s",
                (role, resource_id),
            )
        else:
            # UPSERT — pymssql/SQL Server: brug MERGE eller IF EXISTS
            cur.execute(
                "DELETE FROM RoleResourceAccess WHERE role = %s AND resource_id = %s",
                (role, resource_id),
            )
            cur.execute(
                "INSERT INTO RoleResourceAccess (role, resource_id, access) VALUES (%s, %s, %s)",
                (role, resource_id, access),
            )
        conn.commit()
        conn.close()
    except Exception:
        logger.exception("set_role_resource_access fejlede (role=%s, resource=%s)",
                         role, resource_id)


def create_role(name: str, label: str, rank: int) -> tuple[bool, str]:
    """Returner (success, error_message)."""
    name = (name or "").strip().lower().replace(" ", "_")
    label = (label or "").strip()
    if not name or not label:
        return False, "Navn og label er påkrævet"
    if name in ROLES_META:
        return False, f"Rolle '{name}' findes allerede"
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(
            "INSERT INTO HubRoles (name, label, rank, is_system) VALUES (%s, %s, %s, 0)",
            (name, label, int(rank)),
        )
        conn.commit()
        conn.close()
        reload_roles_cache()
        return True, ""
    except Exception as e:
        return False, str(e)


def update_role(name: str, label: str, rank: int) -> tuple[bool, str]:
    if name not in ROLES_META:
        return False, "Rolle findes ikke"
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(
            "UPDATE HubRoles SET label = %s, rank = %s WHERE name = %s",
            (label.strip(), int(rank), name),
        )
        conn.commit()
        conn.close()
        reload_roles_cache()
        return True, ""
    except Exception as e:
        return False, str(e)


def delete_role(name: str) -> tuple[bool, str]:
    meta = ROLES_META.get(name)
    if not meta:
        return False, "Rolle findes ikke"
    if meta["is_system"]:
        return False, "System-roller kan ikke slettes"
    # Tjek om nogen bruger denne rolle
    try:
        conn = get_conn()
        cur  = conn.cursor(as_dict=True)
        cur.execute("SELECT COUNT(*) AS n FROM HubUsers WHERE role = %s", (name,))
        n = int((cur.fetchone() or {}).get("n", 0) or 0)
        if n > 0:
            conn.close()
            return False, f"{n} brugere har stadig denne rolle — flyt dem først"
        cur.execute("DELETE FROM RoleResourceAccess WHERE role = %s", (name,))
        cur.execute("DELETE FROM HubRoles WHERE name = %s", (name,))
        conn.commit()
        conn.close()
        reload_roles_cache()
        return True, ""
    except Exception as e:
        return False, str(e)


# ---------------------------------------------------------------------------
# Password helpers
# ---------------------------------------------------------------------------

def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


# ---------------------------------------------------------------------------
# Resource access
# ---------------------------------------------------------------------------

def get_user_resource_access(user_id: int) -> dict:
    """Returnér {resource_id: access} for brugerens eksplicitte overrides."""
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(
            "SELECT resource_id, access FROM UserResourceAccess WHERE user_id = %s",
            (user_id,),
        )
        rows = cur.fetchall() or []
        conn.close()
        return {r["resource_id"]: r["access"] for r in rows}
    except Exception:
        return {}


def get_user_teams(user_id: int) -> list:
    """Returnér liste af holdnavne som brugeren er aktivt medlem af i dag."""
    try:
        today = datetime.date.today().isoformat()
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(
            """
            SELECT t.name
            FROM   TeamMemberships tm
            JOIN   Teams t ON t.id = tm.team_id
            WHERE  tm.user_id = %s
              AND  tm.start_date <= %s
              AND  (tm.end_date IS NULL OR tm.end_date >= %s)
            """,
            (user_id, today, today),
        )
        rows = cur.fetchall() or []
        conn.close()
        return [r["name"] for r in rows]
    except Exception:
        return []


def get_user_data_teams(user_id: int) -> list:
    """Returnér holdnavne brugeren eksplicit er begrænset til at se data for
    (HubUserTeamAccess — sættes på admin-brugersiden). Tom liste = ubegrænset.
    """
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(
            """
            SELECT t.name
            FROM   HubUserTeamAccess a
            JOIN   Teams t ON t.id = a.team_id
            WHERE  a.user_id = %s
            """,
            (user_id,),
        )
        rows = cur.fetchall() or []
        conn.close()
        return [r["name"] for r in rows]
    except Exception:
        return []


def allowed_data_teams(user: dict) -> list | None:
    """Teams brugeren må se performance-data for. None = ubegrænset.

    Admin er altid ubegrænset; alle andre begrænses kun hvis admin har sat
    eksplicitte teams på brugeren (HubUserTeamAccess).
    """
    if user.get("role") == "admin":
        return None
    teams = user.get("_data_teams") or []
    return teams if teams else None


def resolve_resource_access(user: dict, resource_id: str, min_role: str, brand=None, required_team: str = None, exclude_roles: list = None) -> str:
    """
    Returnér 'none', 'read' eller 'write' for en given bruger + ressource.

    Rækkefølge:
    1. Eksplicit per-bruger override → brugt direkte (finkornet finjustering).
    2. Eksplicit per-rolle override → brugt direkte (kollektiv styring for alle med samme rolle).
    3. Hardcoded min_role + brand → 'none' hvis ikke kvalificeret, ellers 'write'.
    4. Hvis required_team er sat: tjek holdmedlemskab (gælder salesperson + sales_manager).
    5. Hvis exclude_roles er sat: bloker specifikke roller (admin undtaget).
    """
    # 1. Eksplicit per-bruger override?
    overrides = user.get("_resource_access", {})
    if resource_id in overrides:
        return overrides[resource_id]

    # 2. Eksplicit per-rolle override?
    role_overrides = user.get("_role_access", {})
    if resource_id in role_overrides:
        return role_overrides[resource_id]

    # 3. Rolletjek (rang)
    user_rank = ROLE_RANK.get(user["role"], 0)
    req_rank  = ROLE_RANK.get(min_role, 99)
    if user_rank < req_rank:
        return "none"

    # Brandspærring for sælger og sales manager
    if user["role"] in ("salesperson", "sales_manager") and brand and user.get("brand") != brand:
        return "none"

    # Holdspærring — gælder kun for salesperson (sales_manager og højere bypasser)
    if required_team and user["role"] == "salesperson":
        user_teams = user.get("_teams", [])
        if required_team not in user_teams:
            return "none"

    # Rolle-ekskludering — admin bypasser altid
    if exclude_roles and user["role"] != "admin" and user["role"] in exclude_roles:
        return "none"

    return "write"


# ---------------------------------------------------------------------------
# User lookup
# ---------------------------------------------------------------------------

def authenticate_user(username: str, password: str):
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(
            "SELECT * FROM HubUsers WHERE username = %s AND is_active = 1",
            (username,),
        )
        user = cur.fetchone()
        conn.close()
        if not user or not verify_password(password, user["password_hash"]):
            return None
        user["_resource_access"] = get_user_resource_access(user["id"])
        user["_role_access"]     = get_role_resource_access(user["role"])
        user["_teams"] = get_user_teams(user["id"])
        user["_data_teams"] = get_user_data_teams(user["id"])
        return user
    except Exception:
        logger.exception("authenticate_user fejlede (username=%s)", username)
        return None


def get_user_by_id(user_id: int):
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(
            "SELECT * FROM HubUsers WHERE id = %s AND is_active = 1",
            (user_id,),
        )
        user = cur.fetchone()
        conn.close()
        if user:
            user["_resource_access"] = get_user_resource_access(user_id)
            user["_role_access"]     = get_role_resource_access(user["role"])
            user["_teams"] = get_user_teams(user_id)
            user["_data_teams"] = get_user_data_teams(user_id)
        return user
    except Exception:
        return None


_DEV_USER = {
    "id": 0,
    "username": "dev",
    "name": "Dev User",
    "initials": "DV",
    "role": "admin",
    "brand": None,
    "is_active": 1,
    "_resource_access": {},
    "_role_access": {},
    "_teams": [],
    "_data_teams": [],
}


def get_current_user(request: Request):
    """FastAPI Depends — kaster RequiresLoginException hvis ikke logget ind."""
    if os.getenv("DEV_MODE") == "1":
        return _DEV_USER
    user_id = request.session.get("user_id")
    if not user_id:
        raise RequiresLoginException()
    user = get_user_by_id(user_id)
    if not user:
        raise RequiresLoginException()
    return user


# ---------------------------------------------------------------------------
# Legacy helper (beholdt for bagudkompatibilitet med kategori-gating)
# ---------------------------------------------------------------------------

def has_access(user: dict, min_role: str, brand=None) -> bool:
    return resolve_resource_access(user, "", min_role, brand) != "none"

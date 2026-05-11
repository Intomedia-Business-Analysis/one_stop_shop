import os
import datetime
import pymssql
from passlib.context import CryptContext
from dotenv import load_dotenv
from fastapi import Request

load_dotenv()

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

ROLE_RANK = {
    "salesperson":      1,
    "sales_manager":    2,
    "sales_operations": 3,
    "marketing":        4,
    "management":       5,
    "admin":            6,
}

ROLE_LABELS = {
    "salesperson":      "Sælger",
    "sales_manager":    "Sales Manager",
    "sales_operations": "Sales Operations",
    "marketing":        "Marketing",
    "management":       "Management",
    "admin":            "Admin",
}


class RequiresLoginException(Exception):
    pass


def get_conn():
    return pymssql.connect(
        server=os.getenv("DB_SERVER"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME", "INTOMEDIA"),
        login_timeout=5,
        timeout=5,
    )


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
        # Forecast-gemte prognoser
        """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='HubForecasts' AND xtype='U')
           CREATE TABLE HubForecasts (
               id               INT IDENTITY(1,1) PRIMARY KEY,
               forecast_year    INT           NOT NULL,
               forecast_month   INT           NOT NULL,
               level            NVARCHAR(10)  NOT NULL,
               dimension_key    NVARCHAR(200) NOT NULL,
               pipeline_pct     DECIMAL(5,2)  NOT NULL DEFAULT 30.00,
               manual_amount    DECIMAL(18,2) NOT NULL DEFAULT 0.00,
               forecast_amount  DECIMAL(18,2) NOT NULL DEFAULT 0.00,
               created_by       NVARCHAR(100) NOT NULL,
               created_at       DATETIME      DEFAULT GETDATE(),
               updated_at       DATETIME      DEFAULT GETDATE()
           )""",
        """IF NOT EXISTS (
               SELECT * FROM sys.indexes
               WHERE name='UQ_HubForecasts_Key' AND object_id = OBJECT_ID('HubForecasts')
           )
           ALTER TABLE HubForecasts
           ADD CONSTRAINT UQ_HubForecasts_Key
           UNIQUE (forecast_year, forecast_month, level, dimension_key)""",
        # Tilføj service_activation_date til PipedriveDeals hvis kolonnen mangler
        """IF EXISTS (SELECT * FROM sysobjects WHERE name='PipedriveDeals' AND xtype='U')
           AND NOT EXISTS (
               SELECT * FROM sys.columns
               WHERE object_id = OBJECT_ID('PipedriveDeals')
                 AND name = 'service_activation_date'
           )
           ALTER TABLE PipedriveDeals ADD service_activation_date DATETIME NULL""",
    ]
    try:
        conn = get_conn()
        cur = conn.cursor()
        for sql in stmts:
            cur.execute(sql)
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[init_db] Advarsel — kunne ikke initialisere tabeller: {e}")


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


def resolve_resource_access(user: dict, resource_id: str, min_role: str, brand=None, required_team: str = None, exclude_roles: list = None) -> str:
    """
    Returnér 'none', 'read' eller 'write' for en given bruger + ressource.

    Rækkefølge:
    1. Eksplicit DB-override pr. bruger → brugt direkte (kan udvide ELLER begrænse).
    2. Ingen override: tjek rolle + brand → 'none' hvis ikke kvalificeret, ellers 'write'.
    3. Hvis required_team er sat: tjek holdmedlemskab (gælder salesperson + sales_manager).
    4. Hvis exclude_roles er sat: bloker specifikke roller (admin undtaget).
    """
    # Eksplicit override?
    overrides = user.get("_resource_access", {})
    if resource_id in overrides:
        return overrides[resource_id]

    # Rolletjek
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
        user["_teams"] = get_user_teams(user["id"])
        return user
    except Exception as e:
        print(f"[authenticate_user] FEJL: {e}")
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
            user["_teams"] = get_user_teams(user_id)
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

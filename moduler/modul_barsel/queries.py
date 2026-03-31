from auth import get_conn

# ---------------------------------------------------------------------------
# DB-initialisering
# ---------------------------------------------------------------------------

INIT_STMTS = [
    # Virksomhedens globale barselsindstillinger
    """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='BarselSettings' AND xtype='U')
       CREATE TABLE BarselSettings (
           id         INT IDENTITY(1,1) PRIMARY KEY,
           grav_uger  INT NOT NULL DEFAULT 4,
           mor_uger   INT NOT NULL DEFAULT 26,
           faed_uger  INT NOT NULL DEFAULT 2,
           forl_uger  INT NOT NULL DEFAULT 17,
           tvil_uger  INT NOT NULL DEFAULT 13,
           updated_by INT NOT NULL DEFAULT 0,
           updated_at DATETIME DEFAULT GETDATE()
       )""",
    # Barselsager pr. medarbejder
    """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='BarselCases' AND xtype='U')
       CREATE TABLE BarselCases (
           id           INT IDENTITY(1,1) PRIMARY KEY,
           mor_navn     NVARCHAR(100) NULL,
           far_navn     NVARCHAR(100) NULL,
           termin       NVARCHAR(10)  NULL,
           foedsel_dato NVARCHAR(10)  NULL,
           foedsel_type NVARCHAR(20)  NOT NULL DEFAULT 'Enkeltbarn',
           mor_uger     INT           NULL,
           faed_uger    INT           NULL,
           forl_uger    INT           NULL,
           mor_ferie    INT           NOT NULL DEFAULT 0,
           far_ferie    INT           NOT NULL DEFAULT 0,
           faed_start   NVARCHAR(10)  NULL,
           forl_start   NVARCHAR(10)  NULL,
           created_by   INT           NOT NULL,
           created_at   DATETIME      DEFAULT GETDATE(),
           updated_at   DATETIME      DEFAULT GETDATE()
       )""",
    # Migration: tilføj nye kolonner til eksisterende tabel
    """IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id=OBJECT_ID('BarselCases') AND name='mor_uger')
       ALTER TABLE BarselCases ADD mor_uger INT NULL""",
    """IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id=OBJECT_ID('BarselCases') AND name='faed_uger')
       ALTER TABLE BarselCases ADD faed_uger INT NULL""",
    """IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id=OBJECT_ID('BarselCases') AND name='forl_uger')
       ALTER TABLE BarselCases ADD forl_uger INT NULL""",
    """IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id=OBJECT_ID('BarselCases') AND name='mor_ferie')
       ALTER TABLE BarselCases ADD mor_ferie INT NOT NULL DEFAULT 0""",
    """IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id=OBJECT_ID('BarselCases') AND name='far_ferie')
       ALTER TABLE BarselCases ADD far_ferie INT NOT NULL DEFAULT 0""",
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
    except Exception as e:
        print(f"[init_barsel_db] Advarsel: {e}")


# ---------------------------------------------------------------------------
# Hjælpefunktioner til felt-mapping (DB <-> frontend)
# ---------------------------------------------------------------------------

def _row_to_front(r: dict) -> dict:
    """Konverterer DB-rækkens snake_case-nøgler til camelCase til frontend."""
    return {
        "id":            r["id"],
        "morNavn":       r["mor_navn"] or "",
        "farNavn":       r["far_navn"] or "",
        "termin":        r["termin"] or "",
        "foedselDato":   r["foedsel_dato"] or "",
        "foedsel":       r["foedsel_type"] or "Enkeltbarn",
        "morUger":       r["mor_uger"],   # None = brug company-default
        "faedUger":      r["faed_uger"],  # None = brug company-default
        "forlUger":      r["forl_uger"],  # None = brug company-default
        "morFerie":      r["mor_ferie"] or 0,
        "farFerie":      r["far_ferie"] or 0,
        "faedStart":     r["faed_start"] or "",
        "forlStart":     r["forl_start"] or "",
        "createdBy":     r["created_by"],
        "createdByName": r.get("created_by_name") or "",
    }


def _front_to_db(data: dict) -> dict:
    """Konverterer frontend camelCase til DB snake_case."""
    def _nullable_int(val):
        if val is None or val == "":
            return None
        try:
            return int(val)
        except (ValueError, TypeError):
            return None

    return {
        "mor_navn":     (data.get("morNavn") or "")[:100],
        "far_navn":     (data.get("farNavn") or "")[:100],
        "termin":       (data.get("termin") or "")[:10],
        "foedsel_dato": (data.get("foedselDato") or "")[:10],
        "foedsel_type": (data.get("foedsel") or "Enkeltbarn")[:20],
        "mor_uger":     _nullable_int(data.get("morUger")),
        "faed_uger":    _nullable_int(data.get("faedUger")),
        "forl_uger":    _nullable_int(data.get("forlUger")),
        "mor_ferie":    max(0, int(data.get("morFerie") or 0)),
        "far_ferie":    max(0, int(data.get("farFerie") or 0)),
        "faed_start":   (data.get("faedStart") or "")[:10],
        "forl_start":   (data.get("forlStart") or "")[:10],
    }


def _settings_to_front(s: dict) -> dict:
    return {
        "gravUger": s["grav_uger"],
        "morUger":  s["mor_uger"],
        "faedUger": s["faed_uger"],
        "forlUger": s["forl_uger"],
        "tvilUger": s["tvil_uger"],
    }


def _front_to_settings(data: dict) -> dict:
    return {
        "grav_uger": max(0, int(data.get("gravUger", 4))),
        "mor_uger":  max(0, int(data.get("morUger",  26))),
        "faed_uger": max(0, int(data.get("faedUger", 2))),
        "forl_uger": max(0, int(data.get("forlUger", 17))),
        "tvil_uger": max(0, int(data.get("tvilUger", 13))),
    }


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

_DEFAULT_SETTINGS = {
    "grav_uger": 4, "mor_uger": 26, "faed_uger": 2,
    "forl_uger": 17, "tvil_uger": 13,
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
    except Exception as e:
        print(f"[get_settings] Fejl: {e}")
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
               (grav_uger, mor_uger, faed_uger, forl_uger, tvil_uger, updated_by)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (s["grav_uger"], s["mor_uger"], s["faed_uger"],
             s["forl_uger"], s["tvil_uger"], user_id),
        )
    else:
        cur.execute(
            """UPDATE BarselSettings SET
               grav_uger=%s, mor_uger=%s, faed_uger=%s,
               forl_uger=%s, tvil_uger=%s,
               updated_by=%s, updated_at=GETDATE()""",
            (s["grav_uger"], s["mor_uger"], s["faed_uger"],
             s["forl_uger"], s["tvil_uger"], user_id),
        )
    conn.commit()
    conn.close()

# ---------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------

def get_cases(user_id: int, see_all: bool) -> list:
    """Hent barselsager. HR-brugere ser alle sager med opretterens navn."""
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        if see_all:
            cur.execute(
                """SELECT c.*, u.name AS created_by_name
                   FROM BarselCases c
                   LEFT JOIN HubUsers u ON c.created_by = u.id
                   ORDER BY c.created_at DESC"""
            )
        else:
            cur.execute(
                """SELECT c.*, NULL AS created_by_name
                   FROM BarselCases c
                   WHERE c.created_by = %s
                   ORDER BY c.created_at DESC""",
                (user_id,),
            )
        rows = cur.fetchall()
        conn.close()
        return [_row_to_front(r) for r in rows]
    except Exception as e:
        print(f"[get_cases] Fejl: {e}")
        return []


def create_case(data: dict, user_id: int) -> int:
    """Opret ny barselsag. Returnerer det nye ID."""
    d = _front_to_db(data)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO BarselCases
           (mor_navn, far_navn,
            termin, foedsel_dato, foedsel_type,
            mor_uger, faed_uger, forl_uger,
            mor_ferie, far_ferie,
            faed_start, forl_start, created_by)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (d["mor_navn"], d["far_navn"],
         d["termin"], d["foedsel_dato"], d["foedsel_type"],
         d["mor_uger"], d["faed_uger"], d["forl_uger"],
         d["mor_ferie"], d["far_ferie"],
         d["faed_start"], d["forl_start"], user_id),
    )
    cur.execute("SELECT SCOPE_IDENTITY()")
    new_id = int(cur.fetchone()[0])
    conn.commit()
    conn.close()
    return new_id


def update_case(case_id: int, data: dict, user_id: int, see_all: bool):
    """Opdater eksisterende barselsag."""
    d = _front_to_db(data)
    conn = get_conn()
    cur = conn.cursor()
    base_sql = """UPDATE BarselCases SET
               mor_navn=%s, far_navn=%s,
               termin=%s, foedsel_dato=%s, foedsel_type=%s,
               mor_uger=%s, faed_uger=%s, forl_uger=%s,
               mor_ferie=%s, far_ferie=%s,
               faed_start=%s, forl_start=%s, updated_at=GETDATE()"""
    params = (
        d["mor_navn"], d["far_navn"],
        d["termin"], d["foedsel_dato"], d["foedsel_type"],
        d["mor_uger"], d["faed_uger"], d["forl_uger"],
        d["mor_ferie"], d["far_ferie"],
        d["faed_start"], d["forl_start"],
    )
    if see_all:
        cur.execute(base_sql + " WHERE id=%s", params + (case_id,))
    else:
        cur.execute(base_sql + " WHERE id=%s AND created_by=%s", params + (case_id, user_id))
    conn.commit()
    conn.close()


def delete_case(case_id: int, user_id: int, see_all: bool):
    """Slet barselsag."""
    conn = get_conn()
    cur = conn.cursor()
    if see_all:
        cur.execute("DELETE FROM BarselCases WHERE id=%s", (case_id,))
    else:
        cur.execute(
            "DELETE FROM BarselCases WHERE id=%s AND created_by=%s",
            (case_id, user_id),
        )
    conn.commit()
    conn.close()

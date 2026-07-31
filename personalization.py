"""Personalisering af hubben: favoritter og senest besøgte dashboards.

To tabeller, begge nøglet på (bruger, nav-item-id) fra nav_utils.CATEGORIES:
    HubFavorites     — brugerens stjernemarkerede dashboards/tools
    HubRecentVisits  — én række pr. (bruger, item) med tidspunkt for seneste besøg

Besøgene registreres af usage-middleware'en i app.py. Som i usage_tracking
lægges de i en kø, som en baggrundstråd tømmer i batches — en sidevisning må
ikke vente på en DB-skrivning. I modsætning til usage-loggen skrives besøg OGSÅ
i DEV_MODE: uden dem ville "senest besøgt" altid stå tom under udvikling.

Offentligt API:
    init_personalization_db()                  — opret tabeller (idempotent)
    start_visit_worker()                       — start flush-tråden (idempotent)
    record_visit(user_id, path, query)         — kaldes fra middleware
    get_favorite_ids(user_id)                  — {item_id}
    toggle_favorite(user_id, item_id)          — True hvis den nu er favorit
    get_recent_item_ids(user_id, limit)        — [item_id] nyeste først
"""
import atexit
import logging
import queue
import threading
from datetime import datetime

from auth import get_conn
from nav_utils import resolve_item_id

logger = logging.getLogger(__name__)


# ── Skema ────────────────────────────────────────────────────────────────────

def init_personalization_db() -> None:
    """Opret favorit-/besøgstabellerne hvis de mangler. Idempotent."""
    stmts = [
        """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='HubFavorites' AND xtype='U')
           CREATE TABLE HubFavorites (
               id         INT IDENTITY(1,1) PRIMARY KEY,
               user_id    INT           NOT NULL,
               item_id    NVARCHAR(100) NOT NULL,
               created_at DATETIME      NOT NULL DEFAULT GETDATE(),
               CONSTRAINT UQ_HubFavorites UNIQUE (user_id, item_id)
           )""",
        # Én række pr. (bruger, item) — opdateres ved hvert besøg. Tabellen er
        # dermed lille og afgrænset (brugere × menupunkter), i modsætning til
        # HubUsageLog der har én række pr. sidevisning.
        """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='HubRecentVisits' AND xtype='U')
           CREATE TABLE HubRecentVisits (
               id          INT IDENTITY(1,1) PRIMARY KEY,
               user_id     INT           NOT NULL,
               item_id     NVARCHAR(100) NOT NULL,
               visited_at  DATETIME      NOT NULL DEFAULT GETDATE(),
               visit_count INT           NOT NULL DEFAULT 1,
               CONSTRAINT UQ_HubRecentVisits UNIQUE (user_id, item_id)
           )""",
        """IF NOT EXISTS (
               SELECT * FROM sys.indexes
               WHERE name='IX_HubRecentVisits_user' AND object_id = OBJECT_ID('HubRecentVisits')
           )
           CREATE INDEX IX_HubRecentVisits_user ON HubRecentVisits (user_id, visited_at DESC)""",
    ]
    try:
        conn = get_conn()
        cur = conn.cursor()
        for sql in stmts:
            cur.execute(sql)
        conn.commit()
        conn.close()
    except Exception:
        logger.exception("init_personalization_db: kunne ikke oprette tabeller")


# ── Favoritter ───────────────────────────────────────────────────────────────

def get_favorite_ids(user_id) -> list:
    """Brugerens favorit-item-id'er, nyeste markering først. [] ved DB-fejl.

    Rå id'er — kalderen oversætter dem via nav_utils.visible_items_by_id, så et
    item brugeren har mistet adgang til ikke vises.
    """
    # `is None`, ikke falsy: dev-brugeren har id 0.
    if user_id is None:
        return []
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(
            "SELECT item_id FROM HubFavorites WHERE user_id = %s "
            "ORDER BY created_at DESC, id DESC", (user_id,))
        rows = cur.fetchall() or []
        conn.close()
        return [r["item_id"] for r in rows]
    except Exception:
        logger.exception("get_favorite_ids fejlede (user_id=%s)", user_id)
        return []


def toggle_favorite(user_id, item_id: str) -> bool:
    """Slå favorit til/fra. Returnerer True hvis item'et NU er favorit.

    Kalderen skal have valideret at item_id findes og er synligt for brugeren.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM HubFavorites WHERE user_id = %s AND item_id = %s",
                    (user_id, item_id))
        removed = cur.rowcount > 0
        if not removed:
            cur.execute(
                "INSERT INTO HubFavorites (user_id, item_id) VALUES (%s, %s)",
                (user_id, item_id))
        conn.commit()
        return not removed
    finally:
        conn.close()


# ── Senest besøgt ────────────────────────────────────────────────────────────

def get_recent_item_ids(user_id, limit: int = 10) -> list:
    """Brugerens senest besøgte item-id'er, nyeste først. [] ved DB-fejl."""
    if user_id is None:
        return []
    limit = max(1, min(int(limit or 10), 50))
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(
            f"SELECT TOP {limit} item_id FROM HubRecentVisits "
            "WHERE user_id = %s ORDER BY visited_at DESC, id DESC", (user_id,))
        rows = cur.fetchall() or []
        conn.close()
        return [r["item_id"] for r in rows]
    except Exception:
        logger.exception("get_recent_item_ids fejlede (user_id=%s)", user_id)
        return []


# ── Kø + baggrundstråd (besøgsregistrering) ──────────────────────────────────
_MAX_QUEUE = 2000
_q: "queue.Queue[tuple]" = queue.Queue(maxsize=_MAX_QUEUE)
_worker_started = False
_start_lock = threading.Lock()
_stop = threading.Event()


def record_visit(user_id, path: str, query: str = "") -> None:
    """Registrér et besøg (ikke-blokerende; droppes hvis køen er fuld).

    Stier der ikke er et nav-item (forsiden, kategorisider, admin) ignoreres —
    "senest besøgt" skal vise dashboards og tools, ikke navigationssider.
    """
    if user_id is None:
        return
    item_id = resolve_item_id(path, query)
    if not item_id:
        return
    try:
        _q.put_nowait((user_id, item_id, datetime.now()))
    except queue.Full:
        # Hellere tabe et besøg end at blokere requesten — best effort, som usage.
        pass


def _flush_batch(batch: list) -> None:
    """Upsert de nyeste besøg. Batchen dedupliceres først, så gentagne besøg på
    samme side i samme flush kun koster én skrivning."""
    latest: dict = {}
    for user_id, item_id, ts in batch:
        key = (user_id, item_id)
        if key not in latest or ts > latest[key]:
            latest[key] = ts
    if not latest:
        return
    conn = get_conn()
    try:
        cur = conn.cursor()
        for (user_id, item_id), ts in latest.items():
            cur.execute(
                "MERGE HubRecentVisits AS t "
                "USING (SELECT %s AS user_id, %s AS item_id) AS s "
                "  ON t.user_id = s.user_id AND t.item_id = s.item_id "
                "WHEN MATCHED THEN "
                "  UPDATE SET visited_at = %s, visit_count = t.visit_count + 1 "
                "WHEN NOT MATCHED THEN "
                "  INSERT (user_id, item_id, visited_at) VALUES (s.user_id, s.item_id, %s);",
                (user_id, item_id, ts, ts))
        conn.commit()
    finally:
        conn.close()


def _worker() -> None:
    while not _stop.is_set():
        try:
            first = _q.get(timeout=3.0)
        except queue.Empty:
            continue
        batch = [first]
        for _ in range(200):
            try:
                batch.append(_q.get_nowait())
            except queue.Empty:
                break
        try:
            _flush_batch(batch)
        except Exception:
            logger.exception("besøgs-flush fejlede (%d rækker tabt)", len(batch))


def _drain_remaining() -> None:
    _stop.set()
    batch = []
    while True:
        try:
            batch.append(_q.get_nowait())
        except queue.Empty:
            break
    if batch:
        try:
            _flush_batch(batch)
        except Exception:
            pass


def start_visit_worker() -> None:
    """Start flush-tråden (idempotent). Kører også i DEV_MODE, i modsætning til
    usage-loggen: uden besøg ville "senest besøgt" stå tom under udvikling."""
    global _worker_started
    with _start_lock:
        if _worker_started:
            return
        _worker_started = True
    threading.Thread(target=_worker, name="visit-flush", daemon=True).start()
    atexit.register(_drain_remaining)

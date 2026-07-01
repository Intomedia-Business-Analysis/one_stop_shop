"""Usage-tracking for Intomedia Hub.

Logger sidevisninger (HTML-sider) til tabellen HubUsageLog. For ikke at
blokere request-stien med en synkron pymssql-skrivning lægges hver hændelse
i en in-memory kø, som en baggrundstråd tømmer i batches.

Offentligt API:
    record_pageview(user_id, path, status_code, duration_ms)  — kaldes fra middleware
    start_usage_worker()                                       — kaldes én gang ved opstart
    get_usage_dashboard(days)                                  — data til /admin/usage
"""
import os
import atexit
import logging
import queue
import threading
from datetime import datetime, date, timedelta

from auth import get_conn

logger = logging.getLogger(__name__)

_DEV = os.getenv("DEV_MODE") == "1"

# ── Path → pænt label (fra nav-registret) ──────────────────────────────────
def _build_label_map() -> dict:
    labels = {
        "/":             "Hub forside",
        "/settings":     "Indstillinger",
        "/admin/users":  "Brugerstyring",
        "/admin/roles":  "Rollestyring",
        "/admin/teams":  "Holdstyring",
        "/admin/usage":  "Brugsstatistik",
    }
    try:
        from nav_utils import CATEGORIES
        for cat in CATEGORIES:
            # Kategori-oversigtssiderne (/category/<id>) — så de vises med pænt
            # navn i stedet for den rå sti i brugsstatistikken.
            cat_id = cat.get("id")
            if cat_id:
                labels.setdefault(f"/category/{cat_id}", cat.get("title") or f"/category/{cat_id}")
            for item in cat.get("items", []):
                url = item.get("url")
                if url:
                    labels.setdefault(url, item.get("title") or url)
    except Exception:
        pass
    return labels

_LABEL_MAP = _build_label_map()


def resource_label(path: str) -> str:
    """Slå et menneskeligt navn op for en sti — falder tilbage til selve stien."""
    if path in _LABEL_MAP:
        return _LABEL_MAP[path]
    alt = path[:-1] if path.endswith("/") else path + "/"
    return _LABEL_MAP.get(alt, path)


# ── Kø + baggrundstråd ─────────────────────────────────────────────────────
_MAX_QUEUE      = 5000
_q: "queue.Queue[dict]" = queue.Queue(maxsize=_MAX_QUEUE)
_worker_started = False
_start_lock     = threading.Lock()
_stop           = threading.Event()


def record_pageview(user_id, path: str, status_code=None, duration_ms=None) -> None:
    """Læg en sidevisning i køen (ikke-blokerende; droppes ved overbelastning)."""
    if _DEV:
        return
    try:
        _q.put_nowait({
            "user_id":        user_id,
            "path":           (path or "")[:400],
            "resource_label": resource_label(path or "")[:150],
            "method":         "GET",
            "status_code":    status_code,
            "duration_ms":    duration_ms,
            "created_at":     datetime.now(),
        })
    except queue.Full:
        # Hellere tabe en måling end at blokere/ophobe — tracking er best effort.
        pass


def _flush_batch(batch: list) -> None:
    if not batch:
        return
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.executemany(
            "INSERT INTO HubUsageLog "
            "(user_id, path, resource_label, method, status_code, duration_ms, created_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            [(b["user_id"], b["path"], b["resource_label"], b["method"],
              b["status_code"], b["duration_ms"], b["created_at"]) for b in batch],
        )
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
        for _ in range(300):          # saml flere op for batch-insert
            try:
                batch.append(_q.get_nowait())
            except queue.Empty:
                break
        try:
            _flush_batch(batch)
        except Exception:
            logger.exception("usage-flush fejlede (%d rækker tabt)", len(batch))


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


def start_usage_worker() -> None:
    """Start flush-tråden (idempotent). No-op i DEV_MODE."""
    global _worker_started
    if _DEV:
        return
    with _start_lock:
        if _worker_started:
            return
        _worker_started = True
    threading.Thread(target=_worker, name="usage-flush", daemon=True).start()
    atexit.register(_drain_remaining)


# ── Dashboard-data ─────────────────────────────────────────────────────────
def get_usage_dashboard(days: int = 30) -> dict:
    """Saml KPI'er, daglig serie, top-værktøjer og pr-bruger-tabel."""
    today      = date.today()
    since      = today - timedelta(days=days - 1)
    since_dt   = datetime.combine(since, datetime.min.time())
    today_dt   = datetime.combine(today, datetime.min.time())
    week_dt    = datetime.combine(today - timedelta(days=6),  datetime.min.time())
    month_dt   = datetime.combine(today - timedelta(days=29), datetime.min.time())

    conn = get_conn()
    cur  = conn.cursor(as_dict=True)

    def _kpi(since_value):
        cur.execute(
            "SELECT COUNT(*) AS views, COUNT(DISTINCT user_id) AS users "
            "FROM HubUsageLog WHERE created_at >= %s", (since_value,))
        r = cur.fetchone() or {}
        return int(r.get("views") or 0), int(r.get("users") or 0)

    views_today, users_today = _kpi(today_dt)
    views_7d,    users_7d    = _kpi(week_dt)
    views_30d,   users_30d   = _kpi(month_dt)

    # Daglig serie (nul-udfyldt)
    cur.execute(
        "SELECT CAST(created_at AS DATE) AS d, COUNT(*) AS views, "
        "COUNT(DISTINCT user_id) AS users FROM HubUsageLog "
        "WHERE created_at >= %s GROUP BY CAST(created_at AS DATE) ORDER BY d",
        (since_dt,))
    by_day = {str(r["d"]): r for r in cur.fetchall()}
    daily = []
    for i in range(days):
        d = since + timedelta(days=i)
        row = by_day.get(str(d))
        daily.append({
            "date":  d.isoformat(),
            "label": d.strftime("%d/%m"),
            "views": int(row["views"]) if row else 0,
            "users": int(row["users"]) if row else 0,
        })

    # Mest brugte værktøjer/sider
    cur.execute(
        "SELECT TOP 15 resource_label, COUNT(*) AS views, "
        "COUNT(DISTINCT user_id) AS users FROM HubUsageLog "
        "WHERE created_at >= %s GROUP BY resource_label ORDER BY views DESC",
        (month_dt,))
    # Re-map labels ved query-tid: ældre rækker gemte den rå sti (fx
    # /category/...) før label-mappet kendte dem, så vi slår op igen her.
    top_resources = [{
        "label": resource_label(r["resource_label"]) if r["resource_label"] else "—",
        "views": int(r["views"]), "users": int(r["users"]),
    } for r in cur.fetchall()]

    # Pr. bruger
    cur.execute(
        "SELECT TOP 100 l.user_id, hu.name, hu.initials, hu.role, "
        "COUNT(*) AS views, MAX(l.created_at) AS last_seen "
        "FROM HubUsageLog l LEFT JOIN HubUsers hu ON hu.id = l.user_id "
        "WHERE l.created_at >= %s "
        "GROUP BY l.user_id, hu.name, hu.initials, hu.role ORDER BY views DESC",
        (month_dt,))
    per_user = [{
        "user_id":   r["user_id"],
        "name":      r["name"] or "(ukendt / udlogget)",
        "initials":  r["initials"] or "?",
        "role":      r["role"] or "",
        "views":     int(r["views"]),
        "last_seen": r["last_seen"].strftime("%d/%m %H:%M") if r["last_seen"] else "—",
    } for r in cur.fetchall()]

    # Top-5 mest brugte sider PR. bruger (samme 30-dages vindue som per_user).
    # ROW_NUMBER pr. bruger giver hver bruger sine 5 hyppigste ressourcer, så
    # admin kan folde en bruger ud og se hvad netop dén bruger bruger mest.
    top_by_user: dict = {}
    cur.execute(
        "SELECT user_id, resource_label, views FROM ("
        "  SELECT user_id, resource_label, COUNT(*) AS views, "
        "         ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY COUNT(*) DESC, resource_label) AS rn "
        "  FROM HubUsageLog WHERE created_at >= %s GROUP BY user_id, resource_label"
        ") t WHERE rn <= 5 ORDER BY user_id, views DESC",
        (month_dt,))
    for r in cur.fetchall():
        top_by_user.setdefault(r["user_id"], []).append(
            {"label": resource_label(r["resource_label"]) if r["resource_label"] else "—",
             "views": int(r["views"])})
    for u in per_user:
        u["top"] = top_by_user.get(u["user_id"], [])

    conn.close()

    max_daily = max((d["views"] for d in daily), default=0)
    return {
        "days":          days,
        "views_today":   views_today,  "users_today": users_today,
        "views_7d":      views_7d,     "users_7d":    users_7d,
        "views_30d":     views_30d,    "users_30d":   users_30d,
        "daily":         daily,
        "max_daily":     max_daily,
        "top_resources": top_resources,
        "per_user":      per_user,
    }

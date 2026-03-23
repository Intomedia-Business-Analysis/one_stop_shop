import os
import traceback

import pymssql
from dotenv import load_dotenv
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from auth import ROLE_LABELS, get_current_user, get_user_resource_access, hash_password, init_db

load_dotenv()

router = APIRouter(prefix="/admin", tags=["Admin"])
templates = Jinja2Templates(directory="templates")
templates.env.globals["ROLE_LABELS"] = ROLE_LABELS

ROLES = [
    ("salesperson",      "Sælger"),
    ("sales_manager",    "Sales Manager"),
    ("sales_operations", "Sales Operations"),
    ("marketing",        "Marketing"),
    ("management",       "Management"),
    ("admin",            "Admin"),
]

BRANDS = [
    ("",           "Alle brands (tværgående hold)"),
    ("watch_dk",   "Watch DK"),
    ("watch_int",  "Watch INT"),
    ("watch_no",   "Watch NO"),
    ("watch_se",   "Watch SE"),
    ("watch_de",   "Watch DE"),
    ("finans",     "FINANS DK"),
    ("finans_int", "FINANS Int"),
    ("marketwire", "MarketWire"),
    ("monitor",    "Monitor"),
]


def group_users(users: list) -> list:
    """
    Returns users split into display groups:
    - One group per brand for salesperson + sales_manager roles
    - One standalone group per other role (sales_operations, marketing, management, admin)
    Sorted: brand groups first (alphabetically by label), then other roles in hierarchy order.
    """
    brand_labels = dict(BRANDS)
    role_labels_dict = dict(ROLES)

    brand_groups: dict = {}  # brand_value -> [users]
    role_groups: dict = {}   # role -> [users]

    for u in users:
        if u["role"] in ("salesperson", "sales_manager"):
            b = u.get("brand") or ""
            brand_groups.setdefault(b, []).append(u)
        else:
            role_groups.setdefault(u["role"], []).append(u)

    groups = []

    # Brand groups — sort by human-readable label; empty brand last
    for bval in sorted(brand_groups.keys(), key=lambda b: brand_labels.get(b, "zzz") if b else "zzz"):
        blabel = brand_labels.get(bval, bval) if bval else "Sælgere uden brand"
        groups.append({"title": blabel, "kind": "brand", "users": brand_groups[bval]})

    # Other roles in hierarchy order
    for role in ("sales_operations", "marketing", "management", "admin"):
        if role in role_groups:
            groups.append({"title": role_labels_dict.get(role, role), "kind": "role", "users": role_groups[role]})

    # Any unexpected extra roles
    handled = {"salesperson", "sales_manager", "sales_operations", "marketing", "management", "admin"}
    for role, usr in role_groups.items():
        if role not in handled:
            groups.append({"title": role_labels_dict.get(role, role), "kind": "role", "users": usr})

    return groups


def get_conn():
    return pymssql.connect(
        server=os.getenv("DB_SERVER"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME", "INTOMEDIA"),
        tds_version="7.0",
        login_timeout=5,
        timeout=5,
    )


def require_admin(user: dict):
    if not user or user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Kun admins har adgang til denne side")
    return user


# ---------------------------------------------------------------------------
# DB init (manuelt, hvis init_db() fejlede ved opstart pga. DB nede)
# ---------------------------------------------------------------------------

@router.post("/db-init")
async def admin_db_init(request: Request, user=Depends(get_current_user)):
    require_admin(user)
    try:
        init_db()
        return RedirectResponse("/admin/users?success=db_init", status_code=302)
    except Exception:
        return RedirectResponse("/admin/users?error=db_init_failed", status_code=302)


# ---------------------------------------------------------------------------
# User list
# ---------------------------------------------------------------------------

@router.get("/users", response_class=HTMLResponse)
async def admin_users(request: Request, user=Depends(get_current_user)):
    require_admin(user)
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(
            "SELECT id, username, name, initials, role, brand, is_active, created_at "
            "FROM HubUsers ORDER BY name"
        )
        users = cur.fetchall()
        conn.close()
    except Exception:
        users = []

    return templates.TemplateResponse("admin_users.html", {
        "request":  request,
        "user":     user,
        "users":    users,
        "groups":   group_users(users),
        "roles":    ROLES,
        "brands":   BRANDS,
        "success":  request.query_params.get("success"),
        "error":    request.query_params.get("error"),
    })


# ---------------------------------------------------------------------------
# Create user
# ---------------------------------------------------------------------------

@router.post("/users/create")
async def admin_create_user(request: Request, user=Depends(get_current_user)):
    require_admin(user)
    form = await request.form()
    username = form.get("username", "").strip()
    name     = form.get("name", "").strip()
    initials = form.get("initials", "").strip().upper()
    role     = form.get("role", "salesperson")
    brand    = form.get("brand", "") or None
    password = form.get("password", "").strip()

    if not all([username, name, initials, role, password]):
        return RedirectResponse("/admin/users?error=missing_fields", status_code=302)

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO HubUsers (username, password_hash, name, initials, role, brand, is_active) "
            "VALUES (%s, %s, %s, %s, %s, %s, 1)",
            (username, hash_password(password), name, initials, role, brand),
        )
        conn.commit()
        conn.close()
    except Exception:
        print(traceback.format_exc())
        return RedirectResponse("/admin/users?error=username_taken", status_code=302)

    return RedirectResponse("/admin/users?success=created", status_code=302)


# ---------------------------------------------------------------------------
# Edit user — form page
# ---------------------------------------------------------------------------

@router.get("/users/{user_id}/edit", response_class=HTMLResponse)
async def admin_edit_page(user_id: int, request: Request, user=Depends(get_current_user)):
    require_admin(user)

    # Lazy import to avoid circular dependency (app imports admin_router)
    from app import CATEGORIES

    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute("SELECT * FROM HubUsers WHERE id = %s", (user_id,))
        target = cur.fetchone()
    except Exception:
        target = None
        conn = None

    if not target:
        if conn:
            conn.close()
        raise HTTPException(status_code=404, detail="Bruger ikke fundet")

    # Resource access overrides for this user
    resource_access = get_user_resource_access(user_id)

    # Team memberships
    try:
        cur.execute(
            """
            SELECT tm.id, tm.team_id, tm.role, tm.start_date, tm.end_date, tm.notes,
                   t.name AS team_name, t.brand AS team_brand
            FROM   TeamMemberships tm
            JOIN   Teams t ON t.id = tm.team_id
            WHERE  tm.user_id = %s
            ORDER  BY tm.start_date DESC
            """,
            (user_id,),
        )
        memberships = cur.fetchall()
        cur.execute("SELECT id, name, brand FROM Teams ORDER BY name")
        all_teams = cur.fetchall()
    except Exception:
        memberships = []
        all_teams = []
    finally:
        conn.close()

    return templates.TemplateResponse("admin_edit_user.html", {
        "request":         request,
        "user":            user,
        "target":          target,
        "roles":           ROLES,
        "brands":          BRANDS,
        "categories":      CATEGORIES,
        "resource_access": resource_access,
        "memberships":     memberships,
        "all_teams":       all_teams,
        "success":         request.query_params.get("success"),
        "error":           request.query_params.get("error"),
    })


# ---------------------------------------------------------------------------
# Update user
# ---------------------------------------------------------------------------

@router.post("/users/{user_id}/update")
async def admin_update_user(user_id: int, request: Request, user=Depends(get_current_user)):
    require_admin(user)
    form     = await request.form()
    name     = form.get("name", "").strip()
    initials = form.get("initials", "").strip().upper()
    role     = form.get("role", "salesperson")
    brand    = form.get("brand", "") or None
    is_active = 1 if form.get("is_active") else 0
    new_pw   = form.get("password", "").strip()

    try:
        conn = get_conn()
        cur = conn.cursor()
        if new_pw:
            cur.execute(
                "UPDATE HubUsers SET name=%s, initials=%s, role=%s, brand=%s, "
                "is_active=%s, password_hash=%s WHERE id=%s",
                (name, initials, role, brand, is_active, hash_password(new_pw), user_id),
            )
        else:
            cur.execute(
                "UPDATE HubUsers SET name=%s, initials=%s, role=%s, brand=%s, "
                "is_active=%s WHERE id=%s",
                (name, initials, role, brand, is_active, user_id),
            )
        conn.commit()
        conn.close()
    except Exception:
        print(traceback.format_exc())

    return RedirectResponse(f"/admin/users/{user_id}/edit?success=updated", status_code=302)


# ---------------------------------------------------------------------------
# Resource access overrides (per bruger, per ressource)
# ---------------------------------------------------------------------------

@router.post("/users/{user_id}/resource-access")
async def admin_save_resource_access(user_id: int, request: Request, user=Depends(get_current_user)):
    require_admin(user)
    form = await request.form()

    from app import CATEGORIES
    all_resource_ids = [item["id"] for cat in CATEGORIES for item in cat["items"]]

    try:
        conn = get_conn()
        cur = conn.cursor()
        for rid in all_resource_ids:
            val = form.get(f"access_{rid}", "default")
            # Slet eksisterende override for denne ressource
            cur.execute(
                "DELETE FROM UserResourceAccess WHERE user_id=%s AND resource_id=%s",
                (user_id, rid),
            )
            if val != "default":
                cur.execute(
                    "INSERT INTO UserResourceAccess (user_id, resource_id, access) VALUES (%s, %s, %s)",
                    (user_id, rid, val),
                )
        conn.commit()
        conn.close()
    except Exception:
        print(traceback.format_exc())

    return RedirectResponse(f"/admin/users/{user_id}/edit?success=access_updated", status_code=302)


# ---------------------------------------------------------------------------
# Team memberships (fra brugersiden)
# ---------------------------------------------------------------------------

@router.post("/users/{user_id}/memberships/add")
async def admin_add_user_membership(user_id: int, request: Request, user=Depends(get_current_user)):
    require_admin(user)
    form       = await request.form()
    team_id    = form.get("team_id")
    role       = form.get("role", "member")
    start_date = form.get("start_date", "").strip()
    end_date   = form.get("end_date", "").strip() or None
    notes      = form.get("notes", "").strip() or None

    if not (team_id and start_date):
        return RedirectResponse(f"/admin/users/{user_id}/edit?error=missing_fields", status_code=302)

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO TeamMemberships (user_id, team_id, role, start_date, end_date, notes) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (user_id, int(team_id), role, start_date, end_date, notes),
        )
        conn.commit()
        conn.close()
    except Exception:
        print(traceback.format_exc())
        return RedirectResponse(f"/admin/users/{user_id}/edit?error=db_error", status_code=302)

    return RedirectResponse(f"/admin/users/{user_id}/edit?success=member_added", status_code=302)


@router.post("/users/{user_id}/memberships/{membership_id}/remove")
async def admin_remove_user_membership(
    user_id: int, membership_id: int, request: Request, user=Depends(get_current_user)
):
    require_admin(user)
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM TeamMemberships WHERE id=%s AND user_id=%s",
            (membership_id, user_id),
        )
        conn.commit()
        conn.close()
    except Exception:
        print(traceback.format_exc())

    return RedirectResponse(f"/admin/users/{user_id}/edit?success=member_removed", status_code=302)


# ---------------------------------------------------------------------------
# Teams — liste og opret
# ---------------------------------------------------------------------------

@router.get("/teams", response_class=HTMLResponse)
async def admin_teams_list(request: Request, user=Depends(get_current_user)):
    require_admin(user)
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(
            """
            SELECT t.id, t.name, t.brand, t.description, t.created_at,
                   COUNT(DISTINCT tm.id) AS member_count
            FROM   Teams t
            LEFT JOIN TeamMemberships tm ON tm.team_id = t.id
            GROUP  BY t.id, t.name, t.brand, t.description, t.created_at
            ORDER  BY t.name
            """
        )
        teams = cur.fetchall()
        conn.close()
    except Exception:
        print(traceback.format_exc())
        teams = []

    return templates.TemplateResponse("admin_teams.html", {
        "request": request,
        "user":    user,
        "teams":   teams,
        "brands":  BRANDS,
        "success": request.query_params.get("success"),
        "error":   request.query_params.get("error"),
    })


@router.post("/teams/create")
async def admin_create_team(request: Request, user=Depends(get_current_user)):
    require_admin(user)
    form        = await request.form()
    name        = form.get("name", "").strip()
    brand       = form.get("brand", "") or None
    description = form.get("description", "").strip() or None

    if not name:
        return RedirectResponse("/admin/teams?error=missing_name", status_code=302)

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO Teams (name, brand, description) VALUES (%s, %s, %s)",
            (name, brand, description),
        )
        conn.commit()
        conn.close()
    except Exception:
        print(traceback.format_exc())
        return RedirectResponse("/admin/teams?error=db_error", status_code=302)

    return RedirectResponse("/admin/teams?success=created", status_code=302)


# ---------------------------------------------------------------------------
# Teams — detalje, rediger og medlemskaber
# ---------------------------------------------------------------------------

@router.get("/teams/{team_id}", response_class=HTMLResponse)
async def admin_edit_team(team_id: int, request: Request, user=Depends(get_current_user)):
    require_admin(user)
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute("SELECT * FROM Teams WHERE id = %s", (team_id,))
        team = cur.fetchone()
        if not team:
            conn.close()
            raise HTTPException(status_code=404, detail="Hold ikke fundet")
        cur.execute(
            """
            SELECT tm.id, tm.user_id, tm.role, tm.start_date, tm.end_date, tm.notes,
                   u.name AS user_name, u.initials, u.username
            FROM   TeamMemberships tm
            JOIN   HubUsers u ON u.id = tm.user_id
            WHERE  tm.team_id = %s
            ORDER  BY tm.start_date DESC
            """,
            (team_id,),
        )
        memberships = cur.fetchall()
        cur.execute(
            "SELECT id, name, initials FROM HubUsers WHERE is_active = 1 ORDER BY name"
        )
        all_users = cur.fetchall()
        conn.close()
    except HTTPException:
        raise
    except Exception:
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Databasefejl")

    return templates.TemplateResponse("admin_edit_team.html", {
        "request":     request,
        "user":        user,
        "team":        team,
        "memberships": memberships,
        "all_users":   all_users,
        "brands":      BRANDS,
        "success":     request.query_params.get("success"),
        "error":       request.query_params.get("error"),
    })


@router.post("/teams/{team_id}/update")
async def admin_update_team(team_id: int, request: Request, user=Depends(get_current_user)):
    require_admin(user)
    form        = await request.form()
    name        = form.get("name", "").strip()
    brand       = form.get("brand", "") or None
    description = form.get("description", "").strip() or None

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "UPDATE Teams SET name=%s, brand=%s, description=%s WHERE id=%s",
            (name, brand, description, team_id),
        )
        conn.commit()
        conn.close()
    except Exception:
        print(traceback.format_exc())

    return RedirectResponse(f"/admin/teams/{team_id}?success=updated", status_code=302)


@router.post("/teams/{team_id}/memberships/add")
async def admin_add_team_membership(team_id: int, request: Request, user=Depends(get_current_user)):
    require_admin(user)
    form       = await request.form()
    user_id    = form.get("user_id")
    role       = form.get("role", "member")
    start_date = form.get("start_date", "").strip()
    end_date   = form.get("end_date", "").strip() or None
    notes      = form.get("notes", "").strip() or None

    if not (user_id and start_date):
        return RedirectResponse(f"/admin/teams/{team_id}?error=missing_fields", status_code=302)

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO TeamMemberships (user_id, team_id, role, start_date, end_date, notes) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (int(user_id), team_id, role, start_date, end_date, notes),
        )
        conn.commit()
        conn.close()
    except Exception:
        print(traceback.format_exc())
        return RedirectResponse(f"/admin/teams/{team_id}?error=db_error", status_code=302)

    return RedirectResponse(f"/admin/teams/{team_id}?success=member_added", status_code=302)


@router.post("/teams/{team_id}/memberships/{membership_id}/update")
async def admin_update_team_membership(
    team_id: int, membership_id: int, request: Request, user=Depends(get_current_user)
):
    require_admin(user)
    form       = await request.form()
    role       = form.get("role", "member")
    start_date = form.get("start_date", "").strip()
    end_date   = form.get("end_date", "").strip() or None
    notes      = form.get("notes", "").strip() or None

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "UPDATE TeamMemberships SET role=%s, start_date=%s, end_date=%s, notes=%s "
            "WHERE id=%s AND team_id=%s",
            (role, start_date, end_date, notes, membership_id, team_id),
        )
        conn.commit()
        conn.close()
    except Exception:
        print(traceback.format_exc())

    return RedirectResponse(f"/admin/teams/{team_id}?success=updated", status_code=302)


@router.post("/teams/{team_id}/memberships/{membership_id}/remove")
async def admin_remove_team_membership(
    team_id: int, membership_id: int, request: Request, user=Depends(get_current_user)
):
    require_admin(user)
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM TeamMemberships WHERE id=%s AND team_id=%s",
            (membership_id, team_id),
        )
        conn.commit()
        conn.close()
    except Exception:
        print(traceback.format_exc())

    return RedirectResponse(f"/admin/teams/{team_id}?success=member_removed", status_code=302)

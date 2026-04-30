import traceback

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from auth import ROLE_LABELS, get_current_user, get_user_resource_access, hash_password, init_db

from moduler.modul_admin.queries import (
    db_get_all_users, db_create_user, db_get_user_by_id, db_update_user,
    db_get_user_memberships, db_add_membership, db_remove_membership,
    db_save_resource_access, db_get_all_teams, db_create_team,
    db_get_team_by_id, db_update_team, db_update_membership,
)

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
    brand_labels = dict(BRANDS)
    role_labels_dict = dict(ROLES)
    brand_groups: dict = {}
    role_groups: dict = {}

    for u in users:
        if u["role"] in ("salesperson", "sales_manager"):
            b = u.get("brand") or ""
            brand_groups.setdefault(b, []).append(u)
        else:
            role_groups.setdefault(u["role"], []).append(u)

    groups = []
    for bval in sorted(brand_groups.keys(), key=lambda b: brand_labels.get(b, "zzz") if b else "zzz"):
        blabel = brand_labels.get(bval, bval) if bval else "Sælgere uden brand"
        groups.append({"title": blabel, "kind": "brand", "users": brand_groups[bval]})

    for role in ("sales_operations", "marketing", "management", "admin"):
        if role in role_groups:
            groups.append({"title": role_labels_dict.get(role, role), "kind": "role", "users": role_groups[role]})

    handled = {"salesperson", "sales_manager", "sales_operations", "marketing", "management", "admin"}
    for role, usr in role_groups.items():
        if role not in handled:
            groups.append({"title": role_labels_dict.get(role, role), "kind": "role", "users": usr})

    return groups


def require_admin(user: dict):
    if not user or user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Kun admins har adgang til denne side")
    return user


# ---------------------------------------------------------------------------
# DB init
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
    users = db_get_all_users()
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
    form     = await request.form()
    username = form.get("username", "").strip()
    name     = form.get("name", "").strip()
    initials = form.get("initials", "").strip().upper()
    role     = form.get("role", "salesperson")
    brand    = form.get("brand", "") or None
    password = form.get("password", "").strip()

    if not all([username, name, initials, role, password]):
        return RedirectResponse("/admin/users?error=missing_fields", status_code=302)

    try:
        db_create_user(username, hash_password(password), name, initials, role, brand)
    except Exception:
        print(traceback.format_exc())
        return RedirectResponse("/admin/users?error=username_taken", status_code=302)

    return RedirectResponse("/admin/users?success=created", status_code=302)


# ---------------------------------------------------------------------------
# Edit user
# ---------------------------------------------------------------------------

@router.get("/users/{user_id}/edit", response_class=HTMLResponse)
async def admin_edit_page(user_id: int, request: Request, user=Depends(get_current_user)):
    require_admin(user)
    from app import CATEGORIES

    target = db_get_user_by_id(user_id)
    if not target:
        raise HTTPException(status_code=404, detail="Bruger ikke fundet")

    resource_access = get_user_resource_access(user_id)

    try:
        memberships, all_teams = db_get_user_memberships(user_id)
    except Exception:
        memberships, all_teams = [], []

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
    form      = await request.form()
    name      = form.get("name", "").strip()
    initials  = form.get("initials", "").strip().upper()
    role      = form.get("role", "salesperson")
    brand     = form.get("brand", "") or None
    is_active = 1 if form.get("is_active") else 0
    new_pw    = form.get("password", "").strip()

    try:
        db_update_user(
            user_id, name, initials, role, brand, is_active,
            password_hash=hash_password(new_pw) if new_pw else None
        )
    except Exception:
        print(traceback.format_exc())

    return RedirectResponse(f"/admin/users/{user_id}/edit?success=updated", status_code=302)


# ---------------------------------------------------------------------------
# Resource access
# ---------------------------------------------------------------------------

@router.post("/users/{user_id}/resource-access")
async def admin_save_resource_access(user_id: int, request: Request, user=Depends(get_current_user)):
    require_admin(user)
    form = await request.form()
    from app import CATEGORIES
    all_resource_ids = [item["id"] for cat in CATEGORIES for item in cat["items"]]

    try:
        db_save_resource_access(user_id, all_resource_ids, form)
    except Exception:
        print(traceback.format_exc())

    return RedirectResponse(f"/admin/users/{user_id}/edit?success=access_updated", status_code=302)


# ---------------------------------------------------------------------------
# User memberships
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
        db_add_membership(user_id, int(team_id), role, start_date, end_date, notes)
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
        db_remove_membership(membership_id, user_id=user_id)
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
        teams = db_get_all_teams()
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
        db_create_team(name, brand, description)
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
        team, memberships, all_users = db_get_team_by_id(team_id)
        if not team:
            raise HTTPException(status_code=404, detail="Hold ikke fundet")
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
        db_update_team(team_id, name, brand, description)
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
        db_add_membership(int(user_id), team_id, role, start_date, end_date, notes)
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
        db_update_membership(membership_id, team_id, role, start_date, end_date, notes)
    except Exception:
        print(traceback.format_exc())

    return RedirectResponse(f"/admin/teams/{team_id}?success=updated", status_code=302)


@router.post("/teams/{team_id}/memberships/{membership_id}/remove")
async def admin_remove_team_membership(
    team_id: int, membership_id: int, request: Request, user=Depends(get_current_user)
):
    require_admin(user)
    try:
        db_remove_membership(membership_id, team_id=team_id)
    except Exception:
        print(traceback.format_exc())

    return RedirectResponse(f"/admin/teams/{team_id}?success=member_removed", status_code=302)
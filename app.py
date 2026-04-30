import json
import os

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from typing import Optional

from auth import (
    ROLE_LABELS,
    ROLE_RANK,
    RequiresLoginException,
    authenticate_user,
    get_current_user,
    has_access,
    resolve_resource_access,
    init_db,
)
from moduler.modul_budget.router import router as budget_router
from moduler.modul_admin.router import router as admin_router
from moduler.modul_forcast.router import router as forecast_router
from moduler.modul_perf.router import router as perf_router
from moduler.modul_barsel.router import router as barsel_router
from moduler.modul_barsel.queries import init_barsel_db
from moduler.modul_banner_job.router import router as banner_job_router
from moduler.modul_portfolio_alignment.router import router as portfolio_alignment_router

load_dotenv()

if os.getenv("DEV_MODE") == "1":
    print("[DEV] DEV_MODE=1 — login og SQL-forbindelse er bypassed")

app = FastAPI(title="Intomedia Hub")
init_db()         # Opret hub-tabeller ved opstart (idempotent)
init_barsel_db()  # Opret barseltabeller ved opstart (idempotent)
app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("SECRET_KEY", "skift-denne-noegle"),
)
app.include_router(budget_router)
app.include_router(admin_router)
app.include_router(forecast_router)
app.include_router(perf_router)
app.include_router(barsel_router)
app.include_router(banner_job_router)
app.include_router(portfolio_alignment_router)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")
templates.env.globals["ROLE_LABELS"] = ROLE_LABELS

# ---------------------------------------------------------------------------
# Exception handlers
# ---------------------------------------------------------------------------

@app.exception_handler(RequiresLoginException)
async def requires_login_handler(request: Request, exc: RequiresLoginException):
    return RedirectResponse(url="/login", status_code=302)


# ---------------------------------------------------------------------------
# Tool & Dashboard Registry
# ---------------------------------------------------------------------------

CATEGORIES = [
    {
        "id": "kpi-dashboards",
        "title": "KPI'er og Dashboards",
        "description": "Personlige og team-baserede performance dashboards",
        "icon": "activity",
        "color": "green",
        "min_role": "salesperson",
        "subcategories": [],
        "items": [
            {"id": "kpi-saelger",       "title": "Sælger Dashboard",        "type": "dashboard", "subcategory": None, "brand": None, "min_role": "salesperson",   "url": "/tools/performance/saelger"},
            {"id": "kpi-manager",       "title": "Manager Dashboard",       "type": "dashboard", "subcategory": None, "brand": None, "min_role": "sales_manager", "url": "/tools/performance/manager"},
            {"id": "kpi-afdelingsleder","title": "Afdelingsleder Dashboard", "type": "dashboard", "subcategory": None, "brand": None, "min_role": "management",    "url": "/tools/performance/afdelingsleder"},
    ],
},

    {
        "id": "sales-operations",
        "title": "Sales Operations",
        "description": "Budget og forecast",
        "icon": "settings",
        "color": "amber",
        "min_role": "sales_manager",
        "subcategories": [
            {"id": "budget",    "title": "Budget",    "description": "Budget upload og dashboard",       "brand": None, "min_role": "sales_operations"},
            {"id": "forecast",  "title": "Forecast",  "description": "Salgsprognoser",                   "brand": None, "min_role": "sales_manager"},
            {"id": "alignment", "title": "Alignment", "description": "Pipedrive vs. Zuora ACV-kontrol",  "brand": None, "min_role": "sales_operations"},
        ],
        "items": [
            {"id": "budget-upload-tool",      "title": "Budget",              "type": "tool",      "subcategory": "budget",    "brand": None, "min_role": "sales_operations", "url": "/tools/budget/"},
            {"id": "forecast-tool",           "title": "Forecast",            "type": "tool",      "subcategory": "forecast",  "brand": None, "min_role": "sales_manager",    "url": "/tools/forecast/"},
            {"id": "portfolio-alignment",     "title": "Portfolio Alignment", "type": "dashboard", "subcategory": "alignment", "brand": None, "min_role": "sales_operations", "url": "/tools/portfolio-alignment/"},
        ],
    },

    {
        "id": "banner-job",
        "title": "Banner & Job",
        "description": "Kunde-dashboards for Banner og Job pipeline",
        "icon": "activity",
        "color": "green",
        "min_role": "salesperson",
        "subcategories": [],
        "items": [
            {"id": "banner-job-dashboard", "title": "Banner & Job Dashboard", "type": "dashboard", "subcategory": None, "brand": None, "min_role": "salesperson", "url": "/tools/banner-job/"},
        ],
    },

    {
        "id": "hr",
        "title": "HR",
        "description": "HR-værktøjer",
        "icon": "users",
        "color": "green",
        "min_role": "management",
        "subcategories": [],
        "items": [
            {"id": "barselsberegner", "title": "Barselsplanlægger", "type": "tool", "subcategory": None, "brand": None, "min_role": "management", "url": "/tool/barselsberegner"},
        ],
    },
]


def filter_categories(categories: list, user: dict) -> list:
    result = []
    for cat in categories:
        if not has_access(user, cat["min_role"]):
            continue
        visible_items = []
        for item in cat["items"]:
            access = resolve_resource_access(user, item["id"], item["min_role"], item.get("brand"))
            if access != "none":
                visible_items.append({**item, "access": access})
        visible_subs = [
            sub for sub in cat.get("subcategories", [])
            if has_access(user, sub["min_role"], sub.get("brand"))
        ]
        dashboard_count = sum(1 for i in visible_items if i["type"] == "dashboard")
        tool_count      = sum(1 for i in visible_items if i["type"] == "tool")
        result.append({
            **cat,
            "items":           visible_items,
            "subcategories":   visible_subs,
            "dashboard_count": dashboard_count,
            "tool_count":      tool_count,
        })
    return result


# ---------------------------------------------------------------------------
# Login / Logout
# ---------------------------------------------------------------------------

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    if os.getenv("DEV_MODE") == "1":
        return RedirectResponse("/", status_code=302)
    user_id = request.session.get("user_id")
    if user_id:
        from auth import get_user_by_id
        if get_user_by_id(user_id):
            return RedirectResponse("/", status_code=302)
        # Forældet session (DB nede eller bruger slettet) — ryd op
        request.session.clear()
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@app.post("/login", response_class=HTMLResponse)
async def login_post(request: Request):
    form = await request.form()
    username = form.get("username", "").strip()
    password = form.get("password", "")
    user = authenticate_user(username, password)
    if not user:
        return templates.TemplateResponse("login.html", {
            "request": request,
            "error": "Forkert brugernavn eller adgangskode",
        })
    request.session["user_id"] = user["id"]
    return RedirectResponse("/", status_code=302)


@app.post("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=302)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/intomedia")
async def intomedia_redirect():
    return RedirectResponse("/", status_code=301)


@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request, user=Depends(get_current_user)):
    return templates.TemplateResponse("settings.html", {
        "request": request,
        "user":    user,
    })


@app.post("/settings/change-password")
async def settings_change_password(request: Request, user=Depends(get_current_user)):
    from auth import get_conn as auth_get_conn, verify_password, hash_password
    form            = await request.form()
    current_pw      = form.get("current_password", "")
    new_pw          = form.get("new_password", "").strip()
    confirm_pw      = form.get("confirm_password", "").strip()

    if not all([current_pw, new_pw, confirm_pw]):
        return RedirectResponse("/settings?error=missing_fields", status_code=302)
    if new_pw != confirm_pw:
        return RedirectResponse("/settings?error=pw_mismatch", status_code=302)
    if not verify_password(current_pw, user["password_hash"]):
        return RedirectResponse("/settings?error=pw_wrong", status_code=302)

    try:
        conn = auth_get_conn()
        cur  = conn.cursor()
        cur.execute(
            "UPDATE HubUsers SET password_hash=%s WHERE id=%s",
            (hash_password(new_pw), user["id"]),
        )
        conn.commit()
        conn.close()
    except Exception:
        import traceback
        traceback.print_exc()

    return RedirectResponse("/settings?success=pw_changed", status_code=302)


@app.get("/dashboard/budget", response_class=HTMLResponse)
async def budget_dashboard(request: Request, user=Depends(get_current_user)):
    return templates.TemplateResponse("budget_tool.html", {
        "request": request,
        "user": user,
    })


@app.get("/", response_class=HTMLResponse)
async def hub(request: Request, user=Depends(get_current_user)):
    categories   = filter_categories(CATEGORIES, user)
    total_dash   = sum(c["dashboard_count"] for c in categories)
    total_tools  = sum(c["tool_count"]      for c in categories)
    search_index = []
    for cat in categories:
        for item in cat["items"]:
            search_index.append({
                "id":       item["id"],
                "title":    item["title"],
                "type":     item["type"],
                "category": cat["title"],
                "url":      item["url"],
            })
    return templates.TemplateResponse("hub.html", {
        "request":      request,
        "user":         user,
        "categories":   categories,
        "total_dash":   total_dash,
        "total_tools":  total_tools,
        "cat_count":    len(categories),
        "search_index": json.dumps(search_index),
    })


@app.get("/category/{cat_id}", response_class=HTMLResponse)
async def category_detail(cat_id: str, request: Request, user=Depends(get_current_user)):
    all_cats = filter_categories(CATEGORIES, user)
    cat = next((c for c in all_cats if c["id"] == cat_id), None)
    if not cat:
        raise HTTPException(status_code=404, detail="Kategori ikke fundet eller ingen adgang")
    subs = {}
    for item in cat["items"]:
        key = item["subcategory"] or "Generelt"
        subs.setdefault(key, []).append(item)
    return templates.TemplateResponse("category.html", {
        "request":    request,
        "user":       user,
        "categories": all_cats,
        "cat":        cat,
        "subs":       subs,
        "total_db":   sum(1 for i in cat["items"] if i["type"] == "dashboard"),
        "total_t":    sum(1 for i in cat["items"] if i["type"] == "tool"),
    })


@app.get("/dashboard/{dashboard_id}", response_class=HTMLResponse)
async def dashboard_view(dashboard_id: str, request: Request, user=Depends(get_current_user)):
    return HTMLResponse(f"<h2>Dashboard: {dashboard_id}</h2><p>Bruger: {user['name']} ({user['role']})</p><a href='/'>← Hub</a>")


@app.get("/tool/barselsberegner", response_class=HTMLResponse)
async def barselsberegner_view(request: Request, user=Depends(get_current_user)):
    categories = filter_categories(CATEGORIES, user)
    return templates.TemplateResponse("tool_barselsberegner.html", {
        "request":    request,
        "user":       user,
        "categories": categories,
    })


@app.get("/tool/barselsberegner/app", response_class=HTMLResponse)
async def barselsberegner_app(request: Request, user=Depends(get_current_user)):
    """Serverer selve beregner-appen i en iframe (kræver login)."""
    see_all = user["role"] == "admin"
    return templates.TemplateResponse("barselsberegner_app.html", {
        "request": request,
        "user":    user,
        "see_all": see_all,
    })


@app.get("/tool/{tool_id}", response_class=HTMLResponse)
async def tool_view(tool_id: str, request: Request, user=Depends(get_current_user)):
    return HTMLResponse(f"<h2>Tool: {tool_id}</h2><p>Bruger: {user['name']} ({user['role']})</p><a href='/'>← Hub</a>")


@app.get("/api/search")
async def search_api(q: str, user=Depends(get_current_user)):
    results = []
    for cat in filter_categories(CATEGORIES, user):
        for item in cat["items"]:
            if q.lower() in item["title"].lower():
                results.append({**item, "category": cat["title"]})
    return {"results": results[:10]}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False)
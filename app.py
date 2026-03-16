from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from typing import Optional
import json

from budget_router import router as budget_router

app = FastAPI(title="Intomedia Hub")
app.include_router(budget_router)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ---------------------------------------------------------------------------
# RBAC — Roles
# ---------------------------------------------------------------------------
# Roles (lav til høj adgang):
#   "team"       → Ser kun eget brand-team
#   "management" → Ser alt på tværs, ingen redigering
#   "admin"      → Fuld adgang + brugerstyring

# ---------------------------------------------------------------------------
# Tool & Dashboard Registry
# ---------------------------------------------------------------------------
CATEGORIES = [
    {
        "id": "abonnement",
        "title": "Abonnement",
        "description": "Pipeline og revenue — Watch DK, INT, NO, DE, FINANS, MarketWire, Monitor",
        "icon": "pulse",
        "color": "green",
        "min_role": "team",
        "subcategories": [
            {"id": "watch-dk",     "title": "Watch DK",    "description": "Dansk pipeline og omsætning",            "brand": "watch_dk",    "min_role": "team"},
            {"id": "watch-int",    "title": "Watch INT",   "description": "Internationale abonnementer",             "brand": "watch_int",   "min_role": "team"},
            {"id": "watch-no",     "title": "Watch NO",    "description": "Norsk pipeline og revenue",               "brand": "watch_no",    "min_role": "team"},
            {"id": "watch-de",     "title": "Watch DE",    "description": "Tysk pipeline og revenue",                "brand": "watch_de",    "min_role": "management"},
            {"id": "finans",       "title": "FINANS",      "description": "FINANS abonnement og pipeline",           "brand": "finans",      "min_role": "team"},
            {"id": "marketwire",   "title": "MarketWire",  "description": "MarketWire omsætning og abonnementer",    "brand": "marketwire",  "min_role": "team"},
            {"id": "monitor",      "title": "Monitor",     "description": "Monitor abonnement og pipeline",          "brand": "monitor",     "min_role": "team"},
            {"id": "samlet",       "title": "Samlet",      "description": "Tværgående overblik — alle brands",       "brand": None,          "min_role": "management"},
        ],
        "items": [
            {"id": "abo-watch-dk-pipeline",  "title": "Watch DK Pipeline",      "type": "dashboard", "subcategory": "watch-dk",   "brand": "watch_dk",  "min_role": "team",       "url": "/dashboard/abo-watch-dk-pipeline"},
            {"id": "abo-watch-dk-revenue",   "title": "Watch DK Revenue",        "type": "dashboard", "subcategory": "watch-dk",   "brand": "watch_dk",  "min_role": "team",       "url": "/dashboard/abo-watch-dk-revenue"},
            {"id": "abo-finans-pipeline",    "title": "FINANS Pipeline",          "type": "dashboard", "subcategory": "finans",     "brand": "finans",    "min_role": "team",       "url": "/dashboard/abo-finans-pipeline"},
            {"id": "abo-finans-revenue",     "title": "FINANS Revenue",           "type": "dashboard", "subcategory": "finans",     "brand": "finans",    "min_role": "team",       "url": "/dashboard/abo-finans-revenue"},
            {"id": "abo-samlet",             "title": "Samlet Abonnement",        "type": "dashboard", "subcategory": "samlet",     "brand": None,        "min_role": "management", "url": "/dashboard/abo-samlet"},
            {"id": "abo-samlet-mgmt",        "title": "Samlet Management View",   "type": "dashboard", "subcategory": "samlet",     "brand": None,        "min_role": "management", "url": "/dashboard/abo-samlet-mgmt"},
        ],
    },
    {
        "id": "sales-operations",
        "title": "Sales Operations",
        "description": "Retention, budget, portefølje-afstemning og rapportering",
        "icon": "settings",
        "color": "amber",
        "min_role": "team",
        "subcategories": [
            {"id": "retention",            "title": "Retention",             "description": "Churn og fastholdelse",           "brand": None, "min_role": "team"},
            {"id": "budget",               "title": "Budget",                "description": "Budget upload og dashboard",      "brand": None, "min_role": "team"},
            {"id": "portfolio-afstemning", "title": "Portefølje Afstemning", "description": "Afstemning af portefølje",        "brand": None, "min_role": "team"},
            {"id": "rapportering",         "title": "Rapportering",          "description": "Interne salgsrapporter",          "brand": None, "min_role": "team"},
        ],
        "items": [
            {"id": "budget-dashboard",     "title": "Budget Dashboard 2025",     "type": "dashboard", "subcategory": "budget",               "brand": None, "min_role": "team",       "url": "/dashboard/budget"},
            {"id": "budget-upload-tool",   "title": "Budget Upload Tool",        "type": "tool",      "subcategory": "budget",               "brand": None, "min_role": "team",       "url": "/tools/budget/"},
            {"id": "portfolio-tool",       "title": "Portefølje Afstemning",     "type": "tool",      "subcategory": "portfolio-afstemning", "brand": None, "min_role": "team",       "url": "/tool/portfolio"},
            {"id": "retention-dashboard",  "title": "Retention Dashboard",       "type": "dashboard", "subcategory": "retention",            "brand": None, "min_role": "team",       "url": "/dashboard/retention"},
            {"id": "rapportering-tool",    "title": "Rapporteringsværktøj",      "type": "tool",      "subcategory": "rapportering",         "brand": None, "min_role": "management", "url": "/tool/rapportering"},
        ],
    },
    {
        "id": "annonce",
        "title": "Annonce",
        "description": "Banner, job og samlet annonceoverblik på tværs af medier",
        "icon": "globe",
        "color": "green",
        "min_role": "team",
        "subcategories": [
            {"id": "banner", "title": "Banner", "description": "Banner-annoncering",  "brand": None, "min_role": "team"},
            {"id": "job",    "title": "Job",    "description": "Jobannoncer",          "brand": None, "min_role": "team"},
            {"id": "samlet", "title": "Samlet", "description": "Samlet annonceoverblik", "brand": None, "min_role": "management"},
        ],
        "items": [
            {"id": "banner-dashboard", "title": "Banner Dashboard",  "type": "dashboard", "subcategory": "banner", "brand": None, "min_role": "team",       "url": "/dashboard/banner"},
            {"id": "job-dashboard",    "title": "Job Dashboard",     "type": "dashboard", "subcategory": "job",    "brand": None, "min_role": "team",       "url": "/dashboard/job"},
            {"id": "annonce-samlet",   "title": "Annonce Samlet",    "type": "dashboard", "subcategory": "samlet", "brand": None, "min_role": "management", "url": "/dashboard/annonce-samlet"},
        ],
    },
    {
        "id": "marketing",
        "title": "Marketing",
        "description": "Kampagneanalyse, lead tracking og digital performance",
        "icon": "activity",
        "color": "amber",
        "min_role": "team",
        "subcategories": [],
        "items": [
            {"id": "marketing-dashboard",  "title": "Marketing Dashboard",   "type": "dashboard", "subcategory": None, "brand": None, "min_role": "team", "url": "/dashboard/marketing"},
            {"id": "leads-dashboard",      "title": "Leads Dashboard",       "type": "dashboard", "subcategory": None, "brand": None, "min_role": "team", "url": "/dashboard/leads"},
            {"id": "kampagne-tool",        "title": "Kampagne Planlægning",  "type": "tool",      "subcategory": None, "brand": None, "min_role": "team", "url": "/tool/kampagne"},
        ],
    },
    {
        "id": "hr",
        "title": "HR",
        "description": "Medarbejderoverblik, onboarding flow og HR-rapporter",
        "icon": "users",
        "color": "green",
        "min_role": "management",
        "subcategories": [],
        "items": [
            {"id": "hr-medarbejdere",  "title": "Medarbejderoversigt", "type": "dashboard", "subcategory": None, "brand": None, "min_role": "management", "url": "/dashboard/hr-medarbejdere"},
            {"id": "hr-onboarding",   "title": "Onboarding Flow",     "type": "tool",      "subcategory": None, "brand": None, "min_role": "management", "url": "/tool/onboarding"},
        ],
    },
    {
        "id": "management",
        "title": "Management",
        "description": "Ledelsesrapporter og tværgående overblik på alle brands",
        "icon": "briefcase",
        "color": "green",
        "min_role": "management",
        "subcategories": [],
        "items": [
            {"id": "mgmt-overblik",  "title": "Management Overblik",   "type": "dashboard", "subcategory": None, "brand": None, "min_role": "management", "url": "/dashboard/mgmt-overblik"},
            {"id": "mgmt-rapport",   "title": "Ledelses Rapport",      "type": "dashboard", "subcategory": None, "brand": None, "min_role": "management", "url": "/dashboard/mgmt-rapport"},
        ],
    },
]

# ---------------------------------------------------------------------------
# Mock session — erstat med rigtig auth (JWT / OAuth / AD) i produktion
# ---------------------------------------------------------------------------
MOCK_USERS = {
    "ce": {"name": "Carl-Emil", "initials": "CE", "role": "admin",      "brand": None},
    "anna": {"name": "Anna",      "initials": "AN", "role": "team",       "brand": "finans"},
    "mads": {"name": "Mads",      "initials": "MA", "role": "team",       "brand": "watch_dk"},
    "lars": {"name": "Lars",      "initials": "LA", "role": "management", "brand": None},
}

ROLE_RANK = {"team": 1, "management": 2, "admin": 3}

def get_current_user(request: Request):
    """
    Simpel mock-session. Brug ?user=anna / ?user=mads til at teste roller.
    Erstat med rigtig session/JWT i produktion.
    """
    uid = request.query_params.get("user", "ce")
    return MOCK_USERS.get(uid, MOCK_USERS["ce"])

def has_access(user: dict, min_role: str, brand: Optional[str] = None) -> bool:
    user_rank = ROLE_RANK.get(user["role"], 0)
    req_rank  = ROLE_RANK.get(min_role, 99)
    if user_rank < req_rank:
        return False
    # Team-brugere ser kun eget brand (hvis brand er sat på ressourcen)
    if user["role"] == "team" and brand and user["brand"] != brand:
        return False
    return True

def filter_categories(categories: list, user: dict) -> list:
    """Filtrér kategorier og items baseret på brugerens rolle og brand."""
    result = []
    for cat in categories:
        if not has_access(user, cat["min_role"]):
            continue
        visible_items = [
            item for item in cat["items"]
            if has_access(user, item["min_role"], item.get("brand"))
        ]
        visible_subs = [
            sub for sub in cat.get("subcategories", [])
            if has_access(user, sub["min_role"], sub.get("brand"))
        ]
        dashboard_count = sum(1 for i in visible_items if i["type"] == "dashboard")
        tool_count      = sum(1 for i in visible_items if i["type"] == "tool")
        result.append({
            **cat,
            "items":         visible_items,
            "subcategories": visible_subs,
            "dashboard_count": dashboard_count,
            "tool_count":      tool_count,
        })
    return result

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

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
    # Byg søgeindex til command palette (flad liste)
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
    all_cats   = filter_categories(CATEGORIES, user)
    categories = all_cats
    cat        = next((c for c in all_cats if c["id"] == cat_id), None)
    if not cat:
        raise HTTPException(status_code=404, detail="Kategori ikke fundet eller ingen adgang")

    # Items er allerede filtreret af filter_categories
    subs = {}
    for item in cat["items"]:
        key = item["subcategory"] or "Generelt"
        subs.setdefault(key, []).append(item)

    return templates.TemplateResponse("category.html", {
        "request":    request,
        "user":       user,
        "categories": categories,
        "cat":        cat,
        "subs":       subs,
        "total_db":   sum(1 for i in cat["items"] if i["type"] == "dashboard"),
        "total_t":    sum(1 for i in cat["items"] if i["type"] == "tool"),
    })

@app.get("/dashboard/{dashboard_id}", response_class=HTMLResponse)
async def dashboard_view(dashboard_id: str, request: Request, user=Depends(get_current_user)):
    # Placeholder — erstat med rigtig dashboard-rendering
    return HTMLResponse(f"<h2>Dashboard: {dashboard_id}</h2><p>Bruger: {user['name']} ({user['role']})</p><a href='/'>← Hub</a>")

@app.get("/tool/{tool_id}", response_class=HTMLResponse)
async def tool_view(tool_id: str, request: Request, user=Depends(get_current_user)):
    return HTMLResponse(f"<h2>Tool: {tool_id}</h2><p>Bruger: {user['name']} ({user['role']})</p><a href='/'>← Hub</a>")

@app.get("/api/search")
async def search_api(q: str, user=Depends(get_current_user)):
    """Søge-endpoint til fremtidig live-søgning."""
    results = []
    for cat in filter_categories(CATEGORIES, user):
        for item in cat["items"]:
            if q.lower() in item["title"].lower():
                results.append({**item, "category": cat["title"]})
    return {"results": results[:10]}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)
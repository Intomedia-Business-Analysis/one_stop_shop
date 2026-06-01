"""Shared navigation helpers and registry.

Holder CATEGORIES og filter_categories så ALLE Jinja2Templates-instanser
i appen kan rendere den samme dynamiske sidebar via _sidebar.html.
"""
from auth import ROLE_LABELS, has_access, resolve_resource_access


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
            {"id": "kpi-saelger",       "title": "Sælger Dashboard",        "type": "dashboard", "subcategory": None, "brand": None, "min_role": "salesperson",    "exclude_roles": ["sales_operations", "management"], "url": "/tools/performance/saelger"},
            {"id": "kpi-manager",       "title": "Manager Dashboard",       "type": "dashboard", "subcategory": None, "brand": None, "min_role": "sales_manager",  "url": "/tools/performance/manager"},
            {"id": "kpi-afdelingsleder","title": "Afdelingsleder Dashboard", "type": "dashboard", "subcategory": None, "brand": None, "min_role": "sales_operations","url": "/tools/performance/afdelingsleder"},
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
            {"id": "budget",    "title": "Budget",    "description": "Budget upload og dashboard",       "brand": None, "min_role": "sales_manager"},
            {"id": "forecast",  "title": "Forecast",  "description": "Salgsprognoser",                   "brand": None, "min_role": "sales_manager"},
            {"id": "alignment", "title": "Alignment", "description": "Pipedrive vs. Zuora ACV-kontrol",  "brand": None, "min_role": "sales_operations"},
        ],
        "items": [
            {"id": "budget-upload-tool",      "title": "Budget",              "type": "tool",      "subcategory": "budget",    "brand": None, "min_role": "sales_manager", "url": "/tools/budget/"},
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
        "required_team": "Banner og Job",
        "subcategories": [],
        "items": [
            {"id": "banner-job-dashboard", "title": "Banner & Job Dashboard", "type": "dashboard", "subcategory": None, "brand": None, "min_role": "salesperson", "required_team": "Banner og Job", "exclude_roles": ["sales_operations"], "url": "/tools/banner-job/"},
        ],
    },
    {
        "id": "marketing",
        "title": "Marketing",
        "description": "Lead-konvertering og deal source-analyse",
        "icon": "pulse",
        "color": "green",
        "min_role": "marketing",
        "subcategories": [],
        "items": [
            {"id": "marketing-deal-source", "title": "Deal Source Dashboard", "type": "dashboard", "subcategory": None, "brand": None, "min_role": "marketing", "url": "/tools/marketing/deal-source"},
        ],
    },
    {
        "id": "rotation-dashboards",
        "title": "Rotation Dashboards",
        "description": "Performance dashboards til kontorskærme — Sales, Department, Banner, Job og Media",
        "icon": "activity",
        "color": "green",
        "min_role": "salesperson",
        "subcategories": [],
        "items": [
            {"id": "rotation-autoplay",    "title": "Rotation",                "type": "tool",      "subcategory": None, "brand": None, "min_role": "salesperson", "url": "/tools/rotation/"},
            {"id": "rotation-sales",       "title": "Sales Performance",       "type": "dashboard", "subcategory": None, "brand": None, "min_role": "salesperson", "url": "/tools/rotation/sales-performance"},
            {"id": "rotation-department",  "title": "Department Performance",  "type": "dashboard", "subcategory": None, "brand": None, "min_role": "salesperson", "url": "/tools/rotation/department-performance"},
            {"id": "rotation-banner",      "title": "Banner Performance",      "type": "dashboard", "subcategory": None, "brand": None, "min_role": "salesperson", "url": "/tools/rotation/banner-performance"},
            {"id": "rotation-job",         "title": "Job Performance",         "type": "dashboard", "subcategory": None, "brand": None, "min_role": "salesperson", "url": "/tools/rotation/job-performance"},
            {"id": "rotation-media",       "title": "Media Performance",       "type": "dashboard", "subcategory": None, "brand": None, "min_role": "salesperson", "url": "/tools/rotation/media-performance"},
        ],
    },
    {
        "id": "hr",
        "title": "HR",
        "description": "HR-værktøjer",
        "icon": "users",
        "color": "green",
        "min_role": "salesperson",
        "subcategories": [],
        "items": [
            {"id": "barselsberegner", "title": "Barselsplanlægger", "type": "tool", "subcategory": None, "brand": None, "min_role": "salesperson", "url": "/tool/barselsberegner"},
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
            access = resolve_resource_access(user, item["id"], item["min_role"], item.get("brand"), item.get("required_team"), item.get("exclude_roles"))
            if access != "none":
                visible_items.append({**item, "access": access})
        visible_subs = [
            sub for sub in cat.get("subcategories", [])
            if has_access(user, sub["min_role"], sub.get("brand"))
        ]
        dashboard_count = sum(1 for i in visible_items if i["type"] == "dashboard")
        tool_count      = sum(1 for i in visible_items if i["type"] == "tool")
        if not visible_items and not visible_subs:
            continue
        result.append({
            **cat,
            "items":           visible_items,
            "subcategories":   visible_subs,
            "dashboard_count": dashboard_count,
            "tool_count":      tool_count,
        })
    return result


def register_nav_globals(templates) -> None:
    """Registrer CATEGORIES, filter_categories og ROLE_LABELS på en
    Jinja2Templates-instans, så _sidebar.html kan rendere uden at hver
    route skal sende `categories` i konteksten.
    """
    templates.env.globals["CATEGORIES"]        = CATEGORIES
    templates.env.globals["filter_categories"] = filter_categories
    templates.env.globals["ROLE_LABELS"]       = ROLE_LABELS

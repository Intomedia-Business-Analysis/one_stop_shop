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
            {"id": "kpi-saelger-portfolio", "title": "Sælger Portefølje",   "type": "dashboard", "subcategory": None, "brand": None, "min_role": "salesperson",    "exclude_roles": ["sales_operations", "management"], "url": "/tools/saelger-portfolio/"},
            {"id": "kpi-manager",       "title": "Manager Dashboard",       "type": "dashboard", "subcategory": None, "brand": None, "min_role": "sales_manager",  "url": "/tools/performance/manager"},
            {"id": "kpi-afdelingsleder","title": "Afdelingsleder Dashboard", "type": "dashboard", "subcategory": None, "brand": None, "min_role": "sales_operations","url": "/tools/performance/afdelingsleder"},
            # min_role SKAL matche MIN_ROLLE i modul_benchmark/router.py:
            # sættes den lavere her, får fx en sales manager et menupunkt der
            # svarer 403. management (rang 5) dækker ledelse + admin.
            {"id": "benchmark-medier", "title": "Medie Benchmark",           "type": "dashboard", "subcategory": None, "brand": None, "min_role": "management",     "url": "/tools/benchmark/medier"},
        ],
    },
    {
        "id": "sales-operations",
        "title": "Sales Operations",
        "description": "Budget og forecast",
        "icon": "settings",
        "color": "amber",
        # Sælgere skal kunne lave deres eget forecast — kategorien er åben for
        # salesperson, mens budget/alignment-items stadig kræver højere rang.
        "min_role": "salesperson",
        "subcategories": [
            {"id": "budget",    "title": "Budget",    "description": "Budget upload og dashboard",       "brand": None, "min_role": "sales_manager"},
            {"id": "forecast",  "title": "Forecast",  "description": "Salgsprognoser",                   "brand": None, "min_role": "salesperson"},
            {"id": "alignment", "title": "Alignment", "description": "Pipedrive vs. Zuora ACV-kontrol",  "brand": None, "min_role": "sales_operations"},
        ],
        "items": [
            {"id": "budget-upload-tool",      "title": "Budget",              "type": "tool",      "subcategory": "budget",    "brand": None, "min_role": "sales_manager", "url": "/tools/budget/"},
            {"id": "forecast-tool",           "title": "Forecast",            "type": "tool",      "subcategory": "forecast",  "brand": None, "min_role": "salesperson",    "url": "/tools/forecast/"},
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
            {"id": "klippekort-overblik", "title": "Klippekort Overblik", "type": "dashboard", "subcategory": None, "brand": None, "min_role": "salesperson", "required_team": "Banner og Job", "exclude_roles": ["sales_operations"], "url": "/tools/klippekort/"},
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
            {"id": "rotation-sales-no",    "title": "Sales Performance NO",    "type": "dashboard", "subcategory": None, "brand": None, "min_role": "salesperson", "url": "/tools/rotation/sales-performance?teams=Team%20Watch%20NO"},
            {"id": "rotation-department",  "title": "Department Performance",  "type": "dashboard", "subcategory": None, "brand": None, "min_role": "salesperson", "url": "/tools/rotation/department-performance"},
            {"id": "rotation-banner",      "title": "Banner Performance",      "type": "dashboard", "subcategory": None, "brand": None, "min_role": "salesperson", "url": "/tools/rotation/banner-performance"},
            {"id": "rotation-job",         "title": "Job Performance",         "type": "dashboard", "subcategory": None, "brand": None, "min_role": "salesperson", "url": "/tools/rotation/job-performance"},
            {"id": "rotation-no-adv",      "title": "Advertising Performance NO", "type": "dashboard", "subcategory": None, "brand": None, "min_role": "salesperson", "url": "/tools/rotation/no-advertising-performance"},
            {"id": "rotation-media",       "title": "Media Performance",       "type": "dashboard", "subcategory": None, "brand": None, "min_role": "salesperson", "url": "/tools/rotation/media-performance"},
        ],
    },
    {
        "id": "retention",
        "title": "Retention",
        "description": "Opkald og risiko, samt porteføljens udvikling over tid",
        "icon": "settings",
        "color": "amber",
        # sales_operations SKAL matche MIN_ROLLE i modul_retention/router.py.
        # Hævet fra salesperson 2026-08-10: retention-specialisten er en Sales
        # Operations-bruger, og modulet viser hele firmaets churn-billede, så
        # sælgere og sales managers har ingen adgang længere. Sættes den lavere
        # her end i routeren, får sælgere et menupunkt der svarer 403.
        #
        # exclude_roles står KUN på de to items, ikke her: filter_categories
        # sender ikke exclude_roles med på kategori-tjekket (kun på items), så
        # nøglen ville blive tavst ignoreret på dette niveau. Det virker
        # alligevel, fordi en kategori uden synlige items droppes helt.
        "min_role": "sales_operations",
        "subcategories": [],
        "items": [
            # ØVERST med vilje: modulets INDGANG. "Opkald og risiko" er
            # sammenlægningen (2026-08-27) af Dagens opkald og Churn-risiko —
            # arbejdsgangen begynder her og går derfra til kunde-detaljen.
            # Rækkefølgen i listen ER den viste rækkefølge, så lå den nederst,
            # ville arbejdsgangen være forkert fra første klik. Id'et er
            # UÆNDRET (retention-risk), se RES_RISIKO i router.py for hvorfor.
            {"id": "retention-risk",     "title": "Opkald og risiko",           "type": "dashboard", "subcategory": None, "brand": None, "min_role": "sales_operations", "exclude_roles": ["marketing", "management"], "url": "/retention/risk_overview"},
            # Hed "Porteføljen" til 2026-09-01. Det nye navn indkapsler sidens
            # TO faner ("Operationel og diagnostisk" og "Performance og
            # effekt"), hvor det gamle kun beskrev fane 1. Id'et er UÆNDRET af
            # samme grund som ved retention-risk ovenfor. Ordet "porteføljen"
            # står stadig i panel-overskriften inde på siden, og dér er det
            # rigtigt: dér betyder det bogen af abonnementer, ikke sidenavnet.
            {"id": "retention-overview", "title": "Operationel og Performance", "type": "dashboard", "subcategory": None, "brand": None, "min_role": "sales_operations", "exclude_roles": ["marketing", "management"], "url": "/retention/overview"},
        ],
    },
    {
        "id": "rapportering",
        "title": "Rapportering",
        "description": "ARR-/salgsrapportering og afstemning",
        "icon": "activity",
        "color": "amber",
        "min_role": "sales_operations",
        "subcategories": [],
        "items": [
            {"id": "admin-nysalg", "title": "Monthly Performance Report", "type": "tool", "subcategory": None, "brand": None, "min_role": "management", "url": "/tools/admin-nysalg/"},
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
        # Ressource-baseret kategori-gate: normale roller falder igennem til
        # rang-tjekket (uændret adgang), men en eksplicit RoleResourceAccess-
        # override på kategori-id'et kan åbne kategorien for fx 'screen'-rollen.
        if resolve_resource_access(user, cat["id"], cat["min_role"]) == "none":
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


def visible_items(user: dict, categories: list | None = None) -> list[dict]:
    """Alle nav-items brugeren må se, hver beriget med kategoriens titel.

    `categories` kan gives med, hvis kalderen allerede har kaldt
    filter_categories (så adgangsfiltreringen ikke laves to gange).
    """
    cats = categories if categories is not None else filter_categories(CATEGORIES, user)
    return [{**item, "category": cat["title"]}
            for cat in cats for item in cat["items"]]


def visible_items_by_id(user: dict, categories: list | None = None) -> dict:
    """{item_id: item} for de items brugeren må se — opslag til favoritter/seneste.

    Bruges til at oversætte gemte item-id'er til rigtige menupunkter OG til at
    filtrere dem: et id brugeren ikke længere har adgang til falder simpelthen ud.
    """
    return {item["id"]: item for item in visible_items(user, categories)}


# ── Sti → nav-item ───────────────────────────────────────────────────────────
# Bruges af "senest besøgt": middleware'en kender kun den besøgte sti, mens
# favoritter/seneste arbejder på item-id'er. Undersider tælles som et besøg på
# selve værktøjet (fx /tools/admin-nysalg/35/review → 'admin-nysalg'), så et
# review-besøg også dukker op under seneste.

def _split_url(url: str) -> tuple[str, str]:
    """('/sti', 'query') for et item-url — query er '' når der ikke er nogen."""
    path, _, query = (url or "").partition("?")
    return path, query


# {fuld sti inkl. query: item} — fanger varianter der KUN adskiller sig på query
# (fx Sales Performance NO), som ellers ville kollidere på stien.
_ITEMS_BY_FULL_URL: dict[str, dict] = {}
# {sti uden query: item} — første item på stien vinder (hovedvarianten).
_ITEMS_BY_PATH: dict[str, dict] = {}
for _cat in CATEGORIES:
    for _item in _cat["items"]:
        _p, _q = _split_url(_item.get("url", ""))
        if not _p:
            continue
        if _q:
            _ITEMS_BY_FULL_URL.setdefault(f"{_p}?{_q}", _item)
        _ITEMS_BY_PATH.setdefault(_p, _item)
# Længste sti først, så en underside matcher det mest specifikke item
# (/tools/rotation/sales-performance frem for /tools/rotation/).
_ITEM_PATHS_BY_LENGTH = sorted(_ITEMS_BY_PATH, key=len, reverse=True)


def resolve_item_id(path: str, query: str = "") -> str | None:
    """Item-id'et en besøgt sti hører til — None hvis stien ikke er et nav-item.

    Rækkefølge: eksakt sti+query, eksakt sti, længste sti-præfiks (undersider).
    """
    path = (path or "").rstrip("/") or "/"
    for candidate in (f"{path}?{query}" if query else None,
                      f"{path}/?{query}" if query else None):
        if candidate and candidate in _ITEMS_BY_FULL_URL:
            return _ITEMS_BY_FULL_URL[candidate]["id"]
    for candidate in (path, path + "/"):
        if candidate in _ITEMS_BY_PATH:
            return _ITEMS_BY_PATH[candidate]["id"]
    for item_path in _ITEM_PATHS_BY_LENGTH:
        base = item_path.rstrip("/")
        if base and path.startswith(base) and path[len(base):len(base) + 1] == "/":
            return _ITEMS_BY_PATH[item_path]["id"]
    return None


def register_nav_globals(templates) -> None:
    """Registrer CATEGORIES, filter_categories og ROLE_LABELS på en
    Jinja2Templates-instans, så _sidebar.html kan rendere uden at hver
    route skal sende `categories` i konteksten.
    """
    templates.env.globals["CATEGORIES"]        = CATEGORIES
    templates.env.globals["filter_categories"] = filter_categories
    templates.env.globals["ROLE_LABELS"]       = ROLE_LABELS

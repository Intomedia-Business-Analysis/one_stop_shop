import json
import logging
import os
import secrets
import time

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
    session_user_id,
)
from log_setup import audit_log, setup_logging, _client_ip
from nav_utils import (
    CATEGORIES,
    filter_categories,
    register_nav_globals,
    visible_items_by_id,
)
import personalization
from moduler.modul_budget.router import router as budget_router
from moduler.modul_admin.router import router as admin_router
from moduler.modul_forcast.router import router as forecast_router
from moduler.modul_perf.router import router as perf_router
from moduler.modul_barsel.router import router as barsel_router
from moduler.modul_barsel.queries import init_barsel_db
from moduler.modul_banner_job.router import router as banner_job_router
from moduler.modul_portfolio_alignment.router import router as portfolio_alignment_router
from moduler.modul_rotation.router import router as rotation_router
from moduler.modul_retention.router import router as retention_router
from moduler.modul_marketing.router import router as marketing_router
from moduler.modul_saelger_portfolio.router import router as saelger_portfolio_router
from moduler.modul_klippekort.router import router as klippekort_router
from moduler.modul_admin_nysalg.router import router as admin_nysalg_router
from moduler.modul_admin_nysalg.repo import init_admin_nysalg_db
from usage_tracking import record_pageview, start_usage_worker

load_dotenv()
setup_logging()
logger = logging.getLogger(__name__)

if os.getenv("DEV_MODE") == "1":
    logger.info("[DEV] DEV_MODE=1 — login og SQL-forbindelse er bypassed")

# API-dokumentationen (/docs, /redoc, /openapi.json) er kun slået til i
# DEV_MODE — offentligt eksponeret giver openapi.json et komplet kort over
# alle endpoints og datastrukturer til enhver, der finder sitet.
_docs_enabled = os.getenv("DEV_MODE") == "1"
app = FastAPI(
    title="Intomedia Hub",
    docs_url="/docs" if _docs_enabled else None,
    redoc_url="/redoc" if _docs_enabled else None,
    openapi_url="/openapi.json" if _docs_enabled else None,
)
init_db()         # Opret hub-tabeller ved opstart (idempotent)
init_barsel_db()  # Opret barseltabeller ved opstart (idempotent)
init_admin_nysalg_db()  # Opret admin-nysalg-tabeller ved opstart (idempotent)
personalization.init_personalization_db()  # Favoritter + senest besøgt (idempotent)
start_usage_worker()  # Baggrundstråd der flusher usage-loggen til DB
personalization.start_visit_worker()  # Baggrundstråd der flusher besøg til DB
# Session-nøglen SKAL være sat i .env — med en kendt fallback-nøgle ville
# enhver kunne forfalske session-cookies og logge ind som vilkårlig bruger.
# I DEV_MODE bruges en tilfældig nøgle pr. opstart (sessioner ryger ved genstart).
_secret_key = os.getenv("SECRET_KEY")
if not _secret_key:
    if os.getenv("DEV_MODE") == "1":
        _secret_key = secrets.token_hex(32)
    else:
        raise RuntimeError(
            "SECRET_KEY mangler i .env. Generér én med:\n"
            '  python -c "import secrets; print(secrets.token_hex(32))"\n'
            "og tilføj linjen SECRET_KEY=<nøglen> til .env."
        )
app.add_middleware(
    SessionMiddleware,
    secret_key=_secret_key,
    same_site="lax",  # session-cookien sendes ikke med cross-site POSTs
)


# ---------------------------------------------------------------------------
# CSRF-beskyttelse: afvis skrivende requests fra fremmede sites.
# Browsere sender altid Origin-headeren på cross-site POSTs — matcher den ikke
# vores egen host, er requesten ikke affyret fra hubben selv. Sammen med
# SameSite=lax på session-cookien lukker det CSRF uden tokens i alle formularer.
# Requests uden Origin/Referer (curl, scripts, gamle klienter) tillades.
# ---------------------------------------------------------------------------
from urllib.parse import quote, urlparse  # noqa: E402
from fastapi.responses import JSONResponse  # noqa: E402


@app.middleware("http")
async def csrf_origin_check(request: Request, call_next):
    if request.method in ("POST", "PUT", "PATCH", "DELETE"):
        source = request.headers.get("origin") or request.headers.get("referer")
        if source:
            source_host = urlparse(source).netloc
            if source_host and source_host != request.headers.get("host", ""):
                return JSONResponse(
                    {"detail": "Requesten kommer fra et andet site (CSRF-tjek)"},
                    status_code=403,
                )
    return await call_next(request)


# ---------------------------------------------------------------------------
# Usage-tracking: log sidevisninger (kun HTML-sider, ikke static/data/JSON)
# ---------------------------------------------------------------------------
@app.middleware("http")
async def usage_tracking_middleware(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    try:
        if request.method == "GET" and response.status_code < 400:
            ctype = response.headers.get("content-type", "")
            path  = request.url.path
            # Kun rigtige sider (text/html) — udelukker JSON-data-endpoints,
            # static-filer, billeder og redirects automatisk.
            if ctype.startswith("text/html") and not path.startswith("/static") \
               and path not in ("/login", "/logout"):
                session = request.scope.get("session") or {}
                record_pageview(
                    user_id=session.get("user_id"),
                    path=path,
                    status_code=response.status_code,
                    duration_ms=int((time.perf_counter() - start) * 1000),
                )
                # "Senest besøgt" i sidebaren — kun stier der er et nav-item
                # (dashboards/tools), ikke forsiden og kategorisiderne.
                personalization.record_visit(
                    session_user_id(request), path, request.url.query)
    except Exception:
        pass  # tracking må aldrig vælte et request
    return response
app.include_router(budget_router)
app.include_router(admin_router)
app.include_router(forecast_router)
app.include_router(perf_router)
app.include_router(barsel_router)
app.include_router(banner_job_router)
app.include_router(portfolio_alignment_router)
app.include_router(rotation_router)
app.include_router(retention_router)
app.include_router(marketing_router)
app.include_router(saelger_portfolio_router)
app.include_router(klippekort_router)
app.include_router(admin_nysalg_router)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")
register_nav_globals(templates)

# ---------------------------------------------------------------------------
# Exception handlers
# ---------------------------------------------------------------------------

def _safe_next_url(value) -> str | None:
    """Validér et ?next=-redirect-mål fra login-flowet.

    Kun relative stier på eget site accepteres — alt andet (absolutte URL'er,
    schema-relative '//host', backslash-tricks) afvises, så login ikke kan
    bruges som open redirect til fremmede sites.
    """
    if not value or not isinstance(value, str):
        return None
    if not value.startswith("/") or value.startswith("//") or "\\" in value:
        return None
    return value


@app.exception_handler(RequiresLoginException)
async def requires_login_handler(request: Request, exc: RequiresLoginException):
    # Bevar destinationen (sti + query) gennem login-rundturen, så fx en
    # skærm-URL (/tools/rotation/screen/<id>) lander rigtigt efter login i
    # stedet for at smide konfigurationen væk og ende på forsiden.
    if request.method == "GET":
        next_url = request.url.path + (f"?{request.url.query}" if request.url.query else "")
        if _safe_next_url(next_url) and next_url != "/":
            return RedirectResponse(url="/login?next=" + quote(next_url, safe=""), status_code=302)
    return RedirectResponse(url="/login", status_code=302)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    """Fang alle uhåndterede fejl: log dem og giv klienten et brugbart svar.

    Data-endpoints (fetch/XHR) får JSON med en fejlnøgle, så dashboards kan
    vise "Data utilgængelig" i stedet for tomme charts. Almindelige sidevisninger
    (Accept: text/html) får en lille fejlside. Selve tracebacken ligger i hub.log.
    """
    logger.error(
        "Uhåndteret fejl på %s %s", request.method, request.url.path, exc_info=exc
    )
    if "text/html" in request.headers.get("accept", ""):
        return HTMLResponse(
            "<div style='font-family:sans-serif;max-width:480px;margin:80px auto;"
            "text-align:center'><h2>Der opstod en fejl</h2>"
            "<p>Data kunne ikke hentes. Prøv igen om lidt — fejlen er logget.</p>"
            "<a href='/'>&larr; Tilbage til hubben</a></div>",
            status_code=500,
        )
    return JSONResponse({"error": "Data kunne ikke hentes"}, status_code=500)


# ---------------------------------------------------------------------------
# Tool & Dashboard Registry
# ---------------------------------------------------------------------------

# CATEGORIES og filter_categories er i nav_utils.py


# ---------------------------------------------------------------------------
# Login / Logout
# ---------------------------------------------------------------------------

# Brute-force-bremse: efter _LOGIN_MAX_FAILS fejlede forsøg fra samme IP inden
# for vinduet afvises nye forsøg, til vinduet er udløbet. In-memory — tælleren
# nulstilles ved genstart, hvilket er acceptabelt: formålet er at gøre
# adgangskode-gætteri upraktisk langsomt, ikke at føre evigt regnskab.
_LOGIN_MAX_FAILS = 5
_LOGIN_WINDOW_S  = 15 * 60
_failed_logins: dict[str, list[float]] = {}


def _rate_limit_key(request: Request) -> str:
    """IP der tælles på. X-Forwarded-For er klient-kontrolleret og kan spoofes
    til at omgå blokeringen — den bruges derfor kun med TRUST_PROXY=1, dvs. når
    en reverse proxy foran hubben garanteret sætter headeren.
    """
    if os.getenv("TRUST_PROXY") == "1":
        return _client_ip(request)
    return request.client.host if request.client else "?"


def _login_retry_after(ip: str) -> int:
    """Sekunder til IP'en må prøve igen — 0 hvis den ikke er blokeret."""
    now = time.time()
    attempts = [t for t in _failed_logins.get(ip, []) if now - t < _LOGIN_WINDOW_S]
    if attempts:
        _failed_logins[ip] = attempts
    else:
        _failed_logins.pop(ip, None)
    if len(attempts) >= _LOGIN_MAX_FAILS:
        return int(_LOGIN_WINDOW_S - (now - attempts[0])) + 1
    return 0


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    next_url = _safe_next_url(request.query_params.get("next"))
    if os.getenv("DEV_MODE") == "1":
        return RedirectResponse(next_url or "/", status_code=302)
    user_id = request.session.get("user_id")
    if user_id:
        from auth import get_user_by_id
        if get_user_by_id(user_id):
            return RedirectResponse(next_url or "/", status_code=302)
        # Forældet session (DB nede eller bruger slettet) — ryd op
        request.session.clear()
    return templates.TemplateResponse(request, "login.html", {"error": None, "next": next_url})


@app.post("/login", response_class=HTMLResponse)
async def login_post(request: Request):
    form = await request.form()
    username = form.get("username", "").strip()
    password = form.get("password", "")
    next_url = _safe_next_url(form.get("next"))

    ip = _rate_limit_key(request)
    retry_after = _login_retry_after(ip)
    if retry_after:
        audit_log("login_blokeret", request=request, username=username)
        return templates.TemplateResponse(request, "login.html", {
            "error": "For mange mislykkede forsøg — prøv igen om "
                     f"{max(1, retry_after // 60)} min.",
            "next": next_url,
        }, status_code=429)

    user = authenticate_user(username, password)
    if not user:
        _failed_logins.setdefault(ip, []).append(time.time())
        audit_log("login_afvist", request=request, username=username)
        return templates.TemplateResponse(request, "login.html", {
            "error": "Forkert brugernavn eller adgangskode",
            "next": next_url,
        })
    _failed_logins.pop(ip, None)
    request.session["user_id"] = user["id"]
    audit_log("login_ok", user=user, request=request)
    if next_url:
        # Man var på vej et bestemt sted hen (fx en konfigureret skærm-URL) —
        # land dér i stedet for på forsiden.
        return RedirectResponse(next_url, status_code=302)
    # Skærm-brugere lander direkte på rotationen — de skal kun se den.
    if user.get("role") == "screen":
        return RedirectResponse("/tools/rotation/", status_code=302)
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
    return templates.TemplateResponse(request, "settings.html", {
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
        audit_log("password_aendret", user=user, request=request)
    except Exception:
        logger.exception("Kunne ikke skifte adgangskode for bruger id=%s", user["id"])
        return RedirectResponse("/settings?error=pw_error", status_code=302)

    return RedirectResponse("/settings?success=pw_changed", status_code=302)


@app.post("/settings/change-username")
async def settings_change_username(request: Request, user=Depends(get_current_user)):
    from auth import get_conn as auth_get_conn, verify_password
    form         = await request.form()
    new_username = form.get("new_username", "").strip()
    current_pw   = form.get("current_password", "")

    if not all([new_username, current_pw]):
        return RedirectResponse("/settings?error=un_missing_fields", status_code=302)
    if not verify_password(current_pw, user["password_hash"]):
        return RedirectResponse("/settings?error=un_pw_wrong", status_code=302)
    if new_username == user["username"]:
        return RedirectResponse("/settings?error=un_unchanged", status_code=302)

    try:
        conn = auth_get_conn()
        cur  = conn.cursor()
        # Tjek at brugernavnet ikke allerede er taget af en anden bruger
        cur.execute(
            "SELECT id FROM HubUsers WHERE username=%s AND id<>%s",
            (new_username, user["id"]),
        )
        if cur.fetchone():
            conn.close()
            return RedirectResponse("/settings?error=un_taken", status_code=302)
        cur.execute(
            "UPDATE HubUsers SET username=%s WHERE id=%s",
            (new_username, user["id"]),
        )
        conn.commit()
        conn.close()
        audit_log("brugernavn_aendret", user=user, request=request,
                  nyt_brugernavn=new_username)
    except Exception:
        logger.exception("Kunne ikke skifte brugernavn for bruger id=%s", user["id"])
        return RedirectResponse("/settings?error=un_error", status_code=302)

    return RedirectResponse("/settings?success=un_changed", status_code=302)


@app.get("/dashboard/budget", response_class=HTMLResponse)
async def budget_dashboard(request: Request, user=Depends(get_current_user)):
    return templates.TemplateResponse(request, "budget_tool.html", {
        "user": user,
    })


# ---------------------------------------------------------------------------
# Favoritter og senest besøgt
# ---------------------------------------------------------------------------
# Begge lister gemmes pr. bruger i DB (personalization.py) og oversættes til
# rigtige menupunkter via nav-registret. Et gemt id, som brugeren ikke længere
# har adgang til (eller som er fjernet fra registret), falder stille ud.

def _resolve_items(item_ids: list, by_id: dict) -> list:
    """Item-id'er → nav-items i samme rækkefølge; ukendte/utilgængelige udelades.

    Kun de felter visningen bruger tages med — rolle-/team-krav fra registret
    hører ikke i sidens HTML (listerne serialiseres til JSON i søgepaletten).
    """
    return [{"id": it["id"], "title": it["title"], "type": it["type"],
             "category": it.get("category", ""), "url": it["url"]}
            for it in (by_id[i] for i in item_ids if i in by_id)]


def _personal_lists(user: dict, categories: list | None = None,
                    recent_limit: int = 10) -> tuple[list, list, set]:
    """(favorit-items, senest besøgte items, favorit-id'er) for en bruger.

    Favoritter filtreres bevidst IKKE ud af "senest besøgt": listen skal vise
    hvad man faktisk har været inde på, og et tomt felt ville ellers ligne at
    besøgsregistreringen ikke virker.
    """
    by_id = visible_items_by_id(user, categories)
    fav_ids = personalization.get_favorite_ids(user["id"])
    favorites = _resolve_items(fav_ids, by_id)
    recent_ids = personalization.get_recent_item_ids(user["id"], recent_limit)
    recent = _resolve_items(recent_ids, by_id)
    return favorites, recent, set(fav_ids)


@app.get("/", response_class=HTMLResponse)
async def hub(request: Request, user=Depends(get_current_user)):
    # Skærm-brugere har kun rotationen — send dem direkte derhen.
    if user.get("role") == "screen":
        return RedirectResponse("/tools/rotation/", status_code=302)
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
    favorites, recent, fav_ids = _personal_lists(user, categories, recent_limit=5)
    return templates.TemplateResponse(request, "hub.html", {
        "user":         user,
        "categories":   categories,
        "total_dash":   total_dash,
        "total_tools":  total_tools,
        "cat_count":    len(categories),
        "search_index": json.dumps(search_index),
        "favorites":    favorites,
        "recent_items": recent,
        "fav_ids":      fav_ids,
    })


@app.get("/favorites", response_class=HTMLResponse)
async def favorites_page(request: Request, user=Depends(get_current_user)):
    favorites, _, fav_ids = _personal_lists(user)
    return templates.TemplateResponse(request, "personal_list.html", {
        "user":       user,
        "active_url": "/favorites",
        "title":      "Mine favoritter",
        "subtitle":   "Dine stjernemarkerede dashboards og tools — klik på stjernen "
                      "for at fjerne en favorit.",
        "items":      favorites,
        "fav_ids":    fav_ids,
        "empty_text": "Du har ingen favoritter endnu. Klik på stjernen ud for et "
                      "dashboard eller tool for at samle dine genveje her.",
    })


@app.get("/recent", response_class=HTMLResponse)
async def recent_page(request: Request, user=Depends(get_current_user)):
    _, recent, fav_ids = _personal_lists(user, recent_limit=30)
    return templates.TemplateResponse(request, "personal_list.html", {
        "user":       user,
        "active_url": "/recent",
        "title":      "Seneste",
        "subtitle":   "De dashboards og tools du senest har været inde på — nyeste først.",
        "items":      recent,
        "fav_ids":    fav_ids,
        "empty_text": "Ingen besøg registreret endnu. Åbn et dashboard, så lander "
                      "det her.",
    })


@app.post("/api/favorites/{item_id}/toggle")
async def toggle_favorite_api(item_id: str, request: Request,
                              user=Depends(get_current_user)):
    """Slå favorit til/fra for ét menupunkt. Returnerer den nye tilstand.

    Kun items brugeren faktisk må se kan markeres — ellers kunne et vilkårligt
    id skrives i tabellen.
    """
    if item_id not in visible_items_by_id(user):
        raise HTTPException(status_code=404, detail="Ukendt item eller ingen adgang")
    try:
        is_favorite = personalization.toggle_favorite(user["id"], item_id)
    except Exception:
        logger.exception("Kunne ikke skifte favorit (user_id=%s, item=%s)",
                         user["id"], item_id)
        raise HTTPException(status_code=500, detail="Favoritten kunne ikke gemmes")
    return JSONResponse({"ok": True, "favorite": is_favorite})


@app.get("/api/favorites")
async def favorites_api(user=Depends(get_current_user)):
    """Brugerens favoritter — bruges af søgepaletten og andre sider."""
    by_id = visible_items_by_id(user)
    return {"items": _resolve_items(personalization.get_favorite_ids(user["id"]), by_id)}


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
    return templates.TemplateResponse(request, "category.html", {
        "user":       user,
        "categories": all_cats,
        "cat":        cat,
        "subs":       subs,
        "total_db":   sum(1 for i in cat["items"] if i["type"] == "dashboard"),
        "total_t":    sum(1 for i in cat["items"] if i["type"] == "tool"),
        "fav_ids":    set(personalization.get_favorite_ids(user["id"])),
    })


@app.get("/dashboard/{dashboard_id}", response_class=HTMLResponse)
async def dashboard_view(dashboard_id: str, request: Request, user=Depends(get_current_user)):
    return HTMLResponse(f"<h2>Dashboard: {dashboard_id}</h2><p>Bruger: {user['name']} ({user['role']})</p><a href='/'>← Hub</a>")


@app.get("/tool/barselsberegner", response_class=HTMLResponse)
async def barselsberegner_view(request: Request, user=Depends(get_current_user)):
    categories = filter_categories(CATEGORIES, user)
    return templates.TemplateResponse(request, "tool_barselsberegner.html", {
        "user":       user,
        "categories": categories,
    })


@app.get("/tool/barselsberegner/app", response_class=HTMLResponse)
async def barselsberegner_app(request: Request, user=Depends(get_current_user)):
    """Serverer selve beregner-appen i en iframe (kræver login)."""
    see_all  = user["role"] in ("admin", "management")
    is_admin = user["role"] == "admin"
    return templates.TemplateResponse(request, "barselsberegner_app.html", {
        "user":     user,
        "see_all":  see_all,
        "is_admin": is_admin,
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
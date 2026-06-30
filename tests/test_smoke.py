"""Røgtests for Intomedia Hub.

Fredet adfærd, som en senere refaktorering ikke må knække i stilhed:
  - appen nægter at starte uden SECRET_KEY
  - CSRF-tjekket afviser skrivende requests fra fremmede sites
  - sider og data-endpoints kræver login (redirect til /login)
  - rolle- og team-baseret dataadgang (403 ved fremmed team)
  - den globale fejlhandler giver JSON-fejl + 500 i stedet for tomme data
"""
import os
import subprocess
import sys

import pytest

from conftest import REPO_ROOT


# ---------------------------------------------------------------------------
# Opstart
# ---------------------------------------------------------------------------

def test_app_naegter_start_uden_secret_key(tmp_path):
    """Uden SECRET_KEY (og uden DEV_MODE) skal importen af app fejle højlydt.

    Køres i en subprocess med cwd uden .env-fil, så en lokal .env ikke
    leverer nøglen alligevel.
    """
    env = os.environ.copy()
    env.pop("SECRET_KEY", None)
    env.pop("DEV_MODE", None)
    env["DB_SERVER"] = "db-findes-ikke.invalid"
    env["PYTHONPATH"] = str(REPO_ROOT)
    proc = subprocess.run(
        [sys.executable, "-c", "import app"],
        capture_output=True, text=True, env=env,
        cwd=tmp_path,  # ingen .env her
        timeout=120,
    )
    assert proc.returncode != 0
    assert "SECRET_KEY" in proc.stderr


# ---------------------------------------------------------------------------
# CSRF
# ---------------------------------------------------------------------------

def test_csrf_fremmed_origin_afvises(client):
    r = client.post(
        "/login",
        data={"username": "x", "password": "y"},
        headers={"Origin": "https://ondsindet.example"},
    )
    assert r.status_code == 403


def test_csrf_egen_origin_tillades(client):
    r = client.post(
        "/login",
        data={"username": "x", "password": "y"},
        headers={"Origin": "http://testserver"},
    )
    # Ikke CSRF-blokeret — login fejler blot (ingen DB), så loginsiden vises igen
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# Login-krav
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("path", [
    "/",
    "/settings",
    "/tools/budget/",
    "/tools/budget/medie/data",
    "/admin/users",
    "/tools/rotation/sales-performance-data",
])
def test_kraever_login_redirect(client, path):
    r = client.get(path)
    assert r.status_code == 302
    # Destinationen bevares som ?next=, så man lander rigtigt efter login
    from urllib.parse import quote
    expected = "/login" if path == "/" else "/login?next=" + quote(path, safe="")
    assert r.headers["location"] == expected


def test_login_saetter_session(client, app_module, make_user, monkeypatch):
    user = make_user(role="salesperson")
    monkeypatch.setattr(
        app_module, "authenticate_user",
        lambda u, p: user if (u == "testbruger" and p == "hemmelig") else None,
    )
    r = client.post("/login", data={"username": "testbruger", "password": "hemmelig"})
    assert r.status_code == 302
    assert r.headers["location"] == "/"
    assert "session" in r.cookies


def test_login_forkert_kodeord_afvises(client, app_module, monkeypatch):
    monkeypatch.setattr(app_module, "authenticate_user", lambda u, p: None)
    r = client.post("/login", data={"username": "testbruger", "password": "forkert"})
    assert r.status_code == 200
    assert "Forkert brugernavn eller adgangskode" in r.text


def test_screen_bruger_sendes_til_rotationen_efter_login(client, app_module, make_user, monkeypatch):
    user = make_user(role="screen")
    monkeypatch.setattr(app_module, "authenticate_user", lambda u, p: user)
    r = client.post("/login", data={"username": "skaerm", "password": "x"})
    assert r.status_code == 302
    assert r.headers["location"] == "/tools/rotation/"


def test_login_lander_paa_next_destination(client, app_module, make_user, monkeypatch):
    """Skærm-URL'er (og andre destinationer) skal overleve login-rundturen,
    så en konfigureret skærm ikke taber sin opsætning ved login."""
    user = make_user(role="screen")
    monkeypatch.setattr(app_module, "authenticate_user", lambda u, p: user)
    r = client.post("/login", data={
        "username": "skaerm", "password": "x",
        "next": "/tools/rotation/screen/c33de907",
    })
    assert r.status_code == 302
    assert r.headers["location"] == "/tools/rotation/screen/c33de907"


def test_login_next_bevarer_querystring(client):
    # Uautoriseret GET med query-parametre → hele destinationen med i ?next=
    r = client.get("/tools/rotation/", params={"dashboards": "sales,media", "interval": "60"})
    assert r.status_code == 302
    from urllib.parse import parse_qs, urlparse
    loc = urlparse(r.headers["location"])
    assert loc.path == "/login"
    next_url = urlparse(parse_qs(loc.query)["next"][0])
    assert next_url.path == "/tools/rotation/"
    assert parse_qs(next_url.query) == {"dashboards": ["sales,media"], "interval": ["60"]}


@pytest.mark.parametrize("evil_next", [
    "https://ondsindet.example/phish",
    "//ondsindet.example",
    "/sti\\..\\fusk",
    "relativ-uden-skraastreg",
])
def test_login_next_afviser_open_redirect(client, app_module, make_user, monkeypatch, evil_next):
    user = make_user(role="salesperson")
    monkeypatch.setattr(app_module, "authenticate_user", lambda u, p: user)
    r = client.post("/login", data={"username": "x", "password": "y", "next": evil_next})
    assert r.status_code == 302
    assert r.headers["location"] == "/"


# ---------------------------------------------------------------------------
# Rolle- og teambaseret adgang
# ---------------------------------------------------------------------------

def test_hub_forside_virker_som_admin(client, make_user, auth_override):
    auth_override(make_user(role="admin"))
    r = client.get("/")
    assert r.status_code == 200


def test_budget_kraever_sales_manager(client, make_user, auth_override):
    auth_override(make_user(role="salesperson"))
    r = client.get("/tools/budget/medie/data")
    assert r.status_code == 403


def test_budget_fremmed_team_giver_403(client, make_user, auth_override):
    # Bruger må kun se data for Team FINANS Int — beder om et andet teams budget
    auth_override(make_user(role="sales_manager", data_teams=["Team FINANS Int"]))
    r = client.get("/tools/budget/saelger/data", params={"team": "Team Watch NO"})
    assert r.status_code == 403


@pytest.mark.parametrize("role", ["salesperson", "sales_manager", "management"])
def test_admin_sider_kraever_admin(client, make_user, auth_override, role):
    auth_override(make_user(role=role))
    r = client.get("/admin/users")
    assert r.status_code == 403


def test_screen_bruger_uden_override_blokeres_fra_rotation(client, make_user, auth_override):
    # 'screen' har rang 0 — uden RoleResourceAccess-override gives ingen adgang
    auth_override(make_user(role="screen"))
    r = client.get("/tools/rotation/sales-performance-data")
    assert r.status_code == 302


def test_screen_bruger_med_override_ser_rotationen(client, make_user, auth_override):
    auth_override(make_user(role="screen", role_access={"rotation": "read"}))
    r = client.get("/tools/rotation/")
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# Fejlsynlighed: databasefejl skal give JSON-fejl + 500, ikke tomme data
# ---------------------------------------------------------------------------

def _kaster(*args, **kwargs):
    raise RuntimeError("Simuleret databasefejl")


def test_datafejl_giver_500_med_fejlbesked(client, make_user, auth_override, monkeypatch):
    import moduler.modul_rotation.router as rot
    monkeypatch.setattr(rot, "db_sales_performance", _kaster)
    auth_override(make_user(role="admin"))
    r = client.get("/tools/rotation/sales-performance-data")
    assert r.status_code == 500
    assert r.json() == {"error": "Data kunne ikke hentes"}


def test_budget_datafejl_giver_500_uden_interne_detaljer(client, make_user, auth_override, monkeypatch):
    import moduler.modul_budget.router as bud
    monkeypatch.setattr(bud, "db_medie_query", _kaster)
    auth_override(make_user(role="admin"))
    r = client.get("/tools/budget/medie/data")
    assert r.status_code == 500
    # Intern fejltekst må ikke lække til klienten
    assert "Simuleret databasefejl" not in r.text


# ---------------------------------------------------------------------------
# Forecast: sælger laver eget forecast, manager ser overblik og vurderer
# ---------------------------------------------------------------------------

def test_forecast_tool_aaben_for_saelger(client, make_user, auth_override):
    auth_override(make_user(role="salesperson", teams=["Team Watch DK"]))
    r = client.get("/tools/forecast/")
    assert r.status_code == 200
    assert "Mit forecast" in r.text


def test_forecast_tool_viser_manager_overblik(client, make_user, auth_override):
    auth_override(make_user(role="sales_manager"))
    r = client.get("/tools/forecast/")
    assert r.status_code == 200
    assert "Team-overblik" in r.text
    # Manageren skal også kunne lave sit eget forecast (ud over overblikket)
    assert "Mit forecast" in r.text


def test_forecast_tool_har_medregn_tilvaekst(client, make_user, auth_override):
    auth_override(make_user(role="salesperson", teams=["Team Watch DK"]))
    r = client.get("/tools/forecast/")
    assert r.status_code == 200
    assert "Medregn tilvækst" in r.text


def test_forecast_overview_kraever_manager(client, make_user, auth_override):
    auth_override(make_user(role="salesperson", teams=["Team Watch DK"]))
    r = client.get("/tools/forecast/overview", params={"year": 2026, "month": 7})
    assert r.status_code == 403


def test_forecast_review_kraever_manager(client, make_user, auth_override):
    auth_override(make_user(role="salesperson", teams=["Team Watch DK"]))
    r = client.post("/tools/forecast/review/save", json={
        "year": 2026, "month": 7, "team": "Team Watch DK", "manager_amount": 100000,
    })
    assert r.status_code == 403


def test_forecast_review_fremmed_team_giver_403(client, make_user, auth_override):
    auth_override(make_user(role="sales_manager", data_teams=["Team FINANS Int"]))
    r = client.post("/tools/forecast/review/save", json={
        "year": 2026, "month": 7, "team": "Team Watch NO", "manager_amount": 100000,
    })
    assert r.status_code == 403


def test_forecast_saelger_kan_kun_gemme_egne_teams(client, make_user, auth_override):
    # Rækker på fremmede teams filtreres fra — er der ingen gyldige tilbage, afvises
    auth_override(make_user(role="salesperson", teams=["Team Watch DK"]))
    r = client.post("/tools/forecast/my/save", json={
        "year": 2026, "month": 7,
        "rows": [{"team": "Team Watch NO", "pipeline_pct": 30, "manual_amount": 0, "forecast_total": 50000}],
    })
    assert r.status_code == 400


def test_forecast_my_uden_teams_giver_tom_liste(client, make_user, auth_override):
    auth_override(make_user(role="salesperson", teams=[]))
    r = client.get("/tools/forecast/my", params={"year": 2026, "month": 7})
    assert r.status_code == 200
    assert r.json()["rows"] == []


def test_forecast_saelger_ser_kun_egne_raekker(client, make_user, auth_override, monkeypatch):
    # Datalaget returnerer flere sælgere — /my må kun udlevere brugerens egen række
    import moduler.modul_forcast.router as fc
    monkeypatch.setattr(fc, "db_get_teams", lambda: [{"name": "Team Watch DK", "brand": ""}])
    monkeypatch.setattr(fc, "db_forecast_data", lambda *a, **kw: (
        {"Test Bruger": 100.0, "Anden Sælger": 999.0},  # hist_m1
        {}, {}, {}, {}, {},
    ))
    auth_override(make_user(role="salesperson", teams=["Team Watch DK"]))
    r = client.get("/tools/forecast/my", params={"year": 2026, "month": 7})
    assert r.status_code == 200
    rows = r.json()["rows"]
    assert [row["dimension_key"] for row in rows] == ["Test Bruger"]
    assert rows[0]["team"] == "Team Watch DK"


def test_forecast_overview_viser_kun_ansvarlige_teams(client, make_user, auth_override, monkeypatch):
    # Manager med team-dataadgang begrænset til ét team ser kun dét team
    import moduler.modul_forcast.router as fc
    monkeypatch.setattr(fc, "db_get_teams", lambda: [
        {"name": "Team Watch DK", "brand": ""},
        {"name": "Team Watch NO", "brand": ""},
    ])
    monkeypatch.setattr(fc, "db_active_team_members", lambda names: {n: ["Sælger A"] for n in names})
    monkeypatch.setattr(fc, "db_get_reviews", lambda y, m, names: {})
    monkeypatch.setattr(fc, "db_forecast_data", lambda *a, **kw: ({}, {}, {}, {}, {}, {}))
    auth_override(make_user(role="sales_manager", data_teams=["Team Watch DK"]))
    r = client.get("/tools/forecast/overview", params={"year": 2026, "month": 7})
    assert r.status_code == 200
    assert [t["team"] for t in r.json()["teams"]] == ["Team Watch DK"]


def test_forecast_reminder_vises_ikke_for_manager(client, make_user, auth_override):
    auth_override(make_user(role="sales_manager", teams=["Team Watch DK"]))
    r = client.get("/tools/forecast/reminder")
    assert r.status_code == 200
    assert r.json()["show"] is False


def test_forecast_nav_saelger_ser_forecast_men_ikke_budget(make_user):
    from nav_utils import CATEGORIES, filter_categories
    cats = filter_categories(CATEGORIES, make_user(role="salesperson", teams=["Team Watch DK"]))
    salesops = next((c for c in cats if c["id"] == "sales-operations"), None)
    assert salesops is not None
    item_ids = {i["id"] for i in salesops["items"]}
    assert "forecast-tool" in item_ids
    assert "budget-upload-tool" not in item_ids


# ---------------------------------------------------------------------------
# Søge-API
# ---------------------------------------------------------------------------

def test_search_api(client, make_user, auth_override):
    auth_override(make_user(role="admin"))
    r = client.get("/api/search", params={"q": "budget"})
    assert r.status_code == 200
    assert isinstance(r.json().get("results"), list)

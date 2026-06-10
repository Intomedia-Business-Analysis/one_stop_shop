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
    assert r.headers["location"] == "/login"


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
# Søge-API
# ---------------------------------------------------------------------------

def test_search_api(client, make_user, auth_override):
    auth_override(make_user(role="admin"))
    r = client.get("/api/search", params={"q": "budget"})
    assert r.status_code == 200
    assert isinstance(r.json().get("results"), list)

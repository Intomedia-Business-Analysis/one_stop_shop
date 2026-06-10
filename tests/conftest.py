"""Fælles test-opsætning.

Testene kører UDEN database: DB_SERVER peger på et hostnavn der ikke findes,
så alle pymssql-connects fejler hurtigt, og app'en falder tilbage til sine
default-roller. Login/adgang testes via FastAPI's dependency_overrides, så vi
ikke behøver rigtige brugere i en DB.

VIGTIGT: miljøvariablerne sættes FØR app importeres — load_dotenv() i app.py
og db.py overskriver ikke variabler, der allerede er sat i miljøet.
"""
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# Skal ske før `import app` (og før load_dotenv læser .env)
os.environ["SECRET_KEY"]  = "testnoegle-ikke-til-produktion"
os.environ["DEV_MODE"]    = "0"
os.environ["DB_SERVER"]   = "db-findes-ikke.invalid"  # DNS-fejl = hurtig connect-fejl
os.environ["DB_USER"]     = "test"
os.environ["DB_PASSWORD"] = "test"
os.environ["DB_NAME"]     = "test"

sys.path.insert(0, str(REPO_ROOT))
os.chdir(REPO_ROOT)  # Jinja2Templates("templates") og StaticFiles("static") er relative stier

import pytest  # noqa: E402


@pytest.fixture(scope="session")
def app_module():
    import app as app_mod
    # Usage-tracking forsøger at skrive til DB i baggrunden — slå den fra i tests
    app_mod.record_pageview = lambda **kw: None
    return app_mod


@pytest.fixture
def client(app_module):
    from fastapi.testclient import TestClient
    # raise_server_exceptions=False: vi vil se 500-SVARET (global handler),
    # ikke have exceptionen kastet ind i testen.
    with TestClient(
        app_module.app,
        raise_server_exceptions=False,
        follow_redirects=False,
    ) as c:
        yield c


@pytest.fixture
def make_user():
    """Fabrik for fake brugere i samme form som auth.get_user_by_id leverer."""
    def _make(role="admin", brand=None, teams=None, data_teams=None,
              role_access=None, resource_access=None):
        return {
            "id":            999,
            "username":      "testbruger",
            "name":          "Test Bruger",
            "initials":      "TB",
            "role":          role,
            "brand":         brand,
            "is_active":     1,
            "password_hash": "ikke-en-rigtig-hash",
            "_resource_access": resource_access or {},
            "_role_access":     role_access or {},
            "_teams":           teams or [],
            "_data_teams":      data_teams or [],
        }
    return _make


@pytest.fixture
def auth_override(app_module):
    """Log en fake bruger 'ind' via dependency override. Rydder op efter testen."""
    from auth import get_current_user

    def _set(user):
        app_module.app.dependency_overrides[get_current_user] = lambda: user
        return user

    yield _set
    app_module.app.dependency_overrides.clear()

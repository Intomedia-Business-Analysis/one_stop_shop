"""Central logging-opsætning for Intomedia Hub.

To logfiler i mappen logs/ (roteres automatisk, så de aldrig vokser uendeligt):

    hub.log    — applikationslog: fejl, advarsler og driftsbeskeder fra alle
                 moduler. Det er her man slår op, når et dashboard viser
                 "Data utilgængelig".
    audit.log  — hvem gjorde hvad hvornår: login-forsøg, bruger-/rolle-/
                 holdændringer og budgetændringer. Slettes/roteres adskilt
                 fra hub.log, så historikken bevares længere.

Brug i moduler:

    import logging
    logger = logging.getLogger(__name__)
    ...
    except Exception:
        logger.exception("db_sales_performance fejlede")

Audit:

    from log_setup import audit_log
    audit_log("login_ok", user=user, request=request)
    audit_log("budget_medie_update", user=user, row_id=42, amount=500000)
"""
import logging
import logging.handlers
import os

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")

_FORMAT = "%(asctime)s %(levelname)-8s %(name)s: %(message)s"

_configured = False


def setup_logging() -> None:
    """Sæt fil- + konsollogging op. Idempotent — kaldes ved app-opstart."""
    global _configured
    if _configured:
        return
    _configured = True

    os.makedirs(LOG_DIR, exist_ok=True)
    fmt = logging.Formatter(_FORMAT)

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    file_handler = logging.handlers.RotatingFileHandler(
        os.path.join(LOG_DIR, "hub.log"),
        maxBytes=5 * 1024 * 1024,  # 5 MB pr. fil
        backupCount=5,             # hub.log + hub.log.1 ... hub.log.5
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    root.addHandler(console)

    # Audit-loggen skriver KUN til audit.log (propagate=False), så den ikke
    # drukner i applikationsloggen — og omvendt.
    audit = logging.getLogger("audit")
    audit.setLevel(logging.INFO)
    audit.propagate = False
    audit_handler = logging.handlers.RotatingFileHandler(
        os.path.join(LOG_DIR, "audit.log"),
        maxBytes=5 * 1024 * 1024,
        backupCount=10,            # audit-historik gemmes længere end driftslog
        encoding="utf-8",
    )
    audit_handler.setFormatter(fmt)
    audit.addHandler(audit_handler)


def _client_ip(request) -> str:
    """Klientens IP — tager højde for evt. reverse proxy (X-Forwarded-For)."""
    try:
        fwd = request.headers.get("x-forwarded-for")
        if fwd:
            return fwd.split(",")[0].strip()
        return request.client.host if request.client else "?"
    except Exception:
        return "?"


def audit_log(action: str, user: dict | None = None, request=None, **details) -> None:
    """Skriv en hændelse til audit.log.

    action  — kort maskinlæsbar hændelse, fx 'login_ok', 'user_updated'
    user    — den handlende bruger (dict med username/id) eller None
    request — FastAPI Request; bruges til at logge klient-IP
    details — vilkårlige nøgle=værdi-par, fx target_user_id=7, team='Team X'

    Må aldrig vælte et request — fejl i selve auditeringen sluges.
    """
    try:
        parts = [action]
        if user:
            parts.append(f"user={user.get('username', '?')}(id={user.get('id', '?')})")
        if request is not None:
            parts.append(f"ip={_client_ip(request)}")
        for key, value in details.items():
            parts.append(f"{key}={value}")
        logging.getLogger("audit").info(" ".join(parts))
    except Exception:
        logging.getLogger(__name__).exception("audit_log fejlede")

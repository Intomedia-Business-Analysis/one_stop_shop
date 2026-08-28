"""Tjek opsætningen FØR appen startes.

    python preflight.py

Hubben er 16 moduler oven på én database, en håndfuld filmapper og et par
udgående integrationer. Når noget af det mangler, viser appen "Data
utilgængelig" i et dashboard — den fejler ikke ved opstart. Det er fint i
drift, men umuligt at flytte efter: man opdager først manglerne når en bruger
klikker.

Dette script går det hele igennem og siger hvad der er galt, i én kørsel.
Exit-kode 0 = klar til at starte, 1 = noget kritisk mangler.

Tjekkene er læse-only. Der oprettes ingen tabeller og skrives ingen filer
(bortset fra at skrivbarheden af logs/ og data/ prøves med en midlertidig fil).
"""
import os
import socket
import sys
from pathlib import Path

from env import force_utf8_output, load_env

_ENV_PATH = load_env()

OK = "  ✓ "
ADVARSEL = "  ⚠ "
FEJL = "  ✗ "

_problemer: list[str] = []
_advarsler: list[str] = []


def fejl(besked: str) -> None:
    print(FEJL + besked)
    _problemer.append(besked)


def advar(besked: str) -> None:
    print(ADVARSEL + besked)
    _advarsler.append(besked)


def ok(besked: str) -> None:
    print(OK + besked)


def overskrift(titel: str) -> None:
    print(f"\n── {titel} " + "─" * max(0, 60 - len(titel)))


# ── 1. Miljø ────────────────────────────────────────────────────────────────

def tjek_miljoe() -> None:
    overskrift("Miljø")
    print(f"    Python {sys.version.split()[0]} ({sys.executable})")
    if sys.version_info < (3, 10):
        fejl(f"Python {sys.version_info.major}.{sys.version_info.minor} er for gammel — 3.10+ kræves.")

    if _ENV_PATH:
        ok(f".env læst fra {_ENV_PATH}")
    else:
        advar(".env blev ikke fundet — værdierne skal så komme fra rigtige "
              "miljøvariabler. Sæt HUB_ENV hvis filen ligger et andet sted.")

    for navn in ("DB_SERVER", "DB_USER", "DB_PASSWORD"):
        if os.getenv(navn):
            ok(f"{navn} er sat")
        else:
            fejl(f"{navn} mangler")

    if os.getenv("SECRET_KEY"):
        nøgle = os.getenv("SECRET_KEY", "")
        if len(nøgle) < 32:
            advar(f"SECRET_KEY er kun {len(nøgle)} tegn. Generér en rigtig: "
                  'python -c "import secrets; print(secrets.token_hex(32))"')
        else:
            ok("SECRET_KEY er sat")
    elif os.getenv("DEV_MODE") == "1":
        advar("SECRET_KEY mangler, men DEV_MODE=1 — appen laver en tilfældig "
              "nøgle pr. opstart. Må ikke bruges i drift.")
    else:
        fejl("SECRET_KEY mangler — appen nægter at starte uden.")

    if os.getenv("DEV_MODE") == "1":
        advar("DEV_MODE=1: login og SQL-forbindelse bypasses, og /docs er åben. "
              "Skal være 0 eller usat på serveren.")


# ── 2. Database ─────────────────────────────────────────────────────────────

# Tabeller appen læser. 'kritisk' betyder at hubben er ubrugelig uden —
# resten slår kun de moduler ud der bruger dem.
TABELLER = [
    ("PipedriveDeals",           True,  "næsten alle dashboards"),
    ("HubUsers",                 True,  "login og roller"),
    ("Teams",                    False, "hold og ledere"),
    ("TeamMemberships",          False, "hold og ledere"),
    ("BudgetsIntoMedia",         False, "Budget"),
    ("SalespersonBudget",        False, "Budget pr. sælger"),
    ("HubForecasts",             False, "Forecast"),
    ("ProgrammaticSales",        False, "Marketing/programmatisk omsætning"),
    ("PipeDrive_ACV",            False, "Portfolio Alignment og Retention"),
    ("RetentionOutcomes",        False, "Retention — udfald"),
    ("RetentionConversations",   False, "Retention — samtaler"),
]


def tjek_database() -> None:
    overskrift("Database")
    try:
        from db import check_connection
        info = check_connection()
    except Exception as exc:
        fejl(f"Kunne ikke forbinde: {exc}")
        print("      Tjek DB_SERVER/DB_USER/DB_PASSWORD, og at DB_TDS_VERSION er "
              "7.4 mod en server der kræver kryptering.")
        return

    ok(f"Forbundet til {info['servername']} / {info['database']} som {info['login']}")
    print(f"      TDS {info['tds_version']}, pool {info['pool_size']}, "
          f"DATE som streng: {'ja' if info['date_as_string'] else 'nej'}")

    try:
        from db import new_connection
        conn = new_connection(login_timeout=5, timeout=15)
        try:
            cur = conn.cursor()
            navne = [t[0] for t in TABELLER]
            pladser = ", ".join(["%s"] * len(navne))
            cur.execute(
                f"SELECT name FROM sys.tables WHERE name IN ({pladser})",
                tuple(navne),
            )
            fundet = {r[0] for r in cur.fetchall()}
            # 'retention' er et VIEW hos os, ikke en tabel — slås op separat.
            cur.execute("SELECT 1 FROM sys.views WHERE name = %s", ("retention",))
            retention_view = cur.fetchone() is not None
        finally:
            conn.close()
    except Exception as exc:
        advar(f"Kunne ikke slå tabeller op: {exc}")
        return

    for navn, kritisk, hvem in TABELLER:
        if navn in fundet:
            ok(f"{navn}")
        elif kritisk:
            fejl(f"{navn} mangler — {hvem} virker ikke")
        else:
            advar(f"{navn} mangler — {hvem} virker ikke")

    if retention_view:
        ok("retention (view)")
    else:
        advar("view'et 'retention' mangler — Retention-modulet virker ikke")


# ── 3. Mapper ───────────────────────────────────────────────────────────────

def _skrivbar(sti: Path) -> bool:
    try:
        sti.mkdir(parents=True, exist_ok=True)
        prøve = sti / ".preflight_skrivetest"
        prøve.write_text("ok", encoding="utf-8")
        prøve.unlink()
        return True
    except Exception:
        return False


def _stier_fra_env(navn: str) -> list[Path]:
    """Læs en ';'-separeret liste af stier fra miljøet."""
    rå = (os.getenv(navn) or "").strip()
    return [Path(p.strip()) for p in rå.split(";") if p.strip()]


def tjek_mapper() -> None:
    overskrift("Mapper")
    rod = Path(__file__).resolve().parent

    for navn in ("logs", "data"):
        sti = rod / navn
        if _skrivbar(sti):
            ok(f"{navn}/ kan skrives")
        else:
            fejl(f"{navn}/ kan IKKE skrives ({sti}) — tjenestekontoen mangler "
                 f"rettigheder")

    # Snapshot-mapper: filerne lægges der manuelt fra DataGrip. På serveren
    # ligger de et andet sted end på en pc, og derfor findes overstyringerne.
    for navn, modul in (
        ("ZUORA_SNAPSHOT_DIR", "Portfolio Alignment"),
        ("USAGE_SNAPSHOT_DIR", "Retention — forbrug"),
    ):
        stier = _stier_fra_env(navn)
        if not stier:
            advar(f"{navn} er ikke sat — {modul} falder tilbage til "
                  f"standardstien under brugerens profil, som næppe findes på "
                  f"serveren")
            continue
        fundne = [p for p in stier if p.is_dir()]
        if fundne:
            filer = sum(1 for p in fundne for _ in p.glob("*.*"))
            ok(f"{navn}: {fundne[0]} ({filer} filer)")
        else:
            fejl(f"{navn} peger på {len(stier)} sti(er), og ingen af dem findes "
                 f"— {modul} kan ikke læse sine snapshots: "
                 f"{', '.join(str(p) for p in stier)}")

    udtræk = (os.getenv("ADMIN_NYSALG_EXTRACT_PATH") or "").strip()
    if udtræk:
        if Path(udtræk).exists():
            ok(f"ADMIN_NYSALG_EXTRACT_PATH: {udtræk}")
        else:
            advar(f"ADMIN_NYSALG_EXTRACT_PATH peger på {udtræk}, som ikke findes "
                  f"— filen skal så uploades i UI'en")
    else:
        advar("ADMIN_NYSALG_EXTRACT_PATH er ikke sat — Administrative nysalg "
              "kræver at filen uploades i UI'en")

    rapportmappe = (os.getenv("ADMIN_NYSALG_REPORT_DIR") or "").strip()
    sti = Path(rapportmappe) if rapportmappe else rod / "data" / "admin_nysalg_reports"
    if _skrivbar(sti):
        ok(f"rapportmappe kan skrives: {sti}")
    else:
        fejl(f"rapportmappen kan IKKE skrives: {sti}")


# ── 4. Udgående forbindelser ────────────────────────────────────────────────

def _kan_naa(vaert: str, port: int, timeout: float = 5.0) -> str | None:
    """None hvis forbindelsen lykkedes, ellers en fejlbeskrivelse."""
    try:
        with socket.create_connection((vaert, port), timeout=timeout):
            return None
    except Exception as exc:
        return f"{type(exc).__name__}: {exc}"


def tjek_udgaaende() -> None:
    overskrift("Udgående forbindelser")

    pd_tokens = [n for n in os.environ if n.startswith("PD_TOKEN_") and os.environ[n]]
    if pd_tokens:
        fejlbesked = _kan_naa("api.pipedrive.com", 443)
        if fejlbesked:
            fejl(f"api.pipedrive.com:443 kan ikke nås ({fejlbesked}) — "
                 f"Portfolio Alignment og Klippekort kan ikke skrive til Pipedrive")
        else:
            ok(f"api.pipedrive.com:443 ({len(pd_tokens)} tokens sat)")
    else:
        advar("ingen PD_TOKEN_*-variabler sat — de moduler der skriver til "
              "Pipedrive kan ikke bruges")

    smtp_vaert = (os.getenv("SMTP_HOST") or "").strip()
    smtp_bruger = (os.getenv("SMTP_USER") or "").strip()
    if smtp_bruger:
        vaert = smtp_vaert or "smtp.office365.com"
        port = int((os.getenv("SMTP_PORT") or "587").strip() or 587)
        fejlbesked = _kan_naa(vaert, port)
        if fejlbesked:
            fejl(f"{vaert}:{port} kan ikke nås ({fejlbesked}) — Barsels-"
                 f"planlæggeren kan ikke sende godkendelsesmails. Porten skal "
                 f"åbnes i firewallen.")
        else:
            ok(f"{vaert}:{port}")
    else:
        advar("SMTP_USER er ikke sat — mails ved barselsgodkendelse springes over "
              "(godkendelsen virker stadig)")


# ── Kør det hele ────────────────────────────────────────────────────────────

def main() -> int:
    force_utf8_output()
    print("=" * 66)
    print("  Intomedia Hub — preflight")
    print("=" * 66)

    tjek_miljoe()
    tjek_database()
    tjek_mapper()
    tjek_udgaaende()

    print("\n" + "=" * 66)
    if _problemer:
        print(f"  {len(_problemer)} kritisk(e) problem(er), {len(_advarsler)} advarsel/-ler")
        for p in _problemer:
            print(f"    ✗ {p}")
        print("\n  Appen bør ikke startes før de kritiske problemer er løst.")
        return 1

    if _advarsler:
        print(f"  Klar til at starte — men {len(_advarsler)} advarsel/-ler:")
        for a in _advarsler:
            print(f"    ⚠ {a}")
        print("\n  Hver advarsel svarer til et modul eller en funktion der ikke "
              "virker.")
        return 0

    print("  Alt klar. Start med: python run_server.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())

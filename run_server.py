"""Opstart af Intomedia Hub på serveren.

    python run_server.py

Erstatter `uvicorn app:app --reload` fra udviklingsmaskinen. Forskellene der
betyder noget i drift:

  * ingen --reload. Genindlæseren starter en ekstra proces, holder øje med
    filsystemet og genstarter appen ved enhver filændring — inklusive når
    logrotationen skriver i logs/. Den hører ikke hjemme på en server.
  * host og port læses fra miljøet, så de kan ændres uden at redigere kode.
  * proxy-headers slås til når TRUST_PROXY=1, så klient-IP i audit-loggen og
    login-rate-limiteren er den rigtige bag en reverse proxy. Det er SAMME
    variabel som app.py bruger til rate-limiteren (app.py:244) — ét navn til
    én beslutning, så uvicorn og appen ikke kan komme til at være uenige.
  * TLS termineres af uvicorn selv, når HUB_SSL_KEYFILE og HUB_SSL_CERTFILE
    er sat. Stierne tjekkes FØR uvicorn starter, fordi en tjeneste ikke har
    nogen konsol: uden tjekket dør processen på en manglende, ulæselig eller
    DER-kodet fil, og Task Scheduler viser blot "0x1" uden spor.
  * uvicorns egen adgangslog slås fra som standard: hvert sidevisning står
    allerede i hub.log og i usage-tabellen, og en dobbelt log fylder blot
    disken hurtigere. Sæt HUB_ACCESS_LOG=1 hvis du vil have den.

Miljøvariabler (alle valgfrie, se .env.example):
    HUB_HOST         bind-adresse, standard 0.0.0.0 (alle interfaces)
    HUB_PORT         port, standard 8000
    HUB_WORKERS      antal processer, standard 1
    HUB_ACCESS_LOG   1 = uvicorns adgangslog til
    TRUST_PROXY      1 = stol på X-Forwarded-For/-Proto (kun bag proxy!)
                     Deles med app.py's rate-limiter.
    HUB_SSL_KEYFILE  sti til den private nøgle (PEM). Sat sammen med
                     HUB_SSL_CERTFILE = appen taler HTTPS direkte.
    HUB_SSL_CERTFILE sti til certifikatet (PEM). Helst hele kæden
                     (leaf + mellemcertifikater) i én fil.
    HUB_SSL_CA_CERTS mellemcertifikater, hvis de ikke ligger i CERTFILE.
    HUB_SSL_KEYFILE_PASSWORD   kun hvis nøglen er krypteret.

Om HUB_WORKERS: appen holder baggrundstråde (usage-flush, besøgsregistrering)
og en connection-pool pr. proces. Flere workers giver derfor flere pools og
flere tråde, ikke delt tilstand — det virker, men start med 1 og hæv kun hvis
CPU'en faktisk er flaskehalsen. Sessioner ligger i signerede cookies, så de
overlever både genstart og flere workers.

Som Windows-tjeneste: se README.md — kort fortalt peger NSSM (eller en Task
Scheduler-opgave der starter ved boot) på .venv\\Scripts\\python.exe med dette
script som argument og projektmappen som arbejdsmappe.
"""
import os
import sys
from pathlib import Path

from env import force_utf8_output, load_env

load_env()


def _int_env(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        raise SystemExit(f"{name}='{raw}' er ikke et heltal.")


def _flag(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "yes", "y", "true", "ja")


def _pem(value: str, label: str, env_name: str, kind: str = "cert") -> str:
    """
    Slå en certifikatsti op og fejl højlydt hvis den ikke kan bruges.

    De måder det går galt på i praksis — filen findes ikke, tjenestekontoen må
    ikke læse den, filen er DER/PFX, eller den er gemt med BOM/UTF-16 af et
    Windows-værktøj — giver alle samme intetsigende exit fra uvicorn. Her får
    hver sin besked, og `kind` afgør hvilken openssl-kommando der foreslås:
    nøgler og certifikater konverteres ikke ens.
    """
    p = Path(os.path.expandvars(value)).expanduser()
    if not p.is_file():
        raise SystemExit(
            f"{env_name} peger på '{p}', som ikke findes. "
            f"{label} skal ligge et sted tjenestekontoen kan nå."
        )
    try:
        data = p.read_bytes()
    except PermissionError:
        raise SystemExit(
            f"{env_name}: '{p}' kan ikke læses. Giv den konto opgaven kører "
            f"som (ikke din egen bruger) læserettigheder på filen."
        )
    # Samme regel som OpenSSLs egen PEM-læser: den scanner linje for linje
    # efter '-----BEGIN' og ignorerer alt før. Filer fra `openssl pkcs12` har
    # f.eks. en 'Bag Attributes'-blok foran nøglen, og de er gyldige. Et tjek
    # på filens første bytes ville afvise dem -- og et tjek på om strengen
    # blot FINDES ville omvendt godtage en UTF-16/BOM-fil, som OpenSSL ikke kan
    # læse. Derfor: starter en LINJE med -----BEGIN?
    if not any(line.startswith(b"-----BEGIN") for line in data.splitlines()):
        out = p.with_name(p.stem + ".pem")
        if kind == "key":
            konverter = (
                f'  openssl rsa -inform der -in "{p}" -out "{out}"\n'
                f'  openssl pkcs8 -inform der -nocrypt -in "{p}" -out "{out}"\n'
                f'  openssl pkcs12 -in cert.pfx -nocerts -nodes -out "{out}"'
            )
        else:
            konverter = (
                f'  openssl x509 -inform der -in "{p}" -out "{out}"\n'
                f'  openssl pkcs12 -in cert.pfx -nokeys -out "{out}"'
            )
        # De første bytes afslører som regel hvad filen er: 30 82 = DER/PFX,
        # FF FE / EF BB BF = tekst gemt med BOM (typisk PowerShells Out-File,
        # hvis standard-encoding er UTF-16 henholdsvis UTF-8-med-BOM). Sidst-
        # nævnte ser rigtig ud i Notepad, og er derfor den sværeste at gætte.
        raise SystemExit(
            f"{env_name}: '{p}' er ikke PEM. uvicorn/OpenSSL læser kun PEM "
            f"(tekst der starter med -----BEGIN).\n"
            f"Filen starter med: {data[:4].hex(' ').upper()}\n"
            f"  30 82...     = binær DER/PFX. Konvertér:\n{konverter}\n"
            f"  FF FE / EF BB BF = PEM gemt med BOM (UTF-16/UTF-8-BOM). "
            f"Gem den som ren ASCII:\n"
            f'  Set-Content -Path "{out}" '
            f'-Value (Get-Content "{p}" -Raw) -Encoding ascii'
        )
    return str(p)


def _tls_options() -> dict:
    """Byg uvicorns ssl_*-argumenter, eller {} hvis HTTPS ikke er slået til."""
    keyfile = (os.getenv("HUB_SSL_KEYFILE") or "").strip()
    certfile = (os.getenv("HUB_SSL_CERTFILE") or "").strip()

    if not keyfile and not certfile:
        return {}
    if not keyfile or not certfile:
        mangler = "HUB_SSL_KEYFILE" if not keyfile else "HUB_SSL_CERTFILE"
        raise SystemExit(
            f"{mangler} mangler. HTTPS kræver begge — nøgle og certifikat."
        )

    opts = {
        "ssl_keyfile": _pem(keyfile, "Den private nøgle", "HUB_SSL_KEYFILE", "key"),
        "ssl_certfile": _pem(certfile, "Certifikatet", "HUB_SSL_CERTFILE"),
    }

    ca_certs = (os.getenv("HUB_SSL_CA_CERTS") or "").strip()
    if ca_certs:
        opts["ssl_ca_certs"] = _pem(ca_certs, "CA-kæden", "HUB_SSL_CA_CERTS")

    password = os.getenv("HUB_SSL_KEYFILE_PASSWORD")
    if password:
        opts["ssl_keyfile_password"] = password
    return opts


def main() -> None:
    force_utf8_output()

    host = (os.getenv("HUB_HOST") or "0.0.0.0").strip()
    #"Ved test skriv 8001"
    port = _int_env("HUB_PORT", 8000)
    workers = _int_env("HUB_WORKERS", 1)
    access_log = _flag("HUB_ACCESS_LOG", False)
    trust_proxy = _flag("TRUST_PROXY", False)
    tls = _tls_options()

    # SECRET_KEY tjekkes her OG i app.py. Her, fordi fejlen ellers først kommer
    # inde fra uvicorns worker-proces, hvor beskeden er svær at finde i en
    # tjeneste-log.
    if not os.getenv("SECRET_KEY") and os.getenv("DEV_MODE") != "1":
        raise SystemExit(
            "SECRET_KEY mangler. Generér én med:\n"
            '  python -c "import secrets; print(secrets.token_hex(32))"\n'
            "og skriv SECRET_KEY=<nøglen> i .env. Kør preflight.py for et "
            "samlet tjek."
        )

    import uvicorn

    scheme = "https" if tls else "http"
    print(f"Intomedia Hub starter på {scheme}://{host}:{port}"
          f"  (workers={workers}, proxy-headers={'til' if trust_proxy else 'fra'}"
          f", TLS={'til' if tls else 'fra'})")
    sys.stdout.flush()

    uvicorn.run(
        "app:app",
        host=host,
        port=port,
        workers=workers if workers > 1 else None,
        reload=False,
        access_log=access_log,
        proxy_headers=trust_proxy,
        # Uden dette ignorerer uvicorn X-Forwarded-For uanset proxy_headers.
        forwarded_allow_ips="*" if trust_proxy else None,
        **tls,
    )


if __name__ == "__main__":
    main()

"""SMTP-baseret mail-afsendelse til barselsplanlæggeren.

Bruger Microsoft 365 (smtp.office365.com) med STARTTLS som default, men
kan styres via .env. Alle indstillinger er valgfrie — hvis SMTP_HOST eller
SMTP_USER ikke er sat logger funktionen en advarsel og returnerer uden at
fejle, så godkendelses-flowet altid fungerer selv hvis mailen ikke kan
sendes.

Env-variabler (i .env):
    SMTP_HOST=smtp.office365.com
    SMTP_PORT=587
    SMTP_USER=barsel@intomedia.dk
    SMTP_PASSWORD=...
    SMTP_FROM=barsel@intomedia.dk          # afsender — kan adskille sig fra SMTP_USER
    SMTP_FROM_NAME=Intomedia Hub
    SMTP_USE_TLS=1                          # 1 = STARTTLS (default), 0 = ren SMTP
    SMTP_CA_BUNDLE=                         # valgfri sti til CA-bundle (.pem)
    SMTP_SSL_VERIFY=1                       # 0 = spring cert-verificering over

Bemærk: Office 365 kræver enten en app-password på kontoen eller at
"Authenticated SMTP" er slået til for mailboksen via M365 admin center.

Python 3.13 håndhæver strengere X.509-regler. Hvis du får
"CERTIFICATE_VERIFY_FAILED" (typisk på virksomhedsnetværk med
TLS-inspektion), så sæt enten SMTP_CA_BUNDLE til virksomhedens root-cert,
eller SMTP_SSL_VERIFY=0 som lynfix.
"""
import logging
import os
import re
import smtplib
import ssl
from email.message import EmailMessage
from email.utils import formataddr

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Hjælpere
# ---------------------------------------------------------------------------

_EMAIL_SPLIT_RE = re.compile(r"[,;\s]+")


def parse_recipients(raw: str) -> list[str]:
    """Tager en string (komma/semikolon-separeret) og returnerer en liste af
    e-mailadresser. Tomme felter ignoreres."""
    if not raw:
        return []
    out = []
    for part in _EMAIL_SPLIT_RE.split(raw):
        addr = part.strip()
        if addr and "@" in addr:
            out.append(addr)
    return out


# ---------------------------------------------------------------------------
# Selve afsendelsen
# ---------------------------------------------------------------------------

def _send_smtp(to_addrs: list[str], subject: str, text_body: str,
               html_body: str | None = None) -> bool:
    """Send mail via SMTP. Returnerer True hvis afsendt, False hvis sprunget
    over eller fejlet. Fejler aldrig hårdt — kalderen kan ignorere resultatet."""

    host = os.getenv("SMTP_HOST", "smtp.office365.com")
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USER", "")
    password = os.getenv("SMTP_PASSWORD", "")
    sender = os.getenv("SMTP_FROM", user) or user
    sender_name = os.getenv("SMTP_FROM_NAME", "Intomedia Hub")
    use_tls = os.getenv("SMTP_USE_TLS", "1") != "0"
    ca_bundle = os.getenv("SMTP_CA_BUNDLE", "").strip()
    ssl_verify = os.getenv("SMTP_SSL_VERIFY", "1") != "0"

    if not host or not user or not password or not sender:
        logger.warning(f"SMTP ikke konfigureret — springer afsendelse over (modtagere: {to_addrs})")
        return False
    if not to_addrs:
        logger.info("Ingen modtagere — springer afsendelse over")
        return False

    msg = EmailMessage()
    msg["From"] = formataddr((sender_name, sender))
    # Brug Bcc så modtagerne ikke kan se hinanden (HR-distributionsliste)
    msg["To"] = formataddr((sender_name, sender))
    msg["Bcc"] = ", ".join(to_addrs)
    msg["Subject"] = subject
    msg.set_content(text_body)
    if html_body:
        msg.add_alternative(html_body, subtype="html")

    # Byg SSL-context: prøv først OS-trust store (truststore), så certifi
    # via SMTP_CA_BUNDLE, og fald sidst tilbage til Pythons default. Sæt
    # SMTP_SSL_VERIFY=0 for at deaktivere verifikation helt (lynfix når
    # virksomhedsproxyen laver TLS-inspektion med et ugyldigt root-cert).
    try:
        if not ssl_verify:
            ctx = ssl._create_unverified_context()
            logger.warning("SSL-verificering deaktiveret (SMTP_SSL_VERIFY=0)")
        elif ca_bundle:
            ctx = ssl.create_default_context(cafile=ca_bundle)
        else:
            try:
                import truststore  # type: ignore
                ctx = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            except ImportError:
                ctx = ssl.create_default_context()
    except Exception as e:
        logger.warning(f"Kunne ikke bygge SSL-context: {e} — bruger default")
        ctx = ssl.create_default_context()

    try:
        if use_tls:
            with smtplib.SMTP(host, port, timeout=15) as s:
                s.ehlo()
                s.starttls(context=ctx)
                s.ehlo()
                s.login(user, password)
                s.send_message(msg)
        else:
            with smtplib.SMTP_SSL(host, port, timeout=15, context=ctx) as s:
                s.login(user, password)
                s.send_message(msg)
        logger.info(f"Mail sendt til {len(to_addrs)} modtager(e): {to_addrs}")
        return True
    except Exception:
        # Returværdi-kontrakt: False — mail-fejl må aldrig vælte godkendelses-flowet
        logger.exception("Mailafsendelse fejlede")
        return False


# ---------------------------------------------------------------------------
# Indhold: godkendelses-notifikation
# ---------------------------------------------------------------------------

def _safe(v) -> str:
    return str(v) if v not in (None, "") else "—"


def send_approval_notification(case: dict, notify_emails_raw: str) -> bool:
    """Send mail om at en barselsplan er blevet godkendt.

    `case` skal være output fra queries.get_case() (camelCase). `notify_emails_raw`
    er strengen fra BarselSettings.notify_emails.
    """
    recipients = parse_recipients(notify_emails_raw)
    if not recipients:
        logger.info("notify_emails er tom — springer afsendelse over")
        return False

    medarbejder = case.get("hubUserName") or case.get("morNavn") or case.get("farNavn") or "Medarbejder"
    termin      = case.get("termin")      or "—"
    foedsel     = case.get("foedselDato") or "(termin)"
    approver    = case.get("approvedByName") or "—"

    subject = f"Barselsplan godkendt — {medarbejder}"

    text_body = (
        f"En barselsplan er netop blevet godkendt.\n\n"
        f"Medarbejder:        {medarbejder}\n"
        f"Mor:                {_safe(case.get('morNavn'))}"
        f"{'  (ansat i Intomedia)' if case.get('morAnsat') else ''}\n"
        f"Far / medmor:       {_safe(case.get('farNavn'))}"
        f"{'  (ansat i Intomedia)' if case.get('farAnsat') else ''}\n"
        f"Terminsdato:        {termin}\n"
        f"Faktisk fødselsdato:{_safe(foedsel)}\n"
        f"Godkendt af:        {approver}\n\n"
        f"Log ind på Intomedia Hub for at se den fulde plan:\n"
        f"https://hub.intomedia.dk/tool/barselsberegner\n"
    )

    html_body = f"""\
<!DOCTYPE html>
<html><body style="font-family:Segoe UI,Arial,sans-serif;color:#1c1c1e">
  <h2 style="color:#1A3650;margin-bottom:4px">🍼 Barselsplan godkendt</h2>
  <p style="color:#4b5563;margin-top:0">En barselsplan er netop blevet godkendt og kan hentes i Intomedia Hub.</p>

  <table cellpadding="6" cellspacing="0" style="border-collapse:collapse;font-size:14px;margin-top:12px">
    <tr><td style="color:#6b7280">Medarbejder</td>
        <td><strong>{_safe(medarbejder)}</strong></td></tr>
    <tr><td style="color:#6b7280">Mor</td>
        <td>{_safe(case.get('morNavn'))}{' &nbsp;<span style="color:#1A5C38">· ansat i Intomedia</span>' if case.get('morAnsat') else ''}</td></tr>
    <tr><td style="color:#6b7280">Far / medmor</td>
        <td>{_safe(case.get('farNavn'))}{' &nbsp;<span style="color:#1A5C38">· ansat i Intomedia</span>' if case.get('farAnsat') else ''}</td></tr>
    <tr><td style="color:#6b7280">Terminsdato</td>
        <td>{_safe(termin)}</td></tr>
    <tr><td style="color:#6b7280">Faktisk fødselsdato</td>
        <td>{_safe(foedsel)}</td></tr>
    <tr><td style="color:#6b7280">Godkendt af</td>
        <td>{_safe(approver)}</td></tr>
  </table>

  <p style="margin-top:18px">
    <a href="https://hub.intomedia.dk/tool/barselsberegner"
       style="display:inline-block;padding:9px 18px;background:#2563A8;color:#fff;
              text-decoration:none;border-radius:8px;font-weight:600">
      Åbn Barselsplanlæggeren
    </a>
  </p>

  <p style="font-size:12px;color:#9ca3af;margin-top:24px">
    Denne mail er sendt automatisk fra Intomedia Hub. Modtagerlisten kan
    justeres af admin i barselsindstillingerne.
  </p>
</body></html>
"""

    return _send_smtp(recipients, subject, text_body, html_body)

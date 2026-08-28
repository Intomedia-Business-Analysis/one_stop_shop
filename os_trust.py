"""Udgående HTTPS gennem OS'ets certifikatlager.

Virksomhedsproxyen (Zscaler) laver TLS-inspektion med et eget root-certifikat.
Det ligger i Windows' certifikatlager, men ikke i certifi's bundle, som
requests bruger som standard — derfor fejler kald til f.eks. api.pipedrive.com
med CERTIFICATE_VERIFY_FAILED. `truststore` kan bygge en SSL-context oven på
OS-lageret og løser det.

Hvorfor ikke truststore.inject_into_ssl(), som modulerne brugte før: den
udskifter ssl.SSLContext GLOBALT i processen — også den context uvicorn bygger
for at TERMINERE TLS. En truststore-context på serversiden forsøger at
verificere klientens certifikat; browsere sender ingen, og hver eneste
forbindelse afbrydes så med 'Peer sent no certificates to verify'. Symptomet er
en server der lytter, accepterer TCP og lukker midt i håndtrykket — uden en
linje i loggen. Se README, afsnittet HTTPS.

Derfor er trust-storen her scopet til de udgående kald der har brug for den:

    from os_trust import session
    resp = session().get(url, timeout=30)

Findes truststore ikke, falder vi tilbage til requests' normale opførsel
(certifi). Så virker det uden for virksomhedsnettet, hvor der ikke er nogen
inspektionsproxy at tage højde for.
"""
from __future__ import annotations

import ssl
import threading

import requests
from requests.adapters import HTTPAdapter

_lock = threading.Lock()
_session: requests.Session | None = None


def _os_trust_adapter() -> HTTPAdapter | None:
    try:
        import truststore
    except Exception:
        return None

    ctx = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    class _Adapter(HTTPAdapter):
        def init_poolmanager(self, *args, **kwargs):
            kwargs["ssl_context"] = ctx
            return super().init_poolmanager(*args, **kwargs)

        def proxy_manager_for(self, *args, **kwargs):
            kwargs["ssl_context"] = ctx
            return super().proxy_manager_for(*args, **kwargs)

    return _Adapter()


def session() -> requests.Session:
    """Delt requests-session. Idempotent og trådsikker."""
    global _session
    if _session is not None:
        return _session
    with _lock:
        if _session is None:
            s = requests.Session()
            adapter = _os_trust_adapter()
            if adapter is not None:
                s.mount("https://", adapter)
            _session = s
    return _session

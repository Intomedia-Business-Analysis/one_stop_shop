"""Pipedrive API-klient til Klippekort — opdatér "klip brugt" på en deal.

JP/POL Advertising-kontoen har sit eget API-token og sit eget custom-felt-key
for used_clip_cards. Værdier matcher pipedrive_sync/config.py['jppol_advertising']
så de holdes i sync med det projekt der trækker data tilbage i PipedriveDeals.

Flow: toolet registrerer et forbrug lokalt og kalder update_used_clip_cards med
det nye kumulative antal brugte klip. Næste sync henter feltet ned i tabellen.
"""
from __future__ import annotations

import os
import time

import requests
from dotenv import load_dotenv

load_dotenv()

# Virksomhedsproxy (Zscaler) laver TLS-inspektion med eget root-cert, som ikke
# ligger i certifi's bundle → SSL-fejl mod api.pipedrive.com. truststore bruger
# OS'ets certifikatlager (hvor virksomhedens root ligger) og løser det globalt
# for alle requests-kald i processen. Samme tilgang som modul_barsel/mail.py.
try:
    import truststore
    truststore.inject_into_ssl()
except Exception:
    pass

BASE_URL = "https://api.pipedrive.com/v1"
MAX_RETRIES = 3

# Env-variabel med API-token for JP/POL Advertising (samme navn som sync-projektet).
JPPOL_TOKEN_ENV = "PD_TOKEN_JPPOL"

# Custom-felt-key for "klip brugt" (used_clip_cards) på jppol_advertising-kontoen.
# Kopieret fra pipedrive_sync/config.py['jppol_advertising'].field_map.
USED_CLIP_FIELD_KEY = "83f34a5fb1a534f807a846950b2ac41c6436d7eb"


def _get_token() -> str | None:
    return os.getenv(JPPOL_TOKEN_ENV)


def fetch_org_owners(needed_ids) -> dict:
    """Hent organisationernes ejere fra Pipedrive for de ønskede org_id'er.

    Paginerer /organizations (500 ad gangen) og stopper når alle ønskede id'er
    er fundet (eller der ikke er flere sider). Returnerer {org_id: (navn, email)}.
    Kaster ikke — returnerer det den nåede ved fejl/manglende token.
    """
    token = _get_token()
    if not token:
        return {}
    needed = set()
    for x in (needed_ids or []):
        try:
            needed.add(int(x))
        except (TypeError, ValueError):
            pass
    out: dict = {}
    start = 0
    while needed:
        try:
            resp = requests.get(
                f"{BASE_URL}/organizations",
                params={"api_token": token, "limit": 500, "start": start},
                timeout=60,
            )
            if resp.status_code == 429:
                time.sleep(int(resp.headers.get("Retry-After", 5)))
                continue
            resp.raise_for_status()
            body = resp.json()
        except (requests.RequestException, ValueError):
            break
        for o in (body.get("data") or []):
            oid = o.get("id")
            if oid in needed:
                owner = o.get("owner_id") or {}
                out[oid] = (owner.get("name"), owner.get("email"))
                needed.discard(oid)
        pag = (body.get("additional_data") or {}).get("pagination", {})
        if pag.get("more_items_in_collection") and needed:
            start += 500
        else:
            break
    return out


def update_used_clip_cards(pd_deal_id: int, new_used: int) -> dict:
    """Sæt 'klip brugt' på en Pipedrive-deal til new_used.

    Returnerer en dict med ok-flag — kaster IKKE ved manglende token eller API-fejl,
    så det lokale forbrug stadig kan logges (graceful degradation). Frontend viser
    en advarsel hvis ok er false.
    """
    token = _get_token()
    if not token:
        return {
            "ok": False,
            "reason": f"{JPPOL_TOKEN_ENV} mangler i .env — klip blev logget lokalt, "
                      f"men 'klip brugt' kunne ikke opdateres i Pipedrive.",
        }

    url = f"{BASE_URL}/deals/{int(pd_deal_id)}"
    payload = {USED_CLIP_FIELD_KEY: str(new_used)}
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.put(url, params={"api_token": token}, json=payload, timeout=30)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 5))
                time.sleep(wait)
                continue
            if resp.status_code >= 400:
                try:
                    body = resp.json()
                    last_err = body.get("error") or body.get("error_info") or body
                except Exception:
                    last_err = resp.text[:300]
                return {"ok": False, "reason": f"Pipedrive PUT {resp.status_code}: {last_err}"}
            body = resp.json()
            if not body.get("success"):
                return {"ok": False, "reason": f"Pipedrive-fejl: {body.get('error', body)}"}
            return {"ok": True, "deal_id": int(pd_deal_id), "used_clip_cards": new_used}
        except requests.RequestException as e:
            last_err = str(e)
            time.sleep(1)
    return {"ok": False, "reason": f"Pipedrive utilgængelig efter {MAX_RETRIES} forsøg: {last_err}"}

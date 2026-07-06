"""Pipedrive API-klient (v2) til Klippekort — opdatér "klip brugt" på en deal.

JP/POL Advertising-kontoen har sit eget API-token og sit eget custom-felt-key
for used_clip_cards. Værdier matcher pipedrive_sync/config.py['jppol_advertising']
så de holdes i sync med det projekt der trækker data tilbage i PipedriveDeals.

Flow: toolet registrerer et forbrug lokalt og kalder add_used_clip_cards med
delta'en (antal klip lige registreret/slettet). Funktionen læser Pipedrives
nuværende used_clip_cards live og lægger delta oveni — så toolet aldrig
overskriver klip det ikke selv har registreret (fx 4 klip sat direkte i
Pipedrive). Næste sync henter det opdaterede felt ned i PipedriveDeals, som er
den autoritative kilde til 'Brugt' i dashboardet.

API v2 (v1 lukker 31. juli 2026):
  - Auth via x-api-token-headeren (api_token som query-param findes ikke i v2).
  - Custom fields ligger under data.custom_fields og skrives som
    {"custom_fields": {<felt-key>: <typet værdi>}} med PATCH (før PUT).
  - Pagination er cursor-baseret (additional_data.next_cursor) i stedet for start.
  - Organisationers owner_id er nu kun et tal — navn/email slås op via /v1/users,
    som ikke har noget v2-modstykke og derfor ikke udfases.
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

BASE_URL = "https://api.pipedrive.com/api/v2"
BASE_URL_V1 = "https://api.pipedrive.com/v1"
PAGE_LIMIT = 500
MAX_RETRIES = 3

# Env-variabel med API-token for JP/POL Advertising (samme navn som sync-projektet).
JPPOL_TOKEN_ENV = "PD_TOKEN_JPPOL"

# Custom-felt-key for "klip brugt" (used_clip_cards) på jppol_advertising-kontoen.
# Kopieret fra pipedrive_sync/config.py['jppol_advertising'].field_map.
# Felttypen er 'double' i Pipedrive, så værdien skrives som tal (ikke streng).
USED_CLIP_FIELD_KEY = "83f34a5fb1a534f807a846950b2ac41c6436d7eb"

def _get_token() -> str | None:
    return os.getenv(JPPOL_TOKEN_ENV)


def _headers(token: str) -> dict:
    return {"x-api-token": token}


def _fetch_user_map(token: str) -> dict:
    """Hent {user_id: (navn, email)} for kontoens brugere via /v1/users.

    v2's organizations-svar indeholder kun owner_id som tal, hvor v1 medsendte
    ejerens navn og email inline — så de slås op i ét samlet kald her.
    Kaster ikke — returnerer tom dict ved fejl/manglende adgang.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                f"{BASE_URL_V1}/users", headers=_headers(token), timeout=60,
            )
            if resp.status_code == 429:
                time.sleep(int(resp.headers.get("Retry-After", 5)))
                continue
            if resp.status_code >= 400:
                return {}
            body = resp.json()
            if not body.get("success"):
                return {}
            return {
                u["id"]: (u.get("name"), u.get("email"))
                for u in (body.get("data") or [])
                if u.get("id") is not None
            }
        except (requests.RequestException, ValueError):
            time.sleep(1)
    return {}


def fetch_org_owners(needed_ids) -> dict:
    """Hent organisationernes ejere fra Pipedrive for de ønskede org_id'er.

    Paginerer /organizations (500 ad gangen, cursor-baseret) og stopper når alle
    ønskede id'er er fundet (eller der ikke er flere sider). Returnerer
    {org_id: (navn, email)}. Kaster ikke — returnerer det den nåede ved fejl/
    manglende token.
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
    if not needed:
        return {}
    users = _fetch_user_map(token)
    out: dict = {}
    cursor: str | None = None
    while needed:
        params: dict = {"limit": PAGE_LIMIT}
        if cursor:
            params["cursor"] = cursor
        try:
            resp = requests.get(
                f"{BASE_URL}/organizations",
                headers=_headers(token),
                params=params,
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
                out[oid] = users.get(o.get("owner_id"), (None, None))
                needed.discard(oid)
        cursor = (body.get("additional_data") or {}).get("next_cursor")
        if not cursor:
            break
    return out


def fetch_org_owners_by_ids(ids) -> dict:
    """Hent ejer for specifikke org_id'er via GET /organizations/{id}.

    Målrettet og billigt når kun få (nyligt tilkomne) org'er mangler i cachen —
    modsat fetch_org_owners() der sider gennem ALLE organisationer. Returnerer
    {org_id: (navn, email)}. Kaster ikke — springer blot en org over ved fejl.
    """
    token = _get_token()
    if not token:
        return {}
    wanted = []
    for raw_id in (ids or []):
        try:
            wanted.append(int(raw_id))
        except (TypeError, ValueError):
            continue
    if not wanted:
        return {}
    users = _fetch_user_map(token)
    out: dict = {}
    for oid in wanted:
        url = f"{BASE_URL}/organizations/{oid}"
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = requests.get(url, headers=_headers(token), timeout=30)
                if resp.status_code == 429:
                    time.sleep(int(resp.headers.get("Retry-After", 5)))
                    continue
                if resp.status_code >= 400:
                    break
                body = resp.json()
                if not body.get("success"):
                    break
                o = body.get("data") or {}
                out[oid] = users.get(o.get("owner_id"), (None, None))
                break
            except (requests.RequestException, ValueError):
                time.sleep(1)
    return out


def fetch_used_clip_cards(pd_deal_id: int) -> int | None:
    """Hent den nuværende 'klip brugt' (used_clip_cards) LIVE fra Pipedrive.

    Bruges som grundtal til additiv opdatering, så toolet lægger sin delta oveni
    Pipedrives autoritative tal i stedet for at overskrive det. Læser live (ikke
    fra synkede PipedriveDeals), så på hinanden følgende registreringer akkumulerer
    korrekt uafhængigt af sync-intervallet. Returnerer antallet, 0 hvis feltet er
    tomt, eller None hvis token mangler / dealen ikke kunne læses.
    """
    token = _get_token()
    if not token:
        return None
    url = f"{BASE_URL}/deals/{int(pd_deal_id)}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=_headers(token), timeout=30)
            if resp.status_code == 429:
                time.sleep(int(resp.headers.get("Retry-After", 5)))
                continue
            if resp.status_code >= 400:
                return None
            body = resp.json()
            if not body.get("success"):
                return None
            custom = (body.get("data") or {}).get("custom_fields") or {}
            raw = custom.get(USED_CLIP_FIELD_KEY)
            if raw is None or str(raw).strip() == "":
                return 0
            try:
                return int(float(str(raw)))
            except (TypeError, ValueError):
                return 0
        except requests.RequestException:
            time.sleep(1)
    return None


def add_used_clip_cards(pd_deal_id: int, delta: int) -> dict:
    """Læg delta til Pipedrives nuværende used_clip_cards (additivt, bunder ved 0).

    delta kan være negativ (fx ved sletning af et job). Læser det aktuelle tal
    live fra Pipedrive og pusher current + delta, så toolet aldrig overskriver
    eksisterende klip det ikke selv har registreret. Kaster IKKE — returnerer
    {ok: False, reason} ved manglende token eller hvis tallet ikke kunne læses.
    """
    current = fetch_used_clip_cards(pd_deal_id)
    if current is None:
        return {
            "ok": False,
            "reason": f"Kunne ikke læse nuværende 'klip brugt' fra Pipedrive "
                      f"(token mangler eller deal utilgængelig) — klip blev logget "
                      f"lokalt, men Pipedrive blev ikke opdateret.",
        }
    new_used = max(0, current + int(delta))
    return update_used_clip_cards(pd_deal_id, new_used)


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
    payload = {"custom_fields": {USED_CLIP_FIELD_KEY: int(new_used)}}
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.patch(url, headers=_headers(token), json=payload, timeout=30)
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
                return {"ok": False, "reason": f"Pipedrive PATCH {resp.status_code}: {last_err}"}
            body = resp.json()
            if not body.get("success"):
                return {"ok": False, "reason": f"Pipedrive-fejl: {body.get('error', body)}"}
            return {"ok": True, "deal_id": int(pd_deal_id), "used_clip_cards": new_used}
        except requests.RequestException as e:
            last_err = str(e)
            time.sleep(1)
    return {"ok": False, "reason": f"Pipedrive utilgængelig efter {MAX_RETRIES} forsøg: {last_err}"}

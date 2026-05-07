"""Pipedrive API-klient til Portfolio Alignment — opret alignment-deals.

Hver Pipedrive-konto (scope) har sit eget API-token og sine egne pipelines,
stages og custom-felt-IDs. Tokens læses fra .env (samme navngivning som
pipedrive_sync-projektet bruger).

Anvendelse:
    create_alignment_deal(scope='watch_medier', org_id=12345,
                           site='finanswatch.dk', diff=12345.67, dry_run=False)

For diff > 0  → "pipeline cancellation"-pipeline (PD over-recorded)
For diff < 0  → "customer pipeline"-pipeline       (PD under-recorded)
Title         = "Porteføljeafstemning 2026"
Administrativ = Yes
Sites         = option der matcher den normaliserede site fra alignment-tabellen
Value         = abs(diff) i DKK
"""
from __future__ import annotations

import os
import time
from typing import Optional

import requests
from dotenv import load_dotenv

from moduler.modul_portfolio_alignment.queries import (
    ACCOUNT_SCOPES,
    normalize_site,
)

load_dotenv()


BASE_URL = "https://api.pipedrive.com/v1"
PAGE_LIMIT = 500
MAX_RETRIES = 3
DEAL_TITLE = "Porteføljeafstemning 2026"

# scope-id i one_stop_shop → env-variabel der indeholder API-token.
# Disse navne matcher .env-tokens som brugeren har lagt ind.
SCOPE_TOKEN_ENV: dict[str, str] = {
    "watch_medier": "PD_TOKEN_WATCH_DK_FINANS",
    "watch_no":     "PD_TOKEN_WATCH_NO",
    "watch_se":     "PD_TOKEN_WATCH_SE",
    "watch_de":     "PD_TOKEN_WATCH_DE",
    "monitor":      "PD_TOKEN_MONITOR",
}

# Custom-felt-keys for Administrativ pr. scope. Hver Pipedrive-konto har sit
# eget hash-baserede field key — navnet er ikke nødvendigvis 'Administrativ'.
# Værdier kopieret fra pipedrive_sync/config.py så de holdes i sync.
# Tom streng → opslag via navn (bruges på watch_de hvor feltet bogstaveligt
# hedder 'Administrativ').
ADMIN_FIELD_KEY: dict[str, str] = {
    "watch_medier": "df6dd5cbd8bff4ab30974bbf18b53e8fb8c98ccf",
    "watch_no":     "5494a067b751fedb4457719bfa0bf1a77ebc32e7",
    "watch_se":     "8765a546fe66531b80649dbc10644b5f86607bda",
    "watch_de":     "",
    "monitor":      "a82c8f6f7fa3a2b87103025e9a4f474d7f9710d9",
}

# Pipedrive-pipeline-navne pr. fortegn på diff. Vi matcher case-insensitivt
# substring så små variationer i navngivning på tværs af accounts ikke vælter.
PIPELINE_KEYWORDS_FOR_POS_DIFF = ["cancellation"]   # PD > Zuora → opsigelse
PIPELINE_KEYWORDS_FOR_NEG_DIFF = ["customer"]        # Zuora > PD → manglende deal

# Cache af metadata pr. token (pipelines, stages, deal_fields). Disse er
# ret stabile og bør kun hentes én gang pr. proces.
_META_CACHE: dict[str, dict] = {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_token(scope: str) -> str:
    env_name = SCOPE_TOKEN_ENV.get(scope)
    if not env_name:
        raise ValueError(f"Ukendt scope: {scope!r}. Mulige: {list(SCOPE_TOKEN_ENV)}")
    token = os.getenv(env_name)
    if not token:
        raise RuntimeError(
            f"Mangler API-token i .env: {env_name} (for scope {scope!r}). "
            f"Tilføj fx '{env_name}=<token>' i .env."
        )
    return token


def _api_get(token: str, path: str, params: Optional[dict] = None) -> list | dict:
    """Hent én side. Håndterer 429-rate-limit med Retry-After."""
    p = dict(params or {})
    p["api_token"] = token
    url = f"{BASE_URL}{path}"
    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.get(url, params=p, timeout=30)
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 5))
            time.sleep(wait)
            continue
        resp.raise_for_status()
        body = resp.json()
        if not body.get("success"):
            raise RuntimeError(f"Pipedrive GET {path} fejl: {body.get('error', body)}")
        return body.get("data") or []
    raise RuntimeError(f"Pipedrive GET {path} opgav efter {MAX_RETRIES} forsøg")


def _api_get_all(token: str, path: str, params: Optional[dict] = None) -> list:
    """Henter alle sider (pagination)."""
    out: list = []
    start = 0
    p = dict(params or {})
    p["limit"] = PAGE_LIMIT
    while True:
        p["start"] = start
        p["api_token"] = token
        url = f"{BASE_URL}{path}"
        for attempt in range(1, MAX_RETRIES + 1):
            resp = requests.get(url, params=p, timeout=30)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 5))
                time.sleep(wait)
                continue
            resp.raise_for_status()
            break
        body = resp.json()
        if not body.get("success"):
            raise RuntimeError(f"Pipedrive GET {path} fejl: {body.get('error', body)}")
        out.extend(body.get("data") or [])
        pagination = (body.get("additional_data") or {}).get("pagination", {})
        if pagination.get("more_items_in_collection"):
            start += PAGE_LIMIT
        else:
            break
    return out


def _api_post(token: str, path: str, payload: dict) -> dict:
    url = f"{BASE_URL}{path}"
    resp = requests.post(url, params={"api_token": token}, json=payload, timeout=30)
    if resp.status_code >= 400:
        try:
            body = resp.json()
            err = body.get("error") or body.get("error_info") or body
        except Exception:
            err = resp.text[:300]
        raise RuntimeError(f"Pipedrive POST {path} {resp.status_code}: {err}")
    body = resp.json()
    if not body.get("success"):
        raise RuntimeError(f"Pipedrive POST {path} fejl: {body.get('error', body)}")
    return body.get("data") or {}


# ---------------------------------------------------------------------------
# Metadata-lookup (cached pr. scope-token)
# ---------------------------------------------------------------------------

def _get_meta(scope: str) -> dict:
    """Hent og cache pipelines, stages og deal_fields for et scope."""
    token = _get_token(scope)
    if token in _META_CACHE:
        return _META_CACHE[token]

    pipelines_raw = _api_get(token, "/pipelines")
    stages_raw    = _api_get_all(token, "/stages")
    fields_raw    = _api_get_all(token, "/dealFields")

    pipelines = [
        {"id": p["id"], "name": p.get("name", "")}
        for p in (pipelines_raw or [])
    ]
    stages = [
        {
            "id":          s["id"],
            "name":        s.get("name", ""),
            "pipeline_id": s.get("pipeline_id"),
            "order_nr":    s.get("order_nr", 0),
        }
        for s in stages_raw
    ]
    fields_by_name: dict[str, dict] = {}
    fields_by_key:  dict[str, dict] = {}
    for f in fields_raw:
        opts = {opt["id"]: opt.get("label", "") for opt in (f.get("options") or [])}
        field_data = {
            "key":        f["key"],
            "name":       f.get("name", ""),
            "field_type": f.get("field_type", ""),
            "options":    opts,
        }
        fields_by_name[f["name"].strip().lower()] = field_data
        fields_by_key[f["key"]] = field_data

    meta = {
        "pipelines":      pipelines,
        "stages":         stages,
        "fields_by_name": fields_by_name,
        "fields_by_key":  fields_by_key,
    }
    _META_CACHE[token] = meta
    return meta


def _find_pipeline_by_keyword(meta: dict, keywords: list[str]) -> dict:
    """Vælg pipeline hvis navn (case-insensitivt) indeholder ét af keywords."""
    for kw in keywords:
        kw_low = kw.lower()
        for p in meta["pipelines"]:
            if kw_low in p["name"].lower():
                return p
    raise RuntimeError(
        f"Ingen pipeline matcher nogle af: {keywords}. "
        f"Eksisterende: {[p['name'] for p in meta['pipelines']]}"
    )


def _first_stage_of_pipeline(meta: dict, pipeline_id: int) -> dict:
    """Returnér stage med laveste order_nr i pipelinen."""
    pipeline_stages = [s for s in meta["stages"] if s["pipeline_id"] == pipeline_id]
    if not pipeline_stages:
        raise RuntimeError(f"Pipeline {pipeline_id} har ingen stages")
    return sorted(pipeline_stages, key=lambda s: s.get("order_nr", 0))[0]


def _resolve_field(meta: dict, field_name: str) -> dict:
    f = meta["fields_by_name"].get(field_name.strip().lower())
    if not f:
        raise RuntimeError(
            f"Custom-felt {field_name!r} blev ikke fundet i Pipedrive (deal fields)."
        )
    return f


def _option_id_by_label_match(field: dict, predicate) -> Optional[int]:
    for opt_id, label in field["options"].items():
        if predicate(label):
            return opt_id
    return None


def _site_option_id(field: dict, normalized_site: str) -> int:
    """Find option-ID for sites-feltet hvor labelen normaliserer til samme site.

    Bruger samme normalize_site() som alignment-tabellen, så fx
    'FinansWatch DK' (Pipedrive-label) matcher 'finanswatch.dk' (vores nøgle).
    """
    target = normalize_site(normalized_site) or normalized_site.lower()
    candidates = []
    for opt_id, label in field["options"].items():
        nl = normalize_site(label)
        if nl == target:
            candidates.append((opt_id, label))
    if not candidates:
        labels = ", ".join(sorted(field["options"].values()))
        raise RuntimeError(
            f"Ingen Sites-option matcher {normalized_site!r}. Tilgængelige: {labels}"
        )
    candidates.sort(key=lambda x: len(x[1]))
    return candidates[0][0]


def _admin_yes_option_id(field: dict) -> int:
    if field.get("field_type") not in ("enum", "set"):
        return 1
    opt = _option_id_by_label_match(
        field,
        lambda lbl: lbl.strip().lower() in ("yes", "ja", "true", "1"),
    )
    if opt is not None:
        return opt
    raise RuntimeError(
        f"Kunne ikke finde 'Yes/Ja'-option i administrativ-feltet. "
        f"Options: {field['options']}"
    )


# ---------------------------------------------------------------------------
# Public: opret deal
# ---------------------------------------------------------------------------

def create_alignment_deal(
    scope: str,
    org_id: int,
    site: str,
    diff: float,
    dry_run: bool = False,
) -> dict:
    """Opret en alignment-deal i Pipedrive.

    Returnerer: {ok, deal_id, deal_url, payload, pipeline, stage, dry_run}
    """
    if scope not in ACCOUNT_SCOPES:
        raise ValueError(f"Ukendt scope: {scope!r}")
    if not org_id:
        raise ValueError("org_id er påkrævet — kan ikke oprette deal uden organisation")
    if not site:
        raise ValueError("site er påkrævet")
    if diff is None or abs(diff) < 0.5:
        raise ValueError("diff skal være ≠ 0 — der er intet at afstemme")

    token = _get_token(scope)
    meta  = _get_meta(scope)

    keywords = (
        PIPELINE_KEYWORDS_FOR_POS_DIFF if diff > 0
        else PIPELINE_KEYWORDS_FOR_NEG_DIFF
    )
    pipeline = _find_pipeline_by_keyword(meta, keywords)
    stage    = _first_stage_of_pipeline(meta, pipeline["id"])

    sites_field = _resolve_field(meta, "Sites")
    site_opt_id = _site_option_id(sites_field, site)

    # Administrativ-feltet har forskellige hash-keys pr. konto, men hedder ikke
    # nødvendigvis 'Administrativ' i Pipedrive's UI. Slå op via hardkodet key
    # først; fald tilbage til navne-opslag (watch_de).
    admin_key = ADMIN_FIELD_KEY.get(scope, "")
    if admin_key:
        admin_field = meta["fields_by_key"].get(admin_key)
        if not admin_field:
            raise RuntimeError(
                f"Administrativ-feltet ({admin_key!r}) blev ikke fundet i Pipedrive "
                f"for scope {scope!r}. Tjek hash-keyen i pipedrive_sync/config.py."
            )
    else:
        admin_field = _resolve_field(meta, "Administrativ")
    admin_opt = _admin_yes_option_id(admin_field)

    # Værdi: cancellation = -diff (revenue går væk), customer-deal = -diff (positiv).
    # Begge fanges af samme udtryk fordi diff er signed: diff>0 → negativ værdi,
    # diff<0 → positiv værdi.
    deal_value = round(-diff, 2)

    payload: dict = {
        "title":    DEAL_TITLE,
        "value":    deal_value,
        "currency": "DKK",
        "org_id":   int(org_id),
        "stage_id": stage["id"],
        sites_field["key"]: str(site_opt_id),
        admin_field["key"]: str(admin_opt),
    }

    if dry_run:
        return {
            "ok":       True,
            "dry_run":  True,
            "payload":  payload,
            "pipeline": {"id": pipeline["id"], "name": pipeline["name"]},
            "stage":    {"id": stage["id"], "name": stage["name"]},
        }

    data = _api_post(token, "/deals", payload)
    deal_id = data.get("id")
    company_domain = data.get("company_domain") or ""
    deal_url = (
        f"https://{company_domain}.pipedrive.com/deal/{deal_id}"
        if company_domain and deal_id else None
    )

    return {
        "ok":       True,
        "dry_run":  False,
        "deal_id":  deal_id,
        "deal_url": deal_url,
        "payload":  payload,
        "pipeline": {"id": pipeline["id"], "name": pipeline["name"]},
        "stage":    {"id": stage["id"], "name": stage["name"]},
    }

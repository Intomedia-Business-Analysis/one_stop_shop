"""Pipedrive API-klient til Portfolio Alignment — opret alignment-deals.

Hver Pipedrive-konto (scope) har sit eget API-token og sine egne pipelines,
stages og custom-felt-IDs. Tokens læses fra .env (samme navngivning som
pipedrive_sync-projektet bruger).

Anvendelse:
    create_alignment_deal(scope='watch_medier', org_id=12345,
                           site='finanswatch.dk', diff=12345.67, dry_run=False)

For diff > 0  → "pipeline cancellation"-pipeline (PD over-recorded)
For diff < 0  → "customer pipeline"-pipeline       (PD under-recorded)
Title         = "Porteføljeafstemning <måned> <år>" (bygges dynamisk pr. kørsel)
Administrativ = Yes
Sites         = option der matcher den normaliserede site fra alignment-tabellen
Value         = abs(diff) i DKK
"""
from __future__ import annotations

import os
import time
from datetime import date
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

# Danske månedsnavne — bruges til at bygge titlen dynamisk så hver afstemning
# får den aktuelle måned + år, fx 'Porteføljeafstemning maj 2026'.
_DK_MONTHS = [
    "januar", "februar", "marts", "april", "maj", "juni",
    "juli", "august", "september", "oktober", "november", "december",
]


def get_deal_title(today: Optional[date] = None) -> str:
    """Returnér deal-titel for indeværende måned, fx 'Porteføljeafstemning maj 2026'.

    today-argumentet er kun til test — i produktion bruges date.today().
    """
    d = today or date.today()
    return f"Porteføljeafstemning {_DK_MONTHS[d.month - 1]} {d.year}"

# Alle afstemnings-deals stemples med samme service_activation_date
# (1. januar 2019). Det placerer dem klart i historisk tid så de ikke
# forveksles med nye salg, og holder samme dato på tværs af bulk-kørsler.
SERVICE_ACTIVATION_DATE = "2019-01-01"

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
# Tom streng → opslag via navn-varianter (administrativ/administrative/admin).
ADMIN_FIELD_KEY: dict[str, str] = {
    "watch_medier": "df6dd5cbd8bff4ab30974bbf18b53e8fb8c98ccf",
    "watch_no":     "5494a067b751fedb4457719bfa0bf1a77ebc32e7",
    "watch_se":     "8765a546fe66531b80649dbc10644b5f86607bda",
    "watch_de":     "0dfcf4ecb4a8ac64d36301a200f4c3d518641773",
    "monitor":      "a82c8f6f7fa3a2b87103025e9a4f474d7f9710d9",
}

# Navne der prøves hvis hash-opslag fejler (eller hash ikke er sat).
# Pipedrive-felter kan hedde 'Administrativ' på dk og 'Administrative' på de.
ADMIN_FIELD_NAME_VARIANTS = ["administrativ", "administrative", "admin"]

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


def _site_stem(s: str) -> str:
    """Returnér 'stam'-form: alt før første '.', lowercased, uden whitespace.

    Bruges til scope-bevidst matching: NO/SE/DE-konti har PD-options uden TLD
    ('MedWatch', 'FinansWatch') der ikke kan ramme alignment-target 'medwatch.no'
    via SITE_ALIASES (som peger på .dk). Stem'et er ens på begge sider.
    """
    s = (s or "").strip().lower().replace(" ", "")
    if "." in s:
        s = s.split(".", 1)[0]
    return s


def _site_option_id(field: dict, normalized_site: str) -> int:
    """Find option-ID for sites-feltet hvor labelen matcher target-sitet.

    Først forsøges fuld-normaliseret match (samme alias-tabel som alignment-
    tabellen — bevarer dækning for monitor-sites med æ/ae, kapwatch osv.).
    Hvis intet matcher falder vi tilbage til stem-match, så fx PD-NO's option
    'MedWatch' matcher target 'medwatch.no' selvom SITE_ALIASES['medwatch']
    peger på '.dk'.
    """
    target_full = normalize_site(normalized_site) or normalized_site.lower()
    target_stem = _site_stem(normalized_site)

    full_candidates = []
    stem_candidates = []
    for opt_id, label in field["options"].items():
        if normalize_site(label) == target_full:
            full_candidates.append((opt_id, label))
        if _site_stem(label) == target_stem:
            stem_candidates.append((opt_id, label))

    candidates = full_candidates or stem_candidates
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

def _build_payload(
    scope: str,
    org_id: int,
    site: str,
    diff_signed: float,
    currency: str,
) -> tuple[dict, dict, dict]:
    """Byg Pipedrive deal-payload + returnér (payload, pipeline, stage)."""
    meta = _get_meta(scope)

    keywords = (
        PIPELINE_KEYWORDS_FOR_POS_DIFF if diff_signed > 0
        else PIPELINE_KEYWORDS_FOR_NEG_DIFF
    )
    pipeline = _find_pipeline_by_keyword(meta, keywords)
    stage    = _first_stage_of_pipeline(meta, pipeline["id"])

    sites_field = _resolve_field(meta, "Sites")
    site_opt_id = _site_option_id(sites_field, site)

    admin_field = None
    admin_key = ADMIN_FIELD_KEY.get(scope, "")
    if admin_key:
        admin_field = meta["fields_by_key"].get(admin_key)
    if not admin_field:
        # Hash-opslag fejlede (eller var tom) — prøv navne-varianter.
        for name in ADMIN_FIELD_NAME_VARIANTS:
            admin_field = meta["fields_by_name"].get(name)
            if admin_field:
                break
    if not admin_field:
        raise RuntimeError(
            f"Administrativ-feltet blev ikke fundet i Pipedrive for scope {scope!r}. "
            f"Tjekkede hash {admin_key!r} og navne {ADMIN_FIELD_NAME_VARIANTS}. "
            f"Opdatér ADMIN_FIELD_KEY i pipedrive_api.py."
        )
    admin_opt = _admin_yes_option_id(admin_field)

    sad_field = _resolve_field(meta, "Service Activation Date")

    # Cancellation = -diff (negativ), customer-deal = -diff (positiv).
    deal_value = int(round(-diff_signed))

    payload: dict = {
        "title":    get_deal_title(),
        "value":    deal_value,
        "currency": currency,
        "org_id":   int(org_id),
        "stage_id": stage["id"],
        sites_field["key"]: str(site_opt_id),
        admin_field["key"]: str(admin_opt),
        sad_field["key"]:   SERVICE_ACTIVATION_DATE,
    }
    return payload, pipeline, stage


def preview_alignment_deal(
    scope: str,
    org_id: int,
    site: str,
    diff_signed: float,
    currency: str,
) -> dict:
    """Returnér samme felter som create_alignment_deal — men uden at POSTe."""
    if scope not in ACCOUNT_SCOPES:
        raise ValueError(f"Ukendt scope: {scope!r}")
    if not org_id:
        raise ValueError("org_id er påkrævet")
    if not site:
        raise ValueError("site er påkrævet")
    if diff_signed is None or abs(diff_signed) < 0.5:
        raise ValueError("diff skal være ≠ 0 — der er intet at afstemne")
    payload, pipeline, stage = _build_payload(scope, int(org_id), site, diff_signed, currency)
    return {
        "ok":       True,
        "dry_run":  True,
        "payload":  payload,
        "pipeline": {"id": pipeline["id"], "name": pipeline["name"]},
        "stage":    {"id": stage["id"], "name": stage["name"]},
        "currency": currency,
        "value":    payload["value"],
    }


def create_alignment_deal(
    scope: str,
    org_id: int,
    site: str,
    diff_signed: float,
    currency: str,
    dry_run: bool = False,
) -> dict:
    """Opret en alignment-deal i Pipedrive.

    Returnerer: {ok, deal_id, deal_url, payload, pipeline, stage, dry_run, currency, value}
    """
    if scope not in ACCOUNT_SCOPES:
        raise ValueError(f"Ukendt scope: {scope!r}")
    if not org_id:
        raise ValueError("org_id er paakraevet - kan ikke oprette deal uden organisation")
    if not site:
        raise ValueError("site er paakraevet")
    if diff_signed is None or abs(diff_signed) < 0.5:
        raise ValueError("diff skal vaere != 0 - der er intet at afstemme")

    token = _get_token(scope)
    payload, pipeline, stage = _build_payload(scope, int(org_id), site, diff_signed, currency)

    if dry_run:
        return {
            "ok":       True,
            "dry_run":  True,
            "payload":  payload,
            "pipeline": {"id": pipeline["id"], "name": pipeline["name"]},
            "stage":    {"id": stage["id"], "name": stage["name"]},
            "currency": currency,
            "value":    payload["value"],
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
        "currency": currency,
        "value":    payload["value"],
    }

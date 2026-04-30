"""Portefølje-alignment: sammenligner ACV i Pipedrive med ARR i Zuora.

Pipedrive ligger i INTOMEDIA (SQL Server, pymssql).
Zuora-data læses fra et ugentligt CSV/XLSX-eksport produceret af snapshot-querien
i Redshift via DataGrip — så vi undgår systemintegration mod koncernens dataplatform.

Forventet filplacering (kan overrides via ZUORA_SNAPSHOT_DIR i .env):
    .../Business Analysis/Porteføljer/ACV_snapshot_DDMMYYYY.csv (eller .xlsx)

Forventede kolonner i den rækkefølge:
    snapshot_date, account_number, pipedrive_id, brand, account_type,
    site, currency, arr_local, arr_dkk
(med eller uden header-række — vi læser positionelt.)

Match-nøgle: org_id (Pipedrive) = pipedrive_id (Zuora-account).
Site normaliseres til en kanonisk nøgle (typisk domæne, fx 'finanswatch.dk')
så Pipedrive's "FinansWatch DK" og Zuora's site-værdi mødes.
"""

import glob
import os
import re
import traceback
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import pandas as pd
import pymssql
from dotenv import load_dotenv

load_dotenv()


DEFAULT_SNAPSHOT_DIR = (
    Path.home()
    / "intomedia"
    / "Operations - Dokumenter"
    / "Business Analysis"
    / "Porteføljer"
)
SNAPSHOT_FILENAME_RE = re.compile(r"ACV_snapshot_(\d{2})(\d{2})(\d{4})", re.IGNORECASE)
SNAPSHOT_COLUMNS = [
    "snapshot_date", "account_number", "pipedrive_id", "brand", "account_type",
    "site", "currency", "arr_local", "arr_dkk",
]

# ---------------------------------------------------------------------------
# Account / scope-mapping mellem Pipedrive og Zuora
# ---------------------------------------------------------------------------
# Pipedrive's `account`-felt grupperer deals pr. forretningsenhed. Zuora's
# `brand` gør det samme, men med andre navne. For hvert scope angiver vi:
#   - label:        UI-tekst
#   - pd_account:   værdi i PipedriveDeals.account
#   - pd_deal_types: værdier i PipedriveDeals.deal_type vi tæller som ARR
#   - zuora_brands: værdier i Zuora-snapshot's brand-kolonne
# watch_medier mapper til to Zuora-brands ('Watch' + 'Finans') fordi
# FINANS DK historisk ligger under samme Pipedrive-konto.

ACCOUNT_SCOPES: dict[str, dict] = {
    "watch_medier": {
        "label":         "Watch Medier (DK + Finans)",
        "pd_account":    "watch_medier",
        "pd_deal_types": ["Abonnement"],
        "zuora_brands":  ["Watch", "Finans"],
    },
    "watch_no": {
        "label":         "Watch NO",
        "pd_account":    "watch_no",
        "pd_deal_types": ["Subscription"],
        "zuora_brands":  ["WatchMedierNO"],
    },
    "watch_se": {
        "label":         "Watch SE",
        "pd_account":    "watch_se",
        "pd_deal_types": ["Subscription"],
        "zuora_brands":  ["WatchMedierSE"],
    },
    "watch_de": {
        "label":         "Watch DE",
        "pd_account":    "watch_de",
        "pd_deal_types": ["Subscription", "Press review"],
        "zuora_brands":  ["WatchMedierDE"],
    },
    "monitor": {
        "label":         "Monitormedier",
        "pd_account":    "monitor",
        "pd_deal_types": ["Abonnement"],
        "zuora_brands":  ["Monitormedier"],
    },
}
SCOPE_BY_ZUORA_BRAND: dict[str, str] = {
    brand: scope_id
    for scope_id, cfg in ACCOUNT_SCOPES.items()
    for brand in cfg["zuora_brands"]
}


def list_account_scopes() -> list[dict]:
    """UI-venligt format: id + label, sorteret som vi har defineret dem."""
    return [{"id": k, "label": v["label"]} for k, v in ACCOUNT_SCOPES.items()]


# ---------------------------------------------------------------------------
# Site-normalisering
# ---------------------------------------------------------------------------
# Kanoniske keys er domæne-agtige (matcher Zuora-output fra eksisterende query).
# Aliaser dækker både Pipedrive-formater ("FinansWatch DK") og Zuora-rateplan
# prefixer ("FinansWatch", "Watch Medier"...).

SITE_ALIASES: dict[str, str] = {
    # Watch DK
    "finanswatch dk":       "finanswatch.dk",
    "finanswatch":          "finanswatch.dk",
    "finans dk":            "finans.dk",
    "finans":               "finans.dk",
    "shippingwatch dk":     "shippingwatch.dk",
    "shippingwatch":        "shippingwatch.dk",
    "advokatwatch dk":      "advokatwatch.dk",
    "advokatwatch":         "advokatwatch.dk",
    "amwatch dk":           "amwatch.com",
    "amwatch":              "amwatch.com",
    "energiwatch dk":       "energiwatch.dk",
    "energiwatch":          "energiwatch.dk",
    "energywatch":          "energiwatch.dk",
    "ejendomswatch dk":     "ejendomswatch.dk",
    "ejendomswatch":        "ejendomswatch.dk",
    "mediawatch dk":        "mediawatch.dk",
    "mediawatch":           "mediawatch.dk",
    "medwatch dk":          "medwatch.dk",
    "medwatch":             "medwatch.dk",
    "itwatch dk":           "itwatch.dk",
    "itwatch":              "itwatch.dk",
    "agriwatch dk":         "agriwatch.dk",
    "agriwatch":            "agriwatch.dk",
    "fødevarewatch dk":     "fodevarewatch.dk",
    "fødevare watch dk":    "fodevarewatch.dk",
    "fodevarewatch":        "fodevarewatch.dk",
    "fødevarewatch":        "fodevarewatch.dk",
    "kapitalwatch dk":      "kapwatch.dk",
    "kapitalwatch":         "kapwatch.dk",
    "kapwatch":             "kapwatch.dk",
    "policywatch dk":       "policywatch.dk",
    "policywatch":          "policywatch.dk",
    "policy dk":            "policywatch.dk",
    "detailwatch dk":       "detailwatch.dk",
    "detailwatch":          "detailwatch.dk",
    "mobilitywatch dk":     "mobilitywatch.dk",
    "mobilitywatch":        "mobilitywatch.dk",
    "techwatch dk":         "techwatch.dk",
    "techwatch":            "techwatch.dk",
    "cleantechwatch dk":    "ctwatch.dk",
    "cleantechwatch":       "ctwatch.dk",
    "ctwatch":              "ctwatch.dk",
    "kforum dk":            "kforum.dk",
    "kforum":               "kforum.dk",
    "watch medier dk":      "watchmedier.dk",
    "watch medier":         "watchmedier.dk",
    "watchmedier dk":       "watchmedier.dk",
    "watchmedier.dk":       "watchmedier.dk",
    "watch media":          "watchmedier.dk",
    # Watch NO
    "finanswatch no":       "finanswatch.no",
    "finanswatch.no":       "finanswatch.no",
    "advokatwatch no":      "advokatwatch.no",
    "advokatwatch.no":      "advokatwatch.no",
    "eiendomswatch no":     "eiendomswatch.no",
    "eiendomswatch.no":     "eiendomswatch.no",
    "energiwatch no":       "energiwatch.no",
    "energiwatch.no":       "energiwatch.no",
    "medwatch no":          "medwatch.no",
    "medwatch.no":          "medwatch.no",
    "handelswatch no":      "handelswatch.no",
    "handelswatch.no":      "handelswatch.no",
    "techwatch no":         "techwatch.no",
    "techwatch.no":         "techwatch.no",
    "matvarewatch no":      "matvarewatch.no",
    "matvarewatch.no":      "matvarewatch.no",
    "watchmedia no":        "watchmedia.no",
    "watchmedia.no":        "watchmedia.no",
    # Watch SE
    "finanswatch se":       "finanswatch.se",
    "finanswatch.se":       "finanswatch.se",
    # Watch DE
    "finanzbusiness de":    "finanzbusiness.de",
    "finanzbusiness":       "finanzbusiness.de",
    # Monitor
    "byrummonitor":         "byrummonitor.dk",
    "idrætsmonitor":        "idraetsmonitor.dk",
    "idraetsmonitor":       "idraetsmonitor.dk",
    "justitsmonitor":       "justitsmonitor.dk",
    "klimamonitor":         "klimamonitor.dk",
    "kulturmonitor":        "kulturmonitor.dk",
    "monitormedier":        "monitormedier.dk",
    "naturmonitor":         "naturmonitor.dk",
    "seniormonitor":        "seniormonitor.dk",
    "skolemonitor":         "skolemonitor.dk",
    "socialmonitor":        "socialmonitor.dk",
    "sundhedsmonitor":      "sundhedsmonitor.dk",
    "turistmonitor":        "turistmonitor.dk",
    "uddannelsesmonitor":   "uddannelsesmonitor.dk",
    "all monitor sites":    "monitormedier.dk",
}


def normalize_site(raw: Optional[str]) -> Optional[str]:
    """Map en site-streng til kanonisk nøgle. Returnerer None hvis tom."""
    if not raw:
        return None
    s = raw.strip().lower()
    if not s:
        return None
    # Direkte match først
    if s in SITE_ALIASES:
        return SITE_ALIASES[s]
    # Drop ekstra whitespace
    s2 = re.sub(r"\s+", " ", s)
    if s2 in SITE_ALIASES:
        return SITE_ALIASES[s2]
    # Fjern alle mellemrum
    s3 = s2.replace(" ", "")
    if s3 in SITE_ALIASES:
        return SITE_ALIASES[s3]
    # Hvis det allerede ligner et domæne, behold som-er
    if "." in s2:
        return s2
    return s2  # ukendt — bevar som ligegyldig key (vises som mismatch)


# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------

def get_pipedrive_conn():
    return pymssql.connect(
        server=os.getenv("DB_SERVER"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME", "INTOMEDIA"),
        tds_version="7.0",
        login_timeout=10,
        timeout=30,
    )


# ---------------------------------------------------------------------------
# Snapshot-fil håndtering
# ---------------------------------------------------------------------------

def _candidate_snapshot_dirs() -> list[Path]:
    """Returnerer alle stier der skal afsøges, i prioriteret rækkefølge.

    ZUORA_SNAPSHOT_DIR i .env må indeholde flere stier separeret med ';' —
    så kan samme .env bruges på lokal maskine og remote, hvor mappen ligger
    forskellige steder. Første sti der eksisterer vinder.
    """
    override = os.getenv("ZUORA_SNAPSHOT_DIR", "").strip()
    if override:
        return [Path(p.strip()) for p in override.split(";") if p.strip()]
    return [DEFAULT_SNAPSHOT_DIR]


def get_snapshot_dir() -> Path:
    """Find første kandidat-mappe der faktisk eksisterer.

    Hvis ingen findes, returnerer vi første kandidat alligevel — så fejlbeskeden
    "ingen fil fundet i <sti>" peger på et meningsfuldt sted.
    """
    candidates = _candidate_snapshot_dirs()
    for c in candidates:
        if c.exists():
            return c
    return candidates[0] if candidates else DEFAULT_SNAPSHOT_DIR


def current_snapshot_date() -> Optional[str]:
    """Returner snapshot-dato (ISO YYYY-MM-DD) fra seneste fils navn.

    Bruges som øvre grænse på service_activation_date for Pipedrive — deals
    aktiveret EFTER snapshot-datoen er ikke nået med i Zuora-eksportet, og det
    ville være ufair at flagge dem som mismatch.
    """
    p = find_latest_snapshot()
    if not p:
        return None
    m = SNAPSHOT_FILENAME_RE.search(p.stem)
    if m:
        dd, mm, yyyy = m.groups()
        try:
            return date(int(yyyy), int(mm), int(dd)).isoformat()
        except ValueError:
            pass
    # Fallback: file mtime
    return datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d")


def find_latest_snapshot(folder: Optional[Path] = None) -> Optional[Path]:
    """Find nyeste ACV_snapshot_DDMMYYYY.{csv,xlsx} i mappen.

    Sorterer efter dato i filnavnet hvis muligt; ellers efter mtime.
    """
    folder = folder or get_snapshot_dir()
    if not folder.exists():
        return None
    candidates: list[tuple[date, float, Path]] = []
    for pattern in ("ACV_snapshot_*.csv", "ACV_snapshot_*.xlsx"):
        for p in folder.glob(pattern):
            m = SNAPSHOT_FILENAME_RE.search(p.stem)
            if m:
                dd, mm, yyyy = m.groups()
                try:
                    d = date(int(yyyy), int(mm), int(dd))
                except ValueError:
                    d = date.min
            else:
                d = date.min
            candidates.append((d, p.stat().st_mtime, p))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][2]


# ---------------------------------------------------------------------------
# Pipedrive: ACV pr. (org_id, site)
# ---------------------------------------------------------------------------

def _scope_ids_for(scope: Optional[str]) -> list[str]:
    """'all' / None → alle scopes; ellers en enkelt."""
    if not scope or scope == "all":
        return list(ACCOUNT_SCOPES.keys())
    if scope not in ACCOUNT_SCOPES:
        raise ValueError(f"Ukendt scope: {scope!r}. Mulige: {list(ACCOUNT_SCOPES)}")
    return [scope]


# Faste valutakurser — SKAL holdes i sync med exchange_rates i Zuora-snapshot-querien.
# Pipedrive's value_dkk er sync-tidspunkt-kurser og bruges IKKE; vi rekonverterer
# value × fixed_rate(currency) i SQL så begge sider matcher kurssætning.
PD_FIXED_RATES_SQL = """
    CASE UPPER(LTRIM(RTRIM(d.currency)))
        WHEN 'DKK' THEN 1.0
        WHEN 'EUR' THEN 7.45
        WHEN 'NOK' THEN 0.7
        WHEN 'SEK' THEN 0.7
        WHEN 'USD' THEN 6.4
        ELSE 1.0
    END
"""

# Banner-pipelines (fx 'BannerAds DE') skal aldrig med i abonnements-alignment,
# selv hvis en deal fejlagtigt har deal_type='Subscription'. Bruges sammen med
# scope's deal_type-filter som dobbelt-værn mod fejl-kategorisering.
PD_NOT_BANNER_SQL = "(d.pipeline_name IS NULL OR LOWER(d.pipeline_name) NOT LIKE '%banner%')"
PD_NOT_JOBMARKED_SQL = "(d.pipeline_name IS NULL OR LOWER(d.pipeline_name) NOT LIKE '%jobmarked%')"


def fetch_pipedrive_acv(
    scope: Optional[str] = None,
    snapshot_date: Optional[str] = None,
) -> list[dict]:
    """Pipedrive ACV pr. (scope, org_id, site).

    scope=None eller 'all' → alle definerede scopes.
    scope='watch_no' osv. → kun det ene.

    ACV beregnes som value × FAST kurs (ikke value_dkk fra DB), så det matcher
    Zuora-snapshot's faste kurssætning. Ellers ville rene kursforskelle blive
    flagget som mismatch.

    snapshot_date afgrænser service_activation_date <= snapshot_date — så vi
    kun tæller deals der var aktive på Zuora-snapshot-tidspunktet.
    """
    scope_ids = _scope_ids_for(scope)
    snap_date = snapshot_date or current_snapshot_date()

    out: list[dict] = []
    try:
        conn = get_pipedrive_conn()
        cur = conn.cursor(as_dict=True)
        for scope_id in scope_ids:
            cfg = ACCOUNT_SCOPES[scope_id]
            deal_types = cfg["pd_deal_types"]
            placeholders = ",".join(["%s"] * len(deal_types))
            params = (cfg["pd_account"], *deal_types, snap_date)
            cur.execute(f"""
                SELECT
                    d.org_id,
                    d.org_name,
                    LTRIM(RTRIM(s.value)) AS site_raw,
                    SUM(d.value * {PD_FIXED_RATES_SQL}) AS total_value_dkk_fixed,
                    SUM(d.value)                        AS total_value_local,
                    COUNT(*)                            AS deal_count
                FROM [INTOMEDIA].[dbo].[PipedriveDeals] d
                CROSS APPLY STRING_SPLIT(d.sites, ',')  AS s
                WHERE d.status = 'won'
                  AND d.value IS NOT NULL
                  AND d.org_id IS NOT NULL
                  AND LTRIM(RTRIM(s.value)) <> ''
                  AND d.account = %s
                  AND d.deal_type IN ({placeholders})
                  AND {PD_NOT_BANNER_SQL}
                  AND {PD_NOT_JOBMARKED_SQL}
                  AND d.service_activation_date IS NOT NULL
                  AND d.service_activation_date <= %s
                GROUP BY d.org_id, d.org_name, LTRIM(RTRIM(s.value))
            """, params)
            for r in cur.fetchall():
                site_norm = normalize_site(r["site_raw"])
                if not site_norm:
                    continue
                out.append({
                    "scope":      scope_id,
                    "org_id":     str(r["org_id"]),
                    "org_name":   r["org_name"] or "—",
                    "site":       site_norm,
                    "site_raw":   r["site_raw"],
                    "pd_acv":     float(r["total_value_dkk_fixed"] or 0),
                    "pd_local":   float(r["total_value_local"] or 0),
                    "deal_count": int(r["deal_count"] or 0),
                })
        conn.close()
    except Exception:
        traceback.print_exc()
    return out


def fetch_pipedrive_org_names(scope: Optional[str] = None) -> dict[str, str]:
    """org_id → org_name for ALLE deals i scope's accounts.

    Permissiv lookup: ingen filtrering på status, deal_type, administrativ eller
    activation_date. Bruges som fallback i compare_portfolios til at vise
    kundens navn selv hvis de kun har admin-deals, lost-deals eller deals i
    fremtiden — så "(kunde ikke i Pipedrive)" forbeholdes kunder der reelt
    ikke findes i PD overhovedet.
    """
    scope_ids = _scope_ids_for(scope)
    out: dict[str, str] = {}
    try:
        conn = get_pipedrive_conn()
        cur = conn.cursor(as_dict=True)
        for scope_id in scope_ids:
            cfg = ACCOUNT_SCOPES[scope_id]
            # Tag seneste org_name pr. org_id (kunder kan have skiftet navn)
            cur.execute("""
                SELECT org_id, org_name
                FROM (
                    SELECT
                        org_id,
                        org_name,
                        ROW_NUMBER() OVER (
                            PARTITION BY org_id
                            ORDER BY COALESCE(service_activation_date, '1900-01-01') DESC, pd_deal_id DESC
                        ) AS rn
                    FROM PipedriveDeals
                    WHERE org_id IS NOT NULL
                      AND account = %s
                ) t
                WHERE rn = 1
            """, (cfg["pd_account"],))
            for r in cur.fetchall():
                out[str(r["org_id"])] = r["org_name"] or "—"
        conn.close()
    except Exception:
        traceback.print_exc()
    return out


PD_WEB_SALE_NAME_LIKE = "Web Sale%"  # matcher "Web Sale" + "Web Sale Euro"


def fetch_pipedrive_web_sales(
    scope: Optional[str] = None,
    snapshot_date: Optional[str] = None,
) -> list[dict]:
    """Pipedrive ACV fra Web Sale-organisationerne pr. (scope, site).

    Modstykke til Zuora's Consumer-uden-pipedrive_id rækker. Hver Pipedrive-account
    har én organisation kaldet 'Web Sale' (watch_medier har også 'Web Sale Euro')
    som samler alle B2C-abonnements-deals.

    snapshot_date afgrænser service_activation_date <= snapshot_date.
    """
    scope_ids = _scope_ids_for(scope)
    snap_date = snapshot_date or current_snapshot_date()
    out: list[dict] = []
    try:
        conn = get_pipedrive_conn()
        cur = conn.cursor(as_dict=True)
        for scope_id in scope_ids:
            cfg = ACCOUNT_SCOPES[scope_id]
            deal_types = cfg["pd_deal_types"]
            placeholders = ",".join(["%s"] * len(deal_types))
            params = (cfg["pd_account"], *deal_types, PD_WEB_SALE_NAME_LIKE, snap_date)
            cur.execute(f"""
                SELECT
                    LTRIM(RTRIM(s.value))               AS site_raw,
                    SUM(d.value * {PD_FIXED_RATES_SQL}) AS total_value_dkk_fixed,
                    COUNT(*)                            AS deal_count
                FROM [INTOMEDIA].[dbo].[PipedriveDeals] d
                CROSS APPLY STRING_SPLIT(d.sites, ',')  AS s
                WHERE d.status = 'won'
                  AND d.value IS NOT NULL
                  AND LTRIM(RTRIM(s.value)) <> ''
                  AND d.account = %s
                  AND d.deal_type IN ({placeholders})
                  AND {PD_NOT_BANNER_SQL}
                  AND {PD_NOT_JOBMARKED_SQL}
                  AND d.org_name LIKE %s
                  AND d.service_activation_date IS NOT NULL
                  AND d.service_activation_date <= %s
                GROUP BY LTRIM(RTRIM(s.value))
            """, params)
            for r in cur.fetchall():
                site_norm = normalize_site(r["site_raw"])
                if not site_norm:
                    continue
                out.append({
                    "scope":      scope_id,
                    "site":       site_norm,
                    "site_raw":   r["site_raw"],
                    "pd_acv":     float(r["total_value_dkk_fixed"] or 0),
                    "deal_count": int(r["deal_count"] or 0),
                })
        conn.close()
    except Exception:
        traceback.print_exc()
    return out


def fetch_web_sale_deals(scope: str, site: str, snapshot_date: Optional[str] = None) -> dict:
    """Drill-down: alle Web Sale-deals for én scope filtreret til ét site.

    Site-parameteren er den normaliserede form (fx 'finanswatch.dk'). Vi henter
    alle Web Sale-deals for scopet og filtrerer i Python på normaliseret site,
    så vi rammer alle stavevarianter (FinansWatch DK, FinansWatch, finanswatch.dk).

    snapshot_date afgrænser service_activation_date <= snapshot_date — så
    drill-down spejler aggregat-tallene.
    """
    if scope not in ACCOUNT_SCOPES:
        raise ValueError(f"Ukendt scope: {scope!r}")
    cfg = ACCOUNT_SCOPES[scope]
    deal_types = cfg["pd_deal_types"]
    placeholders = ",".join(["%s"] * len(deal_types))
    snap_date = snapshot_date or current_snapshot_date()
    out = {"scope": scope, "site": site, "snapshot_date": snap_date, "deals": [], "by_status": {}, "by_pipeline": {}}
    try:
        conn = get_pipedrive_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(f"""
            SELECT
                d.pd_deal_id,
                d.title,
                d.status,
                d.pipeline_name,
                d.deal_type,
                d.administrativ,
                CAST(d.value_dkk AS BIGINT)                         AS value_dkk,
                CAST(d.value AS BIGINT)                             AS value_local,
                d.currency,
                CONVERT(NVARCHAR(10), d.service_activation_date, 23) AS activation_date,
                d.org_id,
                d.org_name,
                d.owner_name,
                LTRIM(RTRIM(s.value))                                AS site_raw
            FROM PipedriveDeals d
            CROSS APPLY STRING_SPLIT(d.sites, ',') AS s
            WHERE d.account = %s
              AND d.org_name LIKE %s
              AND LTRIM(RTRIM(s.value)) <> ''
              AND d.deal_type IN ({placeholders})
              AND {PD_NOT_BANNER_SQL}
              AND {PD_NOT_JOBMARKED_SQL}
              AND d.service_activation_date IS NOT NULL
              AND d.service_activation_date <= %s
            ORDER BY d.service_activation_date DESC, d.pd_deal_id DESC
        """, (cfg["pd_account"], PD_WEB_SALE_NAME_LIKE, *deal_types, snap_date))
        for r in cur.fetchall():
            site_norm = normalize_site(r["site_raw"])
            if site_norm != site:
                continue
            deal = {
                "deal_id":         r["pd_deal_id"],
                "title":           r["title"] or "(Uden titel)",
                "status":          r["status"] or "—",
                "pipeline":        r["pipeline_name"] or "—",
                "deal_type":       r["deal_type"] or "—",
                "administrativ":   r["administrativ"],
                "value":           int(r["value_dkk"] or 0),
                "value_local":     int(r["value_local"] or 0),
                "currency":        r["currency"] or "—",
                "activation_date": r["activation_date"] or "—",
                "org_id":          r["org_id"],
                "org_name":        r["org_name"] or "—",
                "owner":           r["owner_name"] or "—",
                "site_raw":        r["site_raw"] or "",
            }
            out["deals"].append(deal)
            out["by_status"][deal["status"]] = out["by_status"].get(deal["status"], 0) + 1
            out["by_pipeline"][deal["pipeline"]] = out["by_pipeline"].get(deal["pipeline"], 0) + 1
        conn.close()
    except Exception:
        traceback.print_exc()
    return out


def fetch_customer_deals(scope: str, org_id: str, snapshot_date: Optional[str] = None) -> dict:
    """Hent alle deals for én kunde (én scope) — drill-down.

    snapshot_date afgrænser service_activation_date <= snapshot_date.
    """
    if scope not in ACCOUNT_SCOPES:
        raise ValueError(f"Ukendt scope: {scope!r}")
    cfg = ACCOUNT_SCOPES[scope]
    snap_date = snapshot_date or current_snapshot_date()
    out = {"org_name": None, "snapshot_date": snap_date, "deals": [], "by_status": {}, "by_pipeline": {}}
    try:
        conn = get_pipedrive_conn()
        cur = conn.cursor(as_dict=True)
        # Find org_name (seneste deal)
        cur.execute("""
            SELECT TOP 1 org_name
            FROM PipedriveDeals
            WHERE org_id = %s AND account = %s
            ORDER BY service_activation_date DESC
        """, (org_id, cfg["pd_account"]))
        n = cur.fetchone()
        out["org_name"] = (n or {}).get("org_name") or org_id

        # Kun rene abonnements-deals (deal_type i scope's pd_deal_types) og ikke
        # banner-pipelines — undgå at fejl-kategoriserede deals dukker op i drill-down
        deal_types = cfg["pd_deal_types"]
        placeholders = ",".join(["%s"] * len(deal_types))
        cur.execute(f"""
            SELECT
                d.pd_deal_id,
                d.title,
                d.status,
                d.pipeline_name,
                d.deal_type,
                d.administrativ,
                CAST(d.value_dkk AS BIGINT) AS value_dkk,
                CONVERT(NVARCHAR(10), d.service_activation_date, 23) AS service_activation_date,
                CONVERT(NVARCHAR(10), d.expected_close_date, 23)     AS expected_close_date,
                COALESCE(d.sites, '') AS sites,
                d.owner_name
            FROM PipedriveDeals d
            WHERE d.org_id = %s
              AND d.account = %s
              AND d.deal_type IN ({placeholders})
              AND {PD_NOT_BANNER_SQL}
              AND {PD_NOT_JOBMARKED_SQL}
              AND d.service_activation_date IS NOT NULL
              AND d.service_activation_date <= %s
            ORDER BY d.service_activation_date DESC, d.pd_deal_id DESC
        """, (org_id, cfg["pd_account"], *deal_types, snap_date))
        for r in cur.fetchall():
            deal = {
                "deal_id":          r["pd_deal_id"],
                "title":            r["title"] or "(Uden titel)",
                "status":           r["status"] or "—",
                "pipeline":         r["pipeline_name"] or "—",
                "deal_type":        r["deal_type"] or "—",
                "administrativ":    r["administrativ"],
                "value":            int(r["value_dkk"] or 0),
                "activation_date":  r["service_activation_date"] or "—",
                "expected_close":   r["expected_close_date"] or "—",
                "sites":            r["sites"] or "—",
                "owner":            r["owner_name"] or "—",
            }
            out["deals"].append(deal)
            out["by_status"][deal["status"]]  = out["by_status"].get(deal["status"], 0) + 1
            out["by_pipeline"][deal["pipeline"]] = out["by_pipeline"].get(deal["pipeline"], 0) + 1
        conn.close()
    except Exception:
        traceback.print_exc()
    return out


# ---------------------------------------------------------------------------
# Zuora snapshot: indlæsning fra CSV/XLSX-eksport
# ---------------------------------------------------------------------------

def _coerce_pipedrive_id(v) -> Optional[str]:
    """pipedrive_id kommer fra CSV som tom/float/int/str — normalisér til str eller None."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    if isinstance(v, float):
        if v != v:  # NaN
            return None
        return str(int(v))
    s = str(v).strip()
    if not s or s.lower() in ("nan", "none", "null"):
        return None
    if s.endswith(".0"):
        s = s[:-2]
    return s


def load_zuora_snapshot(path: Optional[Path] = None) -> dict:
    """Læs Zuora-snapshot fra CSV eller XLSX.

    Returnerer en dict:
        enterprise_rows  — rækker der skal alignment-matches (en pr. account×site)
        web_sales_by_site — Consumer-rækker uden pipedrive_id, aggregeret pr. site
        meta             — path, filnavn, snapshot_date, antal rækker

    Web sales: account_type == 'Consumer' OG pipedrive_id mangler. De repræsenterer
    B2C-abonnementer (selv-betjening på sitet) og findes ikke i Pipedrive — derfor
    skal de ikke generere "Kun i Zuora"-mismatches, men opgøres separat pr. site.
    """
    target = path or find_latest_snapshot()
    if not target:
        folder = get_snapshot_dir()
        raise FileNotFoundError(
            f"Ingen ACV_snapshot_*.csv eller .xlsx fundet i {folder}"
        )

    suffix = target.suffix.lower()
    if suffix == ".xlsx":
        df_raw = pd.read_excel(target, header=None)
    else:
        df_raw = pd.read_csv(target, header=None, sep=",", encoding="utf-8")

    # Hvis første række ligner en header (indeholder ord-strenge i talkolonner),
    # så drop den. Detektér på første kolonne: enten 'snapshot_date' eller en dato-streng.
    first = df_raw.iloc[0]
    if str(first.iloc[0]).strip().lower() == "snapshot_date":
        df = df_raw.iloc[1:].reset_index(drop=True)
    else:
        df = df_raw

    if df.shape[1] < len(SNAPSHOT_COLUMNS):
        raise ValueError(
            f"Snapshot-fil {target.name} har {df.shape[1]} kolonner, "
            f"forventer mindst {len(SNAPSHOT_COLUMNS)}: {SNAPSHOT_COLUMNS}"
        )
    df = df.iloc[:, :len(SNAPSHOT_COLUMNS)]
    df.columns = SNAPSHOT_COLUMNS

    enterprise_raw: list[dict] = []
    web_sales_raw: list[dict] = []
    for _, r in df.iterrows():
        site = str(r["site"]).strip() if pd.notna(r["site"]) else ""
        if not site or site.lower() == "ukendt":
            continue
        site_norm = normalize_site(site) or site.lower()
        try:
            arr = float(r["arr_dkk"]) if pd.notna(r["arr_dkk"]) else 0.0
        except (TypeError, ValueError):
            arr = 0.0
        account_type = str(r["account_type"]).strip() if pd.notna(r["account_type"]) else ""
        pipedrive_id = _coerce_pipedrive_id(r["pipedrive_id"])
        account_number = str(r["account_number"]).strip() if pd.notna(r["account_number"]) else None
        brand = str(r["brand"]).strip() if pd.notna(r["brand"]) else None
        scope_id = SCOPE_BY_ZUORA_BRAND.get(brand) if brand else None

        # Web sale = Consumer-account uden pipedrive_id. Findes ikke i Pipedrive
        # under almindelige organisationer, men på en særlig web-sale-org (se
        # fetch_pipedrive_web_sales). Business uden pipedrive_id forbliver i
        # primary-tabellen som "Kun i Zuora" — det er en datakvalitetsfejl.
        is_web_sale = account_type.lower() == "consumer" and not pipedrive_id

        row = {
            "scope":          scope_id,
            "account_number": account_number,
            "pipedrive_id":   pipedrive_id,
            "account_type":   account_type,
            "brand":          brand,
            "site":           site_norm,
            "arr_dkk":        arr,
        }
        if is_web_sale:
            web_sales_raw.append(row)
        else:
            enterprise_raw.append(row)

    # Enterprise: aggregér til (scope, pipedrive_id, account_number, site) for alignment
    # Scope er en del af nøglen, så samme org_id under to forskellige Zuora-brands
    # ikke smelter sammen.
    ent_agg: dict[tuple, dict] = {}
    for r in enterprise_raw:
        key = (r["scope"], r["pipedrive_id"], r["account_number"], r["site"])
        bucket = ent_agg.setdefault(key, {
            "scope":           r["scope"],
            "pipedrive_id":    r["pipedrive_id"],
            "account_number":  r["account_number"],
            "site":            r["site"],
            "brand":           r["brand"],
            "zuora_arr":       0.0,
        })
        bucket["zuora_arr"] += r["arr_dkk"]
    enterprise_rows = list(ent_agg.values())

    # Web sales: aggregér til (scope, site) — antal accounts + total ARR
    ws_agg: dict[tuple, dict] = {}
    for r in web_sales_raw:
        key = (r["scope"], r["site"])
        bucket = ws_agg.setdefault(key, {
            "scope":         r["scope"],
            "site":          r["site"],
            "zuora_arr":     0.0,
            "account_count": 0,
        })
        bucket["zuora_arr"]    += r["arr_dkk"]
        bucket["account_count"] += 1
    web_sales_by_site = sorted(
        [
            {**v, "zuora_arr": round(v["zuora_arr"], 2)}
            for v in ws_agg.values()
        ],
        key=lambda x: -x["zuora_arr"],
    )

    # Snapshot-dato: prøv første kolonne i filen, ellers fra filnavnet, ellers mtime
    snapshot_date: Optional[str] = None
    try:
        v0 = df.iloc[0]["snapshot_date"]
        if pd.notna(v0):
            snapshot_date = str(v0)[:10]
    except Exception:
        pass
    if not snapshot_date:
        m = SNAPSHOT_FILENAME_RE.search(target.stem)
        if m:
            dd, mm, yyyy = m.groups()
            snapshot_date = f"{yyyy}-{mm}-{dd}"
    if not snapshot_date:
        snapshot_date = datetime.fromtimestamp(target.stat().st_mtime).strftime("%Y-%m-%d")

    meta = {
        "path":              str(target),
        "filename":          target.name,
        "snapshot_date":     snapshot_date,
        "row_count":         len(enterprise_raw) + len(web_sales_raw),
        "enterprise_rows":   len(enterprise_rows),
        "web_sales_rows":    len(web_sales_raw),
        "web_sales_total":   round(sum(w["zuora_arr"] for w in web_sales_by_site), 2),
    }
    return {
        "enterprise_rows":   enterprise_rows,
        "web_sales_by_site": web_sales_by_site,
        "meta":              meta,
    }


# ---------------------------------------------------------------------------
# Sammenligning
# ---------------------------------------------------------------------------

def compare_portfolios(scope: Optional[str] = None) -> dict:
    """Full outer join af Pipedrive-ACV mod Zuora-ARR pr. (scope, org_id, site).

    scope='all' (eller None) → sammenligner alle definerede accounts.
    scope='watch_no'/'watch_de' osv. → kun det ene.

    Match-nøgle inkluderer scope, så samme org_id under to forskellige
    forretningsenheder ikke smelter sammen til én række.

    Pipedrive læses live fra INTOMEDIA. Zuora læses fra seneste snapshot-fil.
    """
    scope_ids = _scope_ids_for(scope)
    snap = load_zuora_snapshot()
    snapshot_meta     = snap["meta"]
    snap_date         = snapshot_meta.get("snapshot_date")
    pd_rows = fetch_pipedrive_acv(scope, snapshot_date=snap_date)
    pd_web  = fetch_pipedrive_web_sales(scope, snapshot_date=snap_date)
    pd_all_org_names  = fetch_pipedrive_org_names(scope)  # permissiv lookup
    all_zu_rows       = snap["enterprise_rows"]
    all_web_sales     = snap["web_sales_by_site"]

    # Filtrér Zuora til de scopes der er valgt
    zu_rows           = [r for r in all_zu_rows if r["scope"] in scope_ids]
    zu_web_sales      = [w for w in all_web_sales if w["scope"] in scope_ids]

    # Index Pipedrive pr. (scope, org_id, site)
    pd_by_key: dict[tuple, dict] = {}
    pd_name_by_org: dict[str, str] = {}
    for r in pd_rows:
        key = (r["scope"], r["org_id"], r["site"])
        pd_by_key[key] = r
        pd_name_by_org.setdefault(r["org_id"], r["org_name"])

    # Index Zuora pr. (scope, pipedrive_id, site) — sum hen over account_numbers
    zu_by_key: dict[tuple, dict] = {}
    for r in zu_rows:
        if not r["pipedrive_id"]:
            # Manglende pipedrive_id på Business-account → vises som "Kun i Zuora"
            # men keyet på account_number så hver problematisk account får sin egen række
            org_key = f"acc:{r['account_number']}"
        else:
            org_key = r["pipedrive_id"]
        key = (r["scope"], org_key, r["site"])
        bucket = zu_by_key.setdefault(key, {
            "scope":          r["scope"],
            "pipedrive_id":   r["pipedrive_id"],
            "site":           r["site"],
            "brand":          r["brand"],
            "zuora_arr":      0.0,
            "account_numbers": [],
        })
        bucket["zuora_arr"] += r["zuora_arr"]
        if r["account_number"] and r["account_number"] not in bucket["account_numbers"]:
            bucket["account_numbers"].append(r["account_number"])

    # Merge: union af alle keys
    all_keys = set(pd_by_key.keys()) | set(zu_by_key.keys())
    rows: list[dict] = []
    for key in all_keys:
        pd_r = pd_by_key.get(key)
        zu_r = zu_by_key.get(key)
        scope_id, org_id, site = key

        pd_acv    = pd_r["pd_acv"] if pd_r else 0.0
        zuora_arr = zu_r["zuora_arr"] if zu_r else 0.0
        diff      = pd_acv - zuora_arr

        if pd_r and zu_r:
            status = "match" if abs(diff) < 1.0 else "mismatch"
            org_name = pd_r["org_name"]
        elif pd_r and not zu_r:
            status = "pd_only"
            org_name = pd_r["org_name"]
        else:
            status = "zuora_only"
            # Først permissiv lookup (alle deals), så aggregat-lookup, så fallback.
            # Permissiv vinder fordi den finder kunder der KUN har admin/lost/open
            # deals — som ikke er med i ACV men eksisterer i PD og er klikbare.
            org_name = (
                pd_all_org_names.get(str(org_id))
                or pd_name_by_org.get(org_id)
                or "(kunde ikke i Pipedrive)"
            )

        scope_label = ACCOUNT_SCOPES.get(scope_id, {}).get("label", scope_id) if scope_id else "(ukendt)"

        rows.append({
            "scope":           scope_id,
            "scope_label":     scope_label,
            "org_id":          None if isinstance(org_id, str) and org_id.startswith("acc:") else org_id,
            "org_name":        org_name,
            "site":            site,
            "pd_acv":          round(pd_acv, 2),
            "zuora_arr":       round(zuora_arr, 2),
            "diff":            round(diff, 2),
            "diff_pct":        round((diff / pd_acv * 100), 1) if pd_acv else None,
            "status":          status,
            "account_numbers": zu_r["account_numbers"] if zu_r else [],
            "deal_count":      pd_r["deal_count"] if pd_r else 0,
            "site_raw":        pd_r["site_raw"] if pd_r else None,
        })

    rows.sort(key=lambda r: (-abs(r["diff"]), r["org_name"] or ""))

    # KPI-summering
    n_match     = sum(1 for r in rows if r["status"] == "match")
    n_mismatch  = sum(1 for r in rows if r["status"] == "mismatch")
    n_pd_only   = sum(1 for r in rows if r["status"] == "pd_only")
    n_zu_only   = sum(1 for r in rows if r["status"] == "zuora_only")
    total_pd    = sum(r["pd_acv"]    for r in rows)
    total_zuora = sum(r["zuora_arr"] for r in rows)

    # ------------------------------------------------------------------
    # Web sales — outer-join PD's "Web Sale"-org mod Zuora's Consumer-no-PD-id
    # på (scope, normaliseret site). Spejler logikken i primary-tabellen.
    # ------------------------------------------------------------------
    pd_web_by_key = {(r["scope"], r["site"]): r for r in pd_web}
    zu_web_by_key = {(w["scope"], w["site"]): w for w in zu_web_sales}
    ws_keys = set(pd_web_by_key) | set(zu_web_by_key)

    web_sales_rows: list[dict] = []
    for k in ws_keys:
        pd_w = pd_web_by_key.get(k)
        zu_w = zu_web_by_key.get(k)
        scope_id, site = k
        pd_acv    = pd_w["pd_acv"]    if pd_w else 0.0
        zuora_arr = zu_w["zuora_arr"] if zu_w else 0.0
        diff      = pd_acv - zuora_arr

        if pd_w and zu_w:
            ws_status = "match" if abs(diff) < 1.0 else "mismatch"
        elif pd_w:
            ws_status = "pd_only"
        else:
            ws_status = "zuora_only"

        web_sales_rows.append({
            "scope":         scope_id,
            "scope_label":   ACCOUNT_SCOPES.get(scope_id, {}).get("label", scope_id) if scope_id else "(ukendt)",
            "site":          site,
            "pd_acv":        round(pd_acv, 2),
            "zuora_arr":     round(zuora_arr, 2),
            "diff":          round(diff, 2),
            "diff_pct":      round((diff / pd_acv * 100), 1) if pd_acv else None,
            "status":        ws_status,
            "deal_count":    pd_w["deal_count"] if pd_w else 0,
            "account_count": zu_w["account_count"] if zu_w else 0,
        })

    web_sales_rows.sort(key=lambda r: (-abs(r["diff"]), r["site"]))

    ws_total_pd     = sum(w["pd_acv"]    for w in web_sales_rows)
    ws_total_zuora  = sum(w["zuora_arr"] for w in web_sales_rows)
    ws_match        = sum(1 for w in web_sales_rows if w["status"] == "match")
    ws_mismatch     = sum(1 for w in web_sales_rows if w["status"] == "mismatch")
    ws_pd_only      = sum(1 for w in web_sales_rows if w["status"] == "pd_only")
    ws_zuora_only   = sum(1 for w in web_sales_rows if w["status"] == "zuora_only")
    ws_accounts_total = sum(w["account_count"] for w in web_sales_rows)

    return {
        "rows":         rows,
        "web_sales":    web_sales_rows,
        "summary": {
            "total_rows":          len(rows),
            "match":               n_match,
            "mismatch":            n_mismatch,
            "pd_only":             n_pd_only,
            "zuora_only":          n_zu_only,
            "total_pd_acv":        round(total_pd, 2),
            "total_zuora":         round(total_zuora, 2),
            "total_diff":          round(total_pd - total_zuora, 2),

            "web_sales_total_pd":    round(ws_total_pd, 2),
            "web_sales_total_zuora": round(ws_total_zuora, 2),
            "web_sales_total_diff":  round(ws_total_pd - ws_total_zuora, 2),
            "web_sales_match":       ws_match,
            "web_sales_mismatch":    ws_mismatch,
            "web_sales_pd_only":     ws_pd_only,
            "web_sales_zuora_only":  ws_zuora_only,
            "web_sales_accounts":    ws_accounts_total,
            "web_sales_sites":       len(web_sales_rows),
        },
        "scope":    scope or "all",
        "scopes":   list_account_scopes(),
        "snapshot": snapshot_meta,
    }

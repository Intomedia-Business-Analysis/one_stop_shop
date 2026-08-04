"""Risikolisten: aktive kunder + usage-signal + zoner, opgjort i ARR.

Her mødes de to halvdele. `queries.db_customers_at_risk_base()` leverer hvem der
er aktiv lige nu, hvem der ejer dem, og hvor mange kroner de står for.
`usage.recency_by_customer()` leverer hvor længe siden de sidst var aktive.
Nøglen er `(account, org_id)` i begge ender — se usage.customer_key for hvorfor
org_id alene ikke er nok.

Oplægget (Churn-risiko dashboard.pptx) vægter zonerne på ANTAL kunder. Her
vægtes de på ARR, fordi en leder handler på kroner: 3 kunder à 500.000 kr. er
ikke det samme som 30 à 5.000. Antallet vises ved siden af, ikke i stedet for.

Tre ting der bevidst IKKE vises som risiko:

1. Kunder uden usage-signal får zonen "intet_signal", ikke "kritisk". Et datahul
   er ikke en tavs kunde.
2. FINANS DK-kunder markeres separat. Sitet sætter aldrig
   access_account_number i Snowplow (verificeret over 3 måneder), så FINANS DK
   bidrager ikke med noget signal — uden markeringen ligner de kunder der er
   holdt op med at læse. Bemærk at et par af dem alligevel HAR et signal (12 af
   456 målt 2026-08-04), fordi deres Zuora-konto også dækker en anden site;
   markeringen siger altså "FINANS DK tæller ikke med", ikke "kan ikke trackes".
3. Kunder uden ACV-række har `arr_dkk = None` og indgår ikke i kronesummen.
   De tælles i antal, så de ikke forsvinder ud af overblikket.
"""

import logging

from .queries import db_customers_at_risk_base
from .usage import (
    ZONE_ATTENTION_DAYS,
    ZONE_CRITICAL_DAYS,
    _account_to_customer_map,
    customer_key,
    load_usage_recency,
    recency_by_customer,
)

logger = logging.getLogger(__name__)

# Rækkefølgen er visningsrækkefølgen: værst først, datahuller sidst.
ZONE_ORDER = ["kritisk", "opmærksomhed", "sund", "intet_signal"]

ZONE_LABELS = {
    "kritisk":      f"Kritisk ({ZONE_CRITICAL_DAYS}+ dage)",
    "opmærksomhed": f"Opmærksomhed ({ZONE_ATTENTION_DAYS}-{ZONE_CRITICAL_DAYS - 1} dage)",
    "sund":         f"Sund (under {ZONE_ATTENTION_DAYS} dage)",
    "intet_signal": "Intet signal",
}

# Site der aldrig sætter access_account_number i Snowplow. Værdien er
# retention-viewets stavemåde (dbo.retention.sites), ikke ACV's.
UNTRACKED_SITES = {"FINANS DK"}


def _finans_dk_status(sites_list: str | None) -> str:
    """'alle', 'nogle' eller 'nej' — hvor meget af kunden der er utrackbart.

    'alle' betyder at kunden umuligt kan have et usage-signal, og at fravær af
    signal derfor intet siger om risiko. 'nogle' betyder at signalet kun dækker
    en del af kundens abonnementer, så det kan undervurdere aktiviteten.
    """
    if not sites_list:
        return "nej"
    sites = {s.strip() for s in sites_list.split(",") if s.strip()}
    if not sites:
        return "nej"
    if sites <= UNTRACKED_SITES:
        return "alle"
    if sites & UNTRACKED_SITES:
        return "nogle"
    return "nej"


def customers_at_risk(owner_name: str | None = None,
                      teams: list | None = None) -> dict:
    """Risikolisten med zone-opgørelse.

    Returnerer:
        rows   — én pr. kunde, sorteret kritisk først og derefter ARR faldende
        zones  — pr. zone: antal kunder, ARR i risiko, antal uden ARR
        meta   — datafriskhed, tærskler, og hvad der ikke kunne kobles

    Mangler recency-filen, returneres alle kunder med zonen "intet_signal" og en
    `meta["usage_error"]` — IKKE en tom liste. En tom liste ville se ud som
    "ingen kunder i risiko", hvilket er den farligste mulige fejlvisning.
    """
    base = db_customers_at_risk_base(owner_name=owner_name, teams=teams)

    recency: dict = {}
    usage_meta: dict = {}
    usage_error: str | None = None
    try:
        recency = recency_by_customer()
        usage_meta = load_usage_recency()["meta"]
    except FileNotFoundError as e:
        usage_error = str(e)
        logger.warning("Recency-data mangler — alle kunder vises uden signal: %s", e)
    except Exception as e:
        usage_error = f"{type(e).__name__}: {e}"
        logger.exception("Recency-data kunne ikke læses")

    # Hvilke kunder KAN vi overhovedet oversætte fra Snowplow til? Bruges til at
    # forklare hvorfor en kunde mangler signal — og den forklaring er forskellen
    # på "vi ved det ikke" og "kunden er helt tavs", som er det stærkeste
    # churn-signal vi har. Uden Zuora-snapshottet springes opdelingen over
    # frem for at lade hele siden fejle.
    kan_oversaettes: set = set()
    try:
        kan_oversaettes = set(_account_to_customer_map().values())
    except Exception:
        logger.warning("Zuora-snapshot utilgængeligt — kan ikke forklare manglende signaler")

    rows = []
    for r in base:
        key = customer_key(r["account"], r["org_id"])
        sig = recency.get(key)
        arr = float(r["arr_dkk"]) if r["arr_dkk"] is not None else None
        rows.append({
            "account":     r["account"],
            "org_id":      r["org_id"],
            "org_name":    r["org_name"],
            "sites":       r["sites"],
            "sites_list":  r["sites_list"],
            "owner_name":  r["owner_name"],
            "teams":       r["teams"],
            "arr_dkk":     arr,
            "zone":        sig["zone"] if sig else "intet_signal",
            "days_since_last_activity": sig["days_since_last_activity"] if sig else None,
            "last_activity_date":       sig["last_activity_date"] if sig else None,
            "active_days":              sig["active_days"] if sig else None,
            "page_views":               sig["page_views"] if sig else None,
            "finans_dk":   _finans_dk_status(r["sites_list"]),
        })

    # Værst først; inden for samme zone de største kroner først. Kunder uden ARR
    # sorteres nederst i deres zone (-1), så de ikke skubber rigtige tal ned.
    rows.sort(key=lambda x: (ZONE_ORDER.index(x["zone"]) if x["zone"] in ZONE_ORDER else 99,
                             -(x["arr_dkk"] if x["arr_dkk"] is not None else -1)))

    zones = {z: {"label": ZONE_LABELS[z], "customers": 0, "arr_dkk": 0.0,
                 "uden_arr": 0, "utrackbare": 0}
             for z in ZONE_ORDER}
    for r in rows:
        b = zones.setdefault(r["zone"], {"label": r["zone"], "customers": 0,
                                         "arr_dkk": 0.0, "uden_arr": 0,
                                         "utrackbare": 0})
        b["customers"] += 1
        if r["arr_dkk"] is None:
            b["uden_arr"] += 1
        else:
            b["arr_dkk"] += r["arr_dkk"]
        if r["finans_dk"] == "alle":
            b["utrackbare"] += 1

    # Hvorfor mangler et signal? Tre helt forskellige tilstande, som ikke må
    # blandes sammen i UI'et. Målt 2026-08-04: 444 utrackbare (2,7 mio.), 2.422
    # uden Zuora-konto (1,8 mio., altså mikrokunder à ~740 kr.) og 620 helt tavse
    # (6,9 mio.). Kun de to første er reel blindhed — de sidste er det stærkeste
    # churn-signal i datasættet og bør flyttes op i zonerne (se planen).
    mangler = {"utrackbar": [0, 0.0], "ingen_zuora": [0, 0.0], "helt_tavs": [0, 0.0]}
    for r in rows:
        if r["zone"] != "intet_signal":
            continue
        arr = r["arr_dkk"] or 0
        if r["finans_dk"] == "alle":
            n = "utrackbar"
        elif kan_oversaettes and customer_key(r["account"], r["org_id"]) not in kan_oversaettes:
            n = "ingen_zuora"
        else:
            n = "helt_tavs"
        mangler[n][0] += 1
        mangler[n][1] += arr

    blind_arr = mangler["utrackbar"][1] + mangler["ingen_zuora"][1]
    arr_total = sum(r["arr_dkk"] for r in rows if r["arr_dkk"] is not None)

    meta = {
        "customers":     len(rows),
        "arr_total":     arr_total,
        "med_signal":    sum(1 for r in rows if r["zone"] != "intet_signal"),
        # Andelen af KRONER vi kan sige noget om. Det er det ærlige svar på "er
        # datagrundlaget godt nok": 30% af kunderne mangler signal, men de bærer
        # kun ~2% af omsætningen, fordi hullerne sidder hos de mindste kunder.
        "arr_daekning":  (arr_total - blind_arr) / arr_total if arr_total else 0,
        "mangler_signal": {k: {"customers": v[0], "arr_dkk": v[1]}
                           for k, v in mangler.items()},
        "uden_arr":      sum(1 for r in rows if r["arr_dkk"] is None),
        "uden_ejer":     sum(1 for r in rows if not r["owner_name"]),
        "uden_team":     sum(1 for r in rows if r["owner_name"] and not r["teams"]),
        "utrackbare":    sum(1 for r in rows if r["finans_dk"] == "alle"),
        "delvist_trackbare": sum(1 for r in rows if r["finans_dk"] == "nogle"),
        "thresholds":    {"attention_days": ZONE_ATTENTION_DAYS,
                          "critical_days":  ZONE_CRITICAL_DAYS},
        # Tærsklerne er oplæggets gæt, ikke et måleresultat. Så længe det er
        # sandt, skal det stå på siden — ellers læses zonerne som fakta.
        "thresholds_validated": False,
        "usage_export_date": usage_meta.get("export_date"),
        "usage_file_age_days": usage_meta.get("file_age_days"),
        "usage_filename":  usage_meta.get("filename"),
        "usage_error":     usage_error,
    }
    return {"rows": rows, "zones": zones, "zone_order": ZONE_ORDER, "meta": meta}

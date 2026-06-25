"""Brand-gruppering for administrative nysalg.

Performance samles pr. brand-gruppe (ikke pr. deal). Grupperne følger hubbens
eksisterende BRAND_GROUPS (constants.py) — site-medlemskab er den autoritative
kilde, med en nøgleord-fallback for Zuora-varianter som 'finans.dk'/'finans.no'.

Grupper (med direktørens ønskede navne):
  Watch          – danske Watch-sites
  Finans         – FINANS DK (finans.dk)
  Monitor        – monitor-sites
  Norge          – Watch NO
  FinansWatch SE – Watch SE (for sig selv)
  FinanzBusiness – Watch DE (for sig selv)
  Marketwire     – vises ALTID, også selvom det ikke er i Zuora-udtrækket
"""
from __future__ import annotations

import re

from constants import BRAND_GROUPS

# Gruppenøgle (BRAND_GROUPS) → visningsnavn
GROUP_LABELS = {
    "watch_dk":   "Watch DK",
    "finans":     "Finans",
    "monitor":    "Monitor",
    "watch_no":   "Watch NO",
    "watch_se":   "FinansWatch SE",
    "watch_de":   "FinanzBusiness",
    "marketwire": "Marketwire",
}

# Fast visningsrækkefølge. Marketwire vises altid (selv uden bevægelser).
# PipeDrive-rækkerne ligger efter abonnements-brandene.
DISPLAY_ORDER = ["Watch DK", "Finans", "Monitor", "Watch NO", "FinansWatch SE",
                 "FinanzBusiness", "Marketwire", "Job", "Banner",
                 "Norge advertising", "Øvrige"]
ALWAYS_SHOWN = ["Marketwire"]

# PipeDrive-hentede brand-rækker (findes ikke i Zuora-udtrækket). Hver række
# vælges via en [account]- eller [team]-scope i PipedriveDeals, filtreret på
# service_activation_date i perioden og [status]='won'. 'pipelines' begrænser
# yderligere (None = alle pipelines for scopen). Bruttonysalg = won-deals uden
# for cancellation-pipelines; opsigelser = cancellation-pipelines (Σ ABS), så
# netto = brutto − opsigelser. Job/Banner/Norge advertising har ingen
# cancellation-pipelines → opsigelser=0. MarketWire har rigtige opsigelser.
PIPEDRIVE_ROWS = [
    {"label": "Job",               "scope_col": "account", "scope_val": "jppol_advertising",    "pipelines": ["job"]},
    {"label": "Banner",            "scope_col": "account", "scope_val": "jppol_advertising",    "pipelines": ["banner"]},
    {"label": "Norge advertising", "scope_col": "account", "scope_val": "watch_no_advertising", "pipelines": ["job", "banner"],
     # Drill-down (kun review-siden): underrækker pr. site, delmængde af totalen.
     "subrows": [
         {"label": "M24",   "site": "Medier24 NO"},
         {"label": "KOM24", "site": "Kom24 NO"},
     ]},
    {"label": "Marketwire",        "scope_col": "team",    "scope_val": "Team Marketwire",      "pipelines": None},
]

# Budget for reklame-rækkerne (BudgetsIntoMedia). WHERE-fragment uden periode —
# periodeafgrænsning på [BudgetDate] lægges på i repo. Job/Banner (jppol_advertising
# = DK) tager DK-budgettet, mens Norge advertising (watch_no_advertising) tager
# Watch NO-budgettet — Watch NO ekskluderes fra Job/Banner, så det ikke tælles to
# gange. DealType-værdierne ('Job'/'Banner'/Brand='Watch NO') er bekræftet i data.
AD_BUDGET_WHERE = {
    "Banner":            "[DealType] = 'Banner' AND [Brand] <> 'Watch NO'",
    "Job":               "[DealType] = 'Job' AND [Brand] <> 'Watch NO'",
    "Norge advertising": "[Brand] = 'Watch NO' AND [DealType] IN ('Banner', 'Job')",
}

# BudgetsIntoMedia.[Brand]-værdier pr. gruppe (matches case-insensitivt).
# SE/DE følger BRAND_GROUP_LABELS i perf-modulet ("Watch SE"/"Watch DE").
BUDGET_BRANDS = {
    "Watch DK":       ["Watch DK", "Watch Int"],
    "Finans":         ["FINANS DK"],
    "Monitor":        ["Monitor"],
    "Watch NO":       ["Watch NO"],
    "FinansWatch SE": ["Watch SE", "FinansWatch SE"],
    "FinanzBusiness": ["Watch DE", "FinanzBusiness"],
    "Marketwire":     ["MarketWire", "marketwire"],
}

# Brands der vises i lokal valuta (resten i DKK). Norge → NOK, Sverige → SEK.
# Salgstallene (brutto/opsigelser/netto) vises i denne valuta; budgettet kommer
# fra BudgetsIntoMedia og vises altid i DKK.
BRAND_CURRENCY = {
    "Watch NO":          "NOK",
    "FinansWatch SE":    "SEK",
    "Norge advertising": "NOK",
}


def brand_currency(label: str) -> str:
    """Salgsvalutaen et brand vises i ('DKK' som standard)."""
    return BRAND_CURRENCY.get(label, "DKK")


# PipeDrive-kontoen hvor et brands organisationer (org-id) findes. Org-id er IKKE
# unikke på tværs af konti (samme id = forskellig virksomhed i hver konto), så
# navneopslag SKAL scopes til den rigtige konto. Watch DK og Finans deler
# watch_medier-kontoen; Monitor og landene har egne konti.
BRAND_ACCOUNT = {
    "Watch DK":       "watch_medier",
    "Finans":         "watch_medier",
    "Monitor":        "monitor",
    "Watch NO":       "watch_no",
    "FinansWatch SE": "watch_se",
    "FinanzBusiness": "watch_de",
}


def brand_account(label: str) -> str | None:
    """PipeDrive-kontoen et brands org-id'er skal slås op i (None hvis ukendt)."""
    return BRAND_ACCOUNT.get(label)


# Eksakt site → gruppelabel (fra constants.BRAND_GROUPS), lowercased opslag.
_SITE_TO_LABEL = {}
for _key, _sites in BRAND_GROUPS.items():
    _lbl = GROUP_LABELS.get(_key, _key)
    for _s in _sites:
        _SITE_TO_LABEL[_s.strip().lower()] = _lbl


def _has_token(s: str, token: str) -> bool:
    """True hvis token optræder som selvstændigt ord eller domæne-suffiks."""
    return re.search(rf"(^|[^a-z]){token}([^a-z]|$)", s) is not None


def classify(site: str) -> str:
    """Map et site-navn til dets brand-gruppelabel.

    Eksakt medlemskab i BRAND_GROUPS vinder; ellers nøgleord-fallback med
    præcedens (de "for sig selv"-grupper før de brede Watch/Finans/Monitor).
    """
    s = (site or "").strip()
    if not s:
        return "Øvrige"
    sl = s.lower()

    hit = _SITE_TO_LABEL.get(sl)
    if hit:
        return hit

    if "marketwire" in sl:
        return "Marketwire"
    if "finanzbusiness" in sl or "finanz.business" in sl or _has_token(sl, "de"):
        return "FinanzBusiness"
    if _has_token(sl, "se"):
        return "FinansWatch SE"
    if _has_token(sl, "no") or "norge" in sl:
        return "Watch NO"
    if "monitor" in sl:
        return "Monitor"
    # FINANS DK (finans.dk) — "finans" men IKKE et FinansWatch-site.
    if "finans" in sl and "watch" not in sl:
        return "Finans"
    if "watch" in sl:
        return "Watch DK"
    return "Øvrige"

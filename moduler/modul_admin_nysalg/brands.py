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
DISPLAY_ORDER = ["Watch DK", "Finans", "Watch NO", "FinansWatch SE",
                 "FinanzBusiness", "Marketwire", "Job", "Banner",
                 "Norge Job", "Norge Banner", "Øvrige"]
ALWAYS_SHOWN = ["Marketwire"]

# Brands der helt udelades af denne rapport (ledelsesønske). Monitor vises i et
# selvstændigt BI-spor; alt Monitor pilles ud af månedsrapporten — både
# abonnements-brandet (Zuora-matches, filtreret i routeren) OG Monitor-andelen af
# banner/job-annoncesalget (DK-annoncerækkerne summeres kun på Watch DK + FINANS
# DK-sites, så Monitor-sites aldrig tælles med).
EXCLUDED_BRANDS = {"Monitor"}

# Geografi + type pr. brand-label → bruges til at gruppere rapporten land for land
# (Subscription før Advertising) med subtotaler. Lande lægges IKKE sammen til én
# grand total (forskellige valutaer) — kun subtotal pr. (land, type) og total pr. land.
BRAND_GEO = {
    "Watch DK":       ("Denmark", "Subscription"),
    "Finans":         ("Denmark", "Subscription"),
    "Job":            ("Denmark", "Advertising"),
    "Banner":         ("Denmark", "Advertising"),
    "Marketwire":     ("Denmark", "Advertising"),
    "Watch NO":       ("Norway",  "Subscription"),
    "Norge Job":      ("Norway",  "Advertising"),
    "Norge Banner":   ("Norway",  "Advertising"),
    "FinansWatch SE": ("Sweden",  "Subscription"),
    "FinanzBusiness": ("Germany", "Subscription"),
}
COUNTRY_ORDER = ["Denmark", "Norway", "Sweden", "Germany"]
COUNTRY_CURRENCY = {"Denmark": "DKK", "Norway": "NOK", "Sweden": "SEK", "Germany": "EUR"}
TYPE_ORDER = ["Subscription", "Advertising"]


def brand_geo(label: str) -> tuple[str, str]:
    """(land, type) for et brand-label; ('Other','Subscription') som fallback."""
    return BRAND_GEO.get(label, ("Other", "Subscription"))

# PipeDrive-hentede brand-rækker (findes ikke i Zuora-udtrækket). Hver række
# vælges via en [account]- eller [team]-scope i PipedriveDeals, filtreret på
# service_activation_date i perioden og [status]='won'. 'pipelines' begrænser
# yderligere (None = alle pipelines for scopen). Bruttonysalg = won-deals uden
# for cancellation-pipelines; opsigelser = cancellation-pipelines (Σ ABS), så
# netto = brutto − opsigelser. Job/Banner/Norge advertising har ingen
# cancellation-pipelines → opsigelser=0. MarketWire har rigtige opsigelser.
# DK-annoncerækkerne (Job/Banner) bygges IKKE her — de har en kilde-brand-opdeling
# (Watch DK / FINANS DK / Finans programmatisk) der spejler banner-/job-performance-
# dashboardet og henter programmatisk salg fra ProgrammaticSales. Se
# repo.dk_advertising_brand_rows. Her ligger kun Norge-annonce + MarketWire.
PIPEDRIVE_ROWS = [
    # Norge-annonce opdelt i job og banner (ledelsesønske). Hovedrækken dækker HELE
    # watch_no_advertising-kontoen; underrækkerne bryder den ned på Medier24 (M24),
    # Kom24 (KOM24), alle Watch-sites (LIKE '%watch%' — fx EnergiWatch/FinansWatch NO)
    # og Shifter. Hver underrække har sit eget site-budget (BudgetsIntoMedia,
    # Brand='Watch NO'), så Σ underrække-budget = hovedrækkens budget og hver
    # forretning kan holdes op mod sit budget. 'site_like' = LIKE-match, 'site' =
    # eksakt match.
    {"label": "Norge Job",       "scope_col": "account", "scope_val": "watch_no_advertising", "pipelines": ["job"],
     "subrows": [
         {"label": "M24",     "site": "Medier24 NO",
          "budget_where": "[Brand]='Watch NO' AND [DealType]='Job' AND [Site]='Medier24 NO'"},
         {"label": "KOM24",   "site": "Kom24 NO",
          "budget_where": "[Brand]='Watch NO' AND [DealType]='Job' AND [Site]='Kom24 NO'"},
         {"label": "Watch",   "site_like": "%watch%",
          "budget_where": "[Brand]='Watch NO' AND [DealType]='Job' AND [Site] LIKE '%Watch%'"},
         {"label": "Shifter", "site": "Shifter",
          "budget_where": "[Brand]='Watch NO' AND [DealType]='Job' AND [Site]='Shifter'"},
     ]},
    {"label": "Norge Banner",    "scope_col": "account", "scope_val": "watch_no_advertising", "pipelines": ["banner"],
     "subrows": [
         {"label": "M24",     "site": "Medier24 NO",
          "budget_where": "[Brand]='Watch NO' AND [DealType]='Banner' AND [Site]='Medier24 NO'"},
         {"label": "KOM24",   "site": "Kom24 NO",
          "budget_where": "[Brand]='Watch NO' AND [DealType]='Banner' AND [Site]='Kom24 NO'"},
         {"label": "Watch",   "site_like": "%watch%",
          "budget_where": "[Brand]='Watch NO' AND [DealType]='Banner' AND [Site] LIKE '%Watch%'"},
         {"label": "Shifter", "site": "Shifter",
          "budget_where": "[Brand]='Watch NO' AND [DealType]='Banner' AND [Site]='Shifter'"},
     ]},
    {"label": "Marketwire",      "scope_col": "team",    "scope_val": "Team Marketwire",      "pipelines": None},
]

# Budget for reklame-rækkerne (BudgetsIntoMedia). WHERE-fragment uden periode —
# periodeafgrænsning på [BudgetDate] lægges på i repo. Job/Banner (jppol_advertising
# = DK) tager DK-budgettet, mens Norge advertising (watch_no_advertising) tager
# Watch NO-budgettet — Watch NO ekskluderes fra Job/Banner, så det ikke tælles to
# gange. DealType-værdierne ('Job'/'Banner'/Brand='Watch NO') er bekræftet i data.
# DK Banner/Job-budget ekskluderer både Watch NO (egne Norge-rækker) OG Monitor
# (Monitor er pillet ud af rapporten), så budget/deviation matcher de Monitor-frie
# omsætningstal.
AD_BUDGET_WHERE = {
    "Banner":       "[DealType] = 'Banner' AND [Brand] NOT IN ('Watch NO','Monitor')",
    "Job":          "[DealType] = 'Job' AND [Brand] NOT IN ('Watch NO','Monitor')",
    "Norge Job":    "[Brand] = 'Watch NO' AND [DealType] = 'Job'",
    "Norge Banner": "[Brand] = 'Watch NO' AND [DealType] = 'Banner'",
}

# BudgetsIntoMedia.[Brand]-værdier pr. gruppe (matches case-insensitivt).
# SE/DE følger BRAND_GROUP_LABELS i perf-modulet ("Watch SE"/"Watch DE").
BUDGET_BRANDS = {
    "Watch DK":       ["Watch DK", "Watch Int"],
    "Finans":         ["FINANS DK"],
    "Watch NO":       ["Watch NO"],
    "FinansWatch SE": ["Watch SE", "FinansWatch SE"],
    "FinanzBusiness": ["Watch DE", "FinanzBusiness"],
    "Marketwire":     ["MarketWire", "marketwire"],
}

# Brands der vises i lokal valuta (resten i DKK). Norge → NOK, Sverige → SEK,
# Tyskland (FinanzBusiness) → EUR. Salgstallene (brutto/opsigelser/netto) vises i
# denne valuta. Budgettet vises i SAMME valuta som salgstallene — så Deviation
# (netto − budget) er meningsfuld — derfor skal DE-budget indlæses i EUR i
# BudgetsIntoMedia. DK/NO/SE-budget er allerede i lokal valuta.
BRAND_CURRENCY = {
    "Watch NO":       "NOK",
    "FinansWatch SE": "SEK",
    "FinanzBusiness": "EUR",
    "Norge Job":      "NOK",
    "Norge Banner":   "NOK",
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

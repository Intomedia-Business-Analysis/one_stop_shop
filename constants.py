"""Fælles domæne-konstanter for hubbens moduler.

Én kilde til sandheden for brand-/site-lister og pipeline-navne, der før var
copy-pastet mellem modul_perf, modul_forcast og modul_rotation. Skal et brand
tilføjes eller omdøbes (som da MarketWire kom med), rettes det nu KUN her.

Bemærk: _ADM_EXCLUDE-filtrene er bevidst IKKE samlet her — perf, rotation og
marketing filtrerer administrative deals forskelligt (perf ekskluderer kun
administrativ='ja', rotation ekskluderer alt udfyldt + System Admin), så en
sammenlægning ville ændre tallene. De bor fortsat i deres egne moduler.
"""

# Alle abonnements-brands/sites (PipedriveDeals.[sites]) på tværs af landene.
SUBSCRIPTION_BRANDS = [
    "EnergiWatch NO", "MobilityWatch DK", "CleantechWatch DK", "TechWatch NO",
    "AdvokatWatch NO", "Kforum DK", "Seniormonitor", "All Monitor Sites",
    "FinansWatch SE", "Watch Medier DK", "Byrummonitor", "ShippingWatch DK",
    "Idrætsmonitor", "Justitsmonitor", "MatvareWatch NO", "Naturmonitor",
    "Socialmonitor", "FinansWatch DK", "Uddannelsesmonitor", "MedWatch NO",
    "Klimamonitor", "EjendomsWatch DK", "FINANS DK", "DetailWatch DK",
    "FinansWatch NO", "AdvokatWatch DK", "ITWatch DK", "KForum",
    "All Watch Sites DK", "EnergiWatch DK", "Medier24 NO", "AgriWatch DK",
    "Skolemonitor", "EiendomsWatch NO", "Kulturmonitor", "Sundhedsmonitor",
    "MarketWire", "Kom24 NO", "AMWatch DK", "KapitalWatch DK",
    "Policy DK", "HandelsWatch NO", "MedWatch DK", "FødevareWatch DK",
    "Fødevare Watch DK", "All Watch Sites NO", "MediaWatch DK", "Turistmonitor",
    "PolicyWatch DK", "Monitormedier",
    # NB: britisk stavemåde ("Defence") — det er værdien PipeDrive sender i [sites].
    "NordicDefenceWatch",
]

# Brand-grupper (account-nøgle → sites) — bruges af perf og forecast.
BRAND_GROUPS: dict[str, list[str]] = {
    "watch_dk": [
        "FinansWatch DK", "Watch Medier DK", "ShippingWatch DK", "EjendomsWatch DK",
        "AdvokatWatch DK", "ITWatch DK", "EnergiWatch DK", "AgriWatch DK",
        "AMWatch DK", "KapitalWatch DK", "MedWatch DK", "FødevareWatch DK",
        "Fødevare Watch DK", "MediaWatch DK", "DetailWatch DK", "KForum", "Kforum DK",
        "All Watch Sites DK", "PolicyWatch DK", "Policy DK", "MobilityWatch DK", "CleantechWatch DK",
    ],
    "finans": ["FINANS DK"],
    "watch_no": [
        "EnergiWatch NO", "TechWatch NO", "AdvokatWatch NO", "MatvareWatch NO",
        "MedWatch NO", "FinansWatch NO", "EiendomsWatch NO", "Kom24 NO",
        "HandelsWatch NO", "Medier24 NO", "All Watch Sites NO",
    ],
    "watch_se": ["FinansWatch SE"],
    "watch_de": ["FinanzBusiness"],
    "monitor": [
        "Seniormonitor", "Byrummonitor", "Idrætsmonitor", "Justitsmonitor",
        "Naturmonitor", "Socialmonitor", "Uddannelsesmonitor", "Klimamonitor",
        "Kulturmonitor", "Sundhedsmonitor", "Skolemonitor", "Turistmonitor",
        "All Monitor Sites", "Monitormedier",
    ],
    "marketwire": ["MarketWire"],
    # Nordic Defence Watch er et tværnordisk brand: det sælges af Team FINANS DK,
    # Team FINANS Int, Team Watch NO og Team Watch SE, og deals ligger spredt
    # over accounts (watch_medier/watch_no/watch_se). Derfor sin egen gruppe
    # frem for at høre under ét land. Sælger-dashboardet scoper på [owner_name]
    # (+ evt. [team]) og aldrig på [account], så hver sælger ser sit eget NDW-salg
    # uanset hvilken account deal'en er oprettet på.
    "nordic_defence": ["NordicDefenceWatch"],
}

# Pipelines der tæller som opsigelser/churn.
CANCELLATION_PIPELINES = ["Cancellation", "Cancellations", "Opsigelser"]

# ── Valuta: hvornår skal et deal-beløb vises råt i lokal valuta? ─────────────
# Norge-, Sverige- og Tyskland-organisationerne har budgetter indlæst i lokal
# valuta (NOK/SEK/EUR), så deres dashboards regner på [value] råt. Alle andre —
# også en dansk sælger der lukker en NOK/SEK/EUR-deal — måles mod DKK-budgetter
# og skal derfor bruge [value_dkk].
#
# Reglen så tidligere KUN på [currency], og det holdt så længe udenlandsk valuta
# betød "udenlandsk sælger". NordicDefenceWatch brød det: brandet sælges på
# tværs af Norden, så danske sælgere lukker rutinemæssigt deals i SEK/NOK — fx
# et NDW-salg på 80.000 SEK (Team Watch DK, DKK-budget), der blev talt med som
# 80.000 i stedet for 54.376 DKK. Derfor skal ORGANISATIONEN med i vurderingen,
# ikke bare valutaen.
#
# Organisationen bestemmes af [team] og falder tilbage til [account] når team er
# NULL — samme team-før-account-mønster som
# modul_perf.queries_afdelingsleder._deal_group. Fallbacket er ikke kosmetisk:
# en del norske deals (også åbne, der tæller i pipeline-widgets) har team=NULL,
# og uden det ville de blive omregnet til DKK i et NOK-dashboard.
LOCAL_CURRENCY_TEAMS = ["Team Watch NO", "Team Watch NO Advertising", "Team Watch SE"]
LOCAL_CURRENCY_ACCOUNTS = ["watch_no", "watch_no_advertising", "watch_se"]
# Watch DE har hverken team eller NOK/SEK — kun account og EUR.
EUR_LOCAL_ACCOUNTS = ["watch_de"]


def _sql_str_list(values) -> str:
    """Værdierne er modul-konstanter (aldrig brugerinput), så de må inlines.

    Alternativet — %s pr. værdi — ville betyde at ~100 kaldsteder skulle have
    parametre flettet ind i den rigtige rækkefølge i deres params-tupler.
    """
    return "(" + ",".join("'" + str(v).replace("'", "''") + "'" for v in values) + ")"


def local_currency_sql(prefix: str = "", eur_local: bool = False) -> str:
    """SQL-prædikat: sandt når deal'ens [value] skal bruges råt i lokal valuta.

    prefix: tabel-alias inkl. punktum, fx "d." når queryen joiner.
    eur_local: medtag Watch DE's EUR — kun for dashboards hvis budget er i EUR.

    NOK og SEK behandles 1:1 inden for NO/SE: en SEK-deal på et norsk team
    omregnes ikke. Det var også adfærden før organisations-guarden, og der
    findes ingen [value_nok]-kolonne at omregne til — kurserne ligger tæt nok
    (~0,63 vs. ~0,68 DKK) at 1:1 er den mindst forkerte tilnærmelse.
    """
    p = prefix
    scandi = (f"{p}[currency] IN ('NOK','SEK')"
              f" AND ({p}[team] IN {_sql_str_list(LOCAL_CURRENCY_TEAMS)}"
              f" OR ({p}[team] IS NULL"
              f" AND {p}[account] IN {_sql_str_list(LOCAL_CURRENCY_ACCOUNTS)}))")
    if not eur_local:
        return scandi
    return (f"({scandi}) OR ({p}[currency] = 'EUR'"
            f" AND {p}[account] IN {_sql_str_list(EUR_LOCAL_ACCOUNTS)})")


def deal_value_sql(prefix: str = "", eur_local: bool = False) -> str:
    """Deal-beløb i dashboardets regne-valuta: lokal for NO/SE(/DE), ellers DKK.

    Returnerer det NØGNE beløb uden CAST/ABS/SUM, så kaldstedet selv vælger sin
    indpakning (`CAST({...} AS DECIMAL(18,2))`, `ABS(...)`, `SUM(...)`).
    """
    p = prefix
    return (f"COALESCE(CASE WHEN ({local_currency_sql(p, eur_local)})"
            f" THEN {p}[value] ELSE {p}[value_dkk] END,{p}[value])")

MONTH_NAMES_DA = [
    "Januar", "Februar", "Marts", "April", "Maj", "Juni",
    "Juli", "August", "September", "Oktober", "November", "December"
]

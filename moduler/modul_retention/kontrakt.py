"""Kontraktfakta pr. kunde fra dm_abonnement-eksporten: naeste fornyelsesdato.

Filen er Zuoras dim_subscription, eksporteret som

    Business Analysis\\Retention\\dm_abonnement_DDMMYYYY.csv

og laeses med samme mappe- og praefiks-maskineri som forbrugsfilerne, se
usage.py. SQL'en ligger i
`Desktop\\DataBase Views DataGrip\\Retention\\Abonnementerne_med_deres_datoer.txt`.

=============================================================================
HVORFOR KUN DATOEN, OG IKKE EN TIMINGFAKTOR
=============================================================================

Prioriteringsmodellen har plads til en timingfaktor i `score = ARR x vaegt x
timing`, og den har staaet paa 1,0 for alle siden modulet blev bygget, fordi
der ikke fandtes en fornyelsesdato. Nu findes der en. Faktoren staar alligevel
paa 1,0, og de tre grunde skal staa her, saa den ikke bliver sat i en fart:

1. DEN KAN IKKE VALIDERES BAGUD. `betalt_til` er trunkeret til ophoersdatoen
   paa alt der er doedt (Ended topper paa dagens dato), og `auto_fornyes` er
   false paa 20.026 ud af 20.026 Ended-abonnementer, altsaa 100 %. Zuora
   saetter flaget naar abonnementet ophoerer. Begge felter er dermed dateret
   EFTER udfaldet, og en kohortemaaling paa dem ville maale sig selv, praecis
   som gruppen uden aktiv konto der rammer 8 til 9 gange basisraten. En aerlig
   proeve kraever et FREMADRETTET forsoeg: frys dagens datoer, vent seks
   maaneder, maal.

2. DAEKNINGEN ER SKAEV PAA BRAND. Maalt 24-08-2026: 8.242 af 15.212
   abonnementer faar en dato, altsaa 54,2 %. Men monitor faar 0 af 3.744 og
   marketwire 0 af 32, fordi datamarten slet ikke indeholder de brands. En
   multiplikator ville derfor skubbe to hele forretningsomraader ned ad listen
   af en DATAGRUND og ikke en forretningsgrund.

3. EN ABSOLUT DAGSGRAENSE GIVER EN SAESONBOELGE, IKKE EN PRIORITERING.
   Fornyelserne klumper: 2.511 i september 2026 og 2.587 i oktober, af cirka
   8.800 i alt. Maalt i dag ligger 64,5 % af kunderne med en dato inden for 60
   dage. Et loeft paa "under 60 dage" ville altsaa ramme to tredjedele af
   listen nu og naesten ingen i december. Hele listen ville hoppe hvert
   efteraar uden at en eneste kunde havde aendret adfaerd.

DATOEN SELV ER DERIMOD ET FAKTUM DER KAN HANDLES PAA, og den er derfor med som
en kolonne og som tie-breaker mellem raekker med samme score. Specialisten kan
se "fornyes om 23 dage" uden at modellen har paastaaet noget den ikke kan vise.

=============================================================================
FAELDER
=============================================================================

GRAINEN PASSER IKKE. Zuora-abonnementet baerer intet site, saa datoen kan kun
knyttes til KUNDEN og ikke til (kunde, site). En kunde med fem sites faar samme
dato paa alle fem raekker. Det er en tilnaermelse, og den staar paa siden.

NAERMESTE FREMTIDIGE DATO, ikke den seneste og ikke en i fortiden. En kunde med
tre abonnementer har tre datoer, og den foerste beslutning er den der kan naas.

`betalt_til` OVERSTIGER ALDRIG 365 DAGE. Maalt: max er praecis 365. Feltet er
enden paa den nu betalte periode og ruller frem ved fornyelse, saa det er en
fornyelsesdato og ikke en kontraktudloebsdato. Kun 1,3 % af de aktive ser
maanedlige ud og 2,3 % kvartalsvise.

`dim_subscription_id` ER 19 CIFRE. Filen laeses med dtype=str, ellers bliver
den en float og de sidste cifre forsvinder uden fejl. Kolonnen bruges ikke her,
men den er en faelde for den naeste der udvider filen.
"""

import logging
from datetime import date
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

ABONNEMENT_PREFIX = "dm_abonnement"
# Kolonnernes raekkefoelge i eksportens SELECT er kontrakten, som i usage.py.
ABONNEMENT_COLUMNS = [
    "dim_subscription_id", "account_number", "subscription_name",
    "subscription_status", "start_dato", "slut_dato", "opsagt_dato",
    "opsagt_grund", "betalt_til", "auto_fornyes", "oprettet", "oprettet_via",
    "aarspris", "dage_aktiv", "dage_foer_opsigelse",
]

_KONTRAKT_CACHE: dict[tuple, dict] = {}


def find_latest_abonnement_file(folder=None):
    """Nyeste dm_abonnement-eksport."""
    from .usage import _find_latest

    return _find_latest(ABONNEMENT_PREFIX, folder)


def fornyelsesdatoer(path=None) -> dict:
    """Naeste fornyelse pr. kunde.

    Returnerer:
        pr_kunde — {kundenoegle: 'YYYY-MM-DD'}, naermeste FREMTIDIGE dato
        meta     — filnavn, eksportdato, taellinger

    Kun abonnementer i status Active taeller. Et ophoert abonnements
    `betalt_til` er trunkeret til ophoersdatoen og ville se ud som en
    fornyelse der lige er passeret.

    Koblingen gaar gennem dm_kobling og IKKE gennem ACV_snapshot. Snapshottet
    indeholder kun de aktive konti, og selv om det ikke laekker noget her (vi
    ser jo kun paa aktive abonnementer), holder det kilden den samme som
    usage.koblingsgrundlag's primaere.
    """
    from moduler.modul_portfolio_alignment.queries import SCOPE_BY_ZUORA_BRAND

    from .usage import _missing_file_error, _date_from_path, customer_key
    from .usage import load_kobling  # noqa: F401  (holder importen ét sted)

    target = path or find_latest_abonnement_file()
    if not target:
        raise _missing_file_error(ABONNEMENT_PREFIX)

    try:
        cache_key = ("kontrakt", str(target), target.stat().st_mtime)
        cached = _KONTRAKT_CACHE.get(cache_key)
        if cached is not None:
            return cached
    except OSError:
        cache_key = None

    if target.suffix.lower() == ".xlsx":
        df = pd.read_excel(target, header=None)
    else:
        df = pd.read_csv(target, header=None, sep=",", encoding="utf-8",
                         dtype=str)
    if str(df.iloc[0].iloc[0]).strip().lower() == "dim_subscription_id":
        df = df.iloc[1:].reset_index(drop=True)
    if df.shape[1] < len(ABONNEMENT_COLUMNS):
        raise ValueError(
            f"Abonnementsfil {target.name} har {df.shape[1]} kolonner, "
            f"forventer mindst {len(ABONNEMENT_COLUMNS)}: {ABONNEMENT_COLUMNS}"
        )
    df = df.iloc[:, :len(ABONNEMENT_COLUMNS)].copy()
    df.columns = ABONNEMENT_COLUMNS
    for kol in ("account_number", "subscription_status", "betalt_til"):
        df[kol] = df[kol].fillna("").astype(str).str.strip()

    # account_number -> kundenoegle, bygget af dm_kobling.
    kob = _kobling_pr_konto(SCOPE_BY_ZUORA_BRAND, customer_key)

    i_dag = date.today().isoformat()
    pr_kunde: dict = {}
    n_aktive = n_uden_dato = n_passeret = n_ukoblet = 0
    for konto, status, betalt in zip(df["account_number"],
                                     df["subscription_status"],
                                     df["betalt_til"]):
        if status != "Active":
            continue
        n_aktive += 1
        if not betalt:
            n_uden_dato += 1
            continue
        if betalt < i_dag:
            # Betalt til en dato der er passeret. Det er en regningssag, ikke
            # en fornyelse man kan naa at ringe foer.
            n_passeret += 1
            continue
        kunde = kob.get(konto)
        if kunde is None:
            n_ukoblet += 1
            continue
        # Naermeste og ikke seneste: den foerste beslutning er den der kan naas.
        if kunde not in pr_kunde or betalt < pr_kunde[kunde]:
            pr_kunde[kunde] = betalt

    meta = {
        "kontrakt_filename":    target.name,
        "kontrakt_export_date": _date_from_path(target),
        "kontrakt_aktive":      n_aktive,
        "kontrakt_uden_dato":   n_uden_dato,
        "kontrakt_passeret":    n_passeret,
        "kontrakt_ukoblet":     n_ukoblet,
        "kontrakt_kunder":      len(pr_kunde),
    }
    result = {"pr_kunde": pr_kunde, "meta": meta}
    if cache_key:
        _KONTRAKT_CACHE[cache_key] = result
    return result


def _kobling_pr_konto(scope_by_brand: dict, customer_key) -> dict:
    """{account_number: kundenoegle} fra dm_kobling-eksporten.

    Ligger her og ikke i usage.load_kobling, fordi den funktion returnerer
    SAET af kunder (hvem kan kobles), mens vi her skal den anden vej, fra en
    konto til dens kunde. To forskellige spoergsmaal om samme fil.
    """
    from .usage import KOBLING_COLUMNS, find_latest_kobling_file

    target = find_latest_kobling_file()
    if not target:
        return {}
    df = pd.read_csv(target, header=None, sep=",", encoding="utf-8", dtype=str)
    if str(df.iloc[0].iloc[0]).strip().lower() == "account_number":
        df = df.iloc[1:].reset_index(drop=True)
    df = df.iloc[:, :len(KOBLING_COLUMNS)].copy()
    df.columns = KOBLING_COLUMNS
    ud = {}
    for konto, org, brand in zip(df["account_number"].fillna("").astype(str).str.strip(),
                                 df["pipedrive_id"].fillna("").astype(str).str.strip(),
                                 df["brand"].fillna("").astype(str).str.strip()):
        if not (konto and org):
            continue
        scope = scope_by_brand.get(brand)
        if scope:
            ud[konto] = customer_key(scope, org)
    return ud


def dage_til(dato: Optional[str]) -> Optional[int]:
    """Dage fra i dag til `dato`, eller None. Negativ hvis den er passeret."""
    if not dato:
        return None
    try:
        return (date.fromisoformat(dato) - date.today()).days
    except ValueError:
        return None

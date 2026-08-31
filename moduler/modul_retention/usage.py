"""Usage: sidevisninger pr. kunde pr. site pr. maaned, som churn-risiko-signal.

Dataen stammer fra Snowplow i Redshift (external schemas `erhvervsmedier_dsa`
og `jyllandsposten_dsa`). Queryen kan IKKE koeres live: Redshift Spectrum
fakturerer pr. byte scannet fra S3 (~$5/TB), og 14 maaneder tager ~40 sekunder,
saa hvert opslag koster baade tid og kroner.

Derfor samme loesning som Zuora-snapshottet i `modul_portfolio_alignment`:
queryen koeres i DataGrip, resultatet lægges som fil i en kendt mappe, og appen
læser den nyeste. Der er hverken scheduler eller cache-tabel; "cachen" er filen
selv plus det mtime-noeglede opslag herunder.

TO EKSPORTER (mappen kan overrides via USAGE_SNAPSHOT_DIR):

    usage_kunde_DDMMYYYY.csv   - forbruget, kunde x site x maaned
    dm_kobling_DDMMYYYY.csv    - hvilke konti hoerer til hvilken kunde

Begge læses POSITIONELT med eller uden header-række. Ændrer du rækkefølgen i
SQL'en, skal USAGE_COLUMNS eller KOBLING_COLUMNS foelge med, ellers læses tal
som tekst uden at fejle. SQL'en ligger i
`Desktop\\DataBase Views DataGrip\\Retention\\`.

FORBRUGSFILEN ER EN UNION AF TO FORMER, og de udelukker hinanden fuldstændigt:

  * `pipedrive_id` udfyldt og `account_number` tom. SQL'en har allerede koblet
    gennem datamartens `dim_account`, og kundenoeglen bygges af (brand,
    pipedrive_id) uden nogen opslagsfil.
  * `account_number` udfyldt og `pipedrive_id` tom. Konti som `dim_account`
    ikke kender. Maalt 24-08-2026: INGEN af dem findes i dm_kobling, nul af
    3.645. De kan kun oversættes gennem ACV_snapshot, og de er naesten alle
    monitor, som datamarten slet ikke har.

FAELDE, og den er tavs: læses en tom celle uden `.fillna("")` foerst, bevarer
pandas 3 den som NaN gennem `.astype(str)`, og `NaN != ""` er SANDT. Saa bliver
alle raekker i den anden form regnet som koblede med NaN som kundenoegle, uden
en fejl og uden en linje i loggen. Se rensningen i load_usage_kunde.

SIGNALET ER MAANEDLIGT, IKKE DAGSBASERET. Recency-modellen (dage siden sidste
aktivitet) er fjernet 2026-08-10 sammen med risk.py. Begrundelsen skal staa
her, saa den ikke bliver genopfundet: fordi tærsklen var 14 dage, kunne zonen
"sund" ikke eksistere naar filen var ældre end 14 dage. Maalt 2026-08-04 var
77 % af kunderne ved 14 dages filalder flyttet til en værre zone udelukkende
pga. filens alder, og ved 30 dage var ALLE kritiske. Et dagsbaseret signal
kræver ugentlig eksport-kadence, som bliver glemt. "Læste 0 gange i sidste HELE
maaned" er derimod et komplet faktum om en afsluttet maaned og raadner ikke.

BAADE sidevisninger og aktive dage bæres videre, og begge bruges. Sidevisninger
afgoer zonen; aktive dage afgoer om der var en vane at miste
(zones.er_vanebruger). Genmaalt 2026-08-11: blandt de 2.064 stoppede
abonnementer er medianen 4,0 sidevisninger pr. abonnement-maaned over hele
vinduet, nul-maaneder talt med, men 69,7 % har over 20 aktive dage i de 12
maaneder foer referencen. To faste besoeg om maaneden er en vane, og volumen
alene kan ikke skelne den fra stoej.

VIGTIGT: org_id er kun unikt INDEN FOR een Pipedrive-account. Noeglen er derfor
(account, org_id), se customer_key(), og aldrig org_id alene. Verificeret
2026-08-04: 1.226 org_id'er findes i baade `Monitor` og `Watch DK` i
PipeDrive_ACV, og org_name matcher i 0 af dem (org_id 3995 er baade
"Soroe Akademis Skole" og "Ret og Raad Sekretariatet A/S"). Noegles usage paa
org_id alene, blandes to fremmede virksomheders besoeg sammen, og en aktiv
fremmed ville goere en tavs kunde sund, altsaa SKJULE risiko.

KENDTE HULLER:

  * Watch-appen kan ikke tilskrives. `prod.watchmedier.native.ios` og
    `.android` har NUL raekker med brugbar `access_agreements` i
    `snowplow_v2_screenview`. En kunde hvor alle læser i appen staar derfor som
    tavs. Det er en manglende implementering, ikke en umulighed, og
    spoergsmaalet ligger hos datascientisten.

  * Finans-appen ER med siden 24-08-2026, fordi den sætter feltet korrekt.
    Signalet er altsaa web for Watch og app plus web for finans.dk, samme
    afgrænsning som sælger-dashboardet bruger. To dashboards i samme hub med
    forskellige tal for samme kunde bliver opdaget, og saa kan ingen vide
    hvilket der er i stykker. Maalt 24-08-2026: af 1.065 finans-konti med
    trafik i juli læste 649 kun paa web, 377 begge steder og 39 KUN i appen. De
    39 stod tidligere som "har aldrig læst". FOELGEN er at volumen ikke er
    sammenlignelig mellem finans og Watch, og en graf der stiller dem op mod
    hinanden skal sige hvilken kilde den hviler paa.

  * Shifter, Kom24 NO og Medier24 NO ligger uden for erhvervsmedier-schemaet.
    marketwire har intet site i `dbo.retention`. Se zones.UNTRACKBARE_SITES.
"""
import logging
import os
import re
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from env import load_env

load_env()

logger = logging.getLogger(__name__)


DEFAULT_USAGE_DIR = (
    Path.home()
    / "intomedia"
    / "Operations - Dokumenter"
    / "Business Analysis"
    / "Retention"
)
# Dato-suffikset DDMMYYYY er faelles for alle eksport-typer.
_FILE_DATE_RE = re.compile(r"_(\d{2})(\d{2})(\d{4})$")

KUNDE_PREFIX = "usage_kunde"
KOBLING_PREFIX = "dm_kobling"
# MAA IKKE begynde med "usage_kunde": _find_latest globber "<praefiks>_*.csv"
# og sorterer paa datoen i filnavnet, saa et navn som usage_kunde_dyb_... ville
# MATCHE og VINDE over den rigtige maanedsfil, fordi dets dato er nyere. Den
# koerende app ville dermed tavst skifte forbrugsvindue, og et abonnement der
# laeste for over 13 maaneder siden og aldrig siden ville falde fra
# aldrig_i_gang (vaegt 1,00) til gaaet_i_staa (0,70) uden at nogen opdagede
# det. Samme begrundelse staar i kalibrering_usage.txt's header.
KALIBRERING_PREFIX = "kalibrering_usage"
# Kolonnernes raekkefoelge i eksportens SELECT er kontrakten. Filerne laeses
# positionelt, saa flyttes en kolonne i SQL'en, skal listen her foelge med.
USAGE_COLUMNS = [
    "pipedrive_id", "account_number", "brand", "b2b_b2c", "site", "maaned",
    "page_views", "artikelvisninger", "aktive_dage", "unikke_brugere",
    "antal_konti",
]
KOBLING_COLUMNS = [
    "account_number", "pipedrive_id", "brand", "superbrand", "b2b_b2c",
    "account_future_cancelled", "konto_status", "foerste_abo_start",
]

# Cache-key: (sti, mtime). Samme fil → samme resultat, ingen genparsning.
_USAGE_CACHE: dict[tuple, dict] = {}


# ---------------------------------------------------------------------------
# Filsøgning
# ---------------------------------------------------------------------------

def _candidate_usage_dirs() -> list[Path]:
    """Alle stier der skal afsøges, i prioriteret rækkefølge.

    USAGE_SNAPSHOT_DIR i .env må indeholde flere stier separeret med ';' — så
    kan samme .env bruges lokalt og på serveren, hvor mappen ligger forskellige
    steder. Første sti der eksisterer vinder.
    """
    override = os.getenv("USAGE_SNAPSHOT_DIR", "").strip()
    if override:
        return [Path(p.strip()) for p in override.split(";") if p.strip()]
    return [DEFAULT_USAGE_DIR]


def get_usage_dir() -> Path:
    """Første kandidat-mappe der faktisk eksisterer.

    Findes ingen, returneres første kandidat alligevel — så fejlbeskeden
    "ingen fil fundet i <sti>" peger på et meningsfuldt sted.
    """
    candidates = _candidate_usage_dirs()
    for c in candidates:
        if c.exists():
            return c
    return candidates[0] if candidates else DEFAULT_USAGE_DIR


def _find_latest(prefix: str, folder: Optional[Path] = None) -> Optional[Path]:
    """Nyeste <prefix>_DDMMYYYY.{csv,xlsx} i mappen.

    Sorterer på datoen i filnavnet hvis den kan læses; ellers på mtime. Begge
    eksport-typer ligger i samme mappe, så prefixet er det der adskiller dem.
    """
    folder = folder or get_usage_dir()
    if not folder.exists():
        return None
    candidates: list[tuple[date, float, Path]] = []
    for pattern in (f"{prefix}_*.csv", f"{prefix}_*.xlsx"):
        for p in folder.glob(pattern):
            m = _FILE_DATE_RE.search(p.stem)
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


def find_latest_usage_file(folder: Optional[Path] = None) -> Optional[Path]:
    """Nyeste usage_kunde-eksport."""
    return _find_latest(KUNDE_PREFIX, folder)


def find_latest_kobling_file(folder: Optional[Path] = None) -> Optional[Path]:
    """Nyeste dm_kobling-eksport."""
    return _find_latest(KOBLING_PREFIX, folder)


def find_latest_kalibrering_file(folder: Optional[Path] = None) -> Optional[Path]:
    """Nyeste kalibrering_usage-eksport, eller None. Kun maalinger bruger den."""
    return _find_latest(KALIBRERING_PREFIX, folder)


def _date_from_path(p: Path) -> Optional[str]:
    """Eksport-dato (ISO) ud af ét filnavn, med filens mtime som fallback."""
    m = _FILE_DATE_RE.search(p.stem)
    if m:
        dd, mm, yyyy = m.groups()
        try:
            return date(int(yyyy), int(mm), int(dd)).isoformat()
        except ValueError:
            pass
    try:
        return datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d")
    except OSError:
        return None


# ---------------------------------------------------------------------------
# Indlæsning
# ---------------------------------------------------------------------------

def _missing_file_error(prefix: str) -> FileNotFoundError:
    """Fejl der viser ALLE konfigurerede stier, om de findes, og filantal i hver.

    `get_usage_dir()` returnerer kun den første sti og kan derfor vildlede om,
    hvor der reelt blev ledt — det er den fælde denne besked findes for.
    """
    lines = []
    for c in _candidate_usage_dirs():
        if c.exists():
            n = len(list(c.glob(f"{prefix}_*.csv")) + list(c.glob(f"{prefix}_*.xlsx")))
            lines.append(f"  [FINDES, {n} fil(er)] {c}")
        else:
            lines.append(f"  [FINDES IKKE]        {c}")
    detalje = "\n".join(lines) or "  (ingen stier konfigureret)"
    return FileNotFoundError(
        f"Ingen {prefix}_*.csv eller .xlsx fundet. Tjekkede stier "
        f"(fra USAGE_SNAPSHOT_DIR i .env):\n{detalje}"
    )


def load_usage_kunde(path: Optional[Path] = None) -> dict:
    """Laes usage_kunde-eksporten som en DataFrame.

    Returnerer en DataFrame og ikke en dict-af-dicts: filen er ~183.000 raekker,
    og `iterrows()` over dem tager minutter. Opslag pr. abonnement bygges een
    gang i forbrug_pr_abonnement().

    Returnerer:
        frame    - DataFrame med kolonnerne i USAGE_COLUMNS, renset
        maaneder - sorteret liste af maaneder i filen ('YYYY-MM')
        meta     - sti, filnavn, eksportdato, taellinger
    """
    target = path or find_latest_usage_file()
    if not target:
        raise _missing_file_error(KUNDE_PREFIX)

    try:
        # Praefikset i noeglen: dm_kobling bruger samme _USAGE_CACHE, og det
        # goer en kollision umulig i stedet for blot usandsynlig.
        cache_key = ("kunde", str(target), target.stat().st_mtime)
        cached = _USAGE_CACHE.get(cache_key)
        if cached is not None:
            return cached
    except OSError:
        cache_key = None

    if target.suffix.lower() == ".xlsx":
        df = pd.read_excel(target, header=None)
    else:
        # dtype=str: kontonumre som K00370461 er tekst. Lader vi pandas gætte
        # pr. blok, kan samme kolonne tolkes forskelligt i to halvdele af filen.
        df = pd.read_csv(target, header=None, sep=",", encoding="utf-8", dtype=str)

    # Header-detektion på første celle — filen kan eksporteres med eller uden.
    if str(df.iloc[0].iloc[0]).strip().lower() == "pipedrive_id":
        df = df.iloc[1:].reset_index(drop=True)

    if df.shape[1] < len(USAGE_COLUMNS):
        raise ValueError(
            f"Usage-fil {target.name} har {df.shape[1]} kolonner, forventer "
            f"mindst {len(USAGE_COLUMNS)}: {USAGE_COLUMNS}"
        )
    df = df.iloc[:, :len(USAGE_COLUMNS)].copy()
    df.columns = USAGE_COLUMNS

    # fillna("") FOERST: i pandas 3 bevarer .astype(str) en tom celle som NaN,
    # og NaN != "" er SANDT. Uden den bliver alle 39.953 ukoblede raekker
    # regnet som koblede med NaN som kundenoegle.
    for kol in ("pipedrive_id", "account_number", "brand", "site"):
        df[kol] = df[kol].fillna("").astype(str).str.strip()
    # [:7] klipper til 'YYYY-MM' uanset om eksporten skriver det korte eller det
    # lange datoformat. Hele modulet sammenligner måneder som tekst.
    df["maaned"] = df["maaned"].astype(str).str.strip().str[:7]
    for kol in ("page_views", "artikelvisninger", "aktive_dage", "unikke_brugere",
                "antal_konti"):
        df[kol] = pd.to_numeric(df[kol], errors="coerce").fillna(0).astype("int64")

    # En ulæselig måned kan ikke placeres på en tidsakse og ville falde stille ud
    # af enhver månedssammenligning. Frasortér den synligt frem for tavst.
    gyldig = df["maaned"].str.match(r"^\d{4}-\d{2}$", na=False)
    if (~gyldig).any():
        logger.warning("Usage-fil %s: %d rækker har ulæselig måned og springes over",
                       target.name, int((~gyldig).sum()))
    df = df[gyldig].reset_index(drop=True)

    meta = {
        "path":         str(target),
        "filename":     target.name,
        # Fra den fil der faktisk blev indlæst — ikke fra et nyt opslag i mappen,
        # som ville give et forkert svar når `path` er givet eksplicit.
        "export_date":  _date_from_path(target),
        "row_count":    len(df),
        # kunder_i_fil og ikke kunder: forbrug_pr_abonnement saetter
        # meta["kunder"] til dem der faktisk blev koblet, og ville overskrive.
        "kunder_i_fil":   int(df.loc[df["pipedrive_id"] != "", "pipedrive_id"].nunique()),
        "konti_ukoblet":  int(df.loc[df["account_number"] != "", "account_number"].nunique()),
        "sites":        int(df["site"].nunique()),
    }
    result = {"frame": df, "maaneder": sorted(df["maaned"].unique().tolist()),
              "meta": meta}
    if cache_key:
        _USAGE_CACHE[cache_key] = result
    return result


# ---------------------------------------------------------------------------
# Oversættelse til kunde-nøgle (account, org_id)
# ---------------------------------------------------------------------------

def customer_key(account: str, org_id) -> tuple[str, str]:
    """Kanonisk kunde-nøgle. Brug ALTID denne — aldrig org_id alene.

    `dbo.retention` har samme grain-forudsætning: dens PARTITION BY indeholder
    `account`, netop fordi org_id kolliderer mellem Pipedrive-accounts.
    """
    return (str(account).strip(), str(org_id).strip())


def load_kobling(path: Optional[Path] = None) -> dict:
    """Laes dm_kobling-eksporten: hvilke konti hoerer til hvilken kunde.

    Filen daekker datamartens `dim_account` og indeholder OGSAA de ophoerte
    konti. Det er hele grunden til at den findes: et fravaer i den siger ikke
    noget om udfaldet, og kilden kan derfor bruges paa en historisk maaned uden
    at laekke.

    Et brand vi ikke kender kan ikke placeres i en account, og raekken udelades
    derfor: at gaette ville koble usage til den forkerte virksomhed. Antallet
    logges, saa et nyt brand bliver synligt i stedet for stille at mangle.

    Returnerer:
        kunder - saet af kundenoegler der kan oversaettes
        aktive - delmaengde med mindst een konto i status Active
        meta   - sti, filnavn, eksportdato, antal konti
    """
    from moduler.modul_portfolio_alignment.queries import SCOPE_BY_ZUORA_BRAND

    target = path or find_latest_kobling_file()
    if not target:
        raise _missing_file_error(KOBLING_PREFIX)

    try:
        cache_key = ("kobling", str(target), target.stat().st_mtime)
        cached = _USAGE_CACHE.get(cache_key)
        if cached is not None:
            return cached
    except OSError:
        cache_key = None

    if target.suffix.lower() == ".xlsx":
        df = pd.read_excel(target, header=None)
    else:
        df = pd.read_csv(target, header=None, sep=",", encoding="utf-8", dtype=str)

    if str(df.iloc[0].iloc[0]).strip().lower() == "account_number":
        df = df.iloc[1:].reset_index(drop=True)
    if df.shape[1] < len(KOBLING_COLUMNS):
        raise ValueError(
            f"Koblingsfil {target.name} har {df.shape[1]} kolonner, forventer "
            f"mindst {len(KOBLING_COLUMNS)}: {KOBLING_COLUMNS}"
        )
    df = df.iloc[:, :len(KOBLING_COLUMNS)].copy()
    df.columns = KOBLING_COLUMNS
    # fillna("") af samme grund som i load_usage_kunde: uden den er en tom
    # celle NaN, og NaN er sand i en if.
    for kol in ("pipedrive_id", "brand", "konto_status"):
        df[kol] = df[kol].fillna("").astype(str).str.strip()

    kunder: set = set()
    aktive: set = set()
    ukendte_brands: dict = {}
    for org, brand, status in zip(df["pipedrive_id"], df["brand"],
                                  df["konto_status"]):
        if not org:
            continue
        scope = SCOPE_BY_ZUORA_BRAND.get(brand)
        if not scope:
            ukendte_brands[brand] = ukendte_brands.get(brand, 0) + 1
            continue
        kunde = customer_key(scope, org)
        kunder.add(kunde)
        # Kolonnen heder active_subscriptions i datamarten og indeholder TEKST,
        # ikke et antal. Eksporten omdoeber den til konto_status.
        if status == "Active":
            aktive.add(kunde)
    if ukendte_brands:
        logger.warning(
            "dm_kobling har brands uden mapping til en Pipedrive-account "
            "(raekkerne udelades af koblingen): %s", ukendte_brands,
        )

    meta = {
        "kobling_path":        str(target),
        "kobling_filename":    target.name,
        "kobling_export_date": _date_from_path(target),
        "kobling_konti":       int(len(df)),
    }
    result = {"kunder": kunder, "aktive": aktive, "meta": meta}
    if cache_key:
        _USAGE_CACHE[cache_key] = result
    return result


def koblingsgrundlag() -> dict:
    """Hvilke kunder KAN oversaettes, og har de en aktiv Zuora-konto.

    TO kilder, og begge er noedvendige:

      dm_kobling    Primaer. Indeholder ophoerte konti, saa den laekker ikke.
                    Den kender ikke monitor: datamarten har slet ikke brandet.
      ACV_snapshot  Tilbagefald. Den ENESTE kilde til monitor, og den eneste vej
                    fra et account_number til en kunde for de konti som
                    dim_account ikke kender.

    LAEKAGE-ADVARSEL: snapshottet indeholder kun de AKTIVE konti, saa et fravaer
    i den halvdel betyder "ophoert". Bruges grundlaget paa en historisk maaned,
    er den halvdel altsaa dateret EFTER udfaldet, og det var praecis den fejl
    der gav gruppen uden kobling 86 til 88 % opsigelsesrate i maalingen
    21-08-2026. Til levende prioritering er det harmloest: dér ER spoergsmaalet
    hvem der er kunde i dag.

    `uden_aktiv_konto` er kunder der kun kan kobles gennem OPHOERTE konti. De
    har en raekke i `dbo.retention` (som kommer fra Pipedrive) men ingen aktiv
    konto i Zuora. De maa IKKE taelle som en almindelig aldrig_i_brug: zonen
    ville saa maale "kontoen er ophoert" og ikke "kunden laeser ikke", og den
    ville derfor maale kunstigt staerkt naar vaegtene proeves efter.

    Returnerer:
        kunder           - koblingsbare kundenoegler, begge kilder
        aktive           - delmaengde med mindst een aktiv Zuora-konto
        uden_aktiv_konto - koblingsbare uden aktiv konto, KUN hvor det vides
        acc_til_kunde    - account_number -> kundenoegle, KUN fra snapshottet
        meta             - taellinger og filnavne fra begge kilder
    """
    from moduler.modul_portfolio_alignment.queries import (
        SCOPE_BY_ZUORA_BRAND,
        load_zuora_snapshot,
    )

    kobling = load_kobling()
    zuora = load_zuora_snapshot()

    acc_til_kunde: dict = {}
    snapshot_kunder: set = set()
    ukendte_brands: dict = {}
    for r in zuora["enterprise_rows"]:
        acct = r.get("account_number")
        org = r.get("pipedrive_id")
        if not (acct and org):
            continue
        brand = str(r.get("brand") or "").strip()
        scope = SCOPE_BY_ZUORA_BRAND.get(brand)
        if not scope:
            ukendte_brands[brand] = ukendte_brands.get(brand, 0) + 1
            continue
        kunde = customer_key(scope, org)
        acc_til_kunde[str(acct).strip()] = kunde
        snapshot_kunder.add(kunde)
    if ukendte_brands:
        logger.warning(
            "Zuora-snapshottet har brands uden mapping til en Pipedrive-account "
            "(raekkerne udelades af koblingen): %s", ukendte_brands,
        )

    kunder = kobling["kunder"] | snapshot_kunder
    # At staa i snapshottet ER at have en aktiv konto: filen er dagens
    # portefoelje. Derfor tæller den som aktiv uden at have en status-kolonne.
    aktive = kobling["aktive"] | snapshot_kunder
    # Traekkes fra dm_koblings kunder og ikke fra hele saettet: en kunde der kun
    # findes i snapshottet har ingen KENDT status, og et gaet paa "ophoert"
    # ville sende alle monitor-kunder i den gruppe.
    uden_aktiv_konto = kobling["kunder"] - aktive

    # Saettet og ikke kun tallet: kohortemaalingen skal kunne SKAERE gruppen ud
    # af baade zonen og basisraten, fordi snapshottet kun kender de aktive
    # konti og derfor er dateret efter udfaldet.
    kun_fra_snapshot = snapshot_kunder - kobling["kunder"]

    meta = dict(kobling["meta"])
    meta["koblingsbare_kunder"] = len(kunder)
    meta["kunder_kun_fra_snapshot"] = len(kun_fra_snapshot)
    meta["kunder_uden_aktiv_konto"] = len(uden_aktiv_konto)
    return {"kunder": kunder, "aktive": aktive,
            "uden_aktiv_konto": uden_aktiv_konto,
            "kun_fra_snapshot": kun_fra_snapshot,
            "acc_til_kunde": acc_til_kunde, "meta": meta}


def latest_complete_month(months: list) -> Optional[str]:
    """Seneste måned i listen som IKKE er den indeværende.

    Queryens vindue slutter i indeværende måned, som altid er ufuldstændig — et
    par dages besøg holdt op mod en hel forrige måned får enhver kunde til at se
    ud som et frit fald. Den må derfor aldrig være default for et churn-signal.
    """
    # strftime("%Y-%m") og ikke isoformat(): månederne i filen er syv tegn, og
    # '2026-08' < '2026-08-01' er SANDT, fordi den korte streng er et præfiks af
    # den lange. Med isoformat ville den ufuldstændige indeværende måned altså
    # blive regnet som komplet — præcis den fejl funktionen findes for.
    this_month = date.today().strftime("%Y-%m")
    complete = [m for m in months if m < this_month]
    return complete[-1] if complete else None


def serie_og_dage(forbrug: dict, kunde: tuple, site: str) -> tuple[dict, dict]:
    """(sidevisninger, aktive dage) pr. måned for ét abonnement.

    Pakkeabonnementer måles på KUNDENS samlede forbrug: `Watch Medier DK` giver
    adgang til alle Watch-titler, og kun 7% af abonnenterne læser pakkens eget
    site, mens 79% læser noget. Se zones.PAKKE_SITES.

    Dagene slås op på SAMME niveau som sidevisningerne — ellers ville en pakke
    få vanebruger-testen på ét site og zonen på syv.

    Ligger her og ikke i risiko.py, fordi kunde-detaljesiden (Kundeside) skal
    tegne præcis den serie, zonen blev beregnet på. To kopier af valget ville
    kunne drive fra hinanden, og så ville grafen modsige zonen ved siden af.
    """
    # Lokal import af samme grund som i forbrug_pr_abonnement: zones.py må
    # kunne få brug for noget herfra uden at der opstår en cirkel.
    from .zones import PAKKE_SITES

    if site in PAKKE_SITES:
        return (forbrug["pr_kunde"].get(kunde, {}),
                forbrug["dage_pr_kunde"].get(kunde, {}))
    return (forbrug["pr_abonnement"].get((kunde, site), {}),
            forbrug["dage_pr_abonnement"].get((kunde, site), {}))


def forbrug_pr_abonnement(path: Optional[Path] = None) -> dict:
    """Sidevisninger slået op på abonnement og på kunde.

    `path` peger normalt på ingenting (None), og så læses den nyeste
    usage_kunde-eksport, som er appens eneste adfærd. Sat af kohortemaaling.py
    til et dybere kalibreringsudtræk, uden at det ændrer noget her.

    Returnerer:
        pr_abonnement      — {(kunde, kanonisk_site): {maaned: sidevisninger}}
        pr_kunde           — {kunde: {maaned: sidevisninger}}
        dage_pr_abonnement — {(kunde, kanonisk_site): {maaned: aktive dage}}
        dage_pr_kunde      — {kunde: {maaned: aktive dage}}
        koblingsbare       — kundenoegler der kan oversaettes, UANSET forbrug
        uden_aktiv_konto   — delmaengde uden aktiv Zuora-konto
        kun_fra_snapshot   — delmaengde der KUN kan kobles via ACV_snapshot
        maaneder           — sorteret liste
        meta               — fra load_usage_kunde, plus tællinger

    `pr_kunde` findes for pakkeabonnementer: `Watch Medier DK` giver adgang til
    alle Watch-titler, og kun 7% af dens 264 abonnenter læser pakkens eget site,
    mens 79% læser noget. Målt på sitet ville hele pakken stå permanent kritisk.
    Se zones.PAKKE_SITES.

    Sitet normaliseres med zones.kanonisk_site, som også folder .com-udgaverne
    ind i søster-sitet — samme funktion som abonnementssiden bruger, og dét er
    grunden til at de to vokabularer mødes (41 af 46 sites matcher direkte).

    TO VEJE til kundenoeglen, se koblingsgrundlag():
      1. Raekken har selv et pipedrive_id, og noeglen bygges af (brand, id).
      2. Raekken har kun et account_number, som slaas op i ACV_snapshot.
    Raekker der ikke kan tage nogen af vejene udelades. Antallet ligger i meta,
    saa hullet er synligt og ikke bare vaek.

    `koblingsbare` er IKKE det samme som noeglerne i `pr_kunde`, og forskellen
    ER zonen aldrig_i_brug: en kunde kan godt kunne oversaettes og alligevel
    ikke have een raekke i forbrugsfilen. Bruges pr_kunde.keys() som
    koblingssaet, forsvinder aldrig_i_brug og alt lander i intet_signal.

    `dage_pr_abonnement`/`dage_pr_kunde` bærer `aktive_dage` og er grundlaget for
    zones.er_vanebruger. Kolonnen har ligget i eksporten hele tiden og blev
    kasseret her indtil 2026-08-10, hvor målingen viste hvorfor den er
    nødvendig: blandt de 2.064 stoppede abonnementer er medianen 4,0
    sidevisninger pr. abonnement-måned med nul-måneder talt med, men 69,7% har
    over 20 aktive dage i de 12 måneder før referencen. To faste besøg om
    måneden er en vane, og sidevisnings-volumen kan ikke se det.

    ADVARSEL om dagene: de lægges sammen som sidevisningerne, så en dag kan
    tælles to gange — to Zuora-konti på samme site, eller for `dage_pr_kunde`
    det samme kalenderdøgn på fem sites. Summen er en ØVRE grænse. Fejlen peger
    mod at kalde noget en vane, altså mod at vise risiko frem for at skjule den.
    En præcis optælling kræver dags-opløsning, som eksporten aggregerer væk.
    """
    # Lokal import: der er ingen cirkel i dag, men lægges den i toppen opstår
    # den i det øjeblik zones.py får brug for noget herfra.
    from moduler.modul_portfolio_alignment.queries import SCOPE_BY_ZUORA_BRAND

    from .zones import kanonisk_site

    usage = load_usage_kunde(path)
    df = usage["frame"]
    grundlag = koblingsgrundlag()
    acc_til_kunde = grundlag["acc_til_kunde"]

    pr_abonnement: dict = {}
    pr_kunde: dict = {}
    dage_pr_abonnement: dict = {}
    dage_pr_kunde: dict = {}
    ukoblede = set()
    ukendte_brands: dict = {}
    raekker_datamart = 0
    raekker_snapshot = 0

    # zip over de rå kolonner og ikke df.iterrows(): iterrows bygger en Series
    # pr. række og tager minutter på 183.000 rækker.
    for org, konto, brand, site, maaned, pv, dage in zip(
            df["pipedrive_id"], df["account_number"], df["brand"], df["site"],
            df["maaned"], df["page_views"], df["aktive_dage"]):
        # Raekkefoelgen er betydningsbaerende: har raekken et pipedrive_id, er
        # den koblet i SQL'en, og account_number er tom. De to former optraeder
        # aldrig sammen, saa et opslag i snapshottet ville vaere spildt.
        if org:
            scope = SCOPE_BY_ZUORA_BRAND.get(brand)
            if not scope:
                ukendte_brands[brand] = ukendte_brands.get(brand, 0) + 1
                continue
            kunde = customer_key(scope, org)
            raekker_datamart += 1
        else:
            kunde = acc_til_kunde.get(konto)
            if kunde is None:
                ukoblede.add(konto)
                continue
            raekker_snapshot += 1
        site_k = kanonisk_site(site)
        # Lægges sammen, ikke tildeles: en kunde kan have flere Zuora-konti på
        # samme site, og .com- og .dk-rækker folder sammen til samme nøgle.
        a = pr_abonnement.setdefault((kunde, site_k), {})
        a[maaned] = a.get(maaned, 0) + int(pv)
        k = pr_kunde.setdefault(kunde, {})
        k[maaned] = k.get(maaned, 0) + int(pv)
        # Samme opsummering for dagene — se advarslen i docstringen om at det
        # gør summen til en øvre grænse.
        da = dage_pr_abonnement.setdefault((kunde, site_k), {})
        da[maaned] = da.get(maaned, 0) + int(dage)
        dk = dage_pr_kunde.setdefault(kunde, {})
        dk[maaned] = dk.get(maaned, 0) + int(dage)

    if ukendte_brands:
        logger.warning(
            "Forbrugsfilen har brands uden mapping til en Pipedrive-account "
            "(raekkerne udelades): %s", ukendte_brands,
        )

    meta = dict(usage["meta"])
    meta.update(grundlag["meta"])
    meta["kunder"] = len(pr_kunde)
    meta["abonnementer"] = len(pr_abonnement)
    # RAEKKER og ikke kunder. Tallene skal paa siden: en raekke fra snapshottet
    # hviler paa en kilde der kun kender de aktive konti, og det er en anden
    # slags sandhed end en raekke der kom koblet fra datamarten.
    meta["raekker_fra_datamart"] = raekker_datamart
    meta["raekker_fra_snapshot"] = raekker_snapshot
    # Antal KONTI der ikke kan kobles, ikke antal rækker — de to tal fortæller
    # vidt forskellige historier.
    meta["konti_uden_kobling"] = len(ukoblede)
    return {"pr_abonnement": pr_abonnement, "pr_kunde": pr_kunde,
            "dage_pr_abonnement": dage_pr_abonnement,
            "dage_pr_kunde": dage_pr_kunde,
            "koblingsbare": grundlag["kunder"],
            "uden_aktiv_konto": grundlag["uden_aktiv_konto"],
            "kun_fra_snapshot": grundlag["kun_fra_snapshot"],
            "maaneder": usage["maaneder"], "meta": meta}


def _aggreger_pr_site(df) -> dict:
    """Ren funktion over usage_kunde-framen: sidevisninger og artikelvisninger
    lagt sammen pr. (kanonisk site, måned), UDEN kundekobling.

    Split ud af `forbrug_pr_site` for at kunne testes mod en lille
    fixture-DataFrame uden filen — samme grund som `koblingsgrundlag` og
    `forbrug_pr_abonnement` er to funktioner og ikke én.

    INGEN KUNDENØGLE. Det er hele grunden til at `monitor` kan vises: 0 af
    monitors 36.707 rækker har et `pipedrive_id` (målt 2026-08-28), så de kan
    kun kobles til en KUNDE gennem ACV_snapshot, som kun kender AKTIVE konti
    — se koblingsgrundlag()'s lækage-advarsel. Et SITE-panel har ikke brug for
    kundenøglen og rammer derfor ikke den begrænsning. Populationen her er
    derfor BREDERE end forbrug_pr_abonnement's: enhver række på sitet tælles
    med, uanset om kunden bag den kan slås op. Det skal stå i panelets
    undertekst, ikke kun her.

    `.com`-udgaver foldes ind i søster-sitet af samme `zones.kanonisk_site`
    som abonnementssiden bruger — det er dét der lader de to vokabularer
    mødes, og derfor bruges den samme funktion begge steder frem for en
    lokal kopi der kunne drive fra den.

    Returnerer {site: {maaned: {"page_views": n, "artikelvisninger": n}}}.
    """
    from moduler.modul_portfolio_alignment.queries import SCOPE_BY_ZUORA_BRAND
    from .zones import kanonisk_site

    # Udenlandske rækker filtreres PR. RÆKKE, ikke pr. site: nordicdefencewatch.dk
    # er blandet (16 rækker brand='Watch', 63 rækker brand='WatchMedierSE',
    # målt 2026-08-28), så et site-niveau-filter ville enten beholde eller
    # kassere begge grupper forkert. Rækker med tomt brand beholdes, de kan
    # ikke placeres i en scope og skal ikke straffes for det.
    UDENLANDSKE_SCOPES = {"watch_no", "watch_se", "watch_de"}

    pr_site: dict = {}
    # zip over de rå kolonner, ikke df.iterrows(): samme begrundelse som
    # forbrug_pr_abonnement — iterrows bygger en Series pr. række og tager
    # minutter på 183.000 rækker.
    for site, maaned, pv, artikler, brand in zip(
            df["site"], df["maaned"], df["page_views"], df["artikelvisninger"],
            df["brand"]):
        if SCOPE_BY_ZUORA_BRAND.get(brand) in UDENLANDSKE_SCOPES:
            continue
        site_k = kanonisk_site(site)
        if site_k is None:
            # marketwire og de sitelose rækker har intet at aggregere PÅ —
            # der findes intet site-panel-bucket for "intet site", i
            # modsætning til risikolistens INTET_SITE, som nøgler på en
            # KUNDE. Rammer aldrig i praksis (usage-filen har altid en
            # site-streng), men skal ikke kunne krasje på en tom.
            continue
        m = pr_site.setdefault(site_k, {})
        c = m.setdefault(maaned, {"page_views": 0, "artikelvisninger": 0})
        c["page_views"] += int(pv)
        c["artikelvisninger"] += int(artikler)
    return pr_site


def forbrug_pr_site(path: Optional[Path] = None) -> dict:
    """Side- og artikelvisninger pr. site pr. måned — Porteføljens
    "Side- og artikelvisninger pr. site"-panel.

    I MODSÆTNING TIL `forbrug_pr_abonnement` er der INGEN kundekobling her,
    se `_aggreger_pr_site`'s docstring for hvorfor det er nødvendigt for at
    kunne vise `monitor`. Konsekvensen er at denne funktions population er
    større: en række uden kendt kunde tælles stadig med her, men ville være
    faldet ud af `forbrug_pr_abonnement`.

    `path` peger normalt på ingenting (None), og så læses den nyeste
    usage_kunde-eksport, som er appens eneste adfærd.

    Returnerer:
        pr_site   — {site: {maaned: {"page_views", "artikelvisninger"}}}
        maaneder  — sorteret liste, 'YYYY-MM', fra usage["maaneder"]
        meta      — fra load_usage_kunde, plus antal sites
    """
    usage = load_usage_kunde(path)
    pr_site = _aggreger_pr_site(usage["frame"])

    meta = dict(usage["meta"])
    meta["sites_med_forbrug"] = len(pr_site)
    return {"pr_site": pr_site, "maaneder": usage["maaneder"], "meta": meta}

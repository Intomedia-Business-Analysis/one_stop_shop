"""Usage: sidevisninger pr. abonnement pr. måned, som churn-risiko-signal.

Dataen stammer fra Snowplow (`snowplow_v2_pageview` i Redshift, external schema
`erhvervsmedier_dsa_prv_external`). Queryen kan IKKE køres live: Redshift
Spectrum fakturerer pr. byte scannet fra S3 (~$5/TB), og 13 måneder tager ~40
sekunder, så hvert opslag koster både tid og kroner.

Derfor samme løsning som Zuora-snapshottet i `modul_portfolio_alignment`:
queryen køres i DataGrip, resultatet lægges som fil i en kendt mappe, og appen
læser den nyeste — så vi undgår systemintegration mod koncernens dataplatform.
Der er derfor hverken scheduler eller cache-tabel; "cachen" er filen selv plus
det mtime-nøglede opslag herunder.

Der er TO eksporter, begge i samme mappe (kan overrides via USAGE_SNAPSHOT_DIR):

    usage_trend_DDMMYYYY.csv    — pr. konto pr. site pr. måned (13 mdr.)
        account_number, site, maaned, page_views, artikelvisninger,
        aktive_dage, unikke_brugere

    usage_recency_DDMMYYYY.csv  — dage siden sidste aktivitet (12 mdr.)
        access_account_number, last_activity_date, days_since_last_activity,
        active_days, page_views

Begge læses positionelt med eller uden header-række, som Zuora-snapshottet.
Ændrer du rækkefølgen i SQL'en, skal USAGE_COLUMNS/RECENCY_COLUMNS følge med.
SQL'en ligger i `Desktop\\DataBase Views DataGrip\\usage_trend.txt` og
`usage_recency.txt`.

TREND ER PRIMÆRSIGNALET, IKKE RECENCY. Målt 2026-08-04: fordi recency-tærsklen
er 14 dage, kan zonen "sund" ikke eksistere når filen er ældre end 14 dage — ved
14 dages alder er 77% af kunderne flyttet til en værre zone udelukkende pga.
filens alder, og ved 30 dage er ALLE kritiske. Et dagsbaseret signal kræver
ugentlig eksport-kadence, som bliver glemt. "Læste 0 gange i sidste HELE måned"
er derimod et komplet faktum om en afsluttet måned og rådner ikke. Se zones.py.

Recency-eksportet beholdes fordi det er billigt og giver dags-opløsning inden
for indeværende måned, men det bærer ikke zonerne længere.

Recency har et bredere vindue med vilje: en kunde der har været tavs længere end
vinduet forsvinder helt ud af resultatet, og "mangler i output" er den højeste
risiko — men kan ikke skelnes fra "aldrig trackt". Med 12 måneder kan de to
tilstande skelnes. Se recency_zone(), som derfor har "intet_signal" som en
selvstændig tilstand og ikke som kritisk.

Nøgle-kæden til retention er indirekte:
    Snowplow access_account_number = Zuora account_number → pipedrive_id = org_id
Zuora-snapshottet er altså påkrævet for at kunne koble usage til en kunde i
`dbo.retention`. Mangler snapshottet, kan rå usage stadig læses — kun
oversættelsen til kunde-nøglen fejler.

VIGTIGT: org_id er kun unikt INDEN FOR én Pipedrive-account. Nøglen er derfor
(account, org_id) — se customer_key() — ikke org_id alene. Verificeret
2026-08-04: 1.226 org_id'er findes i både `Monitor` og `Watch DK` i
PipeDrive_ACV, og org_name matcher i 0 af dem (org_id 3995 er både
"Sorø Akademis Skole" og "Ret og Råd Sekretariatet A/S"). I Zuora-snapshottet
optræder 893 af 8.948 pipedrive_id'er under mere end ét brand. Nøgles usage på
org_id alene, blandes to fremmede virksomheders besøg sammen — og fordi
recency_by_customer() tager min(dage), gør én aktiv fremmed en tavs kunde
"sund". Fejlen peger altså mod at SKJULE risiko.

Kendt begrænsning: FINANS DK's site sætter aldrig `access_account_number` i
Snowplow (komplet tracking-hul, verificeret over 3 måneder), så FINANS
DK-kunder får aldrig et usage-signal. Det skal fremgå i dashboardet, ellers
ligner de kunder uden aktivitet.
"""

import logging
import os
import re
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


DEFAULT_USAGE_DIR = (
    Path.home()
    / "intomedia"
    / "Operations - Dokumenter"
    / "Business Analysis"
    / "Retention"
)
# Dato-suffikset DDMMYYYY er fælles for begge eksport-typer.
_FILE_DATE_RE = re.compile(r"_(\d{2})(\d{2})(\d{4})$")

TREND_PREFIX = "usage_trend"
# Kolonnerækkefølgen i eksportens SELECT er kontrakten. Filen læses positionelt,
# så flyttes en kolonne i SQL'en, skal denne liste følge med.
USAGE_COLUMNS = [
    "account_number", "site", "maaned",
    "page_views", "artikelvisninger", "aktive_dage", "unikke_brugere",
]

RECENCY_PREFIX = "usage_recency"
RECENCY_COLUMNS = [
    "access_account_number", "last_activity_date", "days_since_last_activity",
    "active_days", "page_views",
]

# Zone-tærskler i dage siden sidste aktivitet. 14 dage er fase 1-reglen fra
# churn-oplægget — et gæt, ikke et måleresultat. De SKAL valideres mod
# dbo.retention (hvor lang tavshed havde de kunder, der faktisk churnede?)
# før de bruges til at prioritere en sælgers arbejdsdag.
ZONE_ATTENTION_DAYS = 14
ZONE_CRITICAL_DAYS = 30

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
    """Nyeste usage_trend-eksport."""
    return _find_latest(TREND_PREFIX, folder)


def find_latest_recency_file(folder: Optional[Path] = None) -> Optional[Path]:
    """Nyeste usage_recency-eksport."""
    return _find_latest(RECENCY_PREFIX, folder)


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


def current_usage_date() -> Optional[str]:
    """Eksport-dato for den fil der ville blive indlæst nu.

    Bruges til at vise datafriskhed i dashboardet — uden den kan brugeren ikke
    se, om usage-signalet er fra i går eller fra sidste måned.
    """
    p = find_latest_usage_file()
    return _date_from_path(p) if p else None


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


def _as_int(v) -> Optional[int]:
    """Tolerant int-konvertering. None ved tomt/uparsbart.

    `prev_month_page_views` og `change` er NULL for den første måned i vinduet
    (LAG har intet at se tilbage på), og det skal kunne skelnes fra 0 besøg.
    """
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return int(float(str(v).strip()))
    except (TypeError, ValueError):
        return None


def load_usage_trend(path: Optional[Path] = None) -> dict:
    """Læs usage_trend-eksporten som en DataFrame.

    Returnerer en DataFrame og ikke en dict-af-dicts: filen er ~156.000 rækker,
    og `iterrows()` over dem tager minutter. Opslag pr. abonnement bygges én gang
    i forbrug_pr_abonnement().

    Returnerer:
        frame    — DataFrame med kolonnerne i USAGE_COLUMNS, renset
        maaneder — sorteret liste af måneder i filen ('YYYY-MM')
        meta     — sti, filnavn, eksportdato, tællinger
    """
    target = path or find_latest_usage_file()
    if not target:
        raise _missing_file_error(TREND_PREFIX)

    try:
        # "trend" i nøglen: recency-loaderen bruger samme _USAGE_CACHE, og et
        # præfiks gør en kollision umulig i stedet for blot usandsynlig.
        cache_key = ("trend", str(target), target.stat().st_mtime)
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
    if str(df.iloc[0].iloc[0]).strip().lower() == "account_number":
        df = df.iloc[1:].reset_index(drop=True)

    if df.shape[1] < len(USAGE_COLUMNS):
        raise ValueError(
            f"Usage-fil {target.name} har {df.shape[1]} kolonner, forventer "
            f"mindst {len(USAGE_COLUMNS)}: {USAGE_COLUMNS}"
        )
    df = df.iloc[:, :len(USAGE_COLUMNS)].copy()
    df.columns = USAGE_COLUMNS

    df["account_number"] = df["account_number"].astype(str).str.strip()
    df["site"] = df["site"].astype(str).str.strip()
    # [:7] klipper til 'YYYY-MM' uanset om eksporten skriver det korte eller det
    # lange datoformat. Hele modulet sammenligner måneder som tekst.
    df["maaned"] = df["maaned"].astype(str).str.strip().str[:7]
    for kol in ("page_views", "artikelvisninger", "aktive_dage", "unikke_brugere"):
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
        "konti":        int(df["account_number"].nunique()),
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


def _account_to_customer_map() -> dict:
    """Zuora account_number → (pd_account, org_id).

    Én Zuora-konto har præcis én pipedrive_id, men flere konti kan pege på samme
    organisation. Konti uden pipedrive_id udelades — de kan ikke kobles til en
    kunde i `dbo.retention`.

    Zuora's `brand` oversættes til Pipedrive's `account` med SCOPE_BY_ZUORA_BRAND
    fra alignment-modulet, så mappingen kun findes ét sted. Et brand vi ikke
    kender kan ikke placeres i en account, og rækken udelades derfor — at gætte
    ville koble usage til den forkerte virksomhed. Antallet logges, så et nyt
    brand i Zuora bliver synligt i stedet for stille at mangle.

    Importen ligger inde i funktionen, fordi Zuora-snapshottet kun er nødvendigt
    for oversættelsen; rå usage kan læses uden det.
    """
    from moduler.modul_portfolio_alignment.queries import (
        SCOPE_BY_ZUORA_BRAND,
        load_zuora_snapshot,
    )

    zuora = load_zuora_snapshot()
    acct_to_customer: dict[str, tuple[str, str]] = {}
    ukendte_brands: dict[str, int] = {}
    for r in zuora["enterprise_rows"]:
        acct = r.get("account_number")
        org  = r.get("pipedrive_id")
        if not (acct and org):
            continue
        brand = str(r.get("brand") or "").strip()
        scope = SCOPE_BY_ZUORA_BRAND.get(brand)
        if not scope:
            ukendte_brands[brand] = ukendte_brands.get(brand, 0) + 1
            continue
        acct_to_customer[str(acct).strip()] = customer_key(scope, org)
    if ukendte_brands:
        logger.warning(
            "Zuora-snapshottet har brands uden mapping til en Pipedrive-account "
            "(rækkerne udelades af usage-koblingen): %s", ukendte_brands,
        )
    return acct_to_customer


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


def forbrug_pr_abonnement() -> dict:
    """Sidevisninger slået op på abonnement og på kunde.

    Returnerer:
        pr_abonnement — {(kunde, kanonisk_site): {maaned: sidevisninger}}
        pr_kunde      — {kunde: {maaned: sidevisninger}}
        maaneder      — sorteret liste
        meta          — fra load_usage_trend, plus tællinger

    `pr_kunde` findes for pakkeabonnementer: `Watch Medier DK` giver adgang til
    alle Watch-titler, og kun 7% af dens 264 abonnenter læser pakkens eget site,
    mens 79% læser noget. Målt på sitet ville hele pakken stå permanent kritisk.
    Se zones.PAKKE_SITES.

    Sitet normaliseres med zones.kanonisk_site, som også folder .com-udgaverne
    ind i søster-sitet — samme funktion som abonnementssiden bruger, og dét er
    grunden til at de to vokabularer mødes (41 af 46 sites matcher direkte).

    Kunder uden Zuora-kobling udelades: uden den kan et kontonummer ikke blive
    til en kunde. Antallet ligger i meta, så hullet er synligt og ikke bare væk.
    """
    # Lokal import: der er ingen cirkel i dag, men lægges den i toppen opstår
    # den i det øjeblik zones.py får brug for noget herfra.
    from .zones import kanonisk_site

    usage = load_usage_trend()
    df = usage["frame"]
    acct_to_customer = _account_to_customer_map()

    pr_abonnement: dict = {}
    pr_kunde: dict = {}
    ukoblede = set()

    # zip over de rå kolonner og ikke df.iterrows(): iterrows bygger en Series
    # pr. række og tager minutter på 156.000 rækker.
    for konto, site, maaned, pv in zip(df["account_number"], df["site"],
                                       df["maaned"], df["page_views"]):
        kunde = acct_to_customer.get(konto)
        if kunde is None:
            ukoblede.add(konto)
            continue
        site_k = kanonisk_site(site)
        # Lægges sammen, ikke tildeles: en kunde kan have flere Zuora-konti på
        # samme site, og .com- og .dk-rækker folder sammen til samme nøgle.
        a = pr_abonnement.setdefault((kunde, site_k), {})
        a[maaned] = a.get(maaned, 0) + int(pv)
        k = pr_kunde.setdefault(kunde, {})
        k[maaned] = k.get(maaned, 0) + int(pv)

    meta = dict(usage["meta"])
    meta["kunder"] = len(pr_kunde)
    meta["abonnementer"] = len(pr_abonnement)
    # Antal KONTI der ikke kan kobles, ikke antal rækker — de to tal fortæller
    # vidt forskellige historier.
    meta["konti_uden_zuora"] = len(ukoblede)
    return {"pr_abonnement": pr_abonnement, "pr_kunde": pr_kunde,
            "maaneder": usage["maaneder"], "meta": meta}


# ---------------------------------------------------------------------------
# Recency — dage siden sidste aktivitet (fase 1-signalet)
# ---------------------------------------------------------------------------

def recency_zone(days: Optional[int]) -> str:
    """Zone ud fra dage siden sidste aktivitet.

    `None` giver "intet_signal" — ikke "kritisk". Forskellen er afgørende: en
    kunde uden Snowplow-spor kan være tavs, men kan også bare være utrackbar
    (FINANS DK sætter aldrig access_account_number). At vise et datahul som
    kritisk risiko sender sælgeren efter en kunde, der måske læser hver dag.
    """
    if days is None:
        return "intet_signal"
    if days >= ZONE_CRITICAL_DAYS:
        return "kritisk"
    if days >= ZONE_ATTENTION_DAYS:
        return "opmærksomhed"
    return "sund"


def load_usage_recency(path: Optional[Path] = None) -> dict:
    """Læs usage_recency-eksportet (dage siden sidste aktivitet pr. konto).

    `days_since_last_activity` REGNES HER, ud fra `last_activity_date` og dagens
    dato — ikke læst fra filen. Kolonnen i eksportet var korrekt den dag queryen
    kørte, men et fem dage gammelt eksport ville ellers undervurdere inaktiviteten
    med fem dage, og signalet ville blive mildere med tiden i stedet for skarpere.
    Den eksporterede værdi bevares som `days_at_export` til sammenligning.

    Returnerer:
        rows        — én pr. konto
        by_account  — {access_account_number: row}
        meta        — path, filnavn, export_date, file_age_days, tællinger pr. zone
    """
    target = path or find_latest_recency_file()
    if not target:
        raise _missing_file_error(RECENCY_PREFIX)

    try:
        cache_key = (str(target), target.stat().st_mtime, date.today().isoformat())
        cached = _USAGE_CACHE.get(cache_key)
        if cached is not None:
            return cached
    except OSError:
        cache_key = None

    if target.suffix.lower() == ".xlsx":
        df_raw = pd.read_excel(target, header=None)
    else:
        df_raw = pd.read_csv(target, header=None, sep=",", encoding="utf-8")

    if str(df_raw.iloc[0].iloc[0]).strip().lower() == "access_account_number":
        df = df_raw.iloc[1:].reset_index(drop=True)
    else:
        df = df_raw

    if df.shape[1] < len(RECENCY_COLUMNS):
        raise ValueError(
            f"Recency-fil {target.name} har {df.shape[1]} kolonner, "
            f"forventer mindst {len(RECENCY_COLUMNS)}: {RECENCY_COLUMNS}"
        )
    df = df.iloc[:, :len(RECENCY_COLUMNS)]
    df.columns = RECENCY_COLUMNS

    today = date.today()
    rows: list[dict] = []
    by_account: dict[str, dict] = {}
    zones: dict[str, int] = {}

    for _, r in df.iterrows():
        acct = str(r["access_account_number"]).strip() if pd.notna(r["access_account_number"]) else ""
        if not acct:
            continue
        raw_date = str(r["last_activity_date"]).strip()[:10] if pd.notna(r["last_activity_date"]) else ""
        try:
            last = date.fromisoformat(raw_date)
        except ValueError:
            logger.warning("Ulæselig last_activity_date %r for konto %s — springes over",
                           raw_date, acct)
            continue
        days = (today - last).days
        row = {
            "access_account_number": acct,
            "last_activity_date":    last.isoformat(),
            "days_since_last_activity": days,
            "days_at_export":        _as_int(r["days_since_last_activity"]),
            "active_days":           _as_int(r["active_days"]) or 0,
            "page_views":            _as_int(r["page_views"]) or 0,
            "zone":                  recency_zone(days),
        }
        rows.append(row)
        by_account[acct] = row
        zones[row["zone"]] = zones.get(row["zone"], 0) + 1

    export_date = _date_from_path(target)
    file_age = None
    if export_date:
        try:
            file_age = (today - date.fromisoformat(export_date)).days
        except ValueError:
            pass

    meta = {
        "path":          str(target),
        "filename":      target.name,
        "export_date":   export_date,
        # Hvor gammelt eksportet er. Dashboardet bør vise det: et signal om
        # inaktivitet er selv værdiløst, hvis det er en måned gammelt.
        "file_age_days": file_age,
        "account_count": len(by_account),
        "zones":         zones,
        "thresholds":    {
            "attention_days": ZONE_ATTENTION_DAYS,
            "critical_days":  ZONE_CRITICAL_DAYS,
        },
    }
    result = {"rows": rows, "by_account": by_account, "meta": meta}
    if cache_key:
        _USAGE_CACHE[cache_key] = result
    return result


def recency_by_customer() -> dict:
    """Recency nøglet på (account, org_id) i stedet for Zuora-kontonummer.

    Har en organisation flere Zuora-konti, tæller den MEST NYLIGE aktivitet på
    tværs af dem: er én konto aktiv, er kunden aktiv. Derfor min(dage), ikke
    gennemsnit — et gennemsnit ville få en kunde med én aktiv og én sovende konto
    til at se halvt i risiko ud, hvilket ingen kan handle på.

    Netop dét min() er grunden til at nøglen SKAL indeholde account: delte to
    fremmede virksomheder én nøgle, ville den mest aktive af dem gøre den anden
    "sund", og risikoen ville forsvinde ud af listen uden spor.

    `active_days` og `page_views` summeres, da de er volumen-mål.
    """
    rec = load_usage_recency()
    acct_to_customer = _account_to_customer_map()

    out: dict[tuple[str, str], dict] = {}
    for acct, row in rec["by_account"].items():
        key = acct_to_customer.get(acct)
        if not key:
            continue  # Zuora-konto uden pipedrive_id — kan ikke kobles til en kunde
        bucket = out.get(key)
        if bucket is None:
            out[key] = {
                "days_since_last_activity": row["days_since_last_activity"],
                "last_activity_date":       row["last_activity_date"],
                "active_days":              row["active_days"],
                "page_views":               row["page_views"],
                "zone":                     row["zone"],
                "accounts":                 1,
            }
            continue
        bucket["accounts"]    += 1
        bucket["active_days"] += row["active_days"]
        bucket["page_views"]  += row["page_views"]
        if row["days_since_last_activity"] < bucket["days_since_last_activity"]:
            bucket["days_since_last_activity"] = row["days_since_last_activity"]
            bucket["last_activity_date"]       = row["last_activity_date"]
            bucket["zone"]                     = row["zone"]
    return out

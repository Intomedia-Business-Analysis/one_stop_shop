"""Opsigelser i varsel: abonnementer der ER opsagt, men endnu ikke ophørt.

Datalaget bag panelet "Opsigelser i varsel" på /retention/overview#operationel.
Panelet erstattede "Porteføljen måned for måned" 2026-09-01: en 127-måneders
vækstkurve viser en BEHOLDNING, og fanen "Operationel og diagnostisk" skal vise
noget man kan nå at handle på.

INTET NYT SQL, og det er hele pointen. Reglen for hvornår en opsigelse gælder
ligger i queries.db_opsigelser: en vundet opsigelse tæller kun, når den er
dateret EFTER det seneste livstegn på aftalen. Uden den datosammenligning ville
4.430 genforhandlede abonnementer se opsagte ud. Populationen er
queries.abonnementer_med_ejer, altså PRÆCIS risikolistens: dansk afgrænsning,
B2B-filter, team-filter og ARR pr. abonnement ligger allerede inde i den.

DERFOR KAN DE TO SIDER IKKE BLIVE UENIGE. "Opkald og risiko" viser de samme
abonnementer i sit Opsagt-kort, regnet af de samme to funktioner. Målt
2026-09-01 på 12.984 danske abonnementer: 184 har en gældende opsigelse, hvoraf
175 er i varsel (2,71 mio. kr., 156 kunder) og 9 er forfaldne, altså opsagt,
ophørt, men stadig i bogen. Alle ni er marketwire, den ældste ophørt
2023-03-04. De forfaldne er en kendt fejl i dbo.retention-viewet og hører ikke
til i dette panel, som handler om det der kan nås.

DEN LETTE VEJ ER VALGT. risiko.abonnementer_i_risiko har allerede opsagt_dato
på hver række, men koster 11,7 sekunder, fordi den aggregerer 182.000
forbrugsrækker for at bestemme zoner. Zonen er irrelevant for en opsigelse. De
to opslag her tager 4,9 sekunder tilsammen og er målt 2026-09-01 til at give
nøjagtig samme tal: 184 og 175.
"""
import logging
from datetime import date, timedelta

from .outcomes import INTET_SITE
from .queries import abonnementer_med_ejer, db_opsigelser

logger = logging.getLogger(__name__)

# Hvor mange dage frem "haster" går. Bruges til én kolonne i tabellen, så
# specialisten kan se hvad der løber ud inden næste månedsskifte uden selv at
# regne på datoerne.
HASTER_DAGE = 30


def saml_varsel(abonnementer: list, opsigelser: dict, i_dag: str) -> dict:
    """Aggregér de abonnementer hvis opsigelse endnu ikke er trådt i kraft.

    `abonnementer` er rækker fra queries.abonnementer_med_ejer, `opsigelser` er
    opslaget fra queries.db_opsigelser, og `i_dag` er 'YYYY-MM-DD'.

    REN MED VILJE. Ingen database, intet date.today(). Derfor kan
    roegtest_varsel.py bevise reglerne uden en forbindelse, og derfor kommer
    `i_dag` udefra. Samme mønster som prioritering.py's rene funktioner.

    NØGLEN BRUGER DEN RÅ `sites`, ikke et kanonisk sitenavn: db_opsigelser
    bygger sin nøgle på præcis samme værdi, og marketwires NULL bliver None i
    begge ender. Oversættes den ene side, holder marketwire op med at kunne
    slås op, uden at noget fejler.

    DATOERNE SAMMENLIGNES SOM TEKST. 'YYYY-MM-DD' sorterer leksikografisk som
    den sorterer kronologisk, og resten af modulet gør det samme.

    OPHØR PRÆCIS I DAG ER IKKE VARSEL. Der er intet varsel tilbage at bruge, og
    rækken hører til blandt de forfaldne, som panelet ikke viser.
    """
    haster_til = (date.fromisoformat(i_dag) + timedelta(days=HASTER_DAGE)).isoformat()

    i_alt = 0
    uden_arr = 0
    arr_i_alt = 0.0
    kunder = set()
    pr_maaned: dict = {}
    pr_site: dict = {}

    for a in abonnementer:
        dato = opsigelser.get((a["account"], a["org_id"], a["sites"]))
        if not dato or dato <= i_dag:
            continue

        i_alt += 1
        kunder.add((a["account"], a["org_id"]))

        # NUL ER IKKE UKENDT. arr_pr_abonnement er None, når ACV ikke har et
        # beløb for netop det site, og de rækker må ikke tælle som 0 kr. — så
        # ville summen se komplet ud. De tælles i `uden_arr` i stedet, og
        # panelet skriver at summen er et minimum. Samme regel som
        # queries.db_acv_beloeb_pr_site's nul-kommentar.
        arr = a.get("arr_pr_abonnement")
        arr = float(arr) if arr is not None else None
        if arr is None:
            uden_arr += 1
        else:
            arr_i_alt += arr

        m = pr_maaned.setdefault(dato[:7],
                                 {"maaned": dato[:7], "antal": 0, "arr_dkk": 0.0})
        m["antal"] += 1
        m["arr_dkk"] += arr or 0.0

        # marketwire har sites = None. INTET_SITE er modulets faste bucket, og
        # den SKAL bruges: ellers bliver gruppens nøgle None og står uden navn
        # i tabellen.
        site = a["sites"] or INTET_SITE
        s = pr_site.setdefault((site, a["account"]),
                               {"site": site, "account": a["account"],
                                "antal": 0, "arr_dkk": 0.0, "haster": 0})
        s["antal"] += 1
        s["arr_dkk"] += arr or 0.0
        if dato <= haster_til:
            s["haster"] += 1

    return {
        "i_alt": i_alt,
        "kunder": len(kunder),
        "arr_dkk": arr_i_alt,
        "uden_arr": uden_arr,
        # Månederne KRONOLOGISK: rækkefølgen er selve budskabet i søjlegrafen.
        "pr_maaned": [pr_maaned[k] for k in sorted(pr_maaned)],
        # Sites faldende på antal, med sitenavnet som andet kriterium, så to
        # sites med samme antal ikke bytter plads mellem to kald.
        "pr_site": sorted(pr_site.values(), key=lambda r: (-r["antal"], r["site"])),
        "meta": {"i_dag": i_dag, "haster_dage": HASTER_DAGE},
    }


def opsigelser_i_varsel(owner_name: str | None = None,
                        teams: list | None = None,
                        maaned: str | None = None) -> dict:
    """Hent de to opslag og aggregér dem. Tynd skal om saml_varsel.

    `maaned` er 'YYYY-MM' og defaulter til indeværende, samme valg som
    risiko.abonnementer_i_risiko, så de to ser på samme bog.

    INGEN try/except her. db_opsigelser svarer selv {} ved fejl, og det ville
    give et panel der siger "0 opsigelser" — en løgn, ikke en tom tilstand.
    Fejler abonnementer_med_ejer, skal den boble op og blive til en 500, som
    panelet viser som en fejlbesked. Et halvt panel er værre end et fejlet.
    """
    maaned = maaned or date.today().strftime("%Y-%m")
    abonnementer = abonnementer_med_ejer(maaned, owner_name=owner_name, teams=teams)
    data = saml_varsel(abonnementer, db_opsigelser(), date.today().isoformat())
    data["meta"]["maaned"] = maaned
    data["meta"]["abonnementer_i_bogen"] = len(abonnementer)
    return data

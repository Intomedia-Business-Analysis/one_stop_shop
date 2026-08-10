"""Kunde-detalje: forberedelse til opkaldet (PRD §7.4).

Samler alt specialisten skal have foran sig, før hun ringer: kundens
abonnementer med zone og ARR, 12 måneders forbrug pr. abonnement, ejer og team,
startdato — og hele historikken af tidligere samtaler og udfald.

Siden skal kunne åbnes for ENHVER kunde, ikke kun dem på risikolisten (PRD
§7.4). Ellers kan der ikke registreres et udfald på et sundt abonnement, og så
måler §9 kun de sager vi allerede havde mistanke til.

HVORFOR EN CACHE HER OG IKKE I risiko.py: `abonnementer_i_risiko()` tager 3,6
sekunder selv med varm fil, fordi `forbrug_pr_abonnement()` aggregerer 182.000
rækker ved hvert kald. Arbejdsgangen i PRD §8 åbner én kunde ad gangen, så det
ville være 3,6 sekunder pr. opslag. Cachen ligger i DETTE modul, fordi
acceptkriteriet for hele modellen hænger på risiko.py — en optimering dér ville
kræve, at samtlige zonetal blev bevist igen.
"""
import logging
import time

from .outcomes import INTET_SITE, db_historik, db_seneste_udfald
from .risiko import abonnementer_i_risiko
from .usage import forbrug_pr_abonnement, serie_og_dage
from .zones import foregaaende_maaneder

logger = logging.getLogger(__name__)

# Hvor længe et beregnet risikobillede genbruges. Signalet er MÅNEDLIGT (PRD
# §3), så selv en time ville være fagligt forsvarligt; ti minutter er valgt for
# at en ny usage-eksport eller en ARR-rettelse slår igennem inden for en pause,
# uden at en specialist venter 3,6 sekunder pr. kunde.
CACHE_SEKUNDER = 600

# Antal måneder i trendgrafen på detaljesiden. PRD §7.4: "usage-trend 12 mdr".
TREND_MAANEDER = 12

_cache: dict[tuple, tuple[float, dict]] = {}


def _cachet(noegle: tuple, beregn):
    """Genbrug et resultat i CACHE_SEKUNDER. Ingen baggrundstråd, ingen TTL-ryd.

    Ordboken vokser med én post pr. unik team-afgrænsning, og dem er der en
    håndfuld af — en oprydning ville koste mere kompleksitet end den sparer.
    """
    nu = time.monotonic()
    ramt = _cache.get(noegle)
    if ramt is not None and nu - ramt[0] < CACHE_SEKUNDER:
        return ramt[1]
    vaerdi = beregn()
    _cache[noegle] = (nu, vaerdi)
    return vaerdi


def ryd_cache() -> None:
    """Tøm cachen. Kaldes efter en registrering, så siden ikke viser gamle tal."""
    _cache.clear()


def _risiko(teams, abo_maaned):
    # teams er en liste og kan ikke være nøgle. Sorteret tuple, så to kald med
    # samme teams i forskellig rækkefølge rammer samme post.
    noegle = ("risiko", abo_maaned, tuple(sorted(teams)) if teams else None)
    return _cachet(noegle, lambda: abonnementer_i_risiko(teams=teams,
                                                         abo_maaned=abo_maaned))


def _forbrug():
    return _cachet(("forbrug",), forbrug_pr_abonnement)


def _trend_maaneder(reference: str | None) -> list[str]:
    """12 måneder til og med referencen, ÆLDSTE først (grafen læses venstre→højre).

    Bemærk at vinduet IKKE er det samme som vanebruger-vinduet:
    `foregaaende_maaneder(reference, 12)` er de 12 måneder FØR referencen og
    udelader den. Til vanebruger-testen er det rigtigt, men referencemåneden er
    netop den, `bestem_zone` sammenligner mod de foregående tre — en graf uden
    den ville mangle det punkt, hele zonen afgøres af.

    Konsekvens for UI'et: summen af grafens `dage` er derfor ikke lig
    rækkens `aktive_dage_12m`, som måler vanebruger-vinduet. Vis rækkens eget
    tal, ikke en sum af søjlerne.
    """
    if not reference:
        return []
    return list(reversed(foregaaende_maaneder(reference, TREND_MAANEDER - 1))) + [reference]


def kunde_detalje(account: str, org_id: int, teams: list | None = None,
                  abo_maaned: str | None = None) -> dict:
    """Alt om én kunde, klar til opkaldet.

    `teams` er brugerens dataafgrænsning fra routeren. Er kunden uden for den,
    findes hun ikke i risikobilledet, og siden svarer som for en ukendt kunde —
    det er en adgangsbegrænsning, ikke en fejl.

    Returnerer `abonnementer: []` og `ingen_aktive: True`, hvis kunden ikke har
    aktive abonnementer i måneden. Historikken hentes ALLIGEVEL: en kunde der
    lige er opsagt, er præcis den man har brug for at kunne slå op.
    """
    data = _risiko(teams, abo_maaned)
    rows = [r for r in data["rows"]
            if r["account"] == account and r["org_id"] == org_id]

    historik = db_historik(account, org_id)
    seneste = db_seneste_udfald()
    reference = data["meta"]["reference_maaned"]
    maaneder = _trend_maaneder(reference)

    forbrug = None
    if rows and reference:
        try:
            forbrug = _forbrug()
        except Exception:
            # Zonerne i rows er allerede beregnet; kun grafen mangler. Siden må
            # ikke gå ned, fordi en CSV-fil er væk — men den skal sige det.
            logger.exception("Forbrugsdata kunne ikke læses til kunde-detalje")

    abonnementer = []
    for r in rows:
        kunde = (r["account"], r["org_id"])
        trend = []
        if forbrug is not None:
            serie, dage = serie_og_dage(forbrug, kunde, r["site_kanonisk"])
            trend = [{"maaned": m,
                      "sidevisninger": serie.get(m, 0),
                      "dage": dage.get(m, 0)} for m in maaneder]

        # Nøglen i RetentionOutcomes bruger sentinel for manglende site, mens
        # risikorækken har det rå felt fra dbo.retention (NULL for marketwire).
        u = seneste.get((r["account"], r["org_id"], r["site"] or INTET_SITE))

        abonnementer.append({
            **r,
            "trend": trend,
            "seneste_udfald": u,
            # Eneste kilde til en fornyelsesdato, vi har: den nogen har skrevet
            # ned efter et opkald. Zuora-feltet er stadig ubekræftet, jf. PRD
            # §11 pkt. 1, og derfor er risiko.TIMINGFAKTOR fortsat 1,0.
            "fornyelsesdato": u["renewal_date"] if u else None,
        })

    # Rækkefølge: højeste score først, som på risikolisten. Abonnementer uden
    # score (ukendt ARR) sidst — de er uopgjorte, ikke risikofrie.
    abonnementer.sort(key=lambda a: (a["score"] is None, -(a["score"] or 0)))

    foerste = rows[0] if rows else None
    arr_total = sum(a["arr_dkk"] for a in abonnementer if a["arr_dkk"] is not None)

    return {
        "kunde": {
            "account":   account,
            "org_id":    org_id,
            # kunde_arr_dkk er kundens samlede ARR fra ACV og kan være større
            # end summen her, hvis nogle abonnementer ligger uden for måneden.
            "org_name":  foerste["org_name"] if foerste else None,
            "owner_name": foerste["owner_name"] if foerste else None,
            "teams":     foerste["teams"] if foerste else None,
            "arr_dkk":   foerste["kunde_arr_dkk"] if foerste else None,
            "arr_aktive_abonnementer": arr_total,
            "antal_abonnementer": len(abonnementer),
            "mikrokunde": bool(foerste and foerste["mikrokunde"]),
        },
        "abonnementer": abonnementer,
        "historik":     historik,
        "ingen_aktive": not rows,
        "meta": {
            "abo_maaned":        data["meta"]["abo_maaned"],
            "reference_maaned":  reference,
            "reference_alder":   data["meta"]["reference_alder"],
            "trend_maaneder":    maaneder,
            "usage_error":       data["meta"]["usage_error"],
            "usage_export_date": data["meta"]["usage_export_date"],
            "thresholds_validated": data["meta"]["thresholds_validated"],
        },
    }

"""Kunde-detalje: forberedelse til opkaldet (PRD §7.4).

Samler alt specialisten skal have foran sig, før hun ringer: kundens
abonnementer med zone og ARR, 12 måneders forbrug pr. abonnement, ejer og team,
startdato — og hele historikken af tidligere samtaler og udfald.

Siden skal kunne åbnes for ENHVER kunde, ikke kun dem på risikolisten (PRD
§7.4). Ellers kan der ikke registreres et udfald på et sundt abonnement, og så
måler §9 kun de sager vi allerede havde mistanke til.

CACHEN LIGGER I cache.py, ikke her. `abonnementer_i_risiko()` tager 3,6 sekunder
selv med varm fil, og arbejdsgangen i PRD §8 åbner én kunde ad gangen. Da §7.3's
prioriteringsside kom til, fik den brug for samme beregning — og to caches over
samme tal kan komme i utakt, så de to sider ville kunne vise forskellige zoner
for samme kunde. Se cache.py for hvorfor den heller ikke ligger i risiko.py.
"""
import logging

from . import cache
from .outcomes import INTET_SITE, db_historik, db_seneste_udfald
from .usage import serie_og_dage
from .zones import foregaaende_maaneder, zone_alvor

logger = logging.getLogger(__name__)

# Antal måneder i trendgrafen på detaljesiden. PRD §7.4: "usage-trend 12 mdr".
TREND_MAANEDER = 12


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
    data = cache.risiko(teams, abo_maaned)
    rows = [r for r in data["rows"]
            if r["account"] == account and r["org_id"] == org_id]

    historik = db_historik(account, org_id)
    seneste = db_seneste_udfald()
    reference = data["meta"]["reference_maaned"]
    maaneder = _trend_maaneder(reference)

    forbrug = None
    if rows and reference:
        try:
            forbrug = cache.forbrug()
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
    #
    # Zonens alvor bryder uafgjort. Uden den afgjorde Pythons stabile sort
    # resten ud fra rækkernes tilfældige rækkefølge fra risikolaget, og hos
    # Jyske Bank lå AgriWatch ("tavs længere") derfor over AMWatch ("stoppet")
    # — begge scorer 0,50, fordi tre zoner deler den vægt. Se zones.zone_alvor.
    abonnementer.sort(key=lambda a: (a["score"] is None,
                                     -(a["score"] or 0),
                                     zone_alvor(a["zone"])))

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

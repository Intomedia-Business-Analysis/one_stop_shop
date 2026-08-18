"""Churn-risiko pr. abonnement: zone, vægt og score.

Erstatter recency-modellen i risk.py, jf. PRD §3 hvor 14/30-dages-modellen
udfases. Ligger som eget modul, fordi begge modeller skal kunne køre side om
side indtil siden er lagt om — de har hver sit ZONE_ORDER og ZONE_LABELS, og et
alias-import ville skjule hvilken model man læser.

Måleenheden er abonnementet (account, org_id, site), jf. PRD §2, og tabellen på
§7.2-siden er derfor pr. abonnement. Foldningen til én række pr. kunde hører til
PRD §4 og prioriteringssiden — man kan ikke ringe til et abonnement, men man kan
godt se på et.

TO FORSKELLIGE MÅNEDER, og forvekslingen er den farligste fejl i filen:

    abo_maaned  INDEVÆRENDE måned. Hvilke abonnementer der er aktive lige nu
                (PRD §4, filter 1). Et abonnement der churnede den 1. skal ikke
                på listen — der er ingen at ringe til.
    reference   Sidste HELE måned. Hvilken måned zonen beregnes i. Indeværende
                måned er levende og ville få enhver kunde til at se ud som et
                frit fald.

Et abonnement startet i indeværende måned har derfor en foerste_maaned der
ligger EFTER reference. Det negative tal fanges af sin egen spærring i
zones.bestem_zone, før den forbrugsbaserede datering — se kommentaren dér."""

import logging
from datetime import date

from .queries import abonnementer_med_ejer
from .usage import (
    _account_to_customer_map,
    forbrug_pr_abonnement,
    latest_complete_month,
    serie_og_dage,
)
from .zones import (
    STOPPET_VINDUE,
    VANEBRUGER_VINDUE,
    ZONE_LABELS,
    ZONE_ORDER,
    bestem_zone,
    er_vanebruger,
    foregaaende_maaneder,
    kanonisk_site,
    maaneders_alder,
    zone_vaegt,
)

logger = logging.getLogger(__name__)

# PRD §2 og §10, regel 2: kunder under denne grænse får aldrig et opkald.
# Grænsen ligger på KUNDEN, ikke på abonnementet — en kunde med fem sites à
# 2.000 kr. er ikke en mikrokunde. De vises alligevel på §7.2, men markeret, så
# zonekortenes tal summerer til den virkelige portefølje.
MIKROKUNDE_ARR = 5000.0

# PRD §4. Faktoren kræver en fornyelsesdato, som ikke findes i nogen kilde vi
# har adgang til (PRD §11, punkt 1 og 8). Indtil da er den 1,0 for alle, og
# score er reelt ARR × risikovægt. Konstanten står her frem for at være udeladt,
# så det fremgår at den mangler og ikke er glemt.
TIMINGFAKTOR = 1.0


def abonnementer_i_risiko(owner_name: str | None = None,
                          teams: list | None = None,
                          abo_maaned: str | None = None) -> dict:
    """Aktive abonnementer med zone, vægt og score.

    Returnerer:
        rows        én pr. abonnement, højeste score først
        zones       pr. zone: antal abonnementer, antal kunder, ARR
        zone_order  visningsrækkefølge, importeret fra zones.py
        meta        datafriskhed, dækning og hvad der ikke kunne kobles

    `abo_maaned` findes for at kunne genskabe en verificeret måned i en
    kontrolkørsel. Routeren sender den aldrig — produktionsvisningen skal altid
    være indeværende måned.

    Mangler forbrugsfilen, får ALLE abonnementer zonen "intet_signal" og
    meta["usage_error"] sættes — ikke en tom liste. En tom liste ville læses som
    "ingen risiko", hvilket er den farligste fejlvisning siden kan lave.
    """
    forbrug: dict = {}
    med_zuora: set = set()
    reference: str | None = None
    usage_meta: dict = {}
    usage_error: str | None = None
    try:
        forbrug = forbrug_pr_abonnement()
        reference = latest_complete_month(forbrug["maaneder"])
        # Samme opslag som forbrug_pr_abonnement bruger internt, men vi har brug
        # for det HER: bestem_zone skal kunne skelne "kan ikke oversættes" fra
        # "læser ikke", altså intet_signal fra aldrig_i_brug.
        med_zuora = set(_account_to_customer_map().values())
        usage_meta = forbrug["meta"]
    except FileNotFoundError as e:
        usage_error = str(e)
        logger.warning("Forbrugsfilen mangler — alle abonnementer uden signal: %s", e)
    except Exception as e:
        usage_error = f"{type(e).__name__}: {e}"
        logger.exception("Forbrugsdata kunne ikke læses")

    abo_maaned = abo_maaned or date.today().strftime("%Y-%m")
    abonnementer = abonnementer_med_ejer(abo_maaned, owner_name=owner_name,
                                         teams=teams)

    rows = []
    for a in abonnementer:
        kunde = (a["account"], a["org_id"])
        site = kanonisk_site(a["sites"])

        if reference is None:
            zone, serie, snit = "intet_signal", {}, None
            dage_serie, vanebruger, dage_12m = {}, True, None
        else:
            # Pakke- kontra site-niveau afgøres ét sted, usage.serie_og_dage,
            # så kunde-detaljesiden (PRD §7.4) tegner præcis den serie zonen
            # blev beregnet på.
            serie, dage_serie = serie_og_dage(forbrug, kunde, site)
            zone = bestem_zone(serie, reference, a["foerste_maaned"], site,
                               kunde in med_zuora)
            vanebruger = er_vanebruger(dage_serie, reference)
            dage_12m = sum(dage_serie.get(m, 0)
                           for m in foregaaende_maaneder(reference,
                                                         VANEBRUGER_VINDUE))
            # Samme tre måneder som bestem_zone selv sammenligner mod. Tallet
            # med i rækken, så tabellen kan vise HVORFOR noget er "faldende" i
            # stedet for blot at hævde det.
            tidligere = [serie.get(m, 0)
                         for m in foregaaende_maaneder(reference, STOPPET_VINDUE)]
            snit = sum(tidligere) / len(tidligere)

        # Én gang, ikke to: vægten afhænger nu af vanebruger, og to kald med
        # forskellige argumenter ville give en score der ikke matcher kolonnen.
        vaegt = zone_vaegt(zone, vanebruger)

        arr = (float(a["arr_pr_abonnement"])
               if a["arr_pr_abonnement"] is not None else None)
        kunde_arr = (float(a["kunde_arr_dkk"])
                     if a["kunde_arr_dkk"] is not None else None)

        rows.append({
            "account":         a["account"],
            "org_id":          a["org_id"],
            "org_name":        a["org_name"],
            "site":            a["sites"],
            "site_kanonisk":   site,
            "foerste_maaned":  a["foerste_maaned"],
            "owner_name":      a["owner_name"],
            "teams":           a["teams"],
            "zone":            zone,
            "zone_label":      ZONE_LABELS[zone],
            "vaegt":           vaegt,
            # PRD §3's skel mellem "stoppet vanebruger" og en onboarding-sag.
            # Dagene med i rækken, så tabellen kan vise "56 aktive dage" ved
            # siden af zonen — det er den oplysning der afgør om opkaldet er
            # værd at tage.
            "vanebruger":      vanebruger,
            "aktive_dage_12m": dage_12m,
            "arr_dkk":         arr,
            # 'site' = abonnementets eget beløb fra ACV. 'lige_deling' = kundens
            # ARR delt med antal sites, som er et VALG og ikke en måling. None =
            # ukendt, og de tre skal kunne skelnes på skærmen.
            "arr_kilde":       a.get("arr_kilde"),
            "kunde_arr_dkk":   kunde_arr,
            "sites_i_alt":     a["sites_i_alt"],
            # None og ikke 0 når ARR er ukendt: 0 betyder "ingen risiko", og de
            # to tilstande skal kunne skelnes i UI'et.
            "score":           (arr * vaegt * TIMINGFAKTOR
                                if arr is not None else None),
            "mikrokunde":      kunde_arr is not None and kunde_arr < MIKROKUNDE_ARR,
            "pv_reference":    serie.get(reference, 0) if reference else None,
            "pv_snit_3":       snit,
        })

    # PRD §4: score faldende, tie-breaker ARR faldende. Første element i nøglen
    # er et sandhedsværdi-flag, fordi None ikke kan negeres — abonnementer uden
    # ARR skal ligge sidst, og de er ikke risikofrie, de er uopgjorte.
    rows.sort(key=lambda r: (r["score"] is None,
                             -(r["score"] or 0),
                             -(r["kunde_arr_dkk"] or 0)))

    # zone_vaegt(z) uden andet argument giver zonens NOMINELLE vægt. Kortet viser
    # altså 1,00 for "stoppet", mens de enkelte rækker kan have 0,50 hvis der
    # ikke var en vane at miste. Det er bevidst: kortet beskriver zonen, rækken
    # beskriver abonnementet.
    zones = {z: {"label": ZONE_LABELS[z], "vaegt": zone_vaegt(z),
                 "abonnementer": 0, "kunder": 0, "arr_dkk": 0.0,
                 "uden_arr": 0, "mikrokunder": 0}
             for z in ZONE_ORDER}
    # Kunder pr. zone som MÆNGDER: samme kunde kan ligge i tre zoner med tre
    # sites. Summen af zonernes kundetal er derfor større end meta["kunder"], og
    # det er ikke en fejl — det er præcis den forskel PRD §7.2 vil vise, hvor
    # abonnementer er "hvad der står på spil" og kunder er kapaciteten.
    kunder_pr_zone: dict = {}
    for r in rows:
        b = zones[r["zone"]]
        b["abonnementer"] += 1
        kunder_pr_zone.setdefault(r["zone"], set()).add((r["account"], r["org_id"]))
        if r["arr_dkk"] is None:
            b["uden_arr"] += 1
        else:
            b["arr_dkk"] += r["arr_dkk"]
        if r["mikrokunde"]:
            b["mikrokunder"] += 1
    for z, b in zones.items():
        b["kunder"] = len(kunder_pr_zone.get(z, set()))

    # Signalets alder MÅLES I MÅNEDER, ikke i dage. Et månedligt signal rådner
    # ikke inden for måneden — det var hele grunden til at forlade recency. Men
    # er referencen to måneder gammel, er eksporten ikke kørt, og zonerne
    # beskriver en tilstand der kan være overstået. 0 = referencen er sidste hele
    # måned, altså så friskt som modellen kan blive.
    ref_alder = None
    if reference:
        ref_alder = maaneders_alder(reference, abo_maaned) - 1

    arr_total = sum(r["arr_dkk"] for r in rows if r["arr_dkk"] is not None)
    # "ARR i risiko" er kroner i zoner med vægt over nul. Sund og ny bidrager
    # altså ikke, mens intet_signal gør — et datahul på en stor kunde er ikke
    # risikofrit, det er uoplyst (PRD §3).
    arr_i_risiko = sum(r["arr_dkk"] for r in rows
                       if r["arr_dkk"] is not None and r["vaegt"] > 0)

    meta = {
        "abonnementer":     len(rows),
        "kunder":           len({(r["account"], r["org_id"]) for r in rows}),
        "abo_maaned":       abo_maaned,
        "reference_maaned": reference,
        "reference_alder":  ref_alder,
        "arr_total":        arr_total,
        "arr_i_risiko":     arr_i_risiko,
        "med_signal":       sum(1 for r in rows if r["zone"] != "intet_signal"),
        "uden_arr":         sum(1 for r in rows if r["arr_dkk"] is None),
        "uden_ejer":        sum(1 for r in rows if not r["owner_name"]),
        "uden_team":        sum(1 for r in rows
                                if r["owner_name"] and not r["teams"]),
        # Hvor mange af de stoppede der havde en vane at miste. Tallet hører i
        # forbeholdene: det er selve begrundelsen for at 565 af dem har halv
        # vægt, og uden det ser vægtforskellen vilkårlig ud.
        "stoppet_vanebrugere": sum(1 for r in rows
                                   if r["zone"] == "stoppet" and r["vanebruger"]),
        "stoppet_uden_vane":   sum(1 for r in rows
                                   if r["zone"] == "stoppet" and not r["vanebruger"]),
        "mikrokunder":      sum(1 for r in rows if r["mikrokunde"]),
        "usage_export_date": usage_meta.get("export_date"),
        "usage_filename":    usage_meta.get("filename"),
        "usage_error":       usage_error,
        # Tærsklerne i zones.py er gæt, ikke måleresultater. PRD §9:
        # forudsigelsesraten kalibrerer dem efter 6 måneders udfaldsdata. Så
        # længe dette er False, skal siden sige det.
        "thresholds_validated": False,
    }
    return {"rows": rows, "zones": zones, "zone_order": ZONE_ORDER, "meta": meta}

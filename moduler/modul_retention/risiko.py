"""Churn-risiko pr. abonnement: zone, vægt og score.

Erstatter recency-modellen i risk.py, jf. Zonemodellen hvor 14/30-dages-modellen
udfases. Ligger som eget modul, fordi begge modeller skal kunne køre side om
side indtil siden er lagt om — de har hver sit ZONE_ORDER og ZONE_LABELS, og et
alias-import ville skjule hvilken model man læser.

Måleenheden er abonnementet (account, org_id, site), jf. Definitioner, og
tabellen på Churn-risiko er derfor pr. abonnement. Foldningen til én række pr.
kunde hører til Prioriteringsmodellen og Dagens opkald: man kan ikke ringe til
et abonnement, men man kan godt se på et.

TO FORSKELLIGE MÅNEDER, og forvekslingen er den farligste fejl i filen:

    abo_maaned  INDEVÆRENDE måned. Hvilke abonnementer der er aktive lige nu
                (Prioriteringsmodellen, filter 1). Et abonnement der
                churnede den 1. skal ikke på listen, der er ingen at ringe
                til.
    reference   Sidste HELE måned. Hvilken måned zonen beregnes i. Indeværende
                måned er levende og ville få enhver kunde til at se ud som et
                frit fald.

Et abonnement startet i indeværende måned har derfor en foerste_maaned der
ligger EFTER reference. Det negative tal fanges af sin egen spærring i
zones.bestem_zone, før den forbrugsbaserede datering — se kommentaren dér."""

import logging
from datetime import date

from .kontrakt import dage_til, fornyelsesdatoer
from .queries import UDENLANDSKE_ACCOUNTS, abonnementer_med_ejer, db_opsigelser
from .usage import (
    forbrug_pr_abonnement,
    latest_complete_month,
    serie_og_dage,
)
from .zones import (
    STOPPET_VINDUE,
    VANEBRUGER_VINDUE,
    GRUPPE_HINT,
    GRUPPE_LABELS,
    GRUPPE_ORDER,
    ZONE_LABELS,
    ZONE_ORDER,
    bestem_zone,
    er_vanebruger,
    foregaaende_maaneder,
    kanonisk_site,
    maaneders_alder,
    zone_gruppe,
    zone_vaegt,
)

logger = logging.getLogger(__name__)

# Definitioner og Regler og Guardrails, regel 2: kunder under denne grænse får
# aldrig et opkald. Grænsen ligger på KUNDEN, ikke på abonnementet — en kunde
# med fem sites à 2.000 kr. er ikke en mikrokunde. De vises alligevel på
# Churn-risiko, men markeret, så zonekortenes tal summerer til den virkelige
# portefølje.
MIKROKUNDE_ARR = 5000.0

# Prioriteringsmodellen. FORNYELSESDATOEN FINDES NU, se kontrakt.py, men
# faktoren staar fortsat paa 1,0, og de tre grunde staar dér: den kan ikke
# valideres bagud (Zuora saetter baade betalt_til og auto_fornyes ved ophoer),
# monitor og marketwire har 0 % daekning, og en absolut dagsgraense giver en
# saesonboelge fordi 64,5 % af kunderne med en dato fornyes inden 60 dage.
# Datoen er derfor en kolonne og en tie-breaker, ikke en multiplikator.
# Konstanten staar her frem for at vaere udeladt, saa det fremgaar at den
# mangler og ikke er glemt.
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
    uden_aktiv_konto: set = set()
    reference: str | None = None
    usage_meta: dict = {}
    usage_error: str | None = None
    try:
        forbrug = forbrug_pr_abonnement()
        reference = latest_complete_month(forbrug["maaneder"])
        # bestem_zone skal kunne skelne "kan ikke oversaettes" fra "laeser
        # ikke", altsaa intet_signal fra aldrig_i_brug. Saettet tages FRA
        # forbrug og ikke fra et nyt opslag, saa de to ikke kan drive fra
        # hinanden. NAVNET er historisk: kilden er dm_kobling plus
        # ACV_snapshot, ikke Zuora alene.
        med_zuora = forbrug["koblingsbare"]
        uden_aktiv_konto = forbrug["uden_aktiv_konto"]
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

    # OPSAGT ER IKKE EN RISIKO, DET ER ET FAKTUM. Et abonnement med en gaeldende
    # opsigelse kan ikke reddes af det opkald listen rangerer efter, saa det maa
    # ikke konkurrere med de oevrige om pladserne. Raekken fjernes dog IKKE:
    # kundesiden skal kunne vise "opsagt, ophoerer 31-10-2026", og zonekortene
    # skal fortsat summere til den virkelige portefoelje.
    #
    # Maalt 2026-08-19 paa 15.191 abonnementer: 280 har en gaeldende opsigelse,
    # fordelt paa 9 forfaldne (alle marketwire, aeldste 2023-03-04), 62 ophoert
    # i denne maaned og 209 i opsigelse. 198 af dem havde en score, som nu
    # bliver nul, og arr_i_risiko falder 1.826.871 kr. De resterende 82 havde
    # allerede vaegt nul, heraf 77 i zonen "sund".
    #
    # DE STOERSTE ER NETOP "sund". Energinet EnergiWatch DK til 260.356 kr.
    # ophoerer 29-09-2026 og laeser normalt indtil da. Forbrug forudsiger ikke
    # opsigelse, og derfor kan zonemodellen alene ikke finde disse opkald.
    # Eget try: mangler abonnementsfilen, skal siden stadig virke uden
    # fornyelsesdatoer. En kontraktoplysning maa ikke kunne vaelte risikolisten.
    fornyelser: dict = {}
    kontrakt_meta: dict = {}
    try:
        _k = fornyelsesdatoer()
        fornyelser = _k["pr_kunde"]
        kontrakt_meta = _k["meta"]
    except Exception as e:
        logger.warning("Fornyelsesdatoer kunne ikke laeses: %s", e)

    opsigelser = db_opsigelser()
    # Datoen holdes som TEKST i opslagets eget format ('YYYY-MM-DD'), saa
    # sammenligningen nedenfor er en strengsammenligning. Samme valg som resten
    # af modulet, hvor maaneder ogsaa sammenlignes som tekst.
    i_dag = date.today().isoformat()

    rows = []
    for a in abonnementer:
        kunde = (a["account"], a["org_id"])
        site = kanonisk_site(a["sites"])

        if reference is None:
            zone, serie, snit = "intet_signal", {}, None
            dage_serie, vanebruger, dage_12m = {}, True, None
        else:
            # Pakke- kontra site-niveau afgøres ét sted, usage.serie_og_dage,
            # så kunde-detaljesiden (Kundeside) tegner præcis den serie zonen
            # blev beregnet på.
            serie, dage_serie = serie_og_dage(forbrug, kunde, site)
            zone = bestem_zone(serie, reference, a["foerste_maaned"], site,
                               kunde in med_zuora,
                               kunde not in uden_aktiv_konto)
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
        # Ophoersdatoen kan ligge i fremtiden (varslet loeber) eller i fortiden
        # (aftalen er slut). Begge skal ud af scoren, og vaegten saettes derfor
        # FOER score regnes nedenfor. Nul og ikke None: beloebet er kendt, det er
        # risikoen der er nul.
        opsagt_dato = opsigelser.get((a["account"], a["org_id"], a["sites"]))
        if opsagt_dato:
            vaegt = 0.0

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
            # Kunden kan KUN kobles gennem ophoerte Zuora-konti: Pipedrive siger
            # aktiv, Zuora siger ophoert. Zonen staar uroert indtil gruppen faar
            # sin egen tilstand, men flaget skal med nu, saa zone-maalingen kan
            # skaere gruppen ud af sit grundlag. Uden det maaler aldrig_i_brug
            # "kontoen er ophoert" og ikke "kunden laeser ikke".
            "uden_aktiv_konto": kunde in uden_aktiv_konto,
            "vaegt":           vaegt,
            # Datoen med i raekken, saa UI'et kan skrive den. `opsagt` er sandt
            # naar aftalen allerede er slut, og falsk mens varslet loeber. Den
            # forskel afgoer om der er noget at ringe om.
            "opsagt_dato":     opsagt_dato,
            "opsagt":          bool(opsagt_dato) and opsagt_dato <= i_dag,
            # Zonemodellens skel mellem "stoppet vanebruger" og en onboarding-sag.
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
            # Naeste fornyelse er en KONTRAKTOPLYSNING og ikke et signal.
            # Den ganges bevidst IKKE paa scoren: se kontrakt.py's tre grunde,
            # hvoraf den vaegtigste er at monitor og marketwire faar 0 %
            # daekning og derfor ville blive skubbet ned af en datagrund.
            # Datoen ligger paa KUNDEN, saa alle kundens sites deler den.
            "fornyelse_dato":  fornyelser.get(kunde),
            "fornyelse_dage":  dage_til(fornyelser.get(kunde)),
            "mikrokunde":      kunde_arr is not None and kunde_arr < MIKROKUNDE_ARR,
            "pv_reference":    serie.get(reference, 0) if reference else None,
            "pv_snit_3":       snit,
        })

    # Prioriteringsmodellen: score faldende, saa naermeste fornyelse, saa ARR
    # faldende. Første element i nøglen er et sandhedsværdi-flag, fordi None
    # ikke kan negeres — abonnementer uden ARR skal ligge sidst, og de er ikke
    # risikofrie, de er uopgjorte.
    #
    # FORNYELSEN ER EN TIE-BREAKER OG IKKE EN FAKTOR. Den flytter kun raekker
    # der ellers stod lige, og dér gør den mest gavn: de 3.012 abonnementer
    # uden ARR har alle score None og laa foer i vilkaarlig orden.
    # Manglende dato sorteres SIDST og ikke foerst, saa en kunde uden
    # kontraktoplysning ikke kan overhale en med.
    rows.sort(key=lambda r: (r["score"] is None,
                             -(r["score"] or 0),
                             r["fornyelse_dage"] if r["fornyelse_dage"]
                             is not None else 10**6,
                             -(r["kunde_arr_dkk"] or 0)))

    # zone_vaegt(z) uden andet argument giver zonens NOMINELLE vægt. Kortet viser
    # altså 1,00 for "stoppet", mens de enkelte rækker kan have 0,50 hvis der
    # ikke var en vane at miste. Det er bevidst: kortet beskriver zonen, rækken
    # beskriver abonnementet.
    zones = {z: {"label": ZONE_LABELS[z], "vaegt": zone_vaegt(z),
                 "gruppe": zone_gruppe(z),
                 "abonnementer": 0, "kunder": 0, "arr_dkk": 0.0,
                 "uden_arr": 0, "mikrokunder": 0}
             for z in ZONE_ORDER}
    # Kunder pr. zone som MÆNGDER: samme kunde kan ligge i tre zoner med tre
    # sites. Summen af zonernes kundetal er derfor større end meta["kunder"], og
    # det er ikke en fejl — det er præcis den forskel Churn-risiko vil vise, hvor
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
    # risikofrit, det er uoplyst (Zonemodellen).
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
        "uden_aktiv_konto": sum(1 for r in rows if r["uden_aktiv_konto"]),
        "usage_export_date": usage_meta.get("export_date"),
        # Tre tal og ikke ét: "i opsigelse" er stadig et opkald vaerd, "opsagte"
        # er en konstatering, og "forfaldne" er datahygiejne - de burde vaere
        # lukket i dbo.retention, og alle nu er marketwires 'Opsigelser'.
        "opsagte":          sum(1 for r in rows if r["opsagt"]),
        "i_opsigelse":      sum(1 for r in rows
                                if r["opsagt_dato"] and not r["opsagt"]),
        "opsagte_forfaldne": sum(1 for r in rows if r["opsagt_dato"]
                                 and r["opsagt_dato"] < abo_maaned + "-01"),
        "usage_filename":    usage_meta.get("filename"),
        "kontrakt_filename": kontrakt_meta.get("filename")
                             or kontrakt_meta.get("kontrakt_filename"),
        # Daekningen skal paa siden: 0 % for monitor og marketwire er den
        # grund faktoren staar paa 1,0, og det skal kunne ses.
        "med_fornyelse":     sum(1 for r in rows if r["fornyelse_dato"]),
        "fornyelse_60_dage": sum(1 for r in rows
                                 if r["fornyelse_dage"] is not None
                                 and 0 <= r["fornyelse_dage"] <= 60),
        "usage_error":       usage_error,
        # Tærsklerne i zones.py er gæt, ikke måleresultater. Målingside:
        # forudsigelsesraten kalibrerer dem efter 6 måneders udfaldsdata. Så
        # længe dette er False, skal siden sige det.
        "thresholds_validated": False,
        # Fra data og ikke tastet ind i skabelonen, så en fremtidig ændring af
        # UDENLANDSKE_ACCOUNTS ikke kræver at nogen husker at rette teksten to
        # steder. Listen er altid tom i rækkerne (SQL-filteret i queries.py),
        # dette er kun til forbeholdsteksten.
        "udenlandske_udeladt": list(UDENLANDSKE_ACCOUNTS),
    }
    # Grupperne sendes med som liste og ikke som tre dicts: skabelonen skal
    # tegne dem i raekkefoelge, og en dict har ingen.
    grupper = [{"id": g, "label": GRUPPE_LABELS[g], "hint": GRUPPE_HINT[g]}
               for g in GRUPPE_ORDER]
    return {"rows": rows, "zones": zones, "zone_order": ZONE_ORDER,
            "gruppe_order": grupper, "meta": meta}

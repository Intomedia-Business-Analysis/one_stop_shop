"""Kalibrering af zonevægtene mod FAKTISK churn. Modulets vigtigste
ubesvarede spørgsmål.

    .venv/Scripts/python.exe moduler/modul_retention/kalibrering.py

Spørgsmålet er ikke "hvem er i risiko" men "forudsiger zonen noget". Metoden
bygger paa det eneste vi har: `dbo.retention` ved hvem der forsvandt, måned
for måned. For hver
referencemåned R sammenlignes zonefordelingen for de abonnementer, der var væk H
måneder senere, med fordelingen for dem der stadig var der. Overlapper de to
helt, forudsiger zonen ingenting, og de syv vægte er postulater.

Udbyttet er en MÅLT vægtvektor: churn-raten pr. zone, normaliseret så den
højeste er 1,00. Den kan holdes op mod zones.ZONE_VAEGT, som i dag er syv skøn.

FIRE TING DER SKAL VÆRE RIGTIGE, ellers måler filen sig selv:

1. INGEN FREMADKIGNING. `bestem_zone` er næsten rent bagudskuende, men to steder
   ser den på hele serien: `any(v > 0 for v in forbrug.values())` skiller
   `laenge_tavs` fra `aldrig_i_brug`, og `foerste_kendte_maaned` tager `min` over
   alle måneder. I produktion er det harmløst, fordi der ikke FINDES måneder
   efter referencen. Her ville det lade zonen vide, hvad kunden gjorde bagefter.
   Serien beskæres derfor til måneder <= R, før den sendes ind. Det er også
   grunden til at filen ikke kalder `abonnementer_i_risiko(abo_maaned=R)`: den
   sætter `reference` til seneste hele måned uanset `abo_maaned`, så man ville få
   R's abonnementer med nutidens zoner.

2. FLERE HORISONTER. Et abonnement, hvor kunden holdt op med at læse i marts, kan
   løbe kontraktuelt til årsskiftet. Måles kun R → R+1, bedømmes modellen på en
   forudsigelse, den ikke er bygget til at lave. Filen måler derfor 1, 3 og 6
   måneder. Vinduet koster måneder: H=6 kræver at R+6 er en hel måned, så den
   horisont har færrest referencer bag sig. Antallet står i tabellen.

3. FORSVUNDET ER IKKE OPSAGT. Mellem april og maj 2026 blev
   1.769 abonnementer GENSKABT efter at have været væk. Et abonnement kan altså
   forsvinde og komme tilbage. Kohorten afgøres derfor på tilstanden i R+H — er
   den tilbage dér, tælles den som blevet — og filen tæller særskilt, hvor mange
   der FLIMREDE undervejs. Er det tal stort, er rækkerne støj og ikke kontrakter.

4. `intet_signal` ER IKKE EN FORUDSIGELSE. Zonen har vægt 0,15 og tæller derfor
   med i "i risiko", men den betyder "vi kan ikke se noget". Regnes den med, ser
   modellen skarp ud, fordi den flager alt den er blind for. Forudsigelsesraten
   vises derfor med OG uden, og zonen splittes på sin årsag: en række uden
   Zuora-kobling er en datamangel, et utrackbart site er et kildehul. Skiller
   churn'en sig mellem de to, er "intet signal" et hygiejneproblem.

Filen skriver ikke til databasen og har ingen bivirkninger.
"""
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from moduler.modul_retention.queries import db_abonnementer          # noqa: E402
from moduler.modul_retention.usage import (                          # noqa: E402
    customer_key, forbrug_pr_abonnement, serie_og_dage)
from moduler.modul_retention.zones import (                          # noqa: E402
    NY_MAANEDER, STOPPET_VINDUE, UNTRACKBARE_SITES, ZONE_LABELS, ZONE_ORDER,
    ZONE_VAEGT, bestem_zone, er_vanebruger, forskyd_maaned, kanonisk_site,
    zone_vaegt)

# Hvor meget historik en referencemåned skal have bag sig i forbrugsvinduet, før
# zonen betyder noget. Uden dette ville de tidligste måneder få alt til at se
# `ny` ud — ikke fordi abonnementerne er nye, men fordi serien er beskåret.
MIN_HISTORIK = max(NY_MAANEDER, STOPPET_VINDUE) + 1

HORISONTER = (1, 3, 6)


def beskaer(serie: dict, reference: str) -> dict:
    """Serien som den så ud i `reference`. Se punkt 1 i modul-docstringen."""
    return {m: v for m, v in serie.items() if m <= reference}


def abo_noegle(r: dict) -> tuple:
    """Abonnementets grain, Definitioner: (account, org_id, sites).

    GENNEM `customer_key`, ikke råt fra rækken. `db_abonnementer` er en SQL-query
    og leverer `org_id` som INT (982), mens hvert forbrugsopslag i usage.py er
    nøglet med den kanoniske streng ('982'). Nøgles de hver for sig, rammer
    `kunde in med_zuora` ALDRIG, `er_trackbare` svarer nej, og hele målingen
    kommer ud som 100% "intet signal" i samtlige måneder — hvilket den gjorde,
    første gang filen blev kørt. Fejlen kaster ikke, den svarer bare forkert.

    `sites` kan være None for marketwire, og None er en gyldig del af nøglen her
    — i modsætning til et databaseopslag, hvor NULL = NULL er ukendt.
    """
    return customer_key(r["account"], r["org_id"]) + (r["sites"],)


def hul_aarsag(site, har_zuora: bool) -> str:
    """Hvorfor et abonnement er blindt. Se punkt 4.

    Rækkefølgen er ikke vilkårlig: mangler Zuora-koblingen, kan vi ikke engang
    slå kunden op, og så er sitets trackbarhed uden betydning.

    FLAG 2026-08-25: "utrackbart site" bæres af Shifter/Kom24 NO/Medier24 NO,
    som alle lå under watch_no. Kunder herfra findes ikke længere i
    db_abonnementer efter queries._KUN_DANSKE, så bøtten skrumper til
    marketwires rækker. Se samme note i kohortemaaling.py.
    """
    if not har_zuora:
        return "ingen Zuora-kobling"
    if site in UNTRACKBARE_SITES:
        return "utrackbart site"
    return "(ikke blind)"


def hent_maaneder(maaneder: list) -> dict:
    """{måned: {nøgle: række}}. Én query pr. måned, ca. 2 s stykket."""
    ud = {}
    for m in maaneder:
        ud[m] = {abo_noegle(r): r for r in db_abonnementer(m)}
        print(f"  {m}: {len(ud[m]):>6,} abonnementer")
    return ud


def zone_for(r: dict, kunde: tuple, reference: str, forbrug: dict,
             med_zuora: set, uden_aktiv_konto: set = frozenset()) -> tuple:
    """(zone, vægt, hul-årsag) for ét abonnement i én referencemåned.

    `uden_aktiv_konto` maa KUN sendes med naar referencen er en LEVENDE maaned.
    Saettet bygges paa konto_status, som beskriver tilstanden i dag, saa paa en
    bagdateret kohorte er det et input dateret EFTER udfaldet. Default er
    derfor et tomt saet, og kohortemaaling.py sender det ikke med. Se regel 5 i
    kohortemaaling.py, hvor flaget i stedet maales for sig selv og staar
    maerket som laekket.
    """
    site = kanonisk_site(r["sites"])
    serie, dage = serie_og_dage(forbrug, kunde, site)
    # BESKÆRINGEN. Uden de to linjer måler filen sig selv.
    serie, dage = beskaer(serie, reference), beskaer(dage, reference)

    har_zuora = kunde in med_zuora
    zone = bestem_zone(serie, reference, r["foerste_maaned"], site, har_zuora,
                       kunde not in uden_aktiv_konto)
    # Vægten afhænger af vanen for "stoppet", præcis som i risiko.py — ellers
    # ville den målte vektor sammenlignes med en vægt, ingen række havde.
    vaegt = zone_vaegt(zone, er_vanebruger(dage, reference))
    return zone, vaegt, hul_aarsag(site, har_zuora)


def maal(reference: str, horisont: int, pr_maaned: dict, forbrug: dict,
         med_zuora: set, uden_aktiv_konto: set = frozenset()) -> dict:
    """Zonefordeling for dem der var VÆK i R+H mod dem der stadig var der."""
    slut = forskyd_maaned(reference, -horisont)
    mellem = [forskyd_maaned(reference, -n) for n in range(1, horisont)]

    i_r = pr_maaned[reference]
    i_slut = set(pr_maaned[slut])

    blev: dict = defaultdict(int)
    forsvandt: dict = defaultdict(int)
    huller: dict = defaultdict(lambda: defaultdict(int))
    flimrede = 0

    for noegle, r in i_r.items():
        # Nøglens to første led ER kundenøglen, allerede kanonisk. Bygges den om
        # fra rækken her, er int/str-fælden tilbage. Se abo_noegle.
        kunde = noegle[:2]
        zone, _vaegt, aarsag = zone_for(r, kunde, reference, forbrug, med_zuora)

        var_vaek_undervejs = any(noegle not in pr_maaned[m] for m in mellem
                                 if m in pr_maaned)
        if noegle in i_slut:
            blev[zone] += 1
            blev["_n"] += 1
            if var_vaek_undervejs:
                flimrede += 1
        else:
            forsvandt[zone] += 1
            forsvandt["_n"] += 1
            if zone == "intet_signal":
                huller["forsvandt"][aarsag] += 1
        if noegle in i_slut and zone == "intet_signal":
            huller["blev"][aarsag] += 1

    return {"reference": reference, "horisont": horisont, "slut": slut,
            "blev": blev, "forsvandt": forsvandt, "huller": huller,
            "flimrede": flimrede, "i_alt": len(i_r)}


def _andel(n: int, af: int) -> float:
    return 100.0 * n / af if af else 0.0


def laeg_sammen(maalinger: list) -> dict:
    """Summér flere referencemåneder til én fordeling."""
    blev: dict = defaultdict(int)
    forsvandt: dict = defaultdict(int)
    huller: dict = defaultdict(lambda: defaultdict(int))
    flimrede = 0
    for m in maalinger:
        for z, n in m["blev"].items():
            blev[z] += n
        for z, n in m["forsvandt"].items():
            forsvandt[z] += n
        for gruppe, d in m["huller"].items():
            for aarsag, n in d.items():
                huller[gruppe][aarsag] += n
        flimrede += m["flimrede"]
    return {"blev": blev, "forsvandt": forsvandt, "huller": huller,
            "flimrede": flimrede}


def skriv_horisont(horisont: int, maalinger: list) -> None:
    s = laeg_sammen(maalinger)
    blev, forsvandt = s["blev"], s["forsvandt"]
    n_b, n_f = blev["_n"], forsvandt["_n"]
    refs = ", ".join(m["reference"] for m in maalinger)

    print(f"\n\n{'=' * 72}")
    print(f"HORISONT {horisont} MAANED{'ER' if horisont > 1 else ''}"
          f"   ({len(maalinger)} referencemaaneder: {refs})")
    print(f"{'=' * 72}")
    print(f"  abonnement-maaneder i alt: {n_b + n_f:>8,}")
    print(f"  var der stadig i R+{horisont}:      {n_b:>8,}"
          f"  ({_andel(n_b, n_b + n_f):.1f}%)")
    print(f"  var VAEK i R+{horisont}:            {n_f:>8,}"
          f"  ({_andel(n_f, n_b + n_f):.1f}%)")
    if not n_f:
        print("  ingen forsvandt — intet at maale")
        return

    print(f"\n  {'zone'.ljust(14)} {'blev':>9} {'andel':>7} "
          f"{'forsv.':>7} {'andel':>7} {'lift':>6} {'churn-rate':>11}")
    for z in ZONE_ORDER:
        b, f = blev[z], forsvandt[z]
        if not (b or f):
            continue
        a_b, a_f = _andel(b, n_b), _andel(f, n_f)
        # Lift over 1: zonen er OVERrepraesenteret blandt de forsvundne. Er alle
        # syv taet paa 1, skiller zonen ikke abonnementerne.
        lift = a_f / a_b if a_b else float("inf")
        print(f"  {ZONE_LABELS[z].ljust(14)} {b:>9,} {a_b:>6.1f}% "
              f"{f:>7,} {a_f:>6.1f}% {lift:>6.2f} {_andel(f, b + f):>10.2f}%")

    # Målingsidens forudsigelsesrate, med og uden datahullet. Se punkt 4.
    risiko = [z for z in ZONE_ORDER if ZONE_VAEGT.get(z, 0) > 0]
    for navn, zoner in (("MED intet_signal ", risiko),
                        ("UDEN intet_signal", [z for z in risiko
                                               if z != "intet_signal"])):
        r_f = _andel(sum(forsvandt[z] for z in zoner), n_f)
        r_b = _andel(sum(blev[z] for z in zoner), n_b)
        print(f"\n  forudsigelsesrate {navn}: {r_f:5.1f}% af de forsvundne"
              f" stod i en risikozone")
        print(f"    samme tal for dem der blev:      {r_b:5.1f}%"
              f"   -> forskel {r_f - r_b:+.1f} procentpoint")

    # Hvorfor er de blinde? Skiller churn'en sig mellem de to aarsager, er
    # "intet signal" et hygiejneproblem og ikke et risikosignal.
    h = s["huller"]
    if h["forsvandt"] or h["blev"]:
        print(f"\n  «intet signal» splittet paa aarsag:")
        print(f"    {'aarsag'.ljust(22)} {'blev':>8} {'forsv.':>8} {'churn-rate':>11}")
        for aarsag in sorted(set(h["blev"]) | set(h["forsvandt"])):
            b, f = h["blev"][aarsag], h["forsvandt"][aarsag]
            print(f"    {aarsag.ljust(22)} {b:>8,} {f:>8,} "
                  f"{_andel(f, b + f):>10.2f}%")

    if s["flimrede"]:
        print(f"\n  {s['flimrede']:,} af dem der «blev» var VAEK i en maaned"
              f" undervejs og kom tilbage.")
        print("    Raekkerne flimrer, saa et fravaer i én maaned er ikke i sig"
              " selv en opsigelse.")


def skriv_vaegtvektor(horisont: int, maalinger: list) -> None:
    """Churn-rate pr. zone som vægtvektor, holdt op mod dagens skøn.

    Rå churn-rate og ikke en model: vægten i Prioriteringsmodellen ganges på
    ARR, så det den skal udtrykke er "hvor sandsynligt er det, at dette
    abonnement forsvinder". Normaliseret så den højeste zone er 1,00, fordi
    ZONE_VAEGT er skaleret sådan — ellers kan de to kolonner ikke sammenlignes.
    """
    s = laeg_sammen(maalinger)
    rater = {}
    for z in ZONE_ORDER:
        n = s["blev"][z] + s["forsvandt"][z]
        if n:
            rater[z] = s["forsvandt"][z] / n
    if not rater:
        return
    top = max(rater.values())

    print(f"\n  MAALT VAEGTVEKTOR (horisont {horisont} mdr., hoejeste = 1,00)")
    print(f"    {'zone'.ljust(14)} {'n':>9} {'churn-rate':>11} "
          f"{'maalt':>7} {'i dag':>7} {'forskel':>8}")
    for z in ZONE_ORDER:
        if z not in rater:
            continue
        n = s["blev"][z] + s["forsvandt"][z]
        maalt = rater[z] / top
        nu = ZONE_VAEGT.get(z, 0.0)
        print(f"    {ZONE_LABELS[z].ljust(14)} {n:>9,} {100*rater[z]:>10.2f}% "
              f"{maalt:>7.2f} {nu:>7.2f} {maalt - nu:>+8.2f}")


def main() -> int:
    print("--- forbrugsdata ---")
    forbrug = forbrug_pr_abonnement()
    vindue = sorted(forbrug["maaneder"])
    # Navnet er historisk: saettet er nu koblingsbare kunder fra dm_kobling
    # plus ACV_snapshot. Snapshot-halvdelen kender kun de AKTIVE konti, saa den
    # er dateret efter udfaldet og maa skaeres ud naar zonerne maales paa en
    # historisk maaned. forbrug["uden_aktiv_konto"] er den gruppe.
    med_zuora = forbrug["koblingsbare"]
    # Tomt saet: denne fil maaler bagdaterede referencer, og konto_status er
    # dateret efter udfaldet. Se zone_for's docstring.
    uden_aktiv_konto = frozenset()
    print(f"  vindue: {vindue[0]} .. {vindue[-1]}  ({len(vindue)} maaneder)")

    # R+H skal være en HEL måned. Indeværende måned er ufuldstændig, og et
    # abonnement der endnu ikke har fået sin række ville se ud som churn.
    sidste_hele = forskyd_maaned(date.today().strftime("%Y-%m"), 1)
    # Referencen skal have historik BAG sig, ellers gør beskæringen alt "ny".
    med_historik = [m for m in vindue if vindue.index(m) >= MIN_HISTORIK]
    print(f"  referencer med nok historik: {med_historik[0]} .. {med_historik[-1]}"
          f"  ({len(med_historik)})")
    print(f"  sidste hele maaned: {sidste_hele}")
    # Ingen tavse afgrænsninger: hvad der falder ud, og hvorfor.
    print(f"  udeladt som reference: "
          f"{', '.join(m for m in vindue if m not in med_historik)}"
          f"\n    de foerste {MIN_HISTORIK} maaneder i vinduet mangler historik"
          f" bag sig")

    behov = set()
    pr_horisont: dict = {}
    for h in HORISONTER:
        refs = [m for m in med_historik if forskyd_maaned(m, -h) <= sidste_hele]
        pr_horisont[h] = refs
        for m in refs:
            behov.add(m)
            for n in range(1, h + 1):
                behov.add(forskyd_maaned(m, -n))
        udeladt = [m for m in med_historik if m not in refs]
        print(f"  horisont {h}: {len(refs)} referencer"
              + (f", udeladt {', '.join(udeladt)} (R+{h} er ikke en hel maaned)"
                 if udeladt else ""))

    alle = sorted(m for m in behov if m <= sidste_hele)
    print(f"\n--- abonnementer pr. maaned ({len(alle)} queries) ---")
    pr_maaned = hent_maaneder(alle)

    for h in HORISONTER:
        maalinger = [maal(r, h, pr_maaned, forbrug, med_zuora,
                          uden_aktiv_konto)
                     for r in pr_horisont[h]
                     if forskyd_maaned(r, -h) in pr_maaned]
        if not maalinger:
            print(f"\nHorisont {h}: ingen maalbare referencemaaneder.")
            continue
        skriv_horisont(h, maalinger)
        skriv_vaegtvektor(h, maalinger)

    print(f"\n\n{'=' * 72}")
    print("FORBEHOLD")
    print(f"{'=' * 72}")
    print("  Maalingen siger intet om HVORFOR nogen forsvandt. Et abonnement kan"
          "\n  vaere opsagt, omlagt til en anden raekke eller flyttet ind i en"
          "\n  pakke. Foerst RetentionOutcomes kan skelne de tre, og den er tom"
          "\n  indtil specialisten registrerer den foerste samtale.")
    print("\n  Maj 2026 er en kendt artefakt: 1.769 abonnementer blev genskabt"
          "\n  mellem april og maj, og abonnementstallet"
          "\n  springer fra ca. 12.000 til ca. 15.500. Referencemaaneder paa hver"
          "\n  side af springet er ikke sammenlignelige.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

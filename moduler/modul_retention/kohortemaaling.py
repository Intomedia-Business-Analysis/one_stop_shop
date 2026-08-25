"""Maaler et signal mod RIGTIGE opsigelser paa bagdaterede kohorter.

    python -m moduler.modul_retention.kohortemaaling

HVORFOR FILEN FINDES: den foerste udgave af denne maaling blev koert 21-08-2026
og laa i en scratchpad-mappe. Den er vaek, og designet maatte skrives ned i en
handoff for at kunne genskabes. Vaegtvektoren i zones.py kan ikke saettes uden
denne maaling, saa den hoerer i repoet.

HVAD DEN SVARER PAA: hvor meget oftere siger en kunde i gruppe G op, end
gennemsnittet? Svaret er et INDEKS: 1,00 er gennemsnittet, 2,50 er to en halv
gang saa ofte. Indekset og ikke raten, fordi raten afhaenger af kohorten og
horisonten, mens indekset kan sammenlignes paa tvaers.

=============================================================================
DE FEM REGLER DER GOER MAALINGEN AERLIG
=============================================================================

1. UDFALDET DATERES PAA won_time, IKKE service_activation_date.
   sad er hvornaar abonnementet OPHOERER, won_time hvornaar opsigelsen blev
   REGISTRERET. En opsigelse registreret FOER kohortemaaneden med ophoer ude i
   fremtiden var allerede kendt paa maaletidspunktet, og at forudsige den ville
   vaere gratis point. Udfaldet er derfor won_time EFTER kohortemaaneden.

2. FORBRUGSSERIEN KLIPPES TIL KOHORTEMAANEDEN.
   Uden klipningen maaler filen sig selv: zonen for december ville vaere regnet
   paa laesning fra januar til august, altsaa efter udfaldet.

3. ALLEREDE OPSAGTE ER UDE.
   Et abonnement med en registreret opsigelse pr. kohortemaaneden kan ikke
   siges op igen, og det ville taelle i baade taeller og naevner.

4. >>> EN LAEKKET GRUPPE SKAL UD AF BASISRATEN, IKKE KUN AF SIN ZONE. <<<
   Det er den fejl der oedelagde foerste udgave 21-08. Med den laekkede gruppe
   i naevneren var basisraten 8,84 til 10,52 % i stedet for 5,3 til 5,5 %, og
   ALLE indeks var trykket ned med faktor 1,9: aldrig_i_gang maalte 1,45 i
   stedet for 2,6.

   Her fjernes kunder der KUN kan kobles via ACV_snapshot. Snapshottet
   indeholder kun de AKTIVE konti, saa deres tilstedevaerelse i maalingen er i
   sig selv et udtryk for udfaldet. Beviset for at det ER laekage: utrackbart
   site, som ikke KAN laekke, maalte 0,15, mens gruppen uden kobling maalte 9,0.
   To halvdele af samme zone, 60 gange fra hinanden.

   Kunder koblet via dm_kobling har ikke problemet: filen indeholder ogsaa de
   ophoerte konti, saa et fravaer siger intet om udfaldet.

   >>> KONTROLGRUPPEN "UTRACKBART SITE" ER TABT FRA 2026-08-25. <<<
   Shifter, Kom24 NO og Medier24 NO (zones.UNTRACKBARE_SITES) ligger
   UDELUKKENDE under watch_no, som fra 2026-08-25 er ekskluderet af HELE
   modul_retention (queries.UDENLANDSKE_ACCOUNTS, se queries.py). Kontrollen
   kollapser derfor til marketwires 32 raekker (sites=None), for lidt til at
   sige noget.

   SIDSTE MAALING MED watch_no STADIG MED, koert 2026-08-25 lige foer
   eksklusionen, gemt som kontrollens sidste ord:
     2025-12: 0,00 (49)   2026-01: 0,00 (51)   2026-02: 0,00 (54)
   Praecis nul i alle tre kohorter, som altid. Beviset staar hermed fast, men
   kan ikke gentages paa dansk grundlag alene. Naeste kalibrering skal finde
   en ny leakage-kontrolgruppe, eller acceptere at proeven ikke laengere kan
   koeres, kun den gamle konklusion genbruges.

5. uden_aktiv_konto ER OGSAA LAEKKET, OG BEHANDLES SOM SIN EGEN TABEL.
   Gruppen er de kunder der kun kan kobles gennem ophoerte konti. For en
   kohorte i december 2025 er det i vidt omfang PRAECIS DEM DER SAGDE OP i
   horisonten, og maalt her rammer den 7 til 9 gange basisraten uanset adfaerd.

   Derfor TO basisrater, og de skrives begge ud:
     - hele populationen  ca. 9,5 til 11,5 %
     - uden gruppen       ca. 5 til 6 %
   Adfaerdstabellerne (zoner, recency, artikelvisninger) maales mod den RENE.
   Blev gruppen staaende i naevneren, ville hvert adfaerdsindeks blive trykket
   ned med samme faktor to, praecis som 21-08 hvor aldrig_i_gang maalte 1,45 i
   stedet for 2,6.

   Gruppen forsvinder ikke: den staar i sin egen tabel mod hele populationens
   rate, maerket LAEKKET. Det tal er ikke en vaegt. Det er svaret paa hvor
   meget af aldrig_i_gang's 2,5 der var ophoerte konti og ikke laeseadfaerd.

   Og uanset hvad maa konto_status ALDRIG vaere INPUT til zonen paa en
   historisk maaned. Derfor kaldes bestem_zone her UDEN har_aktiv_konto, mens
   risiko.py sender den med. De to er forskellige med vilje.

=============================================================================
HVAD DEN MAALER
=============================================================================

  zoner            Den nuvaerende model, som den staar i zones.bestem_zone.
  recency          Dage siden sidste laesning, fra sidste_dato i eksporten.
  artikelvisninger Niveau, ikke forandring. Aldrig testet foer.

De to sidste er tilfoejet 24-08-2026 og er de eneste adfaerdsmaal vi ALDRIG har
proevet mod rigtige opsigelser. Faldende forbrug er derimod proevet af: tre
maal, fire taerskler, tre kohorter, 36 kombinationer, ingen over 1,10.

RECENCY KRAEVER KOLONNE 12 sidste_dato i usage_kunde-eksporten. Mangler den,
springes afsnittet over med en besked. Maanedsfilen alene giver kun tre mulige
vaerdier for recency, og 77 % ligger paa nul, saa den kan ikke maales derfra.
"""

import sys
from datetime import date, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv                                       # noqa: E402

load_dotenv()

from db import get_conn                                              # noqa: E402
from moduler.modul_retention.queries import (                        # noqa: E402
    OPSIGELSE_PIPELINES, db_abonnementer,
)
from moduler.modul_retention.usage import (                          # noqa: E402
    customer_key, forbrug_pr_abonnement, load_usage_kunde, serie_og_dage,
)
from moduler.modul_retention.zones import (                          # noqa: E402
    PAKKE_SITES, ZONE_ORDER, bestem_zone, forskyd_maaned, kanonisk_site,
)

# Tre kohorter og seks maaneders horisont, som 21-08. Tre og ikke een, fordi et
# indeks paa een kohorte ikke kan skelnes fra stoej: skifter rangordenen mellem
# de tre, er signalet ikke stabilt nok til at baere en vaegt.
KOHORTER = ["2025-12", "2026-01", "2026-02"]
HORISONT = 6

# MONITOR KAN IKKE MAALES HISTORISK, og det er strukturelt og ikke en fejl vi
# kan rette i denne fil. Datamarten indeholder slet ikke brandet, saa den
# ENESTE vej fra en monitor-konto til en kunde er ACV_snapshot, og snapshottet
# indeholder kun de AKTIVE konti. Baade at staa i det og at mangle i det er
# derfor bestemt af udfaldet.
#
# Maalt 24-08-2026 paa kohorte 2025-12: af de abonnementer der ikke kunne
# kobles maalte monitor 133 raekker med 127 opsigelser, altsaa 95,5 %. Til
# sammenligning maalte watch_no's ukoblede 259 raekker med 1,2 %, altsaa UNDER
# basisraten. Det er ikke et adfaerdssignal, det er en spejling af udfaldet.
#
# Fjernes gruppen ikke, loefter den basisraten og trykker hvert adfaerdsindeks
# ned. Den kan foerst maales naar der findes en monitor-kobling med historik,
# og det spoergsmaal ligger hos datateamet.
EKSKLUDEREDE_ACCOUNTS = {"monitor"}

RECENCY_ORDER = ["R 0-7", "R 8-25", "R 26-60", "R 61-90", "R 90+", "R aldrig"]
ARTIKEL_ORDER = ["art 60+", "art 21-60", "art 6-20", "art 1-5", "art 0"]
FLAG_ORDER = ["uden aktiv konto (LAEKKET)", "har aktiv konto"]
HUL_ORDER = ["uden kobling", "utrackbart site"]


def sidste_dag(maaned: str) -> str:
    """Sidste dag i maaneden som 'YYYY-MM-DD'."""
    aar, md = int(maaned[:4]), int(maaned[5:7])
    foerste_naeste = date(aar + (md // 12), (md % 12) + 1, 1)
    return (foerste_naeste - timedelta(days=1)).isoformat()


def beskaer(serie: dict, maaned: str) -> dict:
    """Forbruget til og med `maaned`. UDEN DEN MAALER FILEN SIG SELV."""
    return {m: v for m, v in serie.items() if m <= maaned}


def db_opsigelser_paa_registrering() -> dict:
    """{(account, org_id, sites): ['YYYY-MM-DD', ...]} - vundne opsigelser.

    Datoen er won_time og IKKE service_activation_date, jf. regel 1. ALLE
    datoer beholdes og ikke kun den seneste: hver kohorte skal kunne spoerge om
    sit eget vindue, og den seneste dato ville svare forkert for den tidligste.
    """
    ph = ",".join(["%s"] * len(OPSIGELSE_PIPELINES))
    conn = get_conn()
    cur = conn.cursor(as_dict=True)
    cur.execute(f"""
        SELECT account, org_id, sites,
               CONVERT(char(10), won_time, 23) AS won_dato
        FROM dbo.PipedriveDeals
        WHERE status = 'won'
          AND org_id IS NOT NULL
          AND won_time IS NOT NULL
          AND pipeline_name IN ({ph});
    """, OPSIGELSE_PIPELINES)
    raekker = cur.fetchall()
    conn.close()

    ud: dict = {}
    for r in raekker:
        kunde = customer_key(r["account"], r["org_id"])
        ud.setdefault((kunde[0], kunde[1], r["sites"]), []).append(r["won_dato"])
    return ud


def laes_ekstra_kolonner() -> tuple[dict, dict, dict]:
    """(sidste_dato, artikler_pr_abonnement, artikler_pr_kunde) fra eksporten.

    Laeses HER og ikke i usage.forbrug_pr_abonnement, saa den koerende app ikke
    baerer omkostningen for noget kun en maaling bruger, og saa en maaling ikke
    kan vaelte en side.

    sidste_dato er tom hvis eksporten mangler kolonne 12.

    FILEN LAESES DIREKTE og ikke gennem load_usage_kunde(). Loaderen skaerer
    til de foerste elleve kolonner (`df.iloc[:, :len(USAGE_COLUMNS)]`), saa
    kolonne 12 findes ikke i dens frame. Det er med vilje i den koerende app,
    men det gjorde foerste udgave af denne maaling blind for sin egen kolonne.
    """
    import pandas as pd

    from moduler.modul_portfolio_alignment.queries import SCOPE_BY_ZUORA_BRAND

    from moduler.modul_retention.usage import (
        USAGE_COLUMNS, find_latest_usage_file,
    )

    target = find_latest_usage_file()
    if target is None:
        raise FileNotFoundError("ingen usage_kunde-eksport fundet")
    df = pd.read_csv(target, header=None, sep=",", encoding="utf-8", dtype=str)
    if str(df.iloc[0].iloc[0]).strip().lower() == "pipedrive_id":
        df = df.iloc[1:].reset_index(drop=True)
    navne = USAGE_COLUMNS + ["sidste_dato"]
    df = df.iloc[:, :len(navne)].copy()
    df.columns = navne[:df.shape[1]]
    for kol in ("pipedrive_id", "brand", "site", "maaned"):
        df[kol] = df[kol].fillna("").astype(str).str.strip()
    df["artikelvisninger"] = (pd.to_numeric(df["artikelvisninger"],
                                            errors="coerce")
                              .fillna(0).astype("int64"))

    har_dato = "sidste_dato" in df.columns
    if har_dato:
        df["sidste_dato"] = df["sidste_dato"].fillna("").astype(str).str.strip()
    datoer = df["sidste_dato"] if har_dato else df["maaned"]

    sidste: dict = {}
    pr_abo: dict = {}
    pr_kunde: dict = {}
    for org, brand, site, maaned, art, dato in zip(
            df["pipedrive_id"], df["brand"], df["site"], df["maaned"],
            df["artikelvisninger"], datoer):
        if not org:
            continue
        scope = SCOPE_BY_ZUORA_BRAND.get(brand)
        if not scope:
            continue
        kunde = customer_key(scope, org)
        site_k = kanonisk_site(site)

        a = pr_abo.setdefault((kunde, site_k), {})
        a[maaned] = a.get(maaned, 0) + int(art)
        k = pr_kunde.setdefault(kunde, {})
        k[maaned] = k.get(maaned, 0) + int(art)

        if har_dato and dato:
            m = sidste.setdefault((kunde, site_k), {})
            # To raekker kan folde til samme kanoniske site (.com og .dk), og
            # da er den SENESTE dato den rigtige.
            if maaned not in m or dato > m[maaned]:
                m[maaned] = dato
    return (sidste if har_dato else {}), pr_abo, pr_kunde


def artikel_baand(pr_abo: dict, pr_kunde: dict, kunde: tuple, site: str,
                  maaned: str) -> str:
    """Artikelvisninger i kohortemaaneden, i baand.

    Pakke- kontra site-niveau afgoeres som i usage.serie_og_dage, ellers ville
    en pakke maales paa et site kun 7 % af dens abonnenter laeser.
    """
    serie = (pr_kunde.get(kunde, {}) if site in PAKKE_SITES
             else pr_abo.get((kunde, site), {}))
    v = serie.get(maaned, 0)
    if v == 0:
        return "art 0"
    if v <= 5:
        return "art 1-5"
    if v <= 20:
        return "art 6-20"
    if v <= 60:
        return "art 21-60"
    return "art 60+"


def recency_baand(sidste: dict, kunde: tuple, site: str, maaned: str,
                  ultimo: str) -> str:
    """Dage fra sidste laesning til ultimo kohortemaaneden, i baand.

    Kun datoer til og med kohortemaaneden taeller, jf. regel 2.

    Baandene ligger i samme stoerrelsesorden som FT's RFV-graenser, men
    tallene er VORES og skal kalibreres paa udfaldet. FT's egne taerskler er
    sat paa en B2C-laeserbase: maalt 24-08-2026 rammer deres graense for
    "engageret" 65,9 % af vores portefoelje.
    """
    serie = sidste.get((kunde, site), {})
    kandidater = [d for m, d in serie.items() if m <= maaned]
    if not kandidater:
        return "R aldrig"
    dage = (date.fromisoformat(ultimo) - date.fromisoformat(max(kandidater))).days
    if dage <= 7:
        return "R 0-7"
    if dage <= 25:
        return "R 8-25"
    if dage <= 60:
        return "R 26-60"
    if dage <= 90:
        return "R 61-90"
    return "R 90+"


def maal_kohorte(maaned: str, forbrug: dict, opsigelser: dict, udeluk: set,
                 sidste: dict, pr_abo: dict, pr_kunde: dict) -> dict:
    """Alle grupperinger for een kohortemaaned."""
    slut = sidste_dag(forskyd_maaned(maaned, -HORISONT))
    ultimo = sidste_dag(maaned)
    koblingsbare = forbrug["koblingsbare"]
    uden_aktiv = forbrug["uden_aktiv_konto"]

    grupper = {"zoner": {}, "recency": {}, "artikler": {}, "flag": {},
               "hul": {}}
    n_udeladt = n_allerede = n_monitor = 0
    alle_n = alle_o = 0

    def taeld(navn, gruppe, opsagt):
        b = grupper[navn].setdefault(gruppe, [0, 0])
        b[0] += 1
        b[1] += 1 if opsagt else 0

    for r in db_abonnementer(maaned):
        kunde = customer_key(r["account"], r["org_id"])
        if kunde[0] in EKSKLUDEREDE_ACCOUNTS:                 # Regel 4
            n_monitor += 1
            continue
        if kunde in udeluk:                                   # Regel 4
            n_udeladt += 1
            continue

        noegle = (kunde[0], kunde[1], r["sites"])
        datoer = opsigelser.get(noegle, [])
        if any(d <= ultimo for d in datoer):                  # Regel 3
            n_allerede += 1
            continue
        opsagt = any(ultimo < d <= slut for d in datoer)      # Regel 1

        site = kanonisk_site(r["sites"])
        serie, _dage = serie_og_dage(forbrug, kunde, site)
        serie = beskaer(serie, maaned)                        # Regel 2

        # UDEN har_aktiv_konto, jf. regel 5: konto_status beskriver i dag.
        z = bestem_zone(serie, maaned, r["foerste_maaned"], site,
                        kunde in koblingsbare)

        # HELE populationen, inklusive den laekkede gruppe. Kun til flag-
        # tabellen og til at kunne skrive begge basisrater ud.
        alle_n += 1
        alle_o += 1 if opsagt else 0
        laekket = kunde in uden_aktiv
        taeld("flag", "uden aktiv konto (LAEKKET)" if laekket
              else "har aktiv konto", opsagt)
        if laekket:
            # Regel 4 igen, i sin skarpe form: gruppen maaler 7 til 9 gange
            # basisraten, og den gjorde det UANSET adfaerd. Blev den staaende i
            # naevneren, ville basisraten fordobles og hvert adfaerdsindeks
            # blive trykket ned med samme faktor. Den taelles derfor med i
            # flag-tabellen og INTET andet sted.
            continue

        taeld("zoner", z, opsagt)
        # ingen_data har TO aarsager, og de opfoerte sig 60 gange forskelligt
        # i maalingen 21-08: utrackbart site 0,15, uden kobling 9,0. Splittet
        # er kontrollen for at den tilbagevaerende halvdel er ren.
        if z == "ingen_data":
            taeld("hul", "utrackbart site" if kunde in koblingsbare
                  else "uden kobling", opsagt)
        else:
            # Et utrackbart abonnement har intet forbrug at maale, og nul ville
            # blive laest som "laeser ikke" i stedet for "vi kan ikke se det".
            taeld("artikler", artikel_baand(pr_abo, pr_kunde, kunde, site,
                                            maaned), opsagt)
            if sidste:
                taeld("recency", recency_baand(sidste, kunde, site, maaned,
                                               ultimo), opsagt)

    n = sum(v[0] for v in grupper["zoner"].values())
    o = sum(v[1] for v in grupper["zoner"].values())
    grupper["basisrate"] = o / n if n else 0.0
    grupper["basisrate_alle"] = alle_o / alle_n if alle_n else 0.0
    grupper["antal"] = n
    grupper["opsagte"] = o
    grupper["antal_alle"] = alle_n
    grupper["opsagte_alle"] = alle_o
    grupper["udeladt"] = n_udeladt
    grupper["allerede_opsagt"] = n_allerede
    grupper["monitor"] = n_monitor
    return grupper


def indeks(fordeling: dict, basisrate: float) -> dict:
    """{gruppe: (antal, opsagte, rate, indeks)}."""
    ud = {}
    for g, (n, opsagt) in fordeling.items():
        if n:
            rate = opsagt / n
            ud[g] = (n, opsagt, rate, rate / basisrate if basisrate else 0.0)
    return ud


def tegn(titel: str, pr_kohorte: list, raekkefoelge: list) -> None:
    """Indeks-tabel med een soejle pr. kohorte. Tomme grupper udelades.

    Grupper der IKKE staar i raekkefoelgen tegnes til sidst, saa en ny vaerdi
    aldrig kan forsvinde tavst fra tabellen.
    """
    kendte = [g for g in raekkefoelge if any(g in d for d in pr_kohorte)]
    ekstra = sorted({g for d in pr_kohorte for g in d} - set(raekkefoelge))
    print()
    print("  " + titel)
    print("  " + "-" * (24 + 14 * len(KOHORTER)))
    print("  %-22s" % "gruppe" + "".join("%13s " % k for k in KOHORTER))
    for g in kendte + ekstra:
        celler = []
        for d in pr_kohorte:
            if g in d:
                n, _o, _r, ix = d[g]
                celler.append("%6.2f (%5d)" % (ix, n))
            else:
                celler.append("%13s" % "-")
        print("  %-22s" % g + "".join("%13s " % c for c in celler))


def main() -> int:
    print("=" * 78)
    print("KOHORTEMAALING  ·  %s  ·  %d maaneders horisont"
          % (", ".join(KOHORTER), HORISONT))
    print("=" * 78)

    forbrug = forbrug_pr_abonnement()
    print("  forbrugsfil : %s" % forbrug["meta"]["filename"])
    print("  koblingsfil : %s" % forbrug["meta"]["kobling_filename"])

    udeluk = set(forbrug["kun_fra_snapshot"])                 # Regel 4
    print("  udeladt     : %d kunder der KUN kan kobles via ACV_snapshot"
          % len(udeluk))
    print("  BEHOLDT     : %d kunder uden aktiv konto, jf. regel 5"
          % len(forbrug["uden_aktiv_konto"]))

    sidste, pr_abo, pr_kunde = laes_ekstra_kolonner()
    if sidste:
        print("  recency     : sidste_dato fundet paa %d enheder" % len(sidste))
    else:
        print("  recency     : SPRINGES OVER, eksporten mangler kolonne 12")
        print("                (koer usage_kunde.txt igen, se dens header)")

    opsigelser = db_opsigelser_paa_registrering()
    print("  opsigelser  : %d abonnementer med mindst een vundet opsigelse"
          % len(opsigelser))

    resultater = []
    for m in KOHORTER:
        r = maal_kohorte(m, forbrug, opsigelser, udeluk, sidste, pr_abo,
                         pr_kunde)
        resultater.append(r)
        print("\n  %s: %d i maalingen, %d opsagt inden for %d mdr."
              "  BASISRATE %.2f %%"
              % (m, r["antal"], r["opsagte"], HORISONT, 100 * r["basisrate"]))
        print("           hele populationen: %d / %d opsagt = %.2f %%"
              "  <- den laekkede gruppe FORDOBLER raten"
              % (r["antal_alle"], r["opsagte_alle"],
                 100 * r["basisrate_alle"]))
        print("           udeladt: %d monitor, %d kun-fra-snapshot, "
              "%d allerede opsagt, %d uden aktiv konto"
              % (r["monitor"], r["udeladt"], r["allerede_opsagt"],
                 r["antal_alle"] - r["antal"]))
        # En horisont der endnu ikke er udloebet UNDERTAELLER opsigelser, og
        # kohortens indeks kan derfor ikke sammenlignes med de oevrige som om
        # den var faerdig. Skrives ud og ikke bare beregnes: ellers ser de tre
        # soejler i tabellen praecis ens ud.
        udloeb = sidste_dag(forskyd_maaned(m, -HORISONT))
        if udloeb > date.today().isoformat():
            mgl = (date.fromisoformat(udloeb) - date.today()).days
            print("           >>> HORISONTEN ER IKKE UDLOEBET: %d dage mangler"
                  " til %s. Raten er for LAV. <<<" % (mgl, udloeb))

    # Adfaerdstabellerne maales mod den RENE basisrate, flag-tabellen mod hele
    # populationens. Blandes de to, sammenlignes en gruppe med et gennemsnit
    # den selv har loeftet.
    for titel, noegle, raekkefoelge, rate in (
            ("ZONER, modellen som den staar i dag", "zoner", ZONE_ORDER,
             "basisrate"),
            ("RECENCY, dage siden sidste laesning", "recency", RECENCY_ORDER,
             "basisrate"),
            ("ARTIKELVISNINGER i kohortemaaneden", "artikler", ARTIKEL_ORDER,
             "basisrate"),
            ("ingen_data SPLITTET paa aarsag", "hul", HUL_ORDER,
             "basisrate"),
            ("KONTO-FLAGET, mod HELE populationen, kun diagnose", "flag",
             FLAG_ORDER, "basisrate_alle"),
    ):
        pr = [indeks(r[noegle], r[rate]) for r in resultater]
        if any(pr):
            tegn(titel + "   indeks (antal)", pr, raekkefoelge)

    print()
    print("  LAES TABELLEN SAADAN: 1,00 er gennemsnittet. Et baand kan kun")
    print("  baere en vaegt hvis det ligger over 1,00 i ALLE tre kohorter OG")
    print("  rangordenen mellem baandene er den samme i alle tre.")
    print()
    print("  KONTO-FLAGET er maerket LAEKKET med vilje: konto_status beskriver")
    print("  i dag og ikke kohortemaaneden. Tallet siger hvor meget af")
    print("  aldrig_i_gang's 2,5 der var ophoerte konti, ikke laeseadfaerd.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

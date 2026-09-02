"""Zoner: hvor et abonnement står i sit forbrugsforløb.

Måleenheden er abonnementet `(account, org_id, site)`, jf. Zonemodellen. Zonen
beregnes på et MÅNEDLIGT signal og ikke på dage siden sidste aktivitet — et
dagsbaseret signal rådner med filens alder, mens "læste i juli" er et komplet
faktum om en afsluttet måned.

Målt 2026-08-06 på 15.205 abonnementer, juli 2026:

    læste i referencemåneden      50%
    stoppet                       13%   (6-8% i en normal måned, se nedenfor)
    tavs længere                   5%
    aldrig læst i vinduet         32%

SÆSON: "stoppet" ligger på 6-8% fra november til april og stiger til 13% i
juli. Halvdelen af sommertallet er ferie, ikke churn. Tærskler må derfor ikke
kalibreres på en sommermåned, og zonefordelingen skal læses med måneden i hånden.

HVORFOR "nystartet" ER EN EGEN TILSTAND (hed `ny` før Omdøbningen 2026-08-25):
mellem april og maj 2026 voksede porteføljen med 2.498 kunder og 3.451
abonnementer, fordelt næsten jævnt over hvert eneste site. Ægte salg ankommer
ikke sådan, så det er efter alt at dømme en bulk-import.
Uden `nystartet` ville hele den tilgang stå som churn-risiko på den første
liste specialisten ser.

Alderen måles IKKE på abonnementets første måned i `dbo.retention` alene, men på
det tidligste af den og den første måned med læsning — se foerste_kendte_maaned.
En bulk-import giver en ny RÆKKE på en relation, der kan være år gammel, og for
59,6% af de abonnementer zonen fangede ved reference 2026-07 var der forbrug før
rækkens egen første måned. Målt på kontraktrækken alene dæmpede zonen 4,8 mio.
kr. i årsværdi, hvoraf 327 abonnementer var etablerede læsere i fald eller stop.
Rettet 2026-08-11.

HVORFOR PAKKER MÅLES PÅ KUNDEN: `Watch Medier DK` giver adgang til alle
Watch-titler. Kun 7% af dens 264 abonnenter læser watchmedier.dk selv, mens 79%
læser noget. Målt på sit eget site ville hele pakken stå permanent kritisk.
Kalderen skal derfor slå forbruget op på kundeniveau for disse sites — se
PAKKE_SITES.
"""
from typing import Optional

from moduler.modul_portfolio_alignment.queries import normalize_site

# Under så mange hele måneder i dbo.retention er et abonnement "nystartet".
# Tre, fordi "paa_vej_ned" sammenligner mod snittet af de tre foregående
# måneder og derfor ikke kan beregnes før.
NY_MAANEDER = 3

# Hvor langt tilbage "paa_vej_ned" og "gaaet_i_staa" kigger efter tidligere
# læsning. Hed STOPPET_VINDUE til Omdøbningen (2026-08-25): navnet holder
# betydningen, den daekker nu begge zoner i stedet for kun én.
FALD_VINDUE = 3

# Hvor stort et fald der skal til for zonen "paa_vej_ned". 0,50 = halveret.
#
# STYRER KUN ZONE-LABELEN, IKKE LAENGERE EN SCORE. Maalt 26-08-2026 (fald-sweep,
# 24 kombinationer af taerskel/vindue/basis, se ZONE_VAEGT's kommentar): ingen
# kombination bestod laesereglen, og heller ikke 0,50 selv (paa_vej_ned churner
# UNDER fast_laeser). "paa_vej_ned" og "fast_laeser" har derfor samme vaegt
# (0,00) og samme gruppe (Ingen handling), og 0,50/3 afgoer stadig hvilken af
# de to labels en laesers kort viser - en reel forskel for specialisten (en
# faldende laeser er stadig vaerre nyt end en stabil, jf. zone_alvor), men uden
# proven grundlag. Aendres graensen, aendres kun HVEM der ser hvilket ord.
FALD_GRAENSE = 0.50

# Diskriminatoren for "vanebruger" er AKTIVE DAGE, ikke sidevisninger, og
# genmålingen 2026-08-11 viser hvorfor: blandt de 2.064 stoppede abonnementer
# er medianen 4,0 sidevisninger pr. abonnement-måned — målt over ALLE måneder
# i vinduet, også dem uden læsning; kun over måneder med læsning er den 9,0 —
# men 69,7% har over 20 aktive dage i de 12 måneder før referencen, median 37
# dage. Ørsted læste AgriWatch på 56 forskellige dage med et snit på 1,7
# sidevisninger — en vane, som volumen alene ville have kaldt støj.
#
# Vanen paavirkede tidligere ZONE_VAEGT (en gaaet_i_staa uden vane vejede
# halvt). Det er FJERNET 25-08-2026, se zone_vaegt()'s docstring - maalt mod
# rigtige opsigelser churnede gruppen UDEN vane lige saa stabilt. Konstanterne
# og er_vanebruger() lever videre som maalt diagnose (kohortemaaling.py) og som
# kontekst paa raekken (risiko.py's "vanebruger"/"aktive_dage_12m"), bare uden
# at saette scoren.
VANEBRUGER_DAGE = 20
VANEBRUGER_VINDUE = 12

# Visningsrækkefølge: værst først, datahuller sidst.
#
# SYV ZONER, IKKE OTTE. `stoppet` og `laenge_tavs` blev lagt sammen til
# `gaaet_i_staa` ved Omdøbningen (2026-08-25): begge betød "signalet er væk,
# lav redningssandsynlighed" og delte allerede vægt for den halvdel uden vane
# (dengang zone_vaegt()'s modifikator, siden fjernet, se ovenfor). Navnene
# laaner FT Strategies' RFV-stil (konkrete nok til at huskes), ikke deres akse
# — se plandokumentet for hvorfor.
ZONE_ORDER = ["aldrig_i_gang", "gaaet_i_staa", "paa_vej_ned", "fast_laeser",
              "nystartet", "lukket_konto", "ingen_data"]

# Vægten i score = ARR × vægt × timingfaktor. MÅLT 25-08-2026 mod rigtige
# opsigelser (kohortemaaling.py: won_time efter kohortemaaneden, tre kohorter
# med udløbet 6-maaneders horisont, 2025-11 til 2026-01). Se skriv_vaegtvektor()
# i den fil for beregningen.
#
# `aldrig_i_gang` og `gaaet_i_staa` har BYTTET RANGORDEN siden Omdøbningen.
# aldrig_i_gang målte konsekvent højest (indeks 1,62-1,82, snit 1,71, churn-rate
# 9,77%) og er nu den værste zone, normaliseret til 1,00. gaaet_i_staa målte
# lavere men stadig over 1,00 i alle tre kohorter (indeks 1,11-1,47, snit 1,33,
# churn-rate 6,80%), normaliseret til 0,70 — OGSAA vanebruger-splittet der før
# gav den 1,00/0,50 er fjernet, se zone_vaegt()'s docstring.
#
# `paa_vej_ned` ER NU AFSKAFFET SOM SCORE, MAALT 26-08-2026. Zonen dumpede sin
# egen laeseregel to gange (21-08: 36 kombinationer af visninger/laesedage/
# unikke_brugere x fire taerskler, ingen over 1,10; 25-08: indeks 0,55 i den
# foerste kohorte). Et fald-sweep 26-08 udvidede gitteret til seks taerskler
# (20-70 %) x to baser (visninger/laesedage) x to vinduer (3 og 6 maaneder, 6
# kun daekket for kohorten 2026-01) - 24 celler, NUL bestod laesereglen (indeks
# over 1,00 i alle tre kohorter, n >= 300, konsistent rangorden). Den taetteste
# celle var "visninger v3 70%" (0,71/0,89/1,24), som baade dumper 2025-11 og
# 2025-12 OG har n=289 under n-graensen. En faldende laeser churner ikke
# oftere end en stabil (paa_vej_ned 4,38 % mod fast_laeser 4,75 %), saa vaegten
# er 0,00 og ikke et skoen. Se kohortemaaling.py's FALD_GITTER.
#
# Konsekvens maalt paa 13.044 danske abonnementer FOER aendringen: Foelg op
# gaar fra 6.779 til 3.890 abonnementer (70,2 til 21,2 mio. kr.), Ingen
# handling fra 5.580 til 8.469. Ring nu er UPAAVIRKET (685 / 3,8 mio. kr., kun
# aldrig_i_gang naar 1,00). Top 20 skifter markant: kun 4 af 20 kunder/sites
# staar fast, fordi store paa_vej_ned-konti (DR, KMD, Deloitte, Ascendis
# Pharma, Takeda) tidligere konkurrerede sig ind paa ARR trods en moderat
# vaegt (score = ARR x vaegt x TIMINGFAKTOR) og nu forsvinder helt fra listen.
#
# `ingen_data` maalte hoejest af alle (indeks ca. 5,3, churn-rate 31%), men det
# er LAEKAGE og ikke risiko: gruppen bestaar naesten kun af raekker uden Zuora-
# kobling, som historisk maalte 7-9x mens den ægte kontrolgruppe ("utrackbart
# site") maalte praecis 0,00 — se kohortemaaling.py regel 4. Kontrolgruppen er
# tabt siden den danske afgraensning 25-08-2026, saa beviset kan ikke gentages,
# men konklusionen bliver staaende: vaegten er 0,15 AF PRINCIP, ikke af maaling,
# og er bevidst udelukket fra normaliseringen i skriv_vaegtvektor().
#
# Tabellen er de NOMINELLE vægte. Alle rækker af samme zone har nu samme
# faktiske vægt (bortset fra opsigelsesfilteret, som nulstiller til 0).
ZONE_VAEGT = {
    "aldrig_i_gang": 1.00,
    "gaaet_i_staa":  0.70,
    "paa_vej_ned":   0.00,
    "ingen_data":    0.15,
    "fast_laeser":   0.00,
    "nystartet":     0.00,
    # Nul og ikke 0,15 som ingen_data, fordi det ikke er et datahul: vi VED
    # hvem kunden er, og Zuora siger at kontoen er ophoert. Maalt 24-08-2026 paa
    # de 1.253 ramte abonnementer: 47 af dem har overhovedet et ARR-beloeb i
    # ACV, mod 87,1 % af de oevrige, og hele gruppen udgoer 336.511 kr. af 202
    # mio. Det stoerste enkelte er 38.143 kr. Der er ingen portefoelje at
    # forsvare, og et opkald om churn er det forkerte opkald.
    "lukket_konto":  0.00,
}

ZONE_LABELS = {
    "gaaet_i_staa":  "Gået i stå",
    "aldrig_i_gang": "Aldrig i gang",
    "paa_vej_ned":   "På vej ned",
    "fast_laeser":   "Fast læser",
    "nystartet":     "Nystartet",
    "lukket_konto":  "Lukket konto",
    "ingen_data":    "Ingen data",
}

# .com-udgaverne er samme produkt på engelsk. Verificeret 2026-08-06: 92% af
# shippingwatch.com's 452 læsende kunder har et abonnement i familien (85% for
# medwatch.com og energywatch.com). Uden foldningen ville 452 kunder stå tavse
# på et site de læser hver uge.
BRAND_FAMILIE = {
    "shippingwatch.com": "shippingwatch.dk",
    "medwatch.com":      "medwatch.dk",
    "energywatch.com":   "energiwatch.dk",
}

# Pakkeabonnementer: forbruget slås op på KUNDEN, ikke på sitet.
PAKKE_SITES = {
    "watchmedier.dk", "monitormedier.dk",
}

# Sites hvor vi ikke har nogen kilde. Et abonnement her får "ingen_data" og
# ALDRIG "aldrig_i_gang" — et datahul er ikke bevis på at kunden er tavs.
#
# DE TRE NO-SITES ER UOPNÅELIGE FRA 2026-08-25. Shifter, Kom24 NO og Medier24 NO
# lå udelukkende under watch_no, som queries._KUN_DANSKE nu udelukker af hele
# modulet. De står med vilje: reglen beskriver Snowplows dækning, ikke modulets
# geografiske afgrænsning, og skal ikke slettes bare fordi den ikke rammer noget
# lige nu.
UNTRACKBARE_SITES = {
    "shifter",      # norsk brand, ikke i erhvervsmedier-schemaet
    "kom24 no",
    "medier24 no",
    None,           # marketwire har intet site i dbo.retention
}

# finans.dk stod her indtil 2026-08-10. Adgangen til jyllandsposten-schemaet kom
# 2026-08-07, og eksporten dækker nu sitet: 1.692 konti, fladt over alle 13 hele
# måneder. Signalet er WEB ALENE — finans-appen er bevidst udeladt af
# usage_trend.txt, fordi en app-migrering i juni 2026 ellers ville have skabt
# falske gaaet_i_staa-zoner. Det gør finans sammenligneligt med Watch-sitene, som
# også er web alene.


# ---------------------------------------------------------------------------
# Månedsregning
# ---------------------------------------------------------------------------

def forskyd_maaned(maaned: str, antal: int) -> str:
    """Måneden `antal` måneder FØR `maaned`. Begge i formatet 'YYYY-MM'.

    Regner i hele måneder siden år 0 for at undgå specialtilfældet ved årsskifte:
    '2026-01' minus 1 skal give '2025-12', ikke '2026-00'.
    """
    aar, md = int(maaned[:4]), int(maaned[5:7])
    i = aar * 12 + (md - 1) - antal
    return f"{i // 12:04d}-{i % 12 + 1:02d}"


def foregaaende_maaneder(maaned: str, antal: int) -> list[str]:
    """De `antal` måneder umiddelbart før `maaned`, nyeste først."""
    return [forskyd_maaned(maaned, n) for n in range(1, antal + 1)]


def maaneders_alder(foerste_maaned: str, reference: str) -> int:
    """Hele måneder fra abonnementets første måned til referencemåneden.

    BEMÆRK grænsen: et abonnement der fandtes før eksportvinduet får vinduets
    første måned som `foerste_maaned` og ser dermed yngre ud end det er. Det er
    ufarligt så længe vinduet er længere end NY_MAANEDER — alt der findes i
    første måned er per definition ældre end tre måneder ved referencen.
    """
    a1, m1 = int(foerste_maaned[:4]), int(foerste_maaned[5:7])
    a2, m2 = int(reference[:4]), int(reference[5:7])
    return (a2 * 12 + m2) - (a1 * 12 + m1)


# ---------------------------------------------------------------------------
# Normalisering
# ---------------------------------------------------------------------------

def kanonisk_site(site: Optional[str]) -> Optional[str]:
    """Site på kanonisk form, med .com-udgaven foldet ind i søster-sitet.

    Bruges på BEGGE sider — abonnementets `sites` fra dbo.retention og
    forbrugets `site` fra Snowplow. At de går gennem samme funktion er hele
    grunden til at de to vokabularer mødes: 41 af 46 sites matcher direkte.
    """
    if site is None:
        return None
    normaliseret = normalize_site(site)
    return BRAND_FAMILIE.get(normaliseret, normaliseret)


def site_stamme(site: Optional[str]) -> Optional[str]:
    """Sitet uden topdomæne: 'finanswatch.dk' -> 'finanswatch'.

    GROVERE END kanonisk_site med vilje, og kun til ét formål: at afgøre om
    et site i Snowplow-eksporten hører til et brand vi følger.

    Why: de to vokabularer er IKKE enige om topdomænet. dbo.retention har
    'Nordic Defence Watch' (som normalize_site gør til
    'nordicdefencewatch', uden TLD), mens forbrugsfilen har
    'nordicdefencewatch.dk'. Et rent mængde-opslag ville derfor smide netop
    det site ud af forbrugspanelet, uden at noget fejlede. Målt 2026-09-02:
    med stammen overlever 19 af 34 sites, og de 15 der falder ud er 12
    monitor-sites, monitormedier.dk, techwatch.no og nordicdefencewatch.dk
    (sidstnævnte er nu MED, netop takket være stammen).

    Grovheden er ufarlig her: den kan kun slå to sites sammen, der deler navn
    før punktummet, og de to hører pr. definition til samme brand
    (medwatch.dk og medwatch.com).
    """
    kanonisk = kanonisk_site(site)
    if kanonisk is None:
        return None
    return kanonisk.split(".", 1)[0]


def er_trackbare(site: Optional[str], har_zuora_kobling: bool) -> bool:
    """Kan dette abonnement overhovedet få et forbrugssignal?

    To uafhængige grunde til nej: sitet har ingen kilde, eller kundens
    Zuora-konto kan ikke oversættes til en kunde. Begge skal give
    "ingen_data" og ikke "aldrig_i_gang".
    """
    return har_zuora_kobling and site not in UNTRACKBARE_SITES


# ---------------------------------------------------------------------------
# Selve zonelogikken
# ---------------------------------------------------------------------------

def foerste_kendte_maaned(forbrug: dict, foerste_maaned: str) -> str:
    """Tidligste spor af abonnementet: registreringen eller den første læsning.

    HVORFOR IKKE BARE `foerste_maaned`: den er en KONTRAKT-kendsgerning fra
    dbo.retention, ikke en oplysning om hvor længe kunden har læst. De to falder
    kun sammen, når rækken og relationen begynder samtidig — og det gør de ikke
    i 59,6% af de abonnementer, `nystartet` fanger ved reference 2026-07. Der er
    forbrug FØR abonnementets egen første måned, median otte måneder.

    Et site lagt ind på en eksisterende kunde, en genforhandlet kontrakt eller
    en bulk-import giver alle en ny RÆKKE på en gammel relation. Læsningen
    afslører det: har kunden læst sitet i et år, er der rigeligt at sammenligne
    med, uanset hvad rækken siger.

    Målt på det tidligste af de to bliver `nystartet` det, navnet lover — "for
    ny til at vurderes" — frem for "kontrakten er ung".

    GRÆNSE: forbrugseksporten dækker 14 måneder. Er relationen ældre end
    vinduet, kan vi ikke se det, og så er den her datering stadig for ung. Det
    er ufarligt, fordi vinduet er langt længere end NY_MAANEDER.
    """
    if not foerste_maaned:
        return foerste_maaned
    laest = [m for m, v in forbrug.items() if v]
    if not laest:
        return foerste_maaned
    return min(min(laest), foerste_maaned)


def er_vanebruger(dage: dict, reference: str) -> bool:
    """Havde abonnementet en vane at miste? Zonemodellens tærskel.

    `dage` er {måned: aktive dage} for abonnementet. For pakkeabonnementer skal
    kalderen sende KUNDENS dage, præcis samme regel som for sidevisningerne —
    ellers testes vanen på ét site mens zonen beregnes på syv.

    Vinduet er de 12 måneder FØR referencen. Referencemåneden tælles ikke med:
    zonen handler netop om at den er tom, og at tage den med ville sænke summen
    for præcis de abonnementer reglen skal bedømme.
    """
    i_vindue = sum(dage.get(m, 0)
                   for m in foregaaende_maaneder(reference, VANEBRUGER_VINDUE))
    return i_vindue > VANEBRUGER_DAGE


def bestem_zone(forbrug: dict,
                reference: str,
                foerste_maaned: str,
                site: Optional[str],
                har_zuora_kobling: bool = True,
                har_aktiv_konto: bool = True) -> str:
    """Zonen for ét abonnement i én referencemåned.

    `forbrug` er {måned: sidevisninger} for dette abonnement — kun måneder med
    læsning behøver at være med, fravær tolkes som nul. For pakkeabonnementer
    (PAKKE_SITES) skal kalderen sende KUNDENS samlede forbrug i stedet for
    sitets; se modul-docstringen.

    `reference` skal være den sidste HELE måned. Indeværende måned er levende og
    ville få enhver kunde til at se ud som et frit fald.

    `har_aktiv_konto` er falsk naar kunden KUN kan kobles gennem ophoerte
    Zuora-konti. Default sand, saa en kalder uden den oplysning ikke faar en
    zone skjult - samme princip som `har_zuora_kobling` lige over: mangler
    en kalder oplysningen, skal fejlen vise for meget risiko, ikke skjule den.

    Rækkefølgen af tjek er betydningsbærende og må ikke ombyttes:
    et nyt abonnement kan ikke være "gaaet i staa", og et utrackbart kan ikke
    være "aldrig i gang".
    """
    if not er_trackbare(site, har_zuora_kobling):
        return "ingen_data"

    # Pipedrive siger aktiv, Zuora har ingen aktiv konto, OG der er ikke laest
    # en enkelt gang i vinduet. To grunde til at tjekket staar praecis her:
    #
    # EFTER er_trackbare, fordi et utrackbart site ikke kan bevise noget om
    # forbrug. De 37 ramte raekker der ligger paa shifter, kom24, medier24 og
    # marketwire skal blive i ingen_data.
    #
    # FOER de forbrugsbaserede tjek, og gated paa at forbruget er tomt. Uden
    # gaten ville de 195 der FAKTISK laeser miste deres zone, og laesningen er
    # det staerkeste bevis vi har: laeser de, er det konto-statussen der er
    # foraeldet, ikke kunden der er vaek. Med gaten flytter kun de tavse.
    #
    # HVORFOR ikke lade dem staa som aldrig_i_gang: maalt 24-08-2026 var 690 af
    # zonens 1.422 abonnementer i denne gruppe. Zonen maalte altsaa knap
    # halvvejs "kontoen er ophoert" i stedet for "kunden laeser ikke", og den
    # ville derfor maale kunstigt staerkt naar vaegtene proeves efter paa
    # rigtige opsigelser.
    if not har_aktiv_konto and not any(v > 0 for v in forbrug.values()):
        return "lukket_konto"

    # Abonnementet fandtes IKKE i referencemåneden. Så kan referencens forbrug
    # umuligt være dets, og enhver sammenligning beskriver kunden frem for denne
    # række. Forbrugssporet kan godt være ældre — kunden læser sitet på en anden
    # aftale eller en pakke — men det gør ikke rækken gammel.
    #
    # Spærringen står FØR den forbrugsbaserede datering, fordi den ellers falder
    # væk netop for de nyeste abonnementer: målt 2026-08-11 gav den seks rækker
    # en rigtig zone, heriblandt en uge gammel aftale som "gaaet_i_staa" med
    # vægt 1,00 — øverst på listen. Se risiko.py's docstring om abo_maaned efter
    # reference.
    if maaneders_alder(foerste_maaned, reference) < 0:
        return "nystartet"

    # Ellers måles alderen på det tidligste SPOR, ikke på kontraktrækken.
    # `paa_vej_ned` kræver tre måneders FORBRUG at sammenligne med — ikke tre
    # måneders abonnement — og de to er forskellige, når rækken er nyere end
    # relationen. Se foerste_kendte_maaned.
    if maaneders_alder(foerste_kendte_maaned(forbrug, foerste_maaned),
                       reference) < NY_MAANEDER:
        return "nystartet"

    tidligere = [forbrug.get(m, 0)
                 for m in foregaaende_maaneder(reference, FALD_VINDUE)]

    if forbrug.get(reference, 0) > 0:
        snit = sum(tidligere) / len(tidligere)
        # snit == 0 betyder at kunden lige er begyndt at læse. Det er ikke et fald.
        if snit > 0 and forbrug[reference] <= snit * (1 - FALD_GRAENSE):
            return "paa_vej_ned"
        return "fast_laeser"

    # TJEK 7 OG 8 SMELTET TIL ÉT ved Omdøbningen (2026-08-25). Der var tidligere
    # skelnet mellem "stoppet" (læste i FALD_VINDUE, ikke i referencen) og
    # "laenge_tavs" (læste engang, men ikke i FALD_VINDUE) — de delte allerede
    # samme vægt for den halvdel uden vane, så skellet bar ingen beslutning.
    # `tidligere` er stadig beregnet ovenfor, til brug i "fast_laeser"/
    # "paa_vej_ned"-grenen, men afgør ikke længere noget her.
    if any(v > 0 for v in forbrug.values()):
        return "gaaet_i_staa"
    return "aldrig_i_gang"


def zone_vaegt(zone: str) -> float:
    """Risikovægten til score = ARR × vægt × timingfaktor (Prioriteringsmodellen).

    HAVDE et vanebruger-argument der halverede "gaaet_i_staa" for abonnementer
    uden 20+ aktive dage i de foregående 12 måneder. Fjernet 25-08-2026: målt
    mod rigtige opsigelser (kohortemaaling.py, tre udløbne kohorter) churner
    gruppen UDEN vane lige så stabilt som gruppen MED vane, det modsatte af
    modellens antagelse (indeks 1,25/1,19/1,47 mod 0,00/0,54/1,20 — sidstnævnte
    for ustabilt og for lille, n=49-89, til at bære en vægtforskel). Se
    er_vanebruger() og risiko.py's "vanebruger"/"aktive_dage_12m" felter, som
    stadig beregnes og vises — kun vægten er upåvirket nu.

    Ukendt zone giver 0,0 og ikke en fejl: en ny zone må aldrig kunne skubbe
    kunder op på listen ved et uheld, kun ned.
    """
    return ZONE_VAEGT.get(zone, 0.0)


# Tre overgrupper over de syv zoner. Syv tilstande er for mange at handle
# paa: FT loeste samme problem ved at folde syv clustre til tre overskrifter.
#
# GRUPPEN UDLEDES AF VAEGTEN og staar bevidst IKKE som en liste af zonenavne.
# En liste mere ville vaere den tredje kopi af samme rangorden (ZONE_ORDER og
# ZONE_VAEGT er de to foerste), og tre kopier bliver uenige. Faar en zone en ny
# vaegt, flytter den gruppe af sig selv.
GRUPPE_ORDER = ["ring_nu", "foelg_op", "ingen_handling"]
GRUPPE_LABELS = {
    "ring_nu":        "Ring nu",
    "foelg_op":       "Følg op",
    "ingen_handling": "Ingen handling",
}
# Gruppen ER vaegtbaandet, og teksten siger det, saa ingen kan tro at
# overskriften er en selvstaendig vurdering oven i modellen.
GRUPPE_HINT = {
    "ring_nu":        "risikovægt 1,00 · aldrig i gang målte højest af de rigtige zoner",
    "foelg_op":       "risikovægt 0,15 til 0,70",
    # paa_vej_ned (0,40) er ude siden 26-08-2026: zonen churner UNDER
    # fast_laeser (4,38 % mod 4,75 %), maalt over 24 taerskel/vindue/basis-
    # kombinationer uden en eneste over 1,00. Foelg op er derfor kun
    # gaaet_i_staa og ingen_data.
    "ingen_handling": "risikovægt 0,00",
}


def zone_gruppe(zone: str) -> str:
    """Zonens overgruppe, udledt af den NOMINELLE vaegt.

    Nominel og faktisk falder nu sammen: siden vanebruger-modifikatoren blev
    fjernet 25-08-2026 (se zone_vaegt()), har alle rækker af samme zone samme
    vægt, bortset fra opsigelsesfilteret.

    Ukendt zone lander i ingen_handling, samme princip som zone_vaegt's 0,0 og
    zone_alvor's sidsteplads: en ny zone maa aldrig kunne skubbe kunder op paa
    listen ved et uheld, kun ned.
    """
    v = ZONE_VAEGT.get(zone, 0.0)
    if v >= 1.00:
        return "ring_nu"
    if v > 0:
        return "foelg_op"
    return "ingen_handling"


def zone_alvor(zone: str) -> int:
    """Zonens alvor som sorteringsnøgle: lavere tal er værre. ZONE_ORDER's index.

    Findes fordi vægten IKKE kan bruges til at bryde uafgjort. To zoner deler
    0,50 (`aldrig_i_gang`, og `gaaet_i_staa` uden vane), og en sortering på
    score alene lader så Pythons stabile sort afgøre resten ud fra den
    rækkefølge rækkerne tilfældigvis kom i. Målt hos Jyske Bank, dengang
    zonerne hed anderledes: AgriWatch ("tavs længere", i dag en del af
    `gaaet_i_staa`) lå over AMWatch ("stoppet", i dag også `gaaet_i_staa`),
    begge 0,50 — og en stoppet vanebruger er den eneste tilstand hvor noget er
    sket for nylig og et opkald kan nå at virke. Den blev begravet.

    Rangordenen er ZONE_ORDER selv, som allerede står værst-først. Den må ikke
    kopieres til en ny liste: to kopier af samme rangorden bliver uenige.

    Ukendt zone lægger sig SIDST, samme princip som zone_vaegt's 0,0 — en ny
    zone må aldrig kunne skubbe kunder op på listen ved et uheld, kun ned. En
    ValueError her ville vælte hele siden på en zone, ingen havde forudset.
    """
    try:
        return ZONE_ORDER.index(zone)
    except ValueError:
        return len(ZONE_ORDER)


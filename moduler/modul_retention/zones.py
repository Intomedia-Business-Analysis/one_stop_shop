"""Zoner: hvor et abonnement står i sit forbrugsforløb.

Måleenheden er abonnementet `(account, org_id, site)`, jf. PRD §3. Zonen
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

HVORFOR "ny" ER EN EGEN TILSTAND: mellem april og maj 2026 voksede porteføljen
med 2.498 kunder og 3.451 abonnementer, fordelt næsten jævnt over hvert eneste
site. Ægte salg ankommer ikke sådan, så det er efter alt at dømme en bulk-import.
Uden `ny` ville hele den tilgang stå som churn-risiko på den første liste
specialisten ser.

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

# Under så mange hele måneder i dbo.retention er et abonnement "ny". Tre, fordi
# "faldende" sammenligner mod snittet af de tre foregående måneder og derfor
# ikke kan beregnes før.
NY_MAANEDER = 3

# Hvor langt tilbage "stoppet" kigger efter tidligere læsning.
STOPPET_VINDUE = 3

# Hvor stort et fald der skal til for zonen "faldende". 0,50 = halveret.
FALD_GRAENSE = 0.50

# PRD §3: "Stoppet vanebruger — over 20 aktive dage i de seneste 12 måneder, nu
# 0. Vægt 1,00" mod "Aldrig i brug — højst 1 aktiv dag i 12 måneder. Vægt 0,50".
# Diskriminatoren er AKTIVE DAGE, ikke sidevisninger, og genmålingen 2026-08-11
# viser hvorfor: blandt de 2.064 stoppede abonnementer er medianen 4,0
# sidevisninger pr. abonnement-måned — målt over ALLE måneder i vinduet, også
# dem uden læsning; kun over måneder med læsning er den 9,0 — men 69,7% har
# over 20 aktive dage i de 12 måneder før referencen, median 37 dage. Ørsted
# læste AgriWatch på 56 forskellige dage med et snit på 1,7 sidevisninger — en
# vane, som volumen alene ville have kaldt støj.
VANEBRUGER_DAGE = 20
VANEBRUGER_VINDUE = 12

# Visningsrækkefølge: værst først, datahuller sidst.
ZONE_ORDER = ["stoppet", "laenge_tavs", "aldrig_i_brug", "faldende",
              "sund", "ny", "intet_signal"]

# Vægten i score = ARR × vægt × timingfaktor. Kun "stoppet" er 1,00, fordi det er
# den eneste tilstand hvor noget er sket for nylig og et opkald kan nå at virke.
#
# `laenge_tavs` sat til 0,50 den 2026-08-10, samme som `aldrig_i_brug`: begge er
# "signalet er væk for længe siden, lav redningssandsynlighed". Valgt frem for
# 0,70 fordi 0,70 ville være det eneste tal i vektoren uden dækning i PRD §3 —
# de øvrige fem kommer derfra. Zonerne vises fortsat hver for sig, de har blot
# samme prioritet, og ZONE_ORDER bryder uafgjort i sorteringen. Alle vægte er
# provisoriske indtil forudsigelsesraten kan kalibrere dem på rigtige udfald
# (PRD §9).
#
# Tabellen er de NOMINELLE vægte. Den faktiske vægt for et abonnement kommer fra
# zone_vaegt(), som sænker "stoppet" til aldrig_i_brug-niveau når der ikke var en
# vane at miste.
ZONE_VAEGT = {
    "stoppet":       1.00,
    "laenge_tavs":   0.50,
    "aldrig_i_brug": 0.50,
    "faldende":      0.40,
    "intet_signal":  0.15,
    "sund":          0.00,
    "ny":            0.00,
}

ZONE_LABELS = {
    "stoppet":       "Stoppet",
    "laenge_tavs":   "Tavs længere",
    "aldrig_i_brug": "Aldrig i brug",
    "faldende":      "Faldende",
    "sund":          "Sund",
    "ny":            "Ny",
    "intet_signal":  "Intet signal",
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

# Sites hvor vi ikke har nogen kilde. Et abonnement her får "intet_signal" og
# ALDRIG "aldrig_i_brug" — et datahul er ikke bevis på at kunden er tavs.
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
# falske stoppet-zoner. Det gør finans sammenligneligt med Watch-sitene, som
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


def er_trackbare(site: Optional[str], har_zuora_kobling: bool) -> bool:
    """Kan dette abonnement overhovedet få et forbrugssignal?

    To uafhængige grunde til nej: sitet har ingen kilde, eller kundens
    Zuora-konto kan ikke oversættes til en kunde. Begge skal give
    "intet_signal" og ikke "aldrig_i_brug".
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
    i 59,6% af de abonnementer, `ny` fanger ved reference 2026-07. Der er
    forbrug FØR abonnementets egen første måned, median otte måneder.

    Et site lagt ind på en eksisterende kunde, en genforhandlet kontrakt eller
    en bulk-import giver alle en ny RÆKKE på en gammel relation. Læsningen
    afslører det: har kunden læst sitet i et år, er der rigeligt at sammenligne
    med, uanset hvad rækken siger.

    Målt på det tidligste af de to bliver `ny` det, navnet lover — "for ny til
    at vurderes" — frem for "kontrakten er ung".

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
    """Havde abonnementet en vane at miste? PRD §3's tærskel.

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
                har_zuora_kobling: bool = True) -> str:
    """Zonen for ét abonnement i én referencemåned.

    `forbrug` er {måned: sidevisninger} for dette abonnement — kun måneder med
    læsning behøver at være med, fravær tolkes som nul. For pakkeabonnementer
    (PAKKE_SITES) skal kalderen sende KUNDENS samlede forbrug i stedet for
    sitets; se modul-docstringen.

    `reference` skal være den sidste HELE måned. Indeværende måned er levende og
    ville få enhver kunde til at se ud som et frit fald.

    Rækkefølgen af tjek er betydningsbærende og må ikke ombyttes:
    et nyt abonnement kan ikke være "stoppet", og et utrackbart kan ikke være
    "aldrig i brug".
    """
    if not er_trackbare(site, har_zuora_kobling):
        return "intet_signal"

    # Abonnementet fandtes IKKE i referencemåneden. Så kan referencens forbrug
    # umuligt være dets, og enhver sammenligning beskriver kunden frem for denne
    # række. Forbrugssporet kan godt være ældre — kunden læser sitet på en anden
    # aftale eller en pakke — men det gør ikke rækken gammel.
    #
    # Spærringen står FØR den forbrugsbaserede datering, fordi den ellers falder
    # væk netop for de nyeste abonnementer: målt 2026-08-11 gav den seks rækker
    # en rigtig zone, heriblandt en uge gammel aftale som "stoppet" med vægt
    # 1,00 — øverst på listen. Se risiko.py's docstring om abo_maaned efter
    # reference.
    if maaneders_alder(foerste_maaned, reference) < 0:
        return "ny"

    # Ellers måles alderen på det tidligste SPOR, ikke på kontraktrækken.
    # `faldende` kræver tre måneders FORBRUG at sammenligne med — ikke tre
    # måneders abonnement — og de to er forskellige, når rækken er nyere end
    # relationen. Se foerste_kendte_maaned.
    if maaneders_alder(foerste_kendte_maaned(forbrug, foerste_maaned),
                       reference) < NY_MAANEDER:
        return "ny"

    tidligere = [forbrug.get(m, 0)
                 for m in foregaaende_maaneder(reference, STOPPET_VINDUE)]

    if forbrug.get(reference, 0) > 0:
        snit = sum(tidligere) / len(tidligere)
        # snit == 0 betyder at kunden lige er begyndt at læse. Det er ikke et fald.
        if snit > 0 and forbrug[reference] <= snit * (1 - FALD_GRAENSE):
            return "faldende"
        return "sund"

    if any(v > 0 for v in tidligere):
        return "stoppet"
    if any(v > 0 for v in forbrug.values()):
        return "laenge_tavs"
    return "aldrig_i_brug"


def zone_vaegt(zone: str, vanebruger: bool = True) -> float:
    """Risikovægten til score = ARR × vægt × timingfaktor (PRD §4).

    `vanebruger` modulerer KUN "stoppet", jf. PRD §3: en stoppet vanebruger er
    1,00 og kræver et opkald i dag, mens et abonnement uden vane at miste er en
    onboarding-sag og deler vægt med "aldrig i brug". De øvrige zoner er
    upåvirkede — "faldende" er 97,7% vanebrugere og ligger allerede under 0,50,
    og "laenge_tavs"/"aldrig_i_brug" er 0,50 uanset.

    Default True, så en kalder uden dage-data ikke får risiko skjult. Fejlen
    peger dermed mod at vise for meget frem for for lidt.

    Slår op i ZONE_VAEGT["aldrig_i_brug"] frem for at hardkode 0,50: ændres den
    vægt, skal de to følges, fordi det er samme argument der bærer dem.

    Ukendt zone giver 0,0 og ikke en fejl: en ny zone må aldrig kunne skubbe
    kunder op på listen ved et uheld, kun ned.
    """
    if zone == "stoppet" and not vanebruger:
        return ZONE_VAEGT["aldrig_i_brug"]
    return ZONE_VAEGT.get(zone, 0.0)


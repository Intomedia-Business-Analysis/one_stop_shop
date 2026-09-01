"""Fælles cache for retention-siderne.

HVORFOR ET EGET MODUL: `abonnementer_i_risiko()` tager 3,6 sekunder, fordi
`forbrug_pr_abonnement()` aggregerer 182.000 rækker ved hvert kald. Både
kunde-detaljen og Dagens opkald har brug for præcis det
samme resultat, og arbejdsgangen skifter mellem de to sider hele dagen.

Cachen lå oprindeligt i kunde.py og flyttede hertil, da Dagens opkald kom
til. Grunden er ikke hukommelse, men UTAKT: to caches over samme beregning
kan blive uenige.
Lander en ny forbrugseksport mellem to udløb, ville prioriteringssiden kunne vise
"stoppet" for en kunde, mens detaljesiden viste "faldende" — i op til
CACHE_SEKUNDER, uden at noget fejlede. Ét sted kan ikke være uenigt med sig selv.

DEN LIGGER IKKE I risiko.py, og det er fortsat med vilje: acceptkriteriet for
hele zonemodellen hænger på, at `abonnementer_i_risiko()` svarer det samme hver
gang den kaldes. En cache dér ville kræve, at samtlige zonetal blev bevist igen.

ÉN ryd_cache, ÉT sted. Routeren kalder den efter en registrering. Ligger der en
cache i et modul, som ryd_cache ikke rører, forsvinder specialistens egen
registrering fra siden i op til ti minutter uden at noget fejler — og det er en
fælde, der venter på den næste, der lægger noget i den.
"""
import logging
import time

from .queries import db_acv_ejere, db_org_navne
from .risiko import abonnementer_i_risiko
from .usage import forbrug_pr_abonnement, forbrug_pr_site
from .varsel import opsigelser_i_varsel

logger = logging.getLogger(__name__)

# Hvor længe et beregnet resultat genbruges. Signalet er MÅNEDLIGT (Zonemodellen), så
# selv en time ville være fagligt forsvarligt; ti minutter er valgt for at en ny
# usage-eksport eller en ARR-rettelse slår igennem inden for en pause, uden at en
# specialist venter 3,6 sekunder pr. kunde.
CACHE_SEKUNDER = 600

_cache: dict[tuple, tuple[float, object]] = {}


def cachet(noegle: tuple, beregn, cache_tomt: bool = True):
    """Genbrug et resultat i CACHE_SEKUNDER. Ingen baggrundstråd, ingen TTL-ryd.

    Ordbogen vokser med én post pr. unik team-afgrænsning, og dem er der en
    håndfuld af — en oprydning ville koste mere kompleksitet end den sparer.

    `cache_tomt=False` gemmer IKKE et tomt resultat. Det er ikke en optimering:
    de DB-funktioner, der svarer `{}` ved fejl, ville ellers lade ét sekunds
    databaseuheld slå siden ud i ti minutter. For risiko- og forbrugsdata er
    tomt derimod en tilstand, der ikke retter sig selv — der er default True.
    """
    nu = time.monotonic()
    ramt = _cache.get(noegle)
    if ramt is not None and nu - ramt[0] < CACHE_SEKUNDER:
        return ramt[1]
    vaerdi = beregn()
    if vaerdi or cache_tomt:
        _cache[noegle] = (nu, vaerdi)
    return vaerdi


def ryd_cache() -> None:
    """Tøm cachen. Kaldes efter en registrering, så siderne ikke viser gamle tal."""
    _cache.clear()


def risiko(teams, abo_maaned):
    """Risikobilledet, cachet. Samme resultat til alle tre retention-sider."""
    # teams er en liste og kan ikke være nøgle. Sorteret tuple, så to kald med
    # samme teams i forskellig rækkefølge rammer samme post.
    noegle = ("risiko", abo_maaned, tuple(sorted(teams)) if teams else None)
    return cachet(noegle, lambda: abonnementer_i_risiko(teams=teams,
                                                        abo_maaned=abo_maaned))


def forbrug():
    """Forbrug pr. abonnement pr. måned, cachet. 182.000 rækker aggregeret."""
    return cachet(("forbrug",), forbrug_pr_abonnement)


def forbrug_site():
    """Forbrug pr. site pr. måned, cachet. Porteføljens engagement-panel.

    Egen cache-post, IKKE en genbrug af forbrug()'s data: forbrug_pr_site
    aggregerer UDEN kundekobling (se dens docstring), så de to funktioner
    læser samme fil men returnerer forskellige ting. Én ryd_cache rammer
    stadig begge, fordi den tømmer hele _cache."""
    return cachet(("forbrug_site",), forbrug_pr_site)


def varsel(teams, maaned):
    """"Opsigelser i varsel"-panelet, cachet.

    4,9 sekunder koldt (abonnementer_med_ejer + db_opsigelser), samme
    team-nøgle-mønster som ejere() ovenfor. `maaned` er med i nøglen, så et
    månedsskift ikke serverer forrige måneds bog i op til ti minutter.
    """
    noegle = ("varsel", maaned, tuple(sorted(teams)) if teams else None)
    return cachet(noegle, lambda: opsigelser_i_varsel(teams=teams, maaned=maaned))


def ejere(teams):
    """{(account, org_id): {...}} — ejer og ARR pr. kunde, cachet.

    KUN `teams`, ingen owner_name. `_resolve_filters` returnerer altid None for
    den: retention er lukket for alt under Sales Operations (besluttet
    2026-08-10), og specialisten skal se hele firmaets churn-billede. En
    parameter, der aldrig varierer, ser ud til at gøre noget den ikke gør — og
    routerens egen docstring advarer mod netop den slags gren.

    0,3 sekunder pr. kald ifølge queries.db_acv_ejere. Det er ingenting alene,
    men når risikobilledet først er cachet, er det den dyreste del af et varmt
    sideopslag.
    """
    noegle = ("ejere", tuple(sorted(teams)) if teams else None)
    return cachet(noegle, lambda: db_acv_ejere(None, teams))


def navne():
    """{(account, org_id): org_name} for ALLE kunder, cachet.

    15.269 rækker pr. kald er for meget på en side, der loades hele dagen, og
    Dagens opkalds side liste 1 har højst MAKS_AABNE_SAGER navne at slå op. Det
    målrettede alternativ ville kræve dynamisk SQL med variabelt antal
    parametre; cachen betaler én gang og holder opslaget på én linje.

    TTL'en er ikke til for at slippe nye NAVNE igennem — der er målt 0
    navneskift på 15.269 kunder — men for nye KUNDER: en kunde oprettet efter
    processens start ville ellers være usynlig indtil genstart.

    `cache_tomt=False`, fordi db_org_navne svarer `{}` ved fejl, og en timeout på
    dette view er set i praksis. Uden det ville ét sekunds uheld tømme navnene på
    hele siden i ti minutter.
    """
    return cachet(("navne",), db_org_navne, cache_tomt=False)

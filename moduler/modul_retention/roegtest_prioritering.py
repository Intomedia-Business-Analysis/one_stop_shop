"""Røgtest af Dagens opkalds side datalag (prioritering.py) UDEN database.

    .venv/Scripts/python.exe moduler/modul_retention/roegtest_prioritering.py

Modstykket til roegtest_outcomes.py, og forskellen er hele pointen: den anden
skriver mod produktion i en transaktion der rulles tilbage, mens denne aldrig
åbner en forbindelse. Det kan lade sig gøre, fordi datalaget er bygget som rene
funktioner over ordbøger — `opfoelgninger` fik netop `seneste` ind udefra frem
for at hente den selv, og `tilbage_paa_listen` har med vilje ingen `i_dag`.
Prisen for at holde det sådan er, at det skal bevises; det er denne fil.

`db.py` åbner ingen forbindelse ved import (poolen er en tom kø), så importen er
ufarlig. De fem funktioner der VILLE røre databasen — cache.risiko, cache.navne,
cache.ejere, db_seneste_udfald og db_maanedens_udfald — byttes ud i afsnit 13 og
lægges tilbage igen bagefter.

Hedder ikke `test_*.py` af samme grund som naboen: `tests/conftest.py` peger
DB_SERVER på et hostnavn der ikke findes, og pytest er i øvrigt ikke installeret.

HVAD DEN IKKE DÆKKER, og det skal stå her frem for at blive opdaget: det fjerde
acceptkriterium — Novo Nordisk øverst med 477.789 kr. og 10 af 10 pladser — er
bundet til rigtige tal og kan kun køres mod produktion. Det samme gælder
zonefordelingen for 2026-07. Denne fil beviser reglerne, ikke tallene.
"""
import datetime as dt
import sys
from decimal import Decimal
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
# FORREST i sys.path, ikke bagerst. Ligger der en anden kopi af repoet i stien —
# og det gør der: worktrees under .claude/worktrees har deres egen
# `moduler`-pakke uden dette modul — så vinder den første. Stien regnes ud fra
# FILENS egen placering, så scriptet finder sin egen kode uanset hvor det køres
# fra, og uanset hvilken kopi man står i.
sys.path.insert(0, str(REPO_ROOT))

from moduler.modul_retention import cache  # noqa: E402
from moduler.modul_retention import outcomes as o  # noqa: E402
from moduler.modul_retention import prioritering as p  # noqa: E402
from moduler.modul_retention import queries as q  # noqa: E402
from moduler.modul_retention.zones import ZONE_ORDER, zone_alvor  # noqa: E402

I_DAG = dt.date(2026, 8, 11)

TORSDAG = dt.date(2026, 8, 6)
FREDAG = dt.date(2026, 8, 7)
LOERDAG = dt.date(2026, 8, 8)
MANDAG = dt.date(2026, 8, 10)

_fejl = []


def tjek(navn, faktisk, forventet=True):
    """Sammenligner og printer. Forventningen skrives ud ved fejl.

    Et bart "FEJL sortering" fortæller ikke hvad der kom i stedet, og det er
    præcis dét man skal bruge for at komme videre.
    """
    ok = faktisk == forventet
    print(("  OK   " if ok else "  FEJL ") + navn)
    if not ok:
        print(f"         fik {faktisk!r}, ventede {forventet!r}")
        _fejl.append(navn)


# ---------------------------------------------------------------- fikstur ---

def udfald(account="watch", org_id=2084, site="amwatch.dk",
           resultat=o.KONTAKT_OPNAAET, vejen_ud="fornyet", followup=None,
           fornyelse=None, created=dt.datetime(2026, 8, 10, 9, 0)):
    """Én række som db_seneste_udfald ville levere den.

    `org_id` er INT som default, og det er ikke tilfældigt: udfalds-rækkerne
    kommer råt fra en INT-kolonne, mens risiko-rækkerne bærer den som STRENG.
    Skriver fikstureren dem ens, forsvinder præcis den fejl testen findes for.
    """
    return {"account": account, "org_id": org_id, "site": site,
            "outcome_id": 1, "created_at": created, "contact_result": resultat,
            "outcome": vejen_ud, "followup_date": followup,
            "renewal_date": fornyelse}


def abo(account="watch", org_id="2084", site="amwatch.dk", zone="stoppet",
        score=1000.0, arr=1000.0, kunde_arr=50000.0, navn="Test A/S",
        mikro=False, opsagt=None):
    """Én række som abonnementer_i_risiko ville levere den. org_id som STRENG."""
    return {"account": account, "org_id": org_id, "site": site, "zone": zone,
            "score": score, "arr_dkk": arr, "kunde_arr_dkk": kunde_arr,
            "org_name": navn, "mikrokunde": mikro,
            "opsagt_dato": opsagt}


def kpi_raekke(cid, account="watch", org_id=2084, vejen_ud="fornyet",
               foer=None, efter=None):
    """Én række som db_maanedens_udfald ville levere den.

    Beløbene er `Decimal`, som pymssql leverer decimal-kolonner. Skrives de som
    float her, bevises additionen aldrig på den type der faktisk kommer ind.
    """
    return {"account": account, "org_id": org_id, "conversation_id": cid,
            "outcome": vejen_ud, "contact_result": o.KONTAKT_OPNAAET,
            "arr_before_dkk": foer, "arr_before_kilde": o.ARR_KILDE_BEKRAEFTET,
            "arr_after_dkk": efter,
            "contacted_at": dt.datetime(2026, 8, 5, 10, 0)}


# ------------------------------------------------------------------ 1: tid ---

print("--- 1: naeste_hverdag ---")
tjek("torsdag -> fredag", o.naeste_hverdag(TORSDAG), FREDAG)
tjek("fredag -> mandag, weekenden springes over", o.naeste_hverdag(FREDAG), MANDAG)
tjek("loerdag -> mandag", o.naeste_hverdag(LOERDAG), MANDAG)
tjek("kommer aldrig ud paa en weekend",
     all(o.naeste_hverdag(TORSDAG + dt.timedelta(days=i)).weekday() < 5
         for i in range(30)))

# Krogen selv: læg en vilkårlig fredag ind og se mandag komme ud.
_rigtige_helligdage = o.HELLIGDAGE
o.HELLIGDAGE = frozenset({FREDAG})
tjek("helligdag springes over som en weekend", o.naeste_hverdag(TORSDAG), MANDAG)
o.HELLIGDAGE = frozenset()
tjek("tom kasse giver ren hverdagsregning", o.naeste_hverdag(TORSDAG), FREDAG)
# Lægges den RIGTIGE kalender tilbage og ikke en tom mængde. Blev den stående
# tom, ville resten af filen måle en anden konfiguration end produktionens.
o.HELLIGDAGE = _rigtige_helligdage


print("--- 1b: den danske helligdagskalender ---")
# Påsken flytter sig, så syv af helligdagene kan ikke skrives i hånden. Ni
# kendte påskedatoer holder algoritmen fast.
for aar, forventet in [(2020, (4, 12)), (2023, (4, 9)), (2024, (3, 31)),
                       (2025, (4, 20)), (2026, (4, 5)), (2027, (3, 28)),
                       (2030, (4, 21))]:
    # IKKE `p` som variabelnavn: modulet er importeret som `p` ovenfor, og en
    # loekkevariabel paa modulniveau ville skygge det for resten af filen.
    paaske = o._paaskedag(aar)
    tjek(f"paaskedag {aar}", (paaske.month, paaske.day), forventet)

h26 = o.danske_helligdage(2026)
tjek("skaertorsdag 2026", dt.date(2026, 4, 2) in h26)
tjek("langfredag 2026", dt.date(2026, 4, 3) in h26)
tjek("2. paaskedag 2026", dt.date(2026, 4, 6) in h26)
tjek("Kristi himmelfart 2026", dt.date(2026, 5, 14) in h26)
tjek("2. pinsedag 2026", dt.date(2026, 5, 25) in h26)
tjek("juleaftensdag med (lukket i praksis)", dt.date(2026, 12, 24) in h26)
tjek("nytaarsaftensdag med", dt.date(2026, 12, 31) in h26)
# Grundlovsdag er bevidst UDE: en halv eller hel fridag afhaengigt af
# arbejdsplads, og vi ved ikke hvad der gaelder her.
tjek("grundlovsdag er IKKE med", dt.date(2026, 6, 5) not in h26)
# Afskaffet ved lov fra 2024. En kalender der stadig holdt den lukket, ville
# forsinke en opfoelgning et doegn hvert foraar.
tjek("store bededag med i 2023", dt.date(2023, 5, 5) in o.danske_helligdage(2023))
tjek("store bededag UDE i 2024",
     dt.date(2024, 4, 26) not in o.danske_helligdage(2024))

# Og saa det der faktisk betyder noget: springer fristerne over dem?
tjek("23/12 -> 28/12, fem dage sprunget",
     o.naeste_hverdag(dt.date(2026, 12, 23)), dt.date(2026, 12, 28))
tjek("30/12 -> 4/1 over aarsskiftet",
     o.naeste_hverdag(dt.date(2026, 12, 30)), dt.date(2027, 1, 4))
tjek("1/4 -> 7/4 over hele paasken",
     o.naeste_hverdag(dt.date(2026, 4, 1)), dt.date(2026, 4, 7))
tjek("kalenderen daekker hele spaendet",
     (min(o.HELLIGDAGE).year, max(o.HELLIGDAGE).year),
     (o.HELLIGDAGE_FOERSTE_AAR, o.HELLIGDAGE_SIDSTE_AAR))


print("--- 2: tilbage_paa_listen (Fristmodellen) ---")
tjek("ingen_kontakt -> naeste hverdag",
     o.tilbage_paa_listen(udfald(resultat=o.INGEN_KONTAKT, vejen_ud=None,
                                 created=dt.datetime(2026, 8, 6, 14, 30))),
     FREDAG)
tjek("ikke_kontaktbar -> 90 dage",
     o.tilbage_paa_listen(udfald(resultat=o.IKKE_KONTAKTBAR, vejen_ud=None,
                                 created=dt.datetime(2026, 8, 6, 14, 30))),
     TORSDAG + dt.timedelta(days=o.IKKE_KONTAKTBAR_DAGE))
tjek("ukendt kontaktresultat -> naeste hverdag, ikke ALDRIG",
     o.tilbage_paa_listen(udfald(resultat="noget_nyt", vejen_ud=None,
                                 created=dt.datetime(2026, 8, 6, 14, 30))),
     FREDAG)
tjek("opsagt -> ALDRIG", o.tilbage_paa_listen(udfald(vejen_ud="opsagt")), o.ALDRIG)
tjek("allerede_opsagt -> ALDRIG",
     o.tilbage_paa_listen(udfald(vejen_ud="allerede_opsagt")), o.ALDRIG)
tjek("forskudt -> followup_date",
     o.tilbage_paa_listen(udfald(vejen_ud="forskudt",
                                 followup=dt.date(2026, 9, 1))),
     dt.date(2026, 9, 1))
tjek("tilbud_sendt -> followup_date",
     o.tilbage_paa_listen(udfald(vejen_ud="tilbud_sendt",
                                 followup=dt.date(2026, 9, 2))),
     dt.date(2026, 9, 2))
tjek("forskudt UDEN followup -> STRAKS",
     o.tilbage_paa_listen(udfald(vejen_ud="forskudt")), o.STRAKS)
tjek("fornyet med fornyelsesdato -> 45 dage foer",
     o.tilbage_paa_listen(udfald(vejen_ud="fornyet",
                                 fornyelse=dt.date(2027, 3, 1))),
     dt.date(2027, 3, 1) - dt.timedelta(days=o.FORNYET_DAGE_FOER))
tjek("fornyet uden dato -> 180 dage fra created_at",
     o.tilbage_paa_listen(udfald(vejen_ud="fornyet")),
     dt.date(2026, 8, 10) + dt.timedelta(days=o.FORNYET_UDEN_DATO))
tjek("nedgraderet foelger fornyet",
     o.tilbage_paa_listen(udfald(vejen_ud="nedgraderet")),
     dt.date(2026, 8, 10) + dt.timedelta(days=o.FORNYET_UDEN_DATO))
# Hvad Specialisten kan registreres hul: 'opgraderet' findes ikke i
# CK_RetOut_outcome. Kommer den nogen sinde ind ad en anden vej, skal kunden
# VISES, ikke forsvinde lydloest.
tjek("ukendt udfald -> STRAKS",
     o.tilbage_paa_listen(udfald(vejen_ud="opgraderet")), o.STRAKS)
tjek("created_at mangler -> STRAKS",
     o.tilbage_paa_listen(udfald(created=None)), o.STRAKS)
tjek("created_at som ren date virker ogsaa",
     o.tilbage_paa_listen(udfald(vejen_ud="fornyet", created=TORSDAG)),
     TORSDAG + dt.timedelta(days=o.FORNYET_UDEN_DATO))
# Hele grunden til at kalderen har én sammenligning og ingen saertilfaelde.
tjek("returnerer ALTID en date, aldrig None",
     all(isinstance(o.tilbage_paa_listen(udfald(**k)), dt.date) for k in [
         {"vejen_ud": "opsagt"}, {"vejen_ud": "fornyet"},
         {"resultat": o.INGEN_KONTAKT, "vejen_ud": None}, {"created": None},
         {"vejen_ud": "vaerdi_ingen_har_set"}]))
tjek("ALDRIG er senere end enhver rigtig frist", o.ALDRIG > dt.date(2100, 1, 1))
tjek("STRAKS er tidligere end enhver rigtig dato", o.STRAKS < dt.date(1900, 1, 1))


print("--- 3: opfoelgninger, nu uden database ---")
# Fire abonnementer. Kun de to aabne med forfalden dato kalder.
seneste_3 = {
    ("watch", "1", "a.dk"): udfald(org_id=1, site="a.dk", vejen_ud="forskudt",
                                   followup=dt.date(2026, 8, 4)),
    ("watch", "2", "b.dk"): udfald(org_id=2, site="b.dk", vejen_ud="tilbud_sendt",
                                   followup=I_DAG),
    ("watch", "3", "c.dk"): udfald(org_id=3, site="c.dk", vejen_ud="tilbud_sendt",
                                   followup=dt.date(2026, 8, 20)),
    # Fornyet, men den GAMLE followup_date ligger stadig paa raekken. Den maa
    # ikke kalde nogen til handling — det er hele grunden til at der filtreres
    # paa outcome og ikke bare paa "har en dato".
    ("watch", "4", "d.dk"): udfald(org_id=4, site="d.dk", vejen_ud="fornyet",
                                   followup=dt.date(2026, 8, 4)),
}
kalder = o.opfoelgninger(seneste_3, I_DAG)
tjek("kun aabne udfald med forfalden dato",
     sorted(r["site"] for r in kalder), ["a.dk", "b.dk"])
tjek("dagens egen dato er MED (<= og ikke <)",
     any(r["site"] == "b.dk" for r in kalder))
tjek("fremtidig opfoelgning kalder ikke endnu",
     all(r["site"] != "c.dk" for r in kalder))
tjek("fornyet med gammel dato kalder ikke",
     all(r["site"] != "d.dk" for r in kalder))
tjek("overskreden fra i gaar forsvinder ikke i morgen",
     len(o.opfoelgninger(seneste_3, dt.date(2026, 8, 20))), 3)


print("--- 4: zone_alvor ---")
tjek("stoppet er vaerst", zone_alvor("stoppet"), 0)
tjek("stoppet foer laenge_tavs", zone_alvor("stoppet") < zone_alvor("laenge_tavs"))
# De to deler vaegt 0,50, og netop derfor skal alvoren kunne skille dem.
tjek("laenge_tavs foer aldrig_i_brug trods samme vaegt",
     zone_alvor("laenge_tavs") < zone_alvor("aldrig_i_brug"))
tjek("intet_signal er sidst af de kendte",
     zone_alvor("intet_signal"), len(ZONE_ORDER) - 1)
tjek("ukendt zone ligger EFTER alle kendte",
     zone_alvor("noget_nyt"), len(ZONE_ORDER))
tjek("ukendt zone kaster ikke", isinstance(zone_alvor(None), int))


print("--- 5: kunde_noegle, broen mellem int og str ---")
tjek("int coerces til str",
     p.kunde_noegle({"account": "watch", "org_id": 2084}), ("watch", "2084"))
tjek("str er uroert",
     p.kunde_noegle({"account": "watch", "org_id": "2084"}), ("watch", "2084"))
# Den fejl der aldrig kaster: et opslag paa (account, '6779') i en ordbog
# noeglet med (account, ' 6779') rammer ikke, og siden viser bare forkert.
tjek("mellemrum strippes i BEGGE led",
     p.kunde_noegle({"account": " watch ", "org_id": " 2084 "}),
     ("watch", "2084"))
tjek("int og str giver SAMME noegle",
     p.kunde_noegle({"account": "watch", "org_id": 2084})
     == p.kunde_noegle({"account": "watch", "org_id": "2084"}))


print("--- 6: fold_opfoelgninger ---")
raekker_6 = [
    udfald(org_id=2084, site="amwatch.dk", vejen_ud="forskudt", followup=I_DAG),
    udfald(org_id=2084, site="agriwatch.dk", vejen_ud="tilbud_sendt",
           followup=dt.date(2026, 8, 4)),
    udfald(account="monitor", org_id=991, site="kommunen.dk",
           vejen_ud="forskudt", followup=I_DAG),
    udfald(org_id=7777, site="medwatch.dk", vejen_ud="forskudt",
           followup=dt.date(2026, 8, 10)),
]
# Navnene er noeglet med org_id som STRENG, raekkerne baerer den som INT.
navne_6 = {("watch", "2084"): "Jyske Bank A/S",
           ("monitor", "991"): "Aarhus Kommune"}
poster_6 = p.fold_opfoelgninger(raekker_6, I_DAG, navne_6)

tjek("foldet pr. kunde, ikke pr. abonnement", len(poster_6), 3)
tjek("aeldste dato vinder inde i posten",
     poster_6[0]["aeldste_opfoelgning"], dt.date(2026, 8, 4))
tjek("int/str-broen rammer navnet", poster_6[0]["org_name"], "Jyske Bank A/S")
tjek("overskreden aftale markeret", poster_6[0]["overskredet"], True)
tjek("dagsaktuel aftale er IKKE overskredet", poster_6[2]["overskredet"], False)
tjek("ukendt navn vaelter ikke sorteringen", poster_6[1]["org_name"], None)
tjek("abonnementerne sorteret i posten",
     poster_6[0]["abonnementer"][0]["site"], "agriwatch.dk")
tjek("aeldste kunde foerst",
     [x["aeldste_opfoelgning"] for x in poster_6],
     sorted(x["aeldste_opfoelgning"] for x in poster_6))

# En `datetime` her ville sammenlignes med en `date` og rejse TypeError foerste
# gang der FINDES en opfoelgning — altsaa i produktion og aldrig hos os.
try:
    p.fold_opfoelgninger(raekker_6, dt.datetime(2026, 8, 11), navne_6)
    tjek("datetime som i_dag rejser TypeError", "ingen fejl", "TypeError")
except TypeError:
    tjek("datetime som i_dag rejser TypeError", True)


print("--- 7: fold_risici, filtrene fra Prioriteringsmodellen ---")
tjek("sund frasorteres", len(p.fold_risici([abo(zone="sund")], {}, I_DAG)), 0)
tjek("mikrokunde frasorteres", len(p.fold_risici([abo(mikro=True)], {}, I_DAG)), 0)
tjek("uden udfald er abonnementet med", len(p.fold_risici([abo()], {}, I_DAG)), 1)
# fornyet 2026-08-10 giver +180 dage, altsaa langt ude i fremtiden.
tjek("uudloebet udsaettelse frasorteres (filter 3+4)",
     len(p.fold_risici([abo()], {("watch", "2084", "amwatch.dk"): udfald()},
                       I_DAG)), 0)
tjek("udloebet udsaettelse er med igen",
     len(p.fold_risici([abo()],
                       {("watch", "2084", "amwatch.dk"):
                        udfald(created=dt.datetime(2025, 1, 1, 9, 0))},
                       I_DAG)), 1)


# Landeafgraensningen flyttede til SQL 2026-08-25 (queries._KUN_DANSKE), saa
# fold_risici filtrerer ikke laengere paa account. En syntetisk watch_no-raekke
# kan derfor ikke bevise noget her; garantien flytter med til fragmentet.
tjek("de tre udenlandske accounts staar i SQL-filteret",
     all(a in q._KUN_DANSKE for a in q.UDENLANDSKE_ACCOUNTS), True)
tjek("de danske accounts staar IKKE i SQL-filteret",
     any(a in q._KUN_DANSKE for a in ("watch_medier", "monitor", "marketwire")),
     False)
tjek("opsagt abonnement frasorteres",
     len(p.fold_risici([abo(opsagt="2026-10-31")], {}, I_DAG)), 0)
# En kunde med ét opsagt og ét levende abonnement skal BLIVE paa listen, men
# posten maa kun indeholde det levende. Ellers taeller det opsagte med i
# "scoren daekker X af Y", og tallet bliver forkert.
blandet = p.fold_risici([abo(site="amwatch.dk", opsagt="2026-10-31"),
                         abo(site="finanswatch.dk")], {}, I_DAG)
tjek("kunde med ét opsagt og ét levende bliver paa listen", len(blandet), 1)
tjek("kun det levende abonnement er i posten",
     blandet[0]["antal_abonnementer"], 1)
tjek("og det er det rigtige", blandet[0]["abonnementer"][0]["site"],
     "finanswatch.dk")
# En dato i FORTIDEN frasorteres ogsaa. Aftalen er slut, saa der er ikke
# engang et varsel at ringe indenfor.
tjek("opsagt med dato i fortiden frasorteres ogsaa",
     len(p.fold_risici([abo(opsagt="2024-01-31")], {}, I_DAG)), 0)


print("--- 8: site-sentinelen ---")
# dbo.retention.sites er NULL for marketwires raekker, og en noegle med NULL i
# kan aldrig slaas op igen. Glemmes `or INTET_SITE`, finder marketwire aldrig
# sit eget udfald — og dukker op paa listen dagen efter opkaldet, uden at noget
# fejler.
mw_abo = abo(account="marketwire", org_id="982", site=None, navn="MW A/S")
tjek("NULL-site finder sit udfald under sentinelen",
     len(p.fold_risici([mw_abo],
                       {("marketwire", "982", o.INTET_SITE):
                        udfald(account="marketwire", org_id=982, site=None)},
                       I_DAG)), 0)
tjek("uden sentinelen ville den slippe igennem",
     len(p.fold_risici([mw_abo], {("marketwire", "982", None): udfald()},
                       I_DAG)), 1)


print("--- 9: fold_risici, foldning og sortering ---")
poster_9 = p.fold_risici([abo(site="a.dk", zone="faldende", score=100.0),
                          abo(site="b.dk", zone="stoppet", score=300.0),
                          abo(site="c.dk", zone="laenge_tavs", score=200.0,
                              arr=None)], {}, I_DAG)
tjek("tre abonnementer bliver én kunde", len(poster_9), 1)
tjek("scoren summeres", poster_9[0]["score"], 600.0)
tjek("vaerste zone vinder", poster_9[0]["vaerste_zone"], "stoppet")
tjek("antal abonnementer talt", poster_9[0]["antal_abonnementer"], 3)
# To tal og ikke én boolean: siden skal kunne sige "scoren daekker 2 af 3".
tjek("antal med kendt ARR talt for sig", poster_9[0]["abonnementer_med_arr"], 2)
tjek("abonnementerne sorteret som paa detaljesiden",
     [a["site"] for a in poster_9[0]["abonnementer"]], ["b.dk", "c.dk", "a.dk"])

tjek("hoejeste score foerst",
     [x["org_name"] for x in p.fold_risici(
         [abo(org_id="1", score=10.0, navn="Lav"),
          abo(org_id="2", score=99.0, navn="Hoej")], {}, I_DAG)],
     ["Hoej", "Lav"])
# Praecis samme score OG samme kunde-ARR. Kun zonen kan skille dem, og uden
# zone_alvor afgjorde Pythons stabile sort det paa den raekkefoelge de kom i.
tjek("alvoren bryder uafgjort",
     [x["org_name"] for x in p.fold_risici(
         [abo(org_id="1", site="x.dk", zone="laenge_tavs", score=500.0,
              navn="Tavs A/S"),
          abo(org_id="2", site="y.dk", zone="stoppet", score=500.0,
              navn="Stoppet A/S")], {}, I_DAG)],
     ["Stoppet A/S", "Tavs A/S"])
# Summen er aldrig None, heller ikke naar alle led mangler ARR.
tjek("kunde uden kendt ARR summerer til 0,0 og vaelter ikke sorteringen",
     p.fold_risici([abo(score=None, arr=None, kunde_arr=None)], {},
                   I_DAG)[0]["score"], 0.0)


print("--- 10: antal_aabne_sager (loftets taeller) ---")
# Kun forskudt og tilbud_sendt. En fornyet kunde med 180 dages udsaettelse
# skylder ingen noget — talte hun med, ville loftet lukke for nye risici inden
# for en uge, netop fordi specialisten havde gjort sit arbejde godt.
tjek("kun forskudt og tilbud_sendt taeller",
     p.antal_aabne_sager({
         ("w", 1, "a"): udfald(org_id=1, vejen_ud="fornyet"),
         ("w", 2, "b"): udfald(org_id=2, vejen_ud="forskudt", followup=I_DAG),
         ("w", 3, "c"): udfald(org_id=3, vejen_ud="tilbud_sendt", followup=I_DAG),
         ("w", 4, "d"): udfald(org_id=4, vejen_ud="opsagt"),
     }), 2)
# Uden foldningen taeller Novo Nordisk som fire sager, og loftet binder cirka en
# tredjedel for tidligt (7.044 abonnementer paa 5.320 kunder, faktor 1,32).
tjek("taelles pr. KUNDE, ikke pr. abonnement",
     p.antal_aabne_sager({
         ("w", 7, "a"): udfald(org_id=7, vejen_ud="forskudt", followup=I_DAG),
         ("w", 7, "b"): udfald(org_id=7, vejen_ud="forskudt", followup=I_DAG),
         ("w", 7, "c"): udfald(org_id=7, vejen_ud="tilbud_sendt", followup=I_DAG),
     }), 1)
tjek("tom ordbog giver nul sager", p.antal_aabne_sager({}), 0)


print("--- 11: afkort_nye_risici og de fire aarsager ---")
mange = p.fold_risici([abo(org_id=str(i), score=float(100 - i), navn=f"K{i}")
                       for i in range(20)], {}, I_DAG)
tjek("20 kunder foldet", len(mange), 20)

r = p.afkort_nye_risici(mange, antal_opfoelgninger=0, aabne_sager=3)
tjek("ingen opfoelgninger -> ti poster", len(r["poster"]), p.LISTE_LAENGDE)
tjek("ingen aarsag naar listen er fuld", r["aarsag"], None)
tjek("plads rapporteret", r["plads"], p.LISTE_LAENGDE)
# Baeres ALTID med, ikke kun naar loftet binder: findes tallet kun i det oejeblik
# vaeggen rammes, kan Målingside aldrig se om det rigtige loft var 25 eller 90.
tjek("aabne sager baeres med selv om loftet ikke binder", r["aabne_sager"], 3)

tjek("seks opfoelgninger -> fire nye",
     len(p.afkort_nye_risici(mange, 6, 3)["poster"]), 4)
r = p.afkort_nye_risici(mange, antal_opfoelgninger=12, aabne_sager=3)
tjek("tolv opfoelgninger -> ingen nye", len(r["poster"]), 0)
tjek("aarsag: opfoelgninger fylder", r["aarsag"], "opfoelgninger_fylder")

r = p.afkort_nye_risici(mange, 0, p.MAKS_AABNE_SAGER)
tjek("loftet binder VED graensen, ikke over den", r["aarsag"], "loft")
tjek("loftet giver ingen poster", len(r["poster"]), 0)
tjek("under graensen binder ikke",
     p.afkort_nye_risici(mange, 0, p.MAKS_AABNE_SAGER - 1)["aarsag"], None)
# Loftet gaar FORUD for pladsregnestykket: det er en haard port (Arbejdsgang).
tjek("loftet vinder over opfoelgninger_fylder",
     p.afkort_nye_risici(mange, 12, p.MAKS_AABNE_SAGER)["aarsag"], "loft")

r = p.afkort_nye_risici(mange[:3], 0, 1)
tjek("faerre kandidater end pladser -> tom_bunke", r["aarsag"], "tom_bunke")
tjek("de tre vises alligevel", len(r["poster"]), 3)

# Den mest beroligende besked oven paa den mest oedelagte tilstand siden kan
# vaere i. Uden dette flag ville et forkert teamnavn svare "du er igennem
# bunken", mens 5.033 kunder stod uroert.
r = p.afkort_nye_risici([], 0, 0, afgraensning_tom=True)
tjek("tom afgraensning siger IKKE tom_bunke", r["aarsag"], "afgraensning_tom")
tjek("afgraensning_tom gaar FORUD for loftet",
     p.afkort_nye_risici([], 0, p.MAKS_AABNE_SAGER,
                         afgraensning_tom=True)["aarsag"], "afgraensning_tom")


print("--- 12: maanedens_kpier ---")
kpi_raekker = [
    kpi_raekke(1, vejen_ud="fornyet", foer=Decimal("100000"),
               efter=Decimal("105000")),
    kpi_raekke(1, vejen_ud="nedgraderet", foer=Decimal("50000"),
               efter=Decimal("30000")),
    kpi_raekke(2, vejen_ud="opsagt", foer=Decimal("80000")),
    kpi_raekke(3, vejen_ud="allerede_opsagt", foer=Decimal("20000")),
    kpi_raekke(4, vejen_ud="forskudt", foer=Decimal("60000")),
    kpi_raekke(5, vejen_ud="fornyet", foer=Decimal("40000"), efter=None),
]
k = p.maanedens_kpier(kpi_raekker, None)
tjek("reddet = 105.000 + 30.000", k["reddet"], 135000.0)
tjek("tabt = 80.000 + 20.000", k["tabt"], 100000.0)
# To udfald paa samtale 1 er ÉT opkald, ikke to.
tjek("samtaler taelles DISTINKT", k["samtaler"], 5)
# Uden dette tal lyver "kroner reddet": en fornyelse uden beloeb taeller som 0 kr.
tjek("fornyelse uden beloeb taelles for sig", k["reddet_uden_beloeb"], 1)
tjek("forskudt bidrager hverken til reddet eller tabt",
     k["reddet"] + k["tabt"], 235000.0)

blandet = kpi_raekker + [
    kpi_raekke(9, account="monitor", org_id=991, vejen_ud="fornyet",
               foer=Decimal("9000"), efter=Decimal("9000")),
    kpi_raekke(9, account="monitor", org_id=991, vejen_ud="opsagt",
               foer=Decimal("7000")),
]
tjek("uden afgraensning er monitor med",
     p.maanedens_kpier(blandet, None)["reddet"], 144000.0)
# Samme afgraensning som listerne nedenunder paa siden. Var de to forskellige,
# ville tallene oeverst beskrive en anden gruppe kunder end raekkerne under dem.
kun_watch = p.maanedens_kpier(blandet, {("watch", "2084")})
tjek("afgraenset: monitor ude af reddet", kun_watch["reddet"], 135000.0)
tjek("afgraenset: monitor ude af tabt", kun_watch["tabt"], 100000.0)
tjek("afgraenset: monitor ude af samtaler", kun_watch["samtaler"], 5)
tom = p.maanedens_kpier(blandet, set())
tjek("tom tilladelsesmaengde giver nul", (tom["reddet"], tom["samtaler"]), (0, 0))
nul = p.maanedens_kpier([], None)
tjek("ingen raekker giver tre nuller",
     (nul["reddet"], nul["tabt"], nul["samtaler"]), (0, 0.0, 0))


print("--- 13: prioriteringsdata, hele laget uden database ---")
# De fem funktioner der ellers ville roere databasen byttes ud. cache.X naas
# gennem modulet, mens db_seneste_udfald og db_maanedens_udfald er importeret
# ind i prioriteringens EGET navnerum og derfor skal byttes DER.
_oprindelig = (cache.risiko, cache.navne, cache.ejere,
               p.db_seneste_udfald, p.db_maanedens_udfald)

# Kunde A staar paa liste 1 med en opfoelgning der forfalder I DAG — og er
# samtidig i risiko. Filter 3 fjerner kun opfoelgninger i FREMTIDEN, saa uden
# udelukkelsen ville hun staa paa begge lister og blive talt to gange: ti
# raekker, ni opkald.
A = ("watch", "2084")
risiko_rows = [abo(org_id=str(i), site=f"s{i}.dk", score=float(100 - i),
                   navn=f"K{i}") for i in range(1, 13)]
risiko_rows.append(abo(org_id="2084", site="amwatch.dk", score=500.0,
                       navn="Jyske Bank A/S"))
seneste_13 = {
    ("watch", "2084", "amwatch.dk"): udfald(org_id=2084, vejen_ud="forskudt",
                                            followup=I_DAG,
                                            created=dt.datetime(2026, 8, 1, 9, 0)),
    # K3 er lige fornyet: udsat 180 dage og skal ikke staa som ny risiko.
    ("watch", "3", "s3.dk"): udfald(org_id=3, site="s3.dk", vejen_ud="fornyet"),
}

cache.navne = lambda: {A: "Jyske Bank A/S"}
p.db_seneste_udfald = lambda: dict(seneste_13)
p.db_maanedens_udfald = lambda maaned: kpi_raekker


def saet_afgraensning(tilladte):
    """Saet BEGGE kilder ud fra samme maengde kunder.

    TO VEJE IND, og det er let at overse: `tilladte` fra cache.ejere afgraenser
    kun `seneste` og KPI'erne, mens liste 2's raekker afgraenses i SQL'en inde i
    abonnementer_i_risiko(teams=...). Prioriteringen laeser dem som ét billede
    og forudsaetter, at de to er afgraenset ens — "saa de to kan ikke komme i
    utakt", som prioriteringsdata selv skriver.

    Derfor er de her afledt af ÉN maengde. En attrap hvor risikoraekkerne var
    ufiltrerede ville ikke afsloere en fejl, den ville opfinde en: foerste
    udgave af denne test lod cache.risiko ignorere `teams` og paastod saa, at
    koden ikke afgraensede liste 2. Det gjorde den; stubben gjorde ikke.
    """
    cache.ejere = lambda teams: {n: {} for n in tilladte}
    cache.risiko = lambda teams, abo_maaned: {
        "rows": [r for r in risiko_rows
                 if not teams or p.kunde_noegle(r) in tilladte],
        "meta": {"reference_maaned": "2026-07"}}


try:
    # Ingen teams: cache.ejere svarer tomt, men `tilladte` bliver None og
    # afgraenser derfor ingenting. Tom maengde og ingen afgraensning er to
    # forskellige ting, og siden skal svare forskelligt paa dem.
    saet_afgraensning(set())
    d = p.prioriteringsdata(I_DAG)

    # TO MAANEDER, og det er ikke en fejl: KPI'erne er indevaerende maaned,
    # zonerne sidste HELE. Skriver siden dem ikke begge, spoerger nogen hver
    # maaned hvorfor "kroner reddet" er lille mens risikolisten er lang.
    tjek("KPI-maaneden er indevaerende", d["maaned"], "2026-08")
    tjek("referencemaaneden er sidste hele", d["reference_maaned"], "2026-07")
    tjek("de to maaneder ER forskellige", d["maaned"] != d["reference_maaned"])
    tjek("meta baeres med til forbeholdet i 7.2", "reference_maaned" in d["meta"])

    nr = d["nye_risici"]
    tjek("liste 1 har den ene kunde med forfalden aftale",
         [x["org_name"] for x in d["opfoelgninger"]], ["Jyske Bank A/S"])
    tjek("dagsaktuel aftale er ikke markeret overskredet",
         d["opfoelgninger"][0]["overskredet"], False)
    tjek("kunden paa liste 1 er UDE af liste 2",
         all((x["account"], x["org_id"]) != A for x in nr["poster"]))
    tjek("udsat kunde (K3) er ude af liste 2",
         all(x["org_name"] != "K3" for x in nr["poster"]))
    tjek("pladsen er ti minus opfoelgningerne", nr["plads"], p.LISTE_LAENGDE - 1)
    tjek("listen er fyldt op til pladsen", len(nr["poster"]), p.LISTE_LAENGDE - 1)
    tjek("ingen aarsag naar listen er fuld", nr["aarsag"], None)
    tjek("den aabne opfoelgning taelles som én sag", nr["aabne_sager"], 1)
    tjek("KPI'erne kommer igennem uafgraenset", d["kpier"]["reddet"], 135000.0)
    tjek("ingen kunde staar paa BEGGE lister",
         not ({(x["account"], x["org_id"]) for x in d["opfoelgninger"]}
              & {(x["account"], x["org_id"]) for x in nr["poster"]}))
    # Uden teams er der INGEN begraensning — ikke "kun kunder med en ACV-raekke".
    # cache.ejere svarer {} ovenfor, og listen er alligevel fuld.
    tjek("uden teams beholdes kunder uden ACV-raekke", len(nr["poster"]) > 0)

    print("  ..med en afgraensning der matcher ingenting:")
    forkert = p.prioriteringsdata(I_DAG, teams=["Watch DK"])  # hedder "Team Watch DK"
    tjek("forkert teamnavn siger afgraensning_tom",
         forkert["nye_risici"]["aarsag"], "afgraensning_tom")
    tjek("og viser ingen poster", len(forkert["nye_risici"]["poster"]), 0)
    tjek("KPI'erne er ogsaa tomme, ikke hele firmaets",
         forkert["kpier"]["reddet"], 0)

    print("  ..med en afgraensning der rammer to kunder:")
    saet_afgraensning({("watch", "1"), ("watch", "2")})
    smal = p.prioriteringsdata(I_DAG, teams=["Team Watch DK"])
    tjek("kun de to tilladte kunder er med",
         [x["org_name"] for x in smal["nye_risici"]["poster"]], ["K1", "K2"])
    tjek("faerre kandidater end pladser siger tom_bunke",
         smal["nye_risici"]["aarsag"], "tom_bunke")
    # Den skarpe: samme fikstur, to forskellige svar. Var de ens, kunne siden
    # ikke skelne "du er faerdig" fra "din adgang er sat forkert op".
    tjek("tom_bunke og afgraensning_tom er IKKE samme svar",
         smal["nye_risici"]["aarsag"] != forkert["nye_risici"]["aarsag"])
    tjek("afgraensningen slaar ogsaa igennem paa liste 1",
         len(smal["opfoelgninger"]), 0)
    tjek("og dermed paa loftets taeller", smal["nye_risici"]["aabne_sager"], 0)
finally:
    # Laeg dem tilbage. Et modul der bliver liggende med en attrap er en faelde
    # for enhver, der importerer denne fil i stedet for at koere den.
    (cache.risiko, cache.navne, cache.ejere,
     p.db_seneste_udfald, p.db_maanedens_udfald) = _oprindelig


print()
if _fejl:
    print(f"FEJLEDE ({len(_fejl)}): " + ", ".join(_fejl))
    sys.exit(1)
print("ALT GRØNT")
sys.exit(0)

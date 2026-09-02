"""Røgtest af Performance-fanens datalag (effekt.py) UDEN database.

    .venv/Scripts/python.exe moduler/modul_retention/roegtest_effekt.py

Beviser MIN-reglen i saml_traefsikkerhed og grupperingen i saml_effekt mod
opdigtede ordbøger, samme disciplin som roegtest_varsel.py. Datalaget er rene
funktioner over lister og ordbøger, og prisen for det er at reglerne skal
bevises et sted. Det er denne fil.

DEN VIGTIGSTE TEST ER TILFÆLDE 3. Målt 2026-09-02 er won_time registreret EFTER
ophørsdatoen i 7,5 % af alle opsigelser, op til 160 dage. Brugte splittet
won_time alene, ville et opkald til en kunde der var væk tælle som "i tide" i
netop de tilfælde. MIN lukker hullet, og tilfælde 3 er beviset.

Hedder ikke `test_*.py` af samme grund som naboerne: tests/conftest.py peger
DB_SERVER på et hostnavn der ikke findes, og scriptet her åbner slet ingen
forbindelse.
"""
import datetime as dt
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
# FORREST i sys.path, ikke bagerst — samme begrundelse som
# roegtest_varsel.py: en worktree har sin egen `moduler`-pakke uden denne fil,
# og uden insert(0, ...) ville den forkerte kopi vinde.
sys.path.insert(0, str(REPO_ROOT))

from moduler.modul_retention.effekt import (  # noqa: E402
    saml_effekt, saml_traefsikkerhed, talt_foerste)
from moduler.modul_retention.outcomes import INTET_SITE  # noqa: E402

_fejl = []


def tjek(navn, faktisk, forventet):
    ok = faktisk == forventet
    print(("  OK   " if ok else "  FEJL ") + navn
          + ("" if ok else f"  (fik {faktisk!r}, forventede {forventet!r})"))
    if not ok:
        _fejl.append(navn)


def abo(account, org_id, sites, arr=None, org_name="Test A/S"):
    """Række som queries.abonnementer_med_ejer leverer den. org_id er STRENG."""
    return {"account": account, "org_id": str(org_id), "org_name": org_name,
            "sites": sites, "arr_pr_abonnement": arr, "owner_name": "Ejer"}


def ops(besluttet, ophoer):
    return {"besluttet": besluttet, "ophoer": ophoer}


def udf(account, org_id, site, dato, outcome="fornyet",
        contact_result="kontakt_opnaaet", conv=1, foer=None, efter=None):
    """Række som outcomes.db_alle_udfald leverer den. org_id er INT, dato datetime."""
    return {"account": account, "org_id": int(org_id), "site": site,
            "conversation_id": conv, "outcome": outcome,
            "contact_result": contact_result,
            "arr_before_dkk": foer, "arr_before_kilde": None,
            "arr_after_dkk": efter, "followup_date": None,
            "contacted_at": dt.datetime.fromisoformat(dato)}


print("── talt_foerste ──")

# ── 1. org_id kommer som INT fra udfaldene og STRENG fra abonnementerne. ──
_t1 = talt_foerste([udf("watch_medier", 2770, "AgriWatch DK", "2026-06-11T11:00")])
tjek("nøglens org_id er en STRENG efter customer_key",
     list(_t1.keys()), [("watch_medier", "2770", "AgriWatch DK")])

# ── 2. FØRSTE samtale vinder, ikke den seneste, og rækkefølgen ind er ligegyldig. ──
_t2 = talt_foerste([
    udf("watch_medier", 1, "a.dk", "2026-07-20T09:00", conv=2),
    udf("watch_medier", 1, "a.dk", "2026-03-04T09:00", conv=1),
])
tjek("den FØRSTE samtale vinder uanset rækkefølge",
     _t2[("watch_medier", "1", "a.dk")], "2026-03-04")

# ── 3. To sites hos samme kunde er to abonnementer, ikke ét. ──
_t3 = talt_foerste([
    udf("watch_medier", 1, "a.dk", "2026-05-01T09:00", conv=1),
    udf("watch_medier", 1, "b.dk", "2026-08-01T09:00", conv=1),
])
tjek("to sites hos samme kunde holdes adskilt", len(_t3), 2)


print()
print("── saml_traefsikkerhed: MIN-reglen ──")

# ── 1. Normaltilfældet (73,9 %): besluttet FØR ophør. MIN giver besluttet,
#      og en samtale i varslet er FOR SENT, ikke i tide. ──
_a = [abo("watch_medier", 1, "a.dk", arr=1000.0)]
_o = {("watch_medier", "1", "a.dk"): ops("2026-08-05", "2026-08-31")}
_s = {("watch_medier", "1", "a.dk"): "2026-08-20"}   # midt i varslet
_r = saml_traefsikkerhed(_a, _o, _s, "2026-08")
tjek("samtale i varslet er FOR SENT", (_r["i_tide"], _r["for_sent"]), (0, 1))
tjek("... kendsgerningen er won_time", _r["raekker"][0]["kendsgerning"], "2026-08-05")
tjek("... og dage er positiv", _r["raekker"][0]["dage"], 15)

# ── 2. Samtale FØR besluttet er i tide, og dage er negativ. ──
_s2 = {("watch_medier", "1", "a.dk"): "2026-07-29"}
_r2 = saml_traefsikkerhed(_a, _o, _s2, "2026-08")
tjek("samtale før beslutningen er I TIDE", (_r2["i_tide"], _r2["for_sent"]), (1, 0))
tjek("... og dage er negativ", _r2["raekker"][0]["dage"], -7)

# ── 3. DE 7,5 %: besluttet EFTER ophør (bagudregistreret). MIN skal give
#      OPHØRET, ellers ville en samtale efter kundens exit tælle som i tide. ──
_o3 = {("watch_medier", "1", "a.dk"): ops("2026-11-20", "2026-08-16")}
_s3 = {("watch_medier", "1", "a.dk"): "2026-09-10"}   # efter ophør, før won_time
_r3 = saml_traefsikkerhed(_a, _o3, _s3, "2026-08")
tjek("bagudregistreret: MIN giver ophøret, ikke won_time",
     _r3["raekker"][0]["kendsgerning"], "2026-08-16")
tjek("... så samtalen er FOR SENT og ikke i tide",
     (_r3["i_tide"], _r3["for_sent"]), (0, 1))

# ── 4. Samtale PRÆCIS på kendsgerningsdatoen er i tide (`<=`). ──
_s4 = {("watch_medier", "1", "a.dk"): "2026-08-05"}
_r4 = saml_traefsikkerhed(_a, _o, _s4, "2026-08")
tjek("samtale præcis på datoen er I TIDE", _r4["i_tide"], 1)
tjek("... og dage er nul", _r4["raekker"][0]["dage"], 0)

# ── 5. Ingen samtale er ALDRIG TALT MED, og må ikke havne i for sent. ──
_r5 = saml_traefsikkerhed(_a, _o, {}, "2026-08")
tjek("ingen samtale giver aldrig", (_r5["i_tide"], _r5["for_sent"], _r5["aldrig"]),
     (0, 0, 1))
tjek("... og den står ikke i tabellen", _r5["raekker"], [])

# ── 6. Populationen afgrænses på OPHØRSMÅNEDEN, ikke på beslutningsmåneden.
#      Her er beslutningen i august, men ophøret i oktober: uden for måneden. ──
_o6 = {("watch_medier", "1", "a.dk"): ops("2026-08-05", "2026-10-31")}
_r6 = saml_traefsikkerhed(_a, _o6, _s, "2026-08")
tjek("ophør uden for måneden tælles ikke med", _r6["i_alt"], 0)

# ── 7. Abonnement UDEN opsigelse forsvinder tavst (langt de fleste). ──
_a7 = [abo("watch_medier", 1, "a.dk", arr=1000.0),
       abo("watch_medier", 2, "b.dk", arr=500.0)]
_o7 = {("watch_medier", "2", "b.dk"): ops("2026-08-05", "2026-08-31")}
_r7 = saml_traefsikkerhed(_a7, _o7, {}, "2026-08")
tjek("abonnement uden opsigelse tælles ikke med", _r7["i_alt"], 1)

# ── 8. marketwire: sites=None matcher opsigelsen på RÅ nøgle, men samtalen på
#      INTET_SITE. Blandes de to vokabularer, kan rækken ikke slås op. ──
_a8 = [abo("marketwire", 5, None, arr=None)]
_o8 = {("marketwire", "5", None): ops("2026-08-05", "2026-08-31")}
_s8 = {("marketwire", "5", INTET_SITE): "2026-07-01"}
_r8 = saml_traefsikkerhed(_a8, _o8, _s8, "2026-08")
tjek("sites=None slås op på rå nøgle og INTET_SITE hver sit sted",
     _r8["i_tide"], 1)
tjek("... og vises med sentinellen som sitenavn",
     _r8["raekker"][0]["site"], INTET_SITE)

# ── 9. De tre spande summerer til i_alt, altid. ──
_a9 = [abo("watch_medier", i, "a.dk", arr=100.0) for i in range(1, 6)]
_o9 = {("watch_medier", str(i), "a.dk"): ops("2026-08-05", "2026-08-31")
       for i in range(1, 6)}
_s9 = {("watch_medier", "1", "a.dk"): "2026-07-01",    # i tide
       ("watch_medier", "2", "a.dk"): "2026-08-20"}    # for sent
_r9 = saml_traefsikkerhed(_a9, _o9, _s9, "2026-08")
tjek("de tre spande summerer til i_alt",
     _r9["i_tide"] + _r9["for_sent"] + _r9["aldrig"], _r9["i_alt"])
tjek("... og i_alt er hele populationen", _r9["i_alt"], 5)
tjek("kun de talte står i tabellen", len(_r9["raekker"]), 2)
tjek("i tide står FØRST i tabellen", _r9["raekker"][0]["org_id"], "1")

# ── 10. Manglende ARR tælles i uden_arr og ikke som nul kroner. ──
_a10 = [abo("watch_medier", 1, "a.dk", arr=None),
        abo("watch_medier", 2, "a.dk", arr=2000.0)]
_o10 = {("watch_medier", "1", "a.dk"): ops("2026-08-05", "2026-08-31"),
        ("watch_medier", "2", "a.dk"): ops("2026-08-05", "2026-08-31")}
_s10 = {("watch_medier", "1", "a.dk"): "2026-08-20",
        ("watch_medier", "2", "a.dk"): "2026-08-20"}
_r10 = saml_traefsikkerhed(_a10, _o10, _s10, "2026-08")
tjek("kun det kendte beløb indgår i arr_for_sent", _r10["arr_for_sent"], 2000.0)
tjek("den ukendte tælles i uden_arr", _r10["uden_arr"], 1)


print()
print("── saml_effekt ──")

# ── 1. Måneden går på contacted_at. Et opkald 31. juli hører i juli. ──
_u1 = [udf("watch_medier", 1, "a.dk", "2026-07-31T23:30", foer=100.0, efter=100.0)]
_e1 = saml_effekt(_u1, None, "2026-07")
tjek("opkald 31. juli kl. 23:30 hører i juli",
     [s["maaned"] for s in _e1["serie"]], ["2026-07"])

# ── 2. Serien er kronologisk — rækkefølgen er selve budskabet i grafen. ──
_u2 = [udf("watch_medier", 1, "a.dk", "2026-08-01T09:00", conv=3, foer=1.0, efter=1.0),
       udf("watch_medier", 2, "b.dk", "2026-06-01T09:00", conv=1, foer=1.0, efter=1.0),
       udf("watch_medier", 3, "c.dk", "2026-07-01T09:00", conv=2, foer=1.0, efter=1.0)]
_e2 = saml_effekt(_u2, None, "2026-08")
tjek("serien er kronologisk", [s["maaned"] for s in _e2["serie"]],
     ["2026-06", "2026-07", "2026-08"])

# ── 3. Én samtale om to abonnementer er ÉT opkald, ikke to. ──
_u3 = [udf("watch_medier", 1, "a.dk", "2026-08-01T09:00", conv=7, foer=1.0, efter=1.0),
       udf("watch_medier", 1, "b.dk", "2026-08-01T09:00", conv=7, foer=1.0, efter=1.0)]
_e3 = saml_effekt(_u3, None, "2026-08")
tjek("to udfald på én samtale er ét opkald", _e3["serie"][0]["samtaler"], 1)
tjek("... men to udfald", _e3["udfald_i_alt"], 2)

# ── 4. `maanedens` er None når referencemåneden ikke har registreringer.
#      Nul kroner og "ingen registreringer" må ikke se ens ud på et kort. ──
_e4 = saml_effekt(_u3, None, "2026-09")
tjek("maanedens er None uden registreringer i måneden", _e4["maanedens"], None)
tjek("... men serien har stadig august", len(_e4["serie"]), 1)

# ── 5. `tilladte` afgrænser, og org_id-typen må ikke vælte den:
#      udfaldene bærer int, nøglen streng. ──
_e5 = saml_effekt(_u2, {("watch_medier", "1")}, "2026-08")
tjek("tilladte afgrænser på tværs af org_id-typen", _e5["udfald_i_alt"], 1)
tjek("... og serien skrumper med", [s["maaned"] for s in _e5["serie"]], ["2026-08"])

# ── 6. Udfald uden outcome (ingen kontakt) hører i kontaktraten, ikke i
#      fordelingen. Biimplikationen i CK_RetOut_outcome_kraever_kontakt. ──
_u6 = [udf("watch_medier", 1, "a.dk", "2026-08-01T09:00", outcome=None,
           contact_result="ingen_kontakt", conv=1),
       udf("watch_medier", 2, "b.dk", "2026-08-02T09:00", foer=5.0, efter=5.0, conv=2)]
_e6 = saml_effekt(_u6, None, "2026-08")
tjek("kontakt_opnaaet tælles for sig", _e6["kontakt_opnaaet"], 1)
tjek("udfald i alt tæller BEGGE", _e6["udfald_i_alt"], 2)
tjek("rækken uden outcome står ikke i fordelingen",
     [f["outcome"] for f in _e6["fordeling"]], ["fornyet"])

# ── 7. Fordelingen er i fast rækkefølge, ikke sorteret på antal: en række der
#      flytter sig fordi et tal ændrede sig, kan ikke følges over tid. ──
_u7 = [udf("watch_medier", 1, "a.dk", "2026-08-01T09:00", outcome="opsagt", foer=9.0, conv=1),
       udf("watch_medier", 2, "b.dk", "2026-08-02T09:00", outcome="opsagt", foer=9.0, conv=2),
       udf("watch_medier", 3, "c.dk", "2026-08-03T09:00", outcome="fornyet", foer=1.0, efter=1.0, conv=3)]
_e7 = saml_effekt(_u7, None, "2026-08")
tjek("fornyet står før opsagt selvom opsagt er flest",
     [f["outcome"] for f in _e7["fordeling"]], ["fornyet", "opsagt"])
tjek("arr_paa_spil er arr_before, også for opsagt",
     [f["arr_paa_spil"] for f in _e7["fordeling"]], [1.0, 18.0])

# ── 8. Manglende arr_before tælles i uden_beloeb og ikke som nul. ──
_u8 = [udf("watch_medier", 1, "a.dk", "2026-08-01T09:00", outcome="forskudt", foer=None, conv=1),
       udf("watch_medier", 2, "b.dk", "2026-08-02T09:00", outcome="forskudt", foer=7.0, conv=2)]
_e8 = saml_effekt(_u8, None, "2026-08")
tjek("kun det kendte beløb indgår i arr_paa_spil",
     _e8["fordeling"][0]["arr_paa_spil"], 7.0)
tjek("den ukendte tælles i uden_beloeb", _e8["fordeling"][0]["uden_beloeb"], 1)


print()
if _fejl:
    print(f"FEJLEDE ({len(_fejl)}): " + ", ".join(_fejl))
    sys.exit(1)
print("ALT GRØNT")
sys.exit(0)

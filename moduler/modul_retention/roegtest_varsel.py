"""Røgtest af "Opsigelser i varsel"-panelets datalag (varsel.py) UDEN database.

    .venv/Scripts/python.exe moduler/modul_retention/roegtest_varsel.py

Beviser saml_varsel()'s regler mod opdigtede ordbøger, samme disciplin som
roegtest_prioritering.py: datalaget er en ren funktion over lister og
ordbøger, og prisen for det er at reglerne skal bevises et sted. Det er denne
fil. Hedder ikke `test_*.py` af samme grund som naboerne: tests/conftest.py
peger DB_SERVER på et hostnavn der ikke findes, og scriptet her åbner slet
ingen forbindelse.
"""
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
# FORREST i sys.path, ikke bagerst — samme begrundelse som
# roegtest_prioritering.py: en worktree har sin egen `moduler`-pakke uden
# denne fil, og uden insert(0, ...) ville den forkerte kopi vinde.
sys.path.insert(0, str(REPO_ROOT))

from moduler.modul_retention.outcomes import INTET_SITE  # noqa: E402
from moduler.modul_retention.varsel import saml_varsel  # noqa: E402

_fejl = []


def tjek(navn, faktisk, forventet):
    ok = faktisk == forventet
    print(("  OK   " if ok else "  FEJL ") + navn
          + ("" if ok else f"  (fik {faktisk!r}, forventede {forventet!r})"))
    if not ok:
        _fejl.append(navn)


def abo(account, org_id, sites, arr=None, org_name="Test A/S"):
    return {"account": account, "org_id": org_id, "org_name": org_name,
            "sites": sites, "arr_pr_abonnement": arr}


# ── 1. En opsigelse dateret FØR i dag er ikke i varsel — den er forfalden. ──
_a1 = [abo("watch_medier", 1, "a.dk", arr=1000.0)]
_o1 = {("watch_medier", 1, "a.dk"): "2026-08-01"}
_r1 = saml_varsel(_a1, _o1, "2026-09-01")
tjek("opsigelse dateret FØR i dag er ikke i varsel", _r1["i_alt"], 0)

# ── 2. Opsigelse PRÆCIS på i dag er ikke i varsel (ophørt i dag). ──
_a2 = [abo("watch_medier", 1, "a.dk", arr=1000.0)]
_o2 = {("watch_medier", 1, "a.dk"): "2026-09-01"}
_r2 = saml_varsel(_a2, _o2, "2026-09-01")
tjek("opsigelse PRÆCIS i dag er ikke i varsel", _r2["i_alt"], 0)

# ── 3. Opsigelse dateret EFTER i dag ER i varsel. ──
_a3 = [abo("watch_medier", 1, "a.dk", arr=1000.0)]
_o3 = {("watch_medier", 1, "a.dk"): "2026-09-02"}
_r3 = saml_varsel(_a3, _o3, "2026-09-01")
tjek("opsigelse dateret EFTER i dag ER i varsel", _r3["i_alt"], 1)
tjek("... og lander i sin ophørsmåned", _r3["pr_maaned"], [
    {"maaned": "2026-09", "antal": 1, "arr_dkk": 1000.0}])

# ── 4. Abonnement uden opsigelse (langt de fleste) forsvinder tavst. ──
_a4 = [abo("watch_medier", 1, "a.dk", arr=1000.0),
       abo("watch_medier", 2, "b.dk", arr=500.0)]
_o4 = {("watch_medier", 2, "b.dk"): "2026-09-15"}
_r4 = saml_varsel(_a4, _o4, "2026-09-01")
tjek("abonnement uden opsigelse tælles ikke med", _r4["i_alt"], 1)

# ── 5. sites=None (marketwire) ender i INTET_SITE-bucketen, forsvinder ikke. ──
_a5 = [abo("marketwire", 5, None, arr=None)]
_o5 = {("marketwire", 5, None): "2026-09-10"}
_r5 = saml_varsel(_a5, _o5, "2026-09-01")
tjek("sites=None matcher opsigelsen på samme rå nøgle", _r5["i_alt"], 1)
tjek("... og lander i INTET_SITE-bucketen", _r5["pr_site"][0]["site"], INTET_SITE)

# ── 6. Manglende ARR tælles i antal, men ikke i arr_dkk, og hæver uden_arr. ──
_a6 = [abo("watch_medier", 1, "a.dk", arr=None),
       abo("watch_medier", 2, "b.dk", arr=2000.0)]
_o6 = {("watch_medier", 1, "a.dk"): "2026-09-10",
       ("watch_medier", 2, "b.dk"): "2026-09-10"}
_r6 = saml_varsel(_a6, _o6, "2026-09-01")
tjek("begge tælles i antal uanset ARR", _r6["i_alt"], 2)
tjek("kun det kendte beløb indgår i summen", _r6["arr_dkk"], 2000.0)
tjek("den ukendte tælles i uden_arr", _r6["uden_arr"], 1)

# ── 7. kunder tæller DISTINKTE (account, org_id), ikke rækker. ──
_a7 = [abo("watch_medier", 1, "a.dk", arr=1000.0),
       abo("watch_medier", 1, "b.dk", arr=1000.0)]
_o7 = {("watch_medier", 1, "a.dk"): "2026-09-10",
       ("watch_medier", 1, "b.dk"): "2026-09-20"}
_r7 = saml_varsel(_a7, _o7, "2026-09-01")
tjek("to opsagte sites hos SAMME kunde tæller 2 abonnementer", _r7["i_alt"], 2)
tjek("... men kun 1 kunde", _r7["kunder"], 1)

# ── 8. Summen af pr_site og pr_maaned matcher i_alt. ──
_a8 = [abo("watch_medier", 1, "a.dk", arr=1000.0),
       abo("watch_medier", 2, "b.dk", arr=500.0),
       abo("watch_medier", 3, "a.dk", arr=250.0)]
_o8 = {("watch_medier", 1, "a.dk"): "2026-09-10",
       ("watch_medier", 2, "b.dk"): "2026-10-05",
       ("watch_medier", 3, "a.dk"): "2026-09-20"}
_r8 = saml_varsel(_a8, _o8, "2026-09-01")
tjek("summen af pr_site matcher i_alt",
     sum(s["antal"] for s in _r8["pr_site"]), _r8["i_alt"])
tjek("summen af pr_maaned matcher i_alt",
     sum(m["antal"] for m in _r8["pr_maaned"]), _r8["i_alt"])
tjek("pr_site er sorteret faldende på antal", _r8["pr_site"][0]["site"], "a.dk")
tjek("pr_maaned er sorteret kronologisk",
     [m["maaned"] for m in _r8["pr_maaned"]], ["2026-09", "2026-10"])

# ── 9. haster-flaget: kun opsigelser inden for HASTER_DAGE (30) tælles med. ──
_a9 = [abo("watch_medier", 1, "a.dk", arr=1000.0),
       abo("watch_medier", 2, "a.dk", arr=1000.0)]
_o9 = {("watch_medier", 1, "a.dk"): "2026-09-10",   # 9 dage frem — haster
       ("watch_medier", 2, "a.dk"): "2026-12-01"}   # langt ude — haster ikke
_r9 = saml_varsel(_a9, _o9, "2026-09-01")
tjek("kun den nære opsigelse tælles i haster", _r9["pr_site"][0]["haster"], 1)


print()
if _fejl:
    print(f"FEJLEDE ({len(_fejl)}): " + ", ".join(_fejl))
    sys.exit(1)
print("ALT GRØNT")
sys.exit(0)

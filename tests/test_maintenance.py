"""Tests for datastatus-dashboardet (/tools/maintenance/).

Det, der kan gå galt her, går galt STILLE: en forkert vurdering giver et grønt
felt over data, der er en uge gamle, og ingen opdager det — det er præcis den
fejl, siden findes for at forhindre. Derfor testes vurderingen som ren logik,
uden database.

Fredet adfærd:
  - et vindue regnes fra den planlagte kørsel PLUS forsinkelsen, aldrig fra nu
  - én manglende kørsel er 'forsinket', to i træk er 'fejl'
  - en fejlet eller hængende kørsel vinder over "data er alligevel friske nok"
  - valutakurser må ikke melde fejl i en weekend, hvor Nationalbanken ikke
    offentliggør
  - adgangskravet i nav_utils skal matche routerens, ellers får brugeren et
    menupunkt der svarer 403
"""
from datetime import datetime, timedelta

import pytest

from moduler.modul_maintenance import queries as q


def _kilde(kilde_id: str) -> dict:
    for k in q.KILDER:
        if k["id"] == kilde_id:
            return k
    raise AssertionError(f"ukendt kilde: {kilde_id}")


# ---------------------------------------------------------------------------
# Planen
# ---------------------------------------------------------------------------

def test_daglig_plan_finder_gaarsdagens_koersel_foer_dagens_er_forfalden():
    """Kl. 09:00 er dagens 16:00-kørsel ikke forfalden — gårsdagens er den seneste."""
    plan = {"tider": ["16:00"]}
    nu = datetime(2026, 9, 17, 9, 0)
    forfaldne = q._forfaldne_koersler(plan, forsinkelse_min=120, nu=nu)
    assert forfaldne[0] == datetime(2026, 9, 16, 16, 0)
    assert forfaldne[1] == datetime(2026, 9, 15, 16, 0)


def test_forsinkelsen_regnes_fra_den_planlagte_tid():
    """En kørsel er først forfalden, når forsinkelsen er gået — ikke kl. 16:00 blankt.

    Uden det ville ACV-updateren melde forsinket hver eneste dag i det kvarter,
    hvor den rent faktisk kører.
    """
    plan = {"tider": ["16:00"]}
    assert q._forfaldne_koersler(plan, 120, datetime(2026, 9, 17, 17, 0))[0] \
        == datetime(2026, 9, 16, 16, 0)          # dagens er endnu ikke forfalden
    assert q._forfaldne_koersler(plan, 120, datetime(2026, 9, 17, 18, 1))[0] \
        == datetime(2026, 9, 17, 16, 0)          # nu er den


def test_gentagelsesvindue_giver_kvartersintervaller():
    plan = {"tider": ["16:00"], "gentag_fra": "06:00",
            "gentag_til": "18:00", "hvert_min": 15}
    tider = q._tider_paa_dagen(plan)
    assert tider[0].strftime("%H:%M") == "06:00"
    assert tider[-1].strftime("%H:%M") == "18:00"
    # 06:00–18:00 med 15 min. = 49 tidspunkter. 16:00 ligger allerede i
    # vinduet og må ikke tælles to gange.
    assert len(tider) == 49


def test_naeste_koersel_ligger_altid_frem_i_tiden():
    plan = {"tider": ["16:00"], "gentag_fra": "06:00",
            "gentag_til": "18:00", "hvert_min": 15}
    nu = datetime(2026, 9, 17, 8, 37)
    assert q._naeste_koersel(plan, nu) == datetime(2026, 9, 17, 8, 45)
    # Efter dagens sidste kørsel skal den rulle over til i morgen tidlig.
    assert q._naeste_koersel(plan, datetime(2026, 9, 17, 19, 0)) \
        == datetime(2026, 9, 18, 6, 0)


# ---------------------------------------------------------------------------
# Vurderingen
# ---------------------------------------------------------------------------

def test_koersel_i_hus_giver_ok():
    kilde = _kilde("acv_updater")
    nu = datetime(2026, 9, 17, 9, 0)
    landet = {"afsluttet": datetime(2026, 9, 16, 16, 12),
              "startet": datetime(2026, 9, 16, 16, 0)}
    assert q._vurder(kilde, None, landet, None, nu)["status"] == "ok"


def test_en_manglende_koersel_giver_forsinket():
    kilde = _kilde("acv_updater")
    nu = datetime(2026, 9, 17, 19, 0)      # dagens 16:00 er forfalden
    landet = {"afsluttet": datetime(2026, 9, 16, 16, 12),
              "startet": datetime(2026, 9, 16, 16, 0)}
    assert q._vurder(kilde, None, landet, None, nu)["status"] == "forsinket"


def test_to_manglende_koersler_giver_fejl():
    kilde = _kilde("acv_updater")
    nu = datetime(2026, 9, 17, 19, 0)
    landet = {"afsluttet": datetime(2026, 9, 15, 16, 12),
              "startet": datetime(2026, 9, 15, 16, 0)}
    assert q._vurder(kilde, None, landet, None, nu)["status"] == "fejl"


def test_fejlet_koersel_vinder_over_friske_data():
    """Data fra i går er inden for vinduet, men kørslen i dag fejlede.

    Statussen skal være 'fejl': nogen skal kigge på det, uanset at tallene
    på dashboardene stadig er brugbare.
    """
    kilde = _kilde("acv_updater")
    nu = datetime(2026, 9, 17, 19, 0)
    seneste = {"status": "error", "startet": datetime(2026, 9, 17, 16, 0),
               "afsluttet": datetime(2026, 9, 17, 16, 1), "besked": "boom"}
    seneste_ok = {"afsluttet": datetime(2026, 9, 17, 16, 0),
                  "startet": datetime(2026, 9, 17, 16, 0)}
    vurdering = q._vurder(kilde, seneste, seneste_ok, None, nu)
    assert vurdering["status"] == "fejl"
    # Forklaringen skal sige, hvor gamle data så ER — ellers ved man ikke,
    # om dashboardene kan bruges imens.
    assert "vellykkede" in vurdering["forklaring"]


def test_koersel_der_lige_er_startet_er_ikke_haengende():
    kilde = _kilde("pipedrive_sync")
    nu = datetime(2026, 9, 17, 8, 40)
    seneste = {"status": "running", "startet": nu - timedelta(minutes=3),
               "afsluttet": None, "besked": None}
    assert q._vurder(kilde, seneste, None, None, nu)["status"] == "ok"


def test_running_raekke_der_aldrig_blev_lukket_giver_haenger():
    kilde = _kilde("pipedrive_sync")
    nu = datetime(2026, 9, 17, 8, 40)
    seneste = {"status": "running",
               "startet": nu - timedelta(hours=q.HAENGER_EFTER_TIMER + 1),
               "afsluttet": None, "besked": None}
    assert q._vurder(kilde, seneste, None, None, nu)["status"] == "haenger"


def test_kilde_uden_plan_og_uden_graense_alarmerer_aldrig():
    """Budgettet uploades manuelt. 58 dage uden upload er ikke en fejl."""
    kilde = _kilde("budget")
    nu = datetime(2026, 9, 17, 9, 0)
    landet = {"afsluttet": nu - timedelta(days=58), "startet": nu - timedelta(days=58)}
    assert q._vurder(kilde, None, landet, None, nu)["status"] == "info"


def test_kilde_med_max_alder_alarmerer_naar_graensen_overskrides():
    kilde = _kilde("zuora_retention")           # max_alder_dage = 40
    nu = datetime(2026, 9, 17, 9, 0)
    frisk = {"afsluttet": nu - timedelta(days=20), "startet": nu - timedelta(days=20)}
    gammel = {"afsluttet": nu - timedelta(days=45), "startet": nu - timedelta(days=45)}
    assert q._vurder(kilde, None, frisk, None, nu)["status"] == "ok"
    assert q._vurder(kilde, None, gammel, None, nu)["status"] == "forsinket"


def test_uden_baade_log_og_faldback_er_status_ukendt():
    kilde = _kilde("programmatic")
    assert q._vurder(kilde, None, None, None, datetime(2026, 9, 17, 9, 0))["status"] \
        == "ukendt"


# ---------------------------------------------------------------------------
# Dato-faldback
# ---------------------------------------------------------------------------

def test_datofaldback_maaler_i_hele_dage_ikke_i_klokkeslaet():
    """En DATE står på midnat og kan aldrig nå kl. 05:00 samme dag.

    Uden dagsammenligningen ville ProgrammaticSales stå permanent forsinket,
    fordi 2026-09-17 00:00 < 2026-09-17 05:00.
    """
    kilde = _kilde("programmatic")              # dagligt 05:00, faldback_tz='dato'
    nu = datetime(2026, 9, 17, 9, 0)
    idag = datetime(2026, 9, 17, 0, 0)
    assert q._vurder(kilde, None, None, idag, nu)["status"] == "ok"


def test_valutakurser_melder_ikke_fejl_i_weekenden():
    """Nationalbanken offentliggør ikke lørdag og søndag.

    Fredagens kurs er den rigtige søndag aften — melder siden fejl dér, lærer
    folk at ignorere den, og så virker den heller ikke om mandagen.
    """
    kilde = _kilde("currencycollector")         # dagligt 16:45, kun_hverdage
    soendag_aften = datetime(2026, 9, 20, 22, 0)      # 2026-09-20 er en søndag
    fredagens_kurs = datetime(2026, 9, 18, 0, 0)      # fredag
    assert q._vurder(kilde, None, None, fredagens_kurs, soendag_aften)["status"] == "ok"


def test_valutakurser_melder_stadig_fejl_paa_en_hverdag():
    kilde = _kilde("currencycollector")
    torsdag_aften = datetime(2026, 9, 17, 22, 0)      # 2026-09-17 er en torsdag
    mandagens_kurs = datetime(2026, 9, 14, 0, 0)
    assert q._vurder(kilde, None, None, mandagens_kurs, torsdag_aften)["status"] == "fejl"


# ---------------------------------------------------------------------------
# Katalogets integritet
# ---------------------------------------------------------------------------

def test_filbaserede_kilder_har_en_finder():
    """Hver kilde med fil=True skal have en opskrift på, hvor filen ligger.

    Uden den falder _fil_status() over en KeyError midt i sideopbygningen — og
    så er der ingen datastatus-side at kigge på.
    """
    from moduler.modul_maintenance.queries import _FIL_FINDERE
    for kilde in q.KILDER:
        if kilde.get("fil"):
            assert kilde["id"] in _FIL_FINDERE, kilde["id"]


def test_filnavnets_dato_laeses_ens_for_alle_eksporttyper():
    from moduler.modul_maintenance.queries import _FIL_DATO_RE
    for navn in ("ACV_snapshot_05092026", "usage_kunde_17092026",
                 "dm_kobling_01082026"):
        assert _FIL_DATO_RE.search(navn).groups()[2] in ("2026",), navn


def test_alle_kilder_har_de_felter_dashboardet_render():
    for kilde in q.KILDER:
        for felt in ("id", "titel", "tabel", "opgave", "plan_tekst", "repo",
                     "bruges_af", "kilde"):
            assert felt in kilde, f"{kilde.get('id')} mangler {felt}"
        if kilde.get("plan"):
            assert "forsinkelse_min" in kilde, kilde["id"]
        else:
            assert "max_alder_dage" in kilde, kilde["id"]


def test_kilde_id_og_logkilde_er_unikke():
    ider = [k["id"] for k in q.KILDER]
    assert len(ider) == len(set(ider))
    kilder = [k["kilde"] for k in q.KILDER]
    assert len(kilder) == len(set(kilder))


def test_nav_kraever_samme_rolle_som_routeren():
    """Sættes nav'en lavere end routeren, får brugeren et menupunkt der svarer 403."""
    from moduler.modul_maintenance.router import MIN_ROLLE
    from nav_utils import CATEGORIES

    drift = next(c for c in CATEGORIES if c["id"] == "drift")
    assert drift["min_role"] == MIN_ROLLE
    item = next(i for i in drift["items"] if i["id"] == "maintenance-datastatus")
    assert item["min_role"] == MIN_ROLLE
    assert item["url"] == "/tools/maintenance/"


# ---------------------------------------------------------------------------
# Spejlkopier
# ---------------------------------------------------------------------------

SPEJLKOPIER = [
    ("../pipedrive_sync/dataloads.py"),
    ("../ACV_updater_pipedrive/dataloads.py"),
    ("../currencycollector/dataloads.py"),
    ("../ProgrammaticFinansSales/dataloads.py"),
]


@pytest.mark.parametrize("relativ_sti", SPEJLKOPIER)
def test_dataloads_kopier_er_identiske_med_originalen(relativ_sti):
    """dataloads.py ligger i fem projekter og skal være ÉN fil.

    Driver kopierne fra hinanden, skriver scriptene til en logtabel, der ikke
    længere passer med den, dashboardet læser — og fejlen viser sig som et
    tomt felt, ikke som en fejl nogen ser.

    Springes over hvis sidemappen ikke er tjekket ud ved siden af (fx i CI,
    hvor kun dette repo hentes).
    """
    from pathlib import Path

    repo = Path(__file__).resolve().parents[1]
    original = repo / "moduler" / "modul_maintenance" / "dataloads.py"
    kopi = (repo / relativ_sti).resolve()
    if not kopi.exists():
        pytest.skip(f"{kopi} er ikke tjekket ud ved siden af dette repo")

    assert kopi.read_text(encoding="utf-8") == original.read_text(encoding="utf-8"), (
        f"{kopi} er drevet fra originalen. Kopiér "
        f"moduler/modul_maintenance/dataloads.py derover igen."
    )

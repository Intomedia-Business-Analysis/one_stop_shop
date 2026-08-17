"""Valuta-reglen: hvornår vises et deal-beløb råt, og hvornår omregnes til DKK?

constants.deal_value_sql() er én kilde til sandheden for alle dashboards. Den
SQL-tekst kører her mod en in-memory SQLite med fixture-deals (samme mønster som
test_admin_nysalg_sql_parity), så reglen testes som SQL og ikke som en
Python-genfortolkning af den.

Baggrunden: reglen så tidligere kun på [currency], så en dansk sælgers
NordicDefenceWatch-salg i SEK blev talt med som 80.000 i stedet for 54.376 DKK.
NDW sælges på tværs af Norden, så det er ikke et engangstilfælde.

Kører både under pytest og som standalone-script:
    python tests/test_valuta.py
"""
import os
import sqlite3
import sys

# Gør repo-roden importerbar når filen køres direkte (uden pytest/conftest).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from constants import deal_value_sql  # noqa: E402

_COLS = ["navn", "team", "account", "currency", "value", "value_dkk"]

# (navn, team, account, currency, value, value_dkk)
_DEALS = [
    # Reference-sagen: dansk sælger, dansk team, DKK-budget, SEK-deal.
    ("ndw_dk_saelger",   "Team Watch DK",  "watch_medier",       "SEK", 80000.0, 54376.0),
    # Samme mønster hos Team Banner (dansk annoncebudget, svensk site).
    ("banner_dk_sek",    "Team Banner",    "jppol_advertising",  "SEK", 29600.0, 20065.84),
    # NO/SE-organisationen: budgetterne ER i lokal valuta → beløbet står råt.
    ("no_saelger",       "Team Watch NO",  "watch_no",           "NOK", 10000.0,  6300.0),
    ("no_advertising",   "Team Watch NO Advertising", "watch_no_advertising", "NOK", 5000.0, 3150.0),
    ("se_saelger",       "Team Watch SE",  "watch_se",           "SEK", 7980.0,   5424.0),
    # team=NULL findes på en del norske deals (også åbne, der tæller i
    # pipeline-widgets) — account'en skal så afgøre det.
    ("no_uden_team",     None,             "watch_no",           "NOK", 5440.0,   3905.92),
    # Watch DE: aldrig team, altid EUR.
    ("watch_de",         None,             "watch_de",           "EUR", 1000.0,   7460.0),
    # Danske teams' EUR-deals hører til DKK-budgetter, også i de dashboards der
    # holder Watch DE's EUR råt.
    ("int_dk_eur",       "Team Watch Int", "watch_medier",       "EUR", 1376.0,  10286.70),
    # Almindelig dansk DKK-deal — uændret uanset regel.
    ("dk_almindelig",    "Team Watch DK",  "watch_medier",       "DKK", 5000.0,   5000.0),
]

# Forventet beløb pr. deal. Første tal = standardreglen (NOK/SEK), andet =
# eur_local-varianten (afdelingsleder + media-performance, hvor Watch DE's
# EUR-budget kræver at DE's beløb bliver i EUR).
_FORVENTET = {
    "ndw_dk_saelger": (54376.0,  54376.0),
    "banner_dk_sek":  (20065.84, 20065.84),
    "no_saelger":     (10000.0,  10000.0),
    "no_advertising":  (5000.0,   5000.0),
    "se_saelger":      (7980.0,   7980.0),
    "no_uden_team":    (5440.0,   5440.0),
    "watch_de":        (7460.0,   1000.0),
    "int_dk_eur":     (10286.70, 10286.70),
    "dk_almindelig":   (5000.0,   5000.0),
}


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE deals (" + ",".join(_COLS) + ")")
    conn.executemany(
        "INSERT INTO deals VALUES (" + ",".join(["?"] * len(_COLS)) + ")", _DEALS)
    return conn


def _belob(conn, eur_local: bool) -> dict:
    sql = f"SELECT [navn], {deal_value_sql(eur_local=eur_local)} AS v FROM deals"
    return {r[0]: r[1] for r in conn.execute(sql)}


def test_standardreglen_omregner_kun_danske_saelgeres_udenlandske_deals():
    faktisk = _belob(_conn(), eur_local=False)
    for navn, (ventet, _) in _FORVENTET.items():
        assert round(faktisk[navn], 2) == round(ventet, 2), navn


def test_eur_local_holder_kun_watch_de_i_euro():
    faktisk = _belob(_conn(), eur_local=True)
    for navn, (_, ventet) in _FORVENTET.items():
        assert round(faktisk[navn], 2) == round(ventet, 2), navn


def test_de_to_varianter_er_enige_om_alt_andet_end_watch_de():
    """Perf- og afdelingsleder-dashboardet skal vise samme tal for samme team.

    Før organisations-guarden var de uenige: afdelingsleder-varianten talte
    danske teams' EUR-deals råt (fx 1.376 i stedet for 10.287 DKK), så de to
    dashboards viste forskellige tal for Team Watch Int og Team Banner.
    """
    conn = _conn()
    standard, eur = _belob(conn, False), _belob(conn, True)
    for navn in _FORVENTET:
        if navn == "watch_de":
            continue
        assert round(standard[navn], 2) == round(eur[navn], 2), navn


def test_ingen_modul_genimplementerer_den_rene_valuta_regel():
    """Regressions-vagt: reglen må kun findes i constants.py.

    Den gamle form (kun [currency], ingen team/account) var copy-pastet ~100
    steder. Dukker den op igen, er valuta-fejlen tilbage i det modul.
    """
    rod = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    gammel = "CASE WHEN [currency] IN ('NOK','SEK')"
    syndere = []
    for mappe, undermapper, filer in os.walk(os.path.join(rod, "moduler")):
        undermapper[:] = [d for d in undermapper if d != "__pycache__"]
        for fil in filer:
            if not fil.endswith(".py") or fil.endswith("-INTOMEDIA-CLS.py"):
                continue
            sti = os.path.join(mappe, fil)
            with open(sti, encoding="utf-8") as fh:
                if gammel in fh.read():
                    syndere.append(os.path.relpath(sti, rod))
    assert not syndere, f"Bruger den gamle valuta-regel i stedet for constants.deal_value_sql: {syndere}"


if __name__ == "__main__":
    for navn, fn in sorted(globals().items()):
        if navn.startswith("test_") and callable(fn):
            fn()
            print(f"ok  {navn}")
    print("alle valuta-tests ok")

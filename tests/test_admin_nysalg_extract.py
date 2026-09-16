"""Indlæsning af Zuora-udtrækket: valutavalg og periodeafgrænsning.

To ting er nemme at tage fejl af, og begge koster forkerte tal i rapporten:

1. Udtrækket har TO valutakolonner. `currency` er kundens kontraktvaluta og
   gælder kun arr_local; `report_currency` er brandets valuta, som alle
   bevægelsestallene (arr_dkk, prev_arr, net_diff, gross_in, gross_out) er
   omregnet til. Rækkens valuta skal følge beløbene — ellers står en
   EUR-betalende Watch SE-kunde med "EUR" ud for et beløb i SEK.

2. Det er runnets Periode — ikke filens indhold — der afgør hvilke måneder der
   rapporteres. Udtrækket indeholder med vilje en ekstra måned FØR årets start,
   fordi LAG() skal bruge en foregående måned for at kunne regne januars
   gross_in. I den seed-måned er prev_arr NULL for alle, så hvert aktivt
   abonnement ser ud som nysalg — den må aldrig komme med i en rapporteret
   periode.

Kører både under pytest og som standalone-script:
    python tests/test_admin_nysalg_extract.py
"""
import csv
import io
import os
import sys

# Gør repo-roden importerbar når filen køres direkte (uden pytest/conftest).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from moduler.modul_admin_nysalg import extract_loader as EL  # noqa: E402

_COLS = ["month_end", "account_number", "pipedrive_id", "contact_companyname",
         "brands", "site", "account_type", "currency", "report_currency",
         "arr_local", "arr_dkk", "prev_arr", "net_diff", "gross_in", "gross_out",
         "movement", "administrativ"]


def _row(month_end, site, currency, report_currency, gross_in=1000, **kw):
    r = {c: "" for c in _COLS}
    r.update({"month_end": month_end, "account_number": "A1", "pipedrive_id": "111",
              "contact_companyname": "Testkunde", "brands": "Watch DK", "site": site,
              "account_type": "Company", "currency": currency,
              "report_currency": report_currency, "arr_local": 1000,
              "arr_dkk": gross_in, "prev_arr": 0, "net_diff": gross_in,
              "gross_in": gross_in, "gross_out": 0, "movement": "Nysalg",
              "administrativ": 0})
    r.update(kw)
    return r


def _load(rows, drop=()):
    """Skriv rækkerne som CSV og kør dem gennem den rigtige indlæser."""
    cols = [c for c in _COLS if c not in drop]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)
    return EL.load_extract(file_bytes=buf.getvalue().encode("utf-8"),
                           filename="udtraek.csv")


# ── Valuta ───────────────────────────────────────────────────────────────────

def test_beloebets_valuta_vinder_over_kundens_kontraktvaluta():
    """EUR-betalende Watch SE-kunde: beløbene er i SEK, så rækken skal vise SEK."""
    rows = _load([_row("2026-09-30", "FinansWatch SE", "EUR", "SEK")])
    assert rows[0].currency == "SEK"


def test_dansk_raekke_er_uaendret():
    rows = _load([_row("2026-09-30", "MedWatch DK", "DKK", "DKK")])
    assert rows[0].currency == "DKK"


def test_gammelt_udtraek_uden_report_currency_falder_tilbage():
    """Udtræk fra før kolonnen fandtes skal stadig kunne køres igennem."""
    rows = _load([_row("2026-09-30", "FinansWatch SE", "EUR", "SEK")],
                 drop=("report_currency",))
    assert rows[0].currency == "EUR"


def test_tom_report_currency_falder_tilbage():
    """En tom celle må ikke give en tom valuta på rækken."""
    rows = _load([_row("2026-09-30", "Watch NO", "NOK", "")])
    assert rows[0].currency == "NOK"


def test_report_currency_er_ikke_paakraevet():
    """Kolonnen er valgfri — den må ikke stå på REQUIRED_COLUMNS."""
    assert "report_currency" not in EL.REQUIRED_COLUMNS
    assert "report_currency" in EL._OPTIONAL_COLUMNS


# ── Periodeafgrænsning ───────────────────────────────────────────────────────

def _flere_maaneder():
    return _load([
        _row("2025-12-31", "MedWatch DK", "DKK", "DKK"),   # seed-måned
        _row("2026-01-31", "MedWatch DK", "DKK", "DKK"),
        _row("2026-09-30", "MedWatch DK", "DKK", "DKK"),
    ])


def test_periode_paa_september_tager_kun_september():
    """Runnets periode — ikke filen — afgør hvad der reviewes og rapporteres."""
    valgt = EL.filter_range(_flere_maaneder(), "2026-09-01", "2026-09-30")
    assert [r.month_end for r in valgt] == ["2026-09-30"]


def test_tom_periode_tager_ogsaa_seed_maaneden():
    """Uden periode kommer december-seeden med, hvor ALT ser ud som nysalg."""
    alle = EL.filter_range(_flere_maaneder(), None, None)
    assert "2025-12-31" in [r.month_end for r in alle]
    assert len(alle) == 3


def test_available_periods_viser_nyeste_foerst():
    assert EL.available_periods(_flere_maaneder()) == ["2026-09", "2026-01", "2025-12"]


# ── Standalone-runner (uden pytest) ──────────────────────────────────────────

if __name__ == "__main__":
    funcs = [v for k, v in sorted(globals().items())
             if k.startswith("test_") and callable(v)]
    for fn in funcs:
        fn()
        print(f"  ok  {fn.__name__}")
    print(f"\n{len(funcs)}/{len(funcs)} tests bestået")

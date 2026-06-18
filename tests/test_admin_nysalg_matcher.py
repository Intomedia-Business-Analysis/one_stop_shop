"""Unit-tests for admin-nysalg-matcheren (ren, IO-fri kerne).

Kører både under pytest og som standalone-script:
    python tests/test_admin_nysalg_matcher.py

Validerende test-case fra overleveringen (§12):
    FinansWatch DK | 2874 | 2026-05-31  →  pos: 876032, neg: -278432
"""
import datetime as dt
import os
import sys

# Gør repo-roden importerbar når filen køres direkte (uden pytest/conftest).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from moduler.modul_admin_nysalg.matcher import (  # noqa: E402
    _as_date, build_index, last_day_of_month, make_key, match_rows, sign_of,
)
from moduler.modul_admin_nysalg.models import AdminDeal, ExtractRow  # noqa: E402


# ── Hjælpere til kortfattede fixtures ────────────────────────────────────────

def _deal(value, site="FinansWatch DK", org="2874", month="2026-05-31", did=None):
    return AdminDeal(deal_id=did or f"d{int(value)}", org_id=org, site=site,
                     month_end=month, value=value, pipeline="", status="")


def _row(net_diff, site="FinansWatch DK", org="2874", month="2026-05-31", gross_in=0.0):
    return ExtractRow(
        month_end=month, account_number="A1", pipedrive_id=org, site=site,
        brands="", account_type="Business", currency="DKK", arr_local=0.0,
        arr_dkk=0.0, prev_arr=0.0, net_diff=net_diff, gross_in=gross_in or net_diff,
        gross_out=0.0, movement="Nysalg" if net_diff > 0 else "Churn", administrativ=0,
    )


# ── Normalisering ────────────────────────────────────────────────────────────

def test_as_date_handles_datetime_and_text():
    assert _as_date(dt.date(2026, 5, 31)) == "2026-05-31"
    assert _as_date(dt.datetime(2026, 5, 31, 0, 0)) == "2026-05-31"
    assert _as_date("2026-05-31") == "2026-05-31"
    assert _as_date("2026-05-31 00:00:00") == "2026-05-31"
    assert _as_date("31-05-2026") == "2026-05-31"


def test_last_day_of_month():
    assert last_day_of_month("2026-05-15") == "2026-05-31"
    assert last_day_of_month("2026-02-10") == "2026-02-28"   # ikke skudår
    assert last_day_of_month(dt.date(2024, 2, 10)) == "2024-02-29"  # skudår
    assert last_day_of_month("2026-12-01") == "2026-12-31"


def test_make_key_identical_across_date_types():
    # Dato som date og som tekst skal give NØJAGTIG samme nøgle.
    assert make_key("FinansWatch DK", 2874, dt.date(2026, 5, 31)) \
        == make_key("FinansWatch DK", "2874", "2026-05-31")


def test_make_key_normalizes_id_whitespace():
    assert make_key("finans.dk", " 99 ", "2026-05-31") == "finans.dk|99|2026-05-31"


def test_sign_of():
    assert sign_of(10) == "pos"
    assert sign_of(-10) == "neg"
    assert sign_of(0) is None
    assert sign_of("abc") is None


def test_site_map_applied_both_sides():
    site_map = {"finans.dk": "FinansWatch DK"}
    idx, dups = build_index([_deal(100, site="finans.dk")], site_map=site_map)
    rows = [_row(100, site="FinansWatch DK")]
    match_rows(rows, idx, dups, site_map=site_map)
    assert rows[0].is_admin_nysalg()


# ── Kernecase: 2874 ──────────────────────────────────────────────────────────

def test_2874_positive_newsale_matches_positive_deal():
    """Nysalgsrækken markeres administrativ med value 876032 (ikke -278432)."""
    deals = [_deal(876032.0, did="POS"), _deal(-278432.0, did="NEG")]
    idx, dups = build_index(deals)

    row = _row(876032.0)             # net_diff > 0 → nysalg
    match_rows([row], idx, dups)

    assert row.is_admin_nysalg() is True
    assert row.match is not None
    assert row.match.deal_id == "POS"
    assert row.match.value == 876032.0
    assert row.match_sign == "pos"
    assert row.ambiguous is False


def test_negative_movement_is_not_matched():
    """net_diff < 0 → ingen matchning; opsigelser styres af administrativ-flaget."""
    deals = [_deal(876032.0, did="POS"), _deal(-278432.0, did="NEG")]
    idx, dups = build_index(deals)

    row = _row(-278432.0)            # net_diff < 0
    match_rows([row], idx, dups)

    assert row.match is None
    assert row.is_admin_nysalg() is False


def test_zero_movement_no_processing():
    idx, dups = build_index([_deal(100.0)])
    row = _row(0.0)
    match_rows([row], idx, dups)
    assert row.match is None
    assert row.match_sign is None


def test_unmatched_newsale_stays_plain():
    """Blank = intet match (ikke 0): urørt nysalg forbliver almindeligt nysalg."""
    idx, dups = build_index([_deal(100.0, org="9999")])
    row = _row(500.0, org="2874")    # ingen admin-deal på denne nøgle
    match_rows([row], idx, dups)
    assert row.match is None
    assert row.is_admin_nysalg() is False


def test_same_sign_duplicates_flag_ambiguous():
    """To deals med samme nøgle+fortegn → første vælges, men rækken er ambiguous."""
    deals = [_deal(100.0, did="FIRST"), _deal(200.0, did="SECOND")]  # samme nøgle, begge pos
    idx, dups = build_index(deals)
    assert len(dups) == 1

    row = _row(100.0)
    match_rows([row], idx, dups)
    assert row.match is not None
    assert row.match.deal_id == "FIRST"   # første vinder (som XOPSLAG)
    assert row.ambiguous is True


def test_opposite_sign_same_key_not_duplicate():
    """Pos og neg på samme nøgle er IKKE en dublet — det er den normale to-deal-case."""
    idx, dups = build_index([_deal(100.0), _deal(-50.0)])
    assert dups == set()


# ── Standalone-runner (uden pytest) ──────────────────────────────────────────

if __name__ == "__main__":
    funcs = [v for k, v in sorted(globals().items())
             if k.startswith("test_") and callable(v)]
    passed = 0
    for fn in funcs:
        fn()
        print(f"  ok  {fn.__name__}")
        passed += 1
    print(f"\n{passed}/{len(funcs)} tests bestået")

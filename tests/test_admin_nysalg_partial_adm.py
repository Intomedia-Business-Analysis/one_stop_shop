"""Unit-tests for delvist administrative bevægelser (effective_adm_in/out).

Automatikken: er den matchede deals værdi mere end tolerancen (1 %) mindre end
gross in/out, er kun deal-værdien administrativ — resten tæller som almindeligt
salg/churn. CFO-casen fra juli 2026-rapporten: Netcompany med gross in 250.000
hvor kun den administrative deal på 110.000 skal trækkes fra Actual Sale.

Kører både under pytest og som standalone-script:
    python tests/test_admin_nysalg_partial_adm.py
"""
import os
import sys

# Gør repo-roden importerbar når filen køres direkte (uden pytest/conftest).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from moduler.modul_admin_nysalg.repo import (  # noqa: E402
    auto_adm_share, effective_adm_in, effective_adm_out, summarize,
)


def _m(**kw):
    """Match-række som dict med samme felter som repo.get_matches leverer."""
    m = {
        "gross_in": 0.0, "gross_out": 0.0, "net_diff": 0.0,
        "is_admin": False, "administrativ": False, "match_sign": None,
        "matched_value": None, "override": None,
        "gross_in_override": None, "gross_out_override": None,
        "adm_in_override": None, "adm_out_override": None,
        "total_excluded": False, "ambiguous": False,
    }
    m.update(kw)
    return m


def _admin_nysalg(gross_in, deal_value, **kw):
    return _m(gross_in=gross_in, net_diff=gross_in, is_admin=True,
              match_sign="pos", matched_value=deal_value, **kw)


def _admin_opsigelse(gross_out, deal_value, **kw):
    return _m(gross_out=gross_out, net_diff=-gross_out,
              match_sign="neg" if deal_value is not None else None,
              matched_value=deal_value, administrativ=deal_value is None, **kw)


# ── auto_adm_share (ren beregning) ───────────────────────────────────────────

def test_auto_share_partial_when_deal_below_gross():
    assert auto_adm_share(250000.0, 110000.0) == 110000.0


def test_auto_share_full_within_tolerance():
    # Under 1 % afvigelse (afrunding/valutakurs) => helt administrativ.
    assert auto_adm_share(250000.0, 249000.0) == 250000.0


def test_auto_share_full_when_deal_at_or_above_gross():
    assert auto_adm_share(250000.0, 250000.0) == 250000.0
    assert auto_adm_share(250000.0, 300000.0) == 250000.0


def test_auto_share_no_deal_value_means_all():
    assert auto_adm_share(250000.0, None) == 250000.0


def test_auto_share_zero_gross():
    assert auto_adm_share(0.0, 110000.0) == 0.0


# ── Nysalgssiden (effective_adm_in) ──────────────────────────────────────────

def test_netcompany_case_partial_admin():
    """Gross in 250.000, administrativ deal 110.000 → kun 110.000 trækkes fra."""
    m = _admin_nysalg(250000.0, 110000.0)
    assert effective_adm_in(m) == 110000.0


def test_deal_equals_gross_stays_fully_admin():
    m = _admin_nysalg(250000.0, 250000.0)
    assert effective_adm_in(m) == 250000.0


def test_manual_override_beats_auto():
    m = _admin_nysalg(250000.0, 110000.0, adm_in_override=50000.0)
    assert effective_adm_in(m) == 50000.0


def test_include_override_without_match_takes_all():
    # Manuelt tvunget administrativ uden matchet deal => alt-eller-intet.
    m = _m(gross_in=250000.0, net_diff=250000.0, override="include")
    assert effective_adm_in(m) == 250000.0


def test_exclude_override_counts_as_plain_sale():
    m = _admin_nysalg(250000.0, 110000.0, override="exclude")
    assert effective_adm_in(m) == 0.0


def test_excluded_row_counts_zero():
    m = _admin_nysalg(250000.0, 110000.0, total_excluded=True)
    assert effective_adm_in(m) == 0.0


def test_non_admin_row_zero():
    m = _m(gross_in=250000.0, net_diff=250000.0)
    assert effective_adm_in(m) == 0.0


def test_auto_share_follows_gross_override():
    # Rettes gross in ned, beregnes andelen af den effektive værdi.
    m = _admin_nysalg(250000.0, 110000.0, gross_in_override=200000.0)
    assert effective_adm_in(m) == 110000.0
    m2 = _admin_nysalg(250000.0, 199000.0, gross_in_override=200000.0)
    assert effective_adm_in(m2) == 200000.0   # inden for 1 %-tolerancen


# ── Opsigelsessiden (effective_adm_out) ──────────────────────────────────────

def test_partial_admin_churn_uses_deal_value():
    m = _admin_opsigelse(109984.0, -50000.0)
    assert effective_adm_out(m) == 50000.0


def test_full_admin_churn_when_deal_matches_gross():
    m = _admin_opsigelse(109984.0, -109984.0)
    assert effective_adm_out(m) == 109984.0


def test_flag_only_churn_stays_all_or_nothing():
    # Kun Zuoras administrativ-flag (ingen matchet deal) => hele gross out.
    m = _admin_opsigelse(109984.0, None)
    assert effective_adm_out(m) == 109984.0


def test_manual_adm_out_override_beats_auto():
    m = _admin_opsigelse(109984.0, -50000.0, adm_out_override=60000.0)
    assert effective_adm_out(m) == 60000.0


# ── Totaler ──────────────────────────────────────────────────────────────────

def test_summarize_partial_admin_counts_rest_as_sale():
    """Netcompany-casen i topkortene: 140.000 skal tælle som rigtigt nysalg."""
    s = summarize([_admin_nysalg(250000.0, 110000.0)])
    assert s["brutto"] == 250000.0
    assert s["adm_nysalg"] == 110000.0
    assert s["netto_tilvaekst"] == 140000.0
    assert s["n_admin"] == 1


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

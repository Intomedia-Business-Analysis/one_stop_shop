"""Unit-tests for Monitor-rapporten (report_scope='monitor').

Monitor-rapporten viser KUN Monitor, opgjort pr. enkelt site — Business Media-
rapporten er alt andet. Testene dækker de rene helpers: site-normalisering
(join mellem Zuora- og budget-stavemåder), relabel til site-rækker og
rækkebygningen med site-budgetter.

Kører både under pytest og som standalone-script:
    python tests/test_admin_nysalg_monitor.py
"""
import os
import sys

# Gør repo-roden importerbar når filen køres direkte (uden pytest/conftest).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from moduler.modul_admin_nysalg.brands import brand_account, brand_geo  # noqa: E402
from moduler.modul_admin_nysalg.repo import (  # noqa: E402
    monitor_brand_rows, monitor_norm, monitor_relabel, summarize_by_brand,
)


def _m(site, gross_in=0.0, gross_out=0.0, **kw):
    """Match-række som dict (samme felter som repo.get_matches leverer)."""
    m = {
        "site": site, "brand": "Monitor", "month_end": "2026-06-30",
        "gross_in": gross_in, "gross_out": gross_out,
        "net_diff": gross_in - gross_out,
        "is_admin": False, "administrativ": False, "match_sign": None,
        "matched_value": None, "override": None,
        "gross_in_override": None, "gross_out_override": None,
        "adm_in_override": None, "adm_out_override": None,
        "total_excluded": False, "ambiguous": False,
    }
    m.update(kw)
    return m


# ── Normalisering (join-nøgle mellem Zuora og BudgetsIntoMedia) ───────────────

def test_monitor_norm_folds_spellings_to_same_key():
    # Zuora-form med landesuffiks, domæneform og budgetform skal mødes.
    assert monitor_norm("Sundhedsmonitor DK") == "sundhedsmonitor"
    assert monitor_norm("sundhedsmonitor.dk") == "sundhedsmonitor"
    assert monitor_norm("Sundhedsmonitor") == "sundhedsmonitor"


def test_monitor_norm_folds_danish_chars():
    # Budget-arket skriver 'idrætsmonitor' (småt + æ).
    assert monitor_norm("idrætsmonitor") == monitor_norm("Idrætsmonitor DK")


# ── Relabel (brand = site) ────────────────────────────────────────────────────

def test_relabel_sets_brand_to_site():
    rows = monitor_relabel([_m("Klimamonitor DK", gross_in=1000.0)])
    assert rows[0]["brand"] == "Klimamonitor DK"
    assert rows[0]["site"] == "Klimamonitor DK"   # site urørt


# ── Rækkebygning med site-budgetter ──────────────────────────────────────────

def test_monitor_rows_join_budget_on_movement_site():
    """Budget og bevægelser skal lande på SAMME række trods forskellig stavemåde."""
    matches = monitor_relabel([_m("Sundhedsmonitor DK", gross_in=5000.0)])
    norm_budgets = {"sundhedsmonitor": ("Sundhedsmonitor", 12000.0)}
    rows = monitor_brand_rows(matches, norm_budgets)
    assert len(rows) == 1
    r = rows[0]
    assert r["brand"] == "Sundhedsmonitor DK"   # bevægelsens navn vinder
    assert r["brutto"] == 5000.0
    assert r["budget"] == 12000.0


def test_monitor_rows_include_budget_only_sites():
    """Sites med budget men uden bevægelser i perioden vises med 0-tal."""
    matches = monitor_relabel([_m("Klimamonitor DK", gross_in=100.0)])
    norm_budgets = {
        "klimamonitor": ("Klimamonitor", 500.0),
        "byrummonitor": ("Byrummonitor", 900.0),   # ingen bevægelser
    }
    rows = monitor_brand_rows(matches, norm_budgets)
    by_brand = {r["brand"]: r for r in rows}
    assert by_brand["Byrummonitor"]["brutto"] == 0.0
    assert by_brand["Byrummonitor"]["budget"] == 900.0


def test_monitor_rows_no_business_media_defaults():
    """Marketwire/BUDGET_BRANDS-rækkerne fra Business Media må IKKE dukke op."""
    rows = monitor_brand_rows(monitor_relabel([_m("Klimamonitor DK", gross_in=1.0)]), {})
    assert [r["brand"] for r in rows] == ["Klimamonitor DK"]


def test_monitor_rows_sites_first_ads_last():
    """Abonnements-sites alfabetisk først; Job/Banner (annonce-rækker) sidst."""
    matches = monitor_relabel([
        _m("Sundhedsmonitor DK", gross_in=1.0),
        _m("Byrummonitor DK", gross_in=1.0),
    ])
    ad_rows = [
        {"brand": "Job", "brutto": 10.0, "adm_nysalg": 0.0, "opsigelser": 0.0,
         "adm_opsigelser": 0.0, "netto": 10.0, "budget": 20.0, "comment": "",
         "n_ambiguous": 0, "currency": "DKK"},
        {"brand": "Banner", "brutto": 5.0, "adm_nysalg": 0.0, "opsigelser": 0.0,
         "adm_opsigelser": 0.0, "netto": 5.0, "budget": 8.0, "comment": "",
         "n_ambiguous": 0, "currency": "DKK"},
    ]
    rows = monitor_brand_rows(matches, {}, extra_rows=ad_rows)
    assert [r["brand"] for r in rows] == \
        ["Byrummonitor DK", "Sundhedsmonitor DK", "Banner", "Job"]


def test_summarize_seed_defaults_off_skips_always_shown():
    rows = summarize_by_brand([], seed_defaults=False)
    assert rows == []
    # ensure_labels skaber 0-rækker uafhængigt af seed_defaults.
    rows = summarize_by_brand([], seed_defaults=False, ensure_labels=["Byrummonitor"])
    assert [r["brand"] for r in rows] == ["Byrummonitor"]


# ── Geo/konto-fallback for site-labels ───────────────────────────────────────

def test_monitor_site_labels_are_danish_subscription():
    assert brand_geo("Sundhedsmonitor DK") == ("Denmark", "Subscription")
    assert brand_geo("Byrummonitor") == ("Denmark", "Subscription")
    # Kendte brands er urørte.
    assert brand_geo("Watch NO") == ("Norway", "Subscription")
    assert brand_geo("Job") == ("Denmark", "Advertising")


def test_monitor_site_labels_use_monitor_account():
    assert brand_account("Sundhedsmonitor DK") == "monitor"
    assert brand_account("Watch DK") == "watch_medier"
    assert brand_account("Helt Ukendt") is None


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

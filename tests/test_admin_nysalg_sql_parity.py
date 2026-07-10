"""Paritetstest: SQL-aggregatet (Fase B) ↔ Python-referencen (effective_*).

Topkort- og per-brand-tallene beregnes i produktionen af repo._agg_sql som én
GROUP BY-query i SQL Server. Python-helperne (effective_gross_in/out,
effective_adm_in/out, summarize, summarize_by_brand) BEHOLDES som den
dokumenterede reference — denne test kører PRÆCIS samme query-tekst mod en
in-memory SQLite med et bredt sæt fixture-rækker (alle kombinationer af
overrides/flags/tolerance-grænser) og asserter at SQL == Python, både række
for række og aggregeret. Uden denne test VIL de to implementeringer drive
fra hinanden.

Kører både under pytest og som standalone-script:
    python tests/test_admin_nysalg_sql_parity.py
"""
import os
import sqlite3
import sys

# Gør repo-roden importerbar når filen køres direkte (uden pytest/conftest).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from moduler.modul_admin_nysalg import repo  # noqa: E402
from moduler.modul_admin_nysalg.brands import classify  # noqa: E402

# Kolonnerne SQL-aggregatet (og effective_*) læser fra admin_nysalg_match.
_COLS = ["run_id", "brand", "site", "net_diff", "gross_in", "gross_out",
         "administrativ", "matched_deal_id", "matched_value", "match_sign",
         "is_admin", "ambiguous", "override", "total_excluded",
         "gross_in_override", "gross_out_override",
         "adm_in_override", "adm_out_override"]


def _row(**kw) -> dict:
    """Match-række som dict med samme felter som repo.get_matches leverer."""
    m = {
        "run_id": 1, "brand": "Watch DK", "site": None,
        "net_diff": 0.0, "gross_in": 0.0, "gross_out": None,
        "administrativ": False, "matched_deal_id": None, "matched_value": None,
        "match_sign": None, "is_admin": False, "ambiguous": False,
        "override": None, "total_excluded": False,
        "gross_in_override": None, "gross_out_override": None,
        "adm_in_override": None, "adm_out_override": None,
    }
    m.update(kw)
    return m


def _fixture_rows() -> list[dict]:
    """Bredt sæt kombinationer af overrides/flags/tolerance-grænser."""
    return [
        # Nysalgssiden
        _row(gross_in=1000.0, net_diff=1000.0),                                   # alm. salg
        _row(gross_in=250000.0, net_diff=250000.0, is_admin=True,                 # helt adm.
             match_sign="pos", matched_value=250000.0, matched_deal_id="d1"),
        _row(gross_in=250000.0, net_diff=250000.0, is_admin=True,                 # delvist adm.
             match_sign="pos", matched_value=110000.0, matched_deal_id="d2"),
        _row(gross_in=250000.0, net_diff=250000.0, is_admin=True,                 # inden for 1 %
             match_sign="pos", matched_value=249000.0, matched_deal_id="d3"),
        _row(gross_in=250000.0, net_diff=250000.0, is_admin=True,                 # deal > gross
             match_sign="pos", matched_value=300000.0, matched_deal_id="d4"),
        _row(gross_in=5000.0, net_diff=5000.0, override="include"),               # manuel include
        _row(gross_in=250000.0, net_diff=250000.0, is_admin=True,                 # manuel exclude
             match_sign="pos", matched_value=110000.0, override="exclude"),
        _row(gross_in=250000.0, net_diff=250000.0, is_admin=True,                 # udeladt række
             match_sign="pos", matched_value=110000.0, total_excluded=True),
        _row(gross_in=100.0, net_diff=100.0, gross_in_override=0.0),              # rettet gross in
        _row(gross_in=250000.0, net_diff=250000.0, is_admin=True,                 # manuel adm-andel
             match_sign="pos", matched_value=110000.0, adm_in_override=50000.0),
        _row(gross_in=-100.0, net_diff=-100.0, is_admin=True,                     # negativ gross in
             match_sign="pos", matched_value=50.0, matched_deal_id="d5"),
        _row(brand="Finans", gross_in=700.0, net_diff=700.0),                     # andet brand
        _row(brand=None, site=None, gross_in=300.0, net_diff=300.0),              # NULL brand → Øvrige
        # Opsigelsessiden
        _row(gross_out=2000.0, net_diff=-2000.0),                                 # alm. churn
        _row(gross_out=None, net_diff=-1500.0),                                   # net_diff-fallback
        _row(gross_out=109984.0, net_diff=-109984.0, administrativ=True),         # flag-only adm.
        _row(gross_out=109984.0, net_diff=-109984.0,                              # delvist adm. churn
             match_sign="neg", matched_value=-50000.0, matched_deal_id="d6"),
        _row(gross_out=100000.0, net_diff=-100000.0,                              # inden for 1 %
             match_sign="neg", matched_value=-99500.0, matched_deal_id="d7"),
        _row(gross_out=109984.0, net_diff=-109984.0,                              # manuel adm-andel
             match_sign="neg", matched_value=-50000.0, adm_out_override=60000.0),
        _row(gross_out=5000.0, net_diff=-5000.0, gross_out_override=1234.56),     # rettet gross out
        _row(gross_out=5000.0, net_diff=-5000.0, total_excluded=True,             # udeladt adm. churn
             administrativ=True),
        # Diverse
        _row(gross_in=8000.0, net_diff=8000.0, is_admin=True, ambiguous=True,     # tvetydigt match
             match_sign="pos", matched_value=4000.0, matched_deal_id="d8"),
        _row(gross_in=0.0, net_diff=0.0, is_admin=True, matched_deal_id="d9"),    # adm. uden bevægelse
        _row(gross_in=0.0, net_diff=0.0),                                         # nul-række (filtreres)
        # Monitor (kun med i monitor-scope, grupperet pr. site)
        _row(brand="Monitor", site="Byrummonitor", gross_in=800.0, net_diff=800.0,
             is_admin=True, match_sign="pos", matched_value=300.0, matched_deal_id="m1"),
        _row(brand="Monitor", site="Klimamonitor", gross_out=400.0, net_diff=-400.0,
             administrativ=True),
        _row(brand="Monitor", site="", gross_in=50.0, net_diff=50.0),             # → 'Ukendt site'
    ]


def _make_db(rows: list[dict]) -> sqlite3.Connection:
    """In-memory SQLite med admin_nysalg_match + fixture-rækkerne.

    Hver række indsættes to gange: under sit run_id (aggregat-testene) og under
    et unikt run_id 1000+i (række-for-række-testen).
    """
    conn = sqlite3.connect(":memory:")
    conn.execute(f"CREATE TABLE admin_nysalg_match ({', '.join(_COLS)})")

    def _vals(m, run_id):
        vals = []
        for c in _COLS:
            v = run_id if c == "run_id" else m.get(c)
            if isinstance(v, bool):
                v = int(v)
            vals.append(v)
        return vals

    for i, m in enumerate(rows):
        conn.execute(
            f"INSERT INTO admin_nysalg_match VALUES ({','.join('?' * len(_COLS))})",
            _vals(m, m["run_id"]))
        conn.execute(
            f"INSERT INTO admin_nysalg_match VALUES ({','.join('?' * len(_COLS))})",
            _vals(m, 1000 + i))
    return conn


def _sql_groups(conn: sqlite3.Connection, run_id: int, scope: str) -> dict:
    """Kør PRÆCIS produktions-SQL'en (repo._agg_sql) mod SQLite."""
    conn.row_factory = sqlite3.Row
    cur = conn.execute(repo._agg_sql(scope, placeholder="?"), (run_id,))
    return {r["label"]: {
        "brutto": float(r["brutto"] or 0),
        "adm_nysalg": float(r["adm_nysalg"] or 0),
        "opsigelser": float(r["opsigelser"] or 0),
        "adm_opsigelser": float(r["adm_opsigelser"] or 0),
        "n_admin": int(r["n_admin"] or 0),
        "n_ambiguous": int(r["n_ambiguous"] or 0),
    } for r in cur.fetchall()}


def _visible(rows: list[dict], scope: str) -> list[dict]:
    """Python-referencens scope-filter (spejler router._visible_matches)."""
    def _label(m):
        return m.get("brand") or classify(m.get("site"))
    if scope == "monitor":
        return repo.monitor_relabel([m for m in rows if _label(m) == "Monitor"])
    return [m for m in rows if _label(m) != "Monitor"]


def _assert_close(a: float, b: float, ctx: str) -> None:
    assert abs(a - b) < 1e-6, f"{ctx}: SQL {a} != Python {b}"


def _assert_groups_match_python(groups: dict, visible: list[dict], scope: str) -> None:
    """SQL-grupperne == summarize_by_brand + summarize over samme rækker."""
    py_rows = {r["brand"]: r for r in repo.summarize_by_brand(visible)}
    for label, g in groups.items():
        py = py_rows.get(label)
        assert py is not None, f"{scope}: SQL-gruppe {label!r} findes ikke i Python"
        for k in ("brutto", "adm_nysalg", "opsigelser", "adm_opsigelser"):
            _assert_close(round(g[k], 2), py[k], f"{scope}/{label}/{k}")
        netto = round((round(g["brutto"], 2) - round(g["adm_nysalg"], 2))
                      - (round(g["opsigelser"], 2) - round(g["adm_opsigelser"], 2)), 2)
        _assert_close(netto, py["netto"], f"{scope}/{label}/netto")
        assert g["n_ambiguous"] == py["n_ambiguous"], f"{scope}/{label}/n_ambiguous"
    # Python-buckets uden SQL-gruppe skal være rene 0-rækker (seedede, fx Marketwire).
    for label, py in py_rows.items():
        if label not in groups:
            assert all(py[k] == 0 for k in ("brutto", "adm_nysalg", "opsigelser",
                                            "adm_opsigelser", "netto")), \
                f"{scope}: Python-bucket {label!r} mangler i SQL men er ikke 0"
    # Aggregeret (topkortene).
    sql_sum = repo.summarize_from_groups(groups)
    py_sum = repo.summarize(visible)
    for k, v in py_sum.items():
        _assert_close(float(sql_sum[k]), float(v), f"{scope}/summary/{k}")


# ── Aggregeret paritet ───────────────────────────────────────────────────────

def test_business_media_aggregate_matches_python():
    rows = _fixture_rows()
    conn = _make_db(rows)
    groups = _sql_groups(conn, 1, "business_media")
    _assert_groups_match_python(groups, _visible(rows, "business_media"),
                                "business_media")


def test_monitor_aggregate_matches_python_per_site():
    rows = _fixture_rows()
    conn = _make_db(rows)
    groups = _sql_groups(conn, 1, "monitor")
    assert set(groups) == {"Byrummonitor", "Klimamonitor", "Ukendt site"}
    _assert_groups_match_python(groups, _visible(rows, "monitor"), "monitor")


def test_exclude_brands_matches_python_hidden():
    """summarize_from_groups(exclude_brands=…) == summarize uden de skjulte brands."""
    rows = _fixture_rows()
    conn = _make_db(rows)
    groups = _sql_groups(conn, 1, "business_media")
    visible = [m for m in _visible(rows, "business_media")
               if (m.get("brand") or classify(m.get("site"))) != "Watch DK"]
    sql_sum = repo.summarize_from_groups(groups, exclude_brands={"Watch DK"})
    py_sum = repo.summarize(visible)
    for k, v in py_sum.items():
        _assert_close(float(sql_sum[k]), float(v), f"hidden/{k}")


# ── Række-for-række-paritet ──────────────────────────────────────────────────

def test_row_level_parity():
    """Hver fixture-række alene i sit eget run: SQL == effective_* pr. række."""
    rows = _fixture_rows()
    conn = _make_db(rows)
    for i, m in enumerate(rows):
        scope = "monitor" if m.get("brand") == "Monitor" else "business_media"
        groups = _sql_groups(conn, 1000 + i, scope)
        eff = {
            "brutto": repo.effective_gross_in(m),
            "adm_nysalg": repo.effective_adm_in(m),
            "opsigelser": repo.effective_gross_out(m),
            "adm_opsigelser": repo.effective_adm_out(m),
        }
        if not groups:
            # Rækken faldt for bidrags-filteret — så skal den også bidrage 0.
            assert all(v == 0 for v in eff.values()), f"række {i}: filtreret men != 0"
            assert not repo.effective_is_admin(m), f"række {i}: filtreret men admin"
            continue
        assert len(groups) == 1, f"række {i}: {len(groups)} grupper"
        g = next(iter(groups.values()))
        for k, v in eff.items():
            _assert_close(g[k], float(v), f"række {i}/{k}")
        exp_admin = 1 if (repo.effective_is_admin(m) or eff["adm_nysalg"] > 0) else 0
        assert g["n_admin"] == exp_admin, f"række {i}/n_admin"
        assert g["n_ambiguous"] == (1 if m.get("ambiguous") else 0), f"række {i}/n_ambiguous"


def test_non_contributing_row_is_filtered_in_sql():
    """Nul-rækken (ingen bevægelse/markering) må slet ikke give en gruppe."""
    rows = _fixture_rows()
    conn = _make_db(rows)
    idx = next(i for i, m in enumerate(rows)
               if not m["gross_in"] and not m.get("net_diff")
               and not m["is_admin"] and not m.get("matched_deal_id")
               and not m.get("administrativ"))
    assert _sql_groups(conn, 1000 + idx, "business_media") == {}


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

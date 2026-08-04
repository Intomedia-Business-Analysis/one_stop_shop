"""Unit-tests for sti → nav-item-opslaget bag "senest besøgt".

Middleware'en kender kun den besøgte sti; favoritter/seneste arbejder på
item-id'er. Går oversættelsen i stykker, holder listen op med at blive fyldt
UDEN at noget fejler — derfor er reglerne fredet her.

Kører både under pytest og som standalone-script:
    python tests/test_nav_recent.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nav_utils import CATEGORIES, resolve_item_id, visible_items_by_id  # noqa: E402


def _admin():
    return {"id": 1, "role": "admin", "brand": None,
            "_resource_access": {}, "_role_access": {}, "_teams": []}


def test_exact_url_matches_item():
    assert resolve_item_id("/tools/klippekort/") == "klippekort-overblik"
    assert resolve_item_id("/tools/performance/saelger") == "kpi-saelger"


def test_trailing_slash_is_ignored():
    # Registret skriver '/tools/klippekort/', browseren kan sende begge former.
    assert resolve_item_id("/tools/klippekort") == "klippekort-overblik"


def test_query_variant_gets_its_own_item():
    """Sales Performance NO adskiller sig KUN på query-strengen."""
    assert resolve_item_id("/tools/rotation/sales-performance") == "rotation-sales"
    assert resolve_item_id("/tools/rotation/sales-performance",
                           "teams=Team%20Watch%20NO") == "rotation-sales-no"


def test_unknown_query_falls_back_to_the_path():
    # Filtre i URL'en må ikke få besøget til at forsvinde.
    assert resolve_item_id("/tools/klippekort/", "maaned=2026-06") == "klippekort-overblik"


def test_subpage_counts_as_a_visit_to_the_tool():
    assert resolve_item_id("/tools/admin-nysalg/35/review") == "admin-nysalg"
    assert resolve_item_id("/tool/barselsberegner/app") == "barselsberegner"


def test_longest_match_wins():
    """/tools/rotation/ er selv et item og præfiks for de andre rotationssider."""
    assert resolve_item_id("/tools/rotation/") == "rotation-autoplay"
    assert resolve_item_id("/tools/rotation/job-performance") == "rotation-job"


def test_navigation_pages_are_not_items():
    for path in ("/", "/settings", "/category/hr", "/admin/users", "/favorites"):
        assert resolve_item_id(path) is None, path


def test_every_registered_item_resolves_to_itself():
    """Alle items i registret skal kunne findes ud fra deres egen URL."""
    for cat in CATEGORIES:
        for item in cat["items"]:
            path, _, query = item["url"].partition("?")
            assert resolve_item_id(path, query) == item["id"], item["url"]


def test_visible_items_by_id_carries_the_category_title():
    by_id = visible_items_by_id(_admin())
    assert by_id["klippekort-overblik"]["category"] == "Banner & Job"


def test_visible_items_by_id_respects_access():
    saelger = {"id": 2, "role": "salesperson", "brand": None,
               "_resource_access": {}, "_role_access": {}, "_teams": []}
    by_id = visible_items_by_id(saelger)
    assert "admin-nysalg" not in by_id      # kræver 'management'
    assert "forecast-tool" in by_id


if __name__ == "__main__":
    funcs = [v for k, v in sorted(globals().items())
             if k.startswith("test_") and callable(v)]
    for fn in funcs:
        fn()
        print(f"  ok  {fn.__name__}")
    print(f"\n{len(funcs)}/{len(funcs)} tests bestået")

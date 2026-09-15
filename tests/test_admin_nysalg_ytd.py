"""Unit-tests for ÅTD-tabellen, baseline og forecast-kolonnen.

Tre ting testes uden database:
  forecast.py         – de hardcodede månedstal rammer de rigtige brand-labels
  baseline-konvertering – rapportens brand-rækker → sale/churn uden adm-andelen
  ytd_brand_rows      – baseline (tidligere måneder) + runnets egne tal for
                        rapportmåneden, med budget hentet for hele ÅTD-perioden

De to DB-kald i ytd_brand_rows (get_baseline og ytd_budgets) byttes ud med
fixtures via monkeypatch, så testen kører uden SQL Server.

Kører både under pytest og som standalone-script:
    python tests/test_admin_nysalg_ytd.py
"""
import os
import sys

# Gør repo-roden importerbar når filen køres direkte (uden pytest/conftest).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from moduler.modul_admin_nysalg import forecast, repo  # noqa: E402


def _brand_row(brand, brutto=0.0, adm_nysalg=0.0, opsigelser=0.0,
               adm_opsigelser=0.0, currency="DKK"):
    """Brand-række i samme facon som summarize_by_brand leverer."""
    return {"brand": brand, "brutto": brutto, "adm_nysalg": adm_nysalg,
            "opsigelser": opsigelser, "adm_opsigelser": adm_opsigelser,
            "netto": (brutto - adm_nysalg) - (opsigelser - adm_opsigelser),
            "budget": 0.0, "currency": currency, "comment": "", "n_ambiguous": 0}


# ── forecast.py ──────────────────────────────────────────────────────────────

def test_forecast_rammer_september():
    """Septembertallene fra ledelsens regneark, pr. brand-label."""
    fc = forecast.month_forecast("2026-09")
    assert fc["Watch DK"] == 610_000
    assert fc["Job"] == 1_000_000
    assert fc["Banner"] == 530_000
    assert fc["Finans"] == 152_000
    assert fc["Watch NO"] == 600_000


def test_forecast_finans_starter_i_september():
    """Finans-abonnement har først forecast fra september — august er tomt."""
    assert "Finans" not in forecast.month_forecast("2026-08")
    assert forecast.month_forecast("2026-08")["Watch DK"] == 480_000


def test_forecast_summeres_over_flere_maaneder():
    """Et run der spænder over flere måneder får summen af månedernes forecast."""
    fc = forecast.range_forecast(["2026-07", "2026-08", "2026-09"])
    assert fc["Job"] == 200_000 + 700_000 + 1_000_000
    assert fc["Finans"] == 152_000       # kun september bidrager


def test_forecast_attach_saetter_none_uden_forecast():
    """Brands uden forecast får None (vises '—'), ikke 0 — de er ikke det samme."""
    rows = forecast.attach([_brand_row("Watch DK"), _brand_row("Marketwire")],
                           ["2026-09"])
    assert rows[0]["forecast"] == 610_000
    assert rows[1]["forecast"] is None


def test_forecast_gaelder_ikke_monitor():
    """Monitor-rapportens rækker er enkelt-sites og har intet forecast."""
    assert forecast.month_forecast("2026-09", scope="monitor") == {}


def test_forecast_labels_findes_i_display_order():
    """Et stavefejlet label ville give en tom kolonne — _validate fanger det."""
    from moduler.modul_admin_nysalg.brands import DISPLAY_ORDER
    for brand in forecast.FORECAST:
        assert brand in DISPLAY_ORDER


# ── Baseline-konvertering ────────────────────────────────────────────────────

def test_baseline_traekker_administrative_fra():
    """Baseline gemmer Actual Sale/Churn — altså ekskl. administrative beløb."""
    rows = repo.baseline_rows_from_brand_rows([
        _brand_row("Watch DK", brutto=250_000, adm_nysalg=110_000,
                   opsigelser=60_000, adm_opsigelser=10_000)])
    assert rows == [{"brand": "Watch DK", "sale": 140_000.0, "churn": 50_000.0,
                     "currency": "DKK"}]


def test_baseline_udelader_underraekker():
    """Underrækker er en drill-down af hovedrækken og ville dobbelttælle i ÅTD."""
    row = _brand_row("Norge Job", brutto=100_000, currency="NOK")
    row["subrows"] = [_brand_row("M24", brutto=60_000, currency="NOK")]
    assert [r["brand"] for r in repo.baseline_rows_from_brand_rows([row])] == ["Norge Job"]


# ── ytd_brand_rows ───────────────────────────────────────────────────────────

def _patch_ytd(monkeypatch, baseline, budgets=None):
    monkeypatch.setattr(repo, "get_baseline", lambda *a, **k: baseline)
    monkeypatch.setattr(repo, "ytd_budgets", lambda *a, **k: budgets or {})


def test_ytd_lægger_baseline_og_rapportmaaned_sammen(monkeypatch):
    """Juli+august fra baseline, september fra runnet man sidder i."""
    _patch_ytd(monkeypatch, {
        "2026-07": {"Watch DK": {"brand": "Watch DK", "sale": 100.0, "churn": 10.0,
                                 "currency": "DKK"}},
        "2026-08": {"Watch DK": {"brand": "Watch DK", "sale": 200.0, "churn": 20.0,
                                 "currency": "DKK"}},
    }, budgets={"Watch DK": 500.0})
    block = repo.ytd_brand_rows(
        "business_media", "2026-07-01", "2026-09-30",
        [_brand_row("Watch DK", brutto=300.0, opsigelser=30.0)], ["2026-09"])
    row = block["rows"][0]
    assert row["brutto"] == 600.0          # 100 + 200 + 300
    assert row["opsigelser"] == 60.0       # 10 + 20 + 30
    assert row["netto"] == 540.0
    assert row["budget"] == 500.0
    assert block["missing"] == []


def test_ytd_rapportmaaned_vinder_over_gemt_baseline(monkeypatch):
    """Kører man september om, skal reviewet man kigger på slå den gemte måned."""
    _patch_ytd(monkeypatch, {
        "2026-09": {"Watch DK": {"brand": "Watch DK", "sale": 999.0, "churn": 0.0,
                                 "currency": "DKK"}},
    })
    block = repo.ytd_brand_rows(
        "business_media", "2026-09-01", "2026-09-30",
        [_brand_row("Watch DK", brutto=300.0)], ["2026-09"])
    assert block["rows"][0]["brutto"] == 300.0


def test_ytd_melder_maaneder_uden_baseline(monkeypatch):
    """Manglende måneder tæller som 0, men skal kunne vises som advarsel."""
    _patch_ytd(monkeypatch, {})
    block = repo.ytd_brand_rows(
        "business_media", "2026-07-01", "2026-09-30",
        [_brand_row("Watch DK", brutto=300.0)], ["2026-09"])
    assert block["missing"] == ["2026-07", "2026-08"]
    assert block["rows"][0]["brutto"] == 300.0


def test_ytd_beholder_lokal_valuta(monkeypatch):
    """Norge må ikke ende i DKK, når tallene kommer fra baseline."""
    _patch_ytd(monkeypatch, {
        "2026-08": {"Watch NO": {"brand": "Watch NO", "sale": 543_000.0,
                                 "churn": 0.0, "currency": "NOK"}},
    })
    block = repo.ytd_brand_rows("business_media", "2026-08-01", "2026-09-30",
                                [], ["2026-09"])
    assert block["rows"][0]["currency"] == "NOK"


def test_ytd_uden_periode_giver_tom_blok(monkeypatch):
    """Uden datointerval kan månederne ikke udledes — ingen ÅTD-tabel."""
    _patch_ytd(monkeypatch, {})
    assert repo.ytd_brand_rows("business_media", None, None, [], [])["rows"] == []


# ── save_baseline: hvad en rapportkørsel må og ikke må overskrive ────────────
# Reglen er hele sikkerheden bag "test bare rapporten": alt hvad direktøren selv
# har tastet på baseline-siden gemmes med source='manual' og skal overleve, at
# en rapport for samme måned køres igen. Testes mod en stub-forbindelse, så der
# hverken kræves database eller skrives noget.

class _FakeCursor:
    """Cursor der svarer på SELECT'en og husker alle udførte statements."""

    def __init__(self, eksisterende):
        self._eksisterende = eksisterende
        self.statements = []

    def execute(self, sql, params=()):
        self.statements.append((" ".join(sql.split()), params))

    def fetchall(self):
        return [{"brand": b, "source": src} for b, src in self._eksisterende.items()]


class _FakeConn:
    def __init__(self, eksisterende):
        self.cur = _FakeCursor(eksisterende)
        self.committed = False

    def cursor(self, **_kw):
        return self.cur

    def commit(self):
        self.committed = True

    def close(self):
        pass


def _koer_save(monkeypatch, eksisterende, rows, **kw):
    conn = _FakeConn(eksisterende)
    monkeypatch.setattr(repo, "get_conn", lambda *a, **k: conn)
    n = repo.save_baseline("business_media", "2026-09", rows, **kw)
    inserts = [p for sql, p in conn.cur.statements if sql.startswith("INSERT")]
    deletes = [p for sql, p in conn.cur.statements if sql.startswith("DELETE")]
    return n, inserts, deletes, conn


def test_rapport_overskriver_ikke_manuelt_tastede_raekker(monkeypatch):
    """Det man selv har rettet, skal stå uændret efter en ny rapportkørsel."""
    n, inserts, deletes, _ = _koer_save(
        monkeypatch,
        {"Watch DK": "manual", "Job": "run"},
        [{"brand": "Watch DK", "sale": 999.0, "churn": 0.0},
         {"brand": "Job", "sale": 500.0, "churn": 0.0}],
        source="run", keep_manual=True)
    rørte = {p[2] for p in inserts} | {p[2] for p in deletes}
    assert "Watch DK" not in rørte, "manuel række blev rørt af en rapportkørsel"
    assert "Job" in rørte and n == 1


def test_manuel_gem_overskriver_alt(monkeypatch):
    """Retter man selv på baseline-siden, vinder man over en rapport-række."""
    n, inserts, _, _ = _koer_save(
        monkeypatch, {"Job": "run"},
        [{"brand": "Job", "sale": 500.0, "churn": 0.0}], source="manual")
    assert n == 1 and inserts[0][2] == "Job" and inserts[0][6] == "manual"


def test_nulstillet_raekke_slettes_i_stedet_for_at_gemmes(monkeypatch):
    """Tømmer man begge felter, skal rækken forsvinde fra ÅTD — ikke stå som 0."""
    n, inserts, deletes, _ = _koer_save(
        monkeypatch, {"Job": "manual"},
        [{"brand": "Job", "sale": 0, "churn": 0}], source="manual")
    assert n == 0 and not inserts
    assert deletes and deletes[0][2] == "Job"


# ── Standalone-runner (uden pytest) ──────────────────────────────────────────

class _Monkey:
    """Minimal monkeypatch-erstatning, så filen også kan køres direkte."""

    def __init__(self):
        self._undo = []

    def setattr(self, obj, name, value):
        self._undo.append((obj, name, getattr(obj, name)))
        setattr(obj, name, value)

    def undo(self):
        for obj, name, old in reversed(self._undo):
            setattr(obj, name, old)
        self._undo.clear()


if __name__ == "__main__":
    import inspect

    funcs = [v for k, v in sorted(globals().items())
             if k.startswith("test_") and callable(v)]
    passed = 0
    for fn in funcs:
        mp = _Monkey()
        try:
            fn(mp) if "monkeypatch" in inspect.signature(fn).parameters else fn()
        finally:
            mp.undo()
        print(f"  ok  {fn.__name__}")
        passed += 1
    print(f"\n{passed}/{len(funcs)} tests bestået")

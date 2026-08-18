"""Tests for Medie Benchmark.

Fredet adfærd:
  - dashboardet er lukket for alt under 'management' (ledelse + admin), både på
    siden, på nav-item'et og på data-endpointsene
  - serie-parsing: parallelle query-lister skal parres på position, og skæve
    input skal give 400 og ikke en halv sammenligning
  - datogrundlag og interval kommer fra whitelists — de interpoleres direkte
    ind i SQL, så en ukendt værdi skal falde tilbage til default, aldrig lande
    i queryen
  - SQL-parametrene i bucket-queryen skal ligge i samme rækkefølge som %s'erne
    (scope-filteret står MELLEM datofiltrene og GROUP BY)

Testene kører uden DB — datalaget monkeypatches, og adgang sættes via
dependency_overrides. Se tests/conftest.py.
"""
import pytest


# ---------------------------------------------------------------------------
# Adgang
# ---------------------------------------------------------------------------

BENCHMARK_PATHS = [
    "/tools/benchmark/medier",
    "/tools/benchmark/filters",
]


@pytest.mark.parametrize("path", BENCHMARK_PATHS)
def test_benchmark_kraever_login(client, path):
    r = client.get(path)
    assert r.status_code == 302


@pytest.mark.parametrize("role", ["salesperson", "sales_manager", "sales_operations", "marketing"])
def test_benchmark_side_kraever_management(client, make_user, auth_override, role):
    """Marketing har rang 4 og må se Deal Source, men IKKE benchmarken —
    dashboardet er forbeholdt ledelsen."""
    auth_override(make_user(role=role))
    assert client.get("/tools/benchmark/medier").status_code == 403


@pytest.mark.parametrize("role", ["management", "admin"])
def test_benchmark_side_aaben_for_ledelse_og_admin(client, make_user, auth_override, role):
    auth_override(make_user(role=role))
    r = client.get("/tools/benchmark/medier")
    assert r.status_code == 200
    assert "Medie Benchmark" in r.text


@pytest.mark.parametrize("path,params", [
    ("/tools/benchmark/filters",        {}),
    ("/tools/benchmark/first-activity", {"site": "DetailWatch DK"}),
    ("/tools/benchmark/compare",        {"s_sites": "DetailWatch DK", "s_start": "2022-03-08"}),
    ("/tools/benchmark/deals",          {"site": "DetailWatch DK", "start_date": "2022-03-08",
                                         "window_days": 8}),
])
def test_benchmark_data_endpoints_kraever_management(client, make_user, auth_override, path, params):
    auth_override(make_user(role="marketing"))
    assert client.get(path, params=params).status_code == 403


def test_benchmark_nav_kun_synlig_for_management(make_user):
    from nav_utils import CATEGORIES, filter_categories

    def item_ids(role):
        cats = filter_categories(CATEGORIES, make_user(role=role))
        kpi = next((c for c in cats if c["id"] == "kpi-dashboards"), None)
        return {i["id"] for i in (kpi or {"items": []})["items"]}

    assert "benchmark-medier" in item_ids("management")
    assert "benchmark-medier" in item_ids("admin")
    for role in ("salesperson", "sales_manager", "sales_operations", "marketing"):
        assert "benchmark-medier" not in item_ids(role), role


def test_benchmark_nav_min_role_matcher_routeren():
    """Nav-item og router skal kræve samme rolle — ellers får brugeren et
    menupunkt der svarer 403."""
    from moduler.modul_benchmark.router import MIN_ROLLE
    from nav_utils import CATEGORIES

    item = next(i for c in CATEGORIES for i in c["items"] if i["id"] == "benchmark-medier")
    assert item["min_role"] == MIN_ROLLE
    assert item["url"] == "/tools/benchmark/medier"


# ---------------------------------------------------------------------------
# Serie-parsing
# ---------------------------------------------------------------------------

def test_parse_series_parrer_paa_position():
    from moduler.modul_benchmark.router import _parse_series

    out = _parse_series(
        ["DetailWatch DK", "NordicDefenceWatch"],
        ["2022-03-08", "2026-08-11"],
        ["DetailWatch", ""],
    )
    assert out == [
        {"sites": ["DetailWatch DK"],     "start_date": "2022-03-08", "label": "DetailWatch"},
        {"sites": ["NordicDefenceWatch"], "start_date": "2026-08-11", "label": ""},
    ]


def test_parse_series_deler_flere_sites_pr_serie():
    """En serie kan dække stavevarianter af samme medie."""
    from moduler.modul_benchmark.router import _parse_series

    out = _parse_series(["FødevareWatch DK,Fødevare Watch DK"], ["2020-01-01"], None)
    assert out[0]["sites"] == ["FødevareWatch DK", "Fødevare Watch DK"]


@pytest.mark.parametrize("sites,starts", [
    (["A", "B"], ["2022-03-08"]),          # færre datoer end serier
    (["A"],      ["2022-03-08", "x"]),     # flere datoer end serier
    ([],         []),                      # ingen serier
    (["A", "B", "C", "D", "E"], ["2022-01-01"] * 5),  # over SERIES_MAX
])
def test_parse_series_afviser_skaeve_input(sites, starts):
    from moduler.modul_benchmark.router import _parse_series

    with pytest.raises(ValueError):
        _parse_series(sites, starts, None)


def test_compare_giver_400_paa_ugyldig_dato(client, make_user, auth_override):
    auth_override(make_user(role="admin"))
    r = client.get("/tools/benchmark/compare",
                   params={"s_sites": "DetailWatch DK", "s_start": "08-03-2022"})
    assert r.status_code == 400
    assert "ÅÅÅÅ-MM-DD" in r.json()["detail"]


def test_compare_giver_400_naar_datoer_mangler(client, make_user, auth_override):
    auth_override(make_user(role="admin"))
    r = client.get("/tools/benchmark/compare",
                   params=[("s_sites", "A"), ("s_sites", "B"), ("s_start", "2022-03-08")])
    assert r.status_code == 400


def test_datafejl_giver_500_uden_interne_detaljer(client, make_user, auth_override, monkeypatch):
    import moduler.modul_benchmark.router as bm

    def _kaster(*a, **kw):
        raise RuntimeError("Simuleret databasefejl")

    monkeypatch.setattr(bm, "db_compare", _kaster)
    auth_override(make_user(role="admin"))
    r = client.get("/tools/benchmark/compare",
                   params={"s_sites": "DetailWatch DK", "s_start": "2022-03-08"})
    assert r.status_code == 500
    assert "Simuleret databasefejl" not in r.text


# ---------------------------------------------------------------------------
# Whitelists: datogrundlag og interval interpoleres ind i SQL
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad", [
    "add_time; DROP TABLE PipedriveDeals--",
    "update_time",   # findes i tabellen, men er ikke på whitelisten
    "",
    None,
])
def test_ukendt_datogrundlag_falder_tilbage_til_default(bad):
    from moduler.modul_benchmark.queries import DEFAULT_DATE_BASIS, resolve_date_basis

    assert resolve_date_basis(bad) == DEFAULT_DATE_BASIS


@pytest.mark.parametrize("bad", ["quarter", "1", "", None])
def test_ukendt_interval_falder_tilbage_til_default(bad):
    from moduler.modul_benchmark.queries import DEFAULT_BUCKET, resolve_bucket

    assert resolve_bucket(bad) == DEFAULT_BUCKET


def test_date_basis_kolonner_er_kendte_pipedrive_kolonner():
    """Værdierne interpoleres direkte ind i SQL — de skal være hårdkodede
    kolonnenavne og ikke kunne indeholde noget andet."""
    import re

    from moduler.modul_benchmark.queries import DATE_BASIS

    for key, col in DATE_BASIS.items():
        assert re.fullmatch(r"d\.\[\w+\]", col), col
        assert col == f"d.[{key}]"


# ---------------------------------------------------------------------------
# Vinduet
# ---------------------------------------------------------------------------

class FakeCur:
    """Opsamler (sql, params) pr. execute og svarer med tomme aggregater."""

    def __init__(self):
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))

    def fetchone(self):
        return {}

    def fetchall(self):
        return []


@pytest.fixture
def fake_conn(monkeypatch):
    """Erstat get_conn i benchmark-modulet med en cursor der ikke rører DB."""
    import moduler.modul_benchmark.queries as q

    cur = FakeCur()

    class FakeConn:
        def cursor(self, as_dict=False):
            return cur

        def close(self):
            pass

    monkeypatch.setattr(q, "get_conn", lambda: FakeConn())
    return cur


def test_auto_vindue_er_det_yngste_medies_alder(fake_conn):
    """Uden window_days måles alle serier over den periode hvor ALLE har data —
    ellers ville DetailWatch' fire år blive holdt op mod NDW's første uge."""
    from datetime import date

    from moduler.modul_benchmark.queries import db_compare

    d = db_compare(
        series=[
            {"sites": ["DetailWatch DK"],     "start_date": "2022-03-08"},
            {"sites": ["NordicDefenceWatch"], "start_date": "2026-08-11"},
        ],
        today=date(2026, 8, 18),
    )
    assert d["window_days"] == 8          # 11.–18. august inkl. begge dage
    assert d["auto_window_days"] == 8
    assert d["window_is_auto"] is True
    assert [s["days_available"] for s in d["series"]] == [1625, 8]
    # Vinduets slutdato vises inklusivt for brugeren
    assert d["series"][1]["window_end"] == "2026-08-18"
    assert d["series"][0]["window_end"] == "2022-03-15"


def test_manuelt_vindue_overstyrer_auto(fake_conn):
    from datetime import date

    from moduler.modul_benchmark.queries import db_compare

    d = db_compare(
        series=[{"sites": ["DetailWatch DK"], "start_date": "2022-03-08"}],
        window_days=365,
        today=date(2026, 8, 18),
    )
    assert d["window_days"] == 365
    assert d["window_is_auto"] is False
    assert d["series"][0]["window_end"] == "2023-03-07"


def test_label_falder_tilbage_til_site_navnene(fake_conn):
    from datetime import date

    from moduler.modul_benchmark.queries import db_compare

    d = db_compare(
        series=[
            {"sites": ["A", "B"], "start_date": "2022-01-01"},
            {"sites": ["C"], "start_date": "2022-01-01", "label": "  Eget navn  "},
        ],
        today=date(2026, 8, 18),
    )
    assert [s["label"] for s in d["series"]] == ["A, B", "Eget navn"]


def test_bucket_query_parametre_staar_i_sql_raekkefoelge(fake_conn):
    """%s'erne i bucket-queryen kommer i rækkefølgen start, bucket, start, slut,
    <scope>, start, bucket — scope-filteret står MELLEM datofiltrene og GROUP BY.

    Bytter man om, afviser SQL Server queryen med 'not contained in either an
    aggregate function or the GROUP BY clause', fordi SELECT- og GROUP
    BY-udtrykkene ikke længere er ens efter parameter-indsættelse.
    """
    from datetime import date

    from moduler.modul_benchmark.queries import db_compare

    db_compare(
        series=[{"sites": ["DetailWatch DK"], "start_date": "2022-03-08"}],
        pipelines=["Company Trial"],
        bucket="week",
        window_days=7,
        today=date(2026, 8, 18),
    )
    # NB: flere queries har GROUP BY (også valuta-fordelingen) — match på
    # bucket_idx, som kun bucket-queryen har.
    sql, params = next((c for c in fake_conn.calls if "bucket_idx" in c[0]), (None, None))
    assert sql is not None, "bucket-queryen blev ikke kørt"
    assert params == ("2022-03-08", 7, "2022-03-08", "2022-03-15",
                      "DetailWatch DK", "Company Trial",
                      "2022-03-08", 7)
    # Ét %s pr. parameter — ellers er de forskudt uanset rækkefølgen
    assert sql.count("%s") == len(params)


def test_totals_query_bruger_eksklusiv_oevre_graense(fake_conn):
    """Datokolonnerne er datetime, så '<= sidste dag' ville skære alt efter
    midnat på den dag væk."""
    from datetime import date

    from moduler.modul_benchmark.queries import db_compare

    db_compare(
        series=[{"sites": ["X"], "start_date": "2026-08-11"}],
        window_days=8,
        today=date(2026, 8, 18),
    )
    sql, params = fake_conn.calls[0]
    assert "<= %s" not in sql
    assert params[0] == "2026-08-19"      # dag 8 slutter FØR den 19.
    assert sql.count("%s") == len(params)


def test_series_deals_parametre_matcher_placeholders(fake_conn):
    from moduler.modul_benchmark.queries import db_series_deals

    db_series_deals(["NordicDefenceWatch"], "2026-08-11", 8,
                    date_basis="add_time", pipelines=["Company Trial"])
    sql, params = fake_conn.calls[0]
    assert sql.count("%s") == len(params)
    assert params[:3] == ("2026-08-11", "2026-08-11", "2026-08-19")


@pytest.mark.parametrize("series,fejl", [
    ([], "mindst"),
    ([{"sites": [], "start_date": "2022-01-01"}], "mangler et medie"),
    ([{"sites": ["A"], "start_date": "ikke-en-dato"}], "ÅÅÅÅ-MM-DD"),
    ([{"sites": ["A"], "start_date": "2099-01-01"}], "fremtiden"),
    ([{"sites": ["A"], "start_date": "2022-01-01"}] * 5, "Højst"),
])
def test_compare_afviser_ugyldigt_input(fake_conn, series, fejl):
    from datetime import date

    from moduler.modul_benchmark.queries import db_compare

    with pytest.raises(ValueError) as e:
        db_compare(series=series, today=date(2026, 8, 18))
    assert fejl in str(e.value)


def test_adm_filter_kan_slaas_af(fake_conn):
    from datetime import date

    from moduler.modul_benchmark.queries import db_compare

    db_compare(series=[{"sites": ["A"], "start_date": "2022-01-01"}],
               exclude_adm=False, today=date(2026, 8, 18))
    assert all("administrativ" not in c[0] for c in fake_conn.calls)

    fake_conn.calls.clear()
    db_compare(series=[{"sites": ["A"], "start_date": "2022-01-01"}],
               exclude_adm=True, today=date(2026, 8, 18))
    assert all("administrativ" in c[0] for c in fake_conn.calls)


# ---------------------------------------------------------------------------
# Slettede deals, Web Sale og valuta
# ---------------------------------------------------------------------------

def test_slettede_deals_udelukkes_altid(fake_conn):
    """status='deleted' er Pipedrives gravsten, ikke en forretningstilstand.

    Blev de talt med i 'deals i vinduet' men ikke i won/open/lost, gik kortet
    ikke op — NordicDefenceWatch viste 145 deals mod 4+117+23=144.
    """
    from datetime import date

    from moduler.modul_benchmark.queries import db_compare

    db_compare(series=[{"sites": ["A"], "start_date": "2022-01-01"}],
               statuses=["won"], today=date(2026, 8, 18))
    assert fake_conn.calls, "ingen queries kørt"
    for sql, _ in fake_conn.calls:
        assert "<> 'deleted'" in sql, sql[:200]


def test_deleted_er_ikke_en_valgmulighed_i_status_dropdown(monkeypatch):
    """Kunne man vælge 'deleted', ville resultatet altid være tomt."""
    import moduler.modul_benchmark.queries as q

    class Cur:
        def __init__(self):
            self.sqls = []

        def execute(self, sql, params=None):
            self.sqls.append(sql)

        def fetchall(self):
            return []

    cur = Cur()

    class Conn:
        def cursor(self, as_dict=False):
            return cur

        def close(self):
            pass

    monkeypatch.setattr(q, "get_conn", lambda: Conn())
    q.db_filter_options()
    status_sql = next(s for s in cur.sqls if "d.status" in s)
    assert "<> 'deleted'" in status_sql


def test_won_er_delt_i_web_sale_og_rigtige_salg(fake_conn):
    """Web Sale er self-service-køb, der oprettes og vindes automatisk.

    Uden opdelingen så NordicDefenceWatch ud til at have vundet 4 deals, hvor
    de 2 var automatiske webkøb — og Won-tallet stemte ikke med Deal Source,
    der udelader pipelinen helt.
    """
    from datetime import date

    from moduler.modul_benchmark.queries import db_compare

    db_compare(series=[{"sites": ["A"], "start_date": "2022-01-01"}],
               today=date(2026, 8, 18))
    totals_sql = fake_conn.calls[0][0]
    assert "AS w_won" in totals_sql and "AS w_won_web" in totals_sql
    # 'won' må IKKE indeholde Web Sale, og webkøbene skal have deres eget felt
    assert "d.status = 'won' AND NOT UPPER(COALESCE(d.[pipeline_name],'')) = 'WEB SALE'" in totals_sql
    assert "d.status = 'won' AND UPPER(COALESCE(d.[pipeline_name],'')) = 'WEB SALE'" in totals_sql


def test_web_sale_matcher_uanset_stavemaade():
    """Pipedrive sender både 'Web Sale' og 'Web sale' alt efter konto."""
    from moduler.modul_benchmark.queries import _WEB_SALE_SQL

    assert "UPPER(" in _WEB_SALE_SQL
    assert "'WEB SALE'" in _WEB_SALE_SQL


@pytest.mark.parametrize("mode", ["only", "exclude"])
def test_drilldown_kan_isolere_web_sale(fake_conn, mode):
    from moduler.modul_benchmark.queries import db_series_deals

    db_series_deals(["A"], "2026-08-11", 8, web_sale=mode)
    sql = fake_conn.calls[0][0]
    if mode == "only":
        assert "AND UPPER(COALESCE(d.[pipeline_name],'')) = 'WEB SALE'" in sql
        assert "AND NOT UPPER(" not in sql
    else:
        assert "AND NOT UPPER(COALESCE(d.[pipeline_name],'')) = 'WEB SALE'" in sql


def test_ugyldig_web_sale_vaerdi_afvises(fake_conn):
    from moduler.modul_benchmark.queries import db_series_deals

    with pytest.raises(ValueError):
        db_series_deals(["A"], "2026-08-11", 8, web_sale="maaske")


def test_drilldown_web_sale_giver_400_paa_ugyldig_vaerdi(client, make_user, auth_override):
    auth_override(make_user(role="admin"))
    r = client.get("/tools/benchmark/deals", params={
        "site": "A", "start_date": "2026-08-11", "window_days": 8, "web_sale": "maaske",
    })
    assert r.status_code == 400


def test_beloeb_er_altid_dkk(fake_conn):
    """Medier sammenlignes på tværs af lande, så enheden skal være ens for alle.

    Derfor [value_dkk] og ikke constants.deal_value_sql(), som lader NO/SE/DE
    regne i lokal valuta (deres budgetter er lagt i lokal valuta — men det er
    ikke det, der sammenlignes her).
    """
    from datetime import date

    from moduler.modul_benchmark.queries import db_compare

    d = db_compare(series=[{"sites": ["A"], "start_date": "2022-01-01"}],
                   today=date(2026, 8, 18))
    assert d["currency"] == "DKK"
    totals_sql = fake_conn.calls[0][0]
    assert "d.value_dkk" in totals_sql
    assert "d.[value]" not in totals_sql   # aldrig råt lokalbeløb i totalerne


def test_testdeals_frasorteres_som_standard(fake_conn):
    """QA-deals ('Test Test' m.fl.) er næsten alle tabte og trækker win rate
    ned uden at være salg. De frasorteres derfor med mindre man beder om andet."""
    from datetime import date

    from moduler.modul_benchmark.queries import db_compare

    d = db_compare(series=[{"sites": ["A"], "start_date": "2022-01-01"}],
                   today=date(2026, 8, 18))
    assert d["exclude_test"] is True
    totals_sql = fake_conn.calls[0][0]
    assert "AND NOT (COALESCE(d.[person_name],'') LIKE 'test%'" in totals_sql


def test_testfilter_kan_slaas_af(fake_conn):
    from datetime import date

    from moduler.modul_benchmark.queries import db_compare

    d = db_compare(series=[{"sites": ["A"], "start_date": "2022-01-01"}],
                   exclude_test=False, today=date(2026, 8, 18))
    assert d["exclude_test"] is False
    # Totals-queryen må ikke filtrere; test-optællingen må stadig gerne
    assert "AND NOT (COALESCE(d.[person_name]" not in fake_conn.calls[0][0]


def test_testfilter_matcher_kun_test_i_ordets_begyndelse():
    """Et naivt LIKE '%test%' ville fjerne rigtige nordiske efternavne —
    Eftestøl, Slettestøl, Gautestad, Bentestuen — og dermed rigtigt salg.

    Reglen skal derfor kræve at 'test' står i begyndelsen af et ord.
    """
    from moduler.modul_benchmark.queries import _TEST_PERSON_SQL

    assert "LIKE 'test%'" in _TEST_PERSON_SQL      # navnets start
    assert "LIKE '% test%'" in _TEST_PERSON_SQL    # efter et mellemrum
    assert "LIKE '%test%'" not in _TEST_PERSON_SQL  # ALDRIG ren delstreng


def test_testfilter_rammer_kun_person_ikke_organisation():
    """Flere rigtige kunder har 'test' i firmanavnet: Skattestyrelsen,
    TestaViva DK ApS, R&D Test Systems A/S, Testcenter Danmark. Et org-filter
    ville fjerne dem alle."""
    from moduler.modul_benchmark.queries import _TEST_PERSON_SQL

    assert "person_name" in _TEST_PERSON_SQL
    assert "org_name" not in _TEST_PERSON_SQL


def test_frasorterede_testdeals_rapporteres_pr_serie(fake_conn):
    """Et filter der arbejder i det skjulte er værre end ingen — der ER
    grænsetilfælde ('Julie Tester' har 3 vundne deals for 22.996 kr.)."""
    from datetime import date

    from moduler.modul_benchmark.queries import db_compare

    d = db_compare(series=[{"sites": ["A"], "start_date": "2022-01-01"}],
                   today=date(2026, 8, 18))
    assert set(d["series"][0]["test_deals"]) == {"deals", "won", "open", "lost"}
    # Optællings-queryen skal bruge KUN-test-varianten, ikke NOT-varianten
    only_sql = next(s for s, _ in fake_conn.calls
                    if "AND (COALESCE(d.[person_name],'') LIKE 'test%'" in s)
    assert "AND NOT (COALESCE(d.[person_name]" not in only_sql


@pytest.mark.parametrize("mode", ["exclude", "only"])
def test_drilldown_kan_vise_eller_skjule_testdeals(fake_conn, mode):
    from moduler.modul_benchmark.queries import db_series_deals

    db_series_deals(["A"], "2026-08-11", 8, test_persons=mode)
    sql = fake_conn.calls[0][0]
    if mode == "only":
        assert "AND (COALESCE(d.[person_name],'') LIKE 'test%'" in sql
        assert "AND NOT (COALESCE(d.[person_name]" not in sql
    else:
        assert "AND NOT (COALESCE(d.[person_name],'') LIKE 'test%'" in sql


def test_ugyldig_test_persons_afvises(fake_conn):
    from moduler.modul_benchmark.queries import db_series_deals

    with pytest.raises(ValueError):
        db_series_deals(["A"], "2026-08-11", 8, test_persons="maaske")


def test_drilldown_returnerer_kontaktperson(fake_conn):
    """Kontaktpersonen er den kolonne filteret ser på — så den skal kunne ses,
    ellers kan man ikke vurdere om en frasortering var rigtig."""
    from moduler.modul_benchmark.queries import db_series_deals

    db_series_deals(["A"], "2026-08-11", 8)
    assert "d.person_name" in fake_conn.calls[0][0]


def test_dato_forslag_ignorerer_testdeals(fake_conn):
    """Ellers kunne den foreslåede launchdato stamme fra en testdeal oprettet
    længe før mediet gik live."""
    from moduler.modul_benchmark.queries import db_first_activity

    db_first_activity(["A"])
    assert "AND NOT (COALESCE(d.[person_name],'') LIKE 'test%'" in fake_conn.calls[0][0]


def test_deals_foer_dag_1_taelles_og_rapporteres(fake_conn):
    """Deals før launchdatoen er bevidst udenfor sammenligningen, men skal
    kunne ses — ellers ligner totalen en fejl.

    NordicDefenceWatch: Deal Source viser 149 deals, benchmarken 144. De 5's
    forskel er 7 tabte deals oprettet 6.–10. august (før live den 11.) minus 2
    Web Sale-køb, som benchmarken tæller med og Deal Source ikke.
    """
    from datetime import date

    from moduler.modul_benchmark.queries import db_compare

    d = db_compare(series=[{"sites": ["A"], "start_date": "2026-08-11"}],
                   today=date(2026, 8, 18))
    assert "before_start" in d["series"][0]
    before = d["series"][0]["before_start"]
    assert set(before) == {"deals", "first_date", "last_date"}
    # Queryen skal kigge FØR startdatoen, ikke efter
    sql, params = next((c for c in fake_conn.calls
                        if "< %s" in c[0] and ">= %s" not in c[0]), (None, None))
    assert sql is not None, "before-start-queryen blev ikke kørt"
    assert params[0] == "2026-08-11"
    assert sql.count("%s") == len(params)


def test_valutafordeling_viser_lokalt_beloeb_ved_siden_af_dkk(fake_conn):
    """Uden det lokale beløb kan et DKK-tal ikke afstemmes mod fx en svensk
    rapport — og man kan ikke se, at der HAR været en omregning."""
    from datetime import date

    from moduler.modul_benchmark.queries import db_compare

    d = db_compare(series=[{"sites": ["FinansWatch SE"], "start_date": "2024-08-14"}],
                   today=date(2026, 8, 18))
    assert d["series"][0]["currencies"] == []   # FakeCur giver tomme rækker
    cur_sql = next(s for s, _ in fake_conn.calls if "value_local" in s)
    assert "SUM(ISNULL(d.value, 0))" in cur_sql
    assert "SUM(ISNULL(d.value_dkk, 0))" in cur_sql
    assert "d.status = 'won'" in cur_sql


# ---------------------------------------------------------------------------
# Marketing-eksporten
# ---------------------------------------------------------------------------

def test_marketing_export_kraever_marketing_adgang(client, make_user, auth_override):
    auth_override(make_user(role="salesperson"))
    assert client.get("/tools/marketing/export").status_code == 403


def test_marketing_export_leverer_alle_statusser(client, make_user, auth_override, monkeypatch):
    """Eksporten må ikke arve deals-tabellens status='won'-krav — hele
    won/open/lost-billedet bag 'Performance pr. Account' skal med."""
    import moduler.modul_marketing.router as mk

    kaldt = {}

    def fake(**kw):
        kaldt.update(kw)
        return {"rows": [{"deal_id": 1, "status": "lost"}], "total": 1, "truncated": False}

    monkeypatch.setattr(mk, "db_export_deals", fake)
    auth_override(make_user(role="marketing"))
    r = client.get("/tools/marketing/export", params={"account": "watch_medier"})
    assert r.status_code == 200
    assert r.json()["rows"][0]["status"] == "lost"
    assert kaldt["accounts"] == ["watch_medier"]


def test_marketing_export_sql_har_ingen_status_eller_side_graense():
    """Kildetekst-tjek: db_export_deals må hverken låse status til 'won' eller
    kræve value_dkk — begge ville skjule åbne og tabte deals i eksporten."""
    import inspect

    from moduler.modul_marketing.queries import _EXPORT_ROW_CAP, db_export_deals

    src = inspect.getsource(db_export_deals)
    assert "status = 'won'" not in src
    assert "value_dkk IS NOT NULL" not in src
    # Udelukkelserne SKAL være med, så eksporten stemmer med tallene på skærmen
    assert "_ADM_EXCLUDE" in src
    assert "_WEB_SALE_EXCLUDE" in src
    assert _EXPORT_ROW_CAP > 0


def test_marketing_export_row_cap_respekteres(monkeypatch):
    """limit klippes til loftet, så en manipuleret query-param ikke kan trække
    hele tabellen ned i browseren."""
    import moduler.modul_marketing.queries as q

    fanget = {}

    class Cur:
        def execute(self, sql, params=None):
            fanget["params"] = params

        def fetchall(self):
            return []

    class Conn:
        def cursor(self, as_dict=False):
            return Cur()

        def close(self):
            pass

    monkeypatch.setattr(q, "get_conn", lambda: Conn())
    q.db_export_deals(limit=10 ** 9)
    assert fanget["params"][-1] == q._EXPORT_ROW_CAP

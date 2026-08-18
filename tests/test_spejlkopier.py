"""Tests for spejlkopi-filteret (constants.mirror_exclude_sql).

Baggrund: banner/job-salget flyttede til en egen Pipedrive-konto 13. februar
2025, men migreringen kopierede de vundne deals ind uden at lukke originalerne.
Uden filter tælles de to gange. Fundet på Lene Jægerums august 2025: dashboardet
viste 18 deals hvor der reelt var 15, og 78.008 kr. for meget.

Fredet adfærd:
  - prædikatet må ALDRIG bygges uden tabel-kvalifikator (den fejl udelukkede
    1.370 deals / 35 mio. kr. i stedet for 575)
  - won_time alene er ikke nok til at identificere en spejling
  - kun banner/job i de gamle konti rammes — aldrig annonce-kontoen selv
  - modul_perf's standardfilter skal indeholde BEGGE filtre
"""
import re

import pytest

from constants import (ADVERTISING_ACCOUNTS, ADVERTISING_PIPELINES,
                       mirror_exclude_sql)


# ---------------------------------------------------------------------------
# Guarden mod tom kvalifikator
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad", ["", "d", "PipedriveDeals", None])
def test_praedikat_kraever_tabel_kvalifikator(bad):
    """Uden kvalifikator resolver SQL Server de ukvalificerede kolonner inde i
    EXISTS til subqueryens EGEN tabel, så korrelationen bliver
    `_m.owner_name = _m.owner_name` — altid sand. Prædikatet udelukkede så hver
    eneste deal i de gamle konti: 1.370 deals og 35 mio. kr., heriblandt hele
    2019-2020 hvor annonce-kontoen slet ikke fandtes.
    """
    with pytest.raises((ValueError, AttributeError, TypeError)):
        mirror_exclude_sql(bad)


def test_default_prefix_kvalificerer_paa_tabelnavn():
    """modul_perf's queries er alle `FROM [dbo].[PipedriveDeals]` uden alias, så
    defaulten skal kvalificere på tabelnavnet."""
    sql = mirror_exclude_sql()
    assert "PipedriveDeals.[owner_name]" in sql
    assert "PipedriveDeals.[won_time]" in sql
    # Aldrig en ukvalificeret ydre reference
    assert "_m.[owner_name] = [owner_name]" not in sql
    assert "_m.[won_time] = [won_time]" not in sql


def test_alias_prefix_bruges_hvor_queryen_joiner():
    sql = mirror_exclude_sql("d.")
    assert "_m.[owner_name] = d.[owner_name]" in sql
    assert "_m.[won_time] = d.[won_time]" in sql
    assert "PipedriveDeals.[owner_name]" not in sql


def test_korrelationen_peger_altid_ud_af_subqueryen():
    """Hver ydre kolonne-reference i EXISTS skal have prefixet — ellers er
    korrelationen selvrefererende og prædikatet matcher alt."""
    prefix = "d."
    sql = mirror_exclude_sql(prefix)
    exists = sql[sql.index("EXISTS ("):]
    # Alle sammenligninger _m.[x] = <noget> skal have <noget> kvalificeret
    for match in re.finditer(r"_m\.\[(\w+)\]\s*=\s*(\[|\w)", exists):
        efter = match.group(2)
        assert efter != "[", (
            f"_m.[{match.group(1)}] sammenlignes med en UKVALIFICERET kolonne — "
            "det gør korrelationen selvrefererende")


# ---------------------------------------------------------------------------
# Prædikatets afgrænsning
# ---------------------------------------------------------------------------

def test_rammer_kun_gamle_konti_og_kun_banner_job():
    sql = mirror_exclude_sql("d.")
    # Annonce-kontoen selv må aldrig udelukkes
    assert "d.[account] NOT IN ('jppol_advertising','watch_no_advertising')" in sql
    # Kun de migrerede pipelines
    assert "IN ('banner','job')" in sql
    assert ADVERTISING_ACCOUNTS == ("jppol_advertising", "watch_no_advertising")
    assert ADVERTISING_PIPELINES == ("banner", "job")


def test_kun_vundne_deals_rammes():
    """Af 1.256 TABTE banner-deals i watch_medier er 0 spejlet, og kun 2 af 58
    åbne. Migreringen kopierede afsluttet forretning, ikke den åbne pipeline —
    så åbne pipeline-widgets skal være upåvirkede."""
    sql = mirror_exclude_sql("d.")
    assert "d.[won_time] IS NOT NULL" in sql
    assert "_m.[status] = 'won'" in sql


def test_wontime_alene_er_ikke_nok():
    """En sælger kan lukke flere deals i samme sekund. Mathias Schubert lukkede
    tre Dansk Fjernvarme-deals 2021-09-24 12:47:30, hvoraf kun én var en
    spejling — #45168 (40.000, 'årsaftale 2021+2022') er en anden aftale og skal
    bevares. Derfor kræves også samme beløb ELLER samme titel."""
    sql = mirror_exclude_sql("d.")
    assert "value_dkk" in sql and "title" in sql
    assert " OR " in sql, "beløb-ELLER-titel mangler"


def test_null_haandteres_paa_begge_sider():
    """En NULL-sammenligning giver NULL (= ikke match), så spejlinger uden beløb
    eller titel ville slippe igennem."""
    sql = mirror_exclude_sql("d.")
    assert "ISNULL(_m.[value_dkk], 0) = ISNULL(d.[value_dkk], 0)" in sql
    assert "ISNULL(_m.[title], '') = ISNULL(d.[title], '')" in sql


def test_praedikat_har_balancerede_parenteser():
    sql = mirror_exclude_sql("d.")
    assert sql.count("(") == sql.count(")"), sql


def test_praedikatet_er_en_and_not_gruppe():
    """Det appenderes til en WHERE, så det skal starte med AND og være én
    negeret gruppe — ellers ændrer det betydningen af de øvrige betingelser."""
    sql = mirror_exclude_sql("d.").strip()
    assert sql.startswith("AND NOT (")
    assert sql.endswith(")")


def test_ingen_parametre_i_praedikatet():
    """Værdierne er modul-konstanter, ikke brugerinput. Kom der %s ind, skulle
    ~30 kaldsteder i modul_perf flette parametre ind i rigtig rækkefølge."""
    assert "%s" not in mirror_exclude_sql("d.")


# ---------------------------------------------------------------------------
# modul_perf bruger det overalt
# ---------------------------------------------------------------------------

def test_modul_perf_standardfilter_indeholder_begge_filtre():
    from moduler.modul_perf.queries import (_ADM_EXCLUDE, _DEAL_EXCLUDE,
                                            _MIRROR_EXCLUDE)

    assert _ADM_EXCLUDE in _DEAL_EXCLUDE
    assert _MIRROR_EXCLUDE in _DEAL_EXCLUDE
    assert "administrativ" in _ADM_EXCLUDE
    assert "PipedriveDeals.[won_time]" in _MIRROR_EXCLUDE


def test_alle_deal_queries_i_modul_perf_har_spejlfilteret():
    """Enhver query der læser beløb eller tæller deals skal have filteret.

    Glemmer man det på én, dobbelt-tælles banner/job igen — og fejlen er tavs.
    Undtaget er opslag der kun henter DISTINCT-værdier til dropdowns, hvor en
    spejlkopi ikke bidrager med andet end de samme værdier.
    """
    import inspect

    from moduler.modul_perf import queries

    src = inspect.getsource(queries)
    # Kun DISTINCT-opslag og eksistens-tjek må mangle filteret
    tilladt_uden = {
        "db_get_filters", "db_owner_in_teams",
        "db_manager_saelger_filters", "db_saelger_meta",
    }
    lines = src.split("\n")
    mangler = []
    for i, line in enumerate(lines):
        if "FROM [dbo].[PipedriveDeals]" not in line:
            continue
        blok = "\n".join(lines[max(0, i - 20):i + 22])
        if "_DEAL_EXCLUDE" in blok or "_MIRROR_EXCLUDE" in blok:
            continue
        fn = next((lines[j].split("(")[0].replace("def ", "").strip()
                   for j in range(i, -1, -1) if lines[j].startswith("def ")), "?")
        if fn not in tilladt_uden:
            mangler.append((i + 1, fn))
    assert not mangler, f"deal-queries uden spejlfilter: {mangler}"


def test_seneste_deals_udstiller_titel():
    """Uden titel ser forskellige deals identiske ud i 'Seneste deals vundet':
    BDO's fem månedsbookinger à 0 kr. rammer samme site, kunde, beløb og dato,
    og kun titlen adskiller dem."""
    import inspect

    from moduler.modul_perf import queries

    src = inspect.getsource(queries.db_saelger_data)
    assert '"title":' in src

    from pathlib import Path
    tpl = Path("templates/perf_saelger.html").read_text(encoding="utf-8")
    assert "<th>Titel</th>" in tpl
    assert "escHtml(deal.title)" in tpl
    # Tomme-tilstanden skal matche antallet af kolonner
    assert 'colspan="6"' in tpl
    assert 'colspan="5"' not in tpl.split("deals-tbody")[1][:400]

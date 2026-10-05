"""Tests for holdmedlemskab på dealens dato (constants.team_member_at_deal_date_sql).

Baggrund: hubben spurgte "er sælgeren medlem af holdet i dag", så en sælgers
historik forsvandt fra det gamle hold, så snart medlemskabet fik en slutdato.
Målt 01-10-2026: Michael Toft havde 9 vundne Watch DK-deals i januar 2026, men
manglede på holdets leaderboard, fordi hans medlemskab sluttede 31-01.

Fredet adfærd:
  - prædikatet må ALDRIG bygges uden tabel-kvalifikator (samme fælde som
    mirror_exclude_sql: korrelationen bliver selvrefererende uden en fejl)
  - medlemskabet vurderes på dealens afgørelse, aldrig på dags dato og aldrig
    på service_activation_date
  - startdatoen tjekkes ikke (15.098 vundne deals ligger før deres ejers
    medlemskabsstart, målt 01-10-2026)"""


import pytest
from constants import team_member_at_deal_date_sql

#------------------------------------------------------
#   Guarden mod tom kvalifikator
#------------------------------------------------------

@pytest.mark.parametrize("bad", ["", "d", "PipedriveDeals", None])
def test_praedikat_kraever_tabel_kvalifikator(bad):
    with pytest.raises(ValueError):
        team_member_at_deal_date_sql("(%s)", prefix=bad)


def test_default_prefix_kvalificerer_paa_tabelnavn():
    """modul_perf og modul_rotation spørger 'FROM [dbo].[PipedriveDeals]' uden
    alias, så default skal kvalificere på tabelnavnet."""
    sql = team_member_at_deal_date_sql("(%s)")
    assert "_u.name = PipedriveDeals.[owner_name]" in sql


def test_alias_prefix_bruger_hvor_queryen_joiner():
    """Leaderboard joiner deals med alias d."""
    sql = team_member_at_deal_date_sql("(%s)", prefix="d.")
    assert "_u.name = d.[owner_name]" in sql
    assert "PipedriveDeals." not in sql


# --------------------------------------------------
#   Datoen medlemskabet vurderes paa
#---------------------------------------------------

def test_vurderes_ikke_paa_dags_dato():
    """Det var selve fejlen: GETDATE() i stedet for dealens dato."""
    sql = team_member_at_deal_date_sql("(%s)", prefix="d.")
    assert "GETDATE" not in sql


def test_vurderes_paa_afgoerelsen_i_fast_raekkefoelge():
    """Vundet, ellers tabt, ellers åben. Tabte deals har ingen won_time,
    så uden close_time ville konverteringsraten miste alle tabte deals."""
    sql = team_member_at_deal_date_sql("(%s)", prefix="d.")
    assert "COALESCE(d.[won_time], d.[close_time], d.[add_time])" in sql


def test_aldrig_service_activation_date():
    """Aktiveringsdatoen kan ligge efter, at sælgeren er stoppet. Så
    ville samme deal tælle i Won-visningen, men ikke i Tilvækst-visningen."""
    sql = team_member_at_deal_date_sql("(%s)", prefix="d.")
    assert "service_activation_date" not in sql


def test_startdato_tjekkes_ikke():
    """15.098 vundne deals ligger før deres ejers medlemskabsstart, fordi
    næsten alle medlemskaber starter 2024-01-01 som pladsholder."""
    sql = team_member_at_deal_date_sql("(%s)", prefix="d.")
    assert "start_date" not in sql


# --------------------------------------------------
#  Form: pladsholdere og parenteser
#---------------------------------------------------

@pytest.mark.parametrize("teams_ph, antal", [("(%s)", 1), ("(%s, %s, %s)", 3)])
def test_et_holdnavn_pr_pladsholder(teams_ph, antal):
    """Kaldstedet lægger holdnavnene i sin params-tuple. Står der flere %s i
    prædikatet end hold, forskydes alle de efterfølgende parametre."""
    sql = team_member_at_deal_date_sql(teams_ph, prefix="d.")
    assert sql.count("%s") == antal


def test_praedikat_har_balancerede_parenteser():
    sql = team_member_at_deal_date_sql("(%s,%s)", prefix="d.")
    assert sql.count("(") == sql.count(")")


def test_praedikatet_starter_ikke_med_and():
    """Kaldstedet skriver selv 'AND {...}'. Et AND i prædikatet ville give 'AND AND'
    og en syntaksfejl."""
    sql = team_member_at_deal_date_sql("(%s)", prefix="d.")
    assert sql.lstrip().upper().startswith("EXISTS")

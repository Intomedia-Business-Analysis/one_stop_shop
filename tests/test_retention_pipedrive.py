"""Pipedrive-payloaden fra en udfaldsregistrering (modul_retention/pipedrive.py).

Kører UDEN netværk og UDEN database: alt der testes her er rene funktioner, og
det er netop derfor de er rene. Payloaden bygges én gang og bruges både af
previewet og af den rigtige afsendelse, så beviser testen indholdet, har den
bevist begge veje.

Hvad testen IKKE dækker: at Pipedrive accepterer payloaden. Feltnavne og
værdier er verificeret mod den rigtige konto 2026-09-02 (se modulets
docstring), men kontrakten kan ændre sig hos dem, og det opdager kun et rigtigt
kald. Derfor findes preview-ruten.
"""
import datetime as dt

from moduler.modul_retention.outcomes import (INGEN_KONTAKT, IKKE_KONTAKTBAR,
                                              KONTAKT_OPNAAET)
from moduler.modul_retention.pipedrive import (byg_aktivitet, byg_emne,
                                               byg_note, kontakt_udfald)

SAMTALE = {
    "account":      "watch_medier",
    "org_id":       "240",
    "contacted_at": dt.datetime(2026, 9, 2, 14, 35),
    "channel":      "telefon",
    "summary":      "Kunden vil gerne fortsætte, men på færre licenser.",
    "created_by":   "Retention Specialist",
}


def _udfald(**kw):
    base = {
        "site":               "FinansWatch DK",
        "contact_result":     KONTAKT_OPNAAET,
        "outcome":            "fornyet",
        "arr_before_dkk":     12500.0,
        "arr_after_local":    None,
        "arr_after_currency": None,
        "fx_rate":            None,
        "arr_after_dkk":      None,
        "renewal_date":       None,
        "expiry_date":        None,
        "followup_date":      None,
        "note":               None,
    }
    base.update(kw)
    return base


# ── Hvad der overhovedet sendes ────────────────────────────────────────────

def test_kun_kontakt_opnaaet_sendes():
    udfald = [
        _udfald(site="A"),
        _udfald(site="B", contact_result=INGEN_KONTAKT, outcome=None),
        _udfald(site="C", contact_result=IKKE_KONTAKTBAR, outcome=None),
    ]
    assert [u["site"] for u in kontakt_udfald(udfald)] == ["A"]


def test_ingen_kontakt_giver_ingen_payload():
    """None og ikke en tom dict: kalderen skal kunne skelne 'intet at sende'
    fra 'det gik galt'."""
    udfald = [_udfald(contact_result=INGEN_KONTAKT, outcome=None)]
    assert byg_aktivitet(SAMTALE, udfald) is None


def test_tom_liste_giver_ingen_payload():
    assert byg_aktivitet(SAMTALE, []) is None


# ── Selve payloaden ────────────────────────────────────────────────────────

def test_payload_har_de_verificerede_felter():
    p = byg_aktivitet(SAMTALE, [_udfald()], ejer_id=7284187)
    assert p["type"] == "call"            # telefon → call, verificeret aktiv
    assert p["done"] is True              # opkaldet ER sket
    assert p["org_id"] == 240             # int, ikke streng
    assert p["due_date"] == "2026-09-02"
    # 14:35 dansk sommertid = 12:35 UTC. Pipedrive gemmer og viser due_time
    # som UTC, se LOKAL_TZ i pipedrive.py.
    assert p["due_time"] == "12:35"       # HH:MM, ikke HH:MM:SS
    assert p["owner_id"] == 7284187


def test_kanalerne_mapper_til_pipedrives_typer():
    for kanal, forventet in (("telefon", "call"), ("mail", "email"),
                             ("moede", "meeting")):
        p = byg_aktivitet({**SAMTALE, "channel": kanal}, [_udfald()])
        assert p["type"] == forventet


def test_ukendt_kanal_falder_tilbage_paa_call():
    """En fjerde kanal i outcomes.KANALER må ikke stoppe registreringen i at
    nå Pipedrive."""
    p = byg_aktivitet({**SAMTALE, "channel": "sms"}, [_udfald()])
    assert p["type"] == "call"


def test_uden_ejer_udelades_owner_id():
    """Slår org-ejeren fejl, sætter Pipedrive selv tokenets bruger. Et
    owner_id på None ville derimod blive afvist."""
    p = byg_aktivitet(SAMTALE, [_udfald()])
    assert "owner_id" not in p


def test_sommertid_og_vintertid_omregnes_forskelligt():
    """CEST er UTC+2, CET er UTC+1. Et fast fradrag ville ramme forkert
    halvdelen af året."""
    sommer = byg_aktivitet(
        {**SAMTALE, "contacted_at": dt.datetime(2026, 7, 1, 12, 0)}, [_udfald()])
    vinter = byg_aktivitet(
        {**SAMTALE, "contacted_at": dt.datetime(2026, 1, 15, 12, 0)}, [_udfald()])
    assert sommer["due_time"] == "10:00"
    assert vinter["due_time"] == "11:00"


def test_opkald_efter_midnat_flytter_ogsaa_datoen():
    """00:30 dansk tid er 22:30 UTC DAGEN FØR. Konverteres kun klokkeslættet,
    lander aktiviteten et døgn galt."""
    p = byg_aktivitet(
        {**SAMTALE, "contacted_at": dt.datetime(2026, 9, 2, 0, 30)}, [_udfald()])
    assert p["due_date"] == "2026-09-01"
    assert p["due_time"] == "22:30"


def test_dato_uden_klokkeslet_udelader_due_time():
    """Og datoen omregnes IKKE: uden klokkeslæt er der intet tidspunkt at
    flytte, og en UTC-omregning kunne kun rykke dagen væk fra den dag,
    samtalen fandt sted."""
    p = byg_aktivitet({**SAMTALE, "contacted_at": dt.date(2026, 9, 2)},
                      [_udfald()])
    assert p["due_date"] == "2026-09-02"
    assert "due_time" not in p


# ── Emnet: det eneste sælgeren ser uden at klikke ──────────────────────────

def test_emne_med_ét_udfald_naevner_udfald_og_site():
    assert byg_emne([_udfald()]) == "Retention: Fornyet (FinansWatch DK)"


def test_emne_med_flere_udfald_taeller_og_lister():
    emne = byg_emne([_udfald(site="A"), _udfald(site="B", outcome="opsagt")])
    assert emne == "Retention: 2 udfald (Fornyet, Opsagt)"


def test_emne_klippes_under_200_tegn():
    mange = [_udfald(site=f"Site {i}", outcome="nedgraderet") for i in range(40)]
    assert len(byg_emne(mange)) <= 200


# ── Noten: HTML, og alt udefra skal escapes ────────────────────────────────

def test_note_escaper_specialistens_tekst():
    """Noten er HTML (39 af 89 noter på kontoen har tags). Et sitenavn eller en
    fritekst med < eller & må ikke kunne lukke et tag."""
    note = byg_note({**SAMTALE, "summary": "Rabat <b>skal</b> aftales & godkendes"},
                    [_udfald(note="a < b & c")])
    assert "&lt;b&gt;skal&lt;/b&gt;" in note
    assert "a &lt; b &amp; c" in note
    # Vores egne tags overlever
    assert "<br>" in note and "<b>Opsummering:</b>" in note


def test_note_naevner_hvem_der_ringede():
    note = byg_note(SAMTALE, [_udfald()])
    assert "Retention Specialist" in note
    assert "Telefon" in note


def test_note_viser_arr_og_datoer_naar_de_findes():
    note = byg_note(SAMTALE, [_udfald(arr_before_dkk=12500,
                                      arr_after_dkk=9000,
                                      renewal_date=dt.date(2027, 1, 1))])
    assert "12.500 kr." in note
    assert "9.000 kr." in note
    assert "Fornyelse: 2027-01-01" in note


def test_note_udelader_arr_naar_der_ikke_er_noget():
    """En linje der siger 'ARR: ingen' er støj, og et 0 ville blive læst som
    en måling."""
    note = byg_note(SAMTALE, [_udfald(arr_before_dkk=None)])
    assert "ARR:" not in note

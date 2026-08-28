"""Tests af datobroen i db.py.

Baggrund står i db.py: serveren kræver TLS, TLS kræver TDS 7.4, og fra 7.4
kommer DATE/DATETIME2 tilbage som rigtige objekter i stedet for strenge.
Modulerne er skrevet til strengene, så db.py kan levere DATE-kolonner som
'YYYY-MM-DD' igen (DB_DATE_AS_STRING).

Det er den slags kode der skal testes præcist, for en forkert type viser sig
ikke som en fejl — den viser sig som "Data utilgængelig" i et dashboard.
"""
import datetime as dt

import db


class FakeCursor:
    """Efterligner en pymssql-cursor: samme rækker ud af alle fetch-veje."""

    def __init__(self, rows):
        self._rows = list(rows)
        self.description = (("kolonne", None),)
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self._rows.pop(0) if self._rows else None

    def fetchmany(self, size=1):
        taget, self._rows = self._rows[:size], self._rows[size:]
        return taget

    def fetchall(self):
        alle, self._rows = self._rows, []
        return alle

    def __iter__(self):
        while self._rows:
            yield self._rows.pop(0)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def close(self):
        pass


class FakeConnection:
    def __init__(self, rows):
        self._rows = rows
        self.lukket = False

    def cursor(self, as_dict=False):
        return FakeCursor(self._rows)

    def close(self):
        self.lukket = True

    def rollback(self):
        pass


# ── _legacy: hvad konverteres, og hvad gør det ikke ─────────────────────────

def test_date_bliver_iso_streng():
    assert db._legacy(dt.date(2026, 8, 14)) == "2026-08-14"


def test_datetime_er_urort():
    """DATETIME kom som objekt også under TDS 7.0 — den må ikke ændres.

    datetime er en subklasse af date, så en isinstance-test ville have fanget
    den her ved en fejl. Derfor tjekker _legacy typen præcist.
    """
    vaerdi = dt.datetime(2026, 8, 14, 15, 4, 5)
    assert db._legacy(vaerdi) is vaerdi


def test_ovrige_typer_er_urorte():
    for vaerdi in (None, 0, 42, "tekst", 3.14, True, b"bytes"):
        assert db._legacy(vaerdi) is vaerdi


def test_rakker_af_alle_former():
    d = dt.date(2026, 1, 2)
    assert db._legacy_row({"a": d, "b": 1}) == {"a": "2026-01-02", "b": 1}
    assert db._legacy_row((d, 1)) == ("2026-01-02", 1)
    assert db._legacy_row([d, 1]) == ["2026-01-02", 1]
    assert db._legacy_row(None) is None


# ── Cursor-wrapperen: alle fetch-veje skal konvertere ───────────────────────

def _rows():
    return [
        {"navn": "a", "dato": dt.date(2026, 8, 14), "tid": dt.datetime(2026, 8, 14, 9, 0)},
        {"navn": "b", "dato": dt.date(2026, 8, 15), "tid": None},
    ]


def test_fetchall_konverterer():
    cur = db._LegacyDateCursor(FakeCursor(_rows()))
    raekker = cur.fetchall()
    assert [r["dato"] for r in raekker] == ["2026-08-14", "2026-08-15"]
    assert raekker[0]["tid"] == dt.datetime(2026, 8, 14, 9, 0)


def test_fetchone_konverterer():
    cur = db._LegacyDateCursor(FakeCursor(_rows()))
    assert cur.fetchone()["dato"] == "2026-08-14"


def test_fetchone_paa_tom_giver_none():
    cur = db._LegacyDateCursor(FakeCursor([]))
    assert cur.fetchone() is None


def test_fetchmany_konverterer():
    cur = db._LegacyDateCursor(FakeCursor(_rows()))
    assert [r["dato"] for r in cur.fetchmany(1)] == ["2026-08-14"]


def test_iteration_konverterer():
    cur = db._LegacyDateCursor(FakeCursor(_rows()))
    assert [r["dato"] for r in cur] == ["2026-08-14", "2026-08-15"]


def test_ovrige_attributter_gaar_igennem():
    """execute, description og close skal virke som på den rigtige cursor."""
    raa = FakeCursor(_rows())
    cur = db._LegacyDateCursor(raa)
    cur.execute("SELECT 1", (2,))
    assert raa.executed == [("SELECT 1", (2,))]
    assert cur.description == raa.description
    cur.close()


def test_context_manager():
    with db._LegacyDateCursor(FakeCursor(_rows())) as cur:
        assert cur.fetchone()["dato"] == "2026-08-14"


# ── Kontakten: DB_DATE_AS_STRING slår broen fra ─────────────────────────────

def test_proxy_pakker_ind_naar_broen_er_taendt(monkeypatch):
    monkeypatch.setattr(db, "DATE_AS_STRING", True)
    conn = db._ConnectionProxy(FakeConnection(_rows()))
    assert isinstance(conn.cursor(as_dict=True), db._LegacyDateCursor)
    assert conn.cursor(as_dict=True).fetchone()["dato"] == "2026-08-14"


def test_proxy_giver_rigtige_datoer_naar_broen_er_slukket(monkeypatch):
    monkeypatch.setattr(db, "DATE_AS_STRING", False)
    conn = db._ConnectionProxy(FakeConnection(_rows()))
    cur = conn.cursor(as_dict=True)
    assert not isinstance(cur, db._LegacyDateCursor)
    assert cur.fetchone()["dato"] == dt.date(2026, 8, 14)


def test_proxy_close_lukker_den_rigtige_forbindelse():
    raa = FakeConnection(_rows())
    conn = db._ConnectionProxy(raa)
    conn.close()
    assert raa.lukket
    conn.close()          # to gange må ikke fejle


def test_pooled_connection_afleverer_til_poolen(monkeypatch):
    """close() på en pooled forbindelse LUKKER den ikke — den genbruges."""
    import queue
    monkeypatch.setattr(db, "_pool", queue.LifoQueue(maxsize=2))
    raa = FakeConnection(_rows())
    db.PooledConnection(raa).close()
    assert not raa.lukket
    assert db._pool.get_nowait() is raa


# ── _required: manglende variabel skal sige HVILKEN ─────────────────────────

def test_required_navngiver_den_manglende_variabel(monkeypatch):
    monkeypatch.delenv("DB_SERVER", raising=False)
    try:
        db._required("DB_SERVER")
    except RuntimeError as exc:
        assert "DB_SERVER" in str(exc)
    else:
        raise AssertionError("burde have kastet RuntimeError")

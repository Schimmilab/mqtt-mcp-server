"""Tests fuer den Store — ohne Broker, reine Logik.

Schwerpunkt liegt auf den Faellen, die am 2026-08-01 real schiefgegangen sind:
Startschwall als frischer Verkehr, Stille ohne Verbindungspruefung, stilles
Loeschen.
"""
from __future__ import annotations

import time

import pytest

from mqtt_mcp_server.store import Store


@pytest.fixture
def store(tmp_path):
    s = Store(tmp_path / "t.db")
    yield s
    s.close()


def test_speichert_und_liest_letzten_wert(store):
    store.add_message("a/b", b"12", 0, False, False)
    store.add_message("a/b", b"13", 0, False, False)
    r = store.get_last("a/b")
    assert r["wert"] == "13"
    assert r["retained"] is False
    assert r["aus_startschwall"] is False


def test_unbekanntes_topic_gibt_none(store):
    assert store.get_last("gibt/es/nicht") is None


def test_historie_ist_zeitlich_sortiert_neueste_zuerst(store):
    jetzt = time.time()
    for i, ts in enumerate([jetzt - 30, jetzt - 20, jetzt - 10]):
        store.add_message("x", str(i).encode(), 0, False, False, ts=ts)
    h = store.get_history("x")
    assert [e["wert"] for e in h] == ["2", "1", "0"]


def test_startschwall_wird_markiert_und_ist_ausblendbar(store):
    """Der Fehler vom 01.08.: 29.664 retained Messages als frischen Verkehr gelesen."""
    store.add_message("s", b"alt", 0, True, True)      # aus dem Backlog
    store.add_message("s", b"neu", 0, False, False)    # echter Verkehr
    alle = store.get_history("s", mit_backlog=True)
    ohne = store.get_history("s", mit_backlog=False)
    assert len(alle) == 2 and len(ohne) == 1
    assert ohne[0]["wert"] == "neu"
    assert any(e["aus_startschwall"] for e in alle)


def test_get_last_nutzt_auch_backlog_werte(store):
    """Der schlafende Shelly: der einzige Wert stammt aus dem Startschwall."""
    store.add_message("schlafend", b"21.5", 0, True, True)
    r = store.get_last("schlafend")
    assert r["wert"] == "21.5"
    assert r["aus_startschwall"] is True   # muss sichtbar bleiben


def test_find_silent_findet_nur_alte(store):
    jetzt = time.time()
    store.add_message("frisch", b"1", 0, False, False, ts=jetzt - 60)
    store.add_message("alt", b"1", 0, False, False, ts=jetzt - 7200)
    treffer = [t["topic"] for t in store.find_silent(3600)]
    assert treffer == ["alt"]


def test_luecken_werden_aus_verbindungsereignissen_gebildet(store):
    store.add_connection_event("connected")
    time.sleep(0.02)
    store.add_connection_event("disconnected", "rc=7")
    time.sleep(0.02)
    store.add_connection_event("connected")
    l = store.get_gaps()
    assert len(l) == 1
    assert l[0]["grund"] == "rc=7"
    assert l[0]["bis"] is not None


def test_offene_luecke_wird_als_offen_gemeldet(store):
    store.add_connection_event("connected")
    store.add_connection_event("disconnected", "kabel")
    l = store.get_gaps()
    assert len(l) == 1 and l[0]["bis"] is None
    assert l[0].get("hinweis") == "noch offen"


def test_retention_loescht_alte_und_berichtet(store):
    jetzt = time.time()
    store.add_message("x", b"uralt", 0, False, False, ts=jetzt - 40 * 86400)
    store.add_message("x", b"neu", 0, False, False, ts=jetzt)
    b = store.aufraeumen(tage=30, max_mb=99999)
    assert b["nach_alter"] == 1
    assert b["aeltester_rest"] is not None       # Bericht ist gefuellt, nicht still
    assert len(store.get_history("x")) == 1


def test_retention_ohne_treffer_meldet_null(store):
    store.add_message("x", b"neu", 0, False, False)
    b = store.aufraeumen(tage=30, max_mb=99999)
    assert b["nach_alter"] == 0 and b["nach_groesse"] == 0


def test_pattern_filtert_topics(store):
    store.add_message("tele/a/SENSOR", b"1", 0, False, False)
    store.add_message("stat/b/RESULT", b"1", 0, False, False)
    t = [x["topic"] for x in store.list_topics(pattern="tele/*")]
    assert t == ["tele/a/SENSOR"]


def test_grosse_payloads_werden_gekuerzt_aber_gemeldet(store):
    store.add_message("gross", b"x" * 900, 0, False, False)
    w = store.get_last("gross")["wert"]
    assert w.endswith("Z.)") and len(w) < 900


def test_binaerdaten_brechen_nicht(store):
    store.add_message("bin", b"\xff\xfe\x00", 0, False, False)
    assert store.get_last("bin")["wert"] is not None

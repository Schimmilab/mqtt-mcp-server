"""Tests fuer die Schreibsicherung — zwei Huerden, beide muessen greifen.

Der Anlass ist real: Am 2026-08-01 stellte sich heraus, dass ein defekter
Zwischenstecker vor IPS-PC und hometux neun Tage Stillstand und sieben Wochen
Datenverlust verursacht hatte. Ein versehentlicher Schaltbefehl haette dieselbe
Wirkung.
"""
from __future__ import annotations

import pytest

from mqtt_mcp_server.collector import Collector
from mqtt_mcp_server.config import Config
from mqtt_mcp_server.store import Store


def _collector(tmp_path, **kw):
    cfg = Config(**kw)
    cfg.db_pfad = tmp_path / "t.db"
    return Collector(cfg, Store(tmp_path / "t.db"))


def test_publish_ist_standardmaessig_gesperrt(tmp_path):
    c = _collector(tmp_path)
    ok, grund = c.darf_publizieren("beliebig/topic")
    assert ok is False
    assert "MQTT_MCP_ALLOW_PUBLISH" in grund     # sagt, wie man es freischaltet


def test_freigeschaltet_darf_normales_topic(tmp_path):
    c = _collector(tmp_path, publish_erlaubt=True)
    ok, _ = c.darf_publizieren("test/spielwiese")
    assert ok is True


@pytest.mark.parametrize("topic", [
    "cmnd/tasmota_5A0DAE/POWER",
    "cmnd/sonoff-4854/POWER1",      # die Server-Dose
    "cmnd/irgendwas/Restart",
    "shellies/shellyhtg3-abc/command",
])
def test_gesperrte_topics_bleiben_auch_im_schreibmodus_gesperrt(tmp_path, topic):
    """Die zweite Huerde: Schreibmodus an, Ziel trotzdem tabu."""
    c = _collector(tmp_path, publish_erlaubt=True)
    ok, grund = c.darf_publizieren(topic)
    assert ok is False, f"{topic} haette gesperrt sein muessen"
    assert "gesperrt" in grund.lower()


def test_sperrliste_ist_konfigurierbar(tmp_path):
    c = _collector(tmp_path, publish_erlaubt=True, gesperrte_topics=["heilig/*"])
    assert c.darf_publizieren("heilig/kuh")[0] is False
    # Der Default greift dann NICHT mehr — bewusst, damit die Liste vollstaendig
    # unter Kontrolle des Nutzers steht.
    assert c.darf_publizieren("cmnd/x/POWER")[0] is True


def test_publish_ohne_verbindung_scheitert_sauber(tmp_path):
    c = _collector(tmp_path, publish_erlaubt=True)
    r = c.publish("test/x", "1")
    assert r["ok"] is False
    assert "verbunden" in r["grund"]


# --- Regression: ReasonCode statt int (gefunden 2026-08-01 im End-to-End-Test) ---

class _FakeReasonCode:
    """Nachbau von paho.ReasonCode: kein int, aber .is_failure."""
    def __init__(self, is_failure: bool): self.is_failure = is_failure
    def __int__(self): raise TypeError("ReasonCode ist kein int")


def test_rc_reasoncode_erfolg_wird_nicht_als_fehler_gelesen(tmp_path):
    """Der Bug: int(rc) warf, der Callback brach ab, es wurde nie abonniert."""
    c = _collector(tmp_path)
    assert c._rc_ist_fehler(_FakeReasonCode(False)) is False


def test_rc_reasoncode_fehler_wird_erkannt(tmp_path):
    c = _collector(tmp_path)
    assert c._rc_ist_fehler(_FakeReasonCode(True)) is True


def test_rc_klassisches_int_funktioniert_weiter(tmp_path):
    c = _collector(tmp_path)
    assert c._rc_ist_fehler(0) is False
    assert c._rc_ist_fehler(5) is True


def test_on_connect_abonniert_bei_erfolg(tmp_path):
    """Kern des Bugs: bei Erfolg MUSS subscribe aufgerufen werden."""
    c = _collector(tmp_path)
    gerufen = []
    class FakeClient:
        def subscribe(self, t, qos=0): gerufen.append(t)
    c._on_connect(FakeClient(), None, None, _FakeReasonCode(False))
    assert gerufen == ["#"]
    assert c.ist_verbunden is True


def test_on_connect_abonniert_nicht_bei_fehler(tmp_path):
    c = _collector(tmp_path)
    gerufen = []
    class FakeClient:
        def subscribe(self, t, qos=0): gerufen.append(t)
    c._on_connect(FakeClient(), None, None, _FakeReasonCode(True))
    assert gerufen == []
    assert c.ist_verbunden is False

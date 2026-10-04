"""End-to-End gegen einen echten (Test-)Broker: Anmeldung, Verbindungsluecken, Verstummen.

Diese drei Faelle waren bis 2026-10-04 nur durch Unit-Tests gedeckt. Jeder Test hat eine
Gegenprobe, die POSITIV ausfallen muss — sonst misst der Test nicht (Top-Regel 6e).
Aufruf: uv run --group dev pytest -q tests/test_broker_e2e.py
"""
from __future__ import annotations

import time

import paho.mqtt.client as mqtt
import pytest

pytest.importorskip("amqtt")

from broker_sim import TestBroker, freier_port, passwortdatei  # noqa: E402
from mqtt_mcp_server.collector import Collector  # noqa: E402
from mqtt_mcp_server.config import Config  # noqa: E402
from mqtt_mcp_server.store import Store  # noqa: E402


def _sammler(tmp_path, port, user=None, pw=None, name="h.db"):
    cfg = Config(host="127.0.0.1", port=port, username=user, password=pw, topics=["sim/#"],
                 db_pfad=tmp_path / name)
    store = Store(cfg.db_pfad, broker=cfg.broker_kennung)
    c = Collector(cfg, store)
    c.start()
    return c, store


def _warte(bedingung, timeout=15.0) -> bool:
    ende = time.time() + timeout
    while time.time() < ende:
        if bedingung():
            return True
        time.sleep(0.1)
    return False


def _abonniert(c, store, d, timeout=10.0) -> bool:
    """Verbunden ist nicht abonniert: on_connect setzt das Flag VOR dem subscribe. Erst wenn eine
    Probe durchkommt, misst der Test — sonst gehen die ersten Nachrichten zwischen beiden verloren."""
    ende = time.time() + timeout
    while time.time() < ende:
        d.publish("sim/probe", "1")
        if store.get_last("sim/probe"):
            return True
        time.sleep(0.2)
    return False


def _geraet(port):
    d = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    d.connect("127.0.0.1", port)
    d.loop_start()
    return d


# ------------------------------------------------------------------ Broker-Anmeldung

def test_anmeldung_richtig_verbindet_falsch_wird_sichtbar(tmp_path):
    port = freier_port()
    b = TestBroker(port, passwortdatei(tmp_path / "pw", "sim", "geheim")).start()
    try:
        gut, _ = _sammler(tmp_path, port, "sim", "geheim", "gut.db")
        schlecht, s_store = _sammler(tmp_path, port, "sim", "falsch", "schlecht.db")
        try:
            # Gegenprobe: mit richtigem Passwort MUSS es klappen, sonst misst der Test nicht
            assert gut.warte_auf_verbindung(10), "richtige Zugangsdaten verbinden nicht"
            # Muss-rot: falsches Passwort darf nicht als verbunden gelten …
            assert not schlecht.warte_auf_verbindung(3)
            # … und muss als Fehler UND als offene Luecke sichtbar werden, nicht still bleiben
            assert _warte(lambda: schlecht.letzter_fehler is not None, 5)
            luecken = s_store.get_gaps()
            assert luecken and luecken[-1]["bis"] is None and "abgelehnt" in luecken[-1]["grund"]
        finally:
            gut.stop(); schlecht.stop()
    finally:
        b.stop()


# ------------------------------------------------------------------ get_gaps

def test_brokerausfall_erzeugt_genau_eine_geschlossene_luecke(tmp_path):
    port = freier_port()
    b = TestBroker(port).start()
    c, store = _sammler(tmp_path, port)
    try:
        assert c.warte_auf_verbindung(10)
        assert store.get_gaps() == []                     # Gegenprobe: ohne Ausfall keine Luecke
        b.stop()
        assert _warte(lambda: not c.ist_verbunden, 10), "Ausfall wurde nicht bemerkt"
        time.sleep(1.5)
        b = TestBroker(port).start()
        assert _warte(lambda: c.ist_verbunden, 20), "kein Reconnect nach Broker-Neustart"
        luecken = store.get_gaps()
        assert len(luecken) == 1
        assert luecken[0]["bis"] is not None and luecken[0]["dauer_minuten"] > 0
    finally:
        c.stop(); b.stop()


def test_offene_luecke_solange_broker_weg(tmp_path):
    port = freier_port()
    b = TestBroker(port).start()
    c, store = _sammler(tmp_path, port)
    try:
        assert c.warte_auf_verbindung(10)
        b.stop()
        assert _warte(lambda: bool(store.get_gaps()), 10)
        assert store.get_gaps()[-1]["bis"] is None        # noch offen, nicht faelschlich geschlossen
    finally:
        c.stop()


# ------------------------------------------------------------------ find_silent

def test_verstummtes_geraet_wird_gefunden_aktives_nicht(tmp_path):
    port = freier_port()
    b = TestBroker(port).start()
    c, store = _sammler(tmp_path, port)
    d = _geraet(port)
    try:
        assert c.warte_auf_verbindung(10) and _abonniert(c, store, d)
        d.publish("sim/leise/temp", "21.0")
        for _ in range(30):                                # 3 s Dauersender
            d.publish("sim/laut/temp", "22.0")
            time.sleep(0.1)
        assert store.get_last("sim/leise/temp") is not None   # Gegenprobe: beide kamen an
        still = {x["topic"] for x in store.find_silent(seit_sekunden=1.5)} - {"sim/probe"}
        assert "sim/leise/temp" in still                   # Muss-rot: das verstummte Geraet
        assert "sim/laut/temp" not in still                # das aktive nicht
    finally:
        d.loop_stop(); d.disconnect(); c.stop(); b.stop()

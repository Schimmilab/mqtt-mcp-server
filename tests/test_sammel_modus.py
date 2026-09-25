"""Sammler und MCP trennen: EIN Sammler als Dienst, beliebig viele Leser.

Anlass 2026-09-25: Der Sammler lief im MCP-Prozess — also nur, solange eine
Claude-Session offen war (Lehre 24.09.: 27,6 h ohne Daten). Auf `leitstand`
soll ein eigenstaendiger Dienst sammeln, der MCP liest per SSH nur noch.
Ein lesender MCP darf dabei weder einen zweiten Sammler starten (doppelte
Zeilen) noch aufraeumen (Retention gehoert dem Schreiber).
"""
from __future__ import annotations

import subprocess
import sys

from mqtt_mcp_server.betrieb import hintergrund_starten, verbindungshinweis
from mqtt_mcp_server.config import Config


class _ZaehlCollector:
    def __init__(self) -> None:
        self.gestartet = 0

    def start(self) -> None:
        self.gestartet += 1


def test_sammeln_ist_per_default_an(monkeypatch, tmp_path):
    monkeypatch.delenv("MQTT_MCP_SAMMELN", raising=False)
    monkeypatch.setenv("MQTT_MCP_DB", str(tmp_path / "t.db"))
    assert Config().sammeln is True


def test_sammeln_laesst_sich_abschalten(monkeypatch, tmp_path):
    monkeypatch.setenv("MQTT_MCP_SAMMELN", "0")
    monkeypatch.setenv("MQTT_MCP_DB", str(tmp_path / "t.db"))
    assert Config().sammeln is False


def test_nur_lesen_startet_weder_sammler_noch_retention(monkeypatch, tmp_path):
    monkeypatch.setenv("MQTT_MCP_SAMMELN", "0")
    monkeypatch.setenv("MQTT_MCP_DB", str(tmp_path / "t.db"))
    c = _ZaehlCollector()
    gestartet = hintergrund_starten(Config(), c, store=None)
    assert c.gestartet == 0
    assert gestartet == []


def test_sammelmodus_startet_sammler_und_retention(monkeypatch, tmp_path):
    # Positivkontrolle: derselbe Aufruf MUSS im Sammelmodus beides starten,
    # sonst beweist der Nur-Lesen-Test nichts.
    monkeypatch.setenv("MQTT_MCP_SAMMELN", "1")
    monkeypatch.setenv("MQTT_MCP_DB", str(tmp_path / "t.db"))
    c = _ZaehlCollector()
    gestartet = hintergrund_starten(Config(), c, store=None, retention=lambda: None)
    assert c.gestartet == 1
    assert gestartet == ["sammler", "retention"]


def test_hinweis_nur_lesen_frische_daten_schweigt():
    assert verbindungshinweis(sammeln=False, ist_verbunden=False, letzte_ts=1000.0, jetzt=1060.0) is None


def test_hinweis_nur_lesen_alte_daten_warnt():
    # Im Nur-Lesen-Modus ist "nicht verbunden" normal — gemessen wird die DATENFRISCHE.
    h = verbindungshinweis(sammeln=False, ist_verbunden=False, letzte_ts=1000.0, jetzt=1000.0 + 1800)
    assert h is not None and "30 min" in h


def test_hinweis_nur_lesen_leere_datenbank_warnt():
    assert verbindungshinweis(sammeln=False, ist_verbunden=False, letzte_ts=None, jetzt=1000.0) is not None


def test_hinweis_sammelmodus_unverbunden_warnt_wie_bisher():
    assert verbindungshinweis(sammeln=True, ist_verbunden=False, letzte_ts=None, jetzt=1000.0) is not None
    assert verbindungshinweis(sammeln=True, ist_verbunden=True, letzte_ts=None, jetzt=1000.0) is None


def test_sammler_modul_braucht_kein_fastmcp():
    # Der Dienst auf leitstand soll ohne MCP-Stack laufen koennen.
    code = ("import sys, mqtt_mcp_server.sammler; "
            "print('fastmcp' in sys.modules)")
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "False"


def test_neueste_ts_leer_und_gefuellt(tmp_path):
    from mqtt_mcp_server.store import Store
    s = Store(tmp_path / "t.db", broker="test:1883")
    assert s.neueste_ts() is None
    s.add_message("a/b", b"1", 0, False, False)
    assert s.neueste_ts() is not None
    s.close()

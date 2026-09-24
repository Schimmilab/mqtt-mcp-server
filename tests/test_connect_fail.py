"""Scheiternder Verbindungsaufbau muss SICHTBAR werden.

Anlass 2026-09-24: 27,6 Stunden ohne Daten. macOS verweigerte Python aus der
IDE-Session den LAN-Zugriff ("No route to host"), `connect_async` scheiterte in
jeder Runde — und der Sammler meldete `verbunden: false` bei
`letzter_fehler: null`, ohne ein einziges connection_event. paho ruft bei einem
gescheiterten Aufbau NICHT `on_disconnect`, sondern nur `on_connect_fail`, und
der war nicht gesetzt.
"""
from __future__ import annotations

import socket
import time

from mqtt_mcp_server.collector import Collector
from mqtt_mcp_server.config import Config
from mqtt_mcp_server.store import Store


def _freier_port() -> int:
    # Port belegen und wieder freigeben: danach lauscht dort niemand -> ConnectionRefused
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _warte(bedingung, timeout: float) -> bool:
    ende = time.time() + timeout
    while time.time() < ende:
        if bedingung():
            return True
        time.sleep(0.05)
    return bedingung()


def test_gescheiterter_aufbau_setzt_letzten_fehler_und_ein_event(tmp_path):
    cfg = Config(host="127.0.0.1", port=_freier_port())
    cfg.db_pfad = tmp_path / "t.db"
    store = Store(tmp_path / "t.db")
    c = Collector(cfg, store)
    c.start()
    try:
        assert _warte(lambda: c.letzter_fehler is not None, 5), \
            "letzter_fehler bleibt leer, obwohl der Aufbau scheitert (Anlassfall 24.09.)"
        assert "Refused" in c.letzter_fehler            # der echte Grund, nicht nur 'fehlgeschlagen'
        assert _warte(lambda: c.fehlversuche >= 2, 6)    # paho versucht es weiter ...
    finally:
        c.stop()
    # ... aber das Protokoll bekommt EIN Event pro Fehlerserie, nicht eins pro Versuch.
    # ⛔ Direkt in der Tabelle zaehlen, NICHT ueber get_gaps(): das fasst
    #    aufeinanderfolgende disconnected-Events ohnehin zu einer Luecke zusammen
    #    und liess eine Sabotage ("Event bei jedem Versuch") gruen durch (24.09.).
    import sqlite3
    con = sqlite3.connect(tmp_path / "t.db")
    n = con.execute("SELECT count(*) FROM connection_events").fetchone()[0]
    con.close()
    assert n == 1, f"{n} Events bei {c.fehlversuche} Fehlversuchen"
    luecken = store.get_gaps()
    assert len(luecken) == 1 and luecken[0]["bis"] is None   # offen -> get_gaps zeigt sie
    assert "fehlgeschlagen" in luecken[0]["grund"]
    assert not c.ist_verbunden


def test_erfolgreicher_aufbau_setzt_die_fehlerserie_zurueck(tmp_path):
    # Ohne Reset wuerde nach einem Wackler NIE wieder ein Event entstehen
    # (fehlversuche bliebe > 1) — der naechste echte Ausfall waere wieder stumm.
    cfg = Config(host="127.0.0.1", port=1)
    cfg.db_pfad = tmp_path / "t.db"
    c = Collector(cfg, Store(tmp_path / "t.db"))
    c.fehlversuche, c.letzter_fehler = 3, "connect fehlgeschlagen (3x): OSError"

    class _Client:
        def subscribe(self, *a, **k): pass

    c._on_connect(_Client(), None, None, 0)
    assert c.fehlversuche == 0
    assert c.letzter_fehler is None
    assert c.ist_verbunden

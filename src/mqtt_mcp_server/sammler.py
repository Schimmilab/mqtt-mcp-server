"""Eigenstaendiger Sammler ohne MCP — laeuft als Dienst (Docker/systemd).

Anlass 2026-09-25: Im MCP-Prozess sammelte der Server nur, solange eine
Claude-Session offen war. Als Dienst auf `leitstand` sammelt er rund um die Uhr;
MCP-Instanzen lesen dieselbe DB mit MQTT_MCP_SAMMELN=0.

Aufruf: mqtt-sammler   (Konfiguration wie beim Server ueber MQTT_MCP_*)
"""
from __future__ import annotations

import signal
import sys
import threading

from .betrieb import hintergrund_starten
from .collector import Collector
from .config import Config
from .store import Store


def main() -> None:
    cfg = Config()
    if not cfg.sammeln:
        sys.exit("mqtt-sammler mit MQTT_MCP_SAMMELN=0 ist ein Widerspruch — abgebrochen.")
    store = Store(cfg.db_pfad, broker=cfg.broker_kennung)
    collector = Collector(cfg, store)
    ende = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: ende.set())
    signal.signal(signal.SIGINT, lambda *_: ende.set())
    hintergrund_starten(cfg, collector, store)
    print(f"[mqtt-sammler] {cfg.broker_kennung} → {cfg.db_pfad}", file=sys.stderr, flush=True)
    try:
        ende.wait()
    finally:
        collector.stop()
        store.close()


if __name__ == "__main__":
    main()

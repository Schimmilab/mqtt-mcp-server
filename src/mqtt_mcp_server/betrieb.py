"""Was im Hintergrund laeuft — getrennt vom MCP, damit der Sammler auch allein laufen kann.

Kein Import von fastmcp hier: `sammler.py` nutzt dieses Modul als Dienst ohne MCP-Stack.
"""
from __future__ import annotations

import sys
import threading
import time
from typing import Callable

from .config import Config

# Im Nur-Lesen-Modus ist "nicht verbunden" der Normalfall — der MCP hat keine
# eigene Broker-Verbindung. Gemessen wird dann die Frische der Datenbank.
FRISCHE_GRENZE_S = 900


def retention_schleife(cfg: Config, store) -> None:
    """Raeumt beim Start und danach stuendlich auf. Meldet auf stderr, was wegfiel."""
    while True:
        try:
            b = store.aufraeumen(cfg.retention_tage, cfg.max_db_mb)
            if b["nach_alter"] or b["nach_groesse"]:
                print(f"[mqtt-mcp] Retention: {b['nach_alter']} Zeilen ueber "
                      f"{cfg.retention_tage} Tage, {b['nach_groesse']} wegen Groesse; "
                      f"{b['db_mb_vorher']} -> {b['db_mb_nachher']} MB; "
                      f"aeltester Rest {b['aeltester_rest']}", file=sys.stderr)
        except Exception as e:  # noqa: BLE001
            print(f"[mqtt-mcp] Retention fehlgeschlagen: {e}", file=sys.stderr)
        time.sleep(3600)


def hintergrund_starten(cfg: Config, collector, store,
                        retention: Callable[[], None] | None = None) -> list[str]:
    """Startet Sammler + Retention — aber nur im Sammelmodus.

    ⛔ Ein lesender Prozess darf weder sammeln (doppelte Zeilen neben dem Dienst)
       noch aufraeumen (Retention gehoert dem Schreiber).
    """
    if not cfg.sammeln:
        return []
    collector.start()
    ziel = retention or (lambda: retention_schleife(cfg, store))
    threading.Thread(target=ziel, daemon=True).start()
    return ["sammler", "retention"]


def verbindungshinweis(sammeln: bool, ist_verbunden: bool, letzte_ts: float | None,
                       jetzt: float | None = None) -> str | None:
    jetzt = time.time() if jetzt is None else jetzt
    if sammeln:
        if ist_verbunden:
            return None
        return ("⚠️ Sammler ist derzeit NICHT verbunden — fehlende Daten koennen daran liegen, "
                "nicht an stillen Geraeten.")
    if letzte_ts is None:
        return "⚠️ Nur-Lesen-Modus: Datenbank ist leer — laeuft der Sammler-Dienst?"
    alter = jetzt - letzte_ts
    if alter > FRISCHE_GRENZE_S:
        return (f"⚠️ Nur-Lesen-Modus: juengste Nachricht ist {int(alter // 60)} min alt — "
                "der Sammler-Dienst schreibt vermutlich nicht mehr.")
    return None

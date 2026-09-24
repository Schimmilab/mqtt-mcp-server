"""MQTT-Sammler: abonniert den Broker und schreibt alles in den Store.

Laeuft in einem eigenen Thread (paho `loop_start`) neben dem stdio-MCP.
Die Koexistenz wurde am 2026-08-01 mit einem Spike verifiziert, bevor der
Rest gebaut wurde — sie war das einzige Risiko, das den Entwurf gekippt haette.
"""
from __future__ import annotations

import fnmatch
import sys
import threading
import time

import paho.mqtt.client as mqtt

from .config import Config
from .store import Store

# Nachrichten, die innerhalb dieser Zeitspanne nach dem Verbinden ankommen,
# gelten als Startschwall (retained Altbestand). Am 2026-08-01 lieferte ein
# Broker 29.664 retained Messages in Sekunden — als frischer Verkehr gelesen
# ergaben die 851 MB/Tag. Deshalb werden sie markiert, nicht verworfen.
BACKLOG_FENSTER_S = 5.0


class Collector:
    def __init__(self, cfg: Config, store: Store) -> None:
        self.cfg = cfg
        self.store = store
        self._verbunden = threading.Event()
        self._connect_ts = 0.0
        self.empfangen = 0
        self.letzter_fehler: str | None = None
        self.fehlversuche = 0

        self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        if cfg.username:
            self._client.username_pw_set(cfg.username, cfg.password or None)
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_connect_fail = self._on_connect_fail
        self._client.on_message = self._on_message

    # ------------------------------------------------------------------ callbacks

    @staticmethod
    def _rc_ist_fehler(rc) -> bool:
        """paho 2.x liefert bei der VERSION2-API ein ReasonCode, kein int.

        ⛔ Gefunden am 2026-08-01 beim ersten End-to-End-Test: `int(rc)` warf einen
        TypeError, der Callback brach ab und es wurde NIE abonniert. Der Server lief
        weiter und antwortete brav — nur ohne Daten. Genau die Sorte stiller Fehler,
        gegen die dieser Server gebaut ist.
        """
        if hasattr(rc, "is_failure"):
            return bool(rc.is_failure)
        try:
            return int(getattr(rc, "value", rc)) != 0
        except (TypeError, ValueError):
            return False

    def _on_connect(self, client, userdata, flags, rc, properties=None) -> None:
        if self._rc_ist_fehler(rc):
            self.letzter_fehler = f"connect rc={rc}"
            self.store.add_connection_event("disconnected", f"connect abgelehnt rc={rc}")
            return
        self._connect_ts = time.time()
        self._verbunden.set()
        self.letzter_fehler = None
        self.fehlversuche = 0
        self.store.add_connection_event("connected", f"{self.cfg.host}:{self.cfg.port}")
        for t in self.cfg.topics:
            client.subscribe(t, qos=0)

    def _on_connect_fail(self, client, userdata) -> None:
        """Verbindungsaufbau gescheitert (TCP/DNS) — paho ruft dann NICHT on_disconnect.

        ⛔ Gefunden am 2026-09-24: 27,6 h ohne Daten, weil macOS Python aus der
        IDE-Session den LAN-Zugriff verweigerte ("No route to host"). Ohne diesen
        Callback blieb `letzter_fehler` leer und es entstand kein einziges Event —
        der Sammler sah aus wie einer, der nur gerade keine Nachrichten bekommt.

        paho uebergibt die Exception nicht, ruft uns aber aus seinem
        `except OSError:`-Block auf — deshalb liefert sys.exc_info() sie hier.
        Ein Event nur beim ERSTEN Fehlversuch einer Serie: paho versucht es mit
        Backoff bis 120 s weiter, ein Event pro Versuch waere Rauschen. Die
        offene Luecke zeigt get_gaps() als "noch offen", bis on_connect sie schliesst.
        """
        exc = sys.exc_info()[1]
        grund = f"{type(exc).__name__}: {exc}" if exc else "unbekannt"
        self.fehlversuche += 1
        self.letzter_fehler = f"connect fehlgeschlagen ({self.fehlversuche}x): {grund}"
        if self.fehlversuche == 1:
            self.store.add_connection_event("disconnected", f"connect fehlgeschlagen: {grund}")

    def _on_disconnect(self, client, userdata, flags=None, rc=None, properties=None) -> None:
        self._verbunden.clear()
        self.store.add_connection_event("disconnected", f"rc={rc}")

    def _on_message(self, client, userdata, msg) -> None:
        self.empfangen += 1
        aus_backlog = (time.time() - self._connect_ts) < BACKLOG_FENSTER_S
        try:
            self.store.add_message(msg.topic, msg.payload, msg.qos, bool(msg.retain), aus_backlog)
        except Exception as e:  # noqa: BLE001
            self.letzter_fehler = f"{type(e).__name__}: {e}"

    # -------------------------------------------------------------------- steuern

    def start(self) -> None:
        # connect_async + loop_start: paho kuemmert sich selbst um Reconnects.
        self._client.connect_async(self.cfg.host, self.cfg.port, keepalive=30)
        self._client.loop_start()

    def stop(self) -> None:
        try:
            self._client.loop_stop()
            self._client.disconnect()
        except Exception:  # noqa: BLE001
            pass

    def warte_auf_verbindung(self, timeout: float = 5.0) -> bool:
        return self._verbunden.wait(timeout)

    @property
    def ist_verbunden(self) -> bool:
        return self._verbunden.is_set()

    # ------------------------------------------------------------------ publizieren

    def darf_publizieren(self, topic: str) -> tuple[bool, str]:
        """Zwei Huerden: Schreibmodus UND Topic-Sperrliste (Entscheidung 5)."""
        if not self.cfg.publish_erlaubt:
            return False, ("Schreibmodus ist aus. Zum Freischalten "
                           "MQTT_MCP_ALLOW_PUBLISH=1 setzen und die Session neu starten.")
        for muster in self.cfg.gesperrte_topics:
            if fnmatch.fnmatch(topic, muster.replace("+", "*")):
                return False, (f"Topic ist gesperrt (Muster '{muster}'). Diese Liste schuetzt "
                               "Geraete, deren Ausfall teuer war — Server-Dosen, Gefrierschrank. "
                               "Aenderbar ueber MQTT_MCP_BLOCKED_TOPICS.")
        return True, ""

    def publish(self, topic: str, payload: str, qos: int = 0, retain: bool = False) -> dict:
        ok, grund = self.darf_publizieren(topic)
        if not ok:
            return {"ok": False, "grund": grund}
        if not self.ist_verbunden:
            return {"ok": False, "grund": "nicht mit dem Broker verbunden"}
        info = self._client.publish(topic, payload, qos=qos, retain=retain)
        info.wait_for_publish(timeout=5)
        return {"ok": info.is_published(), "topic": topic, "qos": qos, "retain": retain}

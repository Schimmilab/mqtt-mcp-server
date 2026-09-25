"""Konfiguration aus Umgebungsvariablen.

Alle Werte haben neutrale Defaults; der Server laeuft ohne Konfiguration gegen
einen lokalen Broker und laesst sich vollstaendig ueber Env-Variablen umstellen.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _bool(name: str, default: bool = False) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "ja", "on")


def _liste(name: str, default: list[str]) -> list[str]:
    v = os.environ.get(name)
    if v is None:
        return list(default)
    return [t.strip() for t in v.split(",") if t.strip()]


# Topics, auf die auch im Schreibmodus NICHT publiziert wird.
# ⚠️ Das ist eine Bremse gegen Fehlgriffe, KEINE Sicherheitsgrenze — wer den
# Server hat, kann die Liste aendern. Echte Erzwingung ginge nur ueber eine
# Broker-ACL. Die Auswahl kommt aus dem 01.08.: ein schaltender Zwischenstecker
# vor IPS-PC + hometux hat neun Tage Stillstand und sieben Wochen Datenverlust
# gekostet; beim Gefrierschrank waere die Folge aufgetautes Gefriergut.
DEFAULT_GESPERRT = [
    "cmnd/+/POWER*",          # jede Tasmota-Schaltdose
    "cmnd/+/Restart",
    "cmnd/+/Reset",
    "cmnd/+/Upgrade",
    "shellies/+/command",
    "shellies/+/relay/+/command",
]


@dataclass
class Config:
    host: str = field(default_factory=lambda: os.environ.get("MQTT_MCP_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.environ.get("MQTT_MCP_PORT", "1883")))
    username: str | None = field(default_factory=lambda: os.environ.get("MQTT_MCP_USERNAME") or None)
    password: str | None = field(default_factory=lambda: os.environ.get("MQTT_MCP_PASSWORD") or None)
    topics: list[str] = field(default_factory=lambda: _liste("MQTT_MCP_TOPICS", ["#"]))
    db_pfad: Path = field(
        default_factory=lambda: Path(
            os.environ.get("MQTT_MCP_DB", str(Path.home() / ".local/share/mqtt-mcp/history.db"))
        ).expanduser()
    )
    # Retention: 30 Tage ODER 2 GB, was zuerst greift (Entscheidung 4).
    retention_tage: int = field(default_factory=lambda: int(os.environ.get("MQTT_MCP_RETENTION_TAGE", "30")))
    max_db_mb: int = field(default_factory=lambda: int(os.environ.get("MQTT_MCP_MAX_DB_MB", "2048")))
    # Schreiben ist aus (Entscheidung 5).
    publish_erlaubt: bool = field(default_factory=lambda: _bool("MQTT_MCP_ALLOW_PUBLISH", False))
    gesperrte_topics: list[str] = field(default_factory=lambda: _liste("MQTT_MCP_BLOCKED_TOPICS", DEFAULT_GESPERRT))
    # Sammelt dieser Prozess selbst? Aus (0), wenn ein eigenstaendiger `mqtt-sammler`
    # dieselbe DB fuellt und der MCP nur liest (Umzug auf leitstand, 2026-09-25).
    sammeln: bool = field(default_factory=lambda: _bool("MQTT_MCP_SAMMELN", True))

    def __post_init__(self) -> None:
        self.db_pfad.parent.mkdir(parents=True, exist_ok=True)

    @property
    def broker_kennung(self) -> str:
        """Was in jede Zeile als Herkunft geschrieben wird.

        `host:port` und nicht etwa der MCP-Servername: Der Servername lebt in
        `~/.claude.json` und kann sich aendern, ohne dass sich die Quelle
        aendert — dann waere die Historie in sich widerspruechlich.
        """
        return f"{self.host}:{self.port}"

    @property
    def peer_dbs(self) -> list[Path]:
        """Datenbanken anderer Broker, gegen die verglichen werden kann.

        ⭐ Default: alle `*.db` NEBEN der eigenen. Seit der Trennung am
        22.08. hat jeder Broker seine eigene Datei im selben Verzeichnis —
        eine Konvention, die keiner Pflege beduerftig ist (Top-Regel 7:
        Mechanismus statt Vorsatz; eine Liste, die jemand nachtragen muesste,
        waere beim dritten Broker veraltet).
        Ueberschreibbar per `MQTT_MCP_PEER_DBS` (kommagetrennt).
        """
        roh = os.environ.get("MQTT_MCP_PEER_DBS")
        if roh:
            return [Path(t.strip()).expanduser() for t in roh.split(",") if t.strip()]
        return sorted(p for p in self.db_pfad.parent.glob("*.db") if p != self.db_pfad)

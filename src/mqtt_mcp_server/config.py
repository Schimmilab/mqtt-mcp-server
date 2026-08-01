"""Konfiguration aus Umgebungsvariablen.

Alle Werte haben Defaults, die auf Schimmis Setup passen — der Server laeuft
also ohne Konfiguration, laesst sich aber vollstaendig umstellen.
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
    host: str = field(default_factory=lambda: os.environ.get("MQTT_MCP_HOST", "192.168.178.65"))
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

    def __post_init__(self) -> None:
        self.db_pfad.parent.mkdir(parents=True, exist_ok=True)

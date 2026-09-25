"""MCP-Server: sieben Werkzeuge auf der MQTT-Historie.

Sechs lesen, eines schreibt — und das schreibende ist doppelt gesichert
(Env-Schalter + Topic-Sperrliste).
"""
from __future__ import annotations

import time

from fastmcp import FastMCP

from .betrieb import hintergrund_starten, verbindungshinweis
from .config import Config
from .collector import Collector
from .store import Store

cfg = Config()
store = Store(cfg.db_pfad, broker=cfg.broker_kennung)
collector = Collector(cfg, store)

mcp = FastMCP("mqtt-mcp-server")


def _seit(stunden: float | None) -> float | None:
    return None if stunden is None else time.time() - stunden * 3600


@mcp.tool
def list_topics(pattern: str | None = None, seit_stunden: float | None = None,
                limit: int = 200) -> dict:
    """Topic-Inventar: welche Topics gibt es, wie viele Nachrichten, wann zuletzt.

    pattern: Glob wie 'tele/*' oder '*SENSOR'. seit_stunden schraenkt auf den
    juengeren Zeitraum ein.
    """
    treffer = store.list_topics(pattern=pattern, since=_seit(seit_stunden), limit=limit)
    return {"anzahl": len(treffer), "topics": treffer,
            "hinweis_verbindung": _verbindungshinweis()}


@mcp.tool
def get_last(topic: str) -> dict:
    """Letzter bekannter Wert eines Topics — sofort, ohne auf die naechste Nachricht zu warten.

    Genau das, was fertige MQTT-MCP-Server nicht koennen: bei einem schlafenden
    Batteriesensor (Shelly H&T) kaeme die naechste Nachricht erst in Stunden.
    """
    r = store.get_last(topic)
    if r is None:
        return {"gefunden": False, "topic": topic,
                "hinweis": ("Kein Wert in der Historie. Das heisst NICHT zwingend, dass das "
                            "Geraet still ist — pruefe mit get_gaps(), ob der Sammler zu der "
                            "Zeit ueberhaupt verbunden war.")}
    return {"gefunden": True, **r}


@mcp.tool
def get_history(topic: str, seit_stunden: float | None = 24, limit: int = 200,
                mit_startschwall: bool = True) -> dict:
    """Verlauf eines Topics mit Zeitstempeln — die Kernfunktion dieses Servers.

    mit_startschwall=False blendet den retained-Altbestand aus, der direkt nach
    dem Verbinden ankommt und einen irrefuehrend frischen Zeitstempel traegt.
    """
    eintraege = store.get_history(topic, since=_seit(seit_stunden), limit=limit,
                                  mit_backlog=mit_startschwall)
    return {"topic": topic, "anzahl": len(eintraege), "eintraege": eintraege,
            "hinweis_verbindung": _verbindungshinweis()}


@mcp.tool
def get_tree(prefix: str = "", tiefe: int = 2, limit: int = 300) -> dict:
    """Topic-Baum ab einem Praefix — der Ueberblick wie im MQTT Explorer."""
    alle = store.list_topics(limit=5000)
    knoten: dict[str, dict] = {}
    for e in alle:
        t = e["topic"]
        if prefix and not t.startswith(prefix):
            continue
        rest = t[len(prefix):].lstrip("/") if prefix else t
        teile = [x for x in rest.split("/") if x][:max(1, tiefe)]
        if not teile:
            continue
        schluessel = "/".join(teile)
        k = knoten.setdefault(schluessel, {"zweig": schluessel, "topics": 0, "nachrichten": 0,
                                           "zuletzt": None})
        k["topics"] += 1
        k["nachrichten"] += e["nachrichten"]
        if k["zuletzt"] is None or (e["zuletzt"] or "") > k["zuletzt"]:
            k["zuletzt"] = e["zuletzt"]
    aus = sorted(knoten.values(), key=lambda x: x["nachrichten"], reverse=True)[:limit]
    return {"prefix": prefix or "(alle)", "zweige": len(aus), "baum": aus}


@mcp.tool
def find_silent(still_seit_stunden: float = 24, limit: int = 100) -> dict:
    """Topics, die seit X Stunden NICHT mehr gemeldet haben.

    Die Frage vom 2026-08-01: 'seit wann meldet dieser Stecker nichts mehr?'
    (sonoff-4854 war seit dem 02.06. still — ein defekter Zwischenstecker).
    """
    stille = store.find_silent(still_seit_stunden * 3600, limit=limit)
    luecken = store.get_gaps()
    return {
        "anzahl": len(stille), "topics": stille,
        "⚠️ vor der Deutung pruefen": (
            f"{len(luecken)} Verbindungsluecke(n) in der Historie — ein Topic kann auch "
            "deshalb 'still' wirken, weil der Sammler nicht verbunden war. Siehe get_gaps()."
        ) if luecken else "keine Verbindungsluecken — Stille ist echte Geraetestille",
    }


@mcp.tool
def get_gaps(seit_stunden: float | None = None) -> dict:
    """Zeitraeume, in denen der Sammler NICHT mit dem Broker verbunden war.

    Trennt 'Geraet ist still' von 'ich habe nicht zugehoert'. Ohne diese
    Auskunft sehen beide Faelle in den Daten identisch aus.
    """
    luecken = store.get_gaps(since=_seit(seit_stunden))
    return {"anzahl": len(luecken), "luecken": luecken,
            "aktuell_verbunden": collector.ist_verbunden,
            "letzter_fehler": collector.letzter_fehler}


@mcp.tool
def broker_konflikte(seit_stunden: float | None = None, limit: int = 200,
                     nur_verdaechtige: bool = True, stichprobe: int = 40) -> dict:
    """Topics, die von MEHR ALS EINEM Broker bespielt werden.

    Der Ownership-Canary fuer den Parallelbetrieb zweier Broker: Die
    gefaehrliche Lage ist nicht "ein Geraet antwortet nicht", sondern "zwei
    Systeme schreiben auf dasselbe Topic und keiner merkt es".

    Verglichen wird ueber ALLE Broker-Datenbanken nebeneinander (jeder Server
    hat seit dem 22.08. seine eigene). `messbar` sagt, ob das Ergebnis
    ueberhaupt belastbar ist — bei `false` oder `"teilweise"` ist eine leere
    Konfliktliste ausdruecklich KEINE Entwarnung.

    ⛔ Eine BRIDGE ist kein Konflikt. Laeuft eine Bridge zwischen den Brokern,
    fuehren beide dieselben Topics — das ist der Normalzustand, nicht die
    Anomalie. Jedes Doppel-Topic wird deshalb eingeordnet:

      gespiegelt                 Bridge belegt (hohe Payload-Gleichheit)
      wahrscheinlich_gespiegelt  Payloads zu einfoermig fuer einen Beweis,
                                 aber der Zeitversatz spricht dafuer
      unabhaengig                ⛔ der echte Befund: zwei Schreiber
      unklar                     nicht entscheidbar — von Hand ansehen

    `nur_verdaechtige=True` (Default) listet Gespiegeltes nicht, sondern
    zaehlt es in `gespiegelt_nicht_gelistet`. Auf `False` setzen, um alles zu
    sehen.
    """
    e = store.broker_konflikte(cfg.peer_dbs, seit_stunden=seit_stunden, limit=limit,
                               nur_verdaechtige=nur_verdaechtige, stichprobe=stichprobe)
    e["anzahl"] = len(e["konflikte"])
    return e


@mcp.tool
def status() -> dict:
    """Zustand des Servers: Verbindung, Datenbestand, Schreibmodus."""
    return {
        "broker": f"{cfg.host}:{cfg.port}",
        "sammelmodus": "eigener Sammler" if cfg.sammeln else "nur lesen (Sammler-Dienst extern)",
        "verbunden": collector.ist_verbunden,
        "empfangen_diese_session": collector.empfangen,
        "hinweis_verbindung": _verbindungshinweis(),
        "letzter_fehler": collector.letzter_fehler,
        "abonniert": cfg.topics,
        "datenbank": str(cfg.db_pfad),
        "broker_kennung": cfg.broker_kennung,
        "vergleichs_datenbanken": [str(p) for p in cfg.peer_dbs],
        "bestand": store.stats(),
        "retention": f"{cfg.retention_tage} Tage / {cfg.max_db_mb} MB",
        "schreibmodus": "AKTIV" if cfg.publish_erlaubt else "aus (read-only)",
        "gesperrte_topics": cfg.gesperrte_topics,
    }


@mcp.tool
def publish(topic: str, payload: str, qos: int = 0, retain: bool = False) -> dict:
    """Nachricht senden. Nur bei aktivem Schreibmodus und nicht auf gesperrte Topics.

    ⚠️ Die Sperrliste ist eine Bremse gegen Fehlgriffe, keine Sicherheitsgrenze.
    """
    return collector.publish(topic, payload, qos=qos, retain=retain)


def _verbindungshinweis() -> str | None:
    return verbindungshinweis(cfg.sammeln, collector.ist_verbunden, store.neueste_ts())


def main() -> None:
    hintergrund_starten(cfg, collector, store)
    try:
        mcp.run()
    finally:
        collector.stop()
        store.close()


if __name__ == "__main__":
    main()

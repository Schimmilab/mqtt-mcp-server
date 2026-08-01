"""SQLite-Speicher: Nachrichten, Verbindungsereignisse, Retention.

Zwei Tabellen, und die zweite ist der Grund, warum dieser Server existiert:

  messages           was kam wann an
  connection_events  wann war der Sammler ueberhaupt verbunden

⚠️ Ohne die zweite Tabelle ist ein Loch in den Daten nicht deutbar. Am
2026-08-01 wurde dreimal „keine Nachrichten" gelesen und „Geraet ist still"
geschlossen — zweimal war es in Wahrheit „ich habe nicht zugehoert" (falscher
Broker, zu kurzes Zeitfenster). In der Datenbank sieht beides identisch aus.
Genau diese Verwechslung hat den Neun-Tage-IPS-Ausfall im Juni sieben Wochen
lang unsichtbar gemacht.
"""
from __future__ import annotations

import fnmatch
import sqlite3
import threading
import time
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          REAL NOT NULL,
    topic       TEXT NOT NULL,
    payload     BLOB,
    qos         INTEGER NOT NULL DEFAULT 0,
    retained    INTEGER NOT NULL DEFAULT 0,  -- MQTT-Retain-Flag
    aus_backlog INTEGER NOT NULL DEFAULT 0   -- kam im Schwall direkt nach dem Verbinden
);
CREATE INDEX IF NOT EXISTS idx_msg_topic_ts ON messages(topic, ts DESC);
CREATE INDEX IF NOT EXISTS idx_msg_ts       ON messages(ts DESC);

CREATE TABLE IF NOT EXISTS connection_events (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    ts     REAL NOT NULL,
    event  TEXT NOT NULL,      -- 'connected' | 'disconnected'
    detail TEXT
);
CREATE INDEX IF NOT EXISTS idx_conn_ts ON connection_events(ts DESC);
"""


def _text(payload: bytes | None, limit: int = 400) -> str:
    if payload is None:
        return ""
    s = payload.decode("utf-8", errors="replace")
    return s if len(s) <= limit else s[:limit] + f"… (+{len(s)-limit} Z.)"


class Store:
    def __init__(self, pfad: Path) -> None:
        self.pfad = pfad
        # check_same_thread=False: der MQTT-Thread schreibt, der MCP-Thread liest.
        # WAL, damit Leser den Schreiber nicht blockieren (im Spike verifiziert).
        self._db = sqlite3.connect(pfad, check_same_thread=False, timeout=10)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.executescript(SCHEMA)
        self._db.commit()
        self._lock = threading.Lock()

    # ---------------------------------------------------------------- schreiben

    def add_message(self, topic: str, payload: bytes, qos: int, retained: bool,
                    aus_backlog: bool, ts: float | None = None) -> None:
        with self._lock:
            self._db.execute(
                "INSERT INTO messages (ts, topic, payload, qos, retained, aus_backlog)"
                " VALUES (?,?,?,?,?,?)",
                (ts if ts is not None else time.time(), topic, payload, qos,
                 1 if retained else 0, 1 if aus_backlog else 0),
            )
            self._db.commit()

    def add_connection_event(self, event: str, detail: str = "") -> None:
        with self._lock:
            self._db.execute(
                "INSERT INTO connection_events (ts, event, detail) VALUES (?,?,?)",
                (time.time(), event, detail),
            )
            self._db.commit()

    # ------------------------------------------------------------------ abfragen

    def list_topics(self, pattern: str | None = None, since: float | None = None,
                    limit: int = 500) -> list[dict]:
        sql = ("SELECT topic, COUNT(*) n, MAX(ts) letzte, MIN(ts) erste,"
               " SUM(retained) ret FROM messages")
        args: list = []
        if since is not None:
            sql += " WHERE ts >= ?"
            args.append(since)
        sql += " GROUP BY topic ORDER BY letzte DESC LIMIT ?"
        args.append(max(1, limit))
        with self._lock:
            rows = self._db.execute(sql, args).fetchall()
        out = []
        for topic, n, letzte, erste, ret in rows:
            if pattern and not fnmatch.fnmatch(topic, pattern):
                continue
            out.append({
                "topic": topic, "nachrichten": n,
                "zuletzt": _iso(letzte), "erstmals": _iso(erste),
                "nur_retained": bool(ret == n),
            })
        return out

    def get_last(self, topic: str) -> dict | None:
        with self._lock:
            r = self._db.execute(
                "SELECT ts, payload, qos, retained, aus_backlog FROM messages"
                " WHERE topic = ? ORDER BY ts DESC LIMIT 1", (topic,)).fetchone()
        if not r:
            return None
        ts, payload, qos, retained, backlog = r
        return {
            "topic": topic, "wert": _text(payload), "zeit": _iso(ts),
            "alter_sekunden": round(time.time() - ts, 1),
            "qos": qos, "retained": bool(retained),
            # Ehrlichkeitsflag: der Wert kam aus dem Startschwall. Der Zeitstempel
            # sagt, wann WIR ihn gesehen haben — nicht, wann er entstanden ist.
            "aus_startschwall": bool(backlog),
        }

    def get_history(self, topic: str, since: float | None = None, until: float | None = None,
                    limit: int = 200, mit_backlog: bool = True) -> list[dict]:
        sql = "SELECT ts, payload, qos, retained, aus_backlog FROM messages WHERE topic = ?"
        args: list = [topic]
        if since is not None:
            sql += " AND ts >= ?"; args.append(since)
        if until is not None:
            sql += " AND ts <= ?"; args.append(until)
        if not mit_backlog:
            sql += " AND aus_backlog = 0"
        sql += " ORDER BY ts DESC LIMIT ?"
        args.append(max(1, limit))
        with self._lock:
            rows = self._db.execute(sql, args).fetchall()
        return [{"zeit": _iso(ts), "wert": _text(p), "qos": q,
                 "retained": bool(r), "aus_startschwall": bool(b)}
                for ts, p, q, r, b in rows]

    def find_silent(self, seit_sekunden: float, limit: int = 200) -> list[dict]:
        grenze = time.time() - seit_sekunden
        with self._lock:
            rows = self._db.execute(
                "SELECT topic, MAX(ts) letzte, COUNT(*) n FROM messages"
                " GROUP BY topic HAVING letzte < ? ORDER BY letzte ASC LIMIT ?",
                (grenze, max(1, limit))).fetchall()
        return [{"topic": t, "zuletzt": _iso(ts), "still_seit_stunden": round((time.time()-ts)/3600, 1),
                 "nachrichten_gesamt": n} for t, ts, n in rows]

    def get_gaps(self, since: float | None = None) -> list[dict]:
        """Zeitraeume, in denen der Sammler NICHT verbunden war.

        Ohne diese Auskunft ist 'keine Daten' nicht von 'Geraet still' zu
        unterscheiden — siehe Modulkopf.
        """
        sql = "SELECT ts, event, detail FROM connection_events"
        args: list = []
        if since is not None:
            sql += " WHERE ts >= ?"; args.append(since)
        sql += " ORDER BY ts ASC"
        with self._lock:
            rows = self._db.execute(sql, args).fetchall()
        luecken, offen = [], None
        for ts, event, detail in rows:
            if event == "disconnected" and offen is None:
                offen = (ts, detail)
            elif event == "connected" and offen is not None:
                luecken.append({"von": _iso(offen[0]), "bis": _iso(ts),
                                "dauer_minuten": round((ts - offen[0]) / 60, 1),
                                "grund": offen[1] or ""})
                offen = None
        if offen is not None:
            luecken.append({"von": _iso(offen[0]), "bis": None,
                            "dauer_minuten": round((time.time() - offen[0]) / 60, 1),
                            "grund": offen[1] or "", "hinweis": "noch offen"})
        return luecken

    def stats(self) -> dict:
        with self._lock:
            n = self._db.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
            t = self._db.execute("SELECT COUNT(DISTINCT topic) FROM messages").fetchone()[0]
            spanne = self._db.execute("SELECT MIN(ts), MAX(ts) FROM messages").fetchone()
        return {"nachrichten": n, "topics": t,
                "aeltestes": _iso(spanne[0]) if spanne[0] else None,
                "neuestes": _iso(spanne[1]) if spanne[1] else None,
                "db_mb": round(self.db_groesse_mb(), 1)}

    def db_groesse_mb(self) -> float:
        gesamt = 0
        for suffix in ("", "-wal", "-shm"):
            p = Path(str(self.pfad) + suffix)
            if p.exists():
                gesamt += p.stat().st_size
        return gesamt / 1024 / 1024

    # ----------------------------------------------------------------- retention

    def aufraeumen(self, tage: int, max_mb: int) -> dict:
        """Loescht nach Alter, danach notfalls nach Groesse.

        Gibt IMMER zurueck, was weggeworfen wurde — stilles Loeschen wuerde die
        Frage 'seit wann meldet der nicht mehr?' unbemerkt unbeantwortbar machen.
        """
        bericht = {"nach_alter": 0, "nach_groesse": 0, "aeltester_rest": None, "db_mb_vorher": round(self.db_groesse_mb(), 1)}
        grenze = time.time() - tage * 86400
        with self._lock:
            cur = self._db.execute("DELETE FROM messages WHERE ts < ?", (grenze,))
            bericht["nach_alter"] = cur.rowcount
            self._db.execute("DELETE FROM connection_events WHERE ts < ?", (grenze,))
            self._db.commit()

        # Notbremse: ein Amok-Publisher kann die Zeitregel wirkungslos machen.
        runden = 0
        while self.db_groesse_mb() > max_mb and runden < 50:
            with self._lock:
                cur = self._db.execute(
                    "DELETE FROM messages WHERE id IN"
                    " (SELECT id FROM messages ORDER BY ts ASC LIMIT 20000)")
                bericht["nach_groesse"] += cur.rowcount
                self._db.commit()
            if cur.rowcount == 0:
                break
            runden += 1

        if bericht["nach_alter"] or bericht["nach_groesse"]:
            with self._lock:
                self._db.execute("VACUUM")
                r = self._db.execute("SELECT MIN(ts) FROM messages").fetchone()
            bericht["aeltester_rest"] = _iso(r[0]) if r and r[0] else None
        bericht["db_mb_nachher"] = round(self.db_groesse_mb(), 1)
        return bericht

    def close(self) -> None:
        with self._lock:
            self._db.close()


def _iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))

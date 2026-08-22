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
    aus_backlog INTEGER NOT NULL DEFAULT 0,  -- kam im Schwall direkt nach dem Verbinden
    broker      TEXT                         -- host:port der Quelle; NULL = Altbestand
);
CREATE INDEX IF NOT EXISTS idx_msg_topic_ts ON messages(topic, ts DESC);
CREATE INDEX IF NOT EXISTS idx_msg_ts       ON messages(ts DESC);
-- ⛔ idx_msg_broker steht bewusst NICHT hier, sondern in _migriere(): Bei einer
--    DB aus der Zeit vor dem 22.08. gibt es die Spalte noch nicht, und
--    `CREATE INDEX` liefe hier VOR dem `ALTER TABLE`. Am Ist gesehen —
--    der erste Testlauf starb mit "no such column: broker".

CREATE TABLE IF NOT EXISTS connection_events (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    ts     REAL NOT NULL,
    event  TEXT NOT NULL,      -- 'connected' | 'disconnected'
    detail TEXT,
    broker TEXT
);
CREATE INDEX IF NOT EXISTS idx_conn_ts ON connection_events(ts DESC);
"""


def _text(payload: bytes | None, limit: int = 400) -> str:
    if payload is None:
        return ""
    s = payload.decode("utf-8", errors="replace")
    return s if len(s) <= limit else s[:limit] + f"… (+{len(s)-limit} Z.)"


class Store:
    """⭐ `broker` beantwortet die Frage WOHER, nicht nur WAS (neu 2026-08-22).

    Bis dahin trug eine Zeile nur `topic` und `ts`. Solange ein Rechner genau
    einen Broker kennt, reicht das — im **Parallelbetrieb** nicht mehr: Dort
    existiert dasselbe Topic auf altem und neuem Broker gleichzeitig, und ein
    Wert ohne Herkunft ist dann nicht falsch, sondern **unzuordenbar**. Das ist
    die unangenehmere Sorte Fehler, weil er wie ein Messwert aussieht.

    ⛔ Bestehende Zeilen bekommen `broker = NULL`, nicht etwa den aktuellen
    Broker nachgetragen. Der Altbestand IST unzuordenbar — ihn nachtraeglich
    zu etikettieren, waere eine erfundene Herkunft.
    """

    def __init__(self, pfad: Path, broker: str | None = None) -> None:
        self.pfad = pfad
        self.broker = broker
        # check_same_thread=False: der MQTT-Thread schreibt, der MCP-Thread liest.
        # WAL, damit Leser den Schreiber nicht blockieren (im Spike verifiziert).
        self._db = sqlite3.connect(pfad, check_same_thread=False, timeout=10)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.executescript(SCHEMA)
        self._migriere()
        self._db.commit()
        self._lock = threading.Lock()

    def _migriere(self) -> None:
        """`broker` in bestehende Datenbanken nachziehen. Idempotent.

        `CREATE TABLE IF NOT EXISTS` fasst eine vorhandene Tabelle nicht an —
        ohne diesen Schritt haette eine bestehende DB die neue Spalte nie
        bekommen und jeder Insert waere gescheitert.
        """
        for tabelle in ("messages", "connection_events"):
            spalten = {r[1] for r in self._db.execute(f"PRAGMA table_info({tabelle})")}
            if "broker" not in spalten:
                self._db.execute(f"ALTER TABLE {tabelle} ADD COLUMN broker TEXT")
        self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_msg_broker ON messages(broker, topic)")

    # ---------------------------------------------------------------- schreiben

    def add_message(self, topic: str, payload: bytes, qos: int, retained: bool,
                    aus_backlog: bool, ts: float | None = None) -> None:
        with self._lock:
            self._db.execute(
                "INSERT INTO messages (ts, topic, payload, qos, retained, aus_backlog, broker)"
                " VALUES (?,?,?,?,?,?,?)",
                (ts if ts is not None else time.time(), topic, payload, qos,
                 1 if retained else 0, 1 if aus_backlog else 0, self.broker),
            )
            self._db.commit()

    def add_connection_event(self, event: str, detail: str = "") -> None:
        with self._lock:
            self._db.execute(
                "INSERT INTO connection_events (ts, event, detail, broker)"
                " VALUES (?,?,?,?)",
                (time.time(), event, detail, self.broker),
            )
            self._db.commit()

    # ------------------------------------------------------------------ abfragen

    def list_topics(self, pattern: str | None = None, since: float | None = None,
                    limit: int = 500) -> list[dict]:
        # ⛔ DAS PATTERN GEHOERT INS SQL, NICHT HINTER DAS LIMIT (Fix 2026-08-22).
        #    Vorher lief `LIMIT ?` im SQL und `fnmatch` DANACH in Python: die
        #    Datenbank lieferte die N zuletzt aktiven Topics, und erst darauf
        #    wurde gefiltert. Ein Topic, das nicht unter die juengsten N fiel,
        #    war unauffindbar — die Antwort war dann ein sauberes `0 Topics`.
        #    ⚠️ Genau die gefaehrliche Sorte Fehler: kein Fehlertext, kein
        #    leerer Rueckgabewert, der nach Fehler aussieht, sondern ein
        #    Ergebnis, das wie ein Befund gelesen wird ("gibt es nicht").
        #    Am Ist gefunden am 22.08. beim Bau der Ownership-Karte:
        #    `list_topics(pattern="display/*", limit=30)` meldete 0, waehrend
        #    `SELECT COUNT(DISTINCT topic) ... GLOB 'display/*'` **7** liefert.
        #    🎯 Der Schaden waere im Parallelbetrieb maximal gewesen: die
        #    Ownership-Pruefung fragt nach Topics, die zwei Besitzer haben —
        #    und "keine gefunden" ist dort exakt die Antwort, die man hoeren
        #    will. Ein kaputter Filter haette die Freigabe erteilt.
        #    SQLite-GLOB und fnmatch sind fuer `*`, `?` und `[...]` deckungs-
        #    gleich, die Aufrufsyntax aendert sich also nicht.
        sql = ("SELECT topic, COUNT(*) n, MAX(ts) letzte, MIN(ts) erste,"
               " SUM(retained) ret,"
               " COUNT(DISTINCT COALESCE(broker,'?')) quellen,"
               " GROUP_CONCAT(DISTINCT COALESCE(broker,'?')) broker FROM messages")
        args: list = []
        bedingungen = []
        if since is not None:
            bedingungen.append("ts >= ?")
            args.append(since)
        if pattern:
            bedingungen.append("topic GLOB ?")
            args.append(pattern)
        if bedingungen:
            sql += " WHERE " + " AND ".join(bedingungen)
        sql += " GROUP BY topic ORDER BY letzte DESC LIMIT ?"
        args.append(max(1, limit))
        with self._lock:
            rows = self._db.execute(sql, args).fetchall()
        out = []
        for topic, n, letzte, erste, ret, quellen, broker in rows:
            eintrag = {
                "topic": topic, "nachrichten": n,
                "zuletzt": _iso(letzte), "erstmals": _iso(erste),
                "nur_retained": bool(ret == n),
                "broker": broker,
            }
            # ⛔ Mehrere Quellen fuer EIN Topic: im Parallelbetrieb ist das der
            #    Befund, nicht ein Detail. Deshalb als eigenes Feld und nicht
            #    nur implizit in einer kommagetrennten Liste.
            if quellen > 1:
                eintrag["mehrere_quellen"] = True
            out.append(eintrag)
        return out

    def get_last(self, topic: str) -> dict | None:
        with self._lock:
            r = self._db.execute(
                "SELECT ts, payload, qos, retained, aus_backlog, broker FROM messages"
                " WHERE topic = ? ORDER BY ts DESC LIMIT 1", (topic,)).fetchone()
        if not r:
            return None
        ts, payload, qos, retained, backlog, broker = r
        return {
            "topic": topic, "wert": _text(payload), "zeit": _iso(ts),
            "alter_sekunden": round(time.time() - ts, 1),
            "qos": qos, "retained": bool(retained), "broker": broker,
            # Ehrlichkeitsflag: der Wert kam aus dem Startschwall. Der Zeitstempel
            # sagt, wann WIR ihn gesehen haben — nicht, wann er entstanden ist.
            "aus_startschwall": bool(backlog),
        }

    def get_history(self, topic: str, since: float | None = None, until: float | None = None,
                    limit: int = 200, mit_backlog: bool = True) -> list[dict]:
        sql = ("SELECT ts, payload, qos, retained, aus_backlog, broker FROM messages"
               " WHERE topic = ?")
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
                 "retained": bool(r), "aus_startschwall": bool(b), "broker": brk}
                for ts, p, q, r, b, brk in rows]

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

    # ------------------------------------------------- Vergleich ueber Broker

    def broker_konflikte(self, peer_pfade: list[Path], seit_stunden: float | None = None,
                         limit: int = 200) -> dict:
        """Welche Topics werden von MEHR ALS EINEM Broker bespielt?

        ⭐ Das ist der Ownership-Canary fuer den IPS-Parallelbetrieb: Solange
        alter und neuer Broker nebeneinander laufen, ist die gefaehrliche Lage
        nicht „ein Geraet antwortet nicht", sondern „zwei Systeme schreiben auf
        dasselbe Topic und keiner merkt es".

        ⛔ WARUM UEBER MEHRERE DATEIEN UND NICHT UEBER EINE SPALTE ALLEIN:
        Seit dem 22.08. hat jeder Server seine eigene DB — die Herkunft ist
        damit **strukturell** garantiert und nicht davon abhaengig, dass die
        `broker`-Spalte korrekt gefuellt wurde. Der Vergleich braucht dann
        aber beide Dateien; ATTACH holt sie in EINE Abfrage. Die Spalte bleibt
        trotzdem noetig: Sie macht jede Zeile selbstbeschreibend, auch wenn
        Dateien kopiert oder spaeter zusammengelegt werden.

        ⛔ NICHT LESBARE PEER-DATENBANKEN WERDEN GEMELDET, NICHT UEBERSPRUNGEN.
        Ein stiller Skip waere hier der schlimmstmoegliche Fehler: Das Ergebnis
        waere „keine Konflikte" — also genau die Antwort, die man hoeren will,
        obwohl gar nicht gemessen wurde. (Top-Regel 6e)
        """
        quellen = [self.pfad] + [p for p in peer_pfade if Path(p) != self.pfad]
        # uri=True ist PFLICHT: ohne sie liest SQLite "file:...?mode=ro" als
        # normalen Dateinamen, jedes ATTACH scheitert — und das Ergebnis waere
        # `messbar: False`. Immerhin laut, nicht still. Am Ist gesehen.
        con = sqlite3.connect(":memory:", uri=True)
        gelesen, nicht_lesbar = [], []
        for i, pfad in enumerate(quellen):
            alias = f"q{i}"
            try:
                con.execute("ATTACH DATABASE ? AS " + alias,
                            (f"file:{Path(pfad)}?mode=ro", ))
                con.execute(f"SELECT 1 FROM {alias}.messages LIMIT 1")
                gelesen.append((alias, str(pfad)))
            except sqlite3.Error as e:
                nicht_lesbar.append({"datei": str(pfad), "grund": str(e)})

        if not gelesen:
            con.close()
            return {"messbar": False, "konflikte": [],
                    "hinweis": "KEINE Datenbank lesbar — dies ist KEIN 'keine Konflikte'.",
                    "nicht_lesbar": nicht_lesbar}

        wo, args = "", []
        if seit_stunden is not None:
            wo = " WHERE ts >= ?"
            args = [time.time() - seit_stunden * 3600] * len(gelesen)
        union = " UNION ALL ".join(
            f"SELECT topic, COALESCE(broker,'?') b, ts FROM {a}.messages{wo}"
            for a, _ in gelesen)
        # ⛔ '?' (= Altbestand ohne Herkunft) zaehlt NICHT als eigene Quelle.
        #    Der Test hat den Denkfehler aufgedeckt: Sonst haette der erste
        #    Lauf nach der Umstellung **jedes** Topic als Konflikt gemeldet —
        #    Altzeilen '?' plus neue Zeilen 'host:port' sind formal zwei
        #    Quellen, inhaltlich aber nur "vorher unbekannt, jetzt bekannt".
        #    Hunderte Pseudo-Konflikte, und nach dem dritten liest niemand
        #    mehr hin: exakt die withings-Falle. Der NULL-Anteil wird
        #    stattdessen als eigenes Feld ausgewiesen (siehe unten) — sichtbar,
        #    aber nicht als Alarm getarnt.
        sql = (f"WITH alle AS ({union})"
               " SELECT topic, COUNT(DISTINCT b), GROUP_CONCAT(DISTINCT b),"
               "        COUNT(*), MAX(ts)"
               " FROM alle WHERE b <> '?' GROUP BY topic"
               " HAVING COUNT(DISTINCT b) > 1"
               " ORDER BY MAX(ts) DESC LIMIT ?")
        rows = con.execute(sql, args + [max(1, limit)]).fetchall()

        # Fuer jeden Konflikt aufschluesseln, wer wie oft und wie zuletzt.
        konflikte = []
        for topic, n_b, broker_liste, n, letzte in rows:
            detail_sql = (f"WITH alle AS ({union})"
                          " SELECT b, COUNT(*), MAX(ts) FROM alle"
                          " WHERE topic = ? AND b <> '?'"
                          " GROUP BY b ORDER BY MAX(ts) DESC")
            je = con.execute(detail_sql, args + [topic]).fetchall()
            konflikte.append({
                "topic": topic,
                "broker_anzahl": n_b,
                "broker": broker_liste,
                "zuletzt": _iso(letzte),
                "je_broker": [{"broker": b, "nachrichten": c, "zuletzt": _iso(t)}
                              for b, c, t in je],
            })
        gesamt_topics = con.execute(
            f"WITH alle AS ({union}) SELECT COUNT(DISTINCT topic) FROM alle", args).fetchone()[0]
        # ⛔ ALTBESTAND OHNE HERKUNFT IST DIE ZWEITE FALLE (und die subtilere).
        #    Zeilen von vor dem 22.08. haben `broker IS NULL` und werden zu
        #    EINER Pseudo-Quelle '?' zusammengefasst. Ein Topic, das damals von
        #    beiden Brokern bespielt wurde, sieht damit aus wie eines mit genau
        #    einer Quelle — der Canary meldet nichts, und das liest sich als
        #    Entwarnung. Es ist aber keine: es ist "nicht messbar".
        #    Deshalb wird der NULL-Anteil ausgewiesen, nicht weggerechnet.
        ohne, alle_n = con.execute(
            f"WITH alle AS ({union}) SELECT SUM(b = '?'), COUNT(*) FROM alle", args).fetchone()
        con.close()

        ergebnis = {
            "messbar": True,
            "gelesene_datenbanken": [d for _, d in gelesen],
            "topics_geprueft": gesamt_topics,
            "konflikte": konflikte,
        }
        if ohne:
            anteil = round(100 * ohne / max(1, alle_n))
            ergebnis["zeilen_ohne_herkunft"] = {"anzahl": ohne, "anteil_prozent": anteil}
            if anteil >= 50:
                ergebnis["messbar"] = "teilweise"
                ergebnis["warnung"] = (
                    f"{anteil} % der Zeilen stammen aus der Zeit vor der Broker-Kennung "
                    "und zaehlen als EINE Quelle. Ein Konflikt darin ist unsichtbar — "
                    "'keine Konflikte' ist fuer diesen Zeitraum NICHT belegt. "
                    "Belastbar wird die Auskunft erst fuer Daten, die nach der "
                    "Umstellung entstanden sind (seit_stunden einschraenken).")
        if nicht_lesbar:
            # Teilmessung ist KEINE Messung — das muss im Ergebnis stehen,
            # nicht nur im Log.
            ergebnis["messbar"] = "teilweise"
            ergebnis["nicht_lesbar"] = nicht_lesbar
            ergebnis["warnung"] = ("Mindestens eine Datenbank war nicht lesbar. "
                                   "'Keine Konflikte' ist damit NICHT belegt.")
        return ergebnis

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

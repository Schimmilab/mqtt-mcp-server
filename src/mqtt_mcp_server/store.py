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
from collections import Counter
from pathlib import Path

# Einordnungen, die KEINE Handlung verlangen — sie werden gezaehlt, nicht gelistet.
_ENTWARNT = {"gespiegelt", "wahrscheinlich_gespiegelt", "kaum_ueberlappung"}

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
                         limit: int = 200, nur_verdaechtige: bool = True,
                         stichprobe: int = 40) -> dict:
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

        ⛔ EINE BRIDGE IST KEIN KONFLIKT (ergaenzt 2026-08-22, direkt nach dem
        ersten echten Lauf). Der meldete **129 von 147 Topics** — nicht wegen
        Doppelsteuerung, sondern weil die Bridge zwischen altem und neuem
        Broker genau das tut, was sie soll: spiegeln. „Zwei Broker fuehren
        dasselbe Topic" ist im Parallelbetrieb *mit Bridge* der NORMALZUSTAND.
        🎯 Ein Werkzeug, das 88 % meldet, wird nach dem dritten Mal ignoriert —
        exakt die Falle, gegen die dieses Werkzeug gebaut wurde. Deshalb wird
        jedes Doppel-Topic eingeordnet und Gespiegeltes nur noch GEZAEHLT,
        nicht gelistet (`nur_verdaechtige=True`, Default).
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

        # Zweiter UNION mit Payload — nur fuer die Spiegelungspruefung, damit
        # die (haeufigere) Konfliktsuche oben die Blobs nicht mitschleppt.
        union_p = " UNION ALL ".join(
            f"SELECT topic, COALESCE(broker,'?') b, ts, payload FROM {a}.messages"
            f" WHERE aus_backlog = 0" + (" AND ts >= ?" if seit_stunden is not None else "")
            for a, _ in gelesen)

        # Fuer jeden Konflikt aufschluesseln, wer wie oft und wie zuletzt.
        konflikte, gespiegelt, verteilung = [], 0, Counter()
        for topic, n_b, broker_liste, n, letzte in rows:
            detail_sql = (f"WITH alle AS ({union})"
                          " SELECT b, COUNT(*), MAX(ts) FROM alle"
                          " WHERE topic = ? AND b <> '?'"
                          " GROUP BY b ORDER BY COUNT(*) DESC")
            je = con.execute(detail_sql, args + [topic]).fetchall()
            eintrag = {
                "topic": topic,
                "broker_anzahl": n_b,
                "broker": broker_liste,
                "zuletzt": _iso(letzte),
                "je_broker": [{"broker": b, "nachrichten": c, "zuletzt": _iso(t)}
                              for b, c, t in je],
            }
            eintrag.update(self._einordnen(con, union_p, args, topic,
                                           [b for b, _, _ in je], stichprobe))
            verteilung[eintrag["einordnung"]] += 1
            # ⭐ Gelistet wird, was NICHT entwarnt ist. Alle drei Entwarnungen
            #    zusammen sind der Grund, warum das Werkzeug ueberhaupt lesbar
            #    bleibt: Beim ersten echten Lauf standen 129 Zeilen da, davon
            #    keine einzige mit Handlungsbedarf.
            if nur_verdaechtige and eintrag["einordnung"] in _ENTWARNT:
                gespiegelt += 1
                continue
            konflikte.append(eintrag)
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
        ergebnis["einordnungen"] = dict(verteilung.most_common())
        # ⭐ „Unklar aus Datenmangel" ist etwas anderes als „unklar trotz Daten".
        #    Frisch getrennte Datenbanken haben zwangslaeufig viele Topics, die
        #    ausser dem Startschwall noch nichts gesehen haben — das als Befund
        #    zu lesen waere falsch, und es loest sich mit Laufzeit von selbst.
        duenn = sum(1 for k in konflikte
                    if k["einordnung"] == "unklar"
                    and ("Stichprobe" in k.get("grund", "")
                         or "Startschwall" in k.get("grund", "")
                         or "zu duenn" in k.get("grund", "")))
        if duenn:
            ergebnis["unklar_wegen_datenmangel"] = duenn
            ergebnis["hinweis"] = (f"{duenn} der gelisteten Topics sind nur deshalb unklar, weil "
                                   "noch zu wenige Nachrichten vorliegen — nicht, weil etwas "
                                   "auffaellig waere. Das klaert sich mit Laufzeit von selbst.")
        if gespiegelt:
            # ⭐ Gezaehlt statt gelistet — dieselbe Loesung wie bei den lauten
            #    Capture-Typen: Das Signal bleibt sichtbar, das Rauschen wird
            #    zu einer Zeile. Mit nur_verdaechtige=False sind sie wieder da.
            ergebnis["entwarnt_nicht_gelistet"] = gespiegelt
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

    # Grenzwerte der Einordnung. Am Ist kalibriert (2026-08-22, 12 Topics ueber
    # beide Broker): Spiegelung liegt bei 95-100 % Paarquote und 19-66 ms
    # Versatz — die Luecke zu allem anderen ist gross, die Schwellen liegen
    # bewusst mittendrin und nicht knapp am Messwert.
    _SPIEGEL_QUOTE = 0.9      # ab hier gilt Payload-Gleichheit als Beleg
    _SPIEGEL_MS = 250         # Versatz, unterhalb dessen eine Bridge plausibel ist
    _VIELFALT_MIN = 0.5       # verschiedene Payloads / Stichprobe

    def _einordnen(self, con, union_p: str, args: list, topic: str,
                   broker: list[str], stichprobe: int) -> dict:
        """Ist das Doppel eine BRIDGE-Spiegelung oder schreiben zwei unabhaengig?

        Verfahren: Fuer jede Nachricht des einen Brokers den ZEITLICH
        NAECHSTEN Partner des anderen suchen und pruefen, ob der Payload
        identisch ist.

        ⛔ „Zeitlich naechster Partner je Nachricht" ist nicht dasselbe wie
        „alle Paare im Zeitfenster". Der erste Anlauf am 22.08. nahm alle —
        ein Kreuzprodukt, das bei hochfrequenten Topics sowohl die Quote als
        auch den Versatz verfaelschte (17 % / 2.327 ms statt real ~100 % /
        ~40 ms). Die Zahl sah plausibel aus und war Unsinn.

        ⭐ PAYLOAD-VIELFALT ENTSCHEIDET, OB DIE QUOTE UEBERHAUPT ETWAS SAGT.
        Bei `tele/.../SENSOR` sind 640 von 640 Payloads verschieden — eine
        Uebereinstimmung ist dort ein starker Beleg. Bei `cmnd/.../POWER`
        gibt es vier Werte (ON/OFF/0/1), da trifft man zufaellig. Genau
        deshalb kam dieses Topic auf 62 %, obwohl der Versatz mit 17 ms
        eindeutig nach Bridge aussah. **Bei geringer Vielfalt wird deshalb
        NICHT „unabhaengig" behauptet, sondern „unklar" gemeldet** — eine
        ehrliche Nichtauskunft statt eines erfundenen Befundes.
        """
        if len(broker) != 2:
            return {"einordnung": "unklar",
                    "grund": f"{len(broker)} Quellen — die Paarpruefung deckt genau zwei ab"}
        a, b = broker
        hole = (f"WITH alle AS ({union_p})"
                " SELECT ts, payload FROM alle WHERE topic = ? AND b = ?"
                " ORDER BY ts DESC LIMIT ?")
        links = con.execute(hole, args + [topic, a, max(5, stichprobe)]).fetchall()
        if not links:
            return {"einordnung": "unklar", "grund": "keine Nachrichten ausserhalb des Startschwalls"}

        partner = (f"WITH alle AS ({union_p})"
                   " SELECT payload, ABS(ts - ?) d FROM alle"
                   " WHERE topic = ? AND b = ? AND ABS(ts - ?) <= 10"
                   " ORDER BY d LIMIT 1")
        treffer, abstaende = 0, []
        for ts, payload in links:
            r = con.execute(partner, args + [ts, topic, b, ts]).fetchone()
            if r is None:
                continue
            abstaende.append(r[1])
            if r[0] == payload:
                treffer += 1
        n = len(links)
        # ⛔ KEIN PARTNER IST NICHT DASSELBE WIE FALSCHER PAYLOAD (Fund im
        #    zweiten Realtest, 22.08.). Erste Fassung teilte die Treffer durch
        #    ALLE Nachrichten — fand sich zu keiner ein Gegenstueck, kam
        #    `quote = 0` heraus und der Code meldete „nur 0 % identische
        #    Payloads … sieht nach zwei Schreibern aus". **Gemessen wurde
        #    aber gar keine Abweichung, sondern Abwesenheit.** Real betraf das
        #    die bedjet-Topics: Sie liefen praktisch nur auf einem Broker.
        # 🎯 Zwei Broker, die dasselbe Topic fuehren, aber NIE gleichzeitig,
        #    sind ein Umzug — keine Doppelsteuerung. Das ist fuer die
        #    Ownership-Frage die Entwarnung, nicht der Alarm.
        gefunden = len(abstaende)
        quote = treffer / gefunden if gefunden else 0.0
        abstaende.sort()
        median_ms = round(abstaende[len(abstaende)//2] * 1000) if abstaende else None
        vielfalt = len({p for _, p in links}) / n

        d = {"paar_quote": round(quote, 2),
             "versatz_ms": median_ms,
             "payload_vielfalt": round(vielfalt, 2),
             "stichprobe": n,
             "mit_partner": gefunden}

        if gefunden / n < 0.5:
            d["einordnung"] = "kaum_ueberlappung"
            d["grund"] = (f"nur {gefunden} von {n} Nachrichten haben ueberhaupt ein Gegenstueck "
                          "beim anderen Broker im 10-s-Fenster — die beiden senden nicht "
                          "gleichzeitig. Das sieht nach Umzug aus, nicht nach Doppelsteuerung.")
            return d
        # ⛔ KORREKTUR AM ERSTEN REALTEST (22.08.): Die Vielfalt greift an der
        #    FALSCHEN Stelle, wenn man sie vorschaltet. Erste Fassung stufte
        #    alles mit wenigen verschiedenen Payloads auf "wahrscheinlich"
        #    zurueck — und ordnete damit 104 Topics ein, die bei einer Quote
        #    von **1,0** standen. Das Rauschen war nur umbenannt, nicht weg.
        # 🎯 Der Denkfehler: Eine geringe Vielfalt macht eine MITTLERE Quote
        #    unbrauchbar (bei vier Werten trifft man in 25 % der Faelle
        #    zufaellig) — eine HOHE Quote dagegen nicht. 40 Treffer in Folge
        #    bei vier moeglichen Werten sind 0,25^40; das ist kein Zufall,
        #    egal wie einfoermig die Payloads sind. Die Vielfalt entscheidet
        #    also erst DANN, wenn die Quote allein nicht reicht.
        if quote >= self._SPIEGEL_QUOTE and gefunden >= 20:
            d["einordnung"] = "gespiegelt"
            d["grund"] = (f"{round(quote*100)} % identische Payloads beim naechsten Partner "
                          f"ueber {gefunden} Paare"
                          + (f", Versatz {median_ms} ms" if median_ms is not None else ""))
        elif quote >= self._SPIEGEL_QUOTE:
            # Hohe Quote, aber duenne Stichprobe — nicht als Beleg verkaufen.
            d["einordnung"] = "wahrscheinlich_gespiegelt"
            d["grund"] = (f"{round(quote*100)} % identische Payloads, aber nur {gefunden} Paare "
                          "in der Stichprobe — zu wenig fuer einen Beleg")
        elif vielfalt < self._VIELFALT_MIN:
            # Mittlere Quote UND einfoermige Payloads: hier liegt der
            # Zufallsbereich, hier wird nichts behauptet.
            zufall = round(100 / max(1, round(vielfalt * n)))
            d["einordnung"] = "unklar"
            d["grund"] = (f"Quote {round(quote*100)} % bei nur {round(vielfalt*n)} verschiedenen "
                          f"Payloads — Zufallstreffer liegen bei ~{zufall} %. Weder Bridge noch "
                          "zwei Schreiber belegt, von Hand ansehen.")
        elif gefunden >= 20:
            d["einordnung"] = "unabhaengig"
            d["grund"] = (f"nur {round(quote*100)} % identische Payloads bei hoher Vielfalt "
                          f"({round(vielfalt*100)} %) ueber {gefunden} Paare — "
                          "sieht nach zwei Schreibern aus")
        else:
            # ⛔ SYMMETRIE, aufgefallen im dritten Realtest: Die Mindest-
            #    Stichprobe galt nur fuer "gespiegelt". Der EINZIGE gemeldete
            #    Verdachtsfall stand danach auf 6 Paaren — ein Alarm aus sechs
            #    Datenpunkten, und zwar im einzigen Feld, das ueberhaupt Alarm
            #    ausloest. Ein Fehlalarm ist dort am teuersten.
            d["einordnung"] = "unklar"
            d["grund"] = (f"Quote {round(quote*100)} % spraeche fuer zwei Schreiber, aber nur "
                          f"{gefunden} Paare in der Stichprobe — zu duenn fuer einen Alarm.")
        return d

    def stats(self) -> dict:
        with self._lock:
            n = self._db.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
            t = self._db.execute("SELECT COUNT(DISTINCT topic) FROM messages").fetchone()[0]
            spanne = self._db.execute("SELECT MIN(ts), MAX(ts) FROM messages").fetchone()
        return {"nachrichten": n, "topics": t,
                "aeltestes": _iso(spanne[0]) if spanne[0] else None,
                "neuestes": _iso(spanne[1]) if spanne[1] else None,
                "db_mb": round(self.db_groesse_mb(), 1)}

    def neueste_ts(self) -> float | None:
        """Zeitstempel der juengsten Nachricht — Frischemass fuer den Nur-Lesen-Modus."""
        with self._lock:
            return self._db.execute("SELECT MAX(ts) FROM messages").fetchone()[0]

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

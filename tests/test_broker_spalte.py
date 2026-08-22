"""Herkunft der Nachrichten — `broker`-Spalte und der Ownership-Canary.

Hintergrund (2026-08-22): Im IPS-Parallelbetrieb laufen alter und neuer
MQTT-Broker nebeneinander, und dieselben Topics existieren auf beiden. Ohne
Herkunft ist ein Wert dann nicht falsch, sondern **unzuordenbar** — die
unangenehmere Sorte Fehler, weil er wie ein Messwert aussieht.
"""
from __future__ import annotations

import sqlite3

from mqtt_mcp_server.store import Store


def _fuelle(pfad, broker, topics, ts0=1000.0):
    st = Store(pfad, broker=broker)
    for i, t in enumerate(topics):
        st.add_message(t, b"x", 0, False, False, ts=ts0 + i)
    return st


def test_broker_wird_geschrieben_und_zurueckgeliefert(tmp_path):
    st = _fuelle(tmp_path / "a.db", "leitstand:1883", ["haus/temp"])
    assert st.get_last("haus/temp")["broker"] == "leitstand:1883"
    assert st.list_topics()[0]["broker"] == "leitstand:1883"
    assert st.get_history("haus/temp")[0]["broker"] == "leitstand:1883"
    st.close()


def test_migration_laesst_altbestand_NULL(tmp_path):
    """⛔ Alte Zeilen duerfen NICHT nachtraeglich etikettiert werden.

    Der Altbestand IST unzuordenbar (er entstand, als beide Broker in dieselbe
    Datei schrieben). Ihm den aktuellen Broker nachzutragen waere eine
    erfundene Herkunft — genau das, was die Spalte verhindern soll.
    """
    pfad = tmp_path / "alt.db"
    con = sqlite3.connect(pfad)          # Schema OHNE broker-Spalte, wie vor dem 22.08.
    con.executescript(
        "CREATE TABLE messages (id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL NOT NULL,"
        " topic TEXT NOT NULL, payload BLOB, qos INTEGER NOT NULL DEFAULT 0,"
        " retained INTEGER NOT NULL DEFAULT 0, aus_backlog INTEGER NOT NULL DEFAULT 0);"
        "CREATE TABLE connection_events (id INTEGER PRIMARY KEY AUTOINCREMENT,"
        " ts REAL NOT NULL, event TEXT NOT NULL, detail TEXT);"
        "INSERT INTO messages (ts, topic, payload) VALUES (1.0, 'alt/wert', CAST('y' AS BLOB));")
    con.commit(); con.close()

    st = Store(pfad, broker="neu:1883")            # Migration laeuft im Konstruktor
    assert st.get_last("alt/wert")["broker"] is None       # Altbestand bleibt unzuordenbar
    st.add_message("neu/wert", b"z", 0, False, False)
    assert st.get_last("neu/wert")["broker"] == "neu:1883"  # Neues wird etikettiert
    st.close()


def test_konflikt_wird_gefunden_und_saubere_trennung_nicht(tmp_path):
    """Positiv- UND Negativkontrolle in einem: der Canary muss BEIDES koennen."""
    a = _fuelle(tmp_path / "a.db", "alt:1883", ["gemeinsam/topic", "nur/alt"])
    b = _fuelle(tmp_path / "b.db", "neu:1883", ["gemeinsam/topic", "nur/neu"])

    # nur_verdaechtige=False: die Einordnung wuerde dieses Doppel sonst als
    # "gespiegelt" wegfiltern — beide Seiten tragen denselben Payload.
    e = a.broker_konflikte([tmp_path / "b.db"], nur_verdaechtige=False)
    assert e["messbar"] is True
    # POSITIVKONTROLLE: das doppelt bespielte Topic MUSS auftauchen.
    assert [k["topic"] for k in e["konflikte"]] == ["gemeinsam/topic"]
    assert {j["broker"] for j in e["konflikte"][0]["je_broker"]} == {"alt:1883", "neu:1883"}
    # NEGATIVKONTROLLE: ohne sie waere der Test auch dann gruen, wenn der
    # Canary schlicht ALLE Topics meldet.
    assert "nur/alt" not in [k["topic"] for k in e["konflikte"]]
    assert e["topics_geprueft"] == 3
    a.close(); b.close()


def test_leere_konfliktliste_ohne_peer_ist_kein_beweis(tmp_path):
    """Ein Broker allein kann per Definition keinen Konflikt haben — das darf
    nicht als 'geprueft und sauber' durchgehen."""
    a = _fuelle(tmp_path / "a.db", "alt:1883", ["gemeinsam/topic"])
    e = a.broker_konflikte([])
    assert e["konflikte"] == []
    assert e["gelesene_datenbanken"] == [str(tmp_path / "a.db")]
    a.close()


def test_nicht_lesbare_peer_db_macht_das_ergebnis_UNBELASTBAR(tmp_path):
    """⛔ DIE WICHTIGSTE KONTROLLE (Top-Regel 6e).

    Ein stiller Skip waere hier der schlimmstmoegliche Fehler: Das Ergebnis
    waere eine leere Konfliktliste — also genau die Antwort, die man hoeren
    will — obwohl die Haelfte der Daten nie gelesen wurde. Im Parallelbetrieb
    haette das die Freigabe zum Umzug erteilt.
    """
    a = _fuelle(tmp_path / "a.db", "alt:1883", ["gemeinsam/topic"])
    kaputt = tmp_path / "kaputt.db"
    kaputt.write_bytes(b"das ist keine sqlite-datenbank")

    e = a.broker_konflikte([kaputt])
    assert e["konflikte"] == []              # leer — aber eben NICHT "sauber"
    assert e["messbar"] == "teilweise"       # ...und das steht im Ergebnis
    assert e["nicht_lesbar"][0]["datei"] == str(kaputt)
    assert "NICHT belegt" in e["warnung"]
    a.close()


def test_altbestand_ohne_herkunft_macht_das_ergebnis_UNBELASTBAR(tmp_path):
    """⛔ Die subtilere Falle: Zeilen von vor der Umstellung.

    `broker IS NULL` wird zu EINER Pseudo-Quelle '?' zusammengefasst. Ein
    Topic, das damals von beiden Brokern bespielt wurde, sieht damit aus wie
    eines mit genau einer Quelle — der Canary meldet nichts, und das liest
    sich als Entwarnung. Genau der Fall, der beim ersten Realeinsatz eintritt:
    die vorhandene `history.db` hat 894.000 Zeilen ohne Herkunft.
    """
    pfad = tmp_path / "gemischt.db"
    st = Store(pfad, broker=None)                    # wie vor dem 22.08.
    for i in range(9):
        st.add_message("gemeinsam/topic", b"x", 0, False, False, ts=1000.0 + i)

    e = st.broker_konflikte([])
    assert e["konflikte"] == []                      # leer...
    assert e["messbar"] == "teilweise"               # ...aber ausdruecklich nicht "sauber"
    assert e["zeilen_ohne_herkunft"] == {"anzahl": 9, "anteil_prozent": 100}
    assert "NICHT belegt" in e["warnung"]
    st.close()


def test_altzeilen_erzeugen_keine_pseudo_konflikte(tmp_path):
    """⛔ Regression aus dem Bau selbst: '?' darf keine eigene Quelle sein.

    Erste Fassung zaehlte den Altbestand als Quelle — damit waere nach der
    Umstellung **jedes** Topic ein "Konflikt" gewesen (Altzeilen + neue
    Zeilen), inhaltlich aber nur "vorher unbekannt, jetzt bekannt". Hunderte
    Fehlalarme, und nach dem dritten liest niemand mehr hin.
    """
    pfad = tmp_path / "gemischt.db"
    st = Store(pfad, broker=None)
    st.add_message("ein/topic", b"x", 0, False, False, ts=1000.0)   # Altbestand
    st.broker = "neu:1883"
    st.add_message("ein/topic", b"x", 0, False, False, ts=2000.0)   # danach
    e = st.broker_konflikte([])
    assert e["konflikte"] == []          # KEIN Pseudo-Konflikt
    assert e["zeilen_ohne_herkunft"]["anzahl"] == 1   # aber sichtbar
    st.close()


def test_reiner_neubestand_ist_belastbar(tmp_path):
    """NEGATIVKONTROLLE zum Test darueber: ohne Altbestand darf NICHT gewarnt
    werden — sonst waere die Warnung Dauerrauschen und niemand liest sie."""
    a = _fuelle(tmp_path / "a.db", "alt:1883", ["nur/alt"])
    b = _fuelle(tmp_path / "b.db", "neu:1883", ["nur/neu"])
    e = a.broker_konflikte([tmp_path / "b.db"])
    assert e["messbar"] is True
    assert "warnung" not in e
    assert "zeilen_ohne_herkunft" not in e
    a.close(); b.close()


# ---------------------------------------------------------------- Einordnung

def _zwei_broker(tmp_path, payloads_a, payloads_b, versatz=0.05):
    a = Store(tmp_path / "a.db", broker="alt:1883")
    b = Store(tmp_path / "b.db", broker="neu:1883")
    for i, p in enumerate(payloads_a):
        a.add_message("t/x", p, 0, False, False, ts=1000.0 + i)
    for i, p in enumerate(payloads_b):
        b.add_message("t/x", p, 0, False, False, ts=1000.0 + i + versatz)
    return a, b


def test_bridge_wird_als_gespiegelt_erkannt_und_nur_gezaehlt(tmp_path):
    """⛔ Der Grund fuer die ganze Einordnung: Beim ersten echten Lauf meldete
    der Canary 129 von 147 Topics — alles die Bridge. Ein Werkzeug, das 88 %
    meldet, wird nach dem dritten Mal ignoriert.
    """
    werte = [f'{{"v":{i}}}'.encode() for i in range(20)]   # jeder Payload einzigartig
    a, b = _zwei_broker(tmp_path, werte, werte)

    e = a.broker_konflikte([tmp_path / "b.db"])
    assert e["konflikte"] == []                    # nicht gelistet...
    assert e["entwarnt_nicht_gelistet"] == 1      # ...sondern gezaehlt
    assert e["einordnungen"] == {"gespiegelt": 1}

    # NEGATIVKONTROLLE zur Filterung: mit nur_verdaechtige=False ist es wieder da,
    # sonst koennte der Filter auch schlicht alles verschlucken.
    e2 = a.broker_konflikte([tmp_path / "b.db"], nur_verdaechtige=False)
    assert e2["konflikte"][0]["einordnung"] == "gespiegelt"
    assert e2["konflikte"][0]["paar_quote"] == 1.0
    a.close(); b.close()


def test_zwei_unabhaengige_schreiber_bleiben_sichtbar(tmp_path):
    """POSITIVKONTROLLE fuer den echten Befund — der darf NIE weggefiltert werden."""
    a_werte = [f'{{"v":{i}}}'.encode() for i in range(20)]
    b_werte = [f'{{"v":{i + 500}}}'.encode() for i in range(20)]   # voellig andere Werte
    a, b = _zwei_broker(tmp_path, a_werte, b_werte)

    e = a.broker_konflikte([tmp_path / "b.db"])
    assert len(e["konflikte"]) == 1
    assert e["konflikte"][0]["einordnung"] == "unabhaengig"
    assert "entwarnt_nicht_gelistet" not in e
    a.close(); b.close()


def test_einfoermige_payloads_ergeben_UNKLAR_statt_eines_befundes(tmp_path):
    """⭐ Die Ehrlichkeitsregel: Bei ON/OFF trifft man zufaellig.

    Am Ist aufgefallen an `cmnd/tasmota_83458B/POWER` — 4 verschiedene Werte,
    Paarquote 62 %, Versatz aber 17 ms. Die Quote allein haette dort
    "unabhaengig" behauptet, also einen Befund erfunden.
    """
    an_aus = [b"ON", b"OFF"] * 10
    gedreht = [b"OFF", b"ON"] * 10          # gleiche Vielfalt, andere Reihenfolge
    a, b = _zwei_broker(tmp_path, an_aus, gedreht, versatz=0.02)

    e = a.broker_konflikte([tmp_path / "b.db"], nur_verdaechtige=False)
    k = e["konflikte"][0]
    assert k["payload_vielfalt"] < 0.5
    # ⛔ Weder "gespiegelt" noch "unabhaengig" — beides waere erfunden.
    assert k["einordnung"] == "unklar"
    assert "Zufallstreffer" in k["grund"]
    a.close(); b.close()


def test_einfoermig_UND_weit_auseinander_ist_unklar(tmp_path):
    """NEGATIVKONTROLLE zum Test darueber: ohne kleinen Versatz darf auch
    nicht 'wahrscheinlich gespiegelt' behauptet werden."""
    # ⚠️ Der Versatz muss ZWISCHEN die Sendezeitpunkte fallen. Erster Anlauf
    #    nahm 4,0 s bei 1-s-Raster — dann ist der naechste Partner wieder eine
    #    andere Nachricht im Abstand 0, und der Test misst nichts. 0,5 s liegt
    #    dagegen echt zwischen zwei Rasterpunkten.
    an_aus = [b"ON", b"OFF"] * 10
    a, b = _zwei_broker(tmp_path, an_aus, an_aus, versatz=0.5)
    e = a.broker_konflikte([tmp_path / "b.db"], nur_verdaechtige=False)
    k = e["konflikte"][0]
    assert k["einordnung"] == "unklar"
    assert "von Hand ansehen" in k["grund"]
    a.close(); b.close()


def test_hohe_quote_schlaegt_geringe_vielfalt(tmp_path):
    """⛔ Korrektur am ersten Realtest (22.08.) — der wichtigste Grenzfall.

    Erste Fassung schaltete die Vielfaltspruefung VOR die Quote und stufte
    damit 104 Topics auf "wahrscheinlich" zurueck, die bei einer Quote von
    **1,0** standen. Das Rauschen war nur umbenannt.

    Der Denkfehler: Geringe Vielfalt macht eine MITTLERE Quote unbrauchbar,
    eine HOHE nicht. 40 Treffer in Folge bei zwei moeglichen Werten sind
    0,5^40 — das ist kein Zufall, egal wie einfoermig die Payloads sind.
    """
    an_aus = [b"ON", b"OFF"] * 15                # nur zwei Werte...
    a, b = _zwei_broker(tmp_path, an_aus, an_aus)  # ...aber deckungsgleich
    e = a.broker_konflikte([tmp_path / "b.db"], nur_verdaechtige=False)
    k = e["konflikte"][0]
    assert k["payload_vielfalt"] < 0.5           # einfoermig
    assert k["paar_quote"] == 1.0                # und trotzdem eindeutig
    assert k["einordnung"] == "gespiegelt"
    a.close(); b.close()


def test_duenne_stichprobe_ist_kein_beleg(tmp_path):
    """NEGATIVKONTROLLE dazu: bei wenigen Nachrichten darf die hohe Quote
    nicht als Beleg durchgehen — sonst reichten drei Zufallstreffer."""
    werte = [f'{{"v":{i}}}'.encode() for i in range(6)]
    a, b = _zwei_broker(tmp_path, werte, werte)
    e = a.broker_konflikte([tmp_path / "b.db"], nur_verdaechtige=False)
    k = e["konflikte"][0]
    assert k["paar_quote"] == 1.0
    assert k["einordnung"] == "wahrscheinlich_gespiegelt"
    assert "zu wenig fuer einen Beleg" in k["grund"]
    a.close(); b.close()


def test_kein_partner_ist_nicht_falscher_payload(tmp_path):
    """⛔ Fund im zweiten Realtest (22.08.) — ein erfundener Befund.

    Erste Fassung teilte die Treffer durch ALLE Nachrichten. Fand sich zu
    keiner ein Gegenstueck, kam `quote = 0` heraus und der Code meldete
    "nur 0 % identische Payloads ... sieht nach zwei Schreibern aus".
    Gemessen wurde aber keine Abweichung, sondern **Abwesenheit**.

    Zwei Broker, die dasselbe Topic fuehren, aber NIE gleichzeitig, sind ein
    Umzug — fuer die Ownership-Frage die Entwarnung, nicht der Alarm.
    """
    a = Store(tmp_path / "a.db", broker="alt:1883")
    b = Store(tmp_path / "b.db", broker="neu:1883")
    for i in range(20):                              # alt sendet frueh
        a.add_message("t/x", f"a{i}".encode(), 0, False, False, ts=1000.0 + i)
    for i in range(20):                              # neu erst Stunden spaeter
        b.add_message("t/x", f"b{i}".encode(), 0, False, False, ts=50000.0 + i)

    k = a.broker_konflikte([tmp_path / "b.db"], nur_verdaechtige=False)["konflikte"][0]
    assert k["mit_partner"] == 0
    assert k["einordnung"] == "kaum_ueberlappung"     # NICHT "unabhaengig"
    assert "Umzug" in k["grund"]
    a.close(); b.close()


def test_ueberlappende_unabhaengige_bleiben_unabhaengig(tmp_path):
    """NEGATIVKONTROLLE dazu: wer GLEICHZEITIG mit anderen Werten sendet,
    muss weiterhin als 'unabhaengig' durchkommen — sonst haette der Fix oben
    den echten Befund gleich mit weggeraeumt."""
    a_w = [f"a{i}".encode() for i in range(25)]
    b_w = [f"b{i}".encode() for i in range(25)]
    a, b = _zwei_broker(tmp_path, a_w, b_w)          # gleiches Zeitraster
    k = a.broker_konflikte([tmp_path / "b.db"], nur_verdaechtige=False)["konflikte"][0]
    assert k["mit_partner"] == 25
    assert k["einordnung"] == "unabhaengig"
    a.close(); b.close()

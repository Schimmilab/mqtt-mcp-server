"""Positivkontrolle fuer `broker_konflikte` — erzeugt einen ECHTEN Doppelbesitz.

Aufruf:  .venv/bin/python tools/canary-doppelbesitz.py
Danach:  broker_konflikte(seit_stunden=0.1)

⭐ Der Sinn: Ein Canary, der nichts findet, ist von einem kaputten Canary nicht
   zu unterscheiden. Dieses Skript legt deshalb einen Fall an, der GEFUNDEN
   WERDEN MUSS — und daneben einen, der NICHT gefunden werden darf.

⛔ Bewusst auf einem EIGENEN Testtopic, nicht am Abluftluefter: Der waere ein
   echtes Schalten. Fuer die Frage "erkennt der Canary zwei unabhaengige
   Schreiber?" ist das Geraet irrelevant, der Wirkkanal aber real.

P  (Positivkontrolle)  kios/canary/doppelbesitz — beide Broker, VERSCHIEDENE
                       Werte, gleiches Zeitraster  -> MUSS "unabhaengig"
N  (Negativkontrolle)  kios/canary/gespiegelt     — beide Broker, IDENTISCHE
                       Werte                      -> MUSS entwarnt werden

Ohne N waere P wertlos: ein Canary, der ALLES als "unabhaengig" meldet,
bestuende die Positivkontrolle genauso.

Ergebnis am 2026-08-22, erster Lauf:
  kios/canary/doppelbesitz  -> unabhaengig  (q=0.0, 25/25 Partner,  6 ms) ✅
  kios/canary/gespiegelt    -> gespiegelt   (q=1.0, 25/25 Partner, 53 ms) ✅
  Default-Modus: P gelistet, N entwarnt und nur gezaehlt.

⚠️ Bewusst OHNE retain — es bleibt nichts im Broker haengen. Die Zeilen stehen
   nur in den History-Datenbanken und werden von der Retention abgeraeumt.
⛔ Bewusst auf einem eigenen kios/canary/-Topic und NICHT an einem Geraet:
   Ein Schaltaktor waere ein echtes Schalten, und fuer die Frage "erkennt der
   Canary zwei Schreiber?" ist das Geraet ohnehin irrelevant.
"""
import time, paho.mqtt.client as mqtt

BROKER = [("leitstand.fritz.box", "L"), ("192.168.178.65", "I")]
N = 25

cl = {}
for host, tag in BROKER:
    c = mqtt.Client(client_id=f"kios-canary-{tag}")
    c.connect(host, 1883, 30); c.loop_start(); cl[tag] = c
    print(f"  verbunden: {host}")

print(f"\nP: {N} Nachrichten je Broker, VERSCHIEDENE Werte, gleiches Raster")
print(f"N: {N} Nachrichten je Broker, IDENTISCHE Werte")
for i in range(N):
    # P — jeder Broker sendet seinen eigenen Wert (= zwei Schreiber)
    cl["L"].publish("kios/canary/doppelbesitz", f'{{"quelle":"leitstand","n":{i}}}', qos=0)
    cl["I"].publish("kios/canary/doppelbesitz", f'{{"quelle":"ips65","n":{i}}}', qos=0)
    # N — beide senden denselben Wert (= wie eine Spiegelung)
    gleich = f'{{"wert":{i}}}'
    cl["L"].publish("kios/canary/gespiegelt", gleich, qos=0)
    cl["I"].publish("kios/canary/gespiegelt", gleich, qos=0)
    time.sleep(0.25)

time.sleep(2)
for c in cl.values():
    c.loop_stop(); c.disconnect()
print("\ngesendet, Verbindungen zu.")

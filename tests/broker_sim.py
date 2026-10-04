"""Stoppbarer Test-Broker (amqtt, reines Python) fuer End-to-End-Tests.

WARUM (2026-10-04): Drei Kernfaehigkeiten des Servers liessen sich am Heim-Broker nicht auf
Kommando herstellen — Broker-Anmeldung (der laeuft ohne Auth), Verbindungsabbrueche (get_gaps)
und verstummende Geraete (find_silent). Sie waren nur durch Unit-Tests gedeckt; am 01.08. fiel
der ReasonCode-Bug erst im End-to-End-Test auf. Dieser Broker laeuft im Testprozess, laesst sich
stoppen und auf demselben Port neu starten, optional mit Passwortdatei.

Passwoerter: amqtt prueft mit pwdlib (argon2). sha512_crypt geht auf Python 3.13 nicht mehr
(kein `crypt`-Modul) — der Broker lehnt dann JEDE Anmeldung ab, auch die richtige.
"""
from __future__ import annotations

import json
import socket
import subprocess
import sys
import time
from pathlib import Path

from pwdlib import PasswordHash


def freier_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def passwortdatei(pfad: Path, benutzer: str, passwort: str) -> Path:
    pfad.write_text(f"{benutzer}:{PasswordHash.recommended().hash(passwort)}\n")
    return pfad


_BROKER_CODE = """
import asyncio, json, logging, sys
logging.disable(logging.CRITICAL)
from amqtt.broker import Broker
cfg = json.loads(sys.argv[1])
async def main():
    await Broker(cfg).start()
    await asyncio.Event().wait()
asyncio.run(main())
"""


class TestBroker:
    """Broker als eigener Prozess: stop() beendet ihn hart — wie ein echter Ausfall.

    ⚠️ Im selben Prozess (erster Wurf) hing amqtt beim shutdown(), solange ein Client
    verbunden war; der Test wartete dann auf den Broker statt auf den Sammler.
    """
    __test__ = False                     # kein pytest-Testfall

    def __init__(self, port: int, passwortdatei: Path | None = None) -> None:
        self.port = port
        self.passwortdatei = passwortdatei
        self._proc: subprocess.Popen | None = None

    def start(self) -> "TestBroker":
        cfg: dict = {"listeners": {"default": {"type": "tcp", "bind": f"127.0.0.1:{self.port}"}},
                     "sys_interval": 0}
        if self.passwortdatei:
            cfg["auth"] = {"allow-anonymous": False, "password-file": str(self.passwortdatei),
                           "plugins": ["auth_file"]}
        else:
            cfg["auth"] = {"allow-anonymous": True, "plugins": ["auth_anonymous"]}
        self._proc = subprocess.Popen([sys.executable, "-W", "ignore", "-c", _BROKER_CODE, json.dumps(cfg)],
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        ende = time.time() + 15
        while time.time() < ende:
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=0.2).close()
                return self
            except OSError:
                if self._proc.poll() is not None:
                    raise RuntimeError("Test-Broker ist beim Start abgestuerzt")
                time.sleep(0.1)
        raise RuntimeError("Test-Broker startet nicht")

    def stop(self) -> None:
        if self._proc and self._proc.poll() is None:
            self._proc.kill()
            self._proc.wait(5)

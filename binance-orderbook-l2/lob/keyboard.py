"""Lecture clavier non bloquante, multi-plateforme, sans dépendance.

``KeyReader`` lit les touches et les transmet à un callback sous forme de
séquences normalisées : caractères simples, "\\t", "\\x1b" (Échap), "\\x7f"
(retour arrière), "\\x1b[C"/"\\x1b[D" (flèches). Dégradé gracieux : si le
terminal ne permet pas la lecture brute, la saisie est simplement désactivée.
Ctrl+C n'est jamais intercepté (``tty.setcbreak`` conserve ISIG) : l'arrêt
propre reste géré par le module ``signal``.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
from typing import Callable

log = logging.getLogger(__name__)


class KeyReader:
    def __init__(self, callback: Callable[[str], None]) -> None:
        self._callback = callback

    async def run(self, stop_event: asyncio.Event) -> None:
        try:
            if os.name == "nt":
                await self._run_windows(stop_event)
            else:
                await self._run_unix(stop_event)
        except Exception as exc:  # jamais bloquant pour l'application
            log.debug("clavier indisponible, saisie désactivée : %s", exc)
            await stop_event.wait()

    async def _run_unix(self, stop_event: asyncio.Event) -> None:
        import termios
        import tty

        if not sys.stdin.isatty():
            await stop_event.wait()
            return
        fd = sys.stdin.fileno()
        saved = termios.tcgetattr(fd)
        tty.setcbreak(fd)  # pas de mode canonique ni d'écho ; ISIG conservé
        loop = asyncio.get_running_loop()
        buffer = ""

        def _on_readable() -> None:
            nonlocal buffer
            try:
                data = os.read(fd, 16).decode("utf-8", "ignore")
            except OSError:
                return
            buffer += data
            while buffer:
                if buffer.startswith("\x1b"):
                    if len(buffer) >= 3 and buffer[1] == "[":
                        self._callback(buffer[:3])
                        buffer = buffer[3:]
                    elif len(buffer) == 1:
                        # Échap seul (aucune suite dans cette lecture).
                        self._callback("\x1b")
                        buffer = ""
                    else:
                        buffer = buffer[1:]  # séquence Alt+x : ignorée
                else:
                    self._callback(buffer[0])
                    buffer = buffer[1:]

        loop.add_reader(fd, _on_readable)
        try:
            await stop_event.wait()
        finally:
            loop.remove_reader(fd)
            termios.tcsetattr(fd, termios.TCSADRAIN, saved)

    async def _run_windows(self, stop_event: asyncio.Event) -> None:
        import msvcrt

        arrows = {"M": "\x1b[C", "K": "\x1b[D"}
        while not stop_event.is_set():
            while msvcrt.kbhit():
                ch = msvcrt.getwch()
                if ch in ("\x00", "\xe0"):  # préfixe des touches spéciales
                    seq = arrows.get(msvcrt.getwch())
                    if seq:
                        self._callback(seq)
                elif ch == "\r":
                    self._callback("\n")
                elif ch == "\x08":
                    self._callback("\x7f")
                else:
                    self._callback(ch)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=0.08)
            except asyncio.TimeoutError:
                pass

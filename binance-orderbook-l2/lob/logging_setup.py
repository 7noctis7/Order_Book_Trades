"""Logging structuré : fichier, console, et tampon des alertes pour l'UI.

Quand le mode démo console est actif, les logs ne peuvent pas partir sur
stdout (ils casseraient l'affichage) : ils vont dans le fichier, et les
derniers WARNING/ERROR sont conservés dans un ring buffer affiché par l'UI.
"""
from __future__ import annotations

import logging
from collections import deque


class RingBufferHandler(logging.Handler):
    """Conserve les N derniers messages >= WARNING, formatés, pour l'UI."""

    def __init__(self, capacity: int = 50) -> None:
        super().__init__(level=logging.WARNING)
        self.records: deque[str] = deque(maxlen=capacity)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.records.append(self.format(record))
        except Exception:
            self.handleError(record)


def setup_logging(level: str, file: str | None, console: bool) -> RingBufferHandler:
    root = logging.getLogger()
    root.setLevel(level)

    detailed = logging.Formatter(
        "%(asctime)s %(levelname)-7s %(name)s — %(message)s", "%H:%M:%S"
    )
    compact = logging.Formatter("%(asctime)s %(levelname)s  %(message)s", "%H:%M:%S")

    ring = RingBufferHandler()
    ring.setFormatter(compact)
    root.addHandler(ring)

    if file:
        file_handler = logging.FileHandler(file, encoding="utf-8")
        file_handler.setFormatter(detailed)
        root.addHandler(file_handler)

    if console:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(detailed)
        root.addHandler(stream_handler)

    # Le keepalive de la lib websockets est trop bavard en DEBUG.
    logging.getLogger("websockets").setLevel(logging.WARNING)
    return ring

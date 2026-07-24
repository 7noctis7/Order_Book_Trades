"""Client WebSocket : connexion, mise en file des messages, reconnexion.

Chaque message reçu est horodaté à la réception (mesure de latence), capturé
en brut si la capture est active, puis poussé dans une ``asyncio.Queue``
consommée par le synchroniseur. Les changements d'état de connexion sont
signalés dans la même file via des ``ControlEvent``, ce qui garantit leur
ordre relatif par rapport aux messages de données.

Déconnexion réseau : reconnexion automatique avec backoff exponentiel
plafonné + jitter. Arrêt : coopératif via ``stop_event``, fermeture propre
de la socket par le contexte ``websockets.connect``.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass
from enum import Enum, auto

import websockets

from .capture import SqliteCaptureWriter
from .config import NetworkConfig
from .trades_feed import TradeTape

log = logging.getLogger(__name__)


class ControlType(Enum):
    CONNECTED = auto()
    DISCONNECTED = auto()


@dataclass(frozen=True)
class ControlEvent:
    type: ControlType


@dataclass(frozen=True)
class RawMessage:
    payload: dict
    recv_time_ms: int  # horodatage local à la réception (epoch ms)


QueueItem = ControlEvent | RawMessage


class WSClient:
    def __init__(
        self,
        url: str,
        symbol: str,
        out_queue: "asyncio.Queue[QueueItem]",
        net: NetworkConfig,
        capture: SqliteCaptureWriter | None = None,
        tape: TradeTape | None = None,
        parse_trade=None,
    ) -> None:
        self._url = url
        self._symbol = symbol
        self._log = logging.getLogger(f"ws.{symbol}")
        self._queue = out_queue
        self._net = net
        self._capture = capture
        self._tape = tape
        self._parse_trade = parse_trade

    async def run(self, stop_event: asyncio.Event) -> None:
        attempt = 0
        while not stop_event.is_set():
            try:
                async with websockets.connect(
                    self._url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                    max_queue=None,
                ) as ws:
                    self._log.info("WebSocket connecté : %s", self._url)
                    attempt = 0
                    await self._queue.put(ControlEvent(ControlType.CONNECTED))
                    await self._recv_loop(ws, stop_event)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # ConnectionClosed, OSError, handshake…
                if stop_event.is_set():
                    break
                self._log.warning("WebSocket déconnecté : %s", exc)

            if stop_event.is_set():
                break
            await self._queue.put(ControlEvent(ControlType.DISCONNECTED))
            delay = min(
                self._net.backoff_base_seconds * (2 ** attempt),
                self._net.backoff_max_seconds,
            )
            delay += random.uniform(0, delay * 0.25)  # jitter anti-tempête
            self._log.warning("reconnexion dans %.1f s (tentative %d)", delay, attempt + 1)
            attempt += 1
            await self._wait(stop_event, delay)
        self._log.info("client WebSocket arrêté")

    async def _recv_loop(self, ws, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                # Timeout court : revenir vérifier stop_event régulièrement,
                # pour un arrêt Ctrl+C réactif même sans trafic.
                raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            recv_ms = time.time_ns() // 1_000_000
            text = raw if isinstance(raw, str) else raw.decode("utf-8", "replace")
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                self._log.warning("message non JSON ignoré (%d octets)", len(text))
                continue
            # Stream combiné : les données utiles sont sous la clé "data".
            if "stream" in payload and isinstance(payload.get("data"), dict):
                payload = payload["data"]
            if self._capture is not None:
                # Capture de CHAQUE message brut, avant tout filtrage.
                self._capture.submit(recv_ms, payload.get("E"), self._symbol, text)
            if payload.get("e") == "trade":
                # Transactions réelles : bande dédiée, hors file de profondeur.
                if self._tape is not None and self._parse_trade is not None:
                    parsed = self._parse_trade(payload)
                    if parsed is not None:
                        self._tape.add(*parsed)
                continue
            await self._queue.put(RawMessage(payload=payload, recv_time_ms=recv_ms))

    @staticmethod
    async def _wait(stop_event: asyncio.Event, timeout: float) -> None:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass

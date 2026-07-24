"""Intégration Kraken Spot : flux WebSocket v2 (canaux book + trade).

Deuxième exchange du moteur — et deuxième *style* de protocole :
- Binance : snapshot REST + deltas séquencés U/u (interface ``ExchangeClient``) ;
- Kraken  : snapshot ET deltas sur le WebSocket, intégrité garantie par un
  checksum CRC32 des 10 meilleurs niveaux, envoyé avec chaque mise à jour.

Une seule connexion WebSocket porte toutes les paires (le protocole v2 est
multi-symboles) : ``KrakenFeed`` démultiplexe vers des ``KrakenPairSync``
passifs qui maintiennent chacun leur carnet — même interface de compteurs
que le sync Binance, donc UI, paper trading et export Prometheus inchangés.

Précision : les messages sont parsés avec ``parse_float=Decimal`` pour
conserver la représentation décimale exacte du fil — condition nécessaire au
calcul du checksum (l'algorithme concatène les chiffres tels que transmis).

Rupture d'intégrité (checksum invalide) : politique identique au live
Binance — abandon complet du carnet et réabonnement (nouveau snapshot).

NB : algorithme de checksum conforme à la documentation Kraken v2 ;
non validé contre le flux réel depuis cet environnement sans réseau.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
import zlib
from datetime import datetime
from decimal import Decimal
from functools import partial

import websockets

from .config import NetworkConfig
from .metrics import LatencyTracker, RateTracker
from .orderbook import OrderBook
from .sync import SyncState
from .trades_feed import TradeTape

log = logging.getLogger("kraken")

CHECKSUM_DEPTH = 10  # niveaux par côté entrant dans le CRC32 (spécification)


def _checksum_part(value: Decimal) -> str:
    """Formate un prix/qty pour le checksum : notation fixe, sans point,
    zéros de tête retirés (ex. Decimal('0.05005') → '5005')."""
    text = format(value, "f").replace(".", "").lstrip("0")
    return text


def book_checksum(book: OrderBook) -> int:
    """CRC32 des 10 meilleurs asks (croissants) puis bids (décroissants)."""
    parts: list[str] = []
    for price, qty in book.top_asks(CHECKSUM_DEPTH):
        parts.append(_checksum_part(price) + _checksum_part(qty))
    for price, qty in book.top_bids(CHECKSUM_DEPTH):
        parts.append(_checksum_part(price) + _checksum_part(qty))
    return zlib.crc32("".join(parts).encode("ascii")) & 0xFFFFFFFF


def _decimal(value) -> Decimal:
    """Coercition défensive : le fil v2 envoie des nombres JSON (préservés en
    Decimal par parse_float), mais un producteur en chaîne reste accepté."""
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _ts_ms(iso_timestamp: str | None) -> int | None:
    if not iso_timestamp:
        return None
    try:
        return int(datetime.fromisoformat(iso_timestamp).timestamp() * 1000)
    except ValueError:
        return None


class KrakenPairSync:
    """Carnet + compteurs d'une paire Kraken (interface identique au live)."""

    def __init__(self, symbol: str, book: OrderBook, depth: int) -> None:
        self.symbol = symbol
        self.book = book
        self.depth = depth
        self.state = SyncState.CONNECTING
        self.events_applied = 0
        self.discarded_stale = 0        # sans objet chez Kraken : reste à 0
        self.resync_count = 0
        self.disconnect_count = 0
        self.last_resync_reason: str | None = None
        self.error_reason: str | None = None
        self.latency = LatencyTracker()
        self.rate = RateTracker()
        self._started = time.monotonic()
        self._log = logging.getLogger(f"kraken.{symbol}")

    @property
    def uptime_seconds(self) -> float:
        return time.monotonic() - self._started

    # ------------------------------------------------------------ messages

    def on_snapshot(self, data: dict) -> None:
        self.book.clear()
        self._apply_levels(data)
        self.book.truncate(self.depth)
        expected = data.get("checksum")
        if expected is not None and book_checksum(self.book) != int(expected):
            self._log.warning("checksum du snapshot invalide : resynchronisation")
            self.request_resync("checksum snapshot invalide")
            return
        self.state = SyncState.STREAMING
        self._log.info("snapshot appliqué : %s", self.book.depth())

    def on_update(self, data: dict, recv_ms: int) -> bool:
        """Applique une mise à jour ; retourne False si resync nécessaire."""
        if self.state is not SyncState.STREAMING:
            return True  # en attente du snapshot de réabonnement
        self._apply_levels(data)
        self.book.truncate(self.depth)
        self.events_applied += 1
        event_ms = _ts_ms(data.get("timestamp"))
        if event_ms is not None:
            self.latency.record(event_ms, recv_ms)
        expected = data.get("checksum")
        if expected is not None and book_checksum(self.book) != int(expected):
            self.request_resync("checksum invalide")
            return False
        return True

    def _apply_levels(self, data: dict) -> None:
        for side, key in (("bid", "bids"), ("ask", "asks")):
            for level in data.get(key, ()):
                self.book.set_level(
                    side, _decimal(level["price"]), _decimal(level["qty"])
                )

    def request_resync(self, reason: str) -> None:
        self.resync_count += 1
        self.last_resync_reason = reason
        self.book.clear()
        self.state = SyncState.BUFFERING


class KrakenFeed:
    """Connexion WebSocket unique : abonnements, routage, reconnexion."""

    def __init__(
        self,
        ws_base: str,
        pairs: list,                      # PairView (sync est un KrakenPairSync)
        depth: int,
        net: NetworkConfig,
        capture=None,
    ) -> None:
        self._ws_base = ws_base
        self._pairs = pairs
        self._by_symbol = {pair.symbol: pair for pair in pairs}
        self._depth = depth
        self._net = net
        self._capture = capture
        self._ws = None

    async def run(self, stop_event: asyncio.Event) -> None:
        attempt = 0
        while not stop_event.is_set():
            try:
                async with websockets.connect(
                    self._ws_base, ping_interval=20, ping_timeout=20, close_timeout=5
                ) as ws:
                    self._ws = ws
                    log.info("WebSocket Kraken connecté : %s", self._ws_base)
                    attempt = 0
                    await self._subscribe(ws)
                    await self._recv_loop(ws, stop_event)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if stop_event.is_set():
                    break
                log.warning("WebSocket Kraken déconnecté : %s", exc)
            self._ws = None
            if stop_event.is_set():
                break
            for pair in self._pairs:
                if pair.sync.state is not SyncState.ERROR:
                    pair.sync.disconnect_count += 1
                    pair.sync.state = SyncState.CONNECTING
                    pair.book.clear()
            delay = min(
                self._net.backoff_base_seconds * (2 ** attempt),
                self._net.backoff_max_seconds,
            )
            delay += random.uniform(0, delay * 0.25)
            log.warning("reconnexion Kraken dans %.1f s", delay)
            attempt += 1
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
        log.info("feed Kraken arrêté")

    async def _subscribe(self, ws, symbols: list[str] | None = None) -> None:
        symbols = symbols or [pair.symbol for pair in self._pairs
                              if pair.sync.state is not SyncState.ERROR]
        for channel, params in (
            ("book", {"depth": self._depth}),
            ("trade", {}),
        ):
            await ws.send(json.dumps({
                "method": "subscribe",
                "params": {"channel": channel, "symbol": symbols, **params},
            }))
        for symbol in symbols:
            sync = self._by_symbol[symbol].sync
            if sync.state is not SyncState.ERROR:
                sync.state = SyncState.BUFFERING

    async def _recv_loop(self, ws, stop_event: asyncio.Event) -> None:
        loads = partial(json.loads, parse_float=Decimal)
        while not stop_event.is_set():
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            recv_ms = time.time_ns() // 1_000_000
            text = raw if isinstance(raw, str) else raw.decode("utf-8", "replace")
            try:
                message = loads(text)
            except json.JSONDecodeError:
                continue
            await self._route(ws, message, text, recv_ms)

    async def _route(self, ws, message: dict, text: str, recv_ms: int) -> None:
        # Réponses d'abonnement : détecter les symboles refusés (définitif).
        if message.get("method") == "subscribe" and message.get("success") is False:
            symbol = (message.get("result") or {}).get("symbol")
            pair = self._by_symbol.get(symbol)
            if pair is not None:
                pair.sync.state = SyncState.ERROR
                pair.sync.error_reason = message.get("error", "abonnement refusé")
                log.warning("Kraken a refusé %s : %s", symbol, pair.sync.error_reason)
            return
        channel = message.get("channel")
        if channel not in ("book", "trade"):
            return  # heartbeat, status, acks…
        for data in message.get("data", ()):
            symbol = data.get("symbol")
            pair = self._by_symbol.get(symbol)
            if pair is None:
                continue
            if self._capture is not None:
                self._capture.submit(
                    recv_ms, _ts_ms(data.get("timestamp")), symbol, text
                )
            sync: KrakenPairSync = pair.sync
            if channel == "trade":
                ts = _ts_ms(data.get("timestamp")) or recv_ms
                try:
                    pair.tape.add(
                        ts, data["price"], data["qty"], data.get("side") == "sell"
                    )
                except (KeyError, TypeError):
                    pass
                continue
            sync.rate.tick()
            if message.get("type") == "snapshot":
                sync.on_snapshot(data)
            elif not sync.on_update(data, recv_ms):
                await self._subscribe(ws, [symbol])  # nouveau snapshot

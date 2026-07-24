"""Machine à états de synchronisation snapshot REST ↔ flux WebSocket.

    CONNECTING → BUFFERING → SNAPSHOT_FETCHED → DISCARDING_STALE
    → VALIDATING_FIRST → STREAMING → (rupture / déconnexion) → resync

Protocole appliqué strictement (voir ``sequencing``) : bufferisation des
messages dès la connexion, snapshot REST en parallèle, purge des événements
périmés, validation du premier événement, puis continuité stricte U == u+1.
Toute rupture rejette intégralement le carnet local et relance un cycle
complet (nouveau snapshot + rebufferisation).
"""
from __future__ import annotations

import asyncio
import logging
import time

from enum import Enum

from .capture import SqliteCaptureWriter
from .config import NetworkConfig
from .exchange import ExchangeClient, Snapshot, SnapshotError
from .metrics import LatencyTracker, RateTracker
from .orderbook import OrderBook
from .sequencing import SeqResult, SequenceValidator
from .ws_client import ControlEvent, ControlType, QueueItem, RawMessage

log = logging.getLogger(__name__)


class SyncState(Enum):
    CONNECTING = "CONNECTING"
    BUFFERING = "BUFFERING"
    SNAPSHOT_FETCHED = "SNAPSHOT_FETCHED"
    DISCARDING_STALE = "DISCARDING_STALE"
    VALIDATING_FIRST = "VALIDATING_FIRST"
    STREAMING = "STREAMING"
    ERROR = "ERROR"  # erreur permanente (ex. symbole inconnu) : paire arrêtée


class _Stopped(Exception):
    """Arrêt demandé par l'utilisateur (Ctrl+C)."""


class _Disconnected(Exception):
    """Connexion WebSocket perdue : attendre la reconnexion puis resync."""


class _Resync(Exception):
    """Rupture de séquence : rejeter le carnet et relancer un cycle complet."""


class _Fatal(Exception):
    """Erreur permanente : arrêter cette paire sans impacter les autres."""


class OrderBookSync:
    def __init__(
        self,
        exchange: ExchangeClient,
        book: OrderBook,
        in_queue: "asyncio.Queue[QueueItem]",
        net: NetworkConfig,
        capture: SqliteCaptureWriter | None = None,
    ) -> None:
        self._exchange = exchange
        self._book = book
        self._log = logging.getLogger(f"sync.{exchange.symbol}")
        self._queue = in_queue
        self._net = net
        self._capture = capture

        # État observable (lu par l'UI, même boucle asyncio : pas de lock).
        self.state = SyncState.CONNECTING
        self.events_applied = 0
        self.discarded_stale = 0
        self.resync_count = 0      # ruptures de séquence
        self.disconnect_count = 0  # pertes de connexion WebSocket
        self.last_resync_reason: str | None = None
        self.error_reason: str | None = None
        self.latency = LatencyTracker()
        self.rate = RateTracker()
        self.started_at = time.monotonic()

    # ------------------------------------------------------------- boucle

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            self._set_state(SyncState.CONNECTING)
            self._book.clear()
            try:
                await self._wait_connected(stop_event)
                while True:
                    try:
                        await self._sync_cycle(stop_event)
                    except _Resync as exc:
                        self.resync_count += 1
                        self.last_resync_reason = str(exc)
                        self._book.clear()
                        self._log.warning(
                            "rupture de séquence → resynchronisation complète : %s",
                            exc,
                        )
            except _Disconnected:
                self.disconnect_count += 1
                self._book.clear()
                self._log.warning(
                    "connexion perdue → attente de reconnexion puis resynchronisation"
                )
            except _Fatal as exc:
                self.error_reason = str(exc)
                self.state = SyncState.ERROR
                self._book.clear()
                self._log.error(
                    "erreur permanente — paire arrêtée (les autres continuent) : %s",
                    exc,
                )
                return
            except _Stopped:
                break
        self._log.info(
            "synchroniseur arrêté (%d événements appliqués, %d resyncs, %d déconnexions)",
            self.events_applied,
            self.resync_count,
            self.disconnect_count,
        )

    # ------------------------------------------------------------- phases

    async def _wait_connected(self, stop_event: asyncio.Event) -> None:
        while True:
            if stop_event.is_set():
                raise _Stopped
            item = await self._get(0.5)
            if isinstance(item, ControlEvent) and item.type is ControlType.CONNECTED:
                return
            # RawMessage avant CONNECTED : impossible en pratique, ignoré.

    async def _sync_cycle(self, stop_event: asyncio.Event) -> None:
        """Un cycle complet snapshot→streaming ; ne sort que par exception."""
        self._set_state(SyncState.BUFFERING)
        buffer: list[RawMessage] = []
        snapshot = await self._fetch_snapshot_while_buffering(buffer, stop_event)

        self._set_state(SyncState.SNAPSHOT_FETCHED)
        self._book.load_snapshot(snapshot)
        if self._capture is not None and snapshot.raw:
            self._capture.submit(
                time.time_ns() // 1_000_000, None, self._exchange.symbol, snapshot.raw
            )
        validator = SequenceValidator(snapshot.last_update_id)
        self._log.info(
            "snapshot chargé : lastUpdateId=%d, %d bids / %d asks, %d messages bufferisés",
            snapshot.last_update_id,
            len(snapshot.bids),
            len(snapshot.asks),
            len(buffer),
        )

        # Rejouer le buffer accumulé pendant l'appel REST…
        self._set_state(SyncState.DISCARDING_STALE)
        for message in buffer:
            self._process_message(message, validator)
        buffer.clear()
        if not validator.synced:
            self._set_state(SyncState.VALIDATING_FIRST)

        # …puis consommer le flux en direct.
        while True:
            item = await self._get(0.5)
            if item is None:
                if stop_event.is_set():
                    raise _Stopped
                continue
            if isinstance(item, ControlEvent):
                if item.type is ControlType.DISCONNECTED:
                    raise _Disconnected
                continue
            self._process_message(item, validator)

    async def _fetch_snapshot_while_buffering(
        self, buffer: list[RawMessage], stop_event: asyncio.Event
    ) -> Snapshot:
        """Snapshot REST en parallèle de la bufferisation du flux WebSocket.

        L'appel ``requests`` (bloquant) part dans un thread ; pendant ce temps
        la file est drainée pour ne perdre aucun delta. Échec REST → nouvel
        essai avec backoff exponentiel, sans cesser de bufferiser.
        """
        attempt = 0
        task = asyncio.create_task(asyncio.to_thread(self._exchange.fetch_snapshot))
        try:
            while True:
                if stop_event.is_set():
                    raise _Stopped
                if task.done():
                    try:
                        return task.result()
                    except SnapshotError as exc:
                        if exc.permanent:
                            raise _Fatal(f"snapshot refusé : {exc}") from exc
                        delay = min(
                            self._net.backoff_base_seconds * (2 ** attempt),
                            self._net.backoff_max_seconds,
                        )
                        attempt += 1
                        self._log.error(
                            "échec snapshot REST (%s) — nouvel essai dans %.1f s",
                            exc,
                            delay,
                        )
                        await self._sleep(stop_event, delay)
                        task = asyncio.create_task(
                            asyncio.to_thread(self._exchange.fetch_snapshot)
                        )
                        continue
                item = await self._get(0.05)
                if item is None:
                    continue
                if isinstance(item, ControlEvent):
                    if item.type is ControlType.DISCONNECTED:
                        raise _Disconnected
                    continue
                buffer.append(item)
                if len(buffer) > self._net.buffer_max_messages:
                    raise _Resync(
                        f"buffer saturé pendant la bufferisation "
                        f"(> {self._net.buffer_max_messages} messages)"
                    )
        finally:
            if not task.done():
                task.cancel()

    # ---------------------------------------------------------- traitement

    def _process_message(
        self, message: RawMessage, validator: SequenceValidator
    ) -> None:
        event = self._exchange.parse_event(message.payload)
        if event is None:
            return
        self.rate.tick()
        delta_ms = self.latency.record(event.event_time_ms, message.recv_time_ms)
        self._log.debug(
            "depthUpdate U=%d u=%d latence=%dms",
            event.first_update_id,
            event.final_update_id,
            delta_ms,
        )

        result = validator.evaluate(event.first_update_id, event.final_update_id)

        if result is SeqResult.DISCARD_STALE:
            self.discarded_stale += 1
            return

        if result is SeqResult.ACCEPT_FIRST:
            self._set_state(SyncState.VALIDATING_FIRST)
            self._book.apply(event)
            validator.commit(event.final_update_id)
            self.events_applied += 1
            self._set_state(SyncState.STREAMING)
            self._log.info(
                "premier événement validé (U=%d <= lastUpdateId+1=%d <= u=%d) → STREAMING",
                event.first_update_id,
                validator.snapshot_last_update_id + 1,
                event.final_update_id,
            )
            return

        if result is SeqResult.ACCEPT:
            self._book.apply(event)
            validator.commit(event.final_update_id)
            self.events_applied += 1
            return

        # RESYNC_GAP / RESYNC_OVERLAP / MALFORMED : rejet complet du carnet.
        raise _Resync(
            f"{result.name} — reçu U={event.first_update_id}, "
            f"u={event.final_update_id}, attendu U={validator.expected_first_id}"
        )

    # ------------------------------------------------------------ utilitaires

    def _set_state(self, state: SyncState) -> None:
        if state is not self.state:
            self._log.debug("état : %s → %s", self.state.value, state.value)
            self.state = state

    async def _get(self, timeout: float) -> QueueItem | None:
        try:
            return await asyncio.wait_for(self._queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    async def _sleep(self, stop_event: asyncio.Event, delay: float) -> None:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=delay)
        except asyncio.TimeoutError:
            return
        raise _Stopped

    @property
    def uptime_seconds(self) -> float:
        return time.monotonic() - self.started_at

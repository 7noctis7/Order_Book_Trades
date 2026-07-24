"""Rejeu d'une capture SQLite : le backtest utilise le vrai moteur.

Le fichier ``capture.db`` (mode capture du live) contient chaque message brut
dans son ordre d'arrivée réel : snapshots REST et messages WebSocket (depth,
trade). Le rejeu reconstruit les carnets avec exactement la même validation
de séquence que le live, alimente les mêmes bandes de transactions, et laisse
l'utilisateur trader contre le même moteur paper — à vitesse variable
(×1 … ×100, pause, plein débit). Un backtest n'est crédible que s'il partage
le code du live : ici, seul le transport change.

L'horloge du rejeu est l'horodatage de réception d'origine (``ts_recv_ms``) :
les ordres fictifs, l'historique de prix et la courbe d'equity sont datés en
temps simulé, ce qui rend les résultats reproductibles.
"""
from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Callable, Sequence

from .exchange import DepthEvent, Snapshot
from .metrics import LatencyTracker, RateTracker
from .paper import PaperEngine, TriggerEvent
from .sequencing import RESYNC_RESULTS, SeqResult, SequenceValidator
from .stats import EquityHistory
from .sync import SyncState

log = logging.getLogger("replay")

SPEEDS = [1, 2, 5, 10, 25, 100, 0]  # 0 = plein débit (sans pacing)


class ReplayController:
    """État partagé du rejeu : vitesse, pause, progression, temps simulé."""

    def __init__(self, speed: int = 10) -> None:
        self.speed = speed if speed in SPEEDS else 10
        self.paused = False
        self.finished = False
        self.sim_time_ms = 0
        self.rows_done = 0
        self.rows_total = 0

    def clock(self) -> int:
        return self.sim_time_ms

    def faster(self) -> None:
        index = SPEEDS.index(self.speed)
        self.speed = SPEEDS[min(index + 1, len(SPEEDS) - 1)]

    def slower(self) -> None:
        index = SPEEDS.index(self.speed)
        self.speed = SPEEDS[max(index - 1, 0)]

    @property
    def speed_label(self) -> str:
        return "MAX" if self.speed == 0 else f"×{self.speed}"

    @property
    def progress(self) -> float:
        return self.rows_done / self.rows_total if self.rows_total else 0.0


class ReplayPairSync:
    """Compteurs et état d'une paire rejouée — même interface que le live."""

    def __init__(self) -> None:
        self.state = SyncState.BUFFERING
        self.events_applied = 0
        self.discarded_stale = 0
        self.resync_count = 0
        self.disconnect_count = 0
        self.last_resync_reason: str | None = None
        self.error_reason: str | None = None
        self.latency = LatencyTracker()
        self.rate = RateTracker()
        self._started = time.monotonic()

    @property
    def uptime_seconds(self) -> float:
        return time.monotonic() - self._started


def _parse_decimal_levels(levels) -> list[tuple[Decimal, Decimal]]:
    return [(Decimal(str(p)), Decimal(str(q))) for p, q in levels]


async def run_replay(
    db_path: str,
    pairs: Sequence,                       # PairView (symbol, book, sync, history, tape)
    paper: PaperEngine | None,
    controller: ReplayController,
    equity: EquityHistory | None,
    stop_event: asyncio.Event,
    notify: Callable[[list[TriggerEvent]], None] | None = None,
    max_gap_ms: int = 5_000,
) -> None:
    """Rejoue la capture dans l'ordre d'arrivée, au rythme demandé."""
    by_symbol = {pair.symbol: pair for pair in pairs}
    validators: dict[str, SequenceValidator | None] = {s: None for s in by_symbol}
    last_sample_s: dict[str, int] = {}
    last_equity_s = 0

    conn = sqlite3.connect(db_path)
    try:
        controller.rows_total = conn.execute(
            "SELECT COUNT(*) FROM messages"
        ).fetchone()[0]
        cursor = conn.execute(
            "SELECT ts_recv_ms, symbol, payload FROM messages ORDER BY id"
        )
        prev_ts: int | None = None
        while not stop_event.is_set():
            rows = cursor.fetchmany(500)
            if not rows:
                break
            for ts_recv, symbol, payload_text in rows:
                if stop_event.is_set():
                    return
                # --- pacing : pause + vitesse (écarts plafonnés) -----------
                while controller.paused and not stop_event.is_set():
                    await asyncio.sleep(0.1)
                if controller.speed and prev_ts is not None:
                    delta_ms = min(max(ts_recv - prev_ts, 0), max_gap_ms)
                    if delta_ms:
                        await asyncio.sleep(delta_ms / 1000 / controller.speed)
                prev_ts = ts_recv
                controller.sim_time_ms = ts_recv
                controller.rows_done += 1

                pair = by_symbol.get(symbol)
                if pair is None:
                    continue
                try:
                    payload = json.loads(payload_text)
                except json.JSONDecodeError:
                    continue
                if "stream" in payload and isinstance(payload.get("data"), dict):
                    payload = payload["data"]

                _dispatch(pair, validators, payload, ts_recv, paper, notify)

                # --- échantillonnage en temps simulé (1 s) -----------------
                second = ts_recv // 1000
                if pair.book.is_ready and last_sample_s.get(symbol) != second:
                    mid = pair.book.mid_price()
                    if mid is not None:
                        pair.history.append(second, float(mid))
                        last_sample_s[symbol] = second
                if equity is not None and paper is not None and second != last_equity_s:
                    mids = {
                        s: p.book.mid_price()
                        for s, p in by_symbol.items()
                        if p.book.is_ready and p.book.mid_price() is not None
                    }
                    equity.append(second, float(paper.summary(mids)["equity"]))
                    last_equity_s = second
    finally:
        conn.close()
    controller.finished = True
    log.info(
        "rejeu terminé : %d messages, temps simulé final %s",
        controller.rows_done,
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(controller.sim_time_ms / 1000)),
    )


def _dispatch(pair, validators, payload: dict, ts_recv: int, paper, notify) -> None:
    symbol = pair.symbol
    sync: ReplayPairSync = pair.sync
    sync.rate.tick()

    # Snapshot REST capturé (pas de clé "e", mais lastUpdateId + bids/asks).
    if "lastUpdateId" in payload and "e" not in payload:
        pair.book.load_snapshot(
            Snapshot(
                last_update_id=int(payload["lastUpdateId"]),
                bids=_parse_decimal_levels(payload.get("bids", [])),
                asks=_parse_decimal_levels(payload.get("asks", [])),
            )
        )
        validators[symbol] = SequenceValidator(int(payload["lastUpdateId"]))
        sync.state = SyncState.VALIDATING_FIRST
        return

    kind = payload.get("e")
    if kind == "trade":
        if pair.tape is not None:
            try:
                pair.tape.add(
                    int(payload["T"]),
                    Decimal(str(payload["p"])),
                    Decimal(str(payload["q"])),
                    bool(payload["m"]),
                )
            except (KeyError, ValueError, ArithmeticError):
                return
            _check_triggers(pair, paper, notify)
        return

    if kind != "depthUpdate":
        return
    if payload.get("E"):
        sync.latency.record(int(payload["E"]), ts_recv)
    validator = validators.get(symbol)
    if validator is None:
        return  # en attente du prochain snapshot dans la capture
    try:
        event = DepthEvent(
            first_update_id=int(payload["U"]),
            final_update_id=int(payload["u"]),
            event_time_ms=int(payload.get("E") or 0),
            bids=_parse_decimal_levels(payload.get("b", [])),
            asks=_parse_decimal_levels(payload.get("a", [])),
        )
    except (KeyError, ValueError, ArithmeticError):
        return
    result = validator.evaluate(event.first_update_id, event.final_update_id)
    if result is SeqResult.DISCARD_STALE:
        sync.discarded_stale += 1
        return
    if result in (SeqResult.ACCEPT, SeqResult.ACCEPT_FIRST):
        pair.book.apply(event)
        validator.commit(event.final_update_id)
        sync.events_applied += 1
        sync.state = SyncState.STREAMING
        _check_triggers(pair, paper, notify)
        return
    if result in RESYNC_RESULTS:
        # Même politique que le live : abandon complet, attente du prochain
        # snapshot présent dans la capture.
        sync.resync_count += 1
        sync.last_resync_reason = f"rejeu : {result.name}"
        validators[symbol] = None
        pair.book.clear()
        sync.state = SyncState.BUFFERING


def _check_triggers(pair, paper, notify) -> None:
    if paper is None:
        return
    last = pair.tape.last if pair.tape is not None else None
    events = paper.check_triggers(pair.symbol, pair.book, last)
    if events and notify is not None:
        notify(events)

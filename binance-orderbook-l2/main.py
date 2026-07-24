"""Point d'entrée : assemblage des composants et boucle d'exécution asyncio.

Arrêt propre sur Ctrl+C : le signal est capté via le module ``signal``
standard (``loop.add_signal_handler`` n'est pas implémenté sur le
ProactorEventLoop de Windows), les tâches se terminent coopérativement,
la socket WebSocket est fermée, la capture SQLite est flushée et fermée.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys

from lob.binance_client import BinanceClient
from lob.capture import SqliteCaptureWriter
from lob.config import AppConfig, ConfigError, load_config
from decimal import Decimal

from lob.exchange import ExchangeClient
from lob.history import PriceHistory, run_sampler
from lob.keyboard import KeyReader
from lob.kraken import KrakenFeed, KrakenPairSync
from lob.metrics_server import PrometheusExporter, escape_label
from lob.paper import PaperEngine
from lob.stats import EquityHistory
from lob.trades_feed import TradeTape
from lob.logging_setup import RingBufferHandler, setup_logging
from lob.orderbook import OrderBook
from lob.sync import OrderBookSync, SyncState
from lob.ui import ConsoleUI, PairView
from lob.ws_client import WSClient

log = logging.getLogger("main")


def build_exchange(cfg: AppConfig, symbol: str) -> ExchangeClient:
    if cfg.exchange == "binance":
        return BinanceClient(cfg, symbol)
    raise ConfigError(
        f"exchange non supporté : {cfg.exchange!r} (seul 'binance' est implémenté)"
    )


_STATE_CODES = {
    SyncState.CONNECTING: 0,
    SyncState.BUFFERING: 1,
    SyncState.SNAPSHOT_FETCHED: 2,
    SyncState.DISCARDING_STALE: 3,
    SyncState.VALIDATING_FIRST: 4,
    SyncState.STREAMING: 5,
    SyncState.ERROR: -1,
}


def _build_collector(pairs, paper, started_monotonic: float):
    """Fabrique le collecteur /metrics : lecture seule d'états existants."""
    import time as _time

    def collect() -> str:
        now_ms = _time.time_ns() // 1_000_000
        out: list[str] = []

        def gauge(name: str, value, labels: str = "") -> None:
            if value is None:
                return
            out.append(f"{name}{{{labels}}} {value}" if labels else f"{name} {value}")

        for pair in pairs:
            sym = f'symbol="{escape_label(pair.symbol)}"'
            sync, book = pair.sync, pair.book
            gauge("lob_pair_state", _STATE_CODES.get(sync.state, 0), sym)
            gauge("lob_events_applied_total", sync.events_applied, sym)
            gauge("lob_resyncs_total", sync.resync_count, sym)
            gauge("lob_disconnects_total", sync.disconnect_count, sym)
            gauge("lob_messages_per_second", round(sync.rate.rate(), 3), sym)
            gauge("lob_latency_ms", sync.latency.p50, f'{sym},quantile="0.5"')
            gauge("lob_latency_ms", sync.latency.p95, f'{sym},quantile="0.95"')
            if book.is_ready:
                bid, ask = book.best_bid(), book.best_ask()
                mid, spread = book.mid_price(), book.spread()
                gauge("lob_best_bid", bid[0] if bid else None, sym)
                gauge("lob_best_ask", ask[0] if ask else None, sym)
                if spread is not None and mid:
                    gauge("lob_spread_bp", round(spread / mid * 10_000, 4), sym)
                imbalance = book.imbalance(10)
                if imbalance is not None:
                    gauge("lob_imbalance", round(imbalance, 4), sym)
            if pair.tape is not None and pair.tape.last is not None:
                gauge("lob_last_price", pair.tape.last, sym)
                gauge("lob_vwap_60s", pair.tape.vwap(now_ms), sym)
        if paper is not None:
            mids = {
                p.symbol: p.book.mid_price()
                for p in pairs
                if p.book.is_ready and p.book.mid_price() is not None
            }
            summary = paper.summary(mids)
            gauge("lob_paper_cash", summary["cash"])
            gauge("lob_paper_equity", summary["equity"])
            gauge("lob_paper_realized_total", summary["realized"])
            gauge("lob_paper_unrealized", summary["unrealized"])
            gauge("lob_paper_open_orders", len(paper.pending))
        gauge(
            "lob_uptime_seconds",
            round(_time.monotonic() - started_monotonic, 1),
        )
        return "\n".join(out) + "\n"

    return collect


async def run_app(cfg: AppConfig, ring: RingBufferHandler) -> None:
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    # SIGINT/SIGTERM via le module signal standard (compatible Windows).
    def _request_stop(signum, frame) -> None:  # noqa: ARG001 (signature imposée)
        loop.call_soon_threadsafe(stop_event.set)

    signal.signal(signal.SIGINT, _request_stop)
    if hasattr(signal, "SIGTERM"):
        try:
            signal.signal(signal.SIGTERM, _request_stop)
        except (ValueError, OSError):
            pass

    capture: SqliteCaptureWriter | None = None
    if cfg.capture.enabled:
        capture = SqliteCaptureWriter(cfg.capture.db_path)
        capture.start()

    # Une pile indépendante par paire : connexion WebSocket, file, carnet,
    # synchroniseur. Isolation totale : un resync ou une paire invalide
    # n'affecte jamais les autres.
    pairs: list[PairView] = []
    tasks: list[asyncio.Task] = []
    if cfg.exchange == "kraken":
        # Kraken : une seule connexion WebSocket porte toutes les paires
        # (snapshot + deltas + checksum sur le même canal).
        for symbol in cfg.symbols:
            book = OrderBook()
            pairs.append(
                PairView(
                    symbol=symbol,
                    book=book,
                    sync=KrakenPairSync(symbol, book, cfg.kraken.depth),
                    history=PriceHistory(),
                    tape=TradeTape(),
                )
            )
        feed = KrakenFeed(
            cfg.kraken.ws_base, pairs, cfg.kraken.depth, cfg.network, capture
        )
        tasks.append(asyncio.create_task(feed.run(stop_event), name="kraken"))
    else:
        # Binance : une pile indépendante par paire (isolation totale).
        for symbol in cfg.symbols:
            exchange = build_exchange(cfg, symbol)
            queue: asyncio.Queue = asyncio.Queue()
            book = OrderBook()
            tape = TradeTape()
            ws = WSClient(
                exchange.ws_url(), symbol, queue, cfg.network, capture,
                tape=tape, parse_trade=BinanceClient.parse_trade,
            )
            sync = OrderBookSync(exchange, book, queue, cfg.network, capture)
            pairs.append(
                PairView(
                    symbol=symbol, book=book, sync=sync,
                    history=PriceHistory(), tape=tape,
                )
            )
            tasks.append(asyncio.create_task(ws.run(stop_event), name=f"ws.{symbol}"))
            tasks.append(
                asyncio.create_task(sync.run(stop_event), name=f"sync.{symbol}")
            )

    paper: PaperEngine | None = None
    if cfg.paper.enabled:
        paper = PaperEngine(
            db_path=cfg.paper.db_path,
            initial_cash=Decimal(str(cfg.paper.initial_cash_usdt)),
            fee_bps=Decimal(str(cfg.paper.fee_bps)),
        )

    equity = EquityHistory() if paper is not None else None
    tasks.append(
        asyncio.create_task(
            run_sampler(pairs, stop_event, paper=paper, equity=equity),
            name="sampler",
        )
    )

    ui: ConsoleUI | None = None
    if cfg.display.enabled:
        ui = ConsoleUI(
            pairs=pairs,
            cfg=cfg.display,
            ring=ring,
            exchange_name=cfg.exchange,
            ws_speed_ms=cfg.ws_speed_ms,
            paper=paper,
            capture=capture,
            log_file=cfg.logging.file,
            equity=equity,
        )
        tasks.append(asyncio.create_task(ui.run(stop_event), name="ui"))
        tasks.append(
            asyncio.create_task(KeyReader(ui.on_key).run(stop_event), name="keyboard")
        )

    if paper is not None:
        notify = ui.notify_triggers if ui is not None else None

        async def run_triggers() -> None:
            """Évalue les ordres LIMIT/STOP contre chaque carnet (4×/s)."""
            while not stop_event.is_set():
                if paper.pending:
                    for pair in pairs:
                        last = pair.tape.last if pair.tape is not None else None
                        events = paper.check_triggers(pair.symbol, pair.book, last)
                        if events and notify is not None:
                            notify(events)
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=0.25)
                except asyncio.TimeoutError:
                    pass

        tasks.append(asyncio.create_task(run_triggers(), name="triggers"))

    exporter: PrometheusExporter | None = None
    if cfg.prometheus.enabled:
        exporter = PrometheusExporter(
            cfg.prometheus.host,
            cfg.prometheus.port,
            _build_collector(pairs, paper, loop.time()),
        )
        exporter.start()

    log.info(
        "démarrage : %s — %d paires (%s), profondeur %d, flux %d ms, display %s, capture %s",
        cfg.exchange,
        len(cfg.symbols),
        ", ".join(cfg.symbols),
        cfg.depth_limit,
        cfg.ws_speed_ms,
        "ON" if cfg.display.enabled else "OFF",
        "ON" if cfg.capture.enabled else "OFF",
    )

    # Attendre l'arrêt demandé OU la fin anormale d'une tâche.
    stop_waiter = asyncio.create_task(stop_event.wait(), name="stop")
    watched = set(tasks)
    while not stop_event.is_set() and watched:
        done, _ = await asyncio.wait(
            [stop_waiter, *watched], return_when=asyncio.FIRST_COMPLETED
        )
        crashed = False
        for task in done & watched:
            watched.discard(task)
            if task.exception():
                log.error(
                    "tâche '%s' terminée en erreur : %r",
                    task.get_name(),
                    task.exception(),
                )
                crashed = True
            # Fin normale (ex. paire en ERROR) : les autres continuent.
        if crashed:
            stop_event.set()

    log.info("arrêt demandé — fermeture en cours…")
    done, pending = await asyncio.wait([stop_waiter, *tasks], timeout=8.0)
    for task in pending:
        task.cancel()
    await asyncio.gather(stop_waiter, *tasks, return_exceptions=True)

    if exporter is not None:
        exporter.stop()
    if capture is not None:
        capture.stop()
    if paper is not None:
        paper.close()
    log.info("arrêt propre terminé")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Reconstruction temps réel d'un carnet d'ordres L2 (Binance)."
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="chemin du fichier de configuration YAML (défaut : config.yaml)",
    )
    args = parser.parse_args()

    try:
        cfg = load_config(args.config)
    except ConfigError as exc:
        print(f"Erreur de configuration : {exc}", file=sys.stderr)
        return 2

    ring = setup_logging(
        level=cfg.logging.level,
        file=cfg.logging.file,
        console=not cfg.display.enabled,
    )

    try:
        asyncio.run(run_app(cfg, ring))
    except KeyboardInterrupt:
        # Filet de sécurité si le signal arrive pendant le teardown asyncio.
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

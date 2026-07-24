"""Backtest interactif : rejoue une capture SQLite contre le moteur paper.

Usage :
    python backtest.py --db data/capture.db [--speed 10] [--fresh]

Mêmes vues et mêmes touches que le live, plus les contrôles de rejeu :
Espace pause/reprise, +/- vitesse (×1 ×2 ×5 ×10 ×25 ×100 MAX).
Le portefeuille de backtest vit dans sa propre base (``data/backtest.db``)
pour ne jamais mélanger entraînement live et sessions de backtest.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sqlite3
import sys
from decimal import Decimal
from pathlib import Path

from lob.config import AppConfig, ConfigError, load_config
from lob.history import PriceHistory
from lob.keyboard import KeyReader
from lob.logging_setup import RingBufferHandler, setup_logging
from lob.orderbook import OrderBook
from lob.paper import PaperEngine
from lob.replay import ReplayController, ReplayPairSync, run_replay
from lob.stats import EquityHistory
from lob.trades_feed import TradeTape
from lob.ui import ConsoleUI, PairView

log = logging.getLogger("backtest")


def _symbols_in_capture(db_path: str, preferred_order: list[str]) -> list[str]:
    with sqlite3.connect(db_path) as conn:
        found = [row[0] for row in conn.execute("SELECT DISTINCT symbol FROM messages")]
    ordered = [s for s in preferred_order if s in found]
    ordered += sorted(s for s in found if s not in ordered)
    return ordered


async def run_backtest(
    cfg: AppConfig, ring: RingBufferHandler, args: argparse.Namespace
) -> None:
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _request_stop(signum, frame) -> None:  # noqa: ARG001
        loop.call_soon_threadsafe(stop_event.set)

    signal.signal(signal.SIGINT, _request_stop)
    if hasattr(signal, "SIGTERM"):
        try:
            signal.signal(signal.SIGTERM, _request_stop)
        except (ValueError, OSError):
            pass

    symbols = _symbols_in_capture(args.db, cfg.symbols)
    if not symbols:
        raise ConfigError(f"aucun message dans {args.db}")

    controller = ReplayController(speed=args.speed)
    if args.fresh:
        Path(args.paper_db).unlink(missing_ok=True)
    paper = PaperEngine(
        db_path=args.paper_db,
        initial_cash=Decimal(str(cfg.paper.initial_cash_usdt)),
        fee_bps=Decimal(str(cfg.paper.fee_bps)),
        clock=controller.clock,
    )
    equity = EquityHistory()

    pairs = [
        PairView(
            symbol=symbol,
            book=OrderBook(),
            sync=ReplayPairSync(),
            history=PriceHistory(),
            tape=TradeTape(),
        )
        for symbol in symbols
    ]

    ui = ConsoleUI(
        pairs=pairs,
        cfg=cfg.display,
        ring=ring,
        exchange_name=cfg.exchange,
        ws_speed_ms=cfg.ws_speed_ms,
        paper=paper,
        capture=None,
        log_file=cfg.logging.file,
        replay=controller,
        equity=equity,
    )
    tasks = [
        asyncio.create_task(
            run_replay(
                args.db, pairs, paper, controller, equity, stop_event,
                notify=ui.notify_triggers,
            ),
            name="replay",
        ),
        asyncio.create_task(ui.run(stop_event), name="ui"),
        asyncio.create_task(KeyReader(ui.on_key).run(stop_event), name="keyboard"),
    ]
    log.info(
        "backtest : %s — %d paires (%s), vitesse initiale %s, portefeuille %s",
        args.db, len(symbols), ", ".join(symbols), controller.speed_label, args.paper_db,
    )
    await stop_event.wait()
    done, pending = await asyncio.wait(tasks, timeout=5)
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)
    paper.close()
    log.info("backtest arrêté proprement")


def main() -> int:
    parser = argparse.ArgumentParser(description="Backtest par rejeu de capture")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--db", default=None, help="capture à rejouer (défaut : capture.db_path)")
    parser.add_argument("--paper-db", default="data/backtest.db")
    parser.add_argument("--speed", type=int, default=10, help="1/2/5/10/25/100, 0 = plein débit")
    parser.add_argument("--fresh", action="store_true", help="repartir d'un portefeuille vierge")
    args = parser.parse_args()
    try:
        cfg = load_config(args.config)
    except ConfigError as exc:
        print(f"Configuration invalide : {exc}", file=sys.stderr)
        return 2
    args.db = args.db or cfg.capture.db_path
    if not Path(args.db).exists():
        print(
            f"Capture introuvable : {args.db}\n"
            "Activer capture.enabled dans config.yaml et lancer main.py pour"
            " enregistrer une session, puis relancer le backtest.",
            file=sys.stderr,
        )
        return 2
    ring = setup_logging(cfg.logging.level, cfg.logging.file, console=not cfg.display.enabled)
    try:
        asyncio.run(run_backtest(cfg, ring, args))
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    main()

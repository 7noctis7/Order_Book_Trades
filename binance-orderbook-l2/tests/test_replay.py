"""Tests du rejeu de capture : séquence, resync sur gap, triggers, équité."""
import asyncio
import json
import sqlite3
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from lob.history import PriceHistory
from lob.orderbook import OrderBook
from lob.paper import PaperEngine
from lob.replay import ReplayController, ReplayPairSync, run_replay
from lob.stats import EquityHistory
from lob.sync import SyncState
from lob.trades_feed import TradeTape
from lob.ui import PairView

_CAPTURE_SCHEMA = """
CREATE TABLE messages (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_recv_ms  INTEGER NOT NULL,
    ts_event_ms INTEGER,
    symbol      TEXT    NOT NULL,
    payload     TEXT    NOT NULL
);
"""


def _snapshot(last_id: int, bid: str, ask: str) -> str:
    return json.dumps(
        {
            "lastUpdateId": last_id,
            "bids": [[bid, "2"], ["98.00", "5"]],
            "asks": [[ask, "2"], ["102.00", "5"]],
        }
    )


def _depth(first: int, final: int, bids=(), asks=()) -> str:
    # Enveloppe de stream combiné, comme capturé en live.
    return json.dumps(
        {
            "stream": "btcusdt@depth@100ms",
            "data": {
                "e": "depthUpdate",
                "E": 1_000,
                "s": "BTCUSDT",
                "U": first,
                "u": final,
                "b": [list(level) for level in bids],
                "a": [list(level) for level in asks],
            },
        }
    )


def _trade(price: str) -> str:
    return json.dumps(
        {
            "e": "trade",
            "E": 1_000,
            "s": "BTCUSDT",
            "p": price,
            "q": "0.5",
            "T": 1_000,
            "m": False,
        }
    )


def _make_capture(path: str, rows: list[tuple[int, str]]) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(_CAPTURE_SCHEMA)
    conn.executemany(
        "INSERT INTO messages (ts_recv_ms, ts_event_ms, symbol, payload)"
        " VALUES (?, NULL, 'BTCUSDT', ?)",
        rows,
    )
    conn.commit()
    conn.close()


def _pair() -> PairView:
    return PairView(
        symbol="BTCUSDT",
        book=OrderBook(),
        sync=ReplayPairSync(),
        history=PriceHistory(),
        tape=TradeTape(),
    )


def _run(db: str, pair: PairView, paper=None, notify=None) -> tuple:
    controller = ReplayController(speed=0)  # plein débit : aucun pacing
    equity = EquityHistory()
    stop = asyncio.Event()
    asyncio.run(
        run_replay(db, [pair], paper, controller, equity, stop, notify=notify)
    )
    return controller, equity


class ReplayTests(unittest.TestCase):
    def test_snapshot_puis_events_appliques(self) -> None:
        rows = [
            (1_000, _depth(90, 95)),   # avant tout snapshot : ignoré sans référence
            (1_100, _snapshot(100, "99.00", "101.00")),
            (1_150, _depth(80, 96)),   # périmé (u <= lastUpdateId) : compté
            (1_200, _depth(98, 103, bids=[("99.50", "1")])),   # chevauche l'id
            (1_300, _depth(104, 110, asks=[("100.90", "3")])),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "capture.db")
            _make_capture(db, rows)
            pair = _pair()
            controller, _ = _run(db, pair)
        self.assertIs(pair.sync.state, SyncState.STREAMING)
        self.assertEqual(pair.sync.events_applied, 2)
        self.assertEqual(pair.sync.discarded_stale, 1)
        self.assertEqual(pair.book.best_bid()[0], Decimal("99.50"))
        self.assertEqual(pair.book.best_ask()[0], Decimal("100.90"))
        self.assertTrue(controller.finished)
        self.assertEqual(controller.rows_done, 5)

    def test_gap_attend_le_snapshot_suivant(self) -> None:
        rows = [
            (1_000, _snapshot(100, "99.00", "101.00")),
            (1_100, _depth(101, 105)),
            (1_200, _depth(120, 125)),               # trou : 106..119 manquants
            (1_300, _depth(126, 130)),               # ignoré (en attente snapshot)
            (1_400, _snapshot(200, "99.10", "100.90")),
            (1_500, _depth(201, 210, bids=[("99.20", "1")])),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "capture.db")
            _make_capture(db, rows)
            pair = _pair()
            _run(db, pair)
        self.assertEqual(pair.sync.resync_count, 1)
        self.assertIs(pair.sync.state, SyncState.STREAMING)
        self.assertEqual(pair.book.best_bid()[0], Decimal("99.20"))

    def test_trade_declenche_un_ordre_limite(self) -> None:
        rows = [
            (1_000, _snapshot(100, "99.00", "101.00")),
            (1_100, _depth(101, 105)),
            (60_000, _trade("98.40")),               # imprime sous la limite
        ]
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "capture.db")
            _make_capture(db, rows)
            pair = _pair()
            controller = ReplayController(speed=0)
            paper = PaperEngine(
                db_path=None,
                initial_cash=Decimal("10000"),
                fee_bps=Decimal("0"),
                clock=controller.clock,
            )
            paper.place_pending("BTCUSDT", "BUY", "LIMIT", Decimal("1"), Decimal("98.5"))
            fired: list = []
            asyncio.run(
                run_replay(
                    db, [pair], paper, controller, EquityHistory(),
                    asyncio.Event(), notify=fired.extend,
                )
            )
        self.assertEqual(len(fired), 1)
        self.assertEqual(fired[0].report.avg_price, Decimal("98.5"))
        # Horodatage simulé : celui de la ligne rejouée, pas l'heure système.
        self.assertEqual(fired[0].report.ts_ms, 60_000)
        self.assertEqual(paper.position_qty("BTCUSDT"), Decimal("1"))

    def test_historique_et_equity_en_temps_simule(self) -> None:
        rows = [(1_000, _snapshot(100, "99.00", "101.00"))]
        rows += [
            (second * 1_000, _depth(99 + second, 99 + second))  # 101, 102, …
            for second in range(2, 8)
        ]
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "capture.db")
            _make_capture(db, rows)
            pair = _pair()
            paper = PaperEngine(
                db_path=None, initial_cash=Decimal("5000"), fee_bps=Decimal("0")
            )
            _, equity = _run(db, pair, paper=paper)
        self.assertGreaterEqual(len(pair.history.samples), 5)
        self.assertEqual(pair.history.samples[0][0], 1)      # secondes simulées
        self.assertEqual([v for _, v in equity.samples][0], 5000.0)


if __name__ == "__main__":
    unittest.main()

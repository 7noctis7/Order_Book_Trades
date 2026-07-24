"""Tests de la bande des transactions : last, VWAP fenêtré, ratio acheteur."""
import unittest
from decimal import Decimal

from lob.binance_client import BinanceClient
from lob.trades_feed import TradeTape


def _d(v) -> Decimal:
    return Decimal(str(v))


class TapeTests(unittest.TestCase):
    def test_last_et_vwap(self) -> None:
        tape = TradeTape(window_seconds=60)
        tape.add(1_000, _d("100"), _d("1"), False)
        tape.add(2_000, _d("110"), _d("3"), True)
        self.assertEqual(tape.last, _d("110"))
        # VWAP = (100*1 + 110*3) / 4 = 107.5
        self.assertEqual(tape.vwap(now_ms=10_000), _d("107.5"))
        self.assertEqual(tape.volume(10_000), _d("4"))

    def test_fenetre_glissante_exclut_les_vieux_trades(self) -> None:
        tape = TradeTape(window_seconds=60)
        tape.add(0, _d("100"), _d("10"), False)          # hors fenêtre à t=61s
        tape.add(60_500, _d("200"), _d("1"), False)
        self.assertEqual(tape.vwap(now_ms=61_000), _d("200"))

    def test_vwap_sans_volume(self) -> None:
        self.assertIsNone(TradeTape().vwap(now_ms=0))

    def test_ratio_acheteur(self) -> None:
        tape = TradeTape()
        tape.add(1, _d("100"), _d("3"), False)  # taker acheteur
        tape.add(2, _d("100"), _d("1"), True)   # taker vendeur
        self.assertAlmostEqual(tape.buy_ratio(now_ms=1000), 0.75)

    def test_parse_trade_binance(self) -> None:
        msg = {"e": "trade", "E": 1, "s": "BTCUSDT", "t": 5,
               "p": "116432.55", "q": "0.004", "T": 1_700_000_000_123, "m": True}
        parsed = BinanceClient.parse_trade(msg)
        self.assertEqual(parsed, (1_700_000_000_123, _d("116432.55"), _d("0.004"), True))
        self.assertIsNone(BinanceClient.parse_trade({"e": "depthUpdate"}))
        self.assertIsNone(BinanceClient.parse_trade({"e": "trade", "p": "x"}))


if __name__ == "__main__":
    unittest.main()

"""Tests des statistiques de performance : vecteurs connus, drawdown, CSV."""
import csv
import tempfile
import unittest
from decimal import Decimal

from lob.paper import PaperEngine
from lob.stats import EquityHistory, compute, export_csv, max_drawdown


def _d(v) -> Decimal:
    return Decimal(str(v))


def _engine_with_trades(pnls_frais_zero) -> PaperEngine:
    """Construit un moteur avec des allers-retours au PnL choisi (frais 0)."""
    engine = PaperEngine(db_path=None, initial_cash=_d("100000"), fee_bps=_d("0"))
    ts = 0
    for pnl in pnls_frais_zero:
        ts += 60_000  # 1 min de détention par trade
        engine._replay(ts, "BTCUSDT", "BUY", _d("1"), _d("100"), _d("0"))
        engine._replay(ts + 60_000, "BTCUSDT", "SELL", _d("1"), _d(100 + pnl), _d("0"))
    return engine


class StatsTests(unittest.TestCase):
    def test_vecteur_connu(self) -> None:
        # 3 gains (+10, +20, +30), 2 pertes (-15, -5)
        engine = _engine_with_trades([10, 20, 30, -15, -5])
        s = compute(engine)
        self.assertEqual((s["trades"], s["wins"], s["losses"]), (5, 3, 2))
        self.assertAlmostEqual(s["win_rate"], 60.0)
        self.assertEqual(s["gross_profit"], _d("60"))
        self.assertEqual(s["gross_loss"], _d("20"))
        self.assertAlmostEqual(s["profit_factor"], 3.0)
        self.assertEqual(s["expectancy"], _d("8"))
        self.assertEqual((s["best"], s["worst"]), (_d("30"), _d("-15")))
        self.assertAlmostEqual(s["avg_holding_s"], 60.0)

    def test_sans_trade(self) -> None:
        engine = PaperEngine(db_path=None, initial_cash=_d("1000"), fee_bps=_d("0"))
        s = compute(engine)
        self.assertEqual(s["trades"], 0)
        self.assertIsNone(s["win_rate"])
        self.assertIsNone(s["profit_factor"])

    def test_profit_factor_sans_perte(self) -> None:
        s = compute(_engine_with_trades([10, 5]))
        self.assertEqual(s["profit_factor"], float("inf"))

    def test_max_drawdown(self) -> None:
        # pic 110 → creux 88 : DD 22 (20 % du pic)
        dd, pct = max_drawdown([100, 110, 95, 88, 105, 120, 118])
        self.assertAlmostEqual(dd, 22.0)
        self.assertAlmostEqual(pct, 20.0)

    def test_drawdown_croissance_monotone(self) -> None:
        self.assertEqual(max_drawdown([1, 2, 3]), (0.0, 0.0))

    def test_stats_integrees_a_l_equity(self) -> None:
        engine = _engine_with_trades([10])
        eq = EquityHistory()
        for v in (100000, 100020, 99990, 100010):
            eq.append(0, v)
        s = compute(engine, eq)
        self.assertAlmostEqual(s["max_drawdown"], 30.0)

    def test_export_csv(self) -> None:
        engine = _engine_with_trades([10, -5])
        engine._replay(999_000, "ETHUSDT", "BUY", _d("2"), _d("50"), _d("0"))
        with tempfile.TemporaryDirectory() as tmp:
            path = export_csv(engine, {"ETHUSDT": _d("55")}, directory=tmp)
            rows = list(csv.reader(open(path, encoding="utf-8")))
        self.assertEqual(rows[0][0], "statut")
        self.assertEqual(len(rows), 1 + 2 + 1)  # entête + 2 clos + 1 ouvert
        ouvert = rows[3]
        self.assertEqual(ouvert[0], "ouvert")
        self.assertEqual(ouvert[7], f"{_d('10'):.8f}")  # (55-50)*2 latent


if __name__ == "__main__":
    unittest.main()

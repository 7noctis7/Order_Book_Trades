"""Tests du moteur de paper trading : exécution, FIFO, frais, persistance."""
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from lob.exchange import Snapshot
from lob.orderbook import OrderBook
from lob.paper import PaperEngine, PaperError


def _d(value) -> Decimal:
    return Decimal(str(value))


def _book() -> OrderBook:
    book = OrderBook()
    book.load_snapshot(
        Snapshot(
            last_update_id=1,
            bids=[(_d("99"), _d("1")), (_d("98"), _d("2")), (_d("97"), _d("5"))],
            asks=[(_d("100"), _d("1")), (_d("101"), _d("2")), (_d("102"), _d("5"))],
        )
    )
    return book


def _engine(cash="10000", fee_bps="0", db=None) -> PaperEngine:
    return PaperEngine(db_path=db, initial_cash=_d(cash), fee_bps=_d(fee_bps))


class ExecutionTests(unittest.TestCase):
    def test_achat_marche_le_carnet_avec_slippage(self) -> None:
        engine = _engine()
        report = engine.market_order("BTCUSDT", "BUY", _d("2"), _book())
        # 1 @ 100 + 1 @ 101 → prix moyen 100.5, 2 niveaux consommés
        self.assertEqual(report.avg_price, _d("100.5"))
        self.assertEqual(report.levels_consumed, 2)
        self.assertEqual(report.slippage, _d("0.5"))
        self.assertEqual(engine.position_qty("BTCUSDT"), _d("2"))
        self.assertEqual(engine.cash, _d("10000") - _d("201"))

    def test_vente_marche_les_bids(self) -> None:
        engine = _engine()
        engine.market_order("BTCUSDT", "BUY", _d("3"), _book())
        report = engine.market_order("BTCUSDT", "SELL", _d("3"), _book())
        # 1 @ 99 + 2 @ 98 → moyen (99 + 196) / 3
        self.assertEqual(report.avg_price, _d("295") / 3)

    def test_liquidite_insuffisante(self) -> None:
        with self.assertRaises(PaperError):
            _engine().market_order("BTCUSDT", "BUY", _d("100"), _book())

    def test_cash_insuffisant(self) -> None:
        engine = _engine(cash="150")
        with self.assertRaises(PaperError):
            engine.market_order("BTCUSDT", "BUY", _d("2"), _book())

    def test_pas_de_vente_a_decouvert(self) -> None:
        with self.assertRaises(PaperError):
            _engine().market_order("BTCUSDT", "SELL", _d("1"), _book())

    def test_carnet_non_pret_refuse(self) -> None:
        with self.assertRaises(PaperError):
            _engine().market_order("BTCUSDT", "BUY", _d("1"), OrderBook())

    def test_quantite_invalide(self) -> None:
        with self.assertRaises(PaperError):
            _engine().market_order("BTCUSDT", "BUY", _d("0"), _book())


class FifoPnlTests(unittest.TestCase):
    def test_vente_consomme_les_lots_fifo(self) -> None:
        engine = _engine()
        engine._replay(1, "ETHUSDT", "BUY", _d("1"), _d("100"), _d("0"))
        engine._replay(2, "ETHUSDT", "BUY", _d("1"), _d("110"), _d("0"))
        engine._replay(3, "ETHUSDT", "SELL", _d("1.5"), _d("120"), _d("0"))

        self.assertEqual(len(engine.closed), 2)
        first, second = engine.closed
        self.assertEqual((first.qty, first.buy_price), (_d("1"), _d("100")))
        self.assertEqual(first.realized, _d("20"))
        self.assertEqual(first.realized_pct, _d("20"))
        self.assertEqual((second.qty, second.buy_price), (_d("0.5"), _d("110")))
        self.assertEqual(second.realized, _d("5"))
        # Reste un demi-lot du second achat, au bon prix.
        self.assertEqual(engine.position_qty("ETHUSDT"), _d("0.5"))
        self.assertEqual(engine.lots["ETHUSDT"][0].price, _d("110"))
        self.assertEqual(engine.realized_total, _d("25"))

    def test_frais_dans_le_pnl_et_le_cash(self) -> None:
        engine = _engine(cash="1000", fee_bps="10")  # 0,10 %
        engine._replay(1, "SOLUSDT", "BUY", _d("1"), _d("100"), _d("0.1"))
        self.assertEqual(engine.cash, _d("899.9"))
        engine._replay(2, "SOLUSDT", "SELL", _d("1"), _d("100"), _d("0.1"))
        self.assertEqual(engine.cash, _d("999.8"))
        # Aller-retour à prix constant : PnL = -2 × frais.
        self.assertEqual(engine.realized_total, _d("-0.2"))

    def test_pnl_latent_et_resume(self) -> None:
        engine = _engine()
        engine._replay(1, "BTCUSDT", "BUY", _d("2"), _d("100"), _d("0"))
        self.assertEqual(engine.unrealized("BTCUSDT", _d("105")), _d("10"))
        summary = engine.summary({"BTCUSDT": _d("105")})
        self.assertEqual(summary["equity"], _d("10000") - _d("200") + _d("210"))
        self.assertEqual(summary["unrealized"], _d("10"))
        self.assertEqual(summary["unrealized_pct"], _d("5"))


class PersistenceTests(unittest.TestCase):
    def test_etat_reconstruit_au_rechargement(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "paper.db")
            engine = _engine(cash="10000", fee_bps="10", db=db)
            engine.market_order("BTCUSDT", "BUY", _d("2"), _book())
            engine.market_order("BTCUSDT", "SELL", _d("1"), _book())
            expected = (engine.cash, engine.position_qty("BTCUSDT"), len(engine.closed))
            engine.close()

            reloaded = _engine(cash="10000", fee_bps="10", db=db)
            self.assertEqual(
                (reloaded.cash, reloaded.position_qty("BTCUSDT"), len(reloaded.closed)),
                expected,
            )
            self.assertEqual(reloaded.closed[0].qty, _d("1"))
            reloaded.close()


if __name__ == "__main__":
    unittest.main()


class PendingOrderTests(unittest.TestCase):
    """Ordres LIMIT / STOP : pose, déclenchement, rejets, persistance."""

    def test_limit_buy_declenche_quand_ask_traverse(self) -> None:
        engine = _engine()
        order = engine.place_pending("BTCUSDT", "BUY", "LIMIT", _d("1"), _d("99.5"))
        # ask à 100 : pas déclenché
        self.assertEqual(engine.check_triggers("BTCUSDT", _book()), [])
        # le marché baisse : meilleur ask 99 <= limite 99.5 → exécuté AU PRIX LIMITE
        book = OrderBook()
        book.load_snapshot(
            Snapshot(1, [(_d("98"), _d("5"))], [(_d("99"), _d("5"))])
        )
        events = engine.check_triggers("BTCUSDT", book)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].order.id, order.id)
        self.assertEqual(events[0].report.avg_price, _d("99.5"))
        self.assertEqual(engine.position_qty("BTCUSDT"), _d("1"))
        self.assertEqual(engine.pending, [])

    def test_limit_declenche_par_transaction_reelle(self) -> None:
        engine = _engine()
        engine.place_pending("BTCUSDT", "BUY", "LIMIT", _d("1"), _d("99.5"))
        # carnet inchangé (ask 100) mais un trade s'imprime à 99.4
        events = engine.check_triggers("BTCUSDT", _book(), last_trade=_d("99.4"))
        self.assertEqual(len(events), 1)
        self.assertIsNotNone(events[0].report)

    def test_stop_loss_vend_au_marche_avec_slippage(self) -> None:
        engine = _engine()
        engine.market_order("BTCUSDT", "BUY", _d("2"), _book())
        engine.place_pending("BTCUSDT", "SELL", "STOP", _d("2"), _d("99"))
        # best_bid 99 <= stop 99 → vente marché : 1@99 + 1@98 = moyen 98.5
        events = engine.check_triggers("BTCUSDT", _book())
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].report.avg_price, _d("98.5"))
        self.assertEqual(engine.position_qty("BTCUSDT"), _d("0"))

    def test_stop_sell_plafonne_a_la_position(self) -> None:
        engine = _engine()
        engine.market_order("BTCUSDT", "BUY", _d("1"), _book())
        engine.place_pending("BTCUSDT", "SELL", "STOP", _d("5"), _d("99"))
        events = engine.check_triggers("BTCUSDT", _book())
        self.assertEqual(events[0].report.qty, _d("1"))  # plafonné, pas rejeté

    def test_sell_sans_position_annule(self) -> None:
        engine = _engine()
        engine.place_pending("BTCUSDT", "SELL", "STOP", _d("1"), _d("99"))
        events = engine.check_triggers("BTCUSDT", _book())
        self.assertIsNone(events[0].report)
        self.assertEqual(events[0].order.status, "CANCELLED")

    def test_limit_buy_cash_insuffisant_rejete(self) -> None:
        engine = _engine(cash="10")
        engine.place_pending("BTCUSDT", "BUY", "LIMIT", _d("1"), _d("100"))
        events = engine.check_triggers("BTCUSDT", _book())
        self.assertIsNone(events[0].report)
        self.assertEqual(events[0].order.status, "REJECTED")

    def test_annulation_manuelle(self) -> None:
        engine = _engine()
        order = engine.place_pending("BTCUSDT", "BUY", "LIMIT", _d("1"), _d("50"))
        engine.cancel_pending(order.id)
        self.assertEqual(engine.pending, [])
        with self.assertRaises(PaperError):
            engine.cancel_pending(order.id)

    def test_ordres_ouverts_persistes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "paper.db")
            engine = _engine(db=db)
            engine.place_pending("BTCUSDT", "BUY", "LIMIT", _d("1"), _d("50"))
            filled = engine.place_pending("ETHUSDT", "SELL", "STOP", _d("1"), _d("99"))
            engine.cancel_pending(filled.id)
            engine.close()
            reloaded = _engine(db=db)
            self.assertEqual(len(reloaded.pending), 1)
            self.assertEqual(reloaded.pending[0].symbol, "BTCUSDT")
            # les nouveaux ids ne réutilisent jamais un id existant
            new = reloaded.place_pending("SOLUSDT", "BUY", "LIMIT", _d("1"), _d("10"))
            self.assertGreater(new.id, filled.id)
            reloaded.close()

    def test_horloge_injectable(self) -> None:
        engine = PaperEngine(
            db_path=None,
            initial_cash=_d("10000"),
            fee_bps=_d("0"),
            clock=lambda: 123_456,
        )
        report = engine.market_order("BTCUSDT", "BUY", _d("1"), _book())
        self.assertEqual(report.ts_ms, 123_456)

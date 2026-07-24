"""Tests de la structure de carnet : application des deltas et requêtes."""
import unittest
from decimal import Decimal

from lob.exchange import DepthEvent, Snapshot
from lob.orderbook import OrderBook


def _d(value: str) -> Decimal:
    return Decimal(value)


def _snapshot() -> Snapshot:
    return Snapshot(
        last_update_id=1000,
        bids=[(_d("100.0"), _d("2")), (_d("99.5"), _d("1")), (_d("99.0"), _d("4"))],
        asks=[(_d("100.5"), _d("1")), (_d("101.0"), _d("3")), (_d("101.5"), _d("2"))],
    )


def _event(bids=(), asks=(), final_id=1001) -> DepthEvent:
    return DepthEvent(
        first_update_id=final_id,
        final_update_id=final_id,
        event_time_ms=0,
        bids=[(_d(p), _d(q)) for p, q in bids],
        asks=[(_d(p), _d(q)) for p, q in asks],
    )


class OrderBookTests(unittest.TestCase):
    def setUp(self) -> None:
        self.book = OrderBook()
        self.book.load_snapshot(_snapshot())

    def test_best_bid_ask_apres_snapshot(self) -> None:
        self.assertEqual(self.book.best_bid(), (_d("100.0"), _d("2")))
        self.assertEqual(self.book.best_ask(), (_d("100.5"), _d("1")))
        self.assertEqual(self.book.spread(), _d("0.5"))
        self.assertEqual(self.book.mid_price(), _d("100.25"))

    def test_mise_a_jour_de_quantite(self) -> None:
        self.book.apply(_event(bids=[("100.0", "5")]))
        self.assertEqual(self.book.best_bid(), (_d("100.0"), _d("5")))

    def test_quantite_nulle_supprime_le_niveau(self) -> None:
        self.book.apply(_event(bids=[("100.0", "0")]))
        self.assertEqual(self.book.best_bid(), (_d("99.5"), _d("1")))

    def test_suppression_d_un_niveau_absent_est_sans_effet(self) -> None:
        self.book.apply(_event(asks=[("250.0", "0")]))
        self.assertEqual(self.book.depth(), (3, 3))

    def test_insertion_d_un_nouveau_meilleur_ask(self) -> None:
        self.book.apply(_event(asks=[("100.2", "7")]))
        self.assertEqual(self.book.best_ask(), (_d("100.2"), _d("7")))
        self.assertEqual(self.book.spread(), _d("0.2"))

    def test_top_niveaux_tries(self) -> None:
        self.assertEqual(
            [price for price, _ in self.book.top_bids(3)],
            [_d("100.0"), _d("99.5"), _d("99.0")],
        )
        self.assertEqual(
            [price for price, _ in self.book.top_asks(2)],
            [_d("100.5"), _d("101.0")],
        )

    def test_imbalance(self) -> None:
        # bids = 7, asks = 6 sur 3 niveaux → 7/13
        self.assertAlmostEqual(self.book.imbalance(3), 7 / 13, places=9)

    def test_last_update_id_suit_les_evenements(self) -> None:
        self.book.apply(_event(bids=[("98.0", "1")], final_id=1042))
        self.assertEqual(self.book.last_update_id, 1042)

    def test_clear(self) -> None:
        self.book.clear()
        self.assertFalse(self.book.is_ready)
        self.assertIsNone(self.book.best_bid())
        self.assertIsNone(self.book.imbalance(5))


if __name__ == "__main__":
    unittest.main()


class GenericLevelTests(unittest.TestCase):
    """set_level / truncate : primitives des protocoles à profondeur fixe."""

    def test_set_level_pose_et_supprime(self) -> None:
        book = OrderBook()
        book.set_level("bid", _d("99"), _d("1"))
        book.set_level("ask", _d("101"), _d("2"))
        self.assertEqual(book.best_bid(), (_d("99"), _d("1")))
        self.assertEqual(book.best_ask(), (_d("101"), _d("2")))
        book.set_level("bid", _d("99"), _d("0"))
        self.assertIsNone(book.best_bid())

    def test_truncate_retire_les_niveaux_eloignes(self) -> None:
        book = OrderBook()
        for i in range(5):
            book.set_level("bid", _d(90 + i), _d("1"))   # 90..94
            book.set_level("ask", _d(100 + i), _d("1"))  # 100..104
        book.truncate(3)
        self.assertEqual([p for p, _ in book.top_bids(9)], [_d(94), _d(93), _d(92)])
        self.assertEqual([p for p, _ in book.top_asks(9)], [_d(100), _d(101), _d(102)])

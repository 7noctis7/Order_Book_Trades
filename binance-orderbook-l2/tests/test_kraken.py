"""Tests Kraken : formatage checksum, snapshot/update, mismatch → resync."""
import json
import unittest
import zlib
from decimal import Decimal
from functools import partial

from lob.kraken import KrakenPairSync, _checksum_part, book_checksum
from lob.orderbook import OrderBook
from lob.sync import SyncState


def _d(v) -> Decimal:
    return Decimal(str(v))


def _loads(text: str) -> dict:
    return json.loads(text, parse_float=Decimal)


def _expected_crc(levels: list[tuple[str, str]]) -> int:
    """Checksum de référence calculé indépendamment depuis les chaînes du fil."""
    concat = "".join(
        p.replace(".", "").lstrip("0") + q.replace(".", "").lstrip("0")
        for p, q in levels
    )
    return zlib.crc32(concat.encode()) & 0xFFFFFFFF


class ChecksumFormatTests(unittest.TestCase):
    def test_format_selon_la_doc(self) -> None:
        # Exemples types de la spécification : point retiré, zéros de tête ôtés,
        # zéros de fin CONSERVÉS (précision telle que transmise).
        self.assertEqual(_checksum_part(_d("0.05005")), "5005")
        self.assertEqual(_checksum_part(_d("34.50000000")), "3450000000")
        self.assertEqual(_checksum_part(_d("45283.5")), "452835")
        self.assertEqual(_checksum_part(_d("1")), "1")

    def test_parse_float_decimal_preserve_la_precision_du_fil(self) -> None:
        data = _loads('{"price": 34.50000000, "qty": 0.05005}')
        self.assertEqual(_checksum_part(data["price"]), "3450000000")
        self.assertEqual(_checksum_part(data["qty"]), "5005")


class BookChecksumTests(unittest.TestCase):
    def _snapshot(self) -> dict:
        # Nombres JSON bruts, comme sur le fil v2 (jamais de chaînes) :
        # parse_float=Decimal doit préserver la précision transmise.
        asks = ",".join(
            f'{{"price": {100+i}.10, "qty": {i+1}.00}}' for i in range(12)
        )
        bids = ",".join(
            f'{{"price": {99-i}.10, "qty": {i+1}.50}}' for i in range(12)
        )
        return _loads(f'{{"symbol":"BTC/USD","asks":[{asks}],"bids":[{bids}]}}')

    def test_checksum_top10_asks_puis_bids(self) -> None:
        sync = KrakenPairSync("BTC/USD", OrderBook(), depth=12)
        data = self._snapshot()
        data["checksum"] = 0  # ignoré : calculé ci-dessous
        sync.on_snapshot({**data, "checksum": None})
        # référence indépendante : 10 asks croissants puis 10 bids décroissants
        asks = [(f"{100+i}.10", f"{i+1}.00") for i in range(10)]
        bids = [(f"{99-i}.10", f"{i+1}.50") for i in range(10)]
        self.assertEqual(book_checksum(sync.book), _expected_crc(asks + bids))

    def test_update_maintient_le_checksum(self) -> None:
        sync = KrakenPairSync("BTC/USD", OrderBook(), depth=12)
        data = self._snapshot()
        sync.on_snapshot({**data, "checksum": None})
        update = _loads(
            '{"symbol":"BTC/USD",'
            '"bids":[{"price": 99.10, "qty": 9.99},'    # remplacement
            '        {"price": 98.10, "qty": 0.00}],'   # suppression
            '"asks":[{"price": 100.05, "qty": 1.23}],'  # insertion devant
            '"checksum": null}'
        )
        ok = sync.on_update(update, recv_ms=0)
        self.assertTrue(ok)
        self.assertEqual(sync.book.best_ask(), (_d("100.05"), _d("1.23")))
        self.assertEqual(sync.book.best_bid(), (_d("99.10"), _d("9.99")))
        # cohérence : checksum calculé == checksum attendu envoyé ensuite
        update2 = _loads('{"symbol":"BTC/USD","bids":[],"asks":[]}')
        update2["checksum"] = book_checksum(sync.book)
        self.assertTrue(sync.on_update(update2, recv_ms=0))
        self.assertEqual(sync.resync_count, 0)

    def test_troncature_a_la_profondeur(self) -> None:
        sync = KrakenPairSync("BTC/USD", OrderBook(), depth=10)
        sync.on_snapshot({**self._snapshot(), "checksum": None})
        self.assertEqual(sync.book.depth(), (10, 10))

    def test_checksum_invalide_declenche_resync(self) -> None:
        sync = KrakenPairSync("BTC/USD", OrderBook(), depth=12)
        sync.on_snapshot({**self._snapshot(), "checksum": None})
        bad = _loads('{"symbol":"BTC/USD","bids":[],"asks":[]}')
        bad["checksum"] = 12345  # forcément faux
        self.assertFalse(sync.on_update(bad, recv_ms=0))
        self.assertEqual(sync.resync_count, 1)
        self.assertIs(sync.state, SyncState.BUFFERING)
        self.assertFalse(sync.book.is_ready)


if __name__ == "__main__":
    unittest.main()

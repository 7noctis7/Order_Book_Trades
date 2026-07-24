"""Structure locale du carnet L2 : bids/asks triés par prix.

``sortedcontainers.SortedDict`` garantit insertion/suppression en O(log n)
avec itération triée native — quantité nulle = suppression du niveau,
conformément au protocole depth de Binance.
"""
from __future__ import annotations

from decimal import Decimal
from itertools import islice

from sortedcontainers import SortedDict

from .exchange import DepthEvent, PriceLevel, Snapshot


class OrderBook:
    def __init__(self) -> None:
        self._bids: SortedDict = SortedDict()  # prix croissants ; best bid = dernier
        self._asks: SortedDict = SortedDict()  # prix croissants ; best ask = premier
        self.last_update_id: int | None = None

    # ------------------------------------------------------------------ écriture

    def load_snapshot(self, snapshot: Snapshot) -> None:
        self.clear()
        for price, qty in snapshot.bids:
            if qty > 0:
                self._bids[price] = qty
        for price, qty in snapshot.asks:
            if qty > 0:
                self._asks[price] = qty
        self.last_update_id = snapshot.last_update_id

    def apply(self, event: DepthEvent) -> None:
        self._apply_side(self._bids, event.bids)
        self._apply_side(self._asks, event.asks)
        self.last_update_id = event.final_update_id

    @staticmethod
    def _apply_side(side: SortedDict, levels: list[PriceLevel]) -> None:
        for price, qty in levels:
            if qty == 0:
                side.pop(price, None)  # quantité nulle = niveau supprimé
            else:
                side[price] = qty

    def set_level(self, side: str, price: Decimal, qty: Decimal) -> None:
        """Pose/remplace/supprime un niveau (qty 0 = suppression).

        Point d'entrée générique pour les protocoles qui envoient des niveaux
        absolus hors DepthEvent Binance (ex. canal book de Kraken)."""
        book_side = self._bids if side == "bid" else self._asks
        if qty == 0:
            book_side.pop(price, None)
        else:
            book_side[price] = qty

    def truncate(self, depth: int) -> None:
        """Tronque chaque côté à ``depth`` niveaux autour du milieu.

        Requis par les flux à profondeur fixe (Kraken) : une mise à jour peut
        faire déborder le carnet ; le client doit retirer les niveaux les plus
        éloignés avant tout calcul de checksum."""
        while len(self._bids) > depth:
            self._bids.popitem(0)    # bid le plus bas
        while len(self._asks) > depth:
            self._asks.popitem(-1)   # ask le plus haut

    def clear(self) -> None:
        self._bids.clear()
        self._asks.clear()
        self.last_update_id = None

    # ------------------------------------------------------------------- lecture

    @property
    def is_ready(self) -> bool:
        return bool(self._bids) and bool(self._asks)

    def depth(self) -> tuple[int, int]:
        """Nombre de niveaux (bids, asks) actuellement en mémoire."""
        return len(self._bids), len(self._asks)

    def best_bid(self) -> PriceLevel | None:
        return self._bids.peekitem(-1) if self._bids else None

    def best_ask(self) -> PriceLevel | None:
        return self._asks.peekitem(0) if self._asks else None

    def spread(self) -> Decimal | None:
        bid, ask = self.best_bid(), self.best_ask()
        if bid is None or ask is None:
            return None
        return ask[0] - bid[0]

    def mid_price(self) -> Decimal | None:
        bid, ask = self.best_bid(), self.best_ask()
        if bid is None or ask is None:
            return None
        return (ask[0] + bid[0]) / 2

    def top_bids(self, n: int) -> list[PriceLevel]:
        """Meilleurs bids, prix décroissants."""
        return [
            (price, self._bids[price])
            for price in islice(self._bids.irange(reverse=True), n)
        ]

    def top_asks(self, n: int) -> list[PriceLevel]:
        """Meilleurs asks, prix croissants."""
        return [
            (price, self._asks[price])
            for price in islice(self._asks.irange(), n)
        ]

    def cumulative_depth(self, n: int) -> tuple[Decimal, Decimal]:
        """Volume cumulé (bids, asks) sur les n meilleurs niveaux."""
        bid_vol = sum((qty for _, qty in self.top_bids(n)), Decimal(0))
        ask_vol = sum((qty for _, qty in self.top_asks(n)), Decimal(0))
        return bid_vol, ask_vol

    def imbalance(self, n: int) -> float | None:
        """Part du volume bid dans le volume total des n meilleurs niveaux.

        Retourne un ratio dans [0, 1] (0.5 = équilibre), ou None si vide.
        """
        bid_vol, ask_vol = self.cumulative_depth(n)
        total = bid_vol + ask_vol
        if total <= 0:
            return None
        return float(bid_vol / total)

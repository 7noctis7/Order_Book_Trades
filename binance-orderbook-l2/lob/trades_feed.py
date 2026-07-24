"""Bande des transactions réelles (flux @trade) : last price, VWAP, volume.

Complète le carnet L2 : le carnet montre l'intention (ordres posés), la bande
montre la réalité (transactions exécutées). Utilisée pour :
- afficher last price et VWAP glissant dans l'UI ;
- déclencher les ordres LIMIT de façon plus réaliste (un ordre limite est
  considéré exécuté quand une transaction réelle s'imprime à son prix).

Structure pure (aucune I/O), alimentée par le client WebSocket en live et
par le moteur de rejeu en backtest.
"""
from __future__ import annotations

from collections import deque
from decimal import Decimal


class TradeTape:
    def __init__(self, window_seconds: int = 60, maxlen: int = 6000) -> None:
        self._window_ms = window_seconds * 1000
        # (ts_ms, prix, quantité, acheteur_est_maker)
        self.trades: deque[tuple[int, Decimal, Decimal, bool]] = deque(maxlen=maxlen)

    def add(self, ts_ms: int, price: Decimal, qty: Decimal, buyer_is_maker: bool) -> None:
        self.trades.append((ts_ms, price, qty, buyer_is_maker))

    @property
    def last(self) -> Decimal | None:
        return self.trades[-1][1] if self.trades else None

    def _window(self, now_ms: int):
        cutoff = now_ms - self._window_ms
        return [t for t in self.trades if t[0] >= cutoff]

    def vwap(self, now_ms: int) -> Decimal | None:
        """VWAP sur la fenêtre glissante (60 s par défaut)."""
        window = self._window(now_ms)
        volume = sum((qty for _, _, qty, _ in window), Decimal(0))
        if volume == 0:
            return None
        return sum((p * q for _, p, q, _ in window), Decimal(0)) / volume

    def volume(self, now_ms: int) -> Decimal:
        return sum((qty for _, _, qty, _ in self._window(now_ms)), Decimal(0))

    def buy_ratio(self, now_ms: int) -> float | None:
        """Part du volume initiée à l'achat (taker achat = maker vendeur)."""
        window = self._window(now_ms)
        total = sum((q for _, _, q, _ in window), Decimal(0))
        if total == 0:
            return None
        buys = sum((q for _, _, q, m in window if not m), Decimal(0))
        return float(buys / total)

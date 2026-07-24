"""Échantillonnage de l'historique de prix (mid) pour le graphique console.

Une tâche unique échantillonne toutes les paires à cadence fixe : l'historique
se construit même pour les paires hors focus, indépendamment du rythme de
rafraîchissement de l'UI.
"""
from __future__ import annotations

import asyncio
import time
from collections import deque


class PriceHistory:
    """Série (timestamp, mid) bornée en mémoire (~1 h à 1 échantillon/s)."""

    def __init__(self, maxlen: int = 3600) -> None:
        self.samples: deque[tuple[float, float]] = deque(maxlen=maxlen)

    def append(self, ts: float, mid: float) -> None:
        self.samples.append((ts, mid))


async def run_sampler(
    pairs, stop_event: asyncio.Event, period: float = 1.0, paper=None, equity=None
) -> None:
    """Échantillonne le mid de chaque paire prête — et l'equity du
    portefeuille fictif si fournie — toutes les ``period`` s."""
    while not stop_event.is_set():
        now = time.time()
        mids = {}
        for pair in pairs:
            if pair.book.is_ready:
                mid = pair.book.mid_price()
                if mid is not None:
                    pair.history.append(now, float(mid))
                    mids[pair.symbol] = mid
        if paper is not None and equity is not None:
            equity.append(now, float(paper.summary(mids)["equity"]))
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=period)
        except asyncio.TimeoutError:
            pass

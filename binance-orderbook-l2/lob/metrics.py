"""Métriques d'exploitation : latence événement→réception et débit.

La latence compare le timestamp Binance (champ ``E``) à l'heure de réception
locale. NB : cette mesure inclut l'écart d'horloge machine↔Binance — une
valeur négative ou aberrante signale une horloge locale non synchronisée (NTP).
"""
from __future__ import annotations

import time
from collections import deque


class LatencyTracker:
    def __init__(self, window: int = 2000) -> None:
        self._samples: deque[int] = deque(maxlen=window)
        self.last_ms: int | None = None

    def record(self, event_time_ms: int, recv_time_ms: int) -> int:
        delta = recv_time_ms - event_time_ms
        self.last_ms = delta
        self._samples.append(delta)
        return delta

    @property
    def count(self) -> int:
        return len(self._samples)

    def percentile(self, pct: float) -> int | None:
        if not self._samples:
            return None
        ordered = sorted(self._samples)
        idx = round(pct / 100 * (len(ordered) - 1))
        return ordered[min(len(ordered) - 1, max(0, idx))]

    @property
    def p50(self) -> int | None:
        return self.percentile(50)

    @property
    def p95(self) -> int | None:
        return self.percentile(95)


class RateTracker:
    """Messages par seconde sur fenêtre glissante."""

    def __init__(self, window_seconds: float = 5.0) -> None:
        self._window = window_seconds
        self._ticks: deque[float] = deque()

    def tick(self, now: float | None = None) -> None:
        now = time.monotonic() if now is None else now
        self._ticks.append(now)
        self._evict(now)

    def rate(self) -> float:
        now = time.monotonic()
        self._evict(now)
        if not self._ticks:
            return 0.0
        elapsed = max(now - self._ticks[0], 1e-9)
        # Fenêtre partiellement remplie au démarrage : diviser par le temps réel.
        return len(self._ticks) / min(max(elapsed, 1.0), self._window)

    def _evict(self, now: float) -> None:
        cutoff = now - self._window
        while self._ticks and self._ticks[0] < cutoff:
            self._ticks.popleft()

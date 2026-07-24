"""Statistiques de performance du paper trading.

Calculées à partir des trades clôturés (FIFO, PnL nets de frais) et de la
courbe d'equity échantillonnée : win rate, profit factor, espérance par
trade, meilleur/pire trade, drawdown maximal, durée moyenne de détention.

Fonctions pures (entrées → dict), donc identiques en live et en backtest,
et testables sans aucune dépendance.
"""
from __future__ import annotations

import csv
import time
from collections import deque
from decimal import Decimal
from pathlib import Path
from typing import Sequence

from .paper import PaperEngine


class EquityHistory:
    """Courbe d'equity (ts, valeur) bornée en mémoire, base du drawdown."""

    def __init__(self, maxlen: int = 14_400) -> None:
        self.samples: deque[tuple[float, float]] = deque(maxlen=maxlen)

    def append(self, ts: float, equity: float) -> None:
        self.samples.append((ts, equity))


def max_drawdown(equity: Sequence[float]) -> tuple[float, float]:
    """(perte maximale depuis un plus-haut, en valeur ; en % du plus-haut)."""
    peak = float("-inf")
    worst = 0.0
    worst_pct = 0.0
    for value in equity:
        peak = max(peak, value)
        draw = peak - value
        if draw > worst:
            worst = draw
            worst_pct = draw / peak * 100 if peak > 0 else 0.0
    return worst, worst_pct


def compute(engine: PaperEngine, equity: EquityHistory | None = None) -> dict:
    """Tableau de bord : agrégats sur les trades clôturés + courbe d'equity."""
    pnls = [trade.realized for trade in engine.closed]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    gross_profit = sum(wins, Decimal(0))
    gross_loss = -sum(losses, Decimal(0))
    count = len(pnls)
    holding_ms = [t.sell_ts_ms - t.buy_ts_ms for t in engine.closed]
    curve = [v for _, v in equity.samples] if equity else []
    dd, dd_pct = max_drawdown(curve) if curve else (0.0, 0.0)
    return {
        "trades": count,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": len(wins) / count * 100 if count else None,
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "profit_factor": (
            float(gross_profit / gross_loss) if gross_loss > 0
            else (float("inf") if gross_profit > 0 else None)
        ),
        "expectancy": (gross_profit - gross_loss) / count if count else None,
        "best": max(pnls) if pnls else None,
        "worst": min(pnls) if pnls else None,
        "avg_holding_s": sum(holding_ms) / len(holding_ms) / 1000 if holding_ms else None,
        "fees_total": engine.fees_total,
        "max_drawdown": dd,
        "max_drawdown_pct": dd_pct,
    }


def export_csv(
    engine: PaperEngine,
    mids: dict[str, Decimal],
    directory: str = "data",
    now: float | None = None,
) -> str:
    """Exporte lots clos + lots ouverts en CSV horodaté ; retourne le chemin."""
    Path(directory).mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d_%H%M%S", time.localtime(now))
    path = str(Path(directory) / f"trades_{stamp}.csv")
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "statut", "symbole", "date_achat", "date_vente", "quantite",
                "prix_achat", "prix_vente", "pnl_usdt", "pnl_pct",
            ]
        )

        def _dt(ts_ms: int) -> str:
            return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts_ms / 1000))

        for t in engine.closed:
            writer.writerow(
                [
                    "clos", t.symbol, _dt(t.buy_ts_ms), _dt(t.sell_ts_ms),
                    str(t.qty), str(t.buy_price), str(t.sell_price),
                    f"{t.realized:.8f}", f"{t.realized_pct:.4f}",
                ]
            )
        for symbol, lots in engine.lots.items():
            mid = mids.get(symbol)
            for lot in lots:
                writer.writerow(
                    [
                        "ouvert", symbol, _dt(lot.ts_ms), "", str(lot.qty),
                        str(lot.price), "",
                        f"{lot.unrealized(mid):.8f}" if mid is not None else "",
                        f"{lot.unrealized_pct(mid):.4f}" if mid is not None else "",
                    ]
                )
    return path

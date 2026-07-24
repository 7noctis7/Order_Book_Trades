"""Paper trading : ordres fictifs exécutés contre le carnet L2 reconstruit.

Réalisme avant tout :
- un ordre au marché « marche » le carnet niveau par niveau : le prix moyen
  d'exécution inclut le slippage réel de la quantité demandée ;
- des frais taker configurables (10 bp par défaut, comme Binance Spot)
  sont appliqués à chaque exécution ;
- position suivie en lots FIFO : chaque vente consomme les achats les plus
  anciens, ce qui donne un historique par lot avec date d'achat ET date de
  vente, PnL réalisé par lot, et PnL latent sur les lots encore ouverts ;
- ordres en attente : LIMIT (exécuté au prix limite quand le marché le
  traverse — ou qu'une transaction réelle s'y imprime) et STOP (déclenche
  un ordre au marché : stop-loss, take-profit, entrée en cassure) ;
- spot uniquement : pas de vente à découvert, achats limités par le cash.

Le PnL est calculé sur les prix *effectifs* (frais inclus) ; les prix
affichés sont les prix moyens d'exécution bruts.

Persistance SQLite : chaque exécution (fill) et chaque ordre en attente sont
enregistrés ; au démarrage, l'état complet (cash, lots, trades clôturés,
ordres ouverts) est reconstruit en rejouant les fills.

Horloge injectable (``clock`` → epoch ms) : en live, l'heure système ; en
backtest, l'heure simulée du rejeu — mêmes maths, mêmes chemins de code.
"""
from __future__ import annotations

import logging
import sqlite3
import time
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Callable

from .orderbook import OrderBook

log = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS fills (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ms   INTEGER NOT NULL,
    symbol  TEXT    NOT NULL,
    side    TEXT    NOT NULL CHECK (side IN ('BUY', 'SELL')),
    qty     TEXT    NOT NULL,   -- Decimal sérialisé en texte : exactitude
    price   TEXT    NOT NULL,   -- prix moyen d'exécution brut
    fee     TEXT    NOT NULL    -- frais en devise de cotation
);
CREATE TABLE IF NOT EXISTS orders (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ms   INTEGER NOT NULL,
    symbol  TEXT    NOT NULL,
    side    TEXT    NOT NULL CHECK (side IN ('BUY', 'SELL')),
    otype   TEXT    NOT NULL CHECK (otype IN ('LIMIT', 'STOP')),
    qty     TEXT    NOT NULL,
    price   TEXT    NOT NULL,
    status  TEXT    NOT NULL DEFAULT 'OPEN'
);
"""

OPEN, FILLED, CANCELLED, REJECTED = "OPEN", "FILLED", "CANCELLED", "REJECTED"


class PaperError(RuntimeError):
    """Ordre rejeté (liquidité, cash, position, carnet non prêt…)."""


@dataclass(frozen=True)
class FillReport:
    """Résultat d'un ordre exécuté (pour affichage immédiat)."""

    ts_ms: int
    symbol: str
    side: str
    qty: Decimal
    avg_price: Decimal
    notional: Decimal
    fee: Decimal
    slippage: Decimal        # écart prix moyen vs meilleur niveau
    levels_consumed: int


@dataclass
class PendingOrder:
    """Ordre en attente : LIMIT (prix limite) ou STOP (déclencheur marché)."""

    id: int
    ts_ms: int
    symbol: str
    side: str
    otype: str      # LIMIT / STOP
    qty: Decimal
    price: Decimal
    status: str = OPEN


@dataclass(frozen=True)
class TriggerEvent:
    """Résultat d'un déclenchement : exécution, rejet ou annulation."""

    order: PendingOrder
    report: FillReport | None
    reason: str | None = None


@dataclass
class Lot:
    """Lot d'achat encore (partiellement) ouvert."""

    ts_ms: int
    qty: Decimal
    price: Decimal       # prix d'achat moyen brut
    price_eff: Decimal   # prix effectif frais inclus (base du PnL)

    def unrealized(self, mid: Decimal) -> Decimal:
        return (mid - self.price_eff) * self.qty

    def unrealized_pct(self, mid: Decimal) -> Decimal:
        return (mid - self.price_eff) / self.price_eff * 100


@dataclass(frozen=True)
class ClosedTrade:
    """Aller-retour clôturé (une vente peut clore plusieurs lots)."""

    symbol: str
    qty: Decimal
    buy_ts_ms: int
    buy_price: Decimal
    buy_eff: Decimal
    sell_ts_ms: int
    sell_price: Decimal
    sell_eff: Decimal

    @property
    def realized(self) -> Decimal:
        return (self.sell_eff - self.buy_eff) * self.qty

    @property
    def realized_pct(self) -> Decimal:
        return (self.sell_eff - self.buy_eff) / self.buy_eff * 100


@dataclass(frozen=True)
class Fill:
    """Exécution mémorisée (marqueurs graphiques, stats, export)."""

    ts_ms: int
    symbol: str
    side: str
    qty: Decimal
    price: Decimal
    fee: Decimal


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


class PaperEngine:
    def __init__(
        self,
        db_path: str | None,
        initial_cash: Decimal,
        fee_bps: Decimal,
        clock: Callable[[], int] | None = None,
    ) -> None:
        self._db_path = db_path
        self._initial_cash = initial_cash
        self._fee_rate = fee_bps / Decimal(10_000)
        self._clock = clock or _now_ms
        self.cash = initial_cash
        self.lots: dict[str, deque[Lot]] = {}
        self.closed: list[ClosedTrade] = []
        self.fills: list[Fill] = []
        self.pending: list[PendingOrder] = []
        self._next_order_id = 1
        self._conn: sqlite3.Connection | None = None
        if db_path:
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(db_path)
            self._conn.executescript(_SCHEMA)
            self._conn.commit()
            self._load()

    # ------------------------------------------------------------ persistance

    def _load(self) -> None:
        assert self._conn is not None
        rows = self._conn.execute(
            "SELECT ts_ms, symbol, side, qty, price, fee FROM fills ORDER BY id"
        ).fetchall()
        for ts_ms, symbol, side, qty, price, fee in rows:
            self._replay(
                int(ts_ms), symbol, side, Decimal(qty), Decimal(price), Decimal(fee)
            )
        for oid, ts_ms, symbol, side, otype, qty, price, status in self._conn.execute(
            "SELECT id, ts_ms, symbol, side, otype, qty, price, status FROM orders"
            " ORDER BY id"
        ):
            self._next_order_id = max(self._next_order_id, int(oid) + 1)
            if status == OPEN:
                self.pending.append(
                    PendingOrder(
                        id=int(oid),
                        ts_ms=int(ts_ms),
                        symbol=symbol,
                        side=side,
                        otype=otype,
                        qty=Decimal(qty),
                        price=Decimal(price),
                    )
                )
        if rows or self.pending:
            log.info(
                "paper trading : %d exécutions rechargées, cash %.2f,"
                " %d trades clôturés, %d ordres en attente",
                len(rows),
                self.cash,
                len(self.closed),
                len(self.pending),
            )

    def _persist_fill(self, report: FillReport) -> None:
        if self._conn is None:
            return
        self._conn.execute(
            "INSERT INTO fills (ts_ms, symbol, side, qty, price, fee)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (
                report.ts_ms,
                report.symbol,
                report.side,
                str(report.qty),
                str(report.avg_price),
                str(report.fee),
            ),
        )
        self._conn.commit()

    def _persist_order(self, order: PendingOrder) -> None:
        if self._conn is None:
            return
        self._conn.execute(
            "INSERT INTO orders (id, ts_ms, symbol, side, otype, qty, price, status)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                order.id,
                order.ts_ms,
                order.symbol,
                order.side,
                order.otype,
                str(order.qty),
                str(order.price),
                order.status,
            ),
        )
        self._conn.commit()

    def _update_order(self, order: PendingOrder) -> None:
        if self._conn is None:
            return
        self._conn.execute(
            "UPDATE orders SET status = ? WHERE id = ?", (order.status, order.id)
        )
        self._conn.commit()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # ------------------------------------------------------------- exécution

    def market_order(
        self, symbol: str, side: str, qty: Decimal, book: OrderBook
    ) -> FillReport:
        """Exécute un ordre au marché fictif contre l'état actuel du carnet."""
        if qty <= 0:
            raise PaperError("quantité invalide (doit être > 0)")
        if not book.is_ready:
            raise PaperError("carnet non synchronisé : ordre refusé")
        levels = book.top_asks(100_000) if side == "BUY" else book.top_bids(100_000)
        if not levels:
            raise PaperError("carnet vide côté opposé : ordre refusé")
        avg_price, consumed = self._walk(levels, qty)
        return self._execute(
            symbol, side, qty, avg_price, slippage=abs(avg_price - levels[0][0]),
            levels_consumed=consumed,
        )

    def _execute(
        self,
        symbol: str,
        side: str,
        qty: Decimal,
        price: Decimal,
        slippage: Decimal = Decimal(0),
        levels_consumed: int = 0,
    ) -> FillReport:
        """Contrôles cash/position puis application + persistance d'un fill."""
        notional = price * qty
        fee = notional * self._fee_rate
        if side == "BUY" and notional + fee > self.cash:
            raise PaperError(
                f"cash insuffisant : coût {notional + fee:,.2f} > solde {self.cash:,.2f}"
            )
        if side == "SELL" and qty > self.position_qty(symbol):
            raise PaperError(
                f"position insuffisante : {self.position_qty(symbol)} disponible"
                " (pas de vente à découvert en spot)"
            )
        report = FillReport(
            ts_ms=self._clock(),
            symbol=symbol,
            side=side,
            qty=qty,
            avg_price=price,
            notional=notional,
            fee=fee,
            slippage=slippage,
            levels_consumed=levels_consumed,
        )
        self._replay(report.ts_ms, symbol, side, qty, price, fee)
        self._persist_fill(report)
        return report

    @staticmethod
    def _walk(
        levels: list[tuple[Decimal, Decimal]], qty: Decimal
    ) -> tuple[Decimal, int]:
        """Prix moyen d'exécution en consommant le carnet niveau par niveau."""
        remaining = qty
        cost = Decimal(0)
        consumed = 0
        for price, available in levels:
            take = min(remaining, available)
            cost += price * take
            remaining -= take
            consumed += 1
            if remaining == 0:
                return cost / qty, consumed
        raise PaperError(
            f"liquidité insuffisante dans le carnet : {qty - remaining} disponible"
            f" sur {qty} demandé"
        )

    # ------------------------------------------------------ ordres en attente

    def place_pending(
        self, symbol: str, side: str, otype: str, qty: Decimal, price: Decimal
    ) -> PendingOrder:
        """Pose un ordre LIMIT ou STOP. Les contrôles cash/position sont faits
        au déclenchement (le contexte peut changer d'ici là) — documenté."""
        if qty <= 0 or price <= 0:
            raise PaperError("quantité et prix doivent être > 0")
        if otype not in ("LIMIT", "STOP"):
            raise PaperError(f"type d'ordre inconnu : {otype}")
        order = PendingOrder(
            id=self._next_order_id,
            ts_ms=self._clock(),
            symbol=symbol,
            side=side,
            otype=otype,
            qty=qty,
            price=price,
        )
        self._next_order_id += 1
        self.pending.append(order)
        self._persist_order(order)
        return order

    def cancel_pending(self, order_id: int) -> PendingOrder:
        for order in self.pending:
            if order.id == order_id:
                order.status = CANCELLED
                self.pending.remove(order)
                self._update_order(order)
                return order
        raise PaperError(f"ordre #{order_id} introuvable ou déjà clôturé")

    def check_triggers(
        self, symbol: str, book: OrderBook, last_trade: Decimal | None = None
    ) -> list[TriggerEvent]:
        """Évalue les ordres en attente d'une paire contre le carnet (et la
        dernière transaction réelle si disponible). Déterministe, sans I/O
        réseau : appelable en live comme en backtest.

        Déclenchements :
        - LIMIT BUY  : best_ask <= limite (ou trade imprimé <= limite) →
          exécuté AU PRIX LIMITE (hypothèse maker, pas de slippage) ;
        - LIMIT SELL : best_bid >= limite (ou trade >= limite) → idem ;
        - STOP  BUY  : best_ask >= stop (ou trade >= stop) → ordre au marché ;
        - STOP  SELL : best_bid <= stop (ou trade <= stop) → ordre au marché
          (stop-loss). Quantité plafonnée à la position restante.
        """
        if not book.is_ready:
            return []
        best_bid, best_ask = book.best_bid(), book.best_ask()
        if best_bid is None or best_ask is None:
            return []
        events: list[TriggerEvent] = []
        for order in [o for o in self.pending if o.symbol == symbol]:
            triggered = self._is_triggered(order, best_bid[0], best_ask[0], last_trade)
            if not triggered:
                continue
            events.append(self._fire(order, book))
        return events

    @staticmethod
    def _is_triggered(
        order: PendingOrder,
        best_bid: Decimal,
        best_ask: Decimal,
        last_trade: Decimal | None,
    ) -> bool:
        p = order.price
        if order.otype == "LIMIT":
            if order.side == "BUY":
                return best_ask <= p or (last_trade is not None and last_trade <= p)
            return best_bid >= p or (last_trade is not None and last_trade >= p)
        # STOP
        if order.side == "BUY":
            return best_ask >= p or (last_trade is not None and last_trade >= p)
        return best_bid <= p or (last_trade is not None and last_trade <= p)

    def _fire(self, order: PendingOrder, book: OrderBook) -> TriggerEvent:
        qty = order.qty
        if order.side == "SELL":
            available = self.position_qty(order.symbol)
            if available <= 0:
                order.status = CANCELLED
                self.pending.remove(order)
                self._update_order(order)
                return TriggerEvent(order, None, "position déjà clôturée")
            qty = min(qty, available)
        try:
            if order.otype == "LIMIT":
                report = self._execute(order.symbol, order.side, qty, order.price)
            else:  # STOP → ordre au marché (slippage réel)
                report = self.market_order(order.symbol, order.side, qty, book)
        except PaperError as exc:
            order.status = REJECTED
            self.pending.remove(order)
            self._update_order(order)
            return TriggerEvent(order, None, str(exc))
        order.status = FILLED
        self.pending.remove(order)
        self._update_order(order)
        return TriggerEvent(order, report, None)

    # ------------------------------------------------------ état du portefeuille

    def _replay(
        self,
        ts_ms: int,
        symbol: str,
        side: str,
        qty: Decimal,
        price: Decimal,
        fee: Decimal,
    ) -> None:
        """Applique une exécution à l'état (utilisé en live et au rechargement)."""
        self.fills.append(Fill(ts_ms, symbol, side, qty, price, fee))
        notional = price * qty
        if side == "BUY":
            self.cash -= notional + fee
            eff = (notional + fee) / qty
            self.lots.setdefault(symbol, deque()).append(
                Lot(ts_ms=ts_ms, qty=qty, price=price, price_eff=eff)
            )
            return

        # SELL : consommation FIFO des lots.
        self.cash += notional - fee
        sell_eff = (notional - fee) / qty
        remaining = qty
        queue = self.lots.setdefault(symbol, deque())
        while remaining > 0 and queue:
            lot = queue[0]
            take = min(remaining, lot.qty)
            self.closed.append(
                ClosedTrade(
                    symbol=symbol,
                    qty=take,
                    buy_ts_ms=lot.ts_ms,
                    buy_price=lot.price,
                    buy_eff=lot.price_eff,
                    sell_ts_ms=ts_ms,
                    sell_price=price,
                    sell_eff=sell_eff,
                )
            )
            lot.qty -= take
            remaining -= take
            if lot.qty == 0:
                queue.popleft()
        # remaining > 0 impossible : garanti par le contrôle de position amont.

    @property
    def db_path(self) -> str | None:
        return self._db_path

    @property
    def fee_rate(self) -> Decimal:
        return self._fee_rate

    @property
    def initial_cash(self) -> Decimal:
        return self._initial_cash

    def fills_for(self, symbol: str, limit: int = 200) -> list[tuple[int, str]]:
        """Dernières exécutions d'une paire : (ts_ms, side), pour le graphique."""
        return [(f.ts_ms, f.side) for f in self.fills if f.symbol == symbol][-limit:]

    def position_qty(self, symbol: str) -> Decimal:
        return sum((lot.qty for lot in self.lots.get(symbol, ())), Decimal(0))

    def position_avg_price(self, symbol: str) -> Decimal | None:
        lots = self.lots.get(symbol)
        if not lots:
            return None
        qty = sum((lot.qty for lot in lots), Decimal(0))
        cost = sum((lot.price_eff * lot.qty for lot in lots), Decimal(0))
        return cost / qty if qty else None

    def unrealized(self, symbol: str, mid: Decimal) -> Decimal:
        return sum(
            (lot.unrealized(mid) for lot in self.lots.get(symbol, ())), Decimal(0)
        )

    @property
    def realized_total(self) -> Decimal:
        return sum((trade.realized for trade in self.closed), Decimal(0))

    @property
    def fees_total(self) -> Decimal:
        return sum((f.fee for f in self.fills), Decimal(0))

    def summary(self, mids: dict[str, Decimal]) -> dict:
        """Vue portefeuille : cash, valeur des positions, equity, PnL."""
        positions_value = Decimal(0)
        unrealized = Decimal(0)
        cost_basis = Decimal(0)
        for symbol, queue in self.lots.items():
            mid = mids.get(symbol)
            for lot in queue:
                cost_basis += lot.price_eff * lot.qty
                if mid is not None:
                    positions_value += mid * lot.qty
                    unrealized += lot.unrealized(mid)
        return {
            "cash": self.cash,
            "positions_value": positions_value,
            "equity": self.cash + positions_value,
            "unrealized": unrealized,
            "unrealized_pct": (
                unrealized / cost_basis * 100 if cost_basis else Decimal(0)
            ),
            "realized": self.realized_total,
            "realized_pct": (
                self.realized_total / self._initial_cash * 100
                if self._initial_cash
                else Decimal(0)
            ),
            "initial_cash": self._initial_cash,
        }

"""Implémentation Binance Spot de l'interface ``ExchangeClient``.

Flux public en lecture seule : aucune clé API nécessaire.
"""
from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation

import requests

from .config import AppConfig
from .exchange import DepthEvent, ExchangeClient, PriceLevel, Snapshot, SnapshotError

log = logging.getLogger(__name__)


def _levels(raw: list) -> list[PriceLevel]:
    """Convertit [["prix","qté"], ...] (chaînes Binance) en Decimal exacts."""
    return [(Decimal(price), Decimal(qty)) for price, qty, *_ in raw]


class BinanceClient(ExchangeClient):
    def __init__(self, cfg: AppConfig, symbol: str) -> None:
        self._symbol = symbol
        self._depth_limit = cfg.depth_limit
        self._speed_ms = cfg.ws_speed_ms
        self._timeout = cfg.network.rest_timeout_seconds
        self._rest_base = cfg.binance.rest_base
        self._ws_base = cfg.binance.ws_base

    @property
    def name(self) -> str:
        return "binance"

    @property
    def symbol(self) -> str:
        return self._symbol

    def ws_url(self) -> str:
        # Stream combiné : profondeur (<sym>@depth[@100ms]) + transactions
        # réelles (<sym>@trade) sur une seule connexion. Les messages arrivent
        # enveloppés : {"stream": "...", "data": {...}}.
        depth = f"{self._symbol.lower()}@depth"
        if self._speed_ms == 100:
            depth += "@100ms"
        trade = f"{self._symbol.lower()}@trade"
        return f"{self._ws_base}/stream?streams={depth}/{trade}"

    def fetch_snapshot(self) -> Snapshot:
        url = f"{self._rest_base}/api/v3/depth"
        params = {"symbol": self._symbol, "limit": self._depth_limit}
        try:
            resp = requests.get(url, params=params, timeout=self._timeout)
        except requests.RequestException as exc:
            raise SnapshotError(f"appel REST snapshot impossible : {exc}") from exc
        if resp.status_code != 200:
            # 400 = requête invalide (ex. code -1121 "Invalid symbol") :
            # erreur permanente, réessayer en boucle serait inutile.
            # 429/418 = rate limit / ban temporaire : transitoire.
            permanent = resp.status_code == 400
            raise SnapshotError(
                f"snapshot HTTP {resp.status_code} : {resp.text[:200]}",
                permanent=permanent,
            )
        try:
            data = resp.json()
            return Snapshot(
                last_update_id=int(data["lastUpdateId"]),
                bids=_levels(data["bids"]),
                asks=_levels(data["asks"]),
                raw=resp.text,
            )
        except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
            raise SnapshotError(f"réponse snapshot invalide : {exc}") from exc

    @staticmethod
    def parse_trade(payload: dict) -> tuple[int, Decimal, Decimal, bool] | None:
        """Message @trade → (ts_ms, prix, qté, acheteur_est_maker) ou None."""
        if payload.get("e") != "trade":
            return None
        try:
            return (
                int(payload["T"]),
                Decimal(payload["p"]),
                Decimal(payload["q"]),
                bool(payload["m"]),
            )
        except (KeyError, ValueError, ArithmeticError):
            return None

    def parse_event(self, payload: dict) -> DepthEvent | None:
        if payload.get("e") != "depthUpdate":
            return None
        try:
            return DepthEvent(
                first_update_id=int(payload["U"]),
                final_update_id=int(payload["u"]),
                event_time_ms=int(payload["E"]),
                bids=_levels(payload.get("b", [])),
                asks=_levels(payload.get("a", [])),
            )
        except (KeyError, TypeError, ValueError, InvalidOperation):
            log.warning("depthUpdate malformé ignoré : %s", str(payload)[:200])
            return None

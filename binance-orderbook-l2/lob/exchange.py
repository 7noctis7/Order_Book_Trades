"""Interface abstraite d'un connecteur exchange (flux depth L2).

Un seul exchange est implémenté aujourd'hui (Binance), mais toute la
logique aval (order book, synchronisation, capture) ne dépend que de
cette interface : brancher un autre exchange = écrire une nouvelle
implémentation de ``ExchangeClient``, sans toucher au reste.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from decimal import Decimal

# (prix, quantité) — Decimal pour des comparaisons de prix exactes
PriceLevel = tuple[Decimal, Decimal]


@dataclass(frozen=True)
class Snapshot:
    """Image complète du carnet à un instant donné (REST)."""

    last_update_id: int
    bids: list[PriceLevel]
    asks: list[PriceLevel]
    raw: str = field(default="", repr=False)  # JSON brut, pour la capture


@dataclass(frozen=True)
class DepthEvent:
    """Delta incrémental du carnet (WebSocket)."""

    first_update_id: int  # champ U
    final_update_id: int  # champ u
    event_time_ms: int    # champ E (epoch ms, horloge Binance)
    bids: list[PriceLevel]
    asks: list[PriceLevel]


class SnapshotError(RuntimeError):
    """Échec de récupération ou de parsing du snapshot REST.

    ``permanent=True`` signale une erreur non transitoire (ex. symbole
    inconnu de l'exchange) : inutile de réessayer en boucle.
    """

    def __init__(self, message: str, permanent: bool = False) -> None:
        super().__init__(message)
        self.permanent = permanent


class ExchangeClient(ABC):
    """Contrat minimal pour alimenter la reconstruction d'un carnet L2."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Nom de l'exchange (ex : 'binance')."""

    @property
    @abstractmethod
    def symbol(self) -> str:
        """Symbole suivi, normalisé (ex : 'BTCUSDT')."""

    @abstractmethod
    def ws_url(self) -> str:
        """URL du flux WebSocket depth pour ce symbole."""

    @abstractmethod
    def fetch_snapshot(self) -> Snapshot:
        """Récupère un snapshot du carnet via REST.

        Appel bloquant : à exécuter via ``asyncio.to_thread``.
        Lève ``SnapshotError`` en cas d'échec réseau ou de réponse invalide.
        """

    @abstractmethod
    def parse_event(self, payload: dict) -> DepthEvent | None:
        """Convertit un payload WebSocket en ``DepthEvent``.

        Retourne ``None`` si le payload n'est pas un depth update
        (ping applicatif, message de souscription, etc.).
        """

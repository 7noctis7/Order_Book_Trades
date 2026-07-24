"""Vérification de continuité des depth updates (protocole Binance U/u).

Règle appliquée (doc Binance « How to manage a local order book correctly ») :

1. ignorer tout événement dont ``u <= lastUpdateId`` du snapshot (périmé) ;
2. le premier événement appliqué doit vérifier ``U <= lastUpdateId + 1 <= u`` ;
3. chaque événement suivant doit vérifier ``U == u_précédent + 1`` exactement ;
4. toute rupture — trou ou chevauchement — invalide le carnet local et impose
   une resynchronisation complète (nouveau snapshot + rebufferisation).

Module volontairement pur (stdlib uniquement) pour être testé isolément.
"""
from __future__ import annotations

from enum import Enum, auto


class SeqResult(Enum):
    DISCARD_STALE = auto()   # antérieur au snapshot : à ignorer silencieusement
    ACCEPT_FIRST = auto()    # premier événement valide après le snapshot
    ACCEPT = auto()          # continuité respectée (U == u_précédent + 1)
    RESYNC_GAP = auto()      # trou de séquence : des deltas ont été manqués
    RESYNC_OVERLAP = auto()  # chevauchement / retour en arrière de séquence
    MALFORMED = auto()       # u < U : événement incohérent


#: Résultats qui imposent le rejet complet du carnet local.
RESYNC_RESULTS = frozenset(
    {SeqResult.RESYNC_GAP, SeqResult.RESYNC_OVERLAP, SeqResult.MALFORMED}
)


class SequenceValidator:
    """Machine de décision pure : ``evaluate()`` ne modifie jamais l'état.

    L'avancement de séquence n'a lieu qu'au ``commit()`` explicite, appelé
    par l'orchestrateur après application effective de l'événement au carnet.
    Cette séparation évaluation/commit rend chaque décision rejouable en test.
    """

    def __init__(self, snapshot_last_update_id: int) -> None:
        self.snapshot_last_update_id = snapshot_last_update_id
        self.last_final_id: int | None = None

    @property
    def synced(self) -> bool:
        """Vrai dès qu'un premier événement a été accepté et commité."""
        return self.last_final_id is not None

    @property
    def expected_first_id(self) -> int:
        """Prochain update ID attendu (pour les messages de diagnostic)."""
        if self.last_final_id is None:
            return self.snapshot_last_update_id + 1
        return self.last_final_id + 1

    def evaluate(self, first_update_id: int, final_update_id: int) -> SeqResult:
        u_first, u_final = first_update_id, final_update_id
        if u_final < u_first:
            return SeqResult.MALFORMED

        if self.last_final_id is None:
            # Phase post-snapshot : purge des périmés puis validation du premier.
            if u_final <= self.snapshot_last_update_id:
                return SeqResult.DISCARD_STALE
            if u_first <= self.snapshot_last_update_id + 1 <= u_final:
                return SeqResult.ACCEPT_FIRST
            # u_first > lastUpdateId + 1 : deltas manqués entre snapshot et flux.
            return SeqResult.RESYNC_GAP

        expected = self.last_final_id + 1
        if u_first == expected:
            return SeqResult.ACCEPT
        if u_first > expected:
            return SeqResult.RESYNC_GAP
        return SeqResult.RESYNC_OVERLAP

    def commit(self, final_update_id: int) -> None:
        """Avance la séquence après application effective d'un événement."""
        self.last_final_id = final_update_id

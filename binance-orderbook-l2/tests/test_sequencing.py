"""Tests de la règle de synchronisation U/u (protocole Binance).

Couvre les quatre familles de cas demandées : cas normal, trou de séquence,
chevauchement, événements périmés — plus les cas limites aux frontières.
"""
import unittest

from lob.sequencing import RESYNC_RESULTS, SeqResult, SequenceValidator

SNAPSHOT_ID = 1000


class SequenceValidatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.validator = SequenceValidator(SNAPSHOT_ID)

    def _sync_to(self, first: int, final: int) -> None:
        """Amène le validateur en état STREAMING avec un premier événement."""
        assert self.validator.evaluate(first, final) is SeqResult.ACCEPT_FIRST
        self.validator.commit(final)

    # ---------------------------------------------------- événements périmés

    def test_evenement_perime_u_egal_lastUpdateId(self) -> None:
        self.assertIs(self.validator.evaluate(990, SNAPSHOT_ID), SeqResult.DISCARD_STALE)
        self.assertFalse(self.validator.synced)

    def test_evenement_perime_u_inferieur(self) -> None:
        self.assertIs(self.validator.evaluate(980, 995), SeqResult.DISCARD_STALE)

    # -------------------------------------------------------- premier événement

    def test_premier_evenement_chevauchant_le_snapshot(self) -> None:
        # U <= lastUpdateId + 1 <= u
        self.assertIs(self.validator.evaluate(995, 1005), SeqResult.ACCEPT_FIRST)

    def test_premier_evenement_cas_limite_exact(self) -> None:
        # U == lastUpdateId + 1 == u : bornes incluses des deux côtés.
        self.assertIs(self.validator.evaluate(1001, 1001), SeqResult.ACCEPT_FIRST)

    def test_premier_evenement_trou(self) -> None:
        # U > lastUpdateId + 1 : deltas manqués entre snapshot et flux.
        self.assertIs(self.validator.evaluate(1002, 1010), SeqResult.RESYNC_GAP)

    # ------------------------------------------------------------- cas normal

    def test_continuite_normale(self) -> None:
        self._sync_to(995, 1005)
        self.assertIs(self.validator.evaluate(1006, 1010), SeqResult.ACCEPT)
        self.validator.commit(1010)
        self.assertIs(self.validator.evaluate(1011, 1011), SeqResult.ACCEPT)
        self.validator.commit(1011)
        self.assertEqual(self.validator.expected_first_id, 1012)

    # ------------------------------------------------------------------- trou

    def test_trou_en_streaming(self) -> None:
        self._sync_to(995, 1005)
        self.assertIs(self.validator.evaluate(1008, 1012), SeqResult.RESYNC_GAP)

    def test_trou_minimal_en_streaming(self) -> None:
        self._sync_to(995, 1005)
        # U == u_précédent + 2 : un seul ID manquant suffit à invalider.
        self.assertIs(self.validator.evaluate(1007, 1009), SeqResult.RESYNC_GAP)

    # ----------------------------------------------------------- chevauchement

    def test_chevauchement_en_streaming(self) -> None:
        self._sync_to(995, 1005)
        self.assertIs(self.validator.evaluate(1004, 1009), SeqResult.RESYNC_OVERLAP)

    def test_rejeu_du_meme_evenement(self) -> None:
        self._sync_to(995, 1005)
        # Doublon strict : chevauchement, jamais ré-appliqué silencieusement.
        self.assertIs(self.validator.evaluate(995, 1005), SeqResult.RESYNC_OVERLAP)

    # ------------------------------------------------------------- malformé

    def test_evenement_malforme_u_inferieur_a_U(self) -> None:
        self.assertIs(self.validator.evaluate(1010, 1005), SeqResult.MALFORMED)

    # -------------------------------------------------------------- invariants

    def test_evaluate_est_pur_sans_commit(self) -> None:
        self.validator.evaluate(995, 1005)
        # Sans commit(), l'état n'avance pas : la même décision est rejouable.
        self.assertIs(self.validator.evaluate(995, 1005), SeqResult.ACCEPT_FIRST)
        self.assertFalse(self.validator.synced)

    def test_resultats_de_resynchronisation(self) -> None:
        self.assertIn(SeqResult.RESYNC_GAP, RESYNC_RESULTS)
        self.assertIn(SeqResult.RESYNC_OVERLAP, RESYNC_RESULTS)
        self.assertIn(SeqResult.MALFORMED, RESYNC_RESULTS)
        self.assertNotIn(SeqResult.ACCEPT, RESYNC_RESULTS)


if __name__ == "__main__":
    unittest.main()

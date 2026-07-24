"""Capture brute des messages dans SQLite (thread dédié + queue).

Choix assumés (validés) :
- SQLite plutôt que JSONL : requêtage SQL direct de l'historique ;
- un seul fichier ``.db`` qui grossit, pas de rotation ;
- CHAQUE message WebSocket brut est enregistré (pas des snapshots
  périodiques), sinon des deltas seraient perdus et l'historique ne
  serait pas rejouable ;
- les snapshots REST sont également enregistrés (``ts_event_ms`` NULL),
  afin que le flux capturé soit rejouable sans aucune source externe.

L'écriture est isolée dans un thread (SQLite bloquant ≠ boucle asyncio),
alimenté par une ``queue.Queue`` thread-safe, avec écriture par lots.
"""
from __future__ import annotations

import logging
import queue
import sqlite3
import threading
from pathlib import Path

log = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_recv_ms  INTEGER NOT NULL,   -- réception locale (epoch ms)
    ts_event_ms INTEGER,            -- champ E Binance (NULL pour un snapshot REST)
    symbol      TEXT    NOT NULL,
    payload     TEXT    NOT NULL    -- JSON brut tel que reçu
);
CREATE INDEX IF NOT EXISTS idx_messages_ts_recv ON messages (ts_recv_ms);
"""

_SENTINEL = object()


class SqliteCaptureWriter:
    def __init__(self, db_path: str, max_pending: int = 100_000) -> None:
        self._db_path = Path(db_path)
        self._queue: queue.Queue = queue.Queue(maxsize=max_pending)
        self._thread = threading.Thread(
            target=self._run, name="sqlite-capture", daemon=False
        )
        self._started = False
        self.written = 0
        self.dropped = 0

    @property
    def db_path(self) -> Path:
        return self._db_path

    def start(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._started = True
        self._thread.start()
        log.info("capture SQLite active → %s", self._db_path)

    def submit(
        self, ts_recv_ms: int, ts_event_ms: int | None, symbol: str, payload: str
    ) -> None:
        """Thread-safe et non bloquant : appelable depuis la boucle asyncio."""
        if not self._started:
            return
        try:
            self._queue.put_nowait((ts_recv_ms, ts_event_ms, symbol, payload))
        except queue.Full:
            self.dropped += 1
            if self.dropped % 1000 == 1:
                log.warning(
                    "file de capture saturée : %d messages perdus", self.dropped
                )

    def stop(self, timeout: float = 10.0) -> None:
        """Vide la file, flush le dernier lot, ferme la base, joint le thread."""
        if not self._started:
            return
        self._queue.put(_SENTINEL)
        self._thread.join(timeout=timeout)
        if self._thread.is_alive():
            log.error("le thread de capture ne s'est pas arrêté dans les temps")
        else:
            log.info(
                "capture SQLite arrêtée proprement (%d messages écrits, %d perdus)",
                self.written,
                self.dropped,
            )

    # ---------------------------------------------------------------- interne

    def _run(self) -> None:
        conn = sqlite3.connect(self._db_path)
        try:
            conn.executescript(_SCHEMA)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            stop = False
            while not stop:
                try:
                    item = self._queue.get(timeout=0.5)
                except queue.Empty:
                    continue
                if item is _SENTINEL:
                    break
                batch = [item]
                # Drainer sans bloquer : écrire par lots limite les commits.
                while True:
                    try:
                        item = self._queue.get_nowait()
                    except queue.Empty:
                        break
                    if item is _SENTINEL:
                        stop = True
                        break
                    batch.append(item)
                conn.executemany(
                    "INSERT INTO messages (ts_recv_ms, ts_event_ms, symbol, payload)"
                    " VALUES (?, ?, ?, ?)",
                    batch,
                )
                conn.commit()
                self.written += len(batch)
        except Exception:
            log.exception("erreur dans le thread de capture SQLite")
        finally:
            try:
                conn.commit()
            finally:
                conn.close()

"""Export Prometheus : exposition texte sur /metrics, zéro dépendance.

Le format d'exposition Prometheus (text/plain version 0.0.4) est trivial à
produire ; ``prometheus_client`` n'apporterait ici que du poids. Un
``ThreadingHTTPServer`` de la bibliothèque standard tourne dans un thread
démon et appelle à chaque scrape un collecteur fourni par l'application —
lecture seule d'états déjà maintenus : coût nul pour le moteur.

Exemple de scrape : ``curl http://127.0.0.1:9109/metrics``.
"""
from __future__ import annotations

import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable

log = logging.getLogger(__name__)


def escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


class PrometheusExporter:
    def __init__(self, host: str, port: int, collect: Callable[[], str]) -> None:
        self._collect = collect
        exporter = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802 (API http.server)
                if self.path.rstrip("/") not in ("", "/metrics"):
                    self.send_response(404)
                    self.end_headers()
                    return
                try:
                    body = exporter._collect().encode("utf-8")
                except Exception:  # le scrape ne doit jamais tuer le serveur
                    log.exception("échec du collecteur de métriques")
                    self.send_response(500)
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header(
                    "Content-Type", "text/plain; version=0.0.4; charset=utf-8"
                )
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args) -> None:  # silencieux (logs applicatifs)
                pass

        self._server = ThreadingHTTPServer((host, port), Handler)
        self._thread = threading.Thread(
            target=self._server.serve_forever, name="prometheus", daemon=True
        )
        self._host, self._port = host, port

    def start(self) -> None:
        self._thread.start()
        log.info("export Prometheus : http://%s:%d/metrics", self._host, self._port)

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()

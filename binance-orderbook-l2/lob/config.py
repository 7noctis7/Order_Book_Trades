"""Chargement et validation de la configuration YAML.

Toute la configuration de l'application passe par ici : aucun paramètre
métier n'est codé en dur ailleurs. Chaque valeur invalide produit une
``ConfigError`` avec un message actionnable.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

VALID_DEPTH_LIMITS = {5, 10, 20, 50, 100, 500, 1000, 5000}
VALID_WS_SPEEDS_MS = {100, 1000}
VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}


class ConfigError(ValueError):
    """Configuration invalide ou illisible."""


@dataclass(frozen=True)
class DisplayConfig:
    enabled: bool = True
    refresh_seconds: float = 1.0
    levels: int = 8
    price_decimals: int | None = None  # None = auto (déduit du tick de la paire)
    qty_decimals: int | None = None    # None = auto


@dataclass(frozen=True)
class CaptureConfig:
    enabled: bool = False
    db_path: str = "data/capture.db"


@dataclass(frozen=True)
class KrakenConfig:
    ws_base: str = "wss://ws.kraken.com/v2"
    depth: int = 100                 # 10 / 25 / 100 / 500 / 1000


@dataclass(frozen=True)
class PrometheusConfig:
    enabled: bool = False
    host: str = "127.0.0.1"
    port: int = 9109


@dataclass(frozen=True)
class PaperConfig:
    enabled: bool = True
    initial_cash_usdt: float = 10_000.0
    fee_bps: float = 10.0            # frais taker simulés (10 bp = 0,10 % Binance Spot)
    db_path: str = "data/paper.db"   # persistance des exécutions fictives


@dataclass(frozen=True)
class NetworkConfig:
    rest_timeout_seconds: float = 10.0
    backoff_base_seconds: float = 1.0
    backoff_max_seconds: float = 60.0
    buffer_max_messages: int = 20000


@dataclass(frozen=True)
class BinanceEndpoints:
    rest_base: str = "https://api.binance.com"
    ws_base: str = "wss://stream.binance.com:9443"


@dataclass(frozen=True)
class LoggingConfig:
    level: str = "INFO"
    file: str | None = "lob.log"


@dataclass(frozen=True)
class AppConfig:
    exchange: str
    symbols: tuple[str, ...]
    depth_limit: int
    ws_speed_ms: int
    display: DisplayConfig
    capture: CaptureConfig
    paper: PaperConfig
    kraken: KrakenConfig
    prometheus: PrometheusConfig
    network: NetworkConfig
    binance: BinanceEndpoints
    logging: LoggingConfig


def _section(data: dict, name: str) -> dict:
    value = data.get(name) or {}
    if not isinstance(value, dict):
        raise ConfigError(f"la section '{name}' doit être un mapping YAML")
    return value


def _positive(value: float, name: str) -> float:
    try:
        value = float(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"'{name}' doit être un nombre, reçu {value!r}") from exc
    if value <= 0:
        raise ConfigError(f"'{name}' doit être strictement positif, reçu {value}")
    return value


def _positive_int(value: int, name: str) -> int:
    try:
        ivalue = int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"'{name}' doit être un entier, reçu {value!r}") from exc
    if ivalue <= 0:
        raise ConfigError(f"'{name}' doit être strictement positif, reçu {ivalue}")
    return ivalue


def _decimals(value, name: str) -> int | None:
    """'auto'/None → None (déduction par paire) ; sinon entier >= 0."""
    if value is None or (isinstance(value, str) and value.strip().lower() == "auto"):
        return None
    try:
        ivalue = int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"'{name}' doit être un entier >= 0 ou 'auto', reçu {value!r}") from exc
    if ivalue < 0:
        raise ConfigError(f"'{name}' doit être >= 0, reçu {ivalue}")
    return ivalue


def load_config(path: str | Path) -> AppConfig:
    """Lit, valide et retourne la configuration applicative."""
    path = Path(path)
    if not path.exists():
        raise ConfigError(
            f"fichier de configuration introuvable : {path} "
            f"(copier config.example.yaml vers {path.name} ?)"
        )
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"YAML invalide dans {path} : {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"{path} doit contenir un mapping YAML à la racine")

    exchange = str(data.get("exchange", "binance")).strip().lower()

    exchange = str(data.get("exchange", "binance")).strip().lower()
    raw_symbols = data.get("symbols", data.get("symbol", "BTCUSDT"))
    if isinstance(raw_symbols, str):
        raw_symbols = [raw_symbols]
    if not isinstance(raw_symbols, list) or not raw_symbols:
        raise ConfigError("'symbols' doit être une liste non vide de paires")
    symbols: list[str] = []
    for item in raw_symbols:
        symbol = str(item).strip().upper()
        if exchange == "binance":
            symbol = symbol.replace("/", "")  # BTC/USDT → BTCUSDT

        if not symbol.replace("/", "").isalnum() or symbol.count("/") > 1:
            raise ConfigError(f"symbole invalide : {item!r}")
        if symbol not in symbols:  # déduplication, ordre préservé
            symbols.append(symbol)

    depth_limit = _positive_int(data.get("depth_limit", 100), "depth_limit")
    if depth_limit not in VALID_DEPTH_LIMITS:
        raise ConfigError(
            f"'depth_limit' doit être dans {sorted(VALID_DEPTH_LIMITS)}, reçu {depth_limit}"
        )

    ws_speed_ms = _positive_int(data.get("ws_speed_ms", 100), "ws_speed_ms")
    if ws_speed_ms not in VALID_WS_SPEEDS_MS:
        raise ConfigError(
            f"'ws_speed_ms' doit être dans {sorted(VALID_WS_SPEEDS_MS)}, reçu {ws_speed_ms}"
        )

    d = _section(data, "display")
    display = DisplayConfig(
        enabled=bool(d.get("enabled", True)),
        refresh_seconds=_positive(d.get("refresh_seconds", 1.0), "display.refresh_seconds"),
        levels=_positive_int(d.get("levels", 8), "display.levels"),
        price_decimals=_decimals(d.get("price_decimals", "auto"), "display.price_decimals"),
        qty_decimals=_decimals(d.get("qty_decimals", "auto"), "display.qty_decimals"),
    )

    c = _section(data, "capture")
    capture = CaptureConfig(
        enabled=bool(c.get("enabled", False)),
        db_path=str(c.get("db_path", "data/capture.db")),
    )
    if capture.enabled and not capture.db_path.strip():
        raise ConfigError("capture.db_path est requis lorsque capture.enabled=true")

    k = _section(data, "kraken")
    kraken = KrakenConfig(
        ws_base=str(k.get("ws_base", "wss://ws.kraken.com/v2")),
        depth=int(k.get("depth", 100)),
    )
    if kraken.depth not in (10, 25, 100, 500, 1000):
        raise ConfigError("kraken.depth doit être 10, 25, 100, 500 ou 1000")

    o = _section(data, "observability")
    prom_raw = o.get("prometheus", {}) if isinstance(o, dict) else {}
    prometheus = PrometheusConfig(
        enabled=bool(prom_raw.get("enabled", False)),
        host=str(prom_raw.get("host", "127.0.0.1")),
        port=int(prom_raw.get("port", 9109)),
    )

    p = _section(data, "paper")
    paper = PaperConfig(
        enabled=bool(p.get("enabled", True)),
        initial_cash_usdt=_positive(p.get("initial_cash_usdt", 10_000.0), "paper.initial_cash_usdt"),
        fee_bps=float(p.get("fee_bps", 10.0)),
        db_path=str(p.get("db_path", "data/paper.db")),
    )
    if paper.fee_bps < 0:
        raise ConfigError("paper.fee_bps doit être >= 0")

    n = _section(data, "network")
    network = NetworkConfig(
        rest_timeout_seconds=_positive(n.get("rest_timeout_seconds", 10.0), "network.rest_timeout_seconds"),
        backoff_base_seconds=_positive(n.get("backoff_base_seconds", 1.0), "network.backoff_base_seconds"),
        backoff_max_seconds=_positive(n.get("backoff_max_seconds", 60.0), "network.backoff_max_seconds"),
        buffer_max_messages=_positive_int(n.get("buffer_max_messages", 20000), "network.buffer_max_messages"),
    )
    if network.backoff_max_seconds < network.backoff_base_seconds:
        raise ConfigError("network.backoff_max_seconds doit être >= backoff_base_seconds")

    b = _section(data, "binance")
    endpoints = BinanceEndpoints(
        rest_base=str(b.get("rest_base", BinanceEndpoints.rest_base)).rstrip("/"),
        ws_base=str(b.get("ws_base", BinanceEndpoints.ws_base)).rstrip("/"),
    )

    lg = _section(data, "logging")
    level = str(lg.get("level", "INFO")).strip().upper()
    if level not in VALID_LOG_LEVELS:
        raise ConfigError(f"'logging.level' doit être dans {sorted(VALID_LOG_LEVELS)}, reçu {level}")
    log_file = lg.get("file", "lob.log")
    logging_cfg = LoggingConfig(level=level, file=str(log_file) if log_file else None)

    return AppConfig(
        exchange=exchange,
        symbols=tuple(symbols),
        depth_limit=depth_limit,
        ws_speed_ms=ws_speed_ms,
        display=display,
        capture=capture,
        paper=paper,
        kraken=kraken,
        prometheus=prometheus,
        network=network,
        binance=endpoints,
        logging=logging_cfg,
    )

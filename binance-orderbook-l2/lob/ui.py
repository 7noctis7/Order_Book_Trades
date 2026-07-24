"""Mode démo console multi-paires : marché, graphique, trades, performance.

Quatre vues (touches m / g / t / p) :
- MARCHÉ      : tableau des paires + ladder détaillé de la paire au focus ;
- GRAPHIQUE   : courbe du mid, marqueurs ▲ achat / ▼ vente, prix d'entrée ;
- TRADES      : portefeuille fictif, ordres en attente, historique des lots ;
- PERFORMANCE : statistiques (win rate, profit factor, drawdown…) + equity.

Ordres fictifs au clavier : ``a``/``v`` puis
  « 0.05 »          → ordre au marché,
  « 0.05@114900 »   → ordre LIMITE,
  « 0.05!112000 »   → ordre STOP (stop-loss / entrée en cassure).
``x`` annule un ordre en attente par son numéro, ``e`` exporte les trades
en CSV. En mode rejeu (backtest) : Espace pause, ``+``/``-`` vitesse.

Écran alternatif restauré à la sortie, frame écrite d'un seul write,
largeur fixe 78 colonnes, décimales déduites du tick réel de chaque paire.
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
from decimal import Decimal, InvalidOperation
from typing import NamedTuple, Sequence

from . import chart as chartlib
from . import stats as statslib
from .capture import SqliteCaptureWriter
from .config import DisplayConfig
from .history import PriceHistory
from .logging_setup import RingBufferHandler
from .orderbook import OrderBook
from .paper import PaperEngine, PaperError, TriggerEvent
from .trades_feed import TradeTape

# ----------------------------------------------------------- codes ANSI

RESET = "\x1b[0m"
BOLD = "\x1b[1m"
DIM = "\x1b[2m"
ALT_ON, ALT_OFF = "\x1b[?1049h", "\x1b[?1049l"
HIDE, SHOW = "\x1b[?25l", "\x1b[?25h"
HOME, CLR_EOS, CLR_EOL = "\x1b[H", "\x1b[0J", "\x1b[K"


def _fg(n: int) -> str:
    return f"\x1b[38;5;{n}m"


AMBER = _fg(214)
GOLD = _fg(178)
GREEN = _fg(42)
RED = _fg(203)
WHITE = _fg(255)
GRAY = _fg(245)
DGRAY = _fg(238)
CYAN = _fg(81)

WIDTH = 78
BAR_WIDTH = 26
MINI_BAR = 10
CHART_HEIGHT = 10
CHART_LABELS = 11
CHART_COLS = WIDTH - CHART_LABELS - 3
CHART_SECONDS_PER_COL = 4
_PARTIALS = " ▏▎▍▌▋▊▉"
SPINNER = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"

# Importé tard pour éviter le cycle ui → sync → ws_client.
from .sync import OrderBookSync, SyncState  # noqa: E402

_STATE_ORDER = [
    SyncState.CONNECTING,
    SyncState.BUFFERING,
    SyncState.SNAPSHOT_FETCHED,
    SyncState.DISCARDING_STALE,
    SyncState.VALIDATING_FIRST,
    SyncState.STREAMING,
]

PAGE_MARKET, PAGE_CHART, PAGE_TRADES, PAGE_PERF = "market", "chart", "trades", "perf"
_TITLES = {
    PAGE_MARKET: "MARCHÉ",
    PAGE_CHART: "GRAPHIQUE",
    PAGE_TRADES: "TRADES",
    PAGE_PERF: "PERFORMANCE",
}


class PairView(NamedTuple):
    symbol: str
    book: OrderBook
    sync: object            # OrderBookSync, KrakenPairSync ou ReplayPairSync
    history: PriceHistory
    tape: TradeTape | None = None


def _enable_vt() -> None:
    """Windows : activer l'interprétation des séquences ANSI (VT100)."""
    if os.name != "nt":
        return
    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32
        handle = kernel32.GetStdHandle(-11)  # STD_OUTPUT_HANDLE
        mode = ctypes.c_uint32()
        if kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            kernel32.SetConsoleMode(handle, mode.value | 0x0004)
    except Exception:
        pass  # au pire, séquences visibles : jamais bloquant


def _bar(fraction: float, width: int) -> str:
    fraction = max(0.0, min(1.0, fraction))
    cells = fraction * width
    full = int(cells)
    partial_idx = int((cells - full) * 8)
    bar = "█" * full
    if full < width and partial_idx > 0:
        bar += _PARTIALS[partial_idx]
    return bar


def _fmt(value: Decimal | float | int | None, decimals: int) -> str:
    if value is None:
        return "—"
    return f"{value:,.{decimals}f}"


def _fit(value: Decimal | float, width: int, decimals: int) -> str:
    """Formate en réduisant les décimales jusqu'à tenir dans ``width``."""
    for d in range(decimals, -1, -1):
        text = f"{value:,.{d}f}"
        if len(text) <= width:
            return f"{text:>{width}}"
    return text[-width:]


def _hms(seconds: float) -> str:
    seconds = int(seconds)
    return f"{seconds // 3600:02d}:{(seconds % 3600) // 60:02d}:{seconds % 60:02d}"


def _mmss(seconds: float) -> str:
    return f"{int(seconds) // 60}m{int(seconds) % 60:02d}s"


def _date(ts_ms: int) -> str:
    return time.strftime("%d/%m %H:%M", time.localtime(ts_ms / 1000))


def _base_asset(symbol: str) -> str:
    if "/" in symbol:
        return symbol.split("/", 1)[0]
    return symbol[:-4] if symbol.endswith("USDT") else symbol


def _quote_asset(symbol: str) -> str:
    if "/" in symbol:
        return symbol.split("/", 1)[1]
    return "USDT"


def _auto_decimals(levels: Sequence[tuple[Decimal, Decimal]], index: int, fallback: int) -> int:
    exponents = [level[index].as_tuple().exponent for level in levels]
    exponents = [e for e in exponents if isinstance(e, int)]
    if not exponents:
        return fallback
    return min(8, max(0, -min(exponents)))


def _pnl_str(amount: Decimal, pct: Decimal, width_amount: int = 10, width_pct: int = 7) -> str:
    color = GREEN if amount >= 0 else RED
    sign = "+" if amount >= 0 else ""
    return (
        f"{color}{sign}{_fit(amount, width_amount - len(sign), 2).strip():>{width_amount}}"
        f" {sign}{pct:>{width_pct - len(sign) - 1}.2f}%{RESET}"
    )


class ConsoleUI:
    def __init__(
        self,
        pairs: Sequence[PairView],
        cfg: DisplayConfig,
        ring: RingBufferHandler,
        exchange_name: str,
        ws_speed_ms: int,
        paper: PaperEngine | None,
        capture: SqliteCaptureWriter | None,
        log_file: str | None,
        replay=None,                      # ReplayController | None
        equity: statslib.EquityHistory | None = None,
    ) -> None:
        self._pairs = list(pairs)
        self._cfg = cfg
        self._ring = ring
        self._exchange = exchange_name.upper()
        self._speed = ws_speed_ms
        self._paper = paper
        self._capture = capture
        self._log_file = log_file
        self._replay = replay
        self._equity = equity
        self._frame_index = 0

        # État d'interaction (mutations depuis on_key, même boucle asyncio).
        self.repaint = asyncio.Event()
        self._page = PAGE_MARKET
        self._focus = 0
        self._order_side: str | None = None   # "BUY" / "SELL" pendant la saisie
        self._order_buffer = ""
        self._cancel_mode = False
        self._cancel_buffer = ""
        self._toast: tuple[str, str, float] | None = None  # (texte, couleur, expiration)

    # ------------------------------------------------------------- clavier

    def on_key(self, seq: str) -> None:
        if self._order_side is not None:
            self._on_key_order(seq)
        elif self._cancel_mode:
            self._on_key_cancel(seq)
        else:
            self._on_key_nav(seq)
        self.repaint.set()

    def _on_key_nav(self, seq: str) -> None:
        count = len(self._pairs)
        if seq in ("\t", "\x1b[C"):
            self._focus = (self._focus + 1) % count
        elif seq == "\x1b[D":
            self._focus = (self._focus - 1) % count
        elif len(seq) == 1 and seq.isdigit() and seq != "0":
            index = int(seq) - 1
            if index < count:
                self._focus = index
        elif seq in ("m", "M"):
            self._page = PAGE_MARKET
        elif seq in ("g", "G"):
            self._page = PAGE_CHART
        elif seq in ("t", "T"):
            self._page = PAGE_TRADES
        elif seq in ("p", "P"):
            self._page = PAGE_PERF
        elif seq in ("a", "A", "b", "B") and self._paper is not None:
            self._order_side, self._order_buffer = "BUY", ""
        elif seq in ("v", "V", "s", "S") and self._paper is not None:
            self._order_side, self._order_buffer = "SELL", ""
        elif seq in ("x", "X") and self._paper is not None and self._paper.pending:
            self._cancel_mode, self._cancel_buffer = True, ""
        elif seq in ("e", "E") and self._paper is not None:
            self._export_csv()
        elif self._replay is not None:
            if seq == " ":
                self._replay.paused = not self._replay.paused
            elif seq in ("+", "="):
                self._replay.faster()
            elif seq == "-":
                self._replay.slower()

    def _on_key_order(self, seq: str) -> None:
        if seq == "\x1b":
            self._order_side, self._order_buffer = None, ""
        elif seq in ("\n", "\r"):
            self._submit_order()
        elif seq == "\x7f":
            self._order_buffer = self._order_buffer[:-1]
        elif len(seq) == 1 and (seq.isdigit() or seq in ".,@!"):
            if len(self._order_buffer) < 24:
                self._order_buffer += "." if seq == "," else seq

    def _on_key_cancel(self, seq: str) -> None:
        if seq == "\x1b":
            self._cancel_mode, self._cancel_buffer = False, ""
        elif seq in ("\n", "\r"):
            self._cancel_mode = False
            try:
                order = self._paper.cancel_pending(int(self._cancel_buffer or "0"))
            except (PaperError, ValueError) as exc:
                self._show_toast(f"annulation impossible : {exc}", RED)
            else:
                self._show_toast(f"✓ ordre #{order.id} annulé", GOLD)
            self._cancel_buffer = ""
        elif seq == "\x7f":
            self._cancel_buffer = self._cancel_buffer[:-1]
        elif len(seq) == 1 and seq.isdigit() and len(self._cancel_buffer) < 6:
            self._cancel_buffer += seq

    def _submit_order(self) -> None:
        assert self._paper is not None and self._order_side is not None
        pair = self._pairs[self._focus]
        side = self._order_side
        buffer = self._order_buffer
        self._order_side, self._order_buffer = None, ""
        otype = None
        if "@" in buffer:
            otype, sep = "LIMIT", "@"
        elif "!" in buffer:
            otype, sep = "STOP", "!"
        try:
            if otype is None:
                qty = Decimal(buffer)
            else:
                qty_text, price_text = buffer.split(sep, 1)
                qty, price = Decimal(qty_text), Decimal(price_text)
        except (InvalidOperation, ValueError):
            self._show_toast("saisie invalide — qté, qté@limite ou qté!stop", RED)
            return
        decimals = self._price_decimals(pair.book)
        verb = "ACHAT" if side == "BUY" else "VENTE"
        try:
            if otype is None:
                report = self._paper.market_order(pair.symbol, side, qty, pair.book)
                self._show_toast(
                    f"✓ {verb} {report.qty.normalize()} {_base_asset(pair.symbol)}"
                    f" @ {report.avg_price:,.{decimals}f}"
                    f" · {report.notional:,.2f} {_quote_asset(pair.symbol)}"
                    f" · frais {report.fee:,.2f}"
                    f" · slippage {report.slippage:,.{decimals}f}"
                    f" ({report.levels_consumed} niv.)",
                    GREEN if side == "BUY" else RED,
                )
            else:
                order = self._paper.place_pending(pair.symbol, side, otype, qty, price)
                label = "LIMITE" if otype == "LIMIT" else "STOP"
                self._show_toast(
                    f"⏳ {label} {verb} #{order.id} :"
                    f" {qty.normalize()} {_base_asset(pair.symbol)}"
                    f" @ {price:,.{decimals}f} — en attente de déclenchement",
                    AMBER,
                )
        except PaperError as exc:
            self._show_toast(f"ordre refusé : {exc}", RED)

    def _export_csv(self) -> None:
        mids = {
            pair.symbol: pair.book.mid_price()
            for pair in self._pairs
            if pair.book.is_ready and pair.book.mid_price() is not None
        }
        try:
            path = statslib.export_csv(self._paper, mids)
        except OSError as exc:
            self._show_toast(f"export impossible : {exc}", RED)
        else:
            self._show_toast(f"✓ trades exportés → {path}", GREEN)

    def notify_triggers(self, events: list[TriggerEvent]) -> None:
        """Toasts pour les ordres déclenchés (appelé par le live et le rejeu)."""
        for event in events[-2:]:
            order = event.order
            label = "LIMITE" if order.otype == "LIMIT" else "STOP"
            verb = "ACHAT" if order.side == "BUY" else "VENTE"
            if event.report is not None:
                r = event.report
                self._show_toast(
                    f"⚡ {label} {verb} #{order.id} exécuté :"
                    f" {r.qty.normalize()} @ {r.avg_price:,.2f}"
                    f" · frais {r.fee:,.2f}",
                    GREEN if order.side == "BUY" else RED,
                )
            else:
                self._show_toast(f"✗ {label} #{order.id} : {event.reason}", GOLD)
        self.repaint.set()

    def _show_toast(self, text: str, color: str) -> None:
        self._toast = (text, color, time.monotonic() + 6.0)

    # ------------------------------------------------------------ exécution

    async def run(self, stop_event: asyncio.Event) -> None:
        _enable_vt()
        out = sys.stdout
        out.write(ALT_ON + HIDE)
        out.flush()
        stop_task = asyncio.create_task(stop_event.wait())
        try:
            while not stop_event.is_set():
                out.write(HOME + self._render() + CLR_EOS)
                out.flush()
                self._frame_index += 1
                repaint_task = asyncio.create_task(self.repaint.wait())
                await asyncio.wait(
                    {stop_task, repaint_task},
                    timeout=self._cfg.refresh_seconds,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                repaint_task.cancel()
                self.repaint.clear()
        finally:
            stop_task.cancel()
            out.write(SHOW + ALT_OFF)
            out.flush()

    # ---------------------------------------------------------- décimales

    def _price_decimals(self, book: OrderBook) -> int:
        if self._cfg.price_decimals is not None:
            return self._cfg.price_decimals
        return _auto_decimals(book.top_bids(3) + book.top_asks(3), 0, 2)

    def _qty_decimals(self, book: OrderBook) -> int:
        if self._cfg.qty_decimals is not None:
            return self._cfg.qty_decimals
        return _auto_decimals(book.top_bids(3) + book.top_asks(3), 1, 4)

    # ------------------------------------------------------------- rendu

    def _render(self) -> str:
        lines: list[str] = [""]
        lines += self._header()
        if self._page == PAGE_TRADES:
            lines += self._trades_page()
        elif self._page == PAGE_PERF:
            lines += self._perf_page()
        else:
            lines += self._table()
            focused = self._pairs[self._focus]
            if self._page == PAGE_CHART:
                lines += self._chart_section(focused)
            else:
                lines += self._focus_section(focused)
        lines += self._footer()
        return "\r\n".join(line + CLR_EOL for line in lines) + "\r\n"

    def _spin(self) -> str:
        return SPINNER[self._frame_index % len(SPINNER)]

    def _now_s(self) -> float:
        if self._replay is not None and self._replay.sim_time_ms:
            return self._replay.sim_time_ms / 1000
        return time.time()

    def _header(self) -> list[str]:
        streaming = sum(1 for p in self._pairs if p.sync.state is SyncState.STREAMING)
        errors = sum(1 for p in self._pairs if p.sync.state is SyncState.ERROR)
        total = len(self._pairs)
        if streaming == total:
            dot, color = "●", GREEN
        elif errors == total:
            dot, color = "✖", RED
        else:
            dot, color = self._spin(), AMBER

        if self._replay is not None:
            r = self._replay
            play = "■ TERMINÉ" if r.finished else ("⏸" if r.paused else "▶")
            status = f"REPLAY {r.speed_label} {play} {r.progress * 100:3.0f}%"
            color = GOLD if not r.finished else GREEN
            dot = "◆"
        else:
            status = f"{streaming}/{total} STREAMING"
        clock = time.strftime("%H:%M:%S", time.localtime(self._now_s()))
        title = _TITLES[self._page]
        left_plain = f" {title} · {self._exchange} SPOT @ {self._speed}ms"
        right_plain = f"{dot} {status}  {clock} "
        gap = WIDTH - len(left_plain) - len(right_plain)
        left = (
            f" {BOLD}{AMBER}{title}{RESET}"
            f" {GRAY}· {self._exchange} SPOT @ {self._speed}ms{RESET}"
        )
        right = f"{color}{dot} {BOLD}{status}{RESET}  {GRAY}{clock}{RESET} "
        tabs_plain = "  ".join(
            f"[{k}] {v}" for k, v in
            (("m", "MARCHÉ"), ("g", "GRAPH"), ("t", "TRADES"), ("p", "PERF"))
        )
        tabs = "  ".join(
            (BOLD + WHITE if self._page == page else DGRAY) + f"[{key}] {label}" + RESET
            for key, label, page in (
                ("m", "MARCHÉ", PAGE_MARKET),
                ("g", "GRAPH", PAGE_CHART),
                ("t", "TRADES", PAGE_TRADES),
                ("p", "PERF", PAGE_PERF),
            )
        )
        rule_fill = WIDTH - len(tabs_plain) - 4
        return [
            left + " " * max(1, gap) + right,
            f"{DGRAY}{'─' * 2}{RESET} {tabs} {DGRAY}{'─' * max(1, rule_fill)}{RESET}",
        ]

    # ------------------------------------------------------------ tableau

    def _table(self) -> list[str]:
        header = (
            f" {DGRAY}   PAIRE      {'MID':>13}  {'SPR bp':>7}"
            f"  IMBALANCE        {'p50':>4}  {'FLUX':>6}  ÉTAT{RESET}"
        )
        lines = [header]
        for index, pair in enumerate(self._pairs):
            lines.append(self._table_row(index, pair))
        return lines

    def _table_row(self, index: int, pair: PairView) -> str:
        sync, book = pair.sync, pair.book
        focused = index == self._focus
        cursor = f"{AMBER}▸{RESET}" if focused else " "
        sym_color = BOLD + WHITE if focused else GRAY
        symbol = f"{pair.symbol:<10.10s}"

        if sync.state is SyncState.STREAMING and book.is_ready:
            decimals = self._price_decimals(book)
            mid = f"{book.mid_price():>13,.{decimals}f}"
            spread, mid_val = book.spread(), book.mid_price()
            bp = (
                f"{spread / mid_val * 10_000:>7,.3f}"
                if spread is not None and mid_val
                else f"{'—':>7}"
            )
            ratio = book.imbalance(self._cfg.levels)
            if ratio is None:
                imb = " " * (MINI_BAR + 5)
            else:
                bid_cells = round(ratio * MINI_BAR)
                imb = (
                    f"{GREEN}{'█' * bid_cells}{RED}{'█' * (MINI_BAR - bid_cells)}"
                    f"{RESET} {GREEN}{ratio * 100:>3.0f}%{RESET}"
                )
            p50 = f"{sync.latency.p50 or 0:>4,.0f}"
            flux = f"{sync.rate.rate():>5.0f}/s"
            state = f"{GREEN}●{RESET}"
            resync_flag = (
                f" {AMBER}R{sync.resync_count}{RESET}" if sync.resync_count else "  "
            )
            return (
                f" {cursor}{index + 1} {sym_color}{symbol}{RESET}"
                f" {WHITE}{mid}{RESET}  {AMBER}{bp}{RESET}"
                f"  {imb}  {CYAN}{p50}{RESET}  {GRAY}{flux}{RESET}"
                f"  {state}{resync_flag}"
            )

        if sync.state is SyncState.ERROR:
            reason = (sync.error_reason or "erreur permanente")
            reason = reason.replace("snapshot refusé : snapshot ", "")[:42]
            return (
                f" {cursor}{index + 1} {sym_color}{symbol}{RESET}"
                f" {RED}✖ INDISPONIBLE{RESET}  {DGRAY}{reason}{RESET}"
            )

        return (
            f" {cursor}{index + 1} {sym_color}{symbol}{RESET}"
            f" {AMBER}{self._spin()} {sync.state.value}{RESET}"
            f"  {DGRAY}resyncs {sync.resync_count} · déco {sync.disconnect_count}{RESET}"
        )

    # -------------------------------------------------------- bande de trades

    def _tape_line(self, pair: PairView, price_dec: int) -> list[str]:
        tape = pair.tape
        if tape is None or tape.last is None:
            return []
        now_ms = int(self._now_s() * 1000)
        vwap = tape.vwap(now_ms)
        volume = tape.volume(now_ms)
        ratio = tape.buy_ratio(now_ms)
        parts = (
            f" {GRAY}LAST{RESET} {WHITE}{tape.last:,.{price_dec}f}{RESET}"
            f"  {GRAY}VWAP60{RESET} {CYAN}{_fmt(vwap, price_dec)}{RESET}"
            f"  {GRAY}VOL60{RESET} {_fit(volume, 9, 4).strip()}"
        )
        if ratio is not None:
            color = GREEN if ratio >= 0.5 else RED
            parts += f"  {GRAY}ACHETEURS{RESET} {color}{ratio * 100:.0f}%{RESET}"
        return [parts]

    # ------------------------------------------------------ vue MARCHÉ (focus)

    def _focus_section(self, pair: PairView) -> list[str]:
        sync, book = pair.sync, pair.book
        title = f"─ {pair.symbol} "
        stats_plain = (
            f" APPL {sync.events_applied:,} · PÉRIMÉS {sync.discarded_stale:,}"
            f" · RESYNC {sync.resync_count} · DÉCO {sync.disconnect_count}"
            f" · UP {_hms(sync.uptime_seconds)} "
        )
        fill = max(1, WIDTH - 1 - len(title) - len(stats_plain))
        lines = [
            "",
            f" {AMBER}{title}{RESET}{DGRAY}{'─' * fill}{RESET}{GRAY}{stats_plain}{RESET}",
        ]
        if sync.state is SyncState.STREAMING and book.is_ready:
            lines += self._tape_line(pair, self._price_decimals(book))
            lines += self._ladder(book)
        elif sync.state is SyncState.ERROR:
            lines += [
                "",
                f"   {RED}✖ Paire indisponible sur cet exchange.{RESET}",
                f"   {DGRAY}{(sync.error_reason or '')[:WIDTH - 6]}{RESET}",
                f"   {DGRAY}Vérifier le symbole dans config.yaml"
                f" (les autres paires continuent).{RESET}",
            ]
        else:
            lines += self._state_machine_panel(sync)
        return lines

    def _ladder(self, book: OrderBook) -> list[str]:
        levels = self._cfg.levels
        price_dec = self._price_decimals(book)
        qty_dec = self._qty_decimals(book)
        asks = book.top_asks(levels)
        bids = book.top_bids(levels)

        ask_cum: list[Decimal] = []
        total = Decimal(0)
        for _, qty in asks:
            total += qty
            ask_cum.append(total)
        bid_cum: list[Decimal] = []
        total = Decimal(0)
        for _, qty in bids:
            total += qty
            bid_cum.append(total)
        max_cum = max(
            [ask_cum[-1] if ask_cum else Decimal(0), bid_cum[-1] if bid_cum else Decimal(0)]
        ) or Decimal(1)

        lines = [
            f" {DGRAY}{'PRIX':>13}  {'QTÉ':>12}  {'CUMUL':>14}  PROFONDEUR{RESET}"
        ]
        for i in range(len(asks) - 1, -1, -1):
            price, qty = asks[i]
            lines.append(
                self._ladder_row(price, qty, ask_cum[i], max_cum, RED, price_dec, qty_dec)
            )
        spread = book.spread()
        mid = book.mid_price()
        spread_bp = (
            f"{spread / mid * 10_000:,.3f}" if spread is not None and mid else "—"
        )
        spread_text = f"─ spread {_fmt(spread, price_dec)} · {spread_bp} bp "
        lines.append(
            f" {AMBER}{spread_text}{'─' * max(0, WIDTH - 2 - len(spread_text))}{RESET}"
        )
        for i, (price, qty) in enumerate(bids):
            lines.append(
                self._ladder_row(price, qty, bid_cum[i], max_cum, GREEN, price_dec, qty_dec)
            )
        return lines

    def _ladder_row(
        self,
        price: Decimal,
        qty: Decimal,
        cum: Decimal,
        max_cum: Decimal,
        color: str,
        price_dec: int,
        qty_dec: int,
    ) -> str:
        price_s = f"{price:>13,.{price_dec}f}"
        qty_s = f"{qty:>12,.{qty_dec}f}"
        cum_s = f"{cum:>14,.{qty_dec}f}"
        bar = _bar(float(cum / max_cum), BAR_WIDTH)
        return (
            f" {color}{price_s}{RESET}  {WHITE}{qty_s}{RESET}"
            f"  {GRAY}{cum_s}{RESET}  {DIM}{color}{bar}{RESET}"
        )

    def _state_machine_panel(self, sync) -> list[str]:
        try:
            current_idx = _STATE_ORDER.index(sync.state)
        except ValueError:
            current_idx = 0
        lines = ["", f"   {GRAY}Synchronisation en cours…{RESET}"]
        for idx, state in enumerate(_STATE_ORDER):
            if idx < current_idx:
                lines.append(f"   {GREEN}✓{RESET} {GRAY}{state.value}{RESET}")
            elif idx == current_idx:
                lines.append(f"   {AMBER}{self._spin()} {BOLD}{state.value}{RESET}")
            else:
                lines.append(f"   {DGRAY}· {state.value}{RESET}")
        if sync.last_resync_reason:
            lines.append(
                f"   {DGRAY}dernière cause : {sync.last_resync_reason[: WIDTH - 22]}{RESET}"
            )
        return lines

    # --------------------------------------------------------- vue GRAPHIQUE

    def _chart_section(self, pair: PairView) -> list[str]:
        book = pair.book
        price_dec = self._price_decimals(book) if book.is_ready else 2
        buckets = chartlib.bucketize(
            pair.history.samples, CHART_COLS, CHART_SECONDS_PER_COL
        )
        window_s = len(buckets) * CHART_SECONDS_PER_COL
        title = f"─ {pair.symbol} · {window_s // 60}m{window_s % 60:02d}s "
        legend_plain = " ▲ achat · ▼ vente · ┄ prix d'entrée moyen "
        fill = max(1, WIDTH - 1 - len(title) - len(legend_plain))
        lines = [
            "",
            f" {AMBER}{title}{RESET}{DGRAY}{'─' * fill}{RESET}"
            f"{GRAY} {GREEN}▲{GRAY} achat · {RED}▼{GRAY} vente ·"
            f" {GOLD}┄{GRAY} prix d'entrée moyen {RESET}",
        ]
        if len(buckets) < 2:
            lines += [
                "",
                f"   {DGRAY}Historique en construction"
                f" ({len(buckets)} point{'s' if len(buckets) > 1 else ''})…"
                f" le graphique apparaît après quelques secondes de flux.{RESET}",
                "",
            ]
            lines += self._position_lines(pair, price_dec)
            return lines

        markers: dict[int, tuple[str, str]] = {}
        if self._paper is not None:
            for fill_row in self._paper.fills_for(pair.symbol):
                col = chartlib.marker_column(buckets, fill_row[0] / 1000)
                if col is not None:
                    char, color = ("▲", GREEN) if fill_row[1] == "BUY" else ("▼", RED)
                    markers[col] = (char, BOLD + color)

        hline = None
        if self._paper is not None:
            avg = self._paper.position_avg_price(pair.symbol)
            if avg is not None:
                hline = float(avg)

        grid, lo, hi = chartlib.render(
            [value for _, value in buckets],
            CHART_HEIGHT,
            CYAN,
            markers=markers,
            hline=hline,
            hline_style=("┄", GOLD),
        )
        lines += chartlib.assemble(
            grid, lo, hi, lambda v: f"{v:,.{price_dec}f}", CHART_LABELS, RESET, GRAY
        )
        lines.append("")
        lines += self._tape_line(pair, price_dec)
        lines += self._position_lines(pair, price_dec)
        return lines

    def _position_lines(self, pair: PairView, price_dec: int) -> list[str]:
        if self._paper is None:
            return []
        qty = self._paper.position_qty(pair.symbol)
        base = _base_asset(pair.symbol)
        if qty == 0:
            realized = sum(
                (t.realized for t in self._paper.closed if t.symbol == pair.symbol),
                Decimal(0),
            )
            color = GREEN if realized >= 0 else RED
            return [
                f" {GRAY}POSITION{RESET} {DGRAY}aucune ({base}){RESET}"
                f"   {GRAY}RÉALISÉ {pair.symbol}{RESET}"
                f" {color}{realized:+,.2f} {_quote_asset(pair.symbol)}{RESET}"
            ]
        avg = self._paper.position_avg_price(pair.symbol)
        mid = pair.book.mid_price()
        line = (
            f" {GRAY}POSITION{RESET} {WHITE}{qty.normalize()} {base}{RESET}"
            f" {GRAY}@ {avg:,.{price_dec}f} (frais incl.){RESET}"
        )
        if mid is not None and avg:
            latent = self._paper.unrealized(pair.symbol, mid)
            pct = (mid - avg) / avg * 100
            line += f"   {GRAY}PNL LATENT{RESET} {_pnl_str(latent, pct)}"
        return [line]

    # ------------------------------------------------------------ vue TRADES

    def _trades_page(self) -> list[str]:
        if self._paper is None:
            return ["", f"   {DGRAY}Paper trading désactivé (paper.enabled=false).{RESET}"]
        mids = {
            pair.symbol: pair.book.mid_price()
            for pair in self._pairs
            if pair.book.is_ready and pair.book.mid_price() is not None
        }
        summary = self._paper.summary(mids)
        lines = [
            f" {GRAY}CASH{RESET} {WHITE}{summary['cash']:,.2f}{RESET}"
            f"  {GRAY}POSITIONS{RESET} {WHITE}{summary['positions_value']:,.2f}{RESET}"
            f"  {GRAY}EQUITY{RESET} {BOLD}{WHITE}{summary['equity']:,.2f}{RESET}"
            f"  {DGRAY}(départ {summary['initial_cash']:,.0f}){RESET}",
            f" {GRAY}LATENT{RESET} {_pnl_str(summary['unrealized'], summary['unrealized_pct'])}"
            f"  {GRAY}RÉALISÉ{RESET} {_pnl_str(summary['realized'], summary['realized_pct'])}"
            f"  {DGRAY}nets de frais{RESET}",
        ]
        lines += self._pending_block()
        lines += [
            "",
            f"{DGRAY}  ACTIF {'ACHAT':<11} {'VENTE':<11} {'QTÉ':>7}"
            f" {'P.ACHAT':>10} {'P.VENTE':>10} {'PNL USDT':>8} {'PNL %':>7}{RESET}",
        ]

        rows: list[tuple[float, str]] = []
        for trade in self._paper.closed:
            rows.append((trade.sell_ts_ms, self._closed_row(trade)))
        for symbol, lots in self._paper.lots.items():
            mid = mids.get(symbol)
            for lot in lots:
                rows.append((lot.ts_ms, self._open_row(symbol, lot, mid)))
        rows.sort(key=lambda item: item[0], reverse=True)

        max_rows = 14 - min(len(self._paper.pending), 5)
        if not rows:
            lines += [
                "",
                f"   {DGRAY}Aucun trade — touche « a » pour un achat fictif,"
                f" « v » pour une vente.{RESET}",
            ]
        else:
            lines += [text for _, text in rows[:max_rows]]
            if len(rows) > max_rows:
                lines.append(
                    f"   {DGRAY}… {len(rows) - max_rows} autres lignes"
                    f" (export complet : touche e){RESET}"
                )
        return lines

    def _pending_block(self) -> list[str]:
        pending = self._paper.pending
        if not pending:
            return []
        title = f"─ ORDRES EN ATTENTE ({len(pending)}) "
        hint = " x annuler "
        fill = max(1, WIDTH - 1 - len(title) - len(hint))
        lines = [
            "",
            f" {GOLD}{title}{RESET}{DGRAY}{'─' * fill}{RESET}{GRAY}{hint}{RESET}",
        ]
        for order in pending[-5:]:
            side = "ACHAT" if order.side == "BUY" else "VENTE"
            otype = "LIMITE" if order.otype == "LIMIT" else "STOP"
            color = GREEN if order.side == "BUY" else RED
            lines.append(
                f"  {AMBER}#{order.id:<4}{RESET}"
                f" {WHITE}{_base_asset(order.symbol):<6.6s}{RESET}"
                f" {color}{side:<6}{RESET} {GOLD}{otype:<7}{RESET}"
                f" {_fit(order.qty, 10, 6)} {GRAY}@{RESET}"
                f" {_fit(order.price, 13, 6)}"
                f"  {DGRAY}posé {_date(order.ts_ms)}{RESET}"
            )
        return lines

    def _closed_row(self, trade) -> str:
        return (
            f"{DGRAY}●{RESET} {WHITE}{_base_asset(trade.symbol):<5.5s}{RESET}"
            f" {GRAY}{_date(trade.buy_ts_ms):<11}{RESET}"
            f" {GRAY}{_date(trade.sell_ts_ms):<11}{RESET}"
            f" {_fit(trade.qty, 7, 6)}"
            f" {_fit(trade.buy_price, 10, 6)}"
            f" {_fit(trade.sell_price, 10, 6)}"
            f" {_pnl_str(trade.realized, trade.realized_pct, 8, 7)}"
        )

    def _open_row(self, symbol: str, lot, mid: Decimal | None) -> str:
        if mid is not None:
            pnl = _pnl_str(lot.unrealized(mid), lot.unrealized_pct(mid), 8, 7)
        else:
            pnl = f"{DGRAY}{'—':>8} {'—':>7}{RESET}"
        return (
            f"{AMBER}○{RESET} {WHITE}{_base_asset(symbol):<5.5s}{RESET}"
            f" {GRAY}{_date(lot.ts_ms):<11}{RESET}"
            f" {DGRAY}{'— ouvert':<11}{RESET}"
            f" {_fit(lot.qty, 7, 6)}"
            f" {_fit(lot.price, 10, 6)}"
            f" {DGRAY}{'—':>10}{RESET}"
            f" {pnl}"
        )

    # ------------------------------------------------------- vue PERFORMANCE

    def _perf_page(self) -> list[str]:
        if self._paper is None:
            return ["", f"   {DGRAY}Paper trading désactivé.{RESET}"]
        stats = statslib.compute(self._paper, self._equity)
        pf = stats["profit_factor"]
        pf_text = "∞" if pf == float("inf") else (f"{pf:.2f}" if pf else "—")
        wr = stats["win_rate"]
        wr_text = f" ({wr:.0f}%)" if wr is not None else ""
        exp = stats["expectancy"]
        if exp is not None:
            exp_color = GREEN if exp >= 0 else RED
            exp_text = f"   {GRAY}ESPÉRANCE{RESET} {exp_color}{exp:+,.2f}{RESET}"
        else:
            exp_text = ""
        lines = [
            f" {GRAY}TRADES CLÔTURÉS{RESET} {WHITE}{stats['trades']}{RESET}"
            f"   {GRAY}GAGNANTS{RESET} {GREEN}{stats['wins']}{RESET}"
            f"{GRAY}{wr_text}{RESET}"
            f"   {GRAY}PERDANTS{RESET} {RED}{stats['losses']}{RESET}"
            f"   {GRAY}PROFIT FACTOR{RESET} {WHITE}{pf_text}{RESET}",
            f" {GRAY}GAINS BRUTS{RESET} {GREEN}+{stats['gross_profit']:,.2f}{RESET}"
            f"   {GRAY}PERTES BRUTES{RESET} {RED}-{stats['gross_loss']:,.2f}{RESET}"
            f"   {GRAY}FRAIS{RESET} {GOLD}{stats['fees_total']:,.2f}{RESET}"
            + exp_text,
        ]
        best, worst = stats["best"], stats["worst"]
        detail = f" {GRAY}MEILLEUR{RESET} "
        detail += f"{GREEN}{best:+,.2f}{RESET}" if best is not None else "—"
        detail += f"   {GRAY}PIRE{RESET} "
        detail += f"{RED}{worst:+,.2f}{RESET}" if worst is not None else "—"
        if stats["avg_holding_s"] is not None:
            detail += f"   {GRAY}DÉTENTION MOY.{RESET} {_mmss(stats['avg_holding_s'])}"
        dd, dd_pct = stats["max_drawdown"], stats["max_drawdown_pct"]
        detail += f"   {GRAY}MAX DD{RESET} {RED}-{dd:,.2f} (-{dd_pct:.2f}%){RESET}"
        lines.append(detail)

        # Courbe d'equity (temps simulé en backtest, temps réel en live).
        samples = list(self._equity.samples) if self._equity is not None else []
        title = "─ EQUITY "
        note = f" {len(samples)} pts · e exporter CSV "
        fill = max(1, WIDTH - 1 - len(title) - len(note))
        lines += [
            "",
            f" {AMBER}{title}{RESET}{DGRAY}{'─' * fill}{RESET}{GRAY}{note}{RESET}",
        ]
        buckets = chartlib.bucketize(samples, CHART_COLS, CHART_SECONDS_PER_COL)
        if len(buckets) < 2:
            lines.append(
                f"   {DGRAY}Courbe en construction — l'equity est"
                f" échantillonnée chaque seconde.{RESET}"
            )
            return lines
        values = [v for _, v in buckets]
        line_color = GREEN if values[-1] >= values[0] else RED
        grid, lo, hi = chartlib.render(
            values,
            CHART_HEIGHT - 2,
            line_color,
            hline=float(self._paper.initial_cash),
            hline_style=("┄", DGRAY),
        )
        lines += chartlib.assemble(
            grid, lo, hi, lambda v: f"{v:,.0f}", CHART_LABELS, RESET, GRAY
        )
        return lines

    # -------------------------------------------------------------- footer

    def _footer(self) -> list[str]:
        lines = [""]
        now = time.monotonic()
        if self._toast is not None and now < self._toast[2]:
            text, color, _ = self._toast
            lines.append(f" {color}{text[: WIDTH - 2]}{RESET}")
        for alert in list(self._ring.records)[-1:]:
            lines.append(f" {GOLD}⚠ {alert[: WIDTH - 4]}{RESET}")

        if self._order_side is not None:
            pair = self._pairs[self._focus]
            verb, color = (
                ("ACHAT", GREEN) if self._order_side == "BUY" else ("VENTE", RED)
            )
            approx = ""
            mid = pair.book.mid_price() if pair.book.is_ready else None
            qty_part = self._order_buffer.split("@")[0].split("!")[0]
            if mid is not None and qty_part:
                try:
                    approx = f" ≈{Decimal(qty_part) * mid:,.0f} {_quote_asset(pair.symbol)}"
                except InvalidOperation:
                    approx = ""
            lines.append(
                f" {BOLD}{color}{verb} {pair.symbol}{RESET}"
                f" {GRAY}qté[@lim|!stop]:{RESET}"
                f" {WHITE}{self._order_buffer}{AMBER}▏{RESET}{GRAY}{approx}{RESET}"
                f"  {DGRAY}⏎ ok · Esc annule{RESET}"
            )
            return lines

        if self._cancel_mode:
            lines.append(
                f" {GOLD}ANNULER L'ORDRE{RESET} {GRAY}n° :{RESET}"
                f" {WHITE}{self._cancel_buffer}{AMBER}▏{RESET}"
                f"  {DGRAY}⏎ confirmer · Esc abandonner{RESET}"
            )
            return lines

        if self._capture is not None:
            capture_s = f"capture ON ({self._capture.written:,})"
        elif self._replay is not None:
            capture_s = "rejeu de capture"
        else:
            capture_s = "capture OFF"
        keys = "m/g/t/p"
        if self._paper is not None:
            keys += " · a/v ordre · x annul · e csv"
        if self._replay is not None:
            keys += " · ␣ +/- rejeu · Ctrl+C"
            lines.append(f" {DGRAY}{keys}{RESET}")
        else:
            keys += f" · Tab/1-{len(self._pairs)} · Ctrl+C · {capture_s}"
            lines.append(f" {DGRAY}{keys}{RESET}")
        return lines

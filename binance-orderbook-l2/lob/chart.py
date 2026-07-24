"""Rendu d'un graphique de prix en console (style asciichart).

Ligne tracée avec ─ ╭ ╮ ╰ ╯ │, marqueurs d'exécutions ▲ (achat) / ▼ (vente),
ligne pointillée du prix d'entrée moyen. Chaque cellule porte (caractère,
couleur) ; l'assemblage minimise la casse : sortie prête à imprimer.
"""
from __future__ import annotations

from bisect import bisect_right
from typing import Callable, Sequence

Cell = tuple[str, str]  # (caractère, code couleur ANSI ou "")


def bucketize(
    samples: Sequence[tuple[float, float]], width: int, seconds_per_col: int
) -> list[tuple[float, float]]:
    """Regroupe les derniers échantillons en ``width`` colonnes max
    (dernière valeur de chaque intervalle de ``seconds_per_col`` secondes)."""
    window = [s for s in samples][-(width * seconds_per_col):]
    if not window:
        return []
    buckets: list[tuple[float, float]] = []
    start = window[0][0]
    for ts, value in window:
        index = int((ts - start) // seconds_per_col)
        if index >= len(buckets):
            buckets.append((ts, value))
        else:
            buckets[-1] = (ts, value)
    return buckets[-width:]


def marker_column(buckets: Sequence[tuple[float, float]], ts: float) -> int | None:
    """Colonne du graphique correspondant à un horodatage d'exécution."""
    if not buckets or ts < buckets[0][0] - 1 or ts > buckets[-1][0] + 60:
        return None
    col = bisect_right([b[0] for b in buckets], ts) - 1
    return max(0, min(col, len(buckets) - 1))


def render(
    values: Sequence[float],
    height: int,
    line_color: str,
    markers: dict[int, tuple[str, str]] | None = None,  # col -> (char, couleur)
    hline: float | None = None,
    hline_style: Cell = ("┄", ""),
) -> tuple[list[list[Cell]], float, float]:
    """Trace ``values`` dans une grille ``height`` × len(values).

    Retourne (grille de cellules, min, max) ; l'appelant ajoute les libellés
    d'axe et assemble les couleurs.
    """
    if not values:
        return [], 0.0, 0.0
    lo, hi = min(values), max(values)
    if hi == lo:  # série plate : élargir pour centrer la ligne
        pad = abs(hi) * 1e-6 or 1e-9
        lo, hi = lo - pad, hi + pad

    def row(value: float) -> int:
        return round((hi - value) / (hi - lo) * (height - 1))

    grid: list[list[Cell]] = [[(" ", "") for _ in values] for _ in range(height)]

    if hline is not None and lo <= hline <= hi:
        r = row(hline)
        for col in range(len(values)):
            grid[r][col] = hline_style

    rows = [row(v) for v in values]
    for col, r in enumerate(rows):
        if col == 0:
            grid[r][col] = ("─", line_color)
            continue
        prev = rows[col - 1]
        if r == prev:
            grid[r][col] = ("─", line_color)
        elif r < prev:  # prix qui monte (ligne vers le haut de l'écran)
            grid[r][col] = ("╭", line_color)
            grid[prev][col] = ("╯", line_color)
            for between in range(r + 1, prev):
                grid[between][col] = ("│", line_color)
        else:  # prix qui descend
            grid[r][col] = ("╰", line_color)
            grid[prev][col] = ("╮", line_color)
            for between in range(prev + 1, r):
                grid[between][col] = ("│", line_color)

    for col, (char, color) in (markers or {}).items():
        if 0 <= col < len(values):
            grid[rows[col]][col] = (char, color)

    return grid, lo, hi


def assemble(
    grid: list[list[Cell]],
    lo: float,
    hi: float,
    label_fmt: Callable[[float], str],
    label_width: int,
    reset: str,
    label_color: str,
) -> list[str]:
    """Assemble grille + libellés d'axe (haut / milieu / bas) en lignes ANSI."""
    height = len(grid)
    lines: list[str] = []
    for r, cells in enumerate(grid):
        if r == 0:
            label = label_fmt(hi)
        elif r == height - 1:
            label = label_fmt(lo)
        elif r == height // 2:
            label = label_fmt((hi + lo) / 2)
        else:
            label = ""
        parts: list[str] = [f"{label_color}{label:>{label_width}}{reset} "]
        current = ""
        for char, color in cells:
            if color != current:
                parts.append(reset if not color else color)
                current = color
            parts.append(char)
        if current:
            parts.append(reset)
        lines.append(" " + "".join(parts))
    return lines

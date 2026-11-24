"""The CLI's header on a terminal: a small CocoIndex coconut beside three lines
of text. It is printed once, when a command starts, and only where it renders
well — an interactive terminal with color on. Everywhere else the CLI prints
the same facts on one line."""

from __future__ import annotations

import os
import shutil
import sys

from cocoindex._version import __version__

# The coconut, one character per pixel; two pixel rows make one text row (each
# cell is a half block). Drawn from the CocoIndex logo's palette.
_PALETTE: dict[str, tuple[int, int, int] | None] = {
    ".": None,
    "S": (0x53, 0x26, 0x38),  # stem
    "G": (0x2F, 0xB8, 0x3A),  # leaf, shaded half
    "L": (0x4C, 0xE8, 0x50),  # leaf
    "l": (0x9C, 0xF5, 0x9E),  # leaf highlight
    "R": (0xFC, 0xF3, 0xD8),  # rim
    "r": (0xE9, 0xD2, 0xA4),  # rim edge: keeps the outline on light themes
    "F": (0xE5, 0x9A, 0x63),  # flesh
    "E": (0x40, 0x1D, 0x30),  # eyes, mouth
    "C": (0xFB, 0x6A, 0x76),  # cheeks
    "B": (0xBE, 0x51, 0x33),  # shell
    "D": (0x7A, 0x28, 0x26),  # shell stripes
}
_ART = (
    "....S.GLl.",
    "..rrrrrGL.",
    ".rFFFFFFr.",
    "rFEFFFFEFr",
    "rCFFEEFFCr",
    "BrRRRRRRrB",
    "BBBBBBBBBB",
    "BDBBDBBBDB",
    ".BBBBBBBB.",
    "..BBBBBB..",
)
_ART_CELLS = len(_ART[0])
_GAP = "   "

_RESET = "\x1b[0m"
_BOLD = "\x1b[1m"
_GRAY = "\x1b[90m"

# The 6 levels of each channel in the 256-color cube.
_CUBE = (0, 95, 135, 175, 215, 255)

# Colors with no close match in the 256-color cube, pinned to the index that
# keeps their hue: the nearest match of the shell is a pink.
_PINNED_256 = {(0xBE, 0x51, 0x33): 130}


def _truecolor() -> bool:
    return os.environ.get("COLORTERM", "").lower() in ("truecolor", "24bit")


def _nearest_256(rgb: tuple[int, int, int]) -> int:
    """The 256-color index closest to `rgb`, over the color cube and the
    gray ramp, unless it is pinned."""
    if rgb in _PINNED_256:
        return _PINNED_256[rgb]

    def dist(a: tuple[int, int, int]) -> int:
        return sum((x - y) ** 2 for x, y in zip(a, rgb))

    best_index, best_dist = 0, None
    for r in range(6):
        for g in range(6):
            for b in range(6):
                d = dist((_CUBE[r], _CUBE[g], _CUBE[b]))
                if best_dist is None or d < best_dist:
                    best_index, best_dist = 16 + 36 * r + 6 * g + b, d
    for i in range(24):
        v = 8 + 10 * i
        d = dist((v, v, v))
        if best_dist is None or d < best_dist:
            best_index, best_dist = 232 + i, d
    return best_index


def _color(rgb: tuple[int, int, int], *, background: bool) -> str:
    layer = 48 if background else 38
    if _truecolor():
        r, g, b = rgb
        return f"\x1b[{layer};2;{r};{g};{b}m"
    return f"\x1b[{layer};5;{_nearest_256(rgb)}m"


def _art_rows() -> list[str]:
    """The coconut as text rows of half blocks."""
    rows = []
    for top_row, bottom_row in zip(_ART[0::2], _ART[1::2]):
        cells = []
        for t, b in zip(top_row, bottom_row):
            top, bottom = _PALETTE[t], _PALETTE[b]
            if top is None and bottom is None:
                cells.append(" ")
            elif bottom is None:
                assert top is not None
                cells.append(f"{_color(top, background=False)}▀{_RESET}")
            elif top is None:
                cells.append(f"{_color(bottom, background=False)}▄{_RESET}")
            else:
                cells.append(
                    f"{_color(top, background=False)}"
                    f"{_color(bottom, background=True)}▀{_RESET}"
                )
        rows.append("".join(cells))
    return rows


def enabled() -> bool:
    """True where colored output renders well: stdout is a terminal, colors
    are on (`NO_COLOR` unset, see no-color.org) and the terminal is not
    `dumb`."""
    if os.environ.get("NO_COLOR") or os.environ.get("TERM") == "dumb":
        return False
    return sys.stdout.isatty()


def _fit(text: str, cells: int, *, keep_end: bool = False) -> str:
    """Fit `text` in `cells`, with `…` where it is cut: at the end, or for a
    path (`keep_end`) at the start, where the file name survives."""
    if len(text) <= cells:
        return text
    if cells <= 1:
        return "…"[:cells]
    if not keep_end:
        return text[: cells - 1] + "…"
    tail = text[-(cells - 1) :]
    # Start at a `/`, so no directory name is cut in half.
    slash = tail.find("/")
    if 0 < slash < len(tail) - 1:
        tail = tail[slash:]
    return "…" + tail


def render(title: str, action: str, path: str) -> str:
    """The header: a blank line, then the coconut with `title` (bold),
    `action` and `path` (gray, labeled `db`) beside it, all fit to the
    terminal width."""
    room = max(shutil.get_terminal_size().columns - 1 - _ART_CELLS - len(_GAP), 8)
    texts = [
        "",
        f"{_BOLD}{_fit(title, room)}{_RESET}",
        _fit(action, room),
        f"{_GRAY}db {_fit(path, room - 3, keep_end=True)}{_RESET}",
        "",
    ]
    rows = [
        f"{art}{_GAP}{text}" if text else art for art, text in zip(_art_rows(), texts)
    ]
    return "\n" + "\n".join(row.rstrip() for row in rows) + "\n"


def version_label() -> str:
    return f"CocoIndex {__version__}"

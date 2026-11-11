"""Tests for the CLI's terminal header."""

from __future__ import annotations

import re

import pytest

from cocoindex._internal import banner

_ESCAPES = re.compile(r"\x1b\[[0-9;]*m")


def _text(out: str) -> list[str]:
    return [_ESCAPES.sub("", row) for row in out.splitlines()]


def test_render_places_text_beside_the_coconut(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("COLORTERM", "truecolor")
    monkeypatch.setenv("COLUMNS", "100")
    out = banner.render("CocoIndex 1.2.3", "Updating App", "./db")
    rows = _text(out)
    # A blank line, then 5 rows of art.
    assert rows[0] == ""
    assert len(rows) == 6
    assert rows[2].endswith("   CocoIndex 1.2.3")
    assert rows[3].endswith("   Updating App")
    assert rows[4].endswith("   db ./db")
    assert "\x1b[38;2;" in out


def test_render_fits_a_narrow_terminal(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COLUMNS", "40")
    rows = _text(
        banner.render(
            "CocoIndex 1.2.3", "Updating App", "/tmp/progress/validate/scenes/f.db"
        )
    )
    assert all(len(row) <= 39 for row in rows)
    # A path keeps its end, cut at a `/`.
    assert rows[4].endswith("db …/validate/scenes/f.db")


def test_256_colors_pick_the_nearest_index(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("COLORTERM", raising=False)
    out = banner.render("x", "y", "z")
    assert "\x1b[38;5;" in out and "\x1b[38;2;" not in out
    # The shell brown keeps its hue: its nearest match, 131, is a pink.
    assert banner._nearest_256((0xBE, 0x51, 0x33)) == 130
    assert banner._nearest_256((0x40, 0x1D, 0x30)) == 236


def test_disabled_without_terminal_or_with_no_color(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Under pytest stdout is captured, so not a terminal.
    assert not banner.enabled()
    monkeypatch.setattr("sys.stdout.isatty", lambda: True)
    assert banner.enabled()
    monkeypatch.setenv("NO_COLOR", "1")
    assert not banner.enabled()

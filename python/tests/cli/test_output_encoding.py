"""The CLI writes UTF-8 to piped output whatever the locale's encoding."""

from __future__ import annotations

import io

from cocoindex.cli import _utf8_when_piped


def test_piped_output_in_a_legacy_encoding_switches_to_utf8() -> None:
    raw = io.BytesIO()
    stream = io.TextIOWrapper(raw, encoding="cp1252")
    _utf8_when_piped(stream)
    stream.write("✓ Dropped App (target states reverted · <0.1s)\n")
    stream.flush()
    assert (
        raw.getvalue().decode("utf-8")
        == "✓ Dropped App (target states reverted · <0.1s)\n"
    )


def test_utf8_output_is_left_alone() -> None:
    stream = io.TextIOWrapper(io.BytesIO(), encoding="UTF8")
    _utf8_when_piped(stream)
    assert stream.encoding == "UTF8"

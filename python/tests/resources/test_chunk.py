"""Tests for Chunk and TextPosition dataclasses."""

import pytest

from cocoindex.resources.chunk import Chunk, TextPosition


class TestTextPosition:
    def test_construction(self) -> None:
        pos = TextPosition(byte_offset=0, char_offset=0, line=1, column=1)
        assert pos.byte_offset == 0
        assert pos.char_offset == 0
        assert pos.line == 1
        assert pos.column == 1

    def test_non_zero_offsets(self) -> None:
        pos = TextPosition(byte_offset=10, char_offset=8, line=3, column=5)
        assert pos.byte_offset == 10
        assert pos.char_offset == 8
        assert pos.line == 3
        assert pos.column == 5

    def test_frozen_immutability(self) -> None:
        pos = TextPosition(byte_offset=0, char_offset=0, line=1, column=1)
        with pytest.raises((AttributeError, TypeError)):
            pos.byte_offset = 99  # type: ignore[misc]

    def test_equality(self) -> None:
        pos1 = TextPosition(byte_offset=5, char_offset=5, line=2, column=3)
        pos2 = TextPosition(byte_offset=5, char_offset=5, line=2, column=3)
        assert pos1 == pos2

    def test_inequality(self) -> None:
        pos1 = TextPosition(byte_offset=0, char_offset=0, line=1, column=1)
        pos2 = TextPosition(byte_offset=1, char_offset=1, line=1, column=2)
        assert pos1 != pos2


class TestChunk:
    def _make_pos(self, byte: int, char: int, line: int, col: int) -> TextPosition:
        return TextPosition(byte_offset=byte, char_offset=char, line=line, column=col)

    def test_construction(self) -> None:
        start = self._make_pos(0, 0, 1, 1)
        end = self._make_pos(5, 5, 1, 6)
        chunk = Chunk(text="hello", start=start, end=end)
        assert chunk.text == "hello"
        assert chunk.start == start
        assert chunk.end == end

    def test_empty_text(self) -> None:
        pos = self._make_pos(0, 0, 1, 1)
        chunk = Chunk(text="", start=pos, end=pos)
        assert chunk.text == ""
        assert chunk.start is pos
        assert chunk.end is pos

    def test_multiline_text(self) -> None:
        start = self._make_pos(0, 0, 1, 1)
        end = self._make_pos(11, 11, 2, 6)
        chunk = Chunk(text="hello\nworld", start=start, end=end)
        assert chunk.text == "hello\nworld"
        assert chunk.start.line == 1
        assert chunk.end.line == 2

    def test_frozen_immutability(self) -> None:
        start = self._make_pos(0, 0, 1, 1)
        end = self._make_pos(5, 5, 1, 6)
        chunk = Chunk(text="hello", start=start, end=end)
        with pytest.raises((AttributeError, TypeError)):
            chunk.text = "other"  # type: ignore[misc]

    def test_equality(self) -> None:
        start = self._make_pos(0, 0, 1, 1)
        end = self._make_pos(5, 5, 1, 6)
        chunk1 = Chunk(text="hello", start=start, end=end)
        chunk2 = Chunk(text="hello", start=start, end=end)
        assert chunk1 == chunk2

    def test_inequality_by_text(self) -> None:
        start = self._make_pos(0, 0, 1, 1)
        end = self._make_pos(5, 5, 1, 6)
        chunk1 = Chunk(text="hello", start=start, end=end)
        chunk2 = Chunk(text="world", start=start, end=end)
        assert chunk1 != chunk2

"""Chunk-related data structures for text processing."""

from dataclasses import dataclass as _dataclass

__all__ = ["Chunk", "TextPosition"]


@_dataclass(frozen=True, slots=True)
class TextPosition:
    """Position information in text with byte offset, character offset, and line/column."""

    byte_offset: int
    """Byte offset from the start of the text."""

    char_offset: int
    """Character (not byte) offset from the start of the text."""

    line: int
    """1-based line number."""

    column: int
    """1-based column number."""


@_dataclass(frozen=True, slots=True)
class Chunk:
    """A chunk of text with its range and position information."""

    text: str
    """The text content of the chunk."""

    start: TextPosition
    """Start position (byte offset, character offset, line, column)."""

    end: TextPosition
    """End position (byte offset, character offset, line, column)."""

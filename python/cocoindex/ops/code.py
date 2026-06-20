r"""Structural code matching over a reusable parsed AST.

Parse source once into a :class:`CodeAst`, then match by-example structural
patterns and/or split it into chunks without re-parsing. Metavariables in a
pattern use the ``\`` sigil (e.g. ``\NAME``, ``\(ARGS*)``).
"""

__all__ = [
    "CodeAst",
    "CodeMatch",
    "match_code",
]

from dataclasses import dataclass as _dataclass

from cocoindex._internal import core as _core
from cocoindex.resources import chunk as _chunk


@_dataclass(frozen=True, slots=True)
class CodeMatch:
    """A structural-match result: a matched node and its captured metavariables."""

    kind: str
    """tree-sitter node kind of the matched node (e.g. ``function_definition``)."""

    chunks: list[_chunk.Chunk]
    """The matched code region(s), each with text and line/column positions.

    Currently always exactly one chunk (the whole matched node); a future
    carve-out feature may return several (e.g. a function's head and tail with the
    body elided)."""

    captures: dict[str, list[_chunk.Chunk]]
    """Captured metavariables: name -> matched region(s).

    Each captured value is a list of chunks (currently always exactly one); the
    captured text is ``m.captures[name][0].text``, with line/column on each chunk."""


class CodeAst:
    r"""A parsed code AST: parse once, then match and/or split without re-parsing.

    Any language with tree-sitter support can be parsed and split; structural
    matching additionally requires a structurally-supported language (a
    ``ValueError`` is raised otherwise).

    Args:
        source: The source code.
        language: Language name or alias (e.g. ``"python"``, ``"rust"``, ``"c++"``).

    Examples:
        >>> ast = CodeAst("def f(a, b): return a + b", language="python")
        >>> [m.captures["NAME"] for m in ast.matches(r"def \NAME(\(ARGS*)):")]
        ['f']
        >>> chunks = ast.split(chunk_size=1000)
    """

    def __init__(self, source: str, language: str) -> None:
        self._ast = _core.CodeAst(source, language)

    @property
    def language(self) -> str:
        """The language this AST was parsed for."""
        return self._ast.language

    @property
    def source(self) -> str:
        """The source text."""
        return self._ast.source

    def matches(self, pattern: str) -> list[CodeMatch]:
        r"""Find every match of a by-example structural ``pattern`` (reuses the parse).

        Raises:
            ValueError: if the language is unsupported for matching, or the
                pattern is malformed.
        """
        source = self._ast.source
        return [_convert_match(m, source) for m in self._ast.matches(pattern)]

    def split(
        self,
        chunk_size: int,
        *,
        min_chunk_size: int | None = None,
        chunk_overlap: int | None = None,
    ) -> list[_chunk.Chunk]:
        """Split into chunks (reuses the parse), syntax-aware for this AST's language.

        Args:
            chunk_size: Target chunk size in bytes.
            min_chunk_size: Minimum chunk size in bytes. Defaults to chunk_size / 2.
            chunk_overlap: Overlap between consecutive chunks in bytes.

        Returns:
            A list of Chunk objects with text content and position information.
        """
        raw = self._ast.split(chunk_size, min_chunk_size, chunk_overlap)
        return [_convert_chunk(c, self._ast.source) for c in raw]


def match_code(pattern: str, source: str, language: str) -> list[CodeMatch]:
    r"""One-shot: parse ``source`` for ``language`` and return all matches of
    ``pattern``. Equivalent to ``CodeAst(source, language).matches(pattern)``."""
    return [
        _convert_match(m, source) for m in _core.match_code(pattern, source, language)
    ]


def _convert_match(raw: "_core.CodeMatch", source: str) -> CodeMatch:
    """Convert a raw PyO3 match to a Python dataclass."""
    return CodeMatch(
        kind=raw.kind,
        chunks=[_convert_chunk(c, source) for c in raw.chunks],
        captures={
            name: [_convert_chunk(c, source) for c in chunks]
            for name, chunks in raw.captures.items()
        },
    )


def _convert_chunk(raw: "_core.Chunk", text: str) -> _chunk.Chunk:
    """Convert a raw PyO3 chunk to a Python Chunk dataclass."""
    chunk_text = text[raw.start_char_offset : raw.end_char_offset]
    return _chunk.Chunk(
        text=chunk_text,
        start=_chunk.TextPosition(
            byte_offset=raw.start_byte,
            char_offset=raw.start_char_offset,
            line=raw.start_line,
            column=raw.start_column,
        ),
        end=_chunk.TextPosition(
            byte_offset=raw.end_byte,
            char_offset=raw.end_char_offset,
            line=raw.end_line,
            column=raw.end_column,
        ),
    )

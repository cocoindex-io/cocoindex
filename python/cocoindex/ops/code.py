r"""Structural code matching over a reusable parsed AST.

Wrap source in a :class:`CodeSource`, then pass it to any parse-consuming API —
structural pattern matching (:class:`CodePattern`, :func:`match_code`),
chunk splitting (:meth:`RecursiveSplitter.split
<cocoindex.ops.text.RecursiveSplitter.split>`), prefilter-index term extraction
(:func:`index_terms`) — and the source is parsed **at most once**.
Metavariables in a pattern use the ``\`` sigil (e.g. ``\NAME``, ``\(ARGS*\)``).
"""

__all__ = [
    "CodeMatch",
    "CodePattern",
    "CodeSource",
    "FileMatch",
    "index_terms",
    "match_code",
    "render_match",
]

import typing as _typing
import warnings as _warnings
from dataclasses import dataclass as _dataclass

from cocoindex._internal import core as _core
from cocoindex.resources import chunk as _chunk
from cocoindex.resources import view as _view


class CodeSource:
    """Source text plus a lazily-parsed, shared AST.

    The universal input for APIs that may need a parse: pass one handle to
    several of them (:meth:`RecursiveSplitter.split
    <cocoindex.ops.text.RecursiveSplitter.split>`,
    :meth:`CodePattern.match_source`, :func:`match_code`,
    :func:`index_terms`, …) and the source is parsed **at most once**, no
    matter how many consumers touch it.

    Construction never parses and never raises: an unknown or non-tree-sitter
    language is fine — each consumer takes its own degraded (non-AST) path,
    exactly as if it had been given a plain ``str``, or raises at call time if
    it genuinely requires an AST.

    Args:
        text: The source text.
        language: Language name, alias, or file extension (e.g. ``"python"``,
            ``"c++"``, ``".rs"``). ``None`` means no syntax awareness.

    Examples:
        >>> src = CodeSource("def f(): pass", language="python")
        >>> splitter = RecursiveSplitter()          # doctest: +SKIP
        >>> chunks = splitter.split(src, chunk_size=1000)   # parses once
        >>> cp = CodePattern(r"def \\NAME(\\(A*\\)):", language="python")
        >>> ms = cp.match_source(src)               # reuses the same parse
    """

    def __init__(self, text: str, language: str | None = None) -> None:
        self._src = _core.CodeSource(text, language)

    @classmethod
    def _from_core(cls, raw: "_core.CodeSource") -> "CodeSource":
        """Wrap an already-built core source (e.g. from ``CodePattern.match_file``,
        with its parse already cached) without copying."""
        self = cls.__new__(cls)
        self._src = raw
        return self

    @property
    def text(self) -> str:
        """The source text."""
        return self._src.text

    @property
    def language(self) -> str | None:
        """The language as given at construction (may be an alias/extension)."""
        return self._src.language

    @property
    def source(self) -> str:
        """Deprecated alias of :attr:`text` (the name the removed ``CodeAst``
        class used); use ``.text``."""
        _warnings.warn(
            "CodeSource.source is deprecated; use CodeSource.text",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._src.text

    def __repr__(self) -> str:
        return f"CodeSource(language={self.language!r}, text_len={len(self.text)})"


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


class CodePattern:
    r"""A compiled structural pattern, built once and reused across many sources.

    Compiling a pattern (and its prefilter) is not free; construct a ``CodePattern``
    once and match it against many files/sources instead of calling
    :func:`match_code` with a pattern string every time (which recompiles).

    Args:
        pattern: The by-example structural pattern (``\`` sigil for metavars).
        language: Language name or alias (e.g. ``"python"``, ``"c++"``).
        min_len: Prefilter tuning — required terms shorter than this are dropped.

    Examples:
        >>> cp = CodePattern(r"def \NAME(\(A*\)):", language="python")
        >>> cp.might_match("x = 1")          # cheap, parse-free pre-check
        False
        >>> [m.captures["NAME"][0].text for m in cp.match_source("def f(): pass")]
        ['f']
    """

    def __init__(self, pattern: str, language: str, *, min_len: int = 3) -> None:
        self._cp = _core.CodePattern(pattern, language, min_len)

    @property
    def language(self) -> str:
        """The language this pattern was compiled for."""
        return self._cp.language

    def might_match(self, source: str) -> bool:
        """Whether ``source`` *might* contain a match — a cheap, parse-free prefilter
        check. ``False`` means it definitely can't (skip it); ``True`` means "maybe"."""
        return self._cp.might_match(source)

    def match_source(self, source: "str | CodeSource") -> list[CodeMatch]:
        """Match against ``source`` — a ``str`` (parsed on the spot) or a
        :class:`CodeSource` (reusing its cached parse) — skipping the parse
        entirely when the prefilter rejects it. Reuses this pattern's
        compilation across calls."""
        if isinstance(source, CodeSource):
            raw, text = self._cp.match_source(source._src), source.text
        else:
            raw, text = self._cp.match_source(source), source
        return [_convert_match(m, text) for m in raw]

    def match_file(self, path: str) -> "FileMatch | None":
        """Read ``path``, prefilter, and (only if it might match) parse + match.

        Returns a :class:`FileMatch` (parsed source + matches) when there is at
        least one match, else ``None`` — so a rejected or non-matching file never
        costs a parse beyond what the prefilter needs. Non-UTF-8 (binary) files are
        skipped (``None``); other I/O errors raise ``OSError``.
        """
        raw = self._cp.match_file(path)
        if raw is None:
            return None
        source = CodeSource._from_core(raw.source)
        return FileMatch(
            path=raw.path,
            source=source,
            matches=[_convert_match(m, source.text) for m in raw.matches],
        )


@_dataclass(frozen=True, slots=True)
class FileMatch:
    """The result of :meth:`CodePattern.match_file`: the parsed source and the
    matches found in one file. The file content is ``file_match.source.text``."""

    path: str
    """The path that was matched."""

    source: CodeSource
    """The parsed source (its AST is already cached — reuse it to split or match
    more patterns without re-parsing)."""

    matches: list[CodeMatch]
    """The matches found (always at least one — ``match_file`` returns ``None``
    when there are none)."""

    @property
    def ast(self) -> CodeSource:
        """Deprecated alias of :attr:`source` (the field's name when it held the
        removed ``CodeAst`` class); use ``.source``."""
        _warnings.warn(
            "FileMatch.ast is deprecated; use FileMatch.source "
            "(and .text for the file content)",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.source


def match_code(
    pattern: str, source: "str | CodeSource", language: str | None = None
) -> list[CodeMatch]:
    r"""One-shot: match ``pattern`` against ``source`` and return all matches.

    ``source`` is a ``str`` (with ``language`` required) or a :class:`CodeSource`
    (whose cached parse is reused; ``language`` must be omitted). Prefer a
    :class:`CodePattern` when matching the same pattern across many sources —
    this recompiles the pattern per call."""
    if isinstance(source, CodeSource):
        raw, text = _core.match_code(pattern, source._src, language), source.text
    else:
        raw, text = _core.match_code(pattern, source, language), source
    return [_convert_match(m, text) for m in raw]


def index_terms(
    source: "str | CodeSource", language: str | None = None, min_len: int = 3
) -> list[str]:
    """Extract the indexable terms of ``source`` (identifiers + string-literal
    content, ``>= min_len`` chars, deduped), for building an external prefilter
    index (FTS / n-grams).

    ``source`` is a ``str`` (with ``language`` required) or a :class:`CodeSource`
    (whose cached parse is reused; ``language`` must be omitted). Raises
    ``ValueError`` for an unknown or non-tree-sitter language — silently
    indexing nothing would poison the index with false negatives."""
    raw = source._src if isinstance(source, CodeSource) else source
    return _core.index_terms(raw, language, min_len)


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


def render_match(source: "CodeSource", match: CodeMatch) -> _view.SourceView:
    r"""Render a match as a source view: context frames of its enclosing scopes,
    the matched range(s) verbatim, and elision cues where material is omitted.

    The returned :class:`~cocoindex.resources.view.SourceView` carries synthetic
    ``text`` plus source-grounded segments — frames are the head lines of
    enclosing scopes, outermost first, and its ``start``/``end`` (frames
    excluded) equal the match's span. Matched ranges are rendered exactly (no
    widening to line starts).

    Args:
        source: The parsed source the match came from (e.g.
            ``file_match.source``); its cached parse is reused.
        match: A match produced against that same source.

    Examples:
        >>> fm = CodePattern(r"return \X", language="python").match_file(path)
        >>> view = render_match(fm.source, fm.matches[0])
        >>> print(view.text)  # frames + matched code
    """
    ranges = [(c.start.byte_offset, c.end.byte_offset) for c in match.chunks]
    raw = _core.render_ranges(source._src, ranges)
    return _convert_source_view(raw)


def _convert_source_view(raw: "_core.SourceView") -> _view.SourceView:
    """Convert a raw PyO3 source view to a Python dataclass."""
    segments = [
        _view.ViewSegment(
            start=_chunk.TextPosition(
                byte_offset=seg.start_byte,
                char_offset=seg.start_char_offset,
                line=seg.start_line,
                column=seg.start_column,
            ),
            end=_chunk.TextPosition(
                byte_offset=seg.end_byte,
                char_offset=seg.end_char_offset,
                line=seg.end_line,
                column=seg.end_column,
            ),
            kind=_typing.cast('_typing.Literal["frame", "content"]', seg.kind),
            summary=seg.summary,
            rendered_start=seg.rendered_start,
            rendered_end=seg.rendered_end,
        )
        for seg in raw.segments
    ]
    return _view.SourceView(text=raw.text, segments=segments)


def __getattr__(name: str) -> _typing.Any:
    # Deprecated alias for the removed ``CodeAst`` class, so annotations and
    # ``isinstance`` checks in older callers keep working. ``CodeSource`` is
    # the single handle now; note its constructor is lazy and tolerant where
    # ``CodeAst``'s was eager and raising, and the old ``.matches`` / ``.split``
    # / ``.index_terms`` methods live on the consumers instead
    # (``CodePattern.match_source`` / ``match_code``, ``RecursiveSplitter.split``,
    # ``index_terms``).
    if name == "CodeAst":
        _warnings.warn(
            "CodeAst is deprecated and now aliases CodeSource; use CodeSource "
            "with CodePattern.match_source / match_code, RecursiveSplitter.split, "
            "and index_terms",
            DeprecationWarning,
            stacklevel=2,
        )
        return CodeSource
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

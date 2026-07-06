"""Source views: partial, elided views of a source file — synthetic text plus
source-grounded segments. The shared output schema of structural chunking and
(future) code-match rendering. Design: ``specs/source_view/spec.md``.
"""

import typing as _typing
from dataclasses import dataclass as _dataclass

from cocoindex.resources.chunk import TextPosition as _TextPosition

__all__ = ["SourceView", "ViewSegment"]


@_dataclass(frozen=True, slots=True)
class ViewSegment:
    """One contiguous piece of a source view's text, grounded in the source.

    The view's ``text`` is exactly the in-order concatenation of each segment's
    rendering: ``summary`` if present, else the source slice ``start``..``end``.
    """

    start: _TextPosition
    """Start of the covered source range."""

    end: _TextPosition
    """End of the covered source range."""

    kind: _typing.Literal["frame", "content"]
    """"frame": context header repeated from enclosing scopes; "content": the view's own material."""

    summary: str | None
    """When present, stands in for the covered source in the view's text (a folded
    layer's overview, a truncated frame line)."""

    rendered_start: int
    """Char offset where this segment's rendering starts in the view's ``text``
    (the rendering is ``summary`` if present, else the source slice)."""

    rendered_end: int
    """Char offset where this segment's rendering ends in the view's ``text``.
    Segments' renderings form a contiguous partition of ``text``: use these to map
    parts of the synthetic text (e.g. its line numbers) back to segments and their
    source positions."""


@_dataclass(frozen=True, slots=True)
class SourceView:
    """A source view: synthetic text plus its source-grounded segments."""

    text: str
    """Full synthetic text — what gets embedded / displayed."""

    segments: list[ViewSegment]
    """Ordered segments; ranges are ascending and pairwise disjoint within a view."""

    @property
    def start(self) -> _TextPosition:
        """Start of the view's own content (frames excluded) — use for citations."""
        for seg in self.segments:
            if seg.kind == "content":
                return seg.start
        return self.segments[0].start

    @property
    def end(self) -> _TextPosition:
        """End of the view's own content (frames excluded) — use for citations."""
        for seg in reversed(self.segments):
            if seg.kind == "content":
                return seg.end
        return self.segments[-1].end

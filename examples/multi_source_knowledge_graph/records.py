"""The records a source produces: the contract with the shared phase (graph.py).

Documents mention entities; entities are what they mention.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Document:
    """One item of any document source."""

    source: str
    key: str  # stable within the source
    kind: str  # "doc" | "issue" | "pull_request" | "release"
    title: str
    text: str
    url: str
    author: str
    status: str
    updated_at: str
    # Entity keys the source already knows structurally, on top of what the
    # resolver finds in the text (e.g. the scope of a conventional-commit title).
    refs: tuple[str, ...] = ()


@dataclass(frozen=True)
class Entity:
    """One item of any entity source."""

    key: str
    name: str
    kind: str
    area: str
    depends_on: tuple[str, ...] = ()

"""Reference resolution: the first shared step after a record exists.

``Catalog.resolve`` maps free text to entity keys by whole-word alias matching.
It is the entity-resolution seam of the pipeline: replace it with an embedding
or LLM resolver — or a call to an external oracle — and nothing else changes.
"""

from __future__ import annotations

import functools
import re
from collections.abc import Iterable
from dataclasses import dataclass

import cocoindex as coco


@dataclass
class CatalogRow:
    """A full row of the catalog table."""

    key: str
    name: str
    kind: str
    area: str
    aliases: list[str]
    depends_on: list[str]


@dataclass(frozen=True)
class Catalog:
    """Every way an entity may be written, mapped to its key."""

    aliases: tuple[tuple[str, str], ...]  # (alias, entity key), sorted

    @classmethod
    def from_rows(cls, rows: Iterable[CatalogRow]) -> Catalog:
        pairs = {
            (alias.lower(), row.key)
            for row in rows
            for alias in (row.key, row.name, *row.aliases)
        }
        return cls(aliases=tuple(sorted(pairs)))

    def __coco_memo_key__(self) -> object:
        return self.aliases

    @functools.cached_property
    def _lookup(self) -> dict[str, str]:
        return dict(self.aliases)

    @functools.cached_property
    def keys(self) -> frozenset[str]:
        return frozenset(self._lookup.values())

    @functools.cached_property
    def _pattern(self) -> re.Pattern[str]:
        # Longest alias first so "oci object storage" wins over "oci".
        alternatives = "|".join(
            re.escape(alias) for alias in sorted(self._lookup, key=len, reverse=True)
        )
        # Letters and digits delimit a word; "_" and "-" do not, so
        # "postgres_target" and "amazon-s3" still match.
        return re.compile(rf"(?<![a-z0-9])(?:{alternatives})(?![a-z0-9])")

    def resolve(self, text: str) -> set[str]:
        """Entity keys mentioned in ``text``."""
        return {self._lookup[m.group(0)] for m in self._pattern.finditer(text.lower())}


# detect_change=True: when the aliases change, every record that resolved
# references against the catalog is re-processed.
CATALOG = coco.ContextKey[Catalog]("catalog", detect_change=True)

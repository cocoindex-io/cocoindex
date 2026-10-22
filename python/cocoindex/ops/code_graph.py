"""Syntactic C/C++ graph facts from the shared native Tree-sitter parser.

References describe source spellings, not resolved symbols. No compiler, include
search, overload selection, macro expansion, or receiver type inference runs.
"""

__all__ = ["CodeEntity", "CodeGraph", "CodeReference", "extract_code_graph"]

import collections as _collections
import hashlib as _hashlib
import json as _json
import pathlib as _pathlib
from dataclasses import dataclass as _dataclass

from cocoindex._internal import core as _core
from cocoindex.ops.code import CodeSource as _CodeSource


@_dataclass(frozen=True)
class CodeEntity:
    """One syntactic declaration, with a file-scoped identity and UTF-8 byte span."""

    id: str
    file_path: str
    kind: str
    name: str
    signature: str
    start_byte: int
    end_byte: int
    parent_id: str | None
    text: str


@_dataclass(frozen=True)
class CodeReference:
    """An unresolved call or base-type spelling at a particular source site.

    ``source_id`` identifies the lexical owner when present. It never identifies
    a resolved callee or base class. ``kind`` is ``call`` or ``inherits``.
    """

    id: str
    file_path: str
    kind: str
    spelling: str
    start_byte: int
    end_byte: int
    source_id: str | None
    resolution: str = "unresolved"


@_dataclass(frozen=True)
class CodeGraph:
    """Per-file declarations and references, including a parse-recovery flag."""

    entities: tuple[CodeEntity, ...]
    references: tuple[CodeReference, ...]
    has_errors: bool


def _identity(*parts: str | int | None) -> str:
    return _hashlib.sha256(
        _json.dumps(parts, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def extract_code_graph(source: _CodeSource, *, file_path: str) -> CodeGraph:
    """Extract C/C++ facts, reusing ``source``'s lazily parsed AST.

    ``file_path`` is a nonempty repository-relative POSIX path, used as the
    identity namespace. IDs survive changes to byte positions and function
    bodies. A changed declaration header, rename, or move can change
    IDs. Identical repeated headers are distinguished by occurrence order;
    inserting an identical declaration can therefore change those IDs.

    Only lexical containment is resolved. Calls and inheritance retain exact
    spellings as unresolved reference nodes, even if a similarly named entity
    exists in this file. Preprocessor subtrees and error subtrees are omitted.
    ``has_errors`` reports parser recovery; this is not a completeness guarantee.
    Unsupported languages raise ``ValueError``.

    Memoized callers must bump their ``@coco.fn(version=...)`` when upgrading
    native extraction semantics or grammars, which Python code tracking cannot
    inspect. See ``examples/code_graph_neo4j`` for a versioned per-file pipeline.
    """
    path = _pathlib.PurePosixPath(file_path)
    if not file_path or path.is_absolute() or ".." in path.parts or str(path) == ".":
        raise ValueError("file_path must be a nonempty repository-relative POSIX path")
    if "\\" in file_path:
        raise ValueError("file_path must use POSIX separators")
    file_path = path.as_posix()
    declarations, references, has_errors = _core.extract_code_graph(source._src)
    encoded = source.text.encode("utf-8")
    counts: _collections.Counter[tuple[str, ...]] = _collections.Counter()
    by_start: dict[int, str] = {}
    entities = []
    for kind, name, signature, start, end, parent_start in declarations:
        parent_id = by_start.get(parent_start) if parent_start is not None else None
        key: tuple[str, ...] = (
            "entity",
            file_path,
            kind,
            name,
            signature,
            parent_id or "",
        )
        occurrence = counts[key]
        counts[key] += 1
        entity_id = _identity(*key, occurrence)
        by_start[start] = entity_id
        entities.append(
            CodeEntity(
                entity_id,
                file_path,
                kind,
                name,
                signature,
                start,
                end,
                parent_id,
                encoded[start:end].decode("utf-8"),
            )
        )
    refs = []
    for kind, spelling, start, end, source_start in references:
        source_id = by_start.get(source_start) if source_start is not None else None
        key = ("reference", file_path, source_id or "", kind, spelling)
        occurrence = counts[key]
        counts[key] += 1
        refs.append(
            CodeReference(
                _identity(*key, occurrence),
                file_path,
                kind,
                spelling,
                start,
                end,
                source_id,
            )
        )
    return CodeGraph(tuple(entities), tuple(refs), has_errors)

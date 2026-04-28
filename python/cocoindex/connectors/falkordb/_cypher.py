"""
Pure Cypher generation for the FalkorDB connector.

This module has no runtime dependency on the ``falkordb`` driver and no I/O —
every function returns a Cypher string suitable for ``graph.query(cypher, params)``
where ``params`` is the dict assembled by the caller.

All identifiers (labels, property names, index field names) are validated by the
caller before being passed in; values always bind via ``$``-parameters.
"""

from __future__ import annotations

import re
from typing import Sequence

__all__ = [
    "IDENTIFIER_RE",
    "build_node_upsert",
    "build_node_delete",
    "build_relationship_upsert",
    "build_relationship_delete",
    "build_node_index_create",
    "build_node_index_drop",
    "build_relationship_index_create",
    "build_relationship_index_drop",
    "build_vector_index_create",
    "build_vector_index_drop",
    "validate_identifier",
]


IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def validate_identifier(name: str, kind: str) -> None:
    """Reject anything that isn't ``[a-zA-Z_][a-zA-Z0-9_]*``.

    Cypher labels and property names cannot be parameter-bound, so untrusted
    names must be validated at API entry — never escaped at query construction
    time.
    """
    if not IDENTIFIER_RE.match(name):
        raise ValueError(
            f"Invalid FalkorDB {kind}: {name!r}. Must match [a-zA-Z_][a-zA-Z0-9_]*."
        )


def _quote(name: str) -> str:
    """Backtick-wrap an already-validated identifier for inline use in Cypher."""
    return f"`{name}`"


def _key_clause(prefix: str, fields: Sequence[str], var: str) -> str:
    """Build ``{<f1>: $<prefix>_0, <f2>: $<prefix>_1, ...}`` for a MATCH/MERGE pattern.

    ``var`` is unused here but accepted so callers can self-document intent
    (e.g. ``var="n"`` makes it clear this clause attaches to ``n``).
    """
    parts = [f"{_quote(f)}: ${prefix}_{i}" for i, f in enumerate(fields)]
    return "{" + ", ".join(parts) + "}"


def build_node_upsert(
    label: str,
    pk_fields: Sequence[str],
    has_value_fields: bool,
) -> str:
    """``MERGE (n:`Label` {pk: $key_0, ...}) [SET n += $props]``.

    ``has_value_fields`` controls whether the ``SET n += $props`` clause is
    emitted. Caller passes ``True`` when there is at least one non-PK column to
    write; otherwise the MERGE alone suffices.
    """
    if not pk_fields:
        raise ValueError("build_node_upsert requires at least one primary key field")
    cypher = f"MERGE (n:{_quote(label)} {_key_clause('key', pk_fields, 'n')})"
    if has_value_fields:
        cypher += " SET n += $props"
    return cypher


def build_node_delete(label: str, pk_fields: Sequence[str]) -> str:
    """``MATCH (n:`Label` {pk: $key_0, ...}) DETACH DELETE n``.

    DETACH DELETE removes any incident edges as a safety measure for nodes that
    are also referenced as relationship endpoints — without it, the DELETE
    would fail on nodes that still have edges.
    """
    if not pk_fields:
        raise ValueError("build_node_delete requires at least one primary key field")
    return (
        f"MATCH (n:{_quote(label)} {_key_clause('key', pk_fields, 'n')}) "
        f"DETACH DELETE n"
    )


def build_relationship_upsert(
    rel_type: str,
    from_label: str,
    from_pk_fields: Sequence[str],
    to_label: str,
    to_pk_fields: Sequence[str],
    rel_pk_fields: Sequence[str],
    has_value_fields: bool,
) -> str:
    """Three MERGEs: source endpoint, target endpoint, then the relationship.

    Endpoint properties are NOT touched — they are owned by their table's own
    record handler. We only ``SET r += $props`` on the relationship itself.
    """
    if not from_pk_fields or not to_pk_fields or not rel_pk_fields:
        raise ValueError(
            "build_relationship_upsert requires PK fields for from, to, and the relationship"
        )
    cypher = (
        f"MERGE (s:{_quote(from_label)} {_key_clause('from_key', from_pk_fields, 's')}) "
        f"MERGE (t:{_quote(to_label)} {_key_clause('to_key', to_pk_fields, 't')}) "
        f"MERGE (s)-[r:{_quote(rel_type)} {_key_clause('rel_key', rel_pk_fields, 'r')}]->(t)"
    )
    if has_value_fields:
        cypher += " SET r += $props"
    return cypher


def build_relationship_delete(rel_type: str, pk_fields: Sequence[str]) -> str:
    """``MATCH ()-[r:`RelType` {pk: $key_0, ...}]->() DELETE r``.

    Endpoints are intentionally not deleted — they're tracked by their own
    table handlers and will be deleted by their own reconciler if orphaned.
    """
    if not pk_fields:
        raise ValueError(
            "build_relationship_delete requires at least one primary key field"
        )
    return (
        f"MATCH ()-[r:{_quote(rel_type)} "
        f"{_key_clause('key', pk_fields, 'r')}]->() DELETE r"
    )


def build_node_index_create(label: str, fields: Sequence[str]) -> str:
    """``CREATE INDEX FOR (e:`Label`) ON (e.`f1`, e.`f2`, ...)``."""
    if not fields:
        raise ValueError("build_node_index_create requires at least one field")
    field_list = ", ".join(f"e.{_quote(f)}" for f in fields)
    return f"CREATE INDEX FOR (e:{_quote(label)}) ON ({field_list})"


def build_node_index_drop(label: str, fields: Sequence[str]) -> str:
    """``DROP INDEX FOR (e:`Label`) ON (e.`f1`, ...)``."""
    if not fields:
        raise ValueError("build_node_index_drop requires at least one field")
    field_list = ", ".join(f"e.{_quote(f)}" for f in fields)
    return f"DROP INDEX FOR (e:{_quote(label)}) ON ({field_list})"


def build_relationship_index_create(rel_type: str, fields: Sequence[str]) -> str:
    """``CREATE INDEX FOR ()-[e:`RelType`]-() ON (e.`f1`, ...)``."""
    if not fields:
        raise ValueError("build_relationship_index_create requires at least one field")
    field_list = ", ".join(f"e.{_quote(f)}" for f in fields)
    return f"CREATE INDEX FOR ()-[e:{_quote(rel_type)}]-() ON ({field_list})"


def build_relationship_index_drop(rel_type: str, fields: Sequence[str]) -> str:
    """``DROP INDEX FOR ()-[e:`RelType`]-() ON (e.`f1`, ...)``."""
    if not fields:
        raise ValueError("build_relationship_index_drop requires at least one field")
    field_list = ", ".join(f"e.{_quote(f)}" for f in fields)
    return f"DROP INDEX FOR ()-[e:{_quote(rel_type)}]-() ON ({field_list})"


def build_vector_index_create(
    label: str,
    field: str,
    dimension: int,
    metric: str,
) -> str:
    """``CREATE VECTOR INDEX FOR (e:`Label`) ON (e.`field`) OPTIONS {...}``.

    ``metric`` is the FalkorDB-side ``similarityFunction`` value
    (e.g. ``"cosine"``, ``"euclidean"``). Caller is responsible for translating
    user-facing names into the FalkorDB vocabulary before invoking.
    """
    if dimension <= 0:
        raise ValueError(f"Invalid vector dimension: {dimension}")
    return (
        f"CREATE VECTOR INDEX FOR (e:{_quote(label)}) ON (e.{_quote(field)}) "
        f"OPTIONS {{dimension: {int(dimension)}, similarityFunction: '{metric}'}}"
    )


def build_vector_index_drop(label: str, field: str) -> str:
    """``DROP VECTOR INDEX FOR (e:`Label`) ON (e.`field`)``.

    Confirmed via spike against FalkorDB latest: the DROP statement does NOT
    take an index name — it identifies the index by (label, field).
    """
    return f"DROP VECTOR INDEX FOR (e:{_quote(label)}) ON (e.{_quote(field)})"

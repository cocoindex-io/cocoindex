"""
Pure Cypher generation for the Neo4j connector.

This module has no runtime dependency on the ``neo4j`` driver and no I/O —
every function returns a Cypher string suitable for ``tx.run(cypher, **params)``
where ``params`` is the dict assembled by the caller.

All identifiers (labels, property names, index names) are validated by the
caller before being passed in; values always bind via ``$``-parameters.

Targets Neo4j 5.18+ (CREATE VECTOR INDEX DDL shipped in 5.18; older
versions need the db.index.vector.createNodeIndex procedure instead).
"""

from __future__ import annotations

import re
from typing import Sequence

__all__ = [
    "IDENTIFIER_RE",
    "build_constraint_create",
    "build_constraint_drop",
    "build_node_delete",
    "build_node_index_create",
    "build_node_index_drop",
    "build_node_upsert",
    "build_relationship_delete",
    "build_relationship_index_create",
    "build_relationship_index_drop",
    "build_relationship_upsert",
    "build_vector_index_create",
    "build_vector_index_drop",
    "constraint_name",
    "index_name",
    "validate_identifier",
    "vector_index_name",
]


IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def validate_identifier(name: str, kind: str) -> None:
    """Reject anything that isn't ``[a-zA-Z_][a-zA-Z0-9_]*``.

    Cypher labels, property names, and index names cannot be parameter-bound,
    so untrusted names must be validated at API entry — never escaped at query
    construction time.
    """
    if not IDENTIFIER_RE.match(name):
        raise ValueError(
            f"Invalid Neo4j {kind}: {name!r}. Must match [a-zA-Z_][a-zA-Z0-9_]*."
        )


def _quote(name: str) -> str:
    """Backtick-wrap an already-validated identifier for inline use in Cypher."""
    return f"`{name}`"


def _key_clause(prefix: str, fields: Sequence[str]) -> str:
    """Build ``{<f1>: $<prefix>_0, <f2>: $<prefix>_1, ...}`` for a MATCH/MERGE pattern."""
    parts = [f"{_quote(f)}: ${prefix}_{i}" for i, f in enumerate(fields)]
    return "{" + ", ".join(parts) + "}"


def index_name(kind: str, label: str, fields: Sequence[str]) -> str:
    """Deterministic index name for a (kind, label, fields) triple.

    Neo4j 5 indexes are identified by name on DROP, so the connector mints
    one at CREATE time and persists it in the tracking record. ``kind`` is
    one of ``"node"`` / ``"rel"``.
    """
    field_part = "__".join(fields)
    return f"coco_idx_{kind}_{label}__{field_part}"


def constraint_name(label: str, fields: Sequence[str]) -> str:
    """Deterministic constraint name for a (label, fields) pair."""
    field_part = "__".join(fields)
    return f"coco_uniq_{label}__{field_part}"


def vector_index_name(label: str, field: str) -> str:
    """Deterministic vector index name for a (label, field) pair."""
    return f"coco_vec_{label}__{field}"


def build_node_upsert(
    label: str,
    pk_fields: Sequence[str],
    has_value_fields: bool,
) -> str:
    """``MERGE (n:`Label` {pk: $key_0, ...}) [SET n += $props]``.

    Same shape as FalkorDB — Neo4j 5 understands the literal property
    pattern in MERGE just fine.
    """
    if not pk_fields:
        raise ValueError("build_node_upsert requires at least one primary key field")
    cypher = f"MERGE (n:{_quote(label)} {_key_clause('key', pk_fields)})"
    if has_value_fields:
        cypher += " SET n += $props"
    return cypher


def build_node_delete(label: str, pk_fields: Sequence[str]) -> str:
    """``MATCH (n:`Label` {pk: $key_0, ...}) DETACH DELETE n``.

    DETACH DELETE removes any incident edges as a safety measure for nodes
    that are also referenced as relationship endpoints — without it, the
    DELETE would fail on nodes that still have edges.
    """
    if not pk_fields:
        raise ValueError("build_node_delete requires at least one primary key field")
    return f"MATCH (n:{_quote(label)} {_key_clause('key', pk_fields)}) DETACH DELETE n"


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
        f"MERGE (s:{_quote(from_label)} {_key_clause('from_key', from_pk_fields)}) "
        f"MERGE (t:{_quote(to_label)} {_key_clause('to_key', to_pk_fields)}) "
        f"MERGE (s)-[r:{_quote(rel_type)} {_key_clause('rel_key', rel_pk_fields)}]->(t)"
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
        f"MATCH ()-[r:{_quote(rel_type)} {_key_clause('key', pk_fields)}]->() DELETE r"
    )


def build_node_index_create(
    name: str,
    label: str,
    fields: Sequence[str],
) -> str:
    """``CREATE INDEX <name> IF NOT EXISTS FOR (n:`Label`) ON (n.`f1`, n.`f2`, ...)``.

    Neo4j requires named indexes; ``IF NOT EXISTS`` makes setup idempotent.
    """
    if not fields:
        raise ValueError("build_node_index_create requires at least one field")
    field_list = ", ".join(f"n.{_quote(f)}" for f in fields)
    return (
        f"CREATE INDEX {_quote(name)} IF NOT EXISTS "
        f"FOR (n:{_quote(label)}) ON ({field_list})"
    )


def build_node_index_drop(name: str) -> str:
    """``DROP INDEX <name> IF EXISTS``.

    Unlike FalkorDB's by-(label, field) drop, Neo4j drops indexes by name
    regardless of kind (node or relationship).
    """
    return f"DROP INDEX {_quote(name)} IF EXISTS"


def build_relationship_index_create(
    name: str,
    rel_type: str,
    fields: Sequence[str],
) -> str:
    """``CREATE INDEX <name> IF NOT EXISTS FOR ()-[r:`RelType`]-() ON (r.`f1`, ...)``."""
    if not fields:
        raise ValueError("build_relationship_index_create requires at least one field")
    field_list = ", ".join(f"r.{_quote(f)}" for f in fields)
    return (
        f"CREATE INDEX {_quote(name)} IF NOT EXISTS "
        f"FOR ()-[r:{_quote(rel_type)}]-() ON ({field_list})"
    )


def build_relationship_index_drop(name: str) -> str:
    """``DROP INDEX <name> IF EXISTS``.

    Same DROP statement as for node indexes — Neo4j unifies the namespace.
    """
    return f"DROP INDEX {_quote(name)} IF EXISTS"


def build_constraint_create(
    name: str,
    label: str,
    fields: Sequence[str],
) -> str:
    """``CREATE CONSTRAINT <name> IF NOT EXISTS FOR (n:`Label`) REQUIRE n.`f1` IS UNIQUE``.

    For a single-field PK uses ``REQUIRE n.f IS UNIQUE``; for compound PKs
    uses ``REQUIRE (n.f1, n.f2) IS NODE KEY`` (Neo4j 5 syntax).
    """
    if not fields:
        raise ValueError("build_constraint_create requires at least one field")
    if len(fields) == 1:
        field_expr = f"n.{_quote(fields[0])} IS UNIQUE"
    else:
        field_list = ", ".join(f"n.{_quote(f)}" for f in fields)
        field_expr = f"({field_list}) IS NODE KEY"
    return (
        f"CREATE CONSTRAINT {_quote(name)} IF NOT EXISTS "
        f"FOR (n:{_quote(label)}) REQUIRE {field_expr}"
    )


def build_constraint_drop(name: str) -> str:
    """``DROP CONSTRAINT <name> IF EXISTS``."""
    return f"DROP CONSTRAINT {_quote(name)} IF EXISTS"


def build_vector_index_create(
    name: str,
    label: str,
    field: str,
    dimension: int,
    metric: str,
) -> str:
    """``CREATE VECTOR INDEX <name> IF NOT EXISTS FOR (n:`Label`) ON n.`field` OPTIONS {...}``.

    ``metric`` is the Neo4j ``vector.similarity_function`` value
    (``"cosine"`` or ``"euclidean"``). Caller is responsible for translating
    user-facing names into the Neo4j vocabulary before invoking.
    """
    if dimension <= 0:
        raise ValueError(f"Invalid vector dimension: {dimension}")
    return (
        f"CREATE VECTOR INDEX {_quote(name)} IF NOT EXISTS "
        f"FOR (n:{_quote(label)}) ON n.{_quote(field)} "
        f"OPTIONS {{ indexConfig: {{ "
        f"`vector.dimensions`: {int(dimension)}, "
        f"`vector.similarity_function`: '{metric}' }} }}"
    )


def build_vector_index_drop(name: str) -> str:
    """``DROP INDEX <name> IF EXISTS``.

    Vector indexes share the index namespace; the same DROP works.
    """
    return f"DROP INDEX {_quote(name)} IF EXISTS"

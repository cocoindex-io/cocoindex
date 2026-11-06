"""
Pure Cypher generation for the LadybugDB connector.

This module has no runtime dependency on the ``ladybug`` driver and no I/O —
every function returns a Cypher string suitable for
``conn.execute(cypher, params)`` where ``params`` is the dict assembled by
the caller.

All identifiers (node table names, relationship table names, property names)
are validated by the caller before being passed in; values always bind via
``$``-parameters.

LadybugDB dialect notes (verified against ladybug 0.22.x, Kuzu-derived):

- Identifiers may be backtick-quoted; labels are case-insensitive.
- There is no ``SET n += $props`` map form — every property write is a
  per-property ``SET n.`field` = $param`` assignment. Upsert builders
  therefore take the value field list, not just a boolean.
- ``MERGE`` with a primary-key pattern is idempotent, so upserts need no
  ``ON CREATE`` / ``ON MATCH`` clauses.
- Node tables are created with an explicit schema and ``PRIMARY KEY``;
  relationship tables carry ``FROM`` / ``TO`` endpoint labels.
- There is no secondary or vector index DDL — ``PRIMARY KEY`` is the only
  index. Vectors live in fixed-size ``DOUBLE[n]`` / ``FLOAT[n]`` columns
  and are compared with functions like ``array_cosine_similarity``.
- Schema evolution uses ``ALTER TABLE ... ADD`` / ``DROP`` column.
- ``DROP TABLE`` on a node table fails while a relationship table still
  references it, so relationship tables are always dropped first.
"""

from __future__ import annotations

import re
from typing import Sequence

__all__ = [
    "IDENTIFIER_RE",
    "build_add_column",
    "build_drop_column",
    "build_node_delete",
    "build_node_delete_all",
    "build_node_table_create",
    "build_node_upsert",
    "build_rel_table_create",
    "build_relationship_delete",
    "build_relationship_delete_all",
    "build_relationship_upsert",
    "build_table_drop",
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
            f"Invalid LadybugDB {kind}: {name!r}. Must match [a-zA-Z_][a-zA-Z0-9_]*."
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


def _set_clause(var: str, fields: Sequence[str], prefix: str) -> str:
    """Build ``SET <var>.`f1` = $<prefix>_0, ...`` for per-property writes."""
    return "SET " + ", ".join(
        f"{var}.{_quote(f)} = ${prefix}_{i}" for i, f in enumerate(fields)
    )


def build_node_upsert(
    label: str,
    pk_fields: Sequence[str],
    value_fields: Sequence[str],
) -> str:
    """``MERGE (n:`Label` {pk: $key_0, ...}) [SET n.`f` = $prop_0, ...]``.

    LadybugDB has no ``SET n += $props`` map form, so every non-PK column is
    assigned individually. With no value fields the MERGE alone suffices.
    """
    if not pk_fields:
        raise ValueError("build_node_upsert requires at least one primary key field")
    cypher = f"MERGE (n:{_quote(label)} {_key_clause('key', pk_fields, 'n')})"
    if value_fields:
        cypher += " " + _set_clause("n", value_fields, "prop")
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


def build_node_delete_all(label: str) -> str:
    """``MATCH (n:`Label`) DETACH DELETE n``.

    Destroys a node table's contents: every node carrying the label goes,
    together with the relationships attached to it.
    """
    return f"MATCH (n:{_quote(label)}) DETACH DELETE n"


def build_relationship_upsert(
    rel_type: str,
    from_label: str,
    from_pk_fields: Sequence[str],
    to_label: str,
    to_pk_fields: Sequence[str],
    rel_pk_fields: Sequence[str],
    value_fields: Sequence[str],
    has_legacy_key: bool,
) -> str:
    """MERGE the source endpoint, the target endpoint, then the relationship.

    Before the relationship MERGE, any other relationship of the type with
    this primary key is deleted: the key names one edge, so one found between
    other endpoints is the edge before its endpoints changed (the MERGE would
    otherwise create a second edge beside it). With ``has_legacy_key``, the
    edge between these endpoints keyed by ``$legacy_key_*`` is deleted too.

    Endpoint properties are NOT touched — they are owned by their table's own
    record handler. Only the relationship's own non-PK fields are SET.
    """
    if not from_pk_fields or not to_pk_fields or not rel_pk_fields:
        raise ValueError(
            "build_relationship_upsert requires PK fields for from, to, and the relationship"
        )
    rel = _quote(rel_type)
    rel_key = _key_clause("rel_key", rel_pk_fields, "r")
    cypher = (
        f"MERGE (s:{_quote(from_label)} {_key_clause('from_key', from_pk_fields, 's')}) "
        f"MERGE (t:{_quote(to_label)} {_key_clause('to_key', to_pk_fields, 't')}) "
        f"WITH s, t "
        f"OPTIONAL MATCH (a)-[o:{rel} {_key_clause('rel_key', rel_pk_fields, 'o')}]->(b) "
        f"WHERE a <> s OR b <> t "
        f"DELETE o "
        f"WITH DISTINCT s, t "
    )
    if has_legacy_key:
        cypher += (
            f"OPTIONAL MATCH (s)-[l:{rel} {_key_clause('legacy_key', rel_pk_fields, 'l')}]->(t) "
            f"DELETE l "
            f"WITH DISTINCT s, t "
        )
    cypher += f"MERGE (s)-[r:{rel} {rel_key}]->(t)"
    if value_fields:
        cypher += " " + _set_clause("r", value_fields, "rel_prop")
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


def build_relationship_delete_all(rel_type: str) -> str:
    """``MATCH ()-[r:`RelType`]->() DELETE r``.

    Destroys a relationship table's contents: every relationship of the type
    goes; the endpoints stay, as with the per-record delete.
    """
    return f"MATCH ()-[r:{_quote(rel_type)}]->() DELETE r"


def build_node_table_create(
    label: str,
    columns: Sequence[tuple[str, str]],
    pk_field: str,
) -> str:
    """``CREATE NODE TABLE `Label` (`f1` T1, ..., PRIMARY KEY (`pk`))``."""
    if not columns:
        raise ValueError("build_node_table_create requires at least one column")
    col_defs = ", ".join(f"{_quote(name)} {ddl}" for name, ddl in columns)
    return f"CREATE NODE TABLE {_quote(label)} ({col_defs}, PRIMARY KEY ({_quote(pk_field)}))"


def build_rel_table_create(
    rel_type: str,
    from_label: str,
    to_label: str,
    columns: Sequence[tuple[str, str]],
) -> str:
    """``CREATE REL TABLE `Type` (FROM `A` TO `B`[, `f1` T1, ...])``."""
    props = "".join(f", {_quote(name)} {ddl}" for name, ddl in columns)
    return (
        f"CREATE REL TABLE {_quote(rel_type)} "
        f"(FROM {_quote(from_label)} TO {_quote(to_label)}{props})"
    )


def build_table_drop(name: str) -> str:
    """``DROP TABLE `Name``` — drops a node or relationship table with its data."""
    return f"DROP TABLE {_quote(name)}"


def build_add_column(table_name: str, column_name: str, column_ddl: str) -> str:
    """``ALTER TABLE `T` ADD `c` <DDL>``."""
    return f"ALTER TABLE {_quote(table_name)} ADD {_quote(column_name)} {column_ddl}"


def build_drop_column(table_name: str, column_name: str) -> str:
    """``ALTER TABLE `T` DROP `c```."""
    return f"ALTER TABLE {_quote(table_name)} DROP {_quote(column_name)}"

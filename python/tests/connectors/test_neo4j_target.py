"""Tests for the Neo4j target connector.

Run with:
    uv run pytest python/tests/connectors/test_neo4j_target.py -v

Unit tests run without a server. Integration tests require a running Neo4j
(spun up automatically via testcontainers when ``NEO4J_TEST_SERVER=1``).
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from typing import Any, Self, cast

import pytest
import pytest_asyncio

import cocoindex as coco
from cocoindex.connectors.neo4j._cypher import (
    build_constraint_create,
    build_constraint_drop,
    build_node_delete,
    build_node_delete_all,
    build_node_delete_batch,
    build_node_index_create,
    build_node_index_drop,
    build_node_upsert,
    build_node_upsert_batch,
    build_relationship_delete,
    build_relationship_delete_all,
    build_relationship_delete_batch,
    build_relationship_index_create,
    build_relationship_index_drop,
    build_relationship_upsert,
    build_relationship_upsert_batch,
    build_vector_index_create,
    build_vector_index_drop,
    constraint_name,
    index_name,
    validate_identifier,
    vector_index_name,
)

from tests import common

coco_env = common.create_test_env(__file__)
_NO_CONTEXT = cast(Any, None)


# =============================================================================
# Skip gates
# =============================================================================

try:
    import neo4j as _neo4j  # type: ignore[import-not-found]  # noqa: F401

    HAS_NEO4J = True
except ImportError:
    HAS_NEO4J = False

requires_neo4j = pytest.mark.skipif(not HAS_NEO4J, reason="neo4j is not installed")

_HAS_NEO4J_SERVER = bool(os.environ.get("NEO4J_TEST_SERVER"))

requires_neo4j_server = pytest.mark.skipif(
    not (HAS_NEO4J and _HAS_NEO4J_SERVER),
    reason="NEO4J_TEST_SERVER is not set",
)


class _FakeTransaction:
    def __init__(self, driver: _FakeDriver) -> None:
        self._driver = driver
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.commit_count = 0
        self.rollback_count = 0

    async def run(self, cypher: str, **params: Any) -> None:
        self.calls.append((cypher, params))
        self._driver.query_count += 1
        if self._driver.fail_on_query == self._driver.query_count:
            raise RuntimeError("simulated Neo4j write failure")

    async def commit(self) -> None:
        self.commit_count += 1

    async def rollback(self) -> None:
        self.rollback_count += 1


class _FakeSession:
    def __init__(self, driver: _FakeDriver) -> None:
        self._driver = driver

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def begin_transaction(self) -> _FakeTransaction:
        tx = _FakeTransaction(self._driver)
        self._driver.transactions.append(tx)
        return tx


class _FakeDriver:
    def __init__(self) -> None:
        self.database: str | None = None
        self.session_count = 0
        self.transactions: list[_FakeTransaction] = []
        self.query_count = 0
        self.fail_on_query: int | None = None

    def session(self, *, database: str) -> _FakeSession:
        self.database = database
        self.session_count += 1
        return _FakeSession(self)


def _make_record_action(
    *,
    table_name: str,
    is_relation: bool,
    pk_field: str,
    record_id: Any,
    value: dict[str, Any] | None,
    from_label: str | None = None,
    from_pk_field: str | None = None,
    from_id: Any | None = None,
    to_label: str | None = None,
    to_pk_field: str | None = None,
    to_id: Any | None = None,
) -> Any:
    from cocoindex.connectors.neo4j import _target as neo_target

    return neo_target._RecordAction(
        table_name=table_name,
        is_relation=is_relation,
        pk_field=pk_field,
        record_id=record_id,
        value=value,
        from_label=from_label,
        from_pk_field=from_pk_field,
        from_id=from_id,
        to_label=to_label,
        to_pk_field=to_pk_field,
        to_id=to_id,
    )


def _fake_applier() -> tuple[Any, _FakeDriver]:
    from cocoindex.connectors.neo4j import _target as neo_target

    driver = _FakeDriver()
    graph = neo_target._GraphHandle(driver, "neo4j")  # type: ignore[arg-type]
    return neo_target._SharedRecordApplier(graph), driver


if HAS_NEO4J:
    from cocoindex.connectors import neo4j as neo  # type: ignore[attr-defined]

    KG_DB: coco.ContextKey[Any] = coco.ContextKey("test_neo4j_kg")


# =============================================================================
# Cypher builder unit tests — no DB needed, no driver needed
# =============================================================================


class TestValidateIdentifier:
    @pytest.mark.parametrize(
        "name", ["users", "_private", "T1", "a_b_c", "X", "Document", "MENTION"]
    )
    def test_valid(self, name: str) -> None:
        validate_identifier(name, "test")

    @pytest.mark.parametrize(
        "name",
        ["my-table", "123abc", "", "has space", "ba`ck", "semi;colon", "a.b", "X-Y"],
    )
    def test_invalid(self, name: str) -> None:
        with pytest.raises(ValueError, match="Invalid Neo4j"):
            validate_identifier(name, "test")


class TestNameBuilders:
    def test_index_name_node(self) -> None:
        assert index_name("node", "Document", ["filename"]) == (
            "coco_idx_node_Document__filename"
        )

    def test_index_name_relationship(self) -> None:
        assert index_name("rel", "MENTION", ["id"]) == "coco_idx_rel_MENTION__id"

    def test_index_name_compound_pk(self) -> None:
        assert index_name("node", "X", ["a", "b"]) == "coco_idx_node_X__a__b"

    def test_constraint_name(self) -> None:
        assert constraint_name("Document", ["filename"]) == (
            "coco_uniq_Document__filename"
        )

    def test_vector_index_name(self) -> None:
        assert vector_index_name("Document", "embedding") == (
            "coco_vec_Document__embedding"
        )


class TestNodeUpsertCypher:
    def test_single_pk_with_props(self) -> None:
        assert (
            build_node_upsert("Document", ["filename"], True)
            == "MERGE (n:`Document` {`filename`: $key_0}) SET n += $props"
        )

    def test_single_pk_no_props(self) -> None:
        assert (
            build_node_upsert("Document", ["filename"], False)
            == "MERGE (n:`Document` {`filename`: $key_0})"
        )

    def test_compound_pk(self) -> None:
        # Same shape as FalkorDB; Neo4j MERGE accepts compound keys.
        assert build_node_upsert("X", ["a", "b"], True) == (
            "MERGE (n:`X` {`a`: $key_0, `b`: $key_1}) SET n += $props"
        )

    def test_empty_pk_raises(self) -> None:
        with pytest.raises(ValueError):
            build_node_upsert("X", [], True)


class TestBatchedCypher:
    def test_node_upsert_uses_unwind_and_row_params(self) -> None:
        assert build_node_upsert_batch("Document", ["filename"]) == (
            "UNWIND $data AS row\n"
            "MERGE (n:`Document` {`filename`: row.key_0}) "
            "SET n += row.props"
        )

    def test_node_delete_uses_unwind(self) -> None:
        assert build_node_delete_batch("Document", ["filename"]) == (
            "UNWIND $data AS row\n"
            "MATCH (n:`Document` {`filename`: row.key_0}) DETACH DELETE n"
        )

    def test_relationship_upsert_uses_unwind(self) -> None:
        assert build_relationship_upsert_batch(
            "REL", "A", ["x"], "B", ["y"], ["id"]
        ) == (
            "UNWIND $data AS row\n"
            "MERGE (s:`A` {`x`: row.from_key_0}) "
            "MERGE (t:`B` {`y`: row.to_key_0}) "
            "MERGE (s)-[r:`REL` {`id`: row.rel_key_0}]->(t) "
            "SET r += row.props"
        )

    def test_relationship_delete_uses_unwind(self) -> None:
        assert build_relationship_delete_batch("REL", ["id"]) == (
            "UNWIND $data AS row\nMATCH ()-[r:`REL` {`id`: row.key_0}]->() DELETE r"
        )

    def test_batch_builders_reject_empty_pk(self) -> None:
        with pytest.raises(ValueError):
            build_node_upsert_batch("X", [])
        with pytest.raises(ValueError):
            build_node_delete_batch("X", [])
        with pytest.raises(ValueError):
            build_relationship_upsert_batch("REL", "A", ["x"], "B", ["y"], [])
        with pytest.raises(ValueError):
            build_relationship_delete_batch("REL", [])


class TestNodeDeleteCypher:
    def test_uses_detach_delete(self) -> None:
        # DETACH DELETE protects against DELETE failing on nodes that still
        # have incident edges another flow owns.
        assert (
            build_node_delete("Document", ["filename"])
            == "MATCH (n:`Document` {`filename`: $key_0}) DETACH DELETE n"
        )

    def test_empty_pk_raises(self) -> None:
        with pytest.raises(ValueError):
            build_node_delete("X", [])

    def test_delete_all_detaches_in_batched_transactions(self) -> None:
        # Table teardown: every node carrying the label, with its relationships,
        # in Neo4j-default-sized inner transactions (auto-commit session only).
        assert build_node_delete_all("Document") == (
            "MATCH (n:`Document`) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS"
        )


class TestRelationshipUpsertCypher:
    def test_three_merges_with_props(self) -> None:
        assert build_relationship_upsert(
            "REL", "Entity", ["value"], "Entity", ["value"], ["id"], True
        ) == (
            "MERGE (s:`Entity` {`value`: $from_key_0}) "
            "MERGE (t:`Entity` {`value`: $to_key_0}) "
            "MERGE (s)-[r:`REL` {`id`: $rel_key_0}]->(t) "
            "SET r += $props"
        )

    def test_no_set_on_endpoints(self) -> None:
        # Endpoints' properties are owned by their own table's _RecordHandler.
        cypher = build_relationship_upsert("REL", "A", ["x"], "B", ["y"], ["id"], True)
        assert "SET s" not in cypher
        assert "SET t" not in cypher
        assert "SET r += $props" in cypher

    def test_no_props(self) -> None:
        assert build_relationship_upsert(
            "REL", "A", ["x"], "B", ["y"], ["id"], False
        ) == (
            "MERGE (s:`A` {`x`: $from_key_0}) "
            "MERGE (t:`B` {`y`: $to_key_0}) "
            "MERGE (s)-[r:`REL` {`id`: $rel_key_0}]->(t)"
        )

    def test_empty_pk_raises(self) -> None:
        with pytest.raises(ValueError):
            build_relationship_upsert("REL", "A", [], "B", ["y"], ["id"], True)
        with pytest.raises(ValueError):
            build_relationship_upsert("REL", "A", ["x"], "B", [], ["id"], True)
        with pytest.raises(ValueError):
            build_relationship_upsert("REL", "A", ["x"], "B", ["y"], [], True)


class TestRelationshipDeleteCypher:
    def test_does_not_cascade(self) -> None:
        cypher = build_relationship_delete("REL", ["id"])
        assert cypher == "MATCH ()-[r:`REL` {`id`: $key_0}]->() DELETE r"
        assert "DELETE s" not in cypher
        assert "DELETE t" not in cypher

    def test_empty_pk_raises(self) -> None:
        with pytest.raises(ValueError):
            build_relationship_delete("REL", [])

    def test_delete_all_keeps_endpoints(self) -> None:
        cypher = build_relationship_delete_all("REL")
        assert cypher == (
            "MATCH ()-[r:`REL`]->() CALL { WITH r DELETE r } IN TRANSACTIONS"
        )
        assert "DETACH" not in cypher


class TestIndexDdlCypher:
    def test_node_index_create_named_with_if_not_exists(self) -> None:
        # Neo4j 5 syntax: CREATE INDEX <name> IF NOT EXISTS FOR (n:L) ON (n.f)
        assert build_node_index_create(
            "coco_idx_node_Document__filename", "Document", ["filename"]
        ) == (
            "CREATE INDEX `coco_idx_node_Document__filename` IF NOT EXISTS "
            "FOR (n:`Document`) ON (n.`filename`)"
        )

    def test_node_index_create_compound(self) -> None:
        assert build_node_index_create("idx_x", "X", ["a", "b"]) == (
            "CREATE INDEX `idx_x` IF NOT EXISTS FOR (n:`X`) ON (n.`a`, n.`b`)"
        )

    def test_node_index_drop_uses_name(self) -> None:
        # Unlike FalkorDB's by-(label,field) drop, Neo4j drops by name.
        assert build_node_index_drop("coco_idx_node_Document__filename") == (
            "DROP INDEX `coco_idx_node_Document__filename` IF EXISTS"
        )

    def test_relationship_index_create(self) -> None:
        assert build_relationship_index_create(
            "coco_idx_rel_REL__id", "REL", ["id"]
        ) == (
            "CREATE INDEX `coco_idx_rel_REL__id` IF NOT EXISTS "
            "FOR ()-[r:`REL`]-() ON (r.`id`)"
        )

    def test_relationship_index_drop_uses_name(self) -> None:
        assert build_relationship_index_drop("coco_idx_rel_REL__id") == (
            "DROP INDEX `coco_idx_rel_REL__id` IF EXISTS"
        )


class TestConstraintDdlCypher:
    def test_single_field_creates_unique_constraint(self) -> None:
        assert build_constraint_create(
            "coco_uniq_Document__filename", "Document", ["filename"]
        ) == (
            "CREATE CONSTRAINT `coco_uniq_Document__filename` IF NOT EXISTS "
            "FOR (n:`Document`) REQUIRE n.`filename` IS UNIQUE"
        )

    def test_compound_creates_node_key(self) -> None:
        # Neo4j 5: REQUIRE (n.a, n.b) IS NODE KEY
        assert build_constraint_create("c", "X", ["a", "b"]) == (
            "CREATE CONSTRAINT `c` IF NOT EXISTS "
            "FOR (n:`X`) REQUIRE (n.`a`, n.`b`) IS NODE KEY"
        )

    def test_drop(self) -> None:
        assert build_constraint_drop("coco_uniq_Document__filename") == (
            "DROP CONSTRAINT `coco_uniq_Document__filename` IF EXISTS"
        )

    def test_empty_fields_raises(self) -> None:
        with pytest.raises(ValueError):
            build_constraint_create("c", "X", [])


class TestVectorIndexCypher:
    def test_create(self) -> None:
        assert build_vector_index_create(
            "coco_vec_Doc__embedding", "Doc", "embedding", 384, "cosine"
        ) == (
            "CREATE VECTOR INDEX `coco_vec_Doc__embedding` IF NOT EXISTS "
            "FOR (n:`Doc`) ON n.`embedding` "
            "OPTIONS { indexConfig: { "
            "`vector.dimensions`: 384, "
            "`vector.similarity_function`: 'cosine' } }"
        )

    def test_drop_uses_name(self) -> None:
        # Neo4j vector indexes share the index namespace, drop by name.
        assert build_vector_index_drop("coco_vec_Doc__embedding") == (
            "DROP INDEX `coco_vec_Doc__embedding` IF EXISTS"
        )

    def test_zero_dimension_rejected(self) -> None:
        with pytest.raises(ValueError):
            build_vector_index_create("c", "Doc", "embedding", 0, "cosine")

    def test_negative_dimension_rejected(self) -> None:
        with pytest.raises(ValueError):
            build_vector_index_create("c", "Doc", "embedding", -1, "cosine")


# =============================================================================
# Identifier-validation-at-API-entry tests (require neo4j package, no server)
# =============================================================================


@requires_neo4j
class TestIdentifierValidationAtApiEntryPoints:
    def test_table_schema_invalid_column(self) -> None:
        with pytest.raises(ValueError, match="column name"):
            neo.TableSchema(
                columns={
                    "id": neo.ColumnDef(type="STRING"),
                    "bad-name": neo.ColumnDef(type="STRING"),
                },
                primary_key="id",
            )

    def test_table_schema_pk_must_exist_in_columns(self) -> None:
        with pytest.raises(ValueError, match="primary_key"):
            neo.TableSchema(
                columns={"id": neo.ColumnDef(type="STRING")},
                primary_key="missing",
            )

    def test_table_target_invalid_name(self) -> None:
        with pytest.raises(ValueError, match="table name"):
            neo.table_target(KG_DB, "bad-table")

    def test_relation_target_invalid_name(self) -> None:
        from typing import cast

        with pytest.raises(ValueError, match="relation table name"):
            neo.relation_target(
                KG_DB,
                "bad-rel",
                cast(Any, None),
                cast(Any, None),
            )

    def test_connection_factory_invalid_database_name(self) -> None:
        with pytest.raises(ValueError, match="database name"):
            neo.ConnectionFactory(uri="bolt://localhost:7687", database="bad name")


# =============================================================================
# TableSchema.from_class type mapping
# =============================================================================


@requires_neo4j
class TestTableSchemaFromClass:
    @pytest.mark.asyncio
    async def test_basic_dataclass(self) -> None:
        @dataclass
        class Row:
            id: str
            count: int
            score: float
            flag: bool

        schema = await neo.TableSchema.from_class(Row, primary_key="id")
        assert schema.primary_key == "id"
        assert schema.columns["id"].type == "STRING"
        assert schema.columns["count"].type == "INTEGER"
        assert schema.columns["score"].type == "FLOAT"
        assert schema.columns["flag"].type == "BOOLEAN"
        assert schema.value_field_names == ["count", "score", "flag"]

    @pytest.mark.asyncio
    async def test_custom_pk(self) -> None:
        @dataclass
        class Doc:
            filename: str
            title: str

        schema = await neo.TableSchema.from_class(Doc, primary_key="filename")
        assert schema.primary_key == "filename"
        assert schema.value_field_names == ["title"]


# =============================================================================
# Table reconciliation unit tests (no DB needed)
# =============================================================================


@requires_neo4j
class TestTableReconcile:
    def test_schema_evolution_under_full_reprocess(self) -> None:
        """Schema evolution during full reprocess must compute column actions and mark lossy."""
        from cocoindex.connectorkits import statediff, target
        from cocoindex.connectors.neo4j import _target as neo_target

        handler = neo_target._TableHandler()
        schema_v1 = neo.TableSchema(
            columns={
                "id": neo.ColumnDef("STRING"),
                "a": neo.ColumnDef("STRING"),
                "b": neo.ColumnDef("STRING"),
            },
            primary_key="id",
        )
        spec_v1 = neo_target._TableSpec(
            table_schema=schema_v1,
            primary_key="id",
            is_relation=False,
            from_label=None,
            from_pk_field=None,
            to_label=None,
            to_pk_field=None,
            managed_by=target.ManagedBy.SYSTEM,
        )

        out_v1 = handler.reconcile(
            neo_target._TableKey("db", "Doc"), spec_v1, [], False
        )
        assert out_v1 is not None
        assert out_v1.action.main_action is None
        assert isinstance(out_v1.tracking_record, statediff.MutualTrackingRecord)
        tracking_v1 = out_v1.tracking_record

        # Evolve schema: drop 'b', add 'c'
        schema_v2 = neo.TableSchema(
            columns={
                "id": neo.ColumnDef("STRING"),
                "a": neo.ColumnDef("STRING"),
                "c": neo.ColumnDef("STRING"),
            },
            primary_key="id",
        )
        spec_v2 = neo_target._TableSpec(
            table_schema=schema_v2,
            primary_key="id",
            is_relation=False,
            from_label=None,
            from_pk_field=None,
            to_label=None,
            to_pk_field=None,
            managed_by=target.ManagedBy.SYSTEM,
        )

        # Incremental run
        out_inc = handler.reconcile(
            neo_target._TableKey("db", "Doc"),
            spec_v2,
            [tracking_v1],
            False,
        )
        assert out_inc is not None
        assert out_inc.action.main_action is None
        assert out_inc.action.column_actions == {
            "field:b": "delete",
            "field:c": "insert",
        }
        assert out_inc.child_invalidation == "lossy"

        # Full reprocess run (prev_may_be_missing=True)
        out_reproc = handler.reconcile(
            neo_target._TableKey("db", "Doc"),
            spec_v2,
            [tracking_v1],
            True,
        )
        assert out_reproc is not None
        assert out_reproc.action.main_action == "upsert"
        assert out_reproc.action.column_actions == {
            "field:id": "upsert",
            "field:a": "upsert",
            "field:b": "delete",
            "field:c": "insert",
        }
        assert out_reproc.child_invalidation == "lossy"

    def test_column_addition_only_not_lossy_on_incremental(self) -> None:
        """Adding a column on incremental update is not lossy, but under full reprocess re-upserts."""
        from cocoindex.connectorkits import statediff, target
        from cocoindex.connectors.neo4j import _target as neo_target

        handler = neo_target._TableHandler()
        schema_v1 = neo.TableSchema(
            columns={
                "id": neo.ColumnDef("STRING"),
                "a": neo.ColumnDef("STRING"),
            },
            primary_key="id",
        )
        spec_v1 = neo_target._TableSpec(
            table_schema=schema_v1,
            primary_key="id",
            is_relation=False,
            from_label=None,
            from_pk_field=None,
            to_label=None,
            to_pk_field=None,
            managed_by=target.ManagedBy.SYSTEM,
        )

        out_v1 = handler.reconcile(
            neo_target._TableKey("db", "Doc"), spec_v1, [], False
        )
        assert out_v1 is not None
        assert isinstance(out_v1.tracking_record, statediff.MutualTrackingRecord)
        tracking_v1 = out_v1.tracking_record

        schema_v2 = neo.TableSchema(
            columns={
                "id": neo.ColumnDef("STRING"),
                "a": neo.ColumnDef("STRING"),
                "b": neo.ColumnDef("STRING"),
            },
            primary_key="id",
        )
        spec_v2 = neo_target._TableSpec(
            table_schema=schema_v2,
            primary_key="id",
            is_relation=False,
            from_label=None,
            from_pk_field=None,
            to_label=None,
            to_pk_field=None,
            managed_by=target.ManagedBy.SYSTEM,
        )

        out_inc = handler.reconcile(
            neo_target._TableKey("db", "Doc"),
            spec_v2,
            [tracking_v1],
            False,
        )
        assert out_inc is not None
        assert out_inc.action.main_action is None
        assert out_inc.action.column_actions == {"field:b": "insert"}
        assert out_inc.child_invalidation is None

        out_reproc = handler.reconcile(
            neo_target._TableKey("db", "Doc"),
            spec_v2,
            [tracking_v1],
            True,
        )
        assert out_reproc is not None
        assert out_reproc.action.main_action == "upsert"
        assert out_reproc.action.column_actions == {
            "field:id": "upsert",
            "field:a": "upsert",
            "field:b": "insert",
        }
        assert out_reproc.child_invalidation == "lossy"

    def test_only_identity_changes_rebuild_the_table(self) -> None:
        """A rebuild deletes every node of the label, so only changes the
        per-record reconcile can't absorb may trigger one: the primary key
        field (and the node/relation kind or endpoints). Attaching a schema
        or changing a property type re-upserts records at most."""
        from cocoindex.connectorkits import statediff, target
        from cocoindex.connectors.neo4j import _target as neo_target

        def spec(
            columns: dict[str, neo.ColumnDef] | None, primary_key: str
        ) -> neo_target._TableSpec:
            schema = (
                neo.TableSchema(columns=columns, primary_key=primary_key)
                if columns is not None
                else None
            )
            return neo_target._TableSpec(
                table_schema=schema,
                primary_key=primary_key,
                is_relation=False,
                from_label=None,
                from_pk_field=None,
                to_label=None,
                to_pk_field=None,
                managed_by=target.ManagedBy.SYSTEM,
            )

        handler = neo_target._TableHandler()
        key = neo_target._TableKey("db", "Doc")

        out = handler.reconcile(key, spec(None, "id"), [], False)
        assert out is not None and out.action.main_action is None
        schemaless = out.tracking_record
        assert isinstance(schemaless, statediff.MutualTrackingRecord)

        # Schemaless -> schema: the new fields show up, nothing is rebuilt.
        with_schema = spec(
            {"id": neo.ColumnDef("STRING"), "a": neo.ColumnDef("STRING")}, "id"
        )
        out = handler.reconcile(key, with_schema, [schemaless], False)
        assert out is not None
        assert out.action.main_action is None
        assert out.action.column_actions == {
            "field:id": "insert",
            "field:a": "insert",
        }
        assert out.child_invalidation is None
        with_schema_tracking = out.tracking_record
        assert isinstance(with_schema_tracking, statediff.MutualTrackingRecord)

        # Primary key type change: lossy (records re-upsert), not a rebuild.
        int_pk = spec(
            {"id": neo.ColumnDef("INTEGER"), "a": neo.ColumnDef("STRING")}, "id"
        )
        out_int = handler.reconcile(key, int_pk, [with_schema_tracking], False)
        assert out_int is not None
        assert out_int.action.main_action is None
        assert out_int.action.column_actions == {"field:id": "replace"}
        assert out_int.child_invalidation == "lossy"

        # Primary key field change: the table is rebuilt from scratch.
        new_pk = spec(
            {"id": neo.ColumnDef("STRING"), "a": neo.ColumnDef("STRING")}, "a"
        )
        out_pk = handler.reconcile(key, new_pk, [with_schema_tracking], False)
        assert out_pk is not None
        assert out_pk.action.main_action == "replace"
        assert out_pk.child_invalidation == "destructive"
        assert out_pk.action.prev_pk_field == "id"


@requires_neo4j
class TestSharedRecordApplierBatching:
    @pytest.mark.asyncio
    async def test_empty_batch_opens_no_transaction(self) -> None:
        applier, driver = _fake_applier()

        await applier._apply_actions(_NO_CONTEXT, [])

        assert driver.session_count == 0
        assert driver.transactions == []

    @pytest.mark.asyncio
    async def test_single_node_upsert_is_one_batch_query(self) -> None:
        applier, driver = _fake_applier()
        action = _make_record_action(
            table_name="Document",
            is_relation=False,
            pk_field="id",
            record_id="doc-1",
            value={"id": "doc-1", "title": "A"},
        )

        await applier._apply_actions(_NO_CONTEXT, [action])

        assert len(driver.transactions) == 1
        tx = driver.transactions[0]
        assert tx.commit_count == 1
        assert tx.rollback_count == 0
        assert tx.calls == [
            (
                build_node_upsert_batch("Document", ["id"]),
                {"data": [{"key_0": "doc-1", "props": {"title": "A"}}]},
            )
        ]

    @pytest.mark.asyncio
    async def test_groups_by_node_label_and_preserves_duplicate_keys(self) -> None:
        applier, driver = _fake_applier()
        actions = [
            _make_record_action(
                table_name="Document",
                is_relation=False,
                pk_field="id",
                record_id="doc-1",
                value={"id": "doc-1", "title": "first"},
            ),
            _make_record_action(
                table_name="Document",
                is_relation=False,
                pk_field="id",
                record_id="doc-2",
                value={"id": "doc-2", "title": "second"},
            ),
            _make_record_action(
                table_name="Document",
                is_relation=False,
                pk_field="id",
                record_id="doc-1",
                value={"id": "doc-1", "title": "last"},
            ),
            _make_record_action(
                table_name="Other",
                is_relation=False,
                pk_field="id",
                record_id="other-1",
                value={"id": "other-1", "title": "other"},
            ),
        ]

        await applier._apply_actions(_NO_CONTEXT, actions)

        tx = driver.transactions[0]
        assert len(tx.calls) == 2
        assert tx.calls[0] == (
            build_node_upsert_batch("Document", ["id"]),
            {
                "data": [
                    {"key_0": "doc-1", "props": {"title": "first"}},
                    {"key_0": "doc-2", "props": {"title": "second"}},
                    {"key_0": "doc-1", "props": {"title": "last"}},
                ]
            },
        )
        assert tx.calls[1] == (
            build_node_upsert_batch("Other", ["id"]),
            {"data": [{"key_0": "other-1", "props": {"title": "other"}}]},
        )

    @pytest.mark.asyncio
    async def test_groups_by_relationship_endpoints_and_type(self) -> None:
        applier, driver = _fake_applier()
        actions = [
            _make_record_action(
                table_name="Person",
                is_relation=False,
                pk_field="id",
                record_id="p1",
                value={"id": "p1"},
            ),
            _make_record_action(
                table_name="Company",
                is_relation=False,
                pk_field="id",
                record_id="c1",
                value={"id": "c1"},
            ),
            _make_record_action(
                table_name="WORKS_AT",
                is_relation=True,
                pk_field="id",
                record_id="r1",
                value={"id": "r1", "role": "engineer"},
                from_label="Person",
                from_pk_field="id",
                from_id="p1",
                to_label="Company",
                to_pk_field="id",
                to_id="c1",
            ),
            _make_record_action(
                table_name="KNOWS",
                is_relation=True,
                pk_field="id",
                record_id="r2",
                value={"id": "r2", "since": 2020},
                from_label="Person",
                from_pk_field="id",
                from_id="p1",
                to_label="Person",
                to_pk_field="id",
                to_id="p2",
            ),
        ]

        await applier._apply_actions(_NO_CONTEXT, actions)

        tx = driver.transactions[0]
        assert len(tx.calls) == 4
        assert tx.calls[0][0] == build_node_upsert_batch("Person", ["id"])
        assert tx.calls[1][0] == build_node_upsert_batch("Company", ["id"])
        assert tx.calls[2] == (
            build_relationship_upsert_batch(
                "WORKS_AT", "Person", ["id"], "Company", ["id"], ["id"]
            ),
            {
                "data": [
                    {
                        "from_key_0": "p1",
                        "to_key_0": "c1",
                        "rel_key_0": "r1",
                        "props": {"role": "engineer"},
                    }
                ]
            },
        )
        assert tx.calls[3][0] == build_relationship_upsert_batch(
            "KNOWS", "Person", ["id"], "Person", ["id"], ["id"]
        )

    @pytest.mark.asyncio
    async def test_update_delete_buckets_keep_existing_order(self) -> None:
        applier, driver = _fake_applier()
        actions = [
            _make_record_action(
                table_name="Document",
                is_relation=False,
                pk_field="id",
                record_id="doc-1",
                value={"id": "doc-1", "title": "updated"},
            ),
            _make_record_action(
                table_name="OldNode",
                is_relation=False,
                pk_field="id",
                record_id="old-node",
                value=None,
            ),
            _make_record_action(
                table_name="REL",
                is_relation=True,
                pk_field="id",
                record_id="rel-1",
                value={"id": "rel-1", "weight": 2},
                from_label="Document",
                from_pk_field="id",
                from_id="doc-1",
                to_label="Document",
                to_pk_field="id",
                to_id="doc-2",
            ),
            _make_record_action(
                table_name="OLD_REL",
                is_relation=True,
                pk_field="id",
                record_id="old-rel",
                value=None,
            ),
        ]

        await applier._apply_actions(_NO_CONTEXT, actions)

        tx = driver.transactions[0]
        assert [cypher for cypher, _ in tx.calls] == [
            build_node_upsert_batch("Document", ["id"]),
            build_relationship_upsert_batch(
                "REL", "Document", ["id"], "Document", ["id"], ["id"]
            ),
            build_relationship_delete_batch("OLD_REL", ["id"]),
            build_node_delete_batch("OldNode", ["id"]),
        ]
        assert tx.calls[-2][1] == {"data": [{"key_0": "old-rel"}]}
        assert tx.calls[-1][1] == {"data": [{"key_0": "old-node"}]}

    @pytest.mark.asyncio
    async def test_same_key_update_then_delete_keeps_delete_last(self) -> None:
        applier, driver = _fake_applier()
        actions = [
            _make_record_action(
                table_name="Document",
                is_relation=False,
                pk_field="id",
                record_id="doc-1",
                value={"id": "doc-1", "title": "updated"},
            ),
            _make_record_action(
                table_name="Document",
                is_relation=False,
                pk_field="id",
                record_id="doc-1",
                value=None,
            ),
        ]

        await applier._apply_actions(_NO_CONTEXT, actions)

        tx = driver.transactions[0]
        assert tx.calls == [
            (
                build_node_upsert_batch("Document", ["id"]),
                {"data": [{"key_0": "doc-1", "props": {"title": "updated"}}]},
            ),
            (
                build_node_delete_batch("Document", ["id"]),
                {"data": [{"key_0": "doc-1"}]},
            ),
        ]

    @pytest.mark.asyncio
    async def test_groups_are_split_into_bounded_chunks(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from cocoindex.connectors.neo4j import _target as neo_target

        monkeypatch.setattr(neo_target, "_NEO4J_MAX_UNWIND_ROWS", 2)
        applier, driver = _fake_applier()
        actions = [
            _make_record_action(
                table_name="Document",
                is_relation=False,
                pk_field="id",
                record_id=f"doc-{i}",
                value={"id": f"doc-{i}"},
            )
            for i in range(5)
        ]

        await applier._apply_actions(_NO_CONTEXT, actions)

        tx = driver.transactions[0]
        assert [len(params["data"]) for _, params in tx.calls] == [2, 2, 1]
        assert sum(len(params["data"]) for _, params in tx.calls) == 5

    @pytest.mark.asyncio
    async def test_failed_batch_rolls_back_and_can_be_retried(self) -> None:
        applier, driver = _fake_applier()
        actions = [
            _make_record_action(
                table_name="Document",
                is_relation=False,
                pk_field="id",
                record_id="doc-1",
                value={"id": "doc-1"},
            ),
            _make_record_action(
                table_name="Other",
                is_relation=False,
                pk_field="id",
                record_id="other-1",
                value={"id": "other-1"},
            ),
        ]
        driver.fail_on_query = 2

        with pytest.raises(RuntimeError, match="simulated Neo4j write failure"):
            await applier._apply_actions(_NO_CONTEXT, actions)

        failed_tx = driver.transactions[0]
        assert failed_tx.commit_count == 0
        assert failed_tx.rollback_count == 1
        assert len(failed_tx.calls) == 2

        driver.fail_on_query = None
        await applier._apply_actions(_NO_CONTEXT, actions)

        retried_tx = driver.transactions[1]
        assert retried_tx.commit_count == 1
        assert retried_tx.rollback_count == 0
        assert len(retried_tx.calls) == 2


# =============================================================================
# Integration tests — require running Neo4j (testcontainers spins one up)
# =============================================================================


@pytest.fixture(scope="module")
def neo4j_uri_auth() -> Iterator[tuple[str, tuple[str, str]]]:
    """Spin up a Neo4j 5.13 container once per test module."""
    if not (HAS_NEO4J and _HAS_NEO4J_SERVER):
        pytest.skip("NEO4J_TEST_SERVER is not set")

    from testcontainers.community.neo4j import Neo4jContainer  # type: ignore[import-untyped]

    container = Neo4jContainer(
        "neo4j:5.26-community", username="neo4j", password="cocoindex"
    )
    container.start()
    try:
        uri = container.get_connection_url()
        auth = ("neo4j", "cocoindex")
        yield uri, auth
    finally:
        container.stop()


@pytest_asyncio.fixture
async def neo4j_clean(
    neo4j_uri_auth: tuple[str, tuple[str, str]],
) -> AsyncIterator[tuple[str, tuple[str, str]]]:
    """Wipe the default `neo4j` database before each test.

    Neo4j community has a single mutable database; isolation between tests
    is by truncation rather than by separate database name.
    """
    uri, auth = neo4j_uri_auth
    driver = _neo4j.AsyncGraphDatabase.driver(uri, auth=auth)
    async with driver.session(database="neo4j") as session:
        await session.run("MATCH (n) DETACH DELETE n")
        # Drop any constraints/indexes the previous test left behind.
        result = await session.run("SHOW CONSTRAINTS YIELD name")
        names = [r["name"] async for r in result]
        for n in names:
            await session.run(f"DROP CONSTRAINT `{n}` IF EXISTS")
        result = await session.run("SHOW INDEXES YIELD name, type")
        idx = [(r["name"], r["type"]) async for r in result]
        for n, t in idx:
            if t == "LOOKUP":
                continue  # auto-created, can't drop
            await session.run(f"DROP INDEX `{n}` IF EXISTS")
    await driver.close()
    yield uri, auth


async def _read_nodes(
    uri: str, auth: tuple[str, str], label: str
) -> list[dict[str, Any]]:
    driver = _neo4j.AsyncGraphDatabase.driver(uri, auth=auth)
    try:
        async with driver.session(database="neo4j") as session:
            result = await session.run(
                f"MATCH (n:`{label}`) RETURN properties(n) AS props"
            )
            return [r["props"] async for r in result]
    finally:
        await driver.close()


async def _query(uri: str, auth: tuple[str, str], cypher: str) -> list[dict[str, Any]]:
    driver = _neo4j.AsyncGraphDatabase.driver(uri, auth=auth)
    try:
        async with driver.session(database="neo4j") as session:
            result = await session.run(cypher)
            return [dict(r) async for r in result]
    finally:
        await driver.close()


async def _count(uri: str, auth: tuple[str, str], cypher: str) -> int:
    """Run ``cypher`` (which must ``RETURN count(...) AS c``) and return it."""
    (row,) = await _query(uri, auth, cypher)
    return int(row["c"])


async def _coco_schema_names(uri: str, auth: tuple[str, str]) -> set[str]:
    """Names of the constraints and indexes this connector created."""
    rows = await _query(uri, auth, "SHOW CONSTRAINTS YIELD name RETURN name")
    rows += await _query(uri, auth, "SHOW INDEXES YIELD name RETURN name")
    return {r["name"] for r in rows if r["name"].startswith("coco_")}


async def _read_relationships(
    uri: str, auth: tuple[str, str], rel_type: str
) -> list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]]:
    driver = _neo4j.AsyncGraphDatabase.driver(uri, auth=auth)
    try:
        async with driver.session(database="neo4j") as session:
            result = await session.run(
                f"MATCH (s)-[r:`{rel_type}`]->(t) "
                f"RETURN properties(s) AS s, properties(t) AS t, properties(r) AS r"
            )
            return [(row["s"], row["t"], row["r"]) async for row in result]
    finally:
        await driver.close()


# Module-level state shared with declare functions (mirrors falkordb pattern).
_node_rows: list[Any] = []
_rel_pairs: list[tuple[Any, Any]] = []
_doc_pk: str = "filename"
_tables_declared: bool = True
_entity_with_schema: bool = True


if HAS_NEO4J:

    @dataclass
    class Document:
        filename: str
        title: str
        summary: str

    @dataclass
    class Entity:
        value: str

    @dataclass
    class RelRow:
        id: str
        predicate: str

    async def _declare_documents_only() -> None:
        schema = await neo.TableSchema.from_class(Document, primary_key="filename")
        table: Any = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "doc_table"),
            neo.mount_table_target,  # type: ignore[arg-type]
            KG_DB,
            "Document",
            schema,
            primary_key="filename",
        )
        for row in _node_rows:
            table.declare_record(row=row)

    async def _declare_documents_keyed_by_doc_pk() -> None:
        schema = await neo.TableSchema.from_class(Document, primary_key=_doc_pk)
        table: Any = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "doc_table"),
            neo.mount_table_target,  # type: ignore[arg-type]
            KG_DB,
            "Document",
            schema,
            primary_key=_doc_pk,
        )
        for row in _node_rows:
            table.declare_record(row=row)

    async def _declare_entities_and_relationships() -> None:
        if not _tables_declared:
            return
        entity_schema = (
            await neo.TableSchema.from_class(Entity, primary_key="value")
            if _entity_with_schema
            else None
        )
        rel_schema = await neo.TableSchema.from_class(RelRow, primary_key="id")
        entity_table: Any = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "entity_table"),
            neo.mount_table_target,
            KG_DB,
            "Entity",
            entity_schema,
            primary_key="value",
        )
        rel_table: Any = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "rel_table"),
            neo.mount_relation_target,
            KG_DB,
            "REL",
            entity_table,
            entity_table,
            rel_schema,
            primary_key="id",
        )
        seen_entities: set[str] = set()
        for from_id, to_id in _rel_pairs:
            for v in (from_id, to_id):
                if v not in seen_entities:
                    entity_table.declare_record(row=Entity(value=v))
                    seen_entities.add(v)
            rel_table.declare_relation(
                from_id=from_id,
                to_id=to_id,
                record=RelRow(id=f"{from_id}->{to_id}", predicate="connects"),
            )


@requires_neo4j_server
@pytest.mark.asyncio
async def test_node_upsert_and_readback(
    neo4j_clean: tuple[str, tuple[str, str]],
) -> None:
    global _node_rows
    uri, auth = neo4j_clean
    _node_rows = [
        Document(filename="a.md", title="A", summary="alpha"),
        Document(filename="b.md", title="B", summary="beta"),
    ]
    coco_env.context_provider.provide(
        KG_DB, neo.ConnectionFactory(uri=uri, auth=auth, database="neo4j")
    )
    app = coco.App(
        coco.AppConfig(name="test_neo4j_node_upsert", environment=coco_env),
        _declare_documents_only,
    )
    await app.update()

    rows = await _read_nodes(uri, auth, "Document")
    by_fn = {r["filename"]: r for r in rows}
    assert set(by_fn) == {"a.md", "b.md"}
    assert by_fn["a.md"]["title"] == "A"
    assert by_fn["a.md"]["summary"] == "alpha"


@requires_neo4j_server
@pytest.mark.asyncio
async def test_reconcile_twice_is_noop(
    neo4j_clean: tuple[str, tuple[str, str]],
) -> None:
    """Second update with identical input must not produce duplicate writes."""
    global _node_rows
    uri, auth = neo4j_clean
    _node_rows = [Document(filename="a.md", title="A", summary="alpha")]
    coco_env.context_provider.provide(
        KG_DB, neo.ConnectionFactory(uri=uri, auth=auth, database="neo4j")
    )
    app = coco.App(
        coco.AppConfig(name="test_neo4j_noop", environment=coco_env),
        _declare_documents_only,
    )
    await app.update()
    rows1 = await _read_nodes(uri, auth, "Document")
    await app.update()  # identical input
    rows2 = await _read_nodes(uri, auth, "Document")
    assert rows1 == rows2
    assert len(rows2) == 1


@requires_neo4j_server
@pytest.mark.asyncio
async def test_modify_value_triggers_one_upsert(
    neo4j_clean: tuple[str, tuple[str, str]],
) -> None:
    global _node_rows
    uri, auth = neo4j_clean
    _node_rows = [Document(filename="a.md", title="A", summary="alpha")]
    coco_env.context_provider.provide(
        KG_DB, neo.ConnectionFactory(uri=uri, auth=auth, database="neo4j")
    )
    app = coco.App(
        coco.AppConfig(name="test_neo4j_modify", environment=coco_env),
        _declare_documents_only,
    )
    await app.update()
    _node_rows[0] = Document(filename="a.md", title="A", summary="ALPHA-2")
    await app.update()
    rows = await _read_nodes(uri, auth, "Document")
    assert len(rows) == 1
    assert rows[0]["summary"] == "ALPHA-2"


@requires_neo4j_server
@pytest.mark.asyncio
async def test_delete_removes_node(
    neo4j_clean: tuple[str, tuple[str, str]],
) -> None:
    global _node_rows
    uri, auth = neo4j_clean
    _node_rows = [
        Document(filename="a.md", title="A", summary="alpha"),
        Document(filename="b.md", title="B", summary="beta"),
    ]
    coco_env.context_provider.provide(
        KG_DB, neo.ConnectionFactory(uri=uri, auth=auth, database="neo4j")
    )
    app = coco.App(
        coco.AppConfig(name="test_neo4j_delete", environment=coco_env),
        _declare_documents_only,
    )
    await app.update()
    assert {r["filename"] for r in await _read_nodes(uri, auth, "Document")} == {
        "a.md",
        "b.md",
    }
    _node_rows.pop()  # drop b.md
    await app.update()
    assert {r["filename"] for r in await _read_nodes(uri, auth, "Document")} == {"a.md"}


@requires_neo4j_server
@pytest.mark.asyncio
async def test_drop_destroys_tables_with_their_contents(
    neo4j_clean: tuple[str, tuple[str, str]],
) -> None:
    """App.drop() destroys the system-managed tables: the nodes, the
    relationships, and the constraints/indexes the connector created."""
    global _rel_pairs, _tables_declared, _entity_with_schema
    uri, auth = neo4j_clean
    _rel_pairs = [("alice", "bob"), ("bob", "carol")]
    _tables_declared = True
    _entity_with_schema = True
    coco_env.context_provider.provide(
        KG_DB, neo.ConnectionFactory(uri=uri, auth=auth, database="neo4j")
    )
    app = coco.App(
        coco.AppConfig(name="test_neo4j_drop", environment=coco_env),
        _declare_entities_and_relationships,
    )
    await app.update()
    assert len(await _read_nodes(uri, auth, "Entity")) == 3
    assert len(await _read_relationships(uri, auth, "REL")) == 2
    assert await _coco_schema_names(uri, auth) == {
        "coco_uniq_Entity__value",
        "coco_idx_rel_REL__id",
    }

    await app.drop()

    assert await _count(uri, auth, "MATCH (n) RETURN count(n) AS c") == 0
    assert await _count(uri, auth, "MATCH ()-[r]->() RETURN count(r) AS c") == 0
    assert await _coco_schema_names(uri, auth) == set()


@requires_neo4j_server
@pytest.mark.asyncio
async def test_undeclared_tables_are_destroyed(
    neo4j_clean: tuple[str, tuple[str, str]],
) -> None:
    """Tables that are no longer declared go the same way as on drop."""
    global _rel_pairs, _tables_declared, _entity_with_schema
    uri, auth = neo4j_clean
    _rel_pairs = [("alice", "bob")]
    _tables_declared = True
    _entity_with_schema = True
    coco_env.context_provider.provide(
        KG_DB, neo.ConnectionFactory(uri=uri, auth=auth, database="neo4j")
    )
    app = coco.App(
        coco.AppConfig(name="test_neo4j_undeclare", environment=coco_env),
        _declare_entities_and_relationships,
    )
    await app.update()
    assert await _count(uri, auth, "MATCH (n) RETURN count(n) AS c") == 2

    _tables_declared = False
    await app.update()
    assert await _count(uri, auth, "MATCH (n) RETURN count(n) AS c") == 0
    assert await _coco_schema_names(uri, auth) == set()


@requires_neo4j_server
@pytest.mark.asyncio
async def test_primary_key_change_rebuilds_table(
    neo4j_clean: tuple[str, tuple[str, str]],
) -> None:
    """Changing the primary key field rebuilds the table: nodes keyed by the
    old field are deleted rather than left behind as untracked stragglers."""
    global _node_rows, _doc_pk
    uri, auth = neo4j_clean
    _node_rows = [
        Document(filename="a.md", title="A", summary="alpha"),
        Document(filename="b.md", title="B", summary="beta"),
    ]
    _doc_pk = "filename"
    coco_env.context_provider.provide(
        KG_DB, neo.ConnectionFactory(uri=uri, auth=auth, database="neo4j")
    )
    app = coco.App(
        coco.AppConfig(name="test_neo4j_pk_change", environment=coco_env),
        _declare_documents_keyed_by_doc_pk,
    )
    await app.update()
    assert await _coco_schema_names(uri, auth) == {"coco_uniq_Document__filename"}

    # Re-key by title, with new titles: the old nodes match neither key.
    _doc_pk = "title"
    _node_rows = [
        Document(filename="a.md", title="A2", summary="alpha"),
        Document(filename="b.md", title="B2", summary="beta"),
    ]
    await app.update()
    assert {r["title"] for r in await _read_nodes(uri, auth, "Document")} == {
        "A2",
        "B2",
    }
    assert await _coco_schema_names(uri, auth) == {"coco_uniq_Document__title"}


@requires_neo4j_server
@pytest.mark.asyncio
async def test_attaching_a_schema_keeps_nodes_and_relationships(
    neo4j_clean: tuple[str, tuple[str, str]],
) -> None:
    """Attaching a schema to a schemaless table is not a rebuild: the nodes
    stay, and so do the relationships of other tables attached to them."""
    global _rel_pairs, _tables_declared, _entity_with_schema
    uri, auth = neo4j_clean
    _rel_pairs = [("alice", "bob"), ("bob", "carol")]
    _tables_declared = True
    _entity_with_schema = False
    coco_env.context_provider.provide(
        KG_DB, neo.ConnectionFactory(uri=uri, auth=auth, database="neo4j")
    )
    app = coco.App(
        coco.AppConfig(name="test_neo4j_schema_attach", environment=coco_env),
        _declare_entities_and_relationships,
    )
    try:
        await app.update()
        assert len(await _read_relationships(uri, auth, "REL")) == 2

        _entity_with_schema = True
        await app.update()
        assert len(await _read_nodes(uri, auth, "Entity")) == 3
        assert len(await _read_relationships(uri, auth, "REL")) == 2
    finally:
        _entity_with_schema = True


@requires_neo4j_server
@pytest.mark.asyncio
async def test_relationship_upsert_with_endpoint_merge(
    neo4j_clean: tuple[str, tuple[str, str]],
) -> None:
    """Verify three-MERGE relationship insert: source endpoint, target endpoint, edge."""
    global _rel_pairs
    uri, auth = neo4j_clean
    _rel_pairs = [("alice", "bob"), ("bob", "carol")]
    coco_env.context_provider.provide(
        KG_DB, neo.ConnectionFactory(uri=uri, auth=auth, database="neo4j")
    )
    app = coco.App(
        coco.AppConfig(name="test_neo4j_rel_upsert", environment=coco_env),
        _declare_entities_and_relationships,
    )
    await app.update()

    nodes = await _read_nodes(uri, auth, "Entity")
    assert {n["value"] for n in nodes} == {"alice", "bob", "carol"}

    edges = await _read_relationships(uri, auth, "REL")
    assert len(edges) == 2
    pairs = {(s["value"], t["value"]) for s, t, _ in edges}
    assert pairs == {("alice", "bob"), ("bob", "carol")}
    for _, _, rel in edges:
        assert rel["predicate"] == "connects"


@requires_neo4j_server
@pytest.mark.asyncio
async def test_batched_apply_mixed_groups_and_deletes(
    neo4j_clean: tuple[str, tuple[str, str]],
) -> None:
    """One applier call batches multiple labels, endpoints, and relationship types."""
    from cocoindex.connectors.neo4j import _target as neo_target

    uri, auth = neo4j_clean
    graph = await neo.ConnectionFactory(uri=uri, auth=auth, database="neo4j").acquire()
    applier = neo_target._SharedRecordApplier(graph)

    await applier._apply_actions(
        _NO_CONTEXT,
        [
            _make_record_action(
                table_name="Person",
                is_relation=False,
                pk_field="id",
                record_id="p1",
                value={"id": "p1", "name": "Alice"},
            ),
            _make_record_action(
                table_name="Person",
                is_relation=False,
                pk_field="id",
                record_id="p2",
                value={"id": "p2", "name": "Bob"},
            ),
            _make_record_action(
                table_name="Company",
                is_relation=False,
                pk_field="id",
                record_id="c1",
                value={"id": "c1", "name": "CocoIndex"},
            ),
            _make_record_action(
                table_name="WORKS_AT",
                is_relation=True,
                pk_field="id",
                record_id="w1",
                value={"id": "w1", "role": "engineer"},
                from_label="Person",
                from_pk_field="id",
                from_id="p1",
                to_label="Company",
                to_pk_field="id",
                to_id="c1",
            ),
            _make_record_action(
                table_name="KNOWS",
                is_relation=True,
                pk_field="id",
                record_id="k1",
                value={"id": "k1", "since": 2020},
                from_label="Person",
                from_pk_field="id",
                from_id="p1",
                to_label="Person",
                to_pk_field="id",
                to_id="p2",
            ),
        ],
    )

    people = {row["id"]: row for row in await _read_nodes(uri, auth, "Person")}
    companies = {row["id"]: row for row in await _read_nodes(uri, auth, "Company")}
    works_at = await _read_relationships(uri, auth, "WORKS_AT")
    knows = await _read_relationships(uri, auth, "KNOWS")
    assert set(people) == {"p1", "p2"}
    assert set(companies) == {"c1"}
    assert len(works_at) == 1
    assert len(knows) == 1
    assert works_at[0][2]["role"] == "engineer"
    assert knows[0][2]["since"] == 2020

    await applier._apply_actions(
        _NO_CONTEXT,
        [
            _make_record_action(
                table_name="WORKS_AT",
                is_relation=True,
                pk_field="id",
                record_id="w1",
                value=None,
            ),
            _make_record_action(
                table_name="KNOWS",
                is_relation=True,
                pk_field="id",
                record_id="k1",
                value=None,
            ),
            _make_record_action(
                table_name="Person",
                is_relation=False,
                pk_field="id",
                record_id="p1",
                value=None,
            ),
            _make_record_action(
                table_name="Person",
                is_relation=False,
                pk_field="id",
                record_id="p2",
                value=None,
            ),
            _make_record_action(
                table_name="Company",
                is_relation=False,
                pk_field="id",
                record_id="c1",
                value=None,
            ),
        ],
    )

    assert await _read_nodes(uri, auth, "Person") == []
    assert await _read_nodes(uri, auth, "Company") == []
    assert await _read_relationships(uri, auth, "WORKS_AT") == []
    assert await _read_relationships(uri, auth, "KNOWS") == []


@requires_neo4j_server
@pytest.mark.asyncio
async def test_vector_index_attached(
    neo4j_clean: tuple[str, tuple[str, str]],
) -> None:
    """declare_vector_index() should create a queryable vector index."""

    @dataclass
    class _DocWithVec:
        filename: str
        title: str
        summary: str

    async def _declare_doc_with_vector_index() -> None:
        schema = await neo.TableSchema.from_class(_DocWithVec, primary_key="filename")
        table: Any = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "doc_table"),
            neo.mount_table_target,
            KG_DB,
            "VecDoc",
            schema,
            primary_key="filename",
        )
        # NOTE: in real flows the vector field would be a separate
        # numpy-array column; this test only exercises that the DDL fires.
        table.declare_vector_index(field="summary", metric="cosine", dimension=4)

    uri, auth = neo4j_clean
    coco_env.context_provider.provide(
        KG_DB, neo.ConnectionFactory(uri=uri, auth=auth, database="neo4j")
    )
    app = coco.App(
        coco.AppConfig(name="test_neo4j_vec_idx", environment=coco_env),
        _declare_doc_with_vector_index,
    )
    await app.update()

    driver = _neo4j.AsyncGraphDatabase.driver(uri, auth=auth)
    try:
        async with driver.session(database="neo4j") as session:
            result = await session.run(
                "SHOW INDEXES YIELD name, type, labelsOrTypes, properties"
            )
            rows = [dict(r) async for r in result]
    finally:
        await driver.close()

    found = False
    for row in rows:
        if (
            row["type"] == "VECTOR"
            and "VecDoc" in (row["labelsOrTypes"] or [])
            and "summary" in (row["properties"] or [])
        ):
            found = True
            break
    assert found, f"Expected vector index on VecDoc.summary; got {rows}"


@requires_neo4j_server
@pytest.mark.asyncio
async def test_neo4j_table_schema_evolution_under_full_reprocess(
    neo4j_clean: tuple[str, tuple[str, str]],
) -> None:
    """A schema change must still be applied and child records re-upserted when running with full_reprocess=True."""

    @dataclass
    class _DocV1:
        filename: str
        title: str
        extra: str

    @dataclass
    class _DocV2:
        filename: str
        title: str
        summary: str

    doc_type: type[Any] = _DocV1
    docs: list[Any] = [
        _DocV1(filename="doc1.md", title="Title 1", extra="extra 1"),
        _DocV1(filename="doc2.md", title="Title 2", extra="extra 2"),
    ]

    async def _declare_docs() -> None:
        schema = await neo.TableSchema.from_class(doc_type, primary_key="filename")
        table: Any = await coco.use_mount(
            coco.component_subpath("setup", "doc_table"),
            neo.mount_table_target,
            KG_DB,
            "DocEvolve",
            schema,
            primary_key="filename",
        )
        for doc in docs:
            table.declare_record(row=doc)

    uri, auth = neo4j_clean
    coco_env.context_provider.provide(
        KG_DB, neo.ConnectionFactory(uri=uri, auth=auth, database="neo4j")
    )
    app = coco.App(
        coco.AppConfig(name="test_neo4j_schema_reprocess", environment=coco_env),
        _declare_docs,
    )
    await app.update()

    nodes_v1 = await _read_nodes(uri, auth, "DocEvolve")
    assert len(nodes_v1) == 2
    by_fn = {r["filename"]: r for r in nodes_v1}
    assert by_fn["doc1.md"]["extra"] == "extra 1"

    # Evolve schema to V2: drop 'extra', add 'summary' and run under full_reprocess=True
    doc_type = _DocV2
    docs = [
        _DocV2(filename="doc1.md", title="Title 1", summary="summary 1"),
        _DocV2(filename="doc2.md", title="Title 2", summary="summary 2"),
    ]
    await app.update(full_reprocess=True)

    nodes_v2 = await _read_nodes(uri, auth, "DocEvolve")
    assert len(nodes_v2) == 2
    by_fn2 = {r["filename"]: r for r in nodes_v2}
    assert by_fn2["doc1.md"]["summary"] == "summary 1"
    assert by_fn2["doc2.md"]["summary"] == "summary 2"

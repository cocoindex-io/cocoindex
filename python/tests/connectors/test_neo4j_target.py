"""Tests for the Neo4j target connector.

Run with:
    uv run pytest python/tests/connectors/test_neo4j_target.py -v

Unit tests run without a server. Integration tests require a running Neo4j
(spun up automatically via testcontainers when ``NEO4J_TEST_SERVER=1``, or
the server at ``NEO4J_URI`` with password ``cocoindex`` when that is set).
"""

from __future__ import annotations

import os
import warnings
from collections.abc import AsyncIterator, Callable, Iterator
from dataclasses import dataclass
from typing import Any, TypeVar

import pytest
import pytest_asyncio

import cocoindex as coco
from cocoindex.connectors.neo4j._cypher import (
    build_constraint_create,
    build_constraint_drop,
    build_node_delete,
    build_node_delete_all,
    build_node_index_create,
    build_node_index_drop,
    build_node_upsert,
    build_relationship_delete,
    build_relationship_delete_all,
    build_relationship_index_create,
    build_relationship_index_drop,
    build_relationship_upsert,
    build_vector_index_create,
    build_vector_index_drop,
    constraint_name,
    index_name,
    validate_identifier,
    vector_index_name,
)

from tests import common

coco_env = common.create_test_env(__file__)


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

_NEO4J_IMAGE = "neo4j:5.26-community"

_F = TypeVar("_F", bound=Callable[..., Any])


def requires_neo4j_server(fn: _F) -> _F:
    fn = pytest.mark.requires_docker(_NEO4J_IMAGE)(fn)
    fn = pytest.mark.skipif(
        not (HAS_NEO4J and _HAS_NEO4J_SERVER),
        reason="NEO4J_TEST_SERVER is not set",
    )(fn)
    return fn


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
    def test_merges_after_deleting_a_moved_edge(self) -> None:
        assert build_relationship_upsert(
            "REL", "Entity", ["value"], "Entity", ["value"], ["id"], True, False
        ) == (
            "MERGE (s:`Entity` {`value`: $from_key_0}) "
            "MERGE (t:`Entity` {`value`: $to_key_0}) "
            "WITH s, t "
            "OPTIONAL MATCH (a)-[o:`REL` {`id`: $rel_key_0}]->(b) "
            "WHERE a <> s OR b <> t "
            "DELETE o "
            "WITH DISTINCT s, t "
            "MERGE (s)-[r:`REL` {`id`: $rel_key_0}]->(t) "
            "SET r += $props"
        )

    def test_no_set_on_endpoints(self) -> None:
        # Endpoints' properties are owned by their own table's _RecordHandler.
        cypher = build_relationship_upsert(
            "REL", "A", ["x"], "B", ["y"], ["id"], True, False
        )
        assert "SET s" not in cypher
        assert "SET t" not in cypher
        assert "SET r += $props" in cypher

    def test_no_props(self) -> None:
        cypher = build_relationship_upsert(
            "REL", "A", ["x"], "B", ["y"], ["id"], False, False
        )
        assert cypher.endswith("MERGE (s)-[r:`REL` {`id`: $rel_key_0}]->(t)")

    def test_legacy_key_deletes_the_edge_between_the_endpoints(self) -> None:
        cypher = build_relationship_upsert(
            "REL", "A", ["x"], "B", ["y"], ["id"], False, True
        )
        assert (
            "WITH DISTINCT s, t "
            "OPTIONAL MATCH (s)-[l:`REL` {`id`: $legacy_key_0}]->(t) "
            "DELETE l "
            "WITH DISTINCT s, t "
            "MERGE (s)-[r:`REL` {`id`: $rel_key_0}]->(t)"
        ) in cypher

    def test_empty_pk_raises(self) -> None:
        with pytest.raises(ValueError):
            build_relationship_upsert("REL", "A", [], "B", ["y"], ["id"], True, False)
        with pytest.raises(ValueError):
            build_relationship_upsert("REL", "A", ["x"], "B", [], ["id"], True, False)
        with pytest.raises(ValueError):
            build_relationship_upsert("REL", "A", ["x"], "B", ["y"], [], True, False)


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
def test_record_sinks_shared_per_connection_factory() -> None:
    """Every table's record handler of one database — node and relation tables
    alike — reconciles to one record sink (and one vector index sink), so a
    batch can order node writes around relation writes. Another factory gets
    its own sinks."""
    from cocoindex.connectors.neo4j import _target as neo_target

    factory = neo.ConnectionFactory(uri="bolt://localhost:7687")
    node = neo_target._RecordHandler("A", False, "id", None, False, factory)
    relation = neo_target._RecordHandler("R", True, "id", None, True, factory)
    other = neo_target._RecordHandler(
        "A",
        False,
        "id",
        None,
        False,
        neo.ConnectionFactory(uri="bolt://localhost:7687"),
    )

    assert node._sink._core == relation._sink._core
    assert node._sink._core != other._sink._core
    assert (
        node.attachments()["vector_index"]._sink._core
        == relation.attachments()["vector_index"]._sink._core
    )


# =============================================================================
# Apply-batch retry on transient errors — fake driver, no server
# =============================================================================


class _FakeTx:
    """Transaction whose runs raise a deadlock while ``failures[0] > 0``."""

    def __init__(self, log: list[str], failures: list[int]) -> None:
        self._log = log
        self._failures = failures

    async def run(self, cypher: str, **params: Any) -> None:
        self._log.append("run")
        if self._failures[0] > 0:
            self._failures[0] -= 1
            raise _neo4j.exceptions.TransientError(
                "Neo.TransientError.Transaction.DeadlockDetected"
            )

    async def commit(self) -> None:
        self._log.append("commit")

    async def rollback(self) -> None:
        self._log.append("rollback")


class _FakeSession:
    def __init__(self, log: list[str], failures: list[int]) -> None:
        self._log = log
        self._failures = failures

    async def __aenter__(self) -> _FakeSession:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def begin_transaction(self) -> _FakeTx:
        self._log.append("begin")
        return _FakeTx(self._log, self._failures)


class _FakeDriver:
    def __init__(self, log: list[str], failures: list[int]) -> None:
        self._log = log
        self._failures = failures

    def session(self, database: str) -> _FakeSession:
        return _FakeSession(self._log, self._failures)


class _ScriptedDriver:
    """A fake driver whose ``fail(tx_number, runs_in_tx)`` decides, at every
    run, whether that run raises."""

    def __init__(
        self, log: list[str], fail: Callable[[int, int], BaseException | None]
    ) -> None:
        self.log = log
        self.fail = fail
        self.txs = 0

    def session(self, database: str) -> _ScriptedSession:
        return _ScriptedSession(self)


class _ScriptedSession:
    def __init__(self, driver: _ScriptedDriver) -> None:
        self._driver = driver

    async def __aenter__(self) -> _ScriptedSession:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def begin_transaction(self) -> _ScriptedTx:
        self._driver.txs += 1
        self._driver.log.append("begin")
        return _ScriptedTx(self._driver, self._driver.txs)


class _ScriptedTx:
    def __init__(self, driver: _ScriptedDriver, number: int) -> None:
        self._driver = driver
        self._number = number
        self._runs = 0

    async def run(self, cypher: str, **params: Any) -> None:
        self._runs += 1
        self._driver.log.append("run")
        err = self._driver.fail(self._number, self._runs)
        if err is not None:
            raise err

    async def commit(self) -> None:
        self._driver.log.append("commit")

    async def rollback(self) -> None:
        self._driver.log.append("rollback")


@requires_neo4j
class TestApplyBatchRetry:
    @pytest.fixture(autouse=True)
    def _no_backoff(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from cocoindex.connectors.neo4j import _target as neo_target

        monkeypatch.setattr(neo_target, "_TXN_RETRY_INITIAL_BACKOFF_SECONDS", 0.0)
        monkeypatch.setattr(neo_target, "_TXN_RETRY_MAX_BACKOFF_SECONDS", 0.0)

    @staticmethod
    def _applier(log: list[str], failures: int) -> Any:
        from cocoindex.connectors.neo4j import _target as neo_target

        graph = neo_target._GraphHandle(_FakeDriver(log, [failures]), "neo4j")

        class _FakeFactory(neo_target.ConnectionFactory):
            async def acquire(self) -> Any:
                return graph

        return neo_target._SharedRecordApplier(
            _FakeFactory(uri="bolt://localhost:7687")
        )

    @staticmethod
    def _node_upsert() -> Any:
        from cocoindex.connectors.neo4j import _target as neo_target

        return neo_target._RecordAction(
            table_name="Doc",
            is_relation=False,
            pk_field="id",
            record_id="d1",
            value={"id": "d1", "title": "t"},
            from_label=None,
            from_pk_field=None,
            from_id=None,
            to_label=None,
            to_pk_field=None,
            to_id=None,
            legacy_id=None,
        )

    @pytest.mark.asyncio
    async def test_deadlock_is_rolled_back_and_retried(self) -> None:
        log: list[str] = []
        applier = self._applier(log, failures=2)
        await applier(None, [self._node_upsert()])
        assert log == ["begin", "run", "rollback"] * 2 + ["begin", "run", "commit"]

    @pytest.mark.asyncio
    async def test_persistent_deadlock_raises_after_max_attempts(self) -> None:
        from cocoindex.connectors.neo4j import _target as neo_target

        log: list[str] = []
        applier = self._applier(log, failures=10**6)
        with pytest.raises(_neo4j.exceptions.TransientError):
            await applier(None, [self._node_upsert()])
        assert log.count("begin") == neo_target._TXN_MAX_ATTEMPTS
        assert "commit" not in log

    @staticmethod
    def _scripted_applier(
        log: list[str], fail: Callable[[int, int], BaseException | None]
    ) -> Any:
        from cocoindex.connectors.neo4j import _target as neo_target

        graph = neo_target._GraphHandle(_ScriptedDriver(log, fail), "neo4j")

        class _FakeFactory(neo_target.ConnectionFactory):
            async def acquire(self) -> Any:
                return graph

        return neo_target._SharedRecordApplier(
            _FakeFactory(uri="bolt://localhost:7687")
        )

    @pytest.mark.asyncio
    async def test_failed_chunk_stops_the_batch_after_earlier_commits(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from cocoindex.connectors.neo4j import _target as neo_target

        monkeypatch.setattr(neo_target, "_MAX_ACTIONS_PER_TXN", 2)
        log: list[str] = []
        applier = self._scripted_applier(
            log,
            lambda tx, run: _neo4j.exceptions.ClientError("constraint")
            if tx == 2
            else None,
        )
        with pytest.raises(_neo4j.exceptions.ClientError):
            await applier(None, [self._node_upsert() for _ in range(5)])
        # The first chunk stays committed, the second rolls back, the third is
        # never attempted, and the error fails the batch.
        assert log == ["begin", "run", "run", "commit", "begin", "run", "rollback"]

    @pytest.mark.asyncio
    async def test_chunk_over_the_memory_limit_is_split_not_retried(self) -> None:
        class _MemoryLimit(_neo4j.exceptions.TransientError):
            code = "Neo.TransientError.General.MemoryPoolOutOfMemoryError"

        log: list[str] = []
        # The server takes at most 2 writes per transaction.
        applier = self._scripted_applier(
            log, lambda tx, run: _MemoryLimit("too big") if run > 2 else None
        )
        await applier(None, [self._node_upsert() for _ in range(5)])
        # 5 → (2, 3) → (2, (1, 2)): every action is committed, in 3
        # transactions, and no failing transaction is retried as it was.
        assert log.count("commit") == 3
        assert log.count("begin") == 5
        assert log.count("run") - log.count("rollback") * 3 == 5

    @pytest.mark.asyncio
    async def test_large_batch_commits_in_bounded_transactions(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from cocoindex.connectors.neo4j import _target as neo_target

        monkeypatch.setattr(neo_target, "_MAX_ACTIONS_PER_TXN", 2)
        log: list[str] = []
        applier = self._applier(log, failures=0)
        await applier(None, [self._node_upsert() for _ in range(5)])
        # 5 actions in transactions of at most 2: 2 + 2 + 1, in order.
        assert log == ["begin", "run", "run", "commit"] * 2 + [
            "begin",
            "run",
            "commit",
        ]


# =============================================================================
# Integration tests — require running Neo4j (testcontainers spins one up)
# =============================================================================


@pytest.fixture(scope="module")
def neo4j_uri_auth() -> Iterator[tuple[str, tuple[str, str]]]:
    """Spin up a Neo4j container once per test module, or use the server at
    ``NEO4J_URI`` (user ``neo4j``, password ``cocoindex``) when it is set."""
    if not (HAS_NEO4J and _HAS_NEO4J_SERVER):
        pytest.skip("NEO4J_TEST_SERVER is not set")
    if uri := os.environ.get("NEO4J_URI"):
        yield uri, ("neo4j", "cocoindex")
        return

    from testcontainers.community.neo4j import Neo4jContainer  # type: ignore[import-untyped]

    container = Neo4jContainer(_NEO4J_IMAGE, username="neo4j", password="cocoindex")
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
            neo.mount_node_target,  # type: ignore[arg-type]
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
            neo.mount_node_target,  # type: ignore[arg-type]
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
            neo.mount_node_target,
            KG_DB,
            "Entity",
            entity_schema,
            primary_key="value",
        )
        rel_table: Any = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "rel_table"),
            neo.mount_relationship_target,
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
            rel_table.declare_relationship(
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


def _planned(actions: list[Any]) -> tuple[dict[str, Any], dict[tuple[str, Any], Any]]:
    """Split preview actions into table actions by table name and record
    actions by ``(table name, record id)``."""
    from cocoindex.connectors.neo4j import _target as neo_target

    tables = {
        a.key.table_name: a for a in actions if isinstance(a, neo_target._TableAction)
    }
    records = {
        (a.table_name, a.record_id): a
        for a in actions
        if isinstance(a, neo_target._RecordAction)
    }
    assert len(actions) == len(tables) + len(records)
    return tables, records


@requires_neo4j_server
@pytest.mark.asyncio
async def test_preview_plans_tables_and_records(
    neo4j_clean: tuple[str, tuple[str, str]],
) -> None:
    """Preview plans the tables and their records without touching the
    database; a real update then applies them."""
    global _rel_pairs, _tables_declared, _entity_with_schema
    uri, auth = neo4j_clean
    _rel_pairs = [("alice", "bob")]
    _tables_declared = True
    _entity_with_schema = True
    factory = neo.ConnectionFactory(uri=uri, auth=auth, database="neo4j")
    coco_env.context_provider.provide(KG_DB, factory)
    app = coco.App(
        coco.AppConfig(name="test_neo4j_preview", environment=coco_env),
        _declare_entities_and_relationships,
    )
    tables, records = _planned(await app.update(preview=True))
    # Building the record handlers opened no connection.
    assert not factory._connections._entries
    assert {name: a.main_action for name, a in tables.items()} == {
        "Entity": "insert",
        "REL": "insert",
    }
    assert {k: a.value for k, a in records.items()} == {
        ("Entity", "alice"): {"value": "alice"},
        ("Entity", "bob"): {"value": "bob"},
        ("REL", "alice->bob"): {"predicate": "connects"},
    }
    rel = records[("REL", "alice->bob")]
    assert (rel.from_label, rel.from_id, rel.to_label, rel.to_id) == (
        "Entity",
        "alice",
        "Entity",
        "bob",
    )
    # Nothing is written: no nodes, no constraints or indexes.
    assert await _count(uri, auth, "MATCH (n) RETURN count(n) AS c") == 0
    assert await _coco_schema_names(uri, auth) == set()

    await app.update()
    assert {n["value"] for n in await _read_nodes(uri, auth, "Entity")} == {
        "alice",
        "bob",
    }
    assert len(await _read_relationships(uri, auth, "REL")) == 1
    assert await _coco_schema_names(uri, auth) == {
        "coco_uniq_Entity__value",
        "coco_idx_rel_REL__id",
    }

    # Against the applied state: the container actions are retained, alongside the new edge and
    # its new endpoint; the unchanged records are not planned. Nothing is
    # applied.
    _rel_pairs = [("alice", "bob"), ("bob", "carol")]
    tables, records = _planned(await app.update(preview=True))
    assert {name: action.main_action for name, action in tables.items()} == {
        "Entity": None,
        "REL": None,
    }
    assert set(records) == {("Entity", "carol"), ("REL", "bob->carol")}
    assert len(await _read_nodes(uri, auth, "Entity")) == 2
    assert len(await _read_relationships(uri, auth, "REL")) == 1

    await app.update()
    assert len(await _read_nodes(uri, auth, "Entity")) == 3
    assert len(await _read_relationships(uri, auth, "REL")) == 2


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
            neo.mount_node_target,
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
            neo.mount_node_target,
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


# =============================================================================
# Node and relationship APIs vs the deprecated table and relation APIs
# =============================================================================


@requires_neo4j
class TestNodeAndRelationshipApiValidation:
    @pytest.mark.parametrize("label", ["A__x", "A_", "__A"])
    def test_labels_that_make_ambiguous_ddl_names_are_rejected(
        self, label: str
    ) -> None:
        """``A__x`` + ``y`` and ``A`` + ``x__y`` would share the constraint
        name ``coco_uniq_A__x__y``; ``A_`` + ``y`` and ``A`` + ``_y`` would
        share ``coco_uniq_A___y``."""
        with pytest.raises(ValueError, match="node label"):
            neo.node_target(KG_DB, label)
        node = neo.NodeTarget(None, None, "Ok", "id")  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="relationship type"):
            neo.relationship_target(KG_DB, label, node, node)

    def test_key_fields_are_not_restricted(self) -> None:
        neo.node_target(KG_DB, "A", primary_key="x__y")
        neo.node_target(KG_DB, "A", primary_key="_y")

    def test_deprecated_table_api_keeps_accepting_them(self) -> None:
        with pytest.warns(DeprecationWarning, match="use neo4j.node_target"):
            neo.table_target(KG_DB, "A__x")
        node = neo.TableTarget(None, None, "Ok", "id")  # type: ignore[arg-type]
        with pytest.warns(DeprecationWarning, match="use neo4j.relationship_target"):
            neo.relation_target(KG_DB, "R__x", node, node)

    def test_relationship_primary_key_must_match_the_schema(self) -> None:
        node = neo.NodeTarget(None, None, "Ok", "id")  # type: ignore[arg-type]
        schema = neo.TableSchema(
            columns={"rel_id": neo.ColumnDef("STRING")}, primary_key="rel_id"
        )
        with pytest.raises(ValueError, match="primary_key"):
            neo.relationship_target(KG_DB, "R", node, node, schema)


@requires_neo4j
class TestVectorIndexApplier:
    """Several declared names can reach one Neo4j index, which is named after
    its label and field."""

    @staticmethod
    def _applier(queries: list[str]) -> Any:
        from cocoindex.connectors.neo4j import _target as neo_target

        class _FakeGraph:
            async def query(self, cypher: str, params: Any = None) -> list[Any]:
                queries.append(cypher)
                return []

        class _FakeFactory(neo_target.ConnectionFactory):
            async def acquire(self) -> Any:
                return _FakeGraph()

        return neo_target._VectorIndexApplier(_FakeFactory(uri="bolt://localhost:7687"))

    @staticmethod
    def _action(name: str, field: str, *, create: bool) -> Any:
        from cocoindex.connectors.neo4j import _target as neo_target

        spec = neo_target._VectorIndexSpec(field=field, metric="cosine", dimension=4)
        return neo_target._VectorIndexAction(
            name=name,
            table_name="Doc",
            index_name=vector_index_name("Doc", field),
            spec=spec if create else None,
        )

    @pytest.mark.asyncio
    async def test_a_rename_keeps_the_index(self) -> None:
        queries: list[str] = []
        await self._applier(queries)(
            None,
            [
                self._action("new", "emb", create=True),
                self._action("old", "emb", create=False),
                self._action("gone", "other", create=False),
            ],
        )
        assert queries == [
            build_vector_index_drop("coco_vec_Doc__emb"),
            build_vector_index_create("coco_vec_Doc__emb", "Doc", "emb", 4, "cosine"),
            build_vector_index_drop("coco_vec_Doc__other"),
        ]

    @pytest.mark.asyncio
    async def test_two_names_for_one_index_fail(self) -> None:
        queries: list[str] = []
        with pytest.raises(ValueError, match="map to one Neo4j index"):
            await self._applier(queries)(
                None,
                [
                    self._action("a", "emb", create=True),
                    self._action("b", "emb", create=True),
                ],
            )
        assert queries == []


@requires_neo4j
@pytest.mark.asyncio
async def test_deprecated_apis_warn() -> None:
    """Each deprecated entry point warns once per call; the node and
    relationship APIs don't. A preview run needs no server."""

    async def declare() -> None:
        old_node: Any = await coco.use_mount(
            coco.component_subpath("old_node"), neo.mount_table_target, KG_DB, "Old"
        )
        neo.declare_table_target(KG_DB, "OldToo")
        old_rel: Any = await coco.use_mount(
            coco.component_subpath("old_rel"),
            neo.mount_relation_target,
            KG_DB,
            "OLD_REL",
            old_node,
            old_node,
        )
        neo.declare_relation_target(KG_DB, "OLD_REL_TOO", old_node, old_node)
        old_rel.declare_relation(from_id="a", to_id="b")

        new_node: Any = await coco.use_mount(
            coco.component_subpath("new_node"), neo.mount_node_target, KG_DB, "New"
        )
        neo.declare_node_target(KG_DB, "NewToo")
        new_rel: Any = await coco.use_mount(
            coco.component_subpath("new_rel"),
            neo.mount_relationship_target,
            KG_DB,
            "NEW_REL",
            new_node,
            old_node,
        )
        neo.declare_relationship_target(KG_DB, "NEW_REL_TOO", old_node, new_node)
        new_rel.declare_relationship(from_id="a", to_id="b")

    coco_env.context_provider.provide(
        KG_DB, neo.ConnectionFactory(uri="bolt://localhost:1")
    )
    app = coco.App(
        coco.AppConfig(name="test_neo4j_deprecations", environment=coco_env), declare
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        await app.update(preview=True)
    deprecated = sorted(
        str(w.message).split(" is deprecated")[0]
        for w in caught
        if issubclass(w.category, DeprecationWarning)
    )
    assert deprecated == [
        "neo4j.RelationTarget.declare_relation",
        "neo4j.declare_relation_target",
        "neo4j.declare_table_target",
        "neo4j.mount_relation_target",
        "neo4j.mount_table_target",
    ]


@requires_neo4j
@pytest.mark.parametrize("collision", ["shared_name", "shared_derived_id"])
@pytest.mark.asyncio
async def test_deprecated_apis_fail_on_colliding_keys(collision: str) -> None:
    """What the docs promise for the deprecated APIs: a label and a
    relationship type of one name, or two edges whose derived ids coincide,
    fail the update (a preview run needs no server)."""

    async def declare() -> None:
        nodes: Any = await coco.use_mount(
            coco.component_subpath("node"), neo.mount_table_target, KG_DB, "N"
        )
        rels: Any = await coco.use_mount(
            coco.component_subpath("rel"),
            neo.mount_relation_target,
            KG_DB,
            "N" if collision == "shared_name" else "R",
            nodes,
            nodes,
        )
        rels.declare_relation(from_id=1, to_id=2)
        if collision == "shared_derived_id":
            rels.declare_relation(from_id="1", to_id="2")

    coco_env.context_provider.provide(
        KG_DB, neo.ConnectionFactory(uri="bolt://localhost:1")
    )
    app = coco.App(
        coco.AppConfig(name=f"test_neo4j_{collision}", environment=coco_env), declare
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        with pytest.raises(Exception, match="already"):
            await app.update(preview=True)


@requires_neo4j_server
@pytest.mark.asyncio
async def test_label_and_relationship_type_of_one_name(
    neo4j_clean: tuple[str, tuple[str, str]],
) -> None:
    """Node tables and relationship tables have keys of their own: a label
    and a relationship type may share a name, and dropping one leaves the
    other."""
    uri, auth = neo4j_clean
    with_relationships = True

    async def declare() -> None:
        things: Any = await coco.use_mount(
            coco.component_subpath("node"), neo.mount_node_target, KG_DB, "Thing"
        )
        things.declare_record(row={"id": "a"})
        things.declare_record(row={"id": "b"})
        if with_relationships:
            rels: Any = await coco.use_mount(
                coco.component_subpath("rel"),
                neo.mount_relationship_target,
                KG_DB,
                "Thing",
                things,
                things,
            )
            rels.declare_relationship(from_id="a", to_id="b")

    coco_env.context_provider.provide(
        KG_DB, neo.ConnectionFactory(uri=uri, auth=auth, database="neo4j")
    )
    app = coco.App(
        coco.AppConfig(name="test_neo4j_shared_name", environment=coco_env), declare
    )
    await app.update()
    assert len(await _read_nodes(uri, auth, "Thing")) == 2
    assert len(await _read_relationships(uri, auth, "Thing")) == 1
    assert await _coco_schema_names(uri, auth) == {
        "coco_uniq_Thing__id",
        "coco_idx_rel_Thing__id",
    }

    with_relationships = False
    await app.update()
    assert len(await _read_nodes(uri, auth, "Thing")) == 2
    assert await _read_relationships(uri, auth, "Thing") == []
    assert await _coco_schema_names(uri, auth) == {"coco_uniq_Thing__id"}


@requires_neo4j_server
@pytest.mark.asyncio
async def test_derived_relationship_ids_are_unambiguous(
    neo4j_clean: tuple[str, tuple[str, str]],
) -> None:
    """Endpoints the deprecated relation API maps to one id (``1`` vs
    ``"1"``, ``"a_N_b", "c"`` vs ``"a", "b_N_c"``) get edges of their own."""
    uri, auth = neo4j_clean
    pairs: list[tuple[Any, Any]] = [
        (1, "1"),
        ("1", 1),
        ("a_N_b", "c"),
        ("a", "b_N_c"),
    ]

    async def declare() -> None:
        nodes: Any = await coco.use_mount(
            coco.component_subpath("node"), neo.mount_node_target, KG_DB, "N"
        )
        for node_id in {v for pair in pairs for v in pair}:
            nodes.declare_record(row={"id": node_id})
        rels: Any = await coco.use_mount(
            coco.component_subpath("rel"),
            neo.mount_relationship_target,
            KG_DB,
            "R",
            nodes,
            nodes,
        )
        for from_id, to_id in pairs:
            rels.declare_relationship(from_id=from_id, to_id=to_id)

    coco_env.context_provider.provide(
        KG_DB, neo.ConnectionFactory(uri=uri, auth=auth, database="neo4j")
    )
    app = coco.App(
        coco.AppConfig(name="test_neo4j_unambiguous_ids", environment=coco_env),
        declare,
    )
    await app.update()
    edges = await _read_relationships(uri, auth, "R")
    assert sorted((repr(s["id"]), repr(t["id"])) for s, t, _ in edges) == sorted(
        (repr(a), repr(b)) for a, b in pairs
    )
    assert len({r["id"] for _, _, r in edges}) == 4


@requires_neo4j_server
@pytest.mark.parametrize("api", ["relationship", "relation"])
@pytest.mark.asyncio
async def test_moved_relationship_leaves_no_duplicate(
    neo4j_clean: tuple[str, tuple[str, str]], api: str
) -> None:
    """An edge with an explicit id whose endpoints change is moved, not
    copied: the edge between the old endpoints goes. A duplicate left by an
    earlier version goes on a full reprocess."""
    uri, auth = neo4j_clean
    to_id = "bob"

    async def declare() -> None:
        mount_node: Any = (
            neo.mount_node_target if api == "relationship" else neo.mount_table_target
        )
        mount_rel: Any = (
            neo.mount_relationship_target
            if api == "relationship"
            else neo.mount_relation_target
        )
        people: Any = await coco.use_mount(
            coco.component_subpath("node"), mount_node, KG_DB, "Person"
        )
        for name in ("alice", "bob", "carol"):
            people.declare_record(row={"id": name})
        knows: Any = await coco.use_mount(
            coco.component_subpath("rel"), mount_rel, KG_DB, "KNOWS", people, people
        )
        declare_edge = (
            knows.declare_relationship
            if api == "relationship"
            else knows.declare_relation
        )
        declare_edge(from_id="alice", to_id=to_id, record={"id": "k1", "since": 1})

    async def knows_edges() -> list[tuple[str, str, str]]:
        return sorted(
            (s["id"], t["id"], r["id"])
            for s, t, r in await _read_relationships(uri, auth, "KNOWS")
        )

    coco_env.context_provider.provide(
        KG_DB, neo.ConnectionFactory(uri=uri, auth=auth, database="neo4j")
    )
    app = coco.App(
        coco.AppConfig(name=f"test_neo4j_moved_rel_{api}", environment=coco_env),
        declare,
    )
    await app.update()
    assert await knows_edges() == [("alice", "bob", "k1")]

    to_id = "carol"
    await app.update()
    assert await knows_edges() == [("alice", "carol", "k1")]

    # A duplicate an earlier version left behind when the edge moved.
    await _query(
        uri,
        auth,
        "MATCH (s:Person {id: 'alice'}), (t:Person {id: 'bob'}) "
        "CREATE (s)-[:KNOWS {id: 'k1', since: 1}]->(t)",
    )
    await app.update(full_reprocess=True)
    assert await knows_edges() == [("alice", "carol", "k1")]


@requires_neo4j_server
@pytest.mark.asyncio
async def test_renamed_vector_index_is_kept(
    neo4j_clean: tuple[str, tuple[str, str]],
) -> None:
    """The Neo4j index is named after its label and field, not the declared
    name: renaming it keeps it. Two declared names on one field fail."""
    uri, auth = neo4j_clean
    index_names = ["vec_a"]

    async def declare() -> None:
        docs: Any = await coco.use_mount(
            coco.component_subpath("node"), neo.mount_node_target, KG_DB, "VecDoc"
        )
        docs.declare_record(row={"id": "d1", "summary": "s"})
        for name in index_names:
            docs.declare_vector_index(name=name, field="summary", dimension=4)

    async def vector_indexes() -> set[str]:
        rows = await _query(
            uri, auth, "SHOW VECTOR INDEXES YIELD name, properties RETURN *"
        )
        return {r["name"] for r in rows if r["properties"] == ["summary"]}

    coco_env.context_provider.provide(
        KG_DB, neo.ConnectionFactory(uri=uri, auth=auth, database="neo4j")
    )
    app = coco.App(
        coco.AppConfig(name="test_neo4j_vec_rename", environment=coco_env), declare
    )
    await app.update()
    assert await vector_indexes() == {"coco_vec_VecDoc__summary"}

    index_names = ["vec_b"]
    await app.update()
    assert await vector_indexes() == {"coco_vec_VecDoc__summary"}

    # A second name for the same field fails where it is declared.
    index_names = ["vec_b", "vec_c"]
    with pytest.raises(Exception, match="both index VecDoc.summary"):
        await app.update()


@requires_neo4j_server
@pytest.mark.asyncio
async def test_switching_from_the_table_and_relation_apis(
    neo4j_clean: tuple[str, tuple[str, str]],
) -> None:
    """The documented switch: mark the old declarations ``managed_by="user"``
    and update once, then declare the same tables with the node and
    relationship APIs. Nodes, constraints and indexes stay; an edge with a
    derived id is replaced by one with the new id, never duplicated."""
    uri, auth = neo4j_clean
    api = "old"
    managed_by = "system"

    @dataclass
    class _Person:
        name: str
        summary: str

    @dataclass
    class _Knows:
        id: str
        since: int

    names = ["alice", "bob", "carol"]
    likes = [("alice", "bob"), ("bob", "carol")]

    async def declare() -> None:
        person_schema = await neo.TableSchema.from_class(_Person, primary_key="name")
        knows_schema = await neo.TableSchema.from_class(_Knows, primary_key="id")
        old = api == "old"
        options: dict[str, Any] = {"managed_by": managed_by} if old else {}
        mount_node: Any = neo.mount_table_target if old else neo.mount_node_target
        person: Any = await coco.use_mount(
            coco.component_subpath("person"),
            mount_node,
            KG_DB,
            "Person",
            person_schema,
            primary_key="name",
            **options,
        )
        mount_rel: Any = (
            neo.mount_relation_target if old else neo.mount_relationship_target
        )
        likes_rel: Any = await coco.use_mount(
            coco.component_subpath("likes"),
            mount_rel,
            KG_DB,
            "LIKES",
            person,
            person,
            **options,
        )
        knows_rel: Any = await coco.use_mount(
            coco.component_subpath("knows"),
            mount_rel,
            KG_DB,
            "KNOWS",
            person,
            person,
            knows_schema,
            primary_key="id",
            **options,
        )
        for name in names:
            person.declare_record(row=_Person(name=name, summary=f"{name}!"))
        person.declare_vector_index(field="summary", dimension=4)
        for from_id, to_id in likes:
            if old:
                likes_rel.declare_relation(from_id=from_id, to_id=to_id)
            else:
                likes_rel.declare_relationship(from_id=from_id, to_id=to_id)
        knows = _Knows(id="k1", since=2020)
        if old:
            knows_rel.declare_relation(from_id="alice", to_id="carol", record=knows)
        else:
            knows_rel.declare_relationship(from_id="alice", to_id="carol", record=knows)

    async def edges(rel_type: str) -> list[tuple[str, str, str]]:
        return sorted(
            (s["name"], t["name"], r["id"])
            for s, t, r in await _read_relationships(uri, auth, rel_type)
        )

    async def assert_graph(likes_ids: list[str]) -> None:
        assert sorted(n["name"] for n in await _read_nodes(uri, auth, "Person")) == (
            names
        )
        assert await edges("LIKES") == [
            (a, b, rel_id) for (a, b), rel_id in zip(likes, likes_ids)
        ]
        assert await edges("KNOWS") == [("alice", "carol", "k1")]
        assert await _coco_schema_names(uri, auth) == {
            "coco_uniq_Person__name",
            "coco_idx_rel_LIKES__id",
            "coco_idx_rel_KNOWS__id",
            "coco_vec_Person__summary",
        }

    legacy_ids = ["Person_alice_Person_bob", "Person_bob_Person_carol"]
    coco_env.context_provider.provide(
        KG_DB, neo.ConnectionFactory(uri=uri, auth=auth, database="neo4j")
    )
    app = coco.App(
        coco.AppConfig(name="test_neo4j_switch_apis", environment=coco_env), declare
    )
    await app.update()
    await assert_graph(legacy_ids)

    # Step 1: hand the tables to the user. Nothing in the graph changes.
    managed_by = "user"
    await app.update()
    await assert_graph(legacy_ids)

    # Step 2: declare them with the node and relationship APIs.
    api = "new"
    await app.update()
    new_ids = [
        "6:Person;5:alice;6:Person;3:bob;",
        "6:Person;3:bob;6:Person;5:carol;",
    ]
    await assert_graph(new_ids)

    # The new tables own the graph from here on.
    await app.update()
    await assert_graph(new_ids)
    await app.drop()
    assert await _count(uri, auth, "MATCH (n) RETURN count(n) AS c") == 0
    assert await _count(uri, auth, "MATCH ()-[r]->() RETURN count(r) AS c") == 0

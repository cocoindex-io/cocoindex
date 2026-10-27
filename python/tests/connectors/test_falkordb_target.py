"""Tests for the FalkorDB target connector.

Run with:
    uv run pytest python/tests/connectors/test_falkordb_target.py -v

Unit tests run without a server. Integration tests require a running FalkorDB
and the env vars below:
    FALKORDB_TEST_SERVER=1     - opt in to integration tests
    FALKORDB_URI=falkor://localhost:6379  (default)
"""

from __future__ import annotations

import os
import uuid as uuid_mod
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import pytest
import pytest_asyncio

import cocoindex as coco

from tests import common

coco_env = common.create_test_env(__file__)


# =============================================================================
# Skip gates
# =============================================================================

try:
    import falkordb  # type: ignore[import-untyped]  # noqa: F401
    from falkordb.asyncio import FalkorDB as AsyncFalkorDB  # type: ignore[import-untyped]

    HAS_FALKORDB = True
except ImportError:
    HAS_FALKORDB = False

requires_falkordb = pytest.mark.skipif(
    not HAS_FALKORDB, reason="falkordb is not installed"
)

_FALKORDB_URI = os.environ.get("FALKORDB_URI", "falkor://localhost:6379")
_HAS_FALKORDB_SERVER = bool(os.environ.get("FALKORDB_TEST_SERVER"))

requires_falkordb_server = pytest.mark.skipif(
    not (HAS_FALKORDB and _HAS_FALKORDB_SERVER),
    reason="FALKORDB_TEST_SERVER is not set",
)

if HAS_FALKORDB:
    from cocoindex.connectors import falkordb as falkor  # type: ignore[attr-defined]
    from cocoindex.connectors.falkordb._cypher import (  # type: ignore[import-untyped]
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
        validate_identifier,
    )

    KG_DB: coco.ContextKey[Any] = coco.ContextKey("test_falkordb_kg")


# =============================================================================
# Unit tests — identifier validation (no DB)
# =============================================================================


@requires_falkordb
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
        with pytest.raises(ValueError, match="Invalid FalkorDB"):
            validate_identifier(name, "test")


@requires_falkordb
class TestIdentifierValidationAtApiEntryPoints:
    def test_table_schema_invalid_column(self) -> None:
        with pytest.raises(ValueError, match="column name"):
            falkor.TableSchema(
                columns={
                    "id": falkor.ColumnDef(type="string"),
                    "bad-name": falkor.ColumnDef(type="string"),
                },
                primary_key="id",
            )

    def test_table_schema_pk_must_exist_in_columns(self) -> None:
        with pytest.raises(ValueError, match="primary_key"):
            falkor.TableSchema(
                columns={"id": falkor.ColumnDef(type="string")},
                primary_key="missing",
            )

    def test_table_target_invalid_name(self) -> None:
        with pytest.raises(ValueError, match="table name"):
            falkor.table_target(KG_DB, "bad-table")

    def test_relation_target_invalid_name(self) -> None:
        # The relation table name validation fires first in relation_target(),
        # before from_table/to_table are touched, so we can pass None safely.
        from typing import cast

        with pytest.raises(ValueError, match="relation table name"):
            falkor.relation_target(
                KG_DB,
                "bad-rel",
                cast(Any, None),
                cast(Any, None),
            )


# =============================================================================
# Unit tests — Cypher generation (no DB)
# =============================================================================


@requires_falkordb
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
        assert build_node_upsert("X", ["a", "b"], True) == (
            "MERGE (n:`X` {`a`: $key_0, `b`: $key_1}) SET n += $props"
        )

    def test_empty_pk_raises(self) -> None:
        with pytest.raises(ValueError):
            build_node_upsert("X", [], True)


@requires_falkordb
class TestNodeDeleteCypher:
    def test_uses_detach_delete(self) -> None:
        # DETACH DELETE is critical: protects against the shared-Entity case
        # where a node still has incident edges that another flow owns.
        assert (
            build_node_delete("Document", ["filename"])
            == "MATCH (n:`Document` {`filename`: $key_0}) DETACH DELETE n"
        )

    def test_delete_all_detaches_whole_label(self) -> None:
        # Table teardown: every node carrying the label, with its relationships.
        assert build_node_delete_all("Document") == (
            "MATCH (n:`Document`) DETACH DELETE n"
        )


@requires_falkordb
class TestRelationshipUpsertCypher:
    def test_merges_after_deleting_a_moved_edge(self) -> None:
        assert build_relationship_upsert(
            "REL", "Entity", ["value"], "Entity", ["value"], ["id"], True
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
        # Critical: SET only on the relationship, not on s or t. Endpoint
        # properties are owned by their table's own _RecordHandler.
        cypher = build_relationship_upsert("REL", "A", ["x"], "B", ["y"], ["id"], True)
        # Three SET-able sites: s, t, r. Only r should have SET applied.
        assert "SET s" not in cypher
        assert "SET t" not in cypher
        assert "SET r += $props" in cypher

    def test_no_props(self) -> None:
        cypher = build_relationship_upsert("REL", "A", ["x"], "B", ["y"], ["id"], False)
        assert cypher.endswith("MERGE (s)-[r:`REL` {`id`: $rel_key_0}]->(t)")


@requires_falkordb
class TestRelationshipDeleteCypher:
    def test_does_not_cascade(self) -> None:
        # Endpoints are NOT deleted by the relationship delete — they live
        # under their own table handlers' tracking and reconciliation.
        cypher = build_relationship_delete("REL", ["id"])
        assert cypher == "MATCH ()-[r:`REL` {`id`: $key_0}]->() DELETE r"
        assert "DELETE s" not in cypher
        assert "DELETE t" not in cypher

    def test_delete_all_keeps_endpoints(self) -> None:
        cypher = build_relationship_delete_all("REL")
        assert cypher == "MATCH ()-[r:`REL`]->() DELETE r"
        assert "DETACH" not in cypher


@requires_falkordb
class TestIndexDdlCypher:
    def test_node_index_create(self) -> None:
        assert (
            build_node_index_create("Document", ["filename"])
            == "CREATE INDEX FOR (e:`Document`) ON (e.`filename`)"
        )

    def test_node_index_create_compound(self) -> None:
        assert build_node_index_create("X", ["a", "b"]) == (
            "CREATE INDEX FOR (e:`X`) ON (e.`a`, e.`b`)"
        )

    def test_node_index_drop(self) -> None:
        assert (
            build_node_index_drop("Document", ["filename"])
            == "DROP INDEX FOR (e:`Document`) ON (e.`filename`)"
        )

    def test_relationship_index_create(self) -> None:
        assert (
            build_relationship_index_create("REL", ["id"])
            == "CREATE INDEX FOR ()-[e:`REL`]-() ON (e.`id`)"
        )

    def test_relationship_index_drop(self) -> None:
        assert (
            build_relationship_index_drop("REL", ["id"])
            == "DROP INDEX FOR ()-[e:`REL`]-() ON (e.`id`)"
        )


@requires_falkordb
class TestVectorIndexCypher:
    def test_create(self) -> None:
        assert build_vector_index_create("Doc", "embedding", 384, "cosine") == (
            "CREATE VECTOR INDEX FOR (e:`Doc`) ON (e.`embedding`) "
            "OPTIONS {dimension: 384, similarityFunction: 'cosine'}"
        )

    def test_drop_uses_label_field_not_name(self) -> None:
        # FalkorDB identifies vector indexes by (label, field), not by name —
        # confirmed via spike against the running server.
        assert build_vector_index_drop("Doc", "embedding") == (
            "DROP VECTOR INDEX FOR (e:`Doc`) ON (e.`embedding`)"
        )

    def test_zero_dimension_rejected(self) -> None:
        with pytest.raises(ValueError):
            build_vector_index_create("Doc", "embedding", 0, "cosine")


# =============================================================================
# Unit tests — TableSchema.from_class type mapping
# =============================================================================


@requires_falkordb
class TestTableSchemaFromClass:
    @pytest.mark.asyncio
    async def test_basic_dataclass(self) -> None:
        @dataclass
        class Row:
            id: str
            count: int
            score: float
            flag: bool

        schema = await falkor.TableSchema.from_class(Row, primary_key="id")
        assert schema.primary_key == "id"
        assert schema.columns["id"].type == "string"
        assert schema.columns["count"].type == "integer"
        assert schema.columns["score"].type == "float"
        assert schema.columns["flag"].type == "boolean"
        assert schema.value_field_names == ["count", "score", "flag"]

    @pytest.mark.asyncio
    async def test_custom_pk(self) -> None:
        @dataclass
        class Doc:
            filename: str
            title: str

        schema = await falkor.TableSchema.from_class(Doc, primary_key="filename")
        assert schema.primary_key == "filename"
        assert schema.value_field_names == ["title"]


# =============================================================================
# Table reconciliation unit tests (no DB needed)
# =============================================================================


@requires_falkordb
class TestTableReconcile:
    def test_schema_evolution_under_full_reprocess(self) -> None:
        """Schema evolution during full reprocess must compute column actions and mark lossy."""
        from cocoindex.connectorkits import statediff, target
        from cocoindex.connectors.falkordb import _target as falkor_target

        handler = falkor_target._TableHandler()
        schema_v1 = falkor.TableSchema(
            columns={
                "id": falkor.ColumnDef("string"),
                "a": falkor.ColumnDef("string"),
                "b": falkor.ColumnDef("string"),
            },
            primary_key="id",
        )
        spec_v1 = falkor_target._TableSpec(
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
            falkor_target._TableKey("db", "Doc"), spec_v1, [], False
        )
        assert out_v1 is not None
        assert out_v1.action.main_action is None
        assert isinstance(out_v1.tracking_record, statediff.MutualTrackingRecord)
        tracking_v1 = out_v1.tracking_record

        # Evolve schema: drop 'b', add 'c'
        schema_v2 = falkor.TableSchema(
            columns={
                "id": falkor.ColumnDef("string"),
                "a": falkor.ColumnDef("string"),
                "c": falkor.ColumnDef("string"),
            },
            primary_key="id",
        )
        spec_v2 = falkor_target._TableSpec(
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
            falkor_target._TableKey("db", "Doc"),
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
            falkor_target._TableKey("db", "Doc"),
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
        from cocoindex.connectors.falkordb import _target as falkor_target

        handler = falkor_target._TableHandler()
        schema_v1 = falkor.TableSchema(
            columns={
                "id": falkor.ColumnDef("string"),
                "a": falkor.ColumnDef("string"),
            },
            primary_key="id",
        )
        spec_v1 = falkor_target._TableSpec(
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
            falkor_target._TableKey("db", "Doc"), spec_v1, [], False
        )
        assert out_v1 is not None
        assert isinstance(out_v1.tracking_record, statediff.MutualTrackingRecord)
        tracking_v1 = out_v1.tracking_record

        schema_v2 = falkor.TableSchema(
            columns={
                "id": falkor.ColumnDef("string"),
                "a": falkor.ColumnDef("string"),
                "b": falkor.ColumnDef("string"),
            },
            primary_key="id",
        )
        spec_v2 = falkor_target._TableSpec(
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
            falkor_target._TableKey("db", "Doc"),
            spec_v2,
            [tracking_v1],
            False,
        )
        assert out_inc is not None
        assert out_inc.action.main_action is None
        assert out_inc.action.column_actions == {"field:b": "insert"}
        assert out_inc.child_invalidation is None

        out_reproc = handler.reconcile(
            falkor_target._TableKey("db", "Doc"),
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
        from cocoindex.connectors.falkordb import _target as falkor_target

        def spec(
            columns: dict[str, falkor.ColumnDef] | None, primary_key: str
        ) -> falkor_target._TableSpec:
            schema = (
                falkor.TableSchema(columns=columns, primary_key=primary_key)
                if columns is not None
                else None
            )
            return falkor_target._TableSpec(
                table_schema=schema,
                primary_key=primary_key,
                is_relation=False,
                from_label=None,
                from_pk_field=None,
                to_label=None,
                to_pk_field=None,
                managed_by=target.ManagedBy.SYSTEM,
            )

        handler = falkor_target._TableHandler()
        key = falkor_target._TableKey("db", "Doc")

        out = handler.reconcile(key, spec(None, "id"), [], False)
        assert out is not None and out.action.main_action is None
        schemaless = out.tracking_record
        assert isinstance(schemaless, statediff.MutualTrackingRecord)

        # Schemaless -> schema: the new fields show up, nothing is rebuilt.
        with_schema = spec(
            {"id": falkor.ColumnDef("string"), "a": falkor.ColumnDef("string")},
            "id",
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
            {"id": falkor.ColumnDef("integer"), "a": falkor.ColumnDef("string")},
            "id",
        )
        out_int = handler.reconcile(key, int_pk, [with_schema_tracking], False)
        assert out_int is not None
        assert out_int.action.main_action is None
        assert out_int.action.column_actions == {"field:id": "replace"}
        assert out_int.child_invalidation == "lossy"

        # Primary key field change: the table is rebuilt from scratch.
        new_pk = spec(
            {"id": falkor.ColumnDef("string"), "a": falkor.ColumnDef("string")},
            "a",
        )
        out_pk = handler.reconcile(key, new_pk, [with_schema_tracking], False)
        assert out_pk is not None
        assert out_pk.action.main_action == "replace"
        assert out_pk.child_invalidation == "destructive"
        assert out_pk.action.prev_pk_field == "id"


@requires_falkordb
def test_record_sinks_shared_per_connection_factory() -> None:
    """Every table's record handler of one database — node and relation tables
    alike — reconciles to one record sink (and one vector index sink), so a
    batch can order node writes around relation writes. Another factory gets
    its own sinks."""
    from cocoindex.connectors.falkordb import _target as falkor_target

    factory = falkor.ConnectionFactory(uri=_FALKORDB_URI)
    node = falkor_target._RecordHandler("A", False, "id", None, factory)
    relation = falkor_target._RecordHandler("R", True, "id", None, factory)
    other = falkor_target._RecordHandler(
        "A", False, "id", None, falkor.ConnectionFactory(uri=_FALKORDB_URI)
    )

    assert node._sink._core == relation._sink._core
    assert node._sink._core != other._sink._core
    assert (
        node.attachments()["vector_index"]._sink._core
        == relation.attachments()["vector_index"]._sink._core
    )


# =============================================================================
# Integration tests — require running FalkorDB
# =============================================================================


@pytest_asyncio.fixture
async def falkor_graph_name() -> AsyncIterator[str]:
    """Yield a unique graph name and tear it down at the end of the test.

    Each test runs against its own graph so concurrent runs and re-runs are
    isolated (FalkorDB graphs are cheap — Redis namespaces).
    """
    name = f"test_{uuid_mod.uuid4().hex[:8]}"
    yield name
    if HAS_FALKORDB:
        try:
            client = AsyncFalkorDB.from_url(_FALKORDB_URI)
            g = client.select_graph(name)
            await g.delete()
            await client.aclose()
        except Exception:  # noqa: BLE001
            pass


async def _read_nodes(graph_name: str, label: str) -> list[dict[str, Any]]:
    """Return all nodes of a given label, with their properties."""
    client = AsyncFalkorDB.from_url(_FALKORDB_URI)
    g = client.select_graph(graph_name)
    res = await g.query(f"MATCH (n:`{label}`) RETURN properties(n)")
    rows = [r[0] for r in res.result_set]
    await client.aclose()
    return rows


async def _count(graph_name: str, cypher: str) -> int:
    """Run ``cypher`` (which must return a single count) and return it."""
    client = AsyncFalkorDB.from_url(_FALKORDB_URI)
    g = client.select_graph(graph_name)
    res = await g.query(cypher)
    await client.aclose()
    return int(res.result_set[0][0])


async def _indexed_labels(graph_name: str) -> set[str]:
    """Labels and relationship types that still carry an index."""
    client = AsyncFalkorDB.from_url(_FALKORDB_URI)
    g = client.select_graph(graph_name)
    res = await g.query("CALL db.indexes() YIELD label RETURN label")
    await client.aclose()
    return {r[0] for r in res.result_set}


async def _read_relationships(
    graph_name: str, rel_type: str
) -> list[tuple[Any, Any, dict[str, Any]]]:
    """Return (from_props, to_props, rel_props) tuples for all edges of a type."""
    client = AsyncFalkorDB.from_url(_FALKORDB_URI)
    g = client.select_graph(graph_name)
    res = await g.query(
        f"MATCH (s)-[r:`{rel_type}`]->(t) "
        f"RETURN properties(s), properties(t), properties(r)"
    )
    out = [(r[0], r[1], r[2]) for r in res.result_set]
    await client.aclose()
    return out


# Per-test global state shared with the declare function (mirrors the
# surrealdb test pattern — globals are re-bound at the top of each test).
_current_graph: str = ""
_node_rows: list[Any] = []
_rel_pairs: list[tuple[Any, Any]] = []
_doc_pk: str = "filename"
_tables_declared: bool = True
_entity_with_schema: bool = True


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
    schema = await falkor.TableSchema.from_class(Document, primary_key="filename")
    table: Any = await coco.use_mount(  # type: ignore[call-overload]
        coco.component_subpath("setup", "doc_table"),
        falkor.mount_table_target,  # type: ignore[arg-type]
        KG_DB,
        "Document",
        schema,
        primary_key="filename",
    )
    for row in _node_rows:
        table.declare_record(row=row)


async def _declare_documents_keyed_by_doc_pk() -> None:
    schema = await falkor.TableSchema.from_class(Document, primary_key=_doc_pk)
    table: Any = await coco.use_mount(  # type: ignore[call-overload]
        coco.component_subpath("setup", "doc_table"),
        falkor.mount_table_target,  # type: ignore[arg-type]
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
        await falkor.TableSchema.from_class(Entity, primary_key="value")
        if _entity_with_schema
        else None
    )
    rel_schema = await falkor.TableSchema.from_class(RelRow, primary_key="id")
    entity_table: Any = await coco.use_mount(  # type: ignore[call-overload]
        coco.component_subpath("setup", "entity_table"),
        falkor.mount_table_target,
        KG_DB,
        "Entity",
        entity_schema,
        primary_key="value",
    )
    rel_table: Any = await coco.use_mount(  # type: ignore[call-overload]
        coco.component_subpath("setup", "rel_table"),
        falkor.mount_relation_target,
        KG_DB,
        "REL",
        entity_table,
        entity_table,
        rel_schema,
        primary_key="id",
    )
    # Drive entity table directly so endpoints exist with their own tracking.
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


@requires_falkordb_server
@pytest.mark.asyncio
async def test_node_upsert_and_readback(falkor_graph_name: str) -> None:
    global _current_graph, _node_rows
    _current_graph = falkor_graph_name
    _node_rows = [
        Document(filename="a.md", title="A", summary="alpha"),
        Document(filename="b.md", title="B", summary="beta"),
    ]
    coco_env.context_provider.provide(
        KG_DB, falkor.ConnectionFactory(uri=_FALKORDB_URI, graph=falkor_graph_name)
    )
    app = coco.App(
        coco.AppConfig(name="test_node_upsert", environment=coco_env),
        _declare_documents_only,
    )
    await app.update()

    rows = await _read_nodes(falkor_graph_name, "Document")
    by_fn = {r["filename"]: r for r in rows}
    assert set(by_fn) == {"a.md", "b.md"}
    assert by_fn["a.md"]["title"] == "A"
    assert by_fn["a.md"]["summary"] == "alpha"


@requires_falkordb_server
@pytest.mark.asyncio
async def test_reconcile_twice_is_noop(falkor_graph_name: str) -> None:
    """Second update with identical input must not produce duplicate writes."""
    global _current_graph, _node_rows
    _current_graph = falkor_graph_name
    _node_rows = [Document(filename="a.md", title="A", summary="alpha")]
    coco_env.context_provider.provide(
        KG_DB, falkor.ConnectionFactory(uri=_FALKORDB_URI, graph=falkor_graph_name)
    )
    app = coco.App(
        coco.AppConfig(name="test_noop", environment=coco_env),
        _declare_documents_only,
    )
    await app.update()
    rows1 = await _read_nodes(falkor_graph_name, "Document")
    await app.update()  # identical input
    rows2 = await _read_nodes(falkor_graph_name, "Document")
    assert rows1 == rows2
    assert len(rows2) == 1


@requires_falkordb_server
@pytest.mark.asyncio
async def test_modify_value_triggers_one_upsert(falkor_graph_name: str) -> None:
    global _current_graph, _node_rows
    _current_graph = falkor_graph_name
    _node_rows = [Document(filename="a.md", title="A", summary="alpha")]
    coco_env.context_provider.provide(
        KG_DB, falkor.ConnectionFactory(uri=_FALKORDB_URI, graph=falkor_graph_name)
    )
    app = coco.App(
        coco.AppConfig(name="test_modify", environment=coco_env),
        _declare_documents_only,
    )
    await app.update()
    _node_rows[0] = Document(filename="a.md", title="A", summary="ALPHA-2")
    await app.update()
    rows = await _read_nodes(falkor_graph_name, "Document")
    assert len(rows) == 1
    assert rows[0]["summary"] == "ALPHA-2"


@requires_falkordb_server
@pytest.mark.asyncio
async def test_delete_removes_node(falkor_graph_name: str) -> None:
    global _current_graph, _node_rows
    _current_graph = falkor_graph_name
    _node_rows = [
        Document(filename="a.md", title="A", summary="alpha"),
        Document(filename="b.md", title="B", summary="beta"),
    ]
    coco_env.context_provider.provide(
        KG_DB, falkor.ConnectionFactory(uri=_FALKORDB_URI, graph=falkor_graph_name)
    )
    app = coco.App(
        coco.AppConfig(name="test_delete", environment=coco_env),
        _declare_documents_only,
    )
    await app.update()
    assert {
        r["filename"] for r in await _read_nodes(falkor_graph_name, "Document")
    } == {
        "a.md",
        "b.md",
    }
    _node_rows.pop()  # drop b.md
    await app.update()
    assert {
        r["filename"] for r in await _read_nodes(falkor_graph_name, "Document")
    } == {"a.md"}


@requires_falkordb_server
@pytest.mark.asyncio
async def test_drop_destroys_tables_with_their_contents(
    falkor_graph_name: str,
) -> None:
    """App.drop() destroys the system-managed tables: the nodes, the
    relationships, and the indexes the connector created."""
    global _current_graph, _rel_pairs, _tables_declared, _entity_with_schema
    _current_graph = falkor_graph_name
    _rel_pairs = [("alice", "bob"), ("bob", "carol")]
    _tables_declared = True
    _entity_with_schema = True
    coco_env.context_provider.provide(
        KG_DB, falkor.ConnectionFactory(uri=_FALKORDB_URI, graph=falkor_graph_name)
    )
    app = coco.App(
        coco.AppConfig(name="test_drop", environment=coco_env),
        _declare_entities_and_relationships,
    )
    await app.update()
    assert len(await _read_nodes(falkor_graph_name, "Entity")) == 3
    assert len(await _read_relationships(falkor_graph_name, "REL")) == 2
    assert await _indexed_labels(falkor_graph_name) == {"Entity", "REL"}

    await app.drop()

    assert await _count(falkor_graph_name, "MATCH (n) RETURN count(n)") == 0
    assert await _count(falkor_graph_name, "MATCH ()-[r]->() RETURN count(r)") == 0
    assert await _indexed_labels(falkor_graph_name) == set()


@requires_falkordb_server
@pytest.mark.asyncio
async def test_undeclared_tables_are_destroyed(falkor_graph_name: str) -> None:
    """Tables that are no longer declared go the same way as on drop."""
    global _current_graph, _rel_pairs, _tables_declared, _entity_with_schema
    _current_graph = falkor_graph_name
    _rel_pairs = [("alice", "bob")]
    _tables_declared = True
    _entity_with_schema = True
    coco_env.context_provider.provide(
        KG_DB, falkor.ConnectionFactory(uri=_FALKORDB_URI, graph=falkor_graph_name)
    )
    app = coco.App(
        coco.AppConfig(name="test_undeclare", environment=coco_env),
        _declare_entities_and_relationships,
    )
    await app.update()
    assert await _count(falkor_graph_name, "MATCH (n) RETURN count(n)") == 2

    _tables_declared = False
    await app.update()
    assert await _count(falkor_graph_name, "MATCH (n) RETURN count(n)") == 0
    assert await _indexed_labels(falkor_graph_name) == set()


@requires_falkordb_server
@pytest.mark.asyncio
async def test_primary_key_change_rebuilds_table(falkor_graph_name: str) -> None:
    """Changing the primary key field rebuilds the table: nodes keyed by the
    old field are deleted rather than left behind as untracked stragglers."""
    global _current_graph, _node_rows, _doc_pk
    _current_graph = falkor_graph_name
    _node_rows = [
        Document(filename="a.md", title="A", summary="alpha"),
        Document(filename="b.md", title="B", summary="beta"),
    ]
    _doc_pk = "filename"
    coco_env.context_provider.provide(
        KG_DB, falkor.ConnectionFactory(uri=_FALKORDB_URI, graph=falkor_graph_name)
    )
    app = coco.App(
        coco.AppConfig(name="test_pk_change", environment=coco_env),
        _declare_documents_keyed_by_doc_pk,
    )
    await app.update()
    assert len(await _read_nodes(falkor_graph_name, "Document")) == 2

    # Re-key by title, with new titles: the old nodes match neither key.
    _doc_pk = "title"
    _node_rows = [
        Document(filename="a.md", title="A2", summary="alpha"),
        Document(filename="b.md", title="B2", summary="beta"),
    ]
    await app.update()
    assert {r["title"] for r in await _read_nodes(falkor_graph_name, "Document")} == {
        "A2",
        "B2",
    }


@requires_falkordb_server
@pytest.mark.asyncio
async def test_attaching_a_schema_keeps_nodes_and_relationships(
    falkor_graph_name: str,
) -> None:
    """Attaching a schema to a schemaless table is not a rebuild: the nodes
    stay, and so do the relationships of other tables attached to them."""
    global _current_graph, _rel_pairs, _tables_declared, _entity_with_schema
    _current_graph = falkor_graph_name
    _rel_pairs = [("alice", "bob"), ("bob", "carol")]
    _tables_declared = True
    _entity_with_schema = False
    coco_env.context_provider.provide(
        KG_DB, falkor.ConnectionFactory(uri=_FALKORDB_URI, graph=falkor_graph_name)
    )
    app = coco.App(
        coco.AppConfig(name="test_schema_attach", environment=coco_env),
        _declare_entities_and_relationships,
    )
    try:
        await app.update()
        assert len(await _read_relationships(falkor_graph_name, "REL")) == 2

        _entity_with_schema = True
        await app.update()
        assert len(await _read_nodes(falkor_graph_name, "Entity")) == 3
        assert len(await _read_relationships(falkor_graph_name, "REL")) == 2
    finally:
        _entity_with_schema = True


@requires_falkordb_server
@pytest.mark.asyncio
async def test_relationship_upsert_with_endpoint_merge(
    falkor_graph_name: str,
) -> None:
    """Verify three-MERGE relationship insert: source endpoint, target endpoint, edge."""
    global _current_graph, _rel_pairs
    _current_graph = falkor_graph_name
    _rel_pairs = [("alice", "bob"), ("bob", "carol")]
    coco_env.context_provider.provide(
        KG_DB, falkor.ConnectionFactory(uri=_FALKORDB_URI, graph=falkor_graph_name)
    )
    app = coco.App(
        coco.AppConfig(name="test_rel_upsert", environment=coco_env),
        _declare_entities_and_relationships,
    )
    await app.update()

    nodes = await _read_nodes(falkor_graph_name, "Entity")
    assert {n["value"] for n in nodes} == {"alice", "bob", "carol"}

    edges = await _read_relationships(falkor_graph_name, "REL")
    assert len(edges) == 2
    pairs = {(s["value"], t["value"]) for s, t, _ in edges}
    assert pairs == {("alice", "bob"), ("bob", "carol")}
    # Relationship props are written
    for _, _, rel in edges:
        assert rel["predicate"] == "connects"


async def _graph_exists(graph_name: str) -> bool:
    client = AsyncFalkorDB.from_url(_FALKORDB_URI)
    try:
        return graph_name in await client.list_graphs()
    finally:
        await client.aclose()


def _planned(actions: list[Any]) -> tuple[dict[str, Any], dict[tuple[str, Any], Any]]:
    """Split preview actions into table actions by table name and record
    actions by ``(table name, record id)``."""
    from cocoindex.connectors.falkordb import _target as falkor_target

    tables = {
        a.key.table_name: a
        for a in actions
        if isinstance(a, falkor_target._TableAction)
    }
    records = {
        (a.table_name, a.record_id): a
        for a in actions
        if isinstance(a, falkor_target._RecordAction)
    }
    assert len(actions) == len(tables) + len(records)
    return tables, records


if HAS_FALKORDB:

    class _FakeGraph:
        """Records every ``query(cypher, params)``; has no ``execute_command``,
        so the best-effort constraint DDL is skipped."""

        def __init__(self) -> None:
            self.queries: list[tuple[str, dict[str, Any]]] = []

        async def query(
            self, cypher: str, params: dict[str, Any] | None = None
        ) -> None:
            self.queries.append((cypher, params or {}))

    class _FakeConnectionFactory(falkor.ConnectionFactory):
        """A ``ConnectionFactory`` whose ``acquire()`` returns a ``_FakeGraph``
        and counts its calls, with no server."""

        def __init__(self) -> None:
            super().__init__(uri=_FALKORDB_URI)
            self.fake_graph = _FakeGraph()
            self.num_acquires = 0

        async def acquire(self) -> Any:
            self.num_acquires += 1
            return self.fake_graph


def _record_writes(graph: Any) -> list[tuple[str, Any]]:
    """The record writes ``graph`` received, in order, as ``(kind, record id)``
    for the ``Entity`` / ``REL`` tables of ``_declare_entities_and_relationships``."""
    kinds = {
        build_node_upsert("Entity", ["value"], has_value_fields=False): "node_upsert",
        build_relationship_upsert(
            "REL", "Entity", ["value"], "Entity", ["value"], ["id"], True
        ): "relation_upsert",
        build_relationship_delete("REL", ["id"]): "relation_delete",
        build_node_delete("Entity", ["value"]): "node_delete",
    }
    return [
        (kinds[cypher], params.get("key_0", params.get("rel_key_0")))
        for cypher, params in graph.queries
        if cypher in kinds
    ]


@requires_falkordb
@pytest.mark.asyncio
async def test_record_sink_orders_writes_across_tables() -> None:
    """The node and relation records of one graph land in one batch of the
    shared record sink, applied as node upserts → relation upserts → relation
    deletes → node deletes."""
    global _rel_pairs, _tables_declared, _entity_with_schema
    _tables_declared = True
    _entity_with_schema = True
    factory = _FakeConnectionFactory()
    coco_env.context_provider.provide(KG_DB, factory)
    app = coco.App(
        coco.AppConfig(name="test_record_sink_order", environment=coco_env),
        _declare_entities_and_relationships,
    )

    _rel_pairs = [("a", "b")]
    await app.update()
    writes = _record_writes(factory.fake_graph)
    # Kinds are in order; within a kind, the order is unspecified.
    assert [kind for kind, _ in writes] == [
        "node_upsert",
        "node_upsert",
        "relation_upsert",
    ]
    assert set(writes) == {
        ("node_upsert", "a"),
        ("node_upsert", "b"),
        ("relation_upsert", "a->b"),
    }

    factory.fake_graph.queries.clear()
    _rel_pairs = [("c", "d")]
    await app.update()
    writes = _record_writes(factory.fake_graph)
    assert [kind for kind, _ in writes] == [
        "node_upsert",
        "node_upsert",
        "relation_upsert",
        "relation_delete",
        "node_delete",
        "node_delete",
    ]
    assert set(writes) == {
        ("node_upsert", "c"),
        ("node_upsert", "d"),
        ("relation_upsert", "c->d"),
        ("relation_delete", "a->b"),
        ("node_delete", "a"),
        ("node_delete", "b"),
    }


@requires_falkordb
@pytest.mark.asyncio
async def test_preview_never_acquires_a_connection() -> None:
    """A preview plans tables and records without ``acquire()`` — before the
    first update and against the applied state alike."""
    global _rel_pairs, _tables_declared, _entity_with_schema
    _tables_declared = True
    _entity_with_schema = True
    factory = _FakeConnectionFactory()
    coco_env.context_provider.provide(KG_DB, factory)
    app = coco.App(
        coco.AppConfig(name="test_preview_no_acquire", environment=coco_env),
        _declare_entities_and_relationships,
    )

    _rel_pairs = [("a", "b")]
    tables, records = _planned(await app.update(preview=True))
    assert set(tables) == {"Entity", "REL"}
    assert set(records) == {("Entity", "a"), ("Entity", "b"), ("REL", "a->b")}
    assert factory.num_acquires == 0

    await app.update()
    num_acquires = factory.num_acquires
    assert num_acquires > 0

    _rel_pairs = [("a", "c")]
    tables, records = _planned(await app.update(preview=True))
    assert {k: a.value is None for k, a in records.items()} == {
        ("Entity", "b"): True,
        ("Entity", "c"): False,
        ("REL", "a->b"): True,
        ("REL", "a->c"): False,
    }
    assert factory.num_acquires == num_acquires


@requires_falkordb_server
@pytest.mark.asyncio
async def test_preview_plans_tables_and_records(falkor_graph_name: str) -> None:
    """Preview plans the tables and their records without touching the graph;
    a real update then applies them."""
    global _current_graph, _rel_pairs, _tables_declared, _entity_with_schema
    _current_graph = falkor_graph_name
    _rel_pairs = [("alice", "bob")]
    _tables_declared = True
    _entity_with_schema = True
    factory = falkor.ConnectionFactory(uri=_FALKORDB_URI, graph=falkor_graph_name)
    coco_env.context_provider.provide(KG_DB, factory)
    app = coco.App(
        coco.AppConfig(name="test_preview", environment=coco_env),
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
    # Nothing is written: the graph does not even exist yet.
    assert not await _graph_exists(falkor_graph_name)

    await app.update()
    assert {n["value"] for n in await _read_nodes(falkor_graph_name, "Entity")} == {
        "alice",
        "bob",
    }
    assert len(await _read_relationships(falkor_graph_name, "REL")) == 1

    # Against the applied state: the container actions are retained, alongside the new edge and its new
    # endpoint; the unchanged records are not planned. Nothing is applied.
    _rel_pairs = [("alice", "bob"), ("bob", "carol")]
    tables, records = _planned(await app.update(preview=True))
    assert {name: action.main_action for name, action in tables.items()} == {
        "Entity": None,
        "REL": None,
    }
    assert set(records) == {("Entity", "carol"), ("REL", "bob->carol")}
    assert len(await _read_nodes(falkor_graph_name, "Entity")) == 2
    assert len(await _read_relationships(falkor_graph_name, "REL")) == 1

    await app.update()
    assert len(await _read_nodes(falkor_graph_name, "Entity")) == 3
    assert len(await _read_relationships(falkor_graph_name, "REL")) == 2


@requires_falkordb_server
@pytest.mark.asyncio
async def test_vector_index_attached(falkor_graph_name: str) -> None:
    """declare_vector_index() should create a queryable vector index."""
    global _current_graph, _node_rows
    _current_graph = falkor_graph_name
    _node_rows = []  # No record traffic — just verify the index DDL fires.

    @dataclass
    class _DocWithVec:
        filename: str
        title: str
        summary: str  # placeholder; vector field is created separately by index

    async def _declare_doc_with_vector_index() -> None:
        schema = await falkor.TableSchema.from_class(
            _DocWithVec, primary_key="filename"
        )
        table: Any = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "doc_table"),
            falkor.mount_table_target,
            KG_DB,
            "VecDoc",
            schema,
            primary_key="filename",
        )
        table.declare_vector_index(field="summary", metric="cosine", dimension=4)

    coco_env.context_provider.provide(
        KG_DB, falkor.ConnectionFactory(uri=_FALKORDB_URI, graph=falkor_graph_name)
    )
    app = coco.App(
        coco.AppConfig(name="test_vec_idx", environment=coco_env),
        _declare_doc_with_vector_index,
    )
    await app.update()

    client = AsyncFalkorDB.from_url(_FALKORDB_URI)
    g = client.select_graph(falkor_graph_name)
    res = await g.query("CALL db.indexes()")
    found = False
    for row in res.result_set:
        # Each row: [label, fields, types_dict, options_dict, ...].
        if row[0] == "VecDoc" and "summary" in row[1]:
            found = True
            break
    await client.aclose()
    assert found, "Expected vector index on VecDoc.summary"


@requires_falkordb_server
@pytest.mark.asyncio
async def test_falkordb_table_schema_evolution_under_full_reprocess(
    falkor_graph_name: str,
) -> None:
    """A schema change must still be applied and child records re-upserted when running with full_reprocess=True."""
    global _current_graph
    _current_graph = falkor_graph_name

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
        schema = await falkor.TableSchema.from_class(doc_type, primary_key="filename")
        table: Any = await coco.use_mount(
            coco.component_subpath("setup", "doc_table"),
            falkor.mount_table_target,
            KG_DB,
            "DocEvolve",
            schema,
            primary_key="filename",
        )
        for doc in docs:
            table.declare_record(row=doc)

    coco_env.context_provider.provide(
        KG_DB, falkor.ConnectionFactory(uri=_FALKORDB_URI, graph=falkor_graph_name)
    )
    app = coco.App(
        coco.AppConfig(name="test_falkordb_schema_reprocess", environment=coco_env),
        _declare_docs,
    )
    await app.update()

    nodes_v1 = await _read_nodes(falkor_graph_name, "DocEvolve")
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

    nodes_v2 = await _read_nodes(falkor_graph_name, "DocEvolve")
    assert len(nodes_v2) == 2
    by_fn2 = {r["filename"]: r for r in nodes_v2}
    assert by_fn2["doc1.md"]["summary"] == "summary 1"
    assert by_fn2["doc2.md"]["summary"] == "summary 2"


# =============================================================================
# Relationship and vector index identity
# =============================================================================


async def _indexes(graph_name: str) -> set[tuple[str, str, str, str]]:
    """``(entity type, label or relationship type, field, index type)`` of
    every index."""
    client = AsyncFalkorDB.from_url(_FALKORDB_URI)
    g = client.select_graph(graph_name)
    res = await g.query(
        "CALL db.indexes() YIELD entitytype, label, types "
        "RETURN entitytype, label, types"
    )
    await client.aclose()
    return {
        (entity_type, label, field, index_type)
        for entity_type, label, types in res.result_set
        for field, index_types in types.items()
        for index_type in index_types
    }


@requires_falkordb
class TestVectorIndexApplier:
    """Several declared names can reach one FalkorDB index, which FalkorDB
    identifies by its label and field."""

    @staticmethod
    def _applier(queries: list[str]) -> Any:
        from cocoindex.connectors.falkordb import _target as falkor_target

        class _FakeGraph:
            async def query(self, cypher: str, params: Any = None) -> Any:
                queries.append(cypher)

        class _FakeFactory(falkor_target.ConnectionFactory):
            async def acquire(self) -> Any:
                return _FakeGraph()

        return falkor_target._VectorIndexApplier(_FakeFactory(uri=_FALKORDB_URI))

    @staticmethod
    def _action(name: str, field: str, *, create: bool) -> Any:
        from cocoindex.connectors.falkordb import _target as falkor_target

        spec = falkor_target._VectorIndexSpec(field=field, metric="cosine", dimension=4)
        return falkor_target._VectorIndexAction(
            name=name, table_name="Doc", field=field, spec=spec if create else None
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
            build_vector_index_drop("Doc", "emb"),
            build_vector_index_create("Doc", "emb", 4, "cosine"),
            build_vector_index_drop("Doc", "other"),
        ]

    @pytest.mark.asyncio
    async def test_two_names_for_one_index_fail(self) -> None:
        queries: list[str] = []
        with pytest.raises(ValueError, match="both index Doc.emb"):
            await self._applier(queries)(
                None,
                [
                    self._action("a", "emb", create=True),
                    self._action("b", "emb", create=True),
                ],
            )
        assert queries == []


@requires_falkordb_server
@pytest.mark.asyncio
async def test_moved_relationship_leaves_no_duplicate(falkor_graph_name: str) -> None:
    """An edge with an explicit id whose endpoints change is moved, not
    copied: the edge between the old endpoints goes. A duplicate left by an
    earlier version goes on a full reprocess."""
    to_id = "bob"

    async def declare() -> None:
        people: Any = await coco.use_mount(
            coco.component_subpath("node"), falkor.mount_table_target, KG_DB, "Person"
        )
        for name in ("alice", "bob", "carol"):
            people.declare_record(row={"id": name})
        knows: Any = await coco.use_mount(
            coco.component_subpath("rel"),
            falkor.mount_relation_target,
            KG_DB,
            "KNOWS",
            people,
            people,
        )
        knows.declare_relation(
            from_id="alice", to_id=to_id, record={"id": "k1", "since": 1}
        )

    async def knows_edges() -> list[tuple[str, str, str]]:
        return sorted(
            (s["id"], t["id"], r["id"])
            for s, t, r in await _read_relationships(falkor_graph_name, "KNOWS")
        )

    coco_env.context_provider.provide(
        KG_DB, falkor.ConnectionFactory(uri=_FALKORDB_URI, graph=falkor_graph_name)
    )
    app = coco.App(
        coco.AppConfig(name="test_falkordb_moved_rel", environment=coco_env),
        declare,
    )
    await app.update()
    assert await knows_edges() == [("alice", "bob", "k1")]

    to_id = "carol"
    await app.update()
    assert await knows_edges() == [("alice", "carol", "k1")]

    # A duplicate an earlier version left behind when the edge moved, where
    # the best-effort uniqueness constraint on the id was not created.
    client = AsyncFalkorDB.from_url(_FALKORDB_URI)
    await client.connection.execute_command(
        "GRAPH.CONSTRAINT",
        "DROP",
        falkor_graph_name,
        "UNIQUE",
        "RELATIONSHIP",
        "KNOWS",
        "PROPERTIES",
        "1",
        "id",
    )
    await client.select_graph(falkor_graph_name).query(
        "MATCH (s:Person {id: 'alice'}), (t:Person {id: 'bob'}) "
        "CREATE (s)-[:KNOWS {id: 'k1', since: 1}]->(t)"
    )
    await client.aclose()
    await app.update(full_reprocess=True)
    assert await knows_edges() == [("alice", "carol", "k1")]


@requires_falkordb_server
@pytest.mark.asyncio
async def test_renamed_vector_index_is_kept(falkor_graph_name: str) -> None:
    """FalkorDB identifies a vector index by its label and field, not the
    declared name: renaming it keeps it. Two declared names on one field
    fail."""
    index_names = ["idx_a"]

    async def declare() -> None:
        docs: Any = await coco.use_mount(
            coco.component_subpath("node"), falkor.mount_table_target, KG_DB, "VecDoc"
        )
        docs.declare_record(row={"id": "d1", "summary": "s"})
        for name in index_names:
            docs.declare_vector_index(name=name, field="summary", dimension=4)

    coco_env.context_provider.provide(
        KG_DB, falkor.ConnectionFactory(uri=_FALKORDB_URI, graph=falkor_graph_name)
    )
    app = coco.App(
        coco.AppConfig(name="test_falkordb_vec_rename", environment=coco_env),
        declare,
    )
    vector_index = ("NODE", "VecDoc", "summary", "VECTOR")
    await app.update()
    assert vector_index in await _indexes(falkor_graph_name)

    index_names = ["idx_b"]
    await app.update()
    assert vector_index in await _indexes(falkor_graph_name)

    # A second name for the same field fails where it is declared.
    index_names = ["idx_b", "idx_c"]
    with pytest.raises(Exception, match="both index VecDoc.summary"):
        await app.update()

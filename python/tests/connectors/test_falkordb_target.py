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
        build_node_index_create,
        build_node_index_drop,
        build_node_upsert,
        build_relationship_delete,
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


@requires_falkordb
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
        # Critical: SET only on the relationship, not on s or t. Endpoint
        # properties are owned by their table's own _RecordHandler.
        cypher = build_relationship_upsert("REL", "A", ["x"], "B", ["y"], ["id"], True)
        # Three SET-able sites: s, t, r. Only r should have SET applied.
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


@requires_falkordb
class TestRelationshipDeleteCypher:
    def test_does_not_cascade(self) -> None:
        # Endpoints are NOT deleted by the relationship delete — they live
        # under their own table handlers' tracking and reconciliation.
        cypher = build_relationship_delete("REL", ["id"])
        assert cypher == "MATCH ()-[r:`REL` {`id`: $key_0}]->() DELETE r"
        assert "DELETE s" not in cypher
        assert "DELETE t" not in cypher


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


async def _declare_entities_and_relationships() -> None:
    entity_schema = await falkor.TableSchema.from_class(Entity, primary_key="value")
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

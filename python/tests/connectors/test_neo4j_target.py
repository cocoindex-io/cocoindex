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
from typing import Any

import pytest
import pytest_asyncio

import cocoindex as coco
from cocoindex.connectors.neo4j._cypher import (
    build_constraint_create,
    build_constraint_drop,
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

requires_neo4j_server = pytest.mark.skipif(
    not (HAS_NEO4J and _HAS_NEO4J_SERVER),
    reason="NEO4J_TEST_SERVER is not set",
)

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
# Integration tests — require running Neo4j (testcontainers spins one up)
# =============================================================================


@pytest.fixture(scope="module")
def neo4j_uri_auth() -> Iterator[tuple[str, tuple[str, str]]]:
    """Spin up a Neo4j 5.13 container once per test module."""
    if not (HAS_NEO4J and _HAS_NEO4J_SERVER):
        pytest.skip("NEO4J_TEST_SERVER is not set")

    from testcontainers.neo4j import Neo4jContainer  # type: ignore[import-untyped]

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

    async def _declare_entities_and_relationships() -> None:
        entity_schema = await neo.TableSchema.from_class(Entity, primary_key="value")
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

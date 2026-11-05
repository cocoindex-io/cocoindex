"""Tests for the LadybugDB target connector.

Run with:
    uv run pytest python/tests/connectors/test_ladybug_target.py -v

Unit tests run without a database. Integration tests use a temporary LadybugDB
file (LadybugDB is embedded, so no server or Docker is needed) and run
whenever the ``ladybug`` package is installed.
"""

from __future__ import annotations

import datetime
import uuid as uuid_mod
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any
import tempfile

import numpy as np
import pytest
from numpy.typing import NDArray

import cocoindex as coco
from cocoindex.resources.schema import VectorSchema

from tests import common

coco_env = common.create_test_env(__file__)


# =============================================================================
# Skip gates
# =============================================================================

try:
    import ladybug  # type: ignore[import-not-found]  # noqa: F401

    HAS_LADYBUG = True
except ImportError:
    HAS_LADYBUG = False

requires_ladybug = pytest.mark.skipif(
    not HAS_LADYBUG, reason="ladybug is not installed"
)

if HAS_LADYBUG:
    from cocoindex.connectors import ladybug as lbg  # type: ignore[attr-defined]
    from cocoindex.connectors.ladybug import _target as lbg_target  # type: ignore[import-untyped]
    from cocoindex.connectors.ladybug._cypher import (  # type: ignore[import-untyped]
        build_add_column,
        build_drop_column,
        build_node_delete,
        build_node_delete_all,
        build_node_table_create,
        build_node_upsert,
        build_rel_table_create,
        build_relationship_delete,
        build_relationship_delete_all,
        build_relationship_upsert,
        build_table_drop,
        validate_identifier,
    )

    LADYBUG_DB: coco.ContextKey[Any] = coco.ContextKey("test_ladybug_db")


# =============================================================================
# Unit tests — identifier validation (no DB)
# =============================================================================


@requires_ladybug
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
        with pytest.raises(ValueError, match="Invalid LadybugDB"):
            validate_identifier(name, "test")


@requires_ladybug
class TestIdentifierValidationAtApiEntryPoints:
    def test_table_schema_invalid_column(self) -> None:
        with pytest.raises(ValueError, match="column name"):
            lbg.TableSchema(
                columns={
                    "id": lbg.ColumnDef("STRING"),
                    "bad-name": lbg.ColumnDef("STRING"),
                },
                primary_key="id",
            )

    def test_table_schema_pk_must_exist_in_columns(self) -> None:
        with pytest.raises(ValueError, match="primary_key"):
            lbg.TableSchema(
                columns={"id": lbg.ColumnDef("STRING")},
                primary_key="missing",
            )

    def test_node_target_invalid_name(self) -> None:
        schema = lbg.TableSchema(
            columns={"id": lbg.ColumnDef("STRING")}, primary_key="id"
        )
        with pytest.raises(ValueError, match="table name"):
            lbg.node_target(LADYBUG_DB, "bad-table", schema)

    def test_node_target_pk_must_match_schema(self) -> None:
        schema = lbg.TableSchema(
            columns={"id": lbg.ColumnDef("STRING")}, primary_key="id"
        )
        with pytest.raises(ValueError, match="primary_key"):
            lbg.node_target(LADYBUG_DB, "T", schema, primary_key="other")


# =============================================================================
# Unit tests — Cypher generation (no DB)
# =============================================================================


@requires_ladybug
class TestNodeUpsertCypher:
    def test_single_pk_with_props(self) -> None:
        assert (
            build_node_upsert("Document", ["filename"], ["title", "count"])
            == "MERGE (n:`Document` {`filename`: $key_0}) "
            "SET n.`title` = $prop_0, n.`count` = $prop_1"
        )

    def test_single_pk_no_props(self) -> None:
        assert (
            build_node_upsert("Document", ["filename"], [])
            == "MERGE (n:`Document` {`filename`: $key_0})"
        )

    def test_empty_pk_raises(self) -> None:
        with pytest.raises(ValueError):
            build_node_upsert("X", [], ["a"])


@requires_ladybug
class TestNodeDeleteCypher:
    def test_uses_detach_delete(self) -> None:
        # DETACH DELETE is critical: protects against the shared-entity case
        # where a node still has incident edges that another flow owns.
        assert (
            build_node_delete("Document", ["filename"])
            == "MATCH (n:`Document` {`filename`: $key_0}) DETACH DELETE n"
        )

    def test_delete_all_detaches_whole_label(self) -> None:
        assert build_node_delete_all("Document") == (
            "MATCH (n:`Document`) DETACH DELETE n"
        )


@requires_ladybug
class TestRelationshipUpsertCypher:
    def test_merges_after_deleting_a_moved_edge(self) -> None:
        assert build_relationship_upsert(
            "REL", "Entity", ["value"], "Entity", ["value"], ["id"], ["since"], False
        ) == (
            "MERGE (s:`Entity` {`value`: $from_key_0}) "
            "MERGE (t:`Entity` {`value`: $to_key_0}) "
            "WITH s, t "
            "OPTIONAL MATCH (a)-[o:`REL` {`id`: $rel_key_0}]->(b) "
            "WHERE a <> s OR b <> t "
            "DELETE o "
            "WITH DISTINCT s, t "
            "MERGE (s)-[r:`REL` {`id`: $rel_key_0}]->(t) "
            "SET r.`since` = $rel_prop_0"
        )

    def test_no_set_on_endpoints(self) -> None:
        # Critical: SET only on the relationship, not on s or t. Endpoint
        # properties are owned by their table's own _RecordHandler.
        cypher = build_relationship_upsert(
            "REL", "A", ["x"], "B", ["y"], ["id"], ["w"], False
        )
        # Three SET-able sites: s, t, r. Only r should have SET applied.
        assert "SET s" not in cypher
        assert "SET t" not in cypher
        assert "SET r.`w` = $rel_prop_0" in cypher

    def test_no_props(self) -> None:
        cypher = build_relationship_upsert(
            "REL", "A", ["x"], "B", ["y"], ["id"], [], False
        )
        assert cypher.endswith("MERGE (s)-[r:`REL` {`id`: $rel_key_0}]->(t)")

    def test_empty_pk_raises(self) -> None:
        with pytest.raises(ValueError):
            build_relationship_upsert("R", "A", [], "B", ["y"], ["id"], [], False)


@requires_ladybug
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


@requires_ladybug
class TestTableDdlCypher:
    def test_node_table_create(self) -> None:
        assert (
            build_node_table_create(
                "Document", [("filename", "STRING"), ("count", "INT64")], "filename"
            )
            == "CREATE NODE TABLE `Document` (`filename` STRING, `count` INT64, "
            "PRIMARY KEY (`filename`))"
        )

    def test_node_table_create_no_columns_raises(self) -> None:
        with pytest.raises(ValueError):
            build_node_table_create("X", [], "id")

    def test_rel_table_create(self) -> None:
        assert (
            build_rel_table_create(
                "REL", "Entity", "Entity", [("id", "STRING"), ("since", "INT64")]
            )
            == "CREATE REL TABLE `REL` (FROM `Entity` TO `Entity`, "
            "`id` STRING, `since` INT64)"
        )

    def test_rel_table_create_no_props(self) -> None:
        assert (
            build_rel_table_create("REL", "A", "B", [])
            == "CREATE REL TABLE `REL` (FROM `A` TO `B`)"
        )

    def test_table_drop(self) -> None:
        assert build_table_drop("Document") == "DROP TABLE `Document`"

    def test_add_column(self) -> None:
        assert (
            build_add_column("Document", "title", "STRING")
            == "ALTER TABLE `Document` ADD `title` STRING"
        )

    def test_drop_column(self) -> None:
        assert (
            build_drop_column("Document", "title")
            == "ALTER TABLE `Document` DROP `title`"
        )


# =============================================================================
# Unit tests — TableSchema.from_class type mapping (no DB)
# =============================================================================


@requires_ladybug
class TestTableSchemaFromClass:
    @pytest.mark.asyncio
    async def test_basic_dataclass(self) -> None:
        @dataclass
        class Row:
            id: str
            count: int
            score: float
            flag: bool

        schema = await lbg.TableSchema.from_class(Row, primary_key="id")
        assert schema.primary_key == "id"
        assert schema.columns["id"].type == "STRING"
        assert schema.columns["count"].type == "INT64"
        assert schema.columns["score"].type == "DOUBLE"
        assert schema.columns["flag"].type == "BOOL"
        assert schema.value_field_names == ["count", "score", "flag"]

    @pytest.mark.asyncio
    async def test_integer_widths(self) -> None:
        @dataclass
        class Row:
            id: int
            a: np.int8
            b: np.int16
            c: np.int32
            d: np.uint64

        schema = await lbg.TableSchema.from_class(Row, primary_key="id")
        assert schema.columns["a"].type == "INT8"
        assert schema.columns["b"].type == "INT16"
        assert schema.columns["c"].type == "INT32"
        assert schema.columns["d"].type == "UINT64"

    @pytest.mark.asyncio
    async def test_temporal_and_misc_types(self) -> None:
        @dataclass
        class Row:
            id: int
            day: datetime.date
            ts: datetime.datetime
            span: datetime.timedelta
            blob: bytes
            uid: uuid_mod.UUID

        schema = await lbg.TableSchema.from_class(Row, primary_key="id")
        assert schema.columns["day"].type == "DATE"
        assert schema.columns["ts"].type == "TIMESTAMP"
        assert schema.columns["span"].type == "INTERVAL"
        assert schema.columns["blob"].type == "BLOB"
        assert schema.columns["uid"].type == "UUID"

    @pytest.mark.asyncio
    async def test_complex_types_map_to_json(self) -> None:

        @dataclass
        class Row:
            id: str
            meta: dict[str, Any]
            tags: list[int]

        schema = await lbg.TableSchema.from_class(Row, primary_key="id")
        assert schema.columns["meta"].type == "JSON"
        # A homogeneous scalar list maps to a native list column.
        assert schema.columns["tags"].type == "INT64[]"

    @pytest.mark.asyncio
    async def test_vector_column(self) -> None:
        @dataclass
        class Row:
            id: str
            embedding: Annotated[
                NDArray[np.float32],
                VectorSchema(size=4, dtype=np.float32),
            ]

        schema = await lbg.TableSchema.from_class(Row, primary_key="id")
        assert schema.columns["embedding"].type == "FLOAT[4]"

    @pytest.mark.asyncio
    async def test_ndarray_without_vector_schema_raises(self) -> None:
        @dataclass
        class Row:
            id: str
            embedding: NDArray[np.float32]

        with pytest.raises(ValueError, match="VectorSchemaProvider is required"):
            await lbg.TableSchema.from_class(Row, primary_key="id")

    @pytest.mark.asyncio
    async def test_type_override(self) -> None:
        @dataclass
        class Row:
            id: str
            score: Annotated[float, lbg.LadybugType("DECIMAL", encoder=str)]

        schema = await lbg.TableSchema.from_class(Row, primary_key="id")
        assert schema.columns["score"].type == "DECIMAL"


# =============================================================================
# Table reconciliation unit tests (no DB needed)
# =============================================================================


@requires_ladybug
class TestTableReconcile:
    def test_schema_evolution_under_full_reprocess(self) -> None:
        """Schema evolution during full reprocess must compute column actions."""
        from cocoindex.connectorkits import statediff, target

        handler = lbg_target._TableHandler()
        schema_v1 = lbg.TableSchema(
            columns={
                "id": lbg.ColumnDef("STRING"),
                "a": lbg.ColumnDef("STRING"),
                "b": lbg.ColumnDef("STRING"),
            },
            primary_key="id",
        )
        schema_v2 = lbg.TableSchema(
            columns={
                "id": lbg.ColumnDef("STRING"),
                "a": lbg.ColumnDef("STRING"),
                "c": lbg.ColumnDef("INT64"),
            },
            primary_key="id",
        )

        def spec_of(schema: lbg.TableSchema[Any]) -> lbg_target._TableSpec:
            return lbg_target._TableSpec(
                table_schema=schema,
                primary_key="id",
                is_relation=False,
                from_label=None,
                from_pk_field=None,
                to_label=None,
                to_pk_field=None,
                managed_by=target.ManagedBy.SYSTEM,
            )

        key = ("db", "T")
        prev_record = statediff.MutualTrackingRecord(
            tracking_record=lbg_target._table_composite_tracking_record_from_spec(
                spec_of(schema_v1)
            ),
            managed_by=target.ManagedBy.SYSTEM,
        )

        out = handler.reconcile(key, spec_of(schema_v2), [prev_record], False)
        assert out is not None
        assert out.action.main_action is None
        # `b` removed, `c` added; `id` and `a` unchanged produce no actions.
        assert out.action.column_actions == {
            "field:b": "delete",
            "field:c": "insert",
        }
        assert out.child_invalidation == "lossy"


# =============================================================================
# Integration tests (temporary embedded database file, no server needed)
# =============================================================================


@pytest.fixture
def ladybug_conn() -> Iterator[Any]:
    """Create a temporary LadybugDB database."""
    with tempfile.TemporaryDirectory() as tmpdir:
        managed_conn = lbg.connect(str(Path(tmpdir) / "test.lbdb"))
        yield managed_conn
        managed_conn.close()


def _read_all(
    conn: Any, cypher: str, params: dict[str, Any] | None = None
) -> list[Any]:
    """Run a read query on a fresh connection and return all rows as lists."""
    reader = ladybug.Connection(conn.database)
    try:
        result = reader.execute(cypher, params or {})
        if isinstance(result, list):
            assert len(result) == 1
            result = result[0]
        return [list(row) for row in result.get_all()]
    finally:
        reader.close()


def _make_env(managed_conn: Any, env_name: str) -> coco.Environment:
    from cocoindex._internal.context_keys import ContextProvider

    ctx = ContextProvider()
    ctx.provide(LADYBUG_DB, managed_conn)
    settings = coco.Settings.from_env(
        db_path=common.get_env_db_path(f"connectors__test_ladybug_target__{env_name}")
    )
    return coco.Environment(settings, context_provider=ctx)


@dataclass
class SimpleRow:
    id: str
    name: str
    value: int


@requires_ladybug
def test_insert_update_delete(ladybug_conn: Any) -> None:
    """Basic CRUD: insert rows, update them, delete them."""
    source_rows: list[SimpleRow] = []
    coco_env = _make_env(ladybug_conn, "crud")

    async def declare_target() -> None:
        table = await coco.use_mount(
            coco.component_subpath("setup", "table"),
            lbg.mount_node_target,
            LADYBUG_DB,
            "Simple",
            await lbg.TableSchema.from_class(SimpleRow, primary_key="id"),
        )
        for row in source_rows:
            table.declare_row(row=row)

    app = coco.App(
        coco.AppConfig(name="test_ladybug_crud", environment=coco_env),
        declare_target,
    )

    # Insert
    source_rows.append(SimpleRow(id="1", name="Alice", value=10))
    source_rows.append(SimpleRow(id="2", name="Bob", value=20))
    app.update_blocking()
    assert _read_all(
        ladybug_conn, "MATCH (n:Simple) RETURN n.id, n.name, n.value ORDER BY n.id"
    ) == [["1", "Alice", 10], ["2", "Bob", 20]]

    # Update
    source_rows[0] = SimpleRow(id="1", name="Alice Updated", value=11)
    app.update_blocking()
    assert _read_all(
        ladybug_conn, "MATCH (n:Simple) RETURN n.id, n.name, n.value ORDER BY n.id"
    ) == [["1", "Alice Updated", 11], ["2", "Bob", 20]]

    # Delete
    source_rows.pop(0)
    app.update_blocking()
    assert _read_all(
        ladybug_conn, "MATCH (n:Simple) RETURN n.id, n.name, n.value ORDER BY n.id"
    ) == [["2", "Bob", 20]]

    # No-op update: same data must not fail or duplicate.
    app.update_blocking()
    assert _read_all(ladybug_conn, "MATCH (n:Simple) RETURN count(*)") == [[1]]


@requires_ladybug
def test_nullable_and_type_roundtrip(ladybug_conn: Any) -> None:
    """Nullable columns round-trip through None, and core types survive."""

    @dataclass
    class TypedRow:
        id: str
        flag: bool | None
        count: int | None
        score: float | None
        stamp: datetime.datetime | None
        uid: uuid_mod.UUID | None

    async def declare_target(rows: list[TypedRow]) -> None:
        table = await coco.use_mount(
            coco.component_subpath("setup", "typed"),
            lbg.mount_node_target,
            LADYBUG_DB,
            "Typed",
            await lbg.TableSchema.from_class(TypedRow, primary_key="id"),
        )
        for row in rows:
            table.declare_row(row=row)

    uid = uuid_mod.uuid4()
    stamp = datetime.datetime(2024, 5, 6, 7, 8, 9)
    rows = [
        TypedRow(id="full", flag=True, count=7, score=1.5, stamp=stamp, uid=uid),
        TypedRow(id="nulls", flag=None, count=None, score=None, stamp=None, uid=None),
    ]
    coco_env = _make_env(ladybug_conn, "types")

    async def declare() -> None:
        await declare_target(rows)

    app = coco.App(
        coco.AppConfig(name="test_ladybug_types", environment=coco_env), declare
    )
    app.update_blocking()

    got = {
        row[0]: row
        for row in _read_all(
            ladybug_conn,
            "MATCH (n:Typed) RETURN n.id, n.flag, n.count, n.score, n.stamp, n.uid",
        )
    }
    assert got["full"] == ["full", True, 7, 1.5, stamp, uid]
    assert got["nulls"] == ["nulls", None, None, None, None, None]

    # Flip nulls to values and back: SET must overwrite, not merge-shallow.
    rows[1] = TypedRow(id="nulls", flag=False, count=1, score=2.5, stamp=stamp, uid=uid)
    app.update_blocking()
    got = {
        row[0]: row
        for row in _read_all(
            ladybug_conn,
            "MATCH (n:Typed) RETURN n.id, n.flag, n.count, n.score, n.stamp, n.uid",
        )
    }
    assert got["nulls"] == ["nulls", False, 1, 2.5, stamp, uid]


@requires_ladybug
def test_relationship_crud(ladybug_conn: Any) -> None:
    """Nodes plus edges: endpoints are MERGEd, edges track their own table."""

    @dataclass
    class Person:
        value: str
        name: str

    @dataclass
    class Knows:
        id: str
        since: int

    edges: list[tuple[str, str, Knows | None]] = []
    coco_env = _make_env(ladybug_conn, "rel")

    async def declare_target() -> None:
        person = await coco.use_mount(
            coco.component_subpath("setup", "person"),
            lbg.mount_node_target,
            LADYBUG_DB,
            "Person",
            await lbg.TableSchema.from_class(Person, primary_key="value"),
            primary_key="value",
        )
        knows = await coco.use_mount(
            coco.component_subpath("setup", "knows"),
            lbg.mount_relationship_target,
            LADYBUG_DB,
            "Knows",
            person,
            person,
            await lbg.TableSchema.from_class(Knows, primary_key="id"),
        )
        person.declare_row(row=Person(value="a", name="Alice"))
        person.declare_row(row=Person(value="b", name="Bob"))
        for from_id, to_id, record in edges:
            knows.declare_relationship(from_id=from_id, to_id=to_id, record=record)

    app = coco.App(
        coco.AppConfig(name="test_ladybug_rel", environment=coco_env), declare_target
    )

    edges.append(("a", "b", Knows(id="e1", since=2020)))
    app.update_blocking()
    assert _read_all(
        ladybug_conn,
        "MATCH (s:Person)-[r:Knows]->(t:Person) RETURN s.value, t.value, r.since",
    ) == [["a", "b", 2020]]
    # Endpoint props are owned by the node table, not the edge write.
    assert _read_all(
        ladybug_conn, "MATCH (p:Person) RETURN p.value, p.name ORDER BY p.value"
    ) == [["a", "Alice"], ["b", "Bob"]]

    # Move the edge to new endpoints under the same id: the stale edge goes.
    edges.clear()
    edges.append(("b", "a", Knows(id="e1", since=2021)))
    app.update_blocking()
    assert _read_all(
        ladybug_conn,
        "MATCH (s:Person)-[r:Knows]->(t:Person) RETURN s.value, t.value, r.since",
    ) == [["b", "a", 2021]]

    # Drop the edge; endpoints stay.
    edges.clear()
    app.update_blocking()
    assert _read_all(ladybug_conn, "MATCH ()-[r:Knows]->() RETURN count(*)") == [[0]]
    assert _read_all(ladybug_conn, "MATCH (p:Person) RETURN count(*)") == [[2]]


@requires_ladybug
def test_schema_evolution_add_column(ladybug_conn: Any) -> None:
    """Adding a column evolves the table in place; existing rows survive."""

    @dataclass
    class RowV1:
        id: str
        name: str

    @dataclass
    class RowV2:
        id: str
        name: str
        extra: str | None = None

    phase: list[str] = ["v1"]
    coco_env = _make_env(ladybug_conn, "evolve")

    async def declare_target() -> None:
        schema: lbg.TableSchema[Any]
        if phase[0] == "v1":
            schema = await lbg.TableSchema.from_class(RowV1, primary_key="id")
        else:
            schema = await lbg.TableSchema.from_class(RowV2, primary_key="id")
        table = await coco.use_mount(
            coco.component_subpath("setup", "evolving"),
            lbg.mount_node_target,
            LADYBUG_DB,
            "Evolving",
            schema,
        )
        if phase[0] == "v1":
            table.declare_row(row=RowV1(id="1", name="one"))
        else:
            table.declare_row(row=RowV2(id="1", name="one", extra="new"))
            table.declare_row(row=RowV2(id="2", name="two", extra=None))

    app = coco.App(
        coco.AppConfig(name="test_ladybug_evolve", environment=coco_env), declare_target
    )
    app.update_blocking()
    assert _read_all(ladybug_conn, "MATCH (n:Evolving) RETURN n.id, n.name") == [
        ["1", "one"]
    ]

    phase[0] = "v2"
    app.update_blocking()
    assert _read_all(
        ladybug_conn, "MATCH (n:Evolving) RETURN n.id, n.name, n.extra ORDER BY n.id"
    ) == [["1", "one", "new"], ["2", "two", None]]


@requires_ladybug
def test_drop_cleans_up(ladybug_conn: Any) -> None:
    """When the target is no longer declared, its tables and rows go away."""
    declare: list[bool] = [True]
    coco_env = _make_env(ladybug_conn, "drop")

    async def declare_target() -> None:
        if not declare[0]:
            return
        table = await coco.use_mount(
            coco.component_subpath("setup", "ephemeral"),
            lbg.mount_node_target,
            LADYBUG_DB,
            "Ephemeral",
            await lbg.TableSchema.from_class(SimpleRow, primary_key="id"),
        )
        table.declare_row(row=SimpleRow(id="1", name="x", value=1))

    app = coco.App(
        coco.AppConfig(name="test_ladybug_drop", environment=coco_env), declare_target
    )
    app.update_blocking()
    assert _read_all(ladybug_conn, "MATCH (n:Ephemeral) RETURN count(*)") == [[1]]

    declare[0] = False
    app.update_blocking()
    assert _read_all(ladybug_conn, "CALL show_tables() RETURN *") == []


@requires_ladybug
def test_vector_column_storage(ladybug_conn: Any) -> None:
    """Embeddings live in fixed-size DOUBLE columns and stay comparable."""

    @dataclass
    class DocRow:
        filename: str
        embedding: Annotated[
            NDArray[np.float32], VectorSchema(size=3, dtype=np.float32)
        ]

    coco_env = _make_env(ladybug_conn, "vector")

    async def declare_target() -> None:
        table = await coco.use_mount(
            coco.component_subpath("setup", "docs"),
            lbg.mount_node_target,
            LADYBUG_DB,
            "Doc",
            await lbg.TableSchema.from_class(DocRow, primary_key="filename"),
            primary_key="filename",
        )
        table.declare_row(row=DocRow(filename="a", embedding=np.array([1.0, 0.0, 0.0])))

    app = coco.App(
        coco.AppConfig(name="test_ladybug_vector", environment=coco_env), declare_target
    )
    app.update_blocking()
    got = _read_all(
        ladybug_conn,
        "MATCH (d:Doc) RETURN d.filename, array_cosine_similarity(d.embedding, [1.0, 0.0, 0.0])",
    )
    assert len(got) == 1
    assert got[0][0] == "a"
    assert got[0][1] == pytest.approx(1.0)

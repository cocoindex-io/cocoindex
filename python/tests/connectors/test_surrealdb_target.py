"""Tests for SurrealDB target connector."""

from __future__ import annotations

import os
import uuid as uuid_mod
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Annotated, Any, Literal

import numpy as np
import pytest
import pytest_asyncio
from numpy.typing import NDArray

import cocoindex as coco
from cocoindex.resources.schema import VectorSchema

from tests import common

coco_env = common.create_test_env(__file__)


# =============================================================================
# Check for surrealdb availability
# =============================================================================

try:
    from surrealdb import AsyncSurreal  # type: ignore[import-untyped]

    HAS_SURREALDB = True
except ImportError:
    HAS_SURREALDB = False

requires_surrealdb = pytest.mark.skipif(
    not HAS_SURREALDB, reason="surrealdb is not installed"
)

# Lazy import — only used inside tests guarded by requires_surrealdb.
# The module itself always imports (guarded import is inside _target.py).
if HAS_SURREALDB:
    from cocoindex.connectors import surrealdb  # type: ignore[attr-defined]
    from cocoindex.connectors.surrealdb._target import (  # type: ignore[import-untyped]
        _format_record_id,
        _validate_identifier,
    )


# =============================================================================
# Config from environment
# =============================================================================

_SURREALDB_URL = os.environ.get("SURREALDB_URL", "ws://localhost:8000/rpc")
_SURREALDB_USER = os.environ.get("SURREALDB_USER", "")
_SURREALDB_PASS = os.environ.get("SURREALDB_PASS", "")
if HAS_SURREALDB:
    SURREAL_DB_KEY: coco.ContextKey[Any] = coco.ContextKey("test_surrealdb_target_db")


# =============================================================================
# Unit tests — identifier validation & record ID formatting (no DB needed)
# =============================================================================


@requires_surrealdb
class TestValidateIdentifier:
    """Tests for _validate_identifier()."""

    @pytest.mark.parametrize("name", ["users", "_private", "T1", "a_b_c", "X"])
    def test_valid_identifiers(self, name: str) -> None:
        _validate_identifier(name, "test")  # should not raise

    @pytest.mark.parametrize(
        "name",
        ["my-table", "123abc", "", "has space", "ba`ck", "semi;colon", "a.b"],
    )
    def test_invalid_identifiers(self, name: str) -> None:
        with pytest.raises(ValueError, match="Invalid SurrealDB"):
            _validate_identifier(name, "test")


@requires_surrealdb
class TestFormatRecordId:
    """Tests for _format_record_id()."""

    def test_string_simple(self) -> None:
        assert _format_record_id("alice") == "`alice`"

    def test_string_with_backtick(self) -> None:
        assert _format_record_id("has`tick") == r"`has\`tick`"

    def test_string_with_backslash(self) -> None:
        assert _format_record_id(r"back\slash") == r"`back\\slash`"

    def test_int(self) -> None:
        assert _format_record_id(42) == "42"

    def test_float(self) -> None:
        assert _format_record_id(3.14) == "3.14"

    def test_string_numeric_stays_quoted(self) -> None:
        # string "123" must remain distinct from int 123
        assert _format_record_id("123") == "`123`"

    def test_string_empty(self) -> None:
        assert _format_record_id("") == "``"


@requires_surrealdb
class TestValidateIdentifierAtApiEntryPoints:
    """Ensure validation fires at public API entry points."""

    def test_table_schema_invalid_column(self) -> None:
        with pytest.raises(ValueError, match="column name"):
            surrealdb.TableSchema(
                columns={"bad-name": surrealdb.ColumnDef(type="string")}
            )

    def test_table_target_invalid_name(self) -> None:
        with pytest.raises(ValueError, match="table name"):
            surrealdb.table_target(SURREAL_DB_KEY, "bad-table")

    def test_relation_target_invalid_name(self) -> None:
        with pytest.raises(ValueError, match="relation table name"):
            surrealdb.relation_target(SURREAL_DB_KEY, "bad-rel", [], [])


# =============================================================================
# Test utilities
# =============================================================================


async def _create_conn(ns: str, db: str) -> Any:
    """Create and configure a SurrealDB connection for testing."""
    conn = AsyncSurreal(_SURREALDB_URL)
    await conn.connect()  # type: ignore[call-arg]
    if _SURREALDB_USER:
        await conn.signin({"username": _SURREALDB_USER, "password": _SURREALDB_PASS})
    await conn.use(ns, db)
    return conn


async def _query(conn: Any, sql: str) -> Any:
    """Execute a SurrealQL query and return the result."""
    return await conn.query(sql)


async def _query_table(conn: Any, table_name: str) -> list[dict[str, Any]]:
    """Read all records from a table."""
    result = await conn.query(f"SELECT * FROM {table_name}")
    if isinstance(result, list):
        return result  # type: ignore[no-any-return]
    return []


async def _table_exists(conn: Any, table_name: str) -> bool:
    """Check whether a table exists via INFO FOR DB."""
    result = await conn.query("INFO FOR DB")
    if isinstance(result, dict):
        tables = result.get("tables", {})
        if isinstance(tables, dict):
            return table_name in tables
    return False


async def _cleanup_conn(conn: Any, ns: str, db: str) -> None:
    """Drop the test database and close the connection."""
    try:
        await conn.query(f"REMOVE DATABASE {db}")
    except Exception:
        pass
    try:
        await conn.query(f"REMOVE NAMESPACE {ns}")
    except Exception:
        pass
    try:
        await conn.close()
    except Exception:
        pass


# =============================================================================
# Test fixtures
# =============================================================================


@pytest_asyncio.fixture
async def surreal_conn() -> AsyncIterator[tuple[Any, str, str]]:
    """Create a SurrealDB connection with a unique test namespace/database."""
    tag = uuid_mod.uuid4().hex[:8]
    ns = f"test_ns_{tag}"
    db = f"test_db_{tag}"
    conn = await _create_conn(ns, db)
    yield conn, ns, db
    await _cleanup_conn(conn, ns, db)


# =============================================================================
# Row types for testing
# =============================================================================


@dataclass
class SimpleRow:
    id: str
    name: str
    value: int


@dataclass
class ExtendedRow:
    id: str
    name: str
    value: int
    extra: str


@dataclass
class RowV1:
    id: str
    name: str


@dataclass
class RowV2:
    id: str
    name: str
    email: str


@dataclass
class RowWithExtra:
    id: str
    name: str
    extra: int


@dataclass
class RowIntId:
    id: int
    name: str


@dataclass
class Person:
    id: str
    name: str


@dataclass
class Post:
    id: str
    title: str


@dataclass
class Org:
    id: str
    org_name: str


@dataclass
class Likes:
    id: str
    rating: int


@dataclass
class LikeData:
    """Relation data without id — id auto-derived from endpoints."""

    rating: int


@dataclass
class VecRow:
    id: str
    content: str
    embedding: Annotated[
        NDArray[np.float32], VectorSchema(dtype=np.dtype(np.float32), size=4)
    ]


@dataclass
class TypesRow:
    id: str
    flag: bool
    count: int
    score: float
    label: str
    created: str  # We'll test string round-trip; datetime needs SDK support


# =============================================================================
# Global state for test app functions
# =============================================================================

_source_rows: list[Any] = []
_row_type: type = SimpleRow
_table_name: str = "test_table"
_table_schema: Any = None  # Will be set per test; None means schemaless
_managed_by: str = "system"


# =============================================================================
# App functions
# =============================================================================


async def declare_table_and_rows() -> None:
    """Declare table and rows from global source data."""
    if _table_schema is not None:
        schema = await surrealdb.TableSchema.from_class(_row_type)
    else:
        schema = None

    table: Any = await coco.use_mount(  # type: ignore[call-overload]
        coco.component_subpath("setup", "table"),
        surrealdb.mount_table_target,  # type: ignore[arg-type]
        SURREAL_DB_KEY,
        _table_name,
        schema,
        managed_by=_managed_by,
    )

    for row in _source_rows:
        table.declare_record(row=row)


async def declare_schemaless_rows() -> None:
    """Declare schemaless table with dict rows."""
    table = await coco.use_mount(  # type: ignore[call-overload]
        coco.component_subpath("setup", "table"),
        surrealdb.mount_table_target,
        SURREAL_DB_KEY,
        _table_name,
        None,  # No schema = SCHEMALESS
    )

    for row in _source_rows:
        table.declare_record(row=row)


async def declare_nothing() -> None:
    """Declare nothing — used to test table cleanup."""
    pass


# =============================================================================
# Normal table tests
# =============================================================================


@requires_surrealdb
@pytest.mark.asyncio
async def test_create_table_schemafull_and_insert(
    surreal_conn: tuple[Any, str, str],
) -> None:
    """Test creating a SCHEMAFULL table and inserting records."""
    conn, ns, db = surreal_conn
    global _source_rows, _row_type, _table_name, _table_schema, _managed_by

    _source_rows = []
    _row_type = SimpleRow
    _table_name = "test_schemafull"
    _table_schema = True  # Signals to use from_class
    _managed_by = "system"

    coco_env.context_provider.provide(
        SURREAL_DB_KEY,
        surrealdb.ConnectionFactory(
            url=_SURREALDB_URL,
            namespace=ns,
            database=db,
            credentials={"username": _SURREALDB_USER, "password": _SURREALDB_PASS}
            if _SURREALDB_USER
            else None,
        ),
    )
    app = coco.App(
        coco.AppConfig(name="test_schemafull_insert", environment=coco_env),
        declare_table_and_rows,
    )

    # Insert initial data
    _source_rows = [
        SimpleRow(id="1", name="Alice", value=100),
        SimpleRow(id="2", name="Bob", value=200),
    ]
    await app.update()

    data = await _query_table(conn, _table_name)
    assert len(data) == 2
    names = {r["name"] for r in data}
    assert names == {"Alice", "Bob"}

    # Add a third record
    _source_rows.append(SimpleRow(id="3", name="Charlie", value=300))
    await app.update()

    data = await _query_table(conn, _table_name)
    assert len(data) == 3


@requires_surrealdb
@pytest.mark.asyncio
async def test_create_table_schemaless_and_insert(
    surreal_conn: tuple[Any, str, str],
) -> None:
    """Test creating a SCHEMALESS table and inserting dict records."""
    conn, ns, db = surreal_conn
    global _source_rows, _table_name, _table_schema, _managed_by

    _table_name = "test_schemaless"
    _table_schema = None
    _managed_by = "system"

    coco_env.context_provider.provide(
        SURREAL_DB_KEY,
        surrealdb.ConnectionFactory(
            url=_SURREALDB_URL,
            namespace=ns,
            database=db,
            credentials={"username": _SURREALDB_USER, "password": _SURREALDB_PASS}
            if _SURREALDB_USER
            else None,
        ),
    )
    app = coco.App(
        coco.AppConfig(name="test_schemaless_insert", environment=coco_env),
        declare_schemaless_rows,
    )

    _source_rows = [{"id": "1", "name": "Alice"}]
    await app.update()

    data = await _query_table(conn, _table_name)
    assert len(data) == 1
    assert data[0]["name"] == "Alice"

    # Insert a record with different fields
    _source_rows.append({"id": "2", "name": "Bob", "extra": 42})
    await app.update()

    data = await _query_table(conn, _table_name)
    assert len(data) == 2


@requires_surrealdb
@pytest.mark.asyncio
async def test_update_record(
    surreal_conn: tuple[Any, str, str],
) -> None:
    """Test updating an existing record."""
    conn, ns, db = surreal_conn
    global _source_rows, _row_type, _table_name, _table_schema, _managed_by

    _source_rows = []
    _row_type = SimpleRow
    _table_name = "test_update"
    _table_schema = True
    _managed_by = "system"

    coco_env.context_provider.provide(
        SURREAL_DB_KEY,
        surrealdb.ConnectionFactory(
            url=_SURREALDB_URL,
            namespace=ns,
            database=db,
            credentials={"username": _SURREALDB_USER, "password": _SURREALDB_PASS}
            if _SURREALDB_USER
            else None,
        ),
    )

    app = coco.App(
        coco.AppConfig(name="test_update_record", environment=coco_env),
        declare_table_and_rows,
    )

    _source_rows = [SimpleRow(id="1", name="Alice", value=100)]
    await app.update()

    # Update
    _source_rows = [SimpleRow(id="1", name="Alice Updated", value=200)]
    await app.update()

    data = await _query_table(conn, _table_name)
    assert len(data) == 1
    assert data[0]["name"] == "Alice Updated"
    assert data[0]["value"] == 200


@requires_surrealdb
@pytest.mark.asyncio
async def test_delete_record(
    surreal_conn: tuple[Any, str, str],
) -> None:
    """Test deleting a record when it's no longer declared."""
    conn, ns, db = surreal_conn
    global _source_rows, _row_type, _table_name, _table_schema, _managed_by

    _source_rows = []
    _row_type = SimpleRow
    _table_name = "test_delete"
    _table_schema = True
    _managed_by = "system"

    coco_env.context_provider.provide(
        SURREAL_DB_KEY,
        surrealdb.ConnectionFactory(
            url=_SURREALDB_URL,
            namespace=ns,
            database=db,
            credentials={"username": _SURREALDB_USER, "password": _SURREALDB_PASS}
            if _SURREALDB_USER
            else None,
        ),
    )

    app = coco.App(
        coco.AppConfig(name="test_delete_record", environment=coco_env),
        declare_table_and_rows,
    )

    _source_rows = [
        SimpleRow(id="1", name="Alice", value=100),
        SimpleRow(id="2", name="Bob", value=200),
    ]
    await app.update()

    # Remove one
    _source_rows = [SimpleRow(id="1", name="Alice", value=100)]
    await app.update()

    data = await _query_table(conn, _table_name)
    assert len(data) == 1
    assert data[0]["name"] == "Alice"


@requires_surrealdb
@pytest.mark.asyncio
async def test_no_op_when_unchanged(
    surreal_conn: tuple[Any, str, str],
) -> None:
    """Test that no errors occur when data is unchanged."""
    conn, ns, db = surreal_conn
    global _source_rows, _row_type, _table_name, _table_schema, _managed_by

    _source_rows = []
    _row_type = SimpleRow
    _table_name = "test_noop"
    _table_schema = True
    _managed_by = "system"

    coco_env.context_provider.provide(
        SURREAL_DB_KEY,
        surrealdb.ConnectionFactory(
            url=_SURREALDB_URL,
            namespace=ns,
            database=db,
            credentials={"username": _SURREALDB_USER, "password": _SURREALDB_PASS}
            if _SURREALDB_USER
            else None,
        ),
    )

    app = coco.App(
        coco.AppConfig(name="test_no_op", environment=coco_env),
        declare_table_and_rows,
    )

    _source_rows = [SimpleRow(id="1", name="Alice", value=100)]
    await app.update()

    # Same data, second update
    await app.update()

    data = await _query_table(conn, _table_name)
    assert len(data) == 1
    assert data[0]["name"] == "Alice"


@requires_surrealdb
@pytest.mark.asyncio
async def test_drop_table_on_removal(
    surreal_conn: tuple[Any, str, str],
) -> None:
    """Test that a table is removed when no longer declared."""
    conn, ns, db = surreal_conn
    global _source_rows, _row_type, _table_name, _table_schema, _managed_by

    _source_rows = []
    _row_type = SimpleRow
    _table_name = "test_drop"
    _table_schema = True
    _managed_by = "system"

    coco_env.context_provider.provide(
        SURREAL_DB_KEY,
        surrealdb.ConnectionFactory(
            url=_SURREALDB_URL,
            namespace=ns,
            database=db,
            credentials={"username": _SURREALDB_USER, "password": _SURREALDB_PASS}
            if _SURREALDB_USER
            else None,
        ),
    )

    app_with_table = coco.App(
        coco.AppConfig(name="test_drop_table", environment=coco_env),
        declare_table_and_rows,
    )

    _source_rows = [SimpleRow(id="1", name="Alice", value=100)]
    await app_with_table.update()

    assert await _table_exists(conn, _table_name)

    # Drop the app — this should revert all target states
    await app_with_table.drop()

    assert not await _table_exists(conn, _table_name)


# =============================================================================
# Schema evolution tests
# =============================================================================


@requires_surrealdb
@pytest.mark.asyncio
async def test_schema_evolution_add_field(
    surreal_conn: tuple[Any, str, str],
) -> None:
    """Test adding a new field to an existing table."""
    conn, ns, db = surreal_conn
    global _source_rows, _row_type, _table_name, _table_schema, _managed_by

    _table_name = "test_evolve_add"
    _table_schema = True
    _managed_by = "system"

    coco_env.context_provider.provide(
        SURREAL_DB_KEY,
        surrealdb.ConnectionFactory(
            url=_SURREALDB_URL,
            namespace=ns,
            database=db,
            credentials={"username": _SURREALDB_USER, "password": _SURREALDB_PASS}
            if _SURREALDB_USER
            else None,
        ),
    )

    # RowV1 (id, name)
    _row_type = RowV1
    _source_rows = [RowV1(id="1", name="Alice")]

    app = coco.App(
        coco.AppConfig(name="test_evolve_add", environment=coco_env),
        declare_table_and_rows,
    )
    await app.update()

    # RowV2 (id, name, email)
    _row_type = RowV2
    _source_rows = [RowV2(id="1", name="Alice", email="alice@example.com")]
    await app.update()

    data = await _query_table(conn, _table_name)
    assert len(data) == 1
    assert data[0].get("email") == "alice@example.com"


@requires_surrealdb
@pytest.mark.asyncio
async def test_schema_evolution_remove_field(
    surreal_conn: tuple[Any, str, str],
) -> None:
    """Test removing a field from an existing table."""
    conn, ns, db = surreal_conn
    global _source_rows, _row_type, _table_name, _table_schema, _managed_by

    _table_name = "test_evolve_remove"
    _table_schema = True
    _managed_by = "system"

    coco_env.context_provider.provide(
        SURREAL_DB_KEY,
        surrealdb.ConnectionFactory(
            url=_SURREALDB_URL,
            namespace=ns,
            database=db,
            credentials={"username": _SURREALDB_USER, "password": _SURREALDB_PASS}
            if _SURREALDB_USER
            else None,
        ),
    )

    # RowWithExtra (id, name, extra)
    _row_type = RowWithExtra
    _source_rows = [RowWithExtra(id="1", name="Alice", extra=42)]

    app = coco.App(
        coco.AppConfig(name="test_evolve_remove", environment=coco_env),
        declare_table_and_rows,
    )
    await app.update()

    # RowV1 (id, name) — 'extra' removed
    _row_type = RowV1
    _source_rows = [RowV1(id="1", name="Alice")]
    await app.update()

    data = await _query_table(conn, _table_name)
    assert len(data) == 1
    assert "extra" not in data[0] or data[0].get("extra") is None


@requires_surrealdb
@pytest.mark.asyncio
async def test_schema_evolution_change_id_type(
    surreal_conn: tuple[Any, str, str],
) -> None:
    """Test changing id type triggers destructive table recreation."""
    conn, ns, db = surreal_conn
    global _source_rows, _row_type, _table_name, _table_schema, _managed_by

    _table_name = "test_evolve_id"
    _table_schema = True
    _managed_by = "system"

    coco_env.context_provider.provide(
        SURREAL_DB_KEY,
        surrealdb.ConnectionFactory(
            url=_SURREALDB_URL,
            namespace=ns,
            database=db,
            credentials={"username": _SURREALDB_USER, "password": _SURREALDB_PASS}
            if _SURREALDB_USER
            else None,
        ),
    )

    # int id
    _row_type = RowIntId
    _source_rows = [RowIntId(id=1, name="Alice")]

    app = coco.App(
        coco.AppConfig(name="test_evolve_id", environment=coco_env),
        declare_table_and_rows,
    )
    await app.update()

    # string id — destructive change
    _row_type = RowV1
    _source_rows = [RowV1(id="new1", name="Bob")]
    await app.update()

    data = await _query_table(conn, _table_name)
    assert len(data) == 1
    assert data[0]["name"] == "Bob"


# =============================================================================
# Relation tests
# =============================================================================


@requires_surrealdb
@pytest.mark.asyncio
async def test_relation_basic(
    surreal_conn: tuple[Any, str, str],
) -> None:
    """Test basic relation table creation and record insertion."""
    conn, ns, db = surreal_conn

    coco_env.context_provider.provide(
        SURREAL_DB_KEY,
        surrealdb.ConnectionFactory(
            url=_SURREALDB_URL,
            namespace=ns,
            database=db,
            credentials={"username": _SURREALDB_USER, "password": _SURREALDB_PASS}
            if _SURREALDB_USER
            else None,
        ),
    )
    person_rows: list[Person] = []
    post_rows: list[Post] = []
    relation_rows: list[dict[str, Any]] = []

    async def declare_with_relations() -> None:
        person_schema = await surrealdb.TableSchema.from_class(Person)
        person_target = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "person"),
            surrealdb.mount_table_target,
            SURREAL_DB_KEY,
            "person",
            person_schema,
        )
        for r in person_rows:
            person_target.declare_record(row=r)

        post_schema = await surrealdb.TableSchema.from_class(Post)
        post_target = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "post"),
            surrealdb.mount_table_target,
            SURREAL_DB_KEY,
            "post",
            post_schema,
        )
        for r in post_rows:  # type: ignore[assignment]
            post_target.declare_record(row=r)  # type: ignore[arg-type]

        likes_schema = await surrealdb.TableSchema.from_class(Likes)
        likes_target = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "likes"),
            surrealdb.mount_relation_target,
            SURREAL_DB_KEY,
            "likes",
            person_target,
            post_target,
            likes_schema,
        )
        for rel in relation_rows:
            likes_target.declare_relation(
                from_id=rel["from_id"],
                to_id=rel["to_id"],
                record=Likes(id=rel["id"], rating=rel["rating"]),
            )

    app = coco.App(
        coco.AppConfig(name="test_rel_basic", environment=coco_env),
        declare_with_relations,
    )

    person_rows = [Person(id="alice", name="Alice")]
    post_rows = [Post(id="post1", title="Hello World")]
    relation_rows = [{"id": "like1", "from_id": "alice", "to_id": "post1", "rating": 5}]
    await app.update()

    # Verify relation exists
    data = await _query_table(conn, "likes")
    assert len(data) == 1
    assert data[0]["rating"] == 5


@requires_surrealdb
@pytest.mark.asyncio
async def test_relation_delete(
    surreal_conn: tuple[Any, str, str],
) -> None:
    """Test deleting relation records."""
    conn, ns, db = surreal_conn

    coco_env.context_provider.provide(
        SURREAL_DB_KEY,
        surrealdb.ConnectionFactory(
            url=_SURREALDB_URL,
            namespace=ns,
            database=db,
            credentials={"username": _SURREALDB_USER, "password": _SURREALDB_PASS}
            if _SURREALDB_USER
            else None,
        ),
    )
    person_rows: list[Person] = []
    post_rows: list[Post] = []
    relation_rows: list[dict[str, Any]] = []

    async def declare_with_relations() -> None:
        person_schema = await surrealdb.TableSchema.from_class(Person)
        person_target = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "person"),
            surrealdb.mount_table_target,
            SURREAL_DB_KEY,
            "person",
            person_schema,
        )
        for r in person_rows:
            person_target.declare_record(row=r)

        post_schema = await surrealdb.TableSchema.from_class(Post)
        post_target = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "post"),
            surrealdb.mount_table_target,
            SURREAL_DB_KEY,
            "post",
            post_schema,
        )
        for r in post_rows:  # type: ignore[assignment]
            post_target.declare_record(row=r)  # type: ignore[arg-type]

        likes_schema = await surrealdb.TableSchema.from_class(Likes)
        likes_target = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "likes"),
            surrealdb.mount_relation_target,
            SURREAL_DB_KEY,
            "likes",
            person_target,
            post_target,
            likes_schema,
        )
        for rel in relation_rows:
            likes_target.declare_relation(
                from_id=rel["from_id"],
                to_id=rel["to_id"],
                record=Likes(id=rel["id"], rating=rel["rating"]),
            )

    app = coco.App(
        coco.AppConfig(name="test_rel_delete", environment=coco_env),
        declare_with_relations,
    )

    person_rows = [Person(id="alice", name="Alice")]
    post_rows = [
        Post(id="post1", title="Hello"),
        Post(id="post2", title="World"),
    ]
    relation_rows = [
        {"id": "like1", "from_id": "alice", "to_id": "post1", "rating": 5},
        {"id": "like2", "from_id": "alice", "to_id": "post2", "rating": 3},
    ]
    await app.update()
    assert len(await _query_table(conn, "likes")) == 2

    # Remove one relation
    relation_rows = [
        {"id": "like1", "from_id": "alice", "to_id": "post1", "rating": 5},
    ]
    await app.update()
    assert len(await _query_table(conn, "likes")) == 1


@requires_surrealdb
@pytest.mark.asyncio
async def test_relation_without_schema(
    surreal_conn: tuple[Any, str, str],
) -> None:
    """Test schemaless relation table."""
    conn, ns, db = surreal_conn

    coco_env.context_provider.provide(
        SURREAL_DB_KEY,
        surrealdb.ConnectionFactory(
            url=_SURREALDB_URL,
            namespace=ns,
            database=db,
            credentials={"username": _SURREALDB_USER, "password": _SURREALDB_PASS}
            if _SURREALDB_USER
            else None,
        ),
    )
    person_rows: list[Person] = []
    post_rows: list[Post] = []
    relation_rows: list[dict[str, Any]] = []

    async def declare_schemaless_relation() -> None:
        person_schema = await surrealdb.TableSchema.from_class(Person)
        person_target = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "person"),
            surrealdb.mount_table_target,
            SURREAL_DB_KEY,
            "person",
            person_schema,
        )
        for r in person_rows:
            person_target.declare_record(row=r)

        post_schema = await surrealdb.TableSchema.from_class(Post)
        post_target = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "post"),
            surrealdb.mount_table_target,
            SURREAL_DB_KEY,
            "post",
            post_schema,
        )
        for r in post_rows:  # type: ignore[assignment]
            post_target.declare_record(row=r)  # type: ignore[arg-type]

        # No schema for the relation table
        likes_target = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "likes"),
            surrealdb.mount_relation_target,
            SURREAL_DB_KEY,
            "likes",
            person_target,
            post_target,
            None,  # Schemaless
        )
        for rel in relation_rows:
            likes_target.declare_relation(
                from_id=rel["from_id"],
                to_id=rel["to_id"],
                record=rel.get("record"),
            )

    app = coco.App(
        coco.AppConfig(name="test_rel_schemaless", environment=coco_env),
        declare_schemaless_relation,
    )

    person_rows = [Person(id="alice", name="Alice")]
    post_rows = [Post(id="post1", title="Hello")]
    relation_rows = [
        {
            "from_id": "alice",
            "to_id": "post1",
            "record": {"mood": "happy"},
        }
    ]
    await app.update()

    data = await _query_table(conn, "likes")
    assert len(data) == 1
    assert data[0].get("mood") == "happy"


@requires_surrealdb
@pytest.mark.asyncio
async def test_relation_polymorphic(
    surreal_conn: tuple[Any, str, str],
) -> None:
    """Test polymorphic relation with multiple from_table types."""
    conn, ns, db = surreal_conn

    coco_env.context_provider.provide(
        SURREAL_DB_KEY,
        surrealdb.ConnectionFactory(
            url=_SURREALDB_URL,
            namespace=ns,
            database=db,
            credentials={"username": _SURREALDB_USER, "password": _SURREALDB_PASS}
            if _SURREALDB_USER
            else None,
        ),
    )
    person_rows: list[Person] = []
    org_rows: list[Org] = []
    post_rows: list[Post] = []
    relation_rows: list[dict[str, Any]] = []

    async def declare_polymorphic() -> None:
        person_schema = await surrealdb.TableSchema.from_class(Person)
        person_target = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "person"),
            surrealdb.mount_table_target,
            SURREAL_DB_KEY,
            "person",
            person_schema,
        )
        for r in person_rows:
            person_target.declare_record(row=r)

        org_schema = await surrealdb.TableSchema.from_class(Org)
        org_target = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "org"),
            surrealdb.mount_table_target,
            SURREAL_DB_KEY,
            "org",
            org_schema,
        )
        for r in org_rows:  # type: ignore[assignment]
            org_target.declare_record(row=r)  # type: ignore[arg-type]

        post_schema = await surrealdb.TableSchema.from_class(Post)
        post_target = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "post"),
            surrealdb.mount_table_target,
            SURREAL_DB_KEY,
            "post",
            post_schema,
        )
        for r in post_rows:  # type: ignore[assignment]
            post_target.declare_record(row=r)  # type: ignore[arg-type]

        # Polymorphic: from_table can be person or org
        likes_schema = await surrealdb.TableSchema.from_class(Likes)
        likes_target = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "likes"),
            surrealdb.mount_relation_target,
            SURREAL_DB_KEY,
            "likes",
            [person_target, org_target],  # Polymorphic from
            post_target,
            likes_schema,
        )
        for rel in relation_rows:
            ft = person_target if rel["from_type"] == "person" else org_target
            likes_target.declare_relation(
                from_id=rel["from_id"],
                to_id=rel["to_id"],
                record=Likes(id=rel["id"], rating=rel["rating"]),
                from_table=ft,
            )

    app = coco.App(
        coco.AppConfig(name="test_rel_poly", environment=coco_env),
        declare_polymorphic,
    )

    person_rows = [Person(id="alice", name="Alice")]
    org_rows = [Org(id="acme", org_name="ACME Corp")]
    post_rows = [Post(id="post1", title="Hello")]
    relation_rows = [
        {
            "id": "like1",
            "from_type": "person",
            "from_id": "alice",
            "to_id": "post1",
            "rating": 5,
        },
        {
            "id": "like2",
            "from_type": "org",
            "from_id": "acme",
            "to_id": "post1",
            "rating": 4,
        },
    ]
    await app.update()

    data = await _query_table(conn, "likes")
    assert len(data) == 2


@requires_surrealdb
@pytest.mark.asyncio
async def test_relation_auto_id_with_update(
    surreal_conn: tuple[Any, str, str],
) -> None:
    """Test that changing from_id with auto-derived id correctly deletes old + creates new."""
    conn, ns, db = surreal_conn

    coco_env.context_provider.provide(
        SURREAL_DB_KEY,
        surrealdb.ConnectionFactory(
            url=_SURREALDB_URL,
            namespace=ns,
            database=db,
            credentials={"username": _SURREALDB_USER, "password": _SURREALDB_PASS}
            if _SURREALDB_USER
            else None,
        ),
    )
    person_rows: list[Person] = []
    post_rows: list[Post] = []
    from_id_value = "alice"

    async def declare_auto_id_relation() -> None:
        person_schema = await surrealdb.TableSchema.from_class(Person)
        person_target = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "person"),
            surrealdb.mount_table_target,
            SURREAL_DB_KEY,
            "person",
            person_schema,
        )
        for r in person_rows:
            person_target.declare_record(row=r)

        post_schema = await surrealdb.TableSchema.from_class(Post)
        post_target = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "post"),
            surrealdb.mount_table_target,
            SURREAL_DB_KEY,
            "post",
            post_schema,
        )
        for r in post_rows:  # type: ignore[assignment]
            post_target.declare_record(row=r)  # type: ignore[arg-type]

        likes_target = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "likes"),
            surrealdb.mount_relation_target,
            SURREAL_DB_KEY,
            "likes",
            person_target,
            post_target,
            None,
        )
        likes_target.declare_relation(
            from_id=from_id_value,
            to_id="post1",
        )

    app = coco.App(
        coco.AppConfig(name="test_auto_id_update", environment=coco_env),
        declare_auto_id_relation,
    )

    # Run 1: relation from alice -> post1
    person_rows = [
        Person(id="alice", name="Alice"),
        Person(id="bob", name="Bob"),
    ]
    post_rows = [Post(id="post1", title="Hello")]
    from_id_value = "alice"
    await app.update()

    data = await _query_table(conn, "likes")
    assert len(data) == 1

    # Run 2: change from_id to bob -> post1 (auto-id changes, old record cleaned up)
    from_id_value = "bob"
    await app.update()

    data = await _query_table(conn, "likes")
    assert len(data) == 1


@requires_surrealdb
@pytest.mark.asyncio
async def test_relation_schema_without_id(
    surreal_conn: tuple[Any, str, str],
) -> None:
    """Test SCHEMAFULL relation where the schema class has no id field."""
    conn, ns, db = surreal_conn

    coco_env.context_provider.provide(
        SURREAL_DB_KEY,
        surrealdb.ConnectionFactory(
            url=_SURREALDB_URL,
            namespace=ns,
            database=db,
            credentials={"username": _SURREALDB_USER, "password": _SURREALDB_PASS}
            if _SURREALDB_USER
            else None,
        ),
    )
    person_rows: list[Person] = []
    post_rows: list[Post] = []
    relation_rows: list[dict[str, Any]] = []

    async def declare_schema_no_id() -> None:
        person_schema = await surrealdb.TableSchema.from_class(Person)
        person_target = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "person"),
            surrealdb.mount_table_target,
            SURREAL_DB_KEY,
            "person",
            person_schema,
        )
        for r in person_rows:
            person_target.declare_record(row=r)

        post_schema = await surrealdb.TableSchema.from_class(Post)
        post_target = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "post"),
            surrealdb.mount_table_target,
            SURREAL_DB_KEY,
            "post",
            post_schema,
        )
        for r in post_rows:  # type: ignore[assignment]
            post_target.declare_record(row=r)  # type: ignore[arg-type]

        # Schema without id field
        likes_schema = await surrealdb.TableSchema.from_class(LikeData)
        likes_target = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "likes"),
            surrealdb.mount_relation_target,
            SURREAL_DB_KEY,
            "likes",
            person_target,
            post_target,
            likes_schema,
        )
        for rel in relation_rows:
            likes_target.declare_relation(
                from_id=rel["from_id"],
                to_id=rel["to_id"],
                record=LikeData(rating=rel["rating"]),
            )

    app = coco.App(
        coco.AppConfig(name="test_rel_no_id_schema", environment=coco_env),
        declare_schema_no_id,
    )

    person_rows = [Person(id="alice", name="Alice")]
    post_rows = [Post(id="post1", title="Hello")]
    relation_rows = [
        {"from_id": "alice", "to_id": "post1", "rating": 5},
    ]
    await app.update()

    data = await _query_table(conn, "likes")
    assert len(data) == 1
    assert data[0]["rating"] == 5


# =============================================================================
# Ordering and shared sink tests
# =============================================================================


@requires_surrealdb
@pytest.mark.asyncio
async def test_transaction_ordering(
    surreal_conn: tuple[Any, str, str],
) -> None:
    """Test that record operations happen in correct order within a transaction."""
    conn, ns, db = surreal_conn

    coco_env.context_provider.provide(
        SURREAL_DB_KEY,
        surrealdb.ConnectionFactory(
            url=_SURREALDB_URL,
            namespace=ns,
            database=db,
            credentials={"username": _SURREALDB_USER, "password": _SURREALDB_PASS}
            if _SURREALDB_USER
            else None,
        ),
    )
    person_rows: list[Person] = []
    relation_rows: list[dict[str, Any]] = []

    async def declare_persons_and_follows() -> None:
        person_schema = await surrealdb.TableSchema.from_class(Person)
        person_target = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "person"),
            surrealdb.mount_table_target,
            SURREAL_DB_KEY,
            "person",
            person_schema,
        )
        for r in person_rows:
            person_target.declare_record(row=r)

        follows_target = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "follows"),
            surrealdb.mount_relation_target,
            SURREAL_DB_KEY,
            "follows",
            person_target,
            person_target,
            None,  # Schemaless
        )
        for rel in relation_rows:
            follows_target.declare_relation(
                from_id=rel["from_id"],
                to_id=rel["to_id"],
            )

    app = coco.App(
        coco.AppConfig(name="test_txn_ordering", environment=coco_env),
        declare_persons_and_follows,
    )

    # Run 1: create persons and follows
    person_rows = [
        Person(id="alice", name="Alice"),
        Person(id="bob", name="Bob"),
    ]
    relation_rows = [
        {"from_id": "alice", "to_id": "bob"},
    ]
    await app.update()

    assert len(await _query_table(conn, "person")) == 2
    assert len(await _query_table(conn, "follows")) == 1

    # Run 2: update persons, add/remove relations
    person_rows = [
        Person(id="alice", name="Alice Updated"),
        Person(id="bob", name="Bob"),
        Person(id="charlie", name="Charlie"),
    ]
    relation_rows = [
        {"from_id": "bob", "to_id": "charlie"},
    ]
    await app.update()

    assert len(await _query_table(conn, "person")) == 3
    follows = await _query_table(conn, "follows")
    assert len(follows) == 1


@requires_surrealdb
@pytest.mark.asyncio
async def test_table_level_ordering(
    surreal_conn: tuple[Any, str, str],
) -> None:
    """Test DDL ordering: create tables before relations, remove relations before tables."""
    conn, ns, db = surreal_conn

    coco_env.context_provider.provide(
        SURREAL_DB_KEY,
        surrealdb.ConnectionFactory(
            url=_SURREALDB_URL,
            namespace=ns,
            database=db,
            credentials={"username": _SURREALDB_USER, "password": _SURREALDB_PASS}
            if _SURREALDB_USER
            else None,
        ),
    )

    async def declare_tables_and_relation() -> None:
        person_schema = await surrealdb.TableSchema.from_class(Person)
        person_target = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "person"),
            surrealdb.mount_table_target,
            SURREAL_DB_KEY,
            "person",
            person_schema,
        )
        person_target.declare_record(row=Person(id="alice", name="Alice"))

        follows_target = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "follows"),
            surrealdb.mount_relation_target,
            SURREAL_DB_KEY,
            "follows",
            person_target,
            person_target,
            None,
        )
        follows_target.declare_relation(from_id="alice", to_id="alice")

    app_create = coco.App(
        coco.AppConfig(name="test_ddl_order", environment=coco_env),
        declare_tables_and_relation,
    )
    await app_create.update()

    assert await _table_exists(conn, "person")
    assert await _table_exists(conn, "follows")

    # Drop — this should remove tables in correct order (relations before normal)
    await app_create.drop()

    assert not await _table_exists(conn, "person")
    assert not await _table_exists(conn, "follows")


@requires_surrealdb
@pytest.mark.asyncio
async def test_multiple_tables_shared_sink(
    surreal_conn: tuple[Any, str, str],
) -> None:
    """Test multiple tables and relations sharing the same sink."""
    conn, ns, db = surreal_conn

    coco_env.context_provider.provide(
        SURREAL_DB_KEY,
        surrealdb.ConnectionFactory(
            url=_SURREALDB_URL,
            namespace=ns,
            database=db,
            credentials={"username": _SURREALDB_USER, "password": _SURREALDB_PASS}
            if _SURREALDB_USER
            else None,
        ),
    )
    person_rows: list[Person] = []
    post_rows: list[Post] = []
    likes_rows: list[dict[str, Any]] = []
    authored_rows: list[dict[str, Any]] = []

    async def declare_multi() -> None:
        person_schema = await surrealdb.TableSchema.from_class(Person)
        person_target = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "person"),
            surrealdb.mount_table_target,
            SURREAL_DB_KEY,
            "person",
            person_schema,
        )
        for r in person_rows:
            person_target.declare_record(row=r)

        post_schema = await surrealdb.TableSchema.from_class(Post)
        post_target = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "post"),
            surrealdb.mount_table_target,
            SURREAL_DB_KEY,
            "post",
            post_schema,
        )
        for r in post_rows:  # type: ignore[assignment]
            post_target.declare_record(row=r)  # type: ignore[arg-type]

        likes_target = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "likes"),
            surrealdb.mount_relation_target,
            SURREAL_DB_KEY,
            "likes",
            person_target,
            post_target,
            None,
        )
        for rel in likes_rows:
            likes_target.declare_relation(
                from_id=rel["from_id"],
                to_id=rel["to_id"],
            )

        authored_target = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "authored"),
            surrealdb.mount_relation_target,
            SURREAL_DB_KEY,
            "authored",
            person_target,
            post_target,
            None,
        )
        for rel in authored_rows:
            authored_target.declare_relation(
                from_id=rel["from_id"],
                to_id=rel["to_id"],
            )

    app = coco.App(
        coco.AppConfig(name="test_shared_sink", environment=coco_env),
        declare_multi,
    )

    person_rows = [
        Person(id="alice", name="Alice"),
        Person(id="bob", name="Bob"),
    ]
    post_rows = [Post(id="post1", title="Hello")]
    likes_rows = [{"from_id": "alice", "to_id": "post1"}]
    authored_rows = [{"from_id": "bob", "to_id": "post1"}]
    await app.update()

    assert len(await _query_table(conn, "person")) == 2
    assert len(await _query_table(conn, "post")) == 1
    assert len(await _query_table(conn, "likes")) == 1
    assert len(await _query_table(conn, "authored")) == 1

    # Update across multiple tables
    person_rows = [Person(id="alice", name="Alice Updated")]
    post_rows = [Post(id="post1", title="Hello Updated")]
    likes_rows = []
    authored_rows = [{"from_id": "alice", "to_id": "post1"}]
    await app.update()

    assert len(await _query_table(conn, "person")) == 1
    assert len(await _query_table(conn, "likes")) == 0
    assert len(await _query_table(conn, "authored")) == 1


# =============================================================================
# Vector index tests
# =============================================================================


@requires_surrealdb
@pytest.mark.asyncio
async def test_vector_index_mtree(
    surreal_conn: tuple[Any, str, str],
) -> None:
    """Test HNSW vector index with cosine metric."""
    conn, ns, db = surreal_conn

    coco_env.context_provider.provide(
        SURREAL_DB_KEY,
        surrealdb.ConnectionFactory(
            url=_SURREALDB_URL,
            namespace=ns,
            database=db,
            credentials={"username": _SURREALDB_USER, "password": _SURREALDB_PASS}
            if _SURREALDB_USER
            else None,
        ),
    )
    rows: list[VecRow] = []

    async def declare_vec_table() -> None:
        schema = await surrealdb.TableSchema.from_class(VecRow)
        table = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "table"),
            surrealdb.mount_table_target,
            SURREAL_DB_KEY,
            "vec_docs",
            schema,
        )
        for r in rows:
            table.declare_record(row=r)

        table.declare_vector_index(
            name="idx_embedding",
            field="embedding",
            metric="cosine",
            method="hnsw",
            dimension=4,
            vector_type="f32",
        )

    app = coco.App(
        coco.AppConfig(name="test_vec_mtree", environment=coco_env),
        declare_vec_table,
    )

    rows = [
        VecRow(
            id="doc1",
            content="Hello",
            embedding=np.array([1.0, 2.0, 3.0, 4.0], dtype=np.float32),
        ),
    ]
    await app.update()

    data = await _query_table(conn, "vec_docs")
    assert len(data) == 1
    # Verify embedding is stored (as array)
    emb = data[0].get("embedding")
    assert emb is not None
    assert len(emb) == 4


@requires_surrealdb
@pytest.mark.asyncio
async def test_vector_index_hnsw(
    surreal_conn: tuple[Any, str, str],
) -> None:
    """Test HNSW vector index."""
    conn, ns, db = surreal_conn

    coco_env.context_provider.provide(
        SURREAL_DB_KEY,
        surrealdb.ConnectionFactory(
            url=_SURREALDB_URL,
            namespace=ns,
            database=db,
            credentials={"username": _SURREALDB_USER, "password": _SURREALDB_PASS}
            if _SURREALDB_USER
            else None,
        ),
    )
    rows: list[VecRow] = []

    async def declare_vec_table() -> None:
        schema = await surrealdb.TableSchema.from_class(VecRow)
        table = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "table"),
            surrealdb.mount_table_target,
            SURREAL_DB_KEY,
            "vec_hnsw",
            schema,
        )
        for r in rows:
            table.declare_record(row=r)

        table.declare_vector_index(
            name="idx_embedding",
            field="embedding",
            metric="euclidean",
            method="hnsw",
            dimension=4,
            vector_type="f32",
        )

    app = coco.App(
        coco.AppConfig(name="test_vec_hnsw", environment=coco_env),
        declare_vec_table,
    )

    rows = [
        VecRow(
            id="doc1",
            content="Test",
            embedding=np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float32),
        ),
    ]
    await app.update()

    data = await _query_table(conn, "vec_hnsw")
    assert len(data) == 1


@requires_surrealdb
@pytest.mark.asyncio
async def test_vector_index_update(
    surreal_conn: tuple[Any, str, str],
) -> None:
    """Test updating vector index spec (metric change)."""
    conn, ns, db = surreal_conn

    coco_env.context_provider.provide(
        SURREAL_DB_KEY,
        surrealdb.ConnectionFactory(
            url=_SURREALDB_URL,
            namespace=ns,
            database=db,
            credentials={"username": _SURREALDB_USER, "password": _SURREALDB_PASS}
            if _SURREALDB_USER
            else None,
        ),
    )
    rows: list[VecRow] = []
    metric: Literal["cosine", "euclidean", "manhattan"] = "cosine"

    async def declare_vec_table() -> None:
        schema = await surrealdb.TableSchema.from_class(VecRow)
        table = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "table"),
            surrealdb.mount_table_target,
            SURREAL_DB_KEY,
            "vec_update",
            schema,
        )
        for r in rows:
            table.declare_record(row=r)

        table.declare_vector_index(
            name="idx_embedding",
            field="embedding",
            metric=metric,
            method="hnsw",
            dimension=4,
            vector_type="f32",
        )

    app = coco.App(
        coco.AppConfig(name="test_vec_update", environment=coco_env),
        declare_vec_table,
    )

    rows = [
        VecRow(
            id="doc1",
            content="Test",
            embedding=np.array([1.0, 2.0, 3.0, 4.0], dtype=np.float32),
        ),
    ]
    await app.update()

    # Change metric
    metric = "euclidean"
    await app.update()

    # Verify data still intact after index recreation
    data = await _query_table(conn, "vec_update")
    assert len(data) == 1


# =============================================================================
# Misc tests
# =============================================================================


@requires_surrealdb
@pytest.mark.asyncio
async def test_user_managed_table(
    surreal_conn: tuple[Any, str, str],
) -> None:
    """Test managed_by='user' — CocoIndex manages rows but not the table DDL."""
    conn, ns, db = surreal_conn

    # Pre-create the table manually
    await _query(conn, "DEFINE TABLE user_managed SCHEMALESS")

    global _source_rows, _row_type, _table_name, _table_schema, _managed_by

    _table_name = "user_managed"
    _table_schema = None
    _managed_by = "user"

    coco_env.context_provider.provide(
        SURREAL_DB_KEY,
        surrealdb.ConnectionFactory(
            url=_SURREALDB_URL,
            namespace=ns,
            database=db,
            credentials={"username": _SURREALDB_USER, "password": _SURREALDB_PASS}
            if _SURREALDB_USER
            else None,
        ),
    )

    app = coco.App(
        coco.AppConfig(name="test_user_managed", environment=coco_env),
        declare_schemaless_rows,
    )

    _source_rows = [{"id": "1", "name": "Alice"}]
    await app.update()

    data = await _query_table(conn, "user_managed")
    assert len(data) == 1

    # Remove all rows
    _source_rows = []
    await app.update()

    data = await _query_table(conn, "user_managed")
    assert len(data) == 0

    # Table should still exist (user-managed)
    assert await _table_exists(conn, "user_managed")


@requires_surrealdb
@pytest.mark.asyncio
async def test_declare_row_alias(
    surreal_conn: tuple[Any, str, str],
) -> None:
    """Test that declare_row works as alias for declare_record."""
    conn, ns, db = surreal_conn

    coco_env.context_provider.provide(
        SURREAL_DB_KEY,
        surrealdb.ConnectionFactory(
            url=_SURREALDB_URL,
            namespace=ns,
            database=db,
            credentials={"username": _SURREALDB_USER, "password": _SURREALDB_PASS}
            if _SURREALDB_USER
            else None,
        ),
    )

    async def declare_with_alias() -> None:
        schema = await surrealdb.TableSchema.from_class(SimpleRow)
        table = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "table"),
            surrealdb.mount_table_target,
            SURREAL_DB_KEY,
            "test_alias",
            schema,
        )
        # Use declare_row (the alias)
        table.declare_row(row=SimpleRow(id="1", name="Alice", value=100))

    app = coco.App(
        coco.AppConfig(name="test_alias", environment=coco_env),
        declare_with_alias,
    )
    await app.update()

    data = await _query_table(conn, "test_alias")
    assert len(data) == 1
    assert data[0]["name"] == "Alice"


@requires_surrealdb
@pytest.mark.asyncio
async def test_schemaless_struct_input(
    surreal_conn: tuple[Any, str, str],
) -> None:
    """Test SCHEMALESS table accepts dataclass instances."""
    conn, ns, db = surreal_conn

    coco_env.context_provider.provide(
        SURREAL_DB_KEY,
        surrealdb.ConnectionFactory(
            url=_SURREALDB_URL,
            namespace=ns,
            database=db,
            credentials={"username": _SURREALDB_USER, "password": _SURREALDB_PASS}
            if _SURREALDB_USER
            else None,
        ),
    )

    async def declare_schemaless_struct() -> None:
        table = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "table"),
            surrealdb.mount_table_target,
            SURREAL_DB_KEY,
            "test_struct",
            None,  # Schemaless
        )
        table.declare_record(row=SimpleRow(id="1", name="Alice", value=100))

    app = coco.App(
        coco.AppConfig(name="test_struct", environment=coco_env),
        declare_schemaless_struct,
    )
    await app.update()

    data = await _query_table(conn, "test_struct")
    assert len(data) == 1
    assert data[0]["name"] == "Alice"
    assert data[0]["value"] == 100


@requires_surrealdb
@pytest.mark.asyncio
async def test_type_mapping(
    surreal_conn: tuple[Any, str, str],
) -> None:
    """Test Python-to-SurrealDB type mapping with various types."""
    conn, ns, db = surreal_conn

    coco_env.context_provider.provide(
        SURREAL_DB_KEY,
        surrealdb.ConnectionFactory(
            url=_SURREALDB_URL,
            namespace=ns,
            database=db,
            credentials={"username": _SURREALDB_USER, "password": _SURREALDB_PASS}
            if _SURREALDB_USER
            else None,
        ),
    )

    async def declare_types_table() -> None:
        schema = await surrealdb.TableSchema.from_class(TypesRow)
        table = await coco.use_mount(  # type: ignore[call-overload]
            coco.component_subpath("setup", "table"),
            surrealdb.mount_table_target,
            SURREAL_DB_KEY,
            "test_types",
            schema,
        )
        table.declare_record(
            row=TypesRow(
                id="1",
                flag=True,
                count=42,
                score=3.14,
                label="hello",
                created="2025-01-01",
            )
        )

    app = coco.App(
        coco.AppConfig(name="test_types", environment=coco_env),
        declare_types_table,
    )
    await app.update()

    data = await _query_table(conn, "test_types")
    assert len(data) == 1
    row = data[0]
    assert row["flag"] is True
    assert row["count"] == 42
    assert abs(row["score"] - 3.14) < 0.01
    assert row["label"] == "hello"

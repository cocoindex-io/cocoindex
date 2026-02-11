"""Tests for SQLite target connector."""

from __future__ import annotations

import sqlite3
import struct
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, Iterator

import numpy as np
import pytest
from numpy.typing import NDArray

import cocoindex as coco
from cocoindex.connectors import sqlite
from cocoindex.resources.schema import VectorSchema

from tests import common

coco_env = common.create_test_env(__file__)


# =============================================================================
# Check for sqlite-vec availability
# =============================================================================

try:
    import sqlite_vec  # type: ignore[import-not-found]

    HAS_SQLITE_VEC = True
except ImportError:
    HAS_SQLITE_VEC = False

requires_sqlite_vec = pytest.mark.skipif(
    not HAS_SQLITE_VEC, reason="sqlite-vec is not installed"
)


# =============================================================================
# Test utilities
# =============================================================================


def read_table_data(
    managed_conn: sqlite.ManagedConnection, table_name: str
) -> list[dict[str, Any]]:
    """Read all rows from a table as a list of dicts."""
    with managed_conn.readonly() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(f'SELECT * FROM "{table_name}"')
        return [dict(row) for row in cursor.fetchall()]


def table_exists(managed_conn: sqlite.ManagedConnection, table_name: str) -> bool:
    """Check if a table exists."""
    with managed_conn.readonly() as conn:
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        return cursor.fetchone() is not None


def get_table_columns(
    managed_conn: sqlite.ManagedConnection, table_name: str
) -> dict[str, str]:
    """Get column names and types for a table."""
    with managed_conn.readonly() as conn:
        cursor = conn.execute(f'PRAGMA table_info("{table_name}")')
        return {row[1]: row[2] for row in cursor.fetchall()}


def decode_vector(blob: bytes, dim: int) -> list[float]:
    """Decode a sqlite-vec vector blob to a list of floats."""
    return list(struct.unpack(f"{dim}f", blob))


# =============================================================================
# Test fixtures
# =============================================================================


@pytest.fixture
def sqlite_db() -> Iterator[tuple[sqlite.ManagedConnection, Path]]:
    """Create a temporary SQLite database."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)

    managed_conn = sqlite.connect(db_path)
    yield managed_conn, db_path
    managed_conn.close()
    db_path.unlink(missing_ok=True)


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


# =============================================================================
# Source data (global state for tests)
# =============================================================================

_source_rows: list[Any] = []
_row_type: type = SimpleRow
_table_name: str = "test_table"
_sqlite_db: sqlite.SqliteDatabase | None = None


# =============================================================================
# App functions
# =============================================================================


async def declare_table_and_rows() -> None:
    """Declare table and rows from global source data."""
    assert _sqlite_db is not None

    table = coco.mount_run(
        coco.component_subpath("setup", "table"),
        _sqlite_db.declare_table_target,
        _table_name,
        await sqlite.TableSchema.from_class(_row_type, primary_key=["id"]),
    ).result()

    for row in _source_rows:
        table.declare_row(row=row)


# =============================================================================
# Non-vector test cases
# =============================================================================


def test_create_table_and_insert_rows(
    sqlite_db: tuple[sqlite.ManagedConnection, Path],
) -> None:
    """Test creating a table and inserting rows."""
    managed_conn, _ = sqlite_db
    global _source_rows, _row_type, _table_name, _sqlite_db

    _source_rows = []
    _row_type = SimpleRow
    _table_name = "test_create"

    with sqlite.register_db("test_create_db", managed_conn) as db:
        _sqlite_db = db

        app = coco.App(
            coco.AppConfig(name="test_create_table_and_insert", environment=coco_env),
            declare_table_and_rows,
        )

        # Insert initial data
        _source_rows = [
            SimpleRow(id="1", name="Alice", value=100),
            SimpleRow(id="2", name="Bob", value=200),
        ]
        app.update()

        assert table_exists(managed_conn, _table_name)
        data = read_table_data(managed_conn, _table_name)
        assert len(data) == 2
        assert {"id": "1", "name": "Alice", "value": 100} in data
        assert {"id": "2", "name": "Bob", "value": 200} in data

        # Insert more data
        _source_rows.append(SimpleRow(id="3", name="Charlie", value=300))
        app.update()

        data = read_table_data(managed_conn, _table_name)
        assert len(data) == 3
        assert {"id": "3", "name": "Charlie", "value": 300} in data


def test_update_row(sqlite_db: tuple[sqlite.ManagedConnection, Path]) -> None:
    """Test updating an existing row."""
    managed_conn, _ = sqlite_db
    global _source_rows, _row_type, _table_name, _sqlite_db

    _source_rows = []
    _row_type = SimpleRow
    _table_name = "test_update"

    with sqlite.register_db("test_update_db", managed_conn) as db:
        _sqlite_db = db

        app = coco.App(
            coco.AppConfig(name="test_update_row", environment=coco_env),
            declare_table_and_rows,
        )

        # Insert initial data
        _source_rows = [
            SimpleRow(id="1", name="Alice", value=100),
            SimpleRow(id="2", name="Bob", value=200),
        ]
        app.update()

        data = read_table_data(managed_conn, _table_name)
        assert len(data) == 2

        # Update a row
        _source_rows = [
            SimpleRow(id="1", name="Alice Updated", value=150),
            SimpleRow(id="2", name="Bob", value=200),
        ]
        app.update()

        data = read_table_data(managed_conn, _table_name)
        assert len(data) == 2
        assert {"id": "1", "name": "Alice Updated", "value": 150} in data
        assert {"id": "2", "name": "Bob", "value": 200} in data


def test_delete_row(sqlite_db: tuple[sqlite.ManagedConnection, Path]) -> None:
    """Test deleting a row."""
    managed_conn, _ = sqlite_db
    global _source_rows, _row_type, _table_name, _sqlite_db

    _source_rows = []
    _row_type = SimpleRow
    _table_name = "test_delete"

    with sqlite.register_db("test_delete_db", managed_conn) as db:
        _sqlite_db = db

        app = coco.App(
            coco.AppConfig(name="test_delete_row", environment=coco_env),
            declare_table_and_rows,
        )

        # Insert initial data
        _source_rows = [
            SimpleRow(id="1", name="Alice", value=100),
            SimpleRow(id="2", name="Bob", value=200),
            SimpleRow(id="3", name="Charlie", value=300),
        ]
        app.update()

        data = read_table_data(managed_conn, _table_name)
        assert len(data) == 3

        # Delete a row
        _source_rows = [
            SimpleRow(id="1", name="Alice", value=100),
            SimpleRow(id="3", name="Charlie", value=300),
        ]
        app.update()

        data = read_table_data(managed_conn, _table_name)
        assert len(data) == 2
        assert {"id": "1", "name": "Alice", "value": 100} in data
        assert {"id": "3", "name": "Charlie", "value": 300} in data
        # Bob should be deleted
        assert not any(row["id"] == "2" for row in data)


def test_different_schema_types(
    sqlite_db: tuple[sqlite.ManagedConnection, Path],
) -> None:
    """Test creating tables with different schema types (dataclass with different columns)."""
    managed_conn, _ = sqlite_db

    extended_rows: list[ExtendedRow] = []

    with sqlite.register_db("test_schema_types_db", managed_conn) as db:

        async def declare_extended_table() -> None:
            table = coco.mount_run(
                coco.component_subpath("setup", "table"),
                db.declare_table_target,
                "extended_table",
                await sqlite.TableSchema.from_class(ExtendedRow, primary_key=["id"]),
            ).result()
            for row in extended_rows:
                table.declare_row(row=row)

        app = coco.App(
            coco.AppConfig(name="test_different_schema_types", environment=coco_env),
            declare_extended_table,
        )

        extended_rows.extend(
            [
                ExtendedRow(id="1", name="Alice", value=100, extra="extra_data"),
                ExtendedRow(id="2", name="Bob", value=200, extra="more_data"),
            ]
        )
        app.update()

        columns = get_table_columns(managed_conn, "extended_table")
        assert "extra" in columns

        data = read_table_data(managed_conn, "extended_table")
        assert len(data) == 2
        assert {"id": "1", "name": "Alice", "value": 100, "extra": "extra_data"} in data
        assert {"id": "2", "name": "Bob", "value": 200, "extra": "more_data"} in data


def test_drop_table(sqlite_db: tuple[sqlite.ManagedConnection, Path]) -> None:
    """Test dropping a table when no longer declared."""
    managed_conn, _ = sqlite_db
    global _source_rows, _row_type, _table_name, _sqlite_db

    _source_rows = []
    _row_type = SimpleRow
    _table_name = "test_drop"

    with sqlite.register_db("test_drop_db", managed_conn) as db:
        _sqlite_db = db

        async def declare_table_conditionally() -> None:
            if _source_rows:  # Only declare if there are rows
                table = coco.mount_run(
                    coco.component_subpath("setup", "table"),
                    db.declare_table_target,
                    _table_name,
                    await sqlite.TableSchema.from_class(_row_type, primary_key=["id"]),
                ).result()
                for row in _source_rows:
                    table.declare_row(row=row)

        app = coco.App(
            coco.AppConfig(name="test_drop_table", environment=coco_env),
            declare_table_conditionally,
        )

        # Create table with data
        _source_rows = [SimpleRow(id="1", name="Alice", value=100)]
        app.update()

        assert table_exists(managed_conn, _table_name)

        # Remove all rows (table should be dropped)
        _source_rows = []
        app.update()

        assert not table_exists(managed_conn, _table_name)


def test_no_change_optimization(
    sqlite_db: tuple[sqlite.ManagedConnection, Path],
) -> None:
    """Test that unchanged data doesn't cause unnecessary updates."""
    managed_conn, _ = sqlite_db
    global _source_rows, _row_type, _table_name, _sqlite_db

    _source_rows = []
    _row_type = SimpleRow
    _table_name = "test_no_change"

    with sqlite.register_db("test_no_change_db", managed_conn) as db:
        _sqlite_db = db

        app = coco.App(
            coco.AppConfig(name="test_no_change", environment=coco_env),
            declare_table_and_rows,
        )

        # Insert initial data
        _source_rows = [
            SimpleRow(id="1", name="Alice", value=100),
            SimpleRow(id="2", name="Bob", value=200),
        ]
        app.update()

        data1 = read_table_data(managed_conn, _table_name)
        assert len(data1) == 2

        # Run update again with same data - should be a no-op
        app.update()

        data2 = read_table_data(managed_conn, _table_name)
        assert data1 == data2


def test_multiple_tables(sqlite_db: tuple[sqlite.ManagedConnection, Path]) -> None:
    """Test managing multiple tables in the same database."""
    managed_conn, _ = sqlite_db

    table1_rows: list[SimpleRow] = []
    table2_rows: list[SimpleRow] = []

    with sqlite.register_db("multi_table_db", managed_conn) as db:

        async def declare_multiple_tables() -> None:
            schema = await sqlite.TableSchema.from_class(SimpleRow, primary_key=["id"])

            table1 = coco.mount_run(
                coco.component_subpath("setup", "table1"),
                db.declare_table_target,
                "users",
                schema,
            ).result()

            table2 = coco.mount_run(
                coco.component_subpath("setup", "table2"),
                db.declare_table_target,
                "products",
                schema,
            ).result()

            for row in table1_rows:
                table1.declare_row(row=row)

            for row in table2_rows:
                table2.declare_row(row=row)

        app = coco.App(
            coco.AppConfig(name="test_multiple_tables", environment=coco_env),
            declare_multiple_tables,
        )

        # Insert data into both tables
        table1_rows.extend(
            [
                SimpleRow(id="u1", name="User1", value=1),
                SimpleRow(id="u2", name="User2", value=2),
            ]
        )
        table2_rows.extend(
            [
                SimpleRow(id="p1", name="Product1", value=100),
                SimpleRow(id="p2", name="Product2", value=200),
            ]
        )
        app.update()

        assert table_exists(managed_conn, "users")
        assert table_exists(managed_conn, "products")

        users_data = read_table_data(managed_conn, "users")
        products_data = read_table_data(managed_conn, "products")

        assert len(users_data) == 2
        assert len(products_data) == 2


def test_dict_rows(sqlite_db: tuple[sqlite.ManagedConnection, Path]) -> None:
    """Test using dict rows instead of dataclass rows."""
    managed_conn, _ = sqlite_db

    dict_rows: list[dict[str, Any]] = []

    with sqlite.register_db("dict_table_db", managed_conn) as db:

        def declare_dict_table() -> None:
            table = coco.mount_run(
                coco.component_subpath("setup", "table"),
                db.declare_table_target,
                "dict_table",
                sqlite.TableSchema(
                    {
                        "id": sqlite.ColumnDef(type="TEXT", nullable=False),
                        "name": sqlite.ColumnDef(type="TEXT"),
                        "count": sqlite.ColumnDef(type="INTEGER"),
                    },
                    primary_key=["id"],
                ),
            ).result()

            for row in dict_rows:
                table.declare_row(row=row)

        app = coco.App(
            coco.AppConfig(name="test_dict_rows", environment=coco_env),
            declare_dict_table,
        )

        dict_rows.extend(
            [
                {"id": "1", "name": "Item1", "count": 10},
                {"id": "2", "name": "Item2", "count": 20},
            ]
        )
        app.update()

        data = read_table_data(managed_conn, "dict_table")
        assert len(data) == 2
        assert {"id": "1", "name": "Item1", "count": 10} in data


def test_user_managed_table(sqlite_db: tuple[sqlite.ManagedConnection, Path]) -> None:
    """Test user-managed table (CocoIndex only manages rows, not DDL)."""
    managed_conn, _ = sqlite_db

    # Pre-create the table manually
    with managed_conn.transaction() as conn:
        conn.execute("""
            CREATE TABLE user_managed (
                id TEXT PRIMARY KEY,
                name TEXT,
                value INTEGER
            )
        """)

    user_rows: list[SimpleRow] = []

    with sqlite.register_db("user_managed_db", managed_conn) as db:

        async def declare_user_managed_rows() -> None:
            table = coco.mount_run(
                coco.component_subpath("setup", "table"),
                db.declare_table_target,
                "user_managed",
                await sqlite.TableSchema.from_class(SimpleRow, primary_key=["id"]),
                managed_by="user",
            ).result()

            for row in user_rows:
                table.declare_row(row=row)

        app = coco.App(
            coco.AppConfig(name="test_user_managed", environment=coco_env),
            declare_user_managed_rows,
        )

        # Insert rows
        user_rows.extend(
            [
                SimpleRow(id="1", name="Alice", value=100),
            ]
        )
        app.update()

        data = read_table_data(managed_conn, "user_managed")
        assert len(data) == 1

        # Clear rows - table should remain (user-managed)
        user_rows.clear()
        app.update()

        # Table should still exist
        assert table_exists(managed_conn, "user_managed")
        # But rows should be deleted
        data = read_table_data(managed_conn, "user_managed")
        assert len(data) == 0


# =============================================================================
# Virtual table tests
# =============================================================================


@requires_sqlite_vec
def test_vec0_virtual_table_basic(
    sqlite_db: tuple[sqlite.ManagedConnection, Path],
) -> None:
    """Test creating a basic vec0 virtual table with vectors."""
    managed_conn, _ = sqlite_db

    @dataclass
    class Vec0Row:
        id: int
        content: str
        embedding: Annotated[
            NDArray[np.float32], VectorSchema(dtype=np.dtype(np.float32), size=4)
        ]

    rows: list[Vec0Row] = []

    with sqlite.register_db("vec0_basic_db", managed_conn) as db:

        async def declare_vec0_table() -> None:
            table = coco.mount_run(
                coco.component_subpath("setup", "table"),
                db.declare_table_target,
                table_name="vec0_docs",
                table_schema=await sqlite.TableSchema.from_class(
                    Vec0Row,
                    primary_key=["id"],
                ),
                virtual_table_def=sqlite.Vec0TableDef(),
            ).result()

            for row in rows:
                table.declare_row(row=row)

        app = coco.App(
            coco.AppConfig(name="Vec0BasicTest", environment=coco_env),
            declare_vec0_table,
        )

        # Insert initial rows
        rows = [
            Vec0Row(
                id=1,
                content="Doc 1",
                embedding=np.array([1.0, 2.0, 3.0, 4.0], dtype=np.float32),
            ),
            Vec0Row(
                id=2,
                content="Doc 2",
                embedding=np.array([5.0, 6.0, 7.0, 8.0], dtype=np.float32),
            ),
        ]
        app.update()

        # Verify table was created as virtual table
        with managed_conn.readonly() as conn:
            cursor = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='vec0_docs'"
            )
            sql = cursor.fetchone()[0]
            assert "VIRTUAL TABLE" in sql
            assert "USING vec0" in sql

        # Verify data and decode vectors
        data = read_table_data(managed_conn, "vec0_docs")
        assert len(data) == 2

        doc1 = next(row for row in data if row["id"] == 1)
        assert doc1["content"] == "Doc 1"
        vector1 = decode_vector(doc1["embedding"], 4)
        assert vector1 == pytest.approx([1.0, 2.0, 3.0, 4.0])

        doc2 = next(row for row in data if row["id"] == 2)
        assert doc2["content"] == "Doc 2"
        vector2 = decode_vector(doc2["embedding"], 4)
        assert vector2 == pytest.approx([5.0, 6.0, 7.0, 8.0])

        # Test update
        rows[0] = Vec0Row(
            id=1,
            content="Updated Doc 1",
            embedding=np.array([9.0, 9.0, 9.0, 9.0], dtype=np.float32),
        )
        app.update()

        data = read_table_data(managed_conn, "vec0_docs")
        assert len(data) == 2
        doc1 = next(row for row in data if row["id"] == 1)
        assert doc1["content"] == "Updated Doc 1"
        vector1 = decode_vector(doc1["embedding"], 4)
        assert vector1 == pytest.approx([9.0, 9.0, 9.0, 9.0])

        # Test delete
        rows.pop(0)  # Remove doc1
        app.update()

        data = read_table_data(managed_conn, "vec0_docs")
        assert len(data) == 1
        assert data[0]["id"] == 2


@requires_sqlite_vec
def test_vec0_with_partition_keys(
    sqlite_db: tuple[sqlite.ManagedConnection, Path],
) -> None:
    """Test vec0 virtual table with partition keys."""
    managed_conn, _ = sqlite_db

    @dataclass
    class Vec0PartitionedRow:
        id: int
        year: int
        content: str
        embedding: Annotated[
            NDArray[np.float32], VectorSchema(dtype=np.dtype(np.float32), size=2)
        ]

    rows: list[Vec0PartitionedRow] = []

    with sqlite.register_db("vec0_partition_db", managed_conn) as db:

        async def declare_partitioned_table() -> None:
            table = coco.mount_run(
                coco.component_subpath("setup", "table"),
                db.declare_table_target,
                table_name="vec0_partitioned",
                table_schema=await sqlite.TableSchema.from_class(
                    Vec0PartitionedRow,
                    primary_key=["id"],
                ),
                virtual_table_def=sqlite.Vec0TableDef(
                    partition_key_columns=["year"],
                ),
            ).result()

            for row in rows:
                table.declare_row(row=row)

        app = coco.App(
            coco.AppConfig(name="Vec0PartitionTest", environment=coco_env),
            declare_partitioned_table,
        )

        rows = [
            Vec0PartitionedRow(
                id=1,
                year=2024,
                content="Old doc",
                embedding=np.array([1.0, 2.0], dtype=np.float32),
            ),
            Vec0PartitionedRow(
                id=2,
                year=2025,
                content="New doc",
                embedding=np.array([3.0, 4.0], dtype=np.float32),
            ),
        ]
        app.update()

        # Verify CREATE statement includes partition key
        with managed_conn.readonly() as conn:
            cursor = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='vec0_partitioned'"
            )
            sql = cursor.fetchone()[0]
            assert "partition key" in sql.lower()

        data = read_table_data(managed_conn, "vec0_partitioned")
        assert len(data) == 2


@requires_sqlite_vec
def test_vec0_with_auxiliary_columns(
    sqlite_db: tuple[sqlite.ManagedConnection, Path],
) -> None:
    """Test vec0 virtual table with auxiliary columns."""
    managed_conn, _ = sqlite_db

    @dataclass
    class Vec0AuxRow:
        id: int
        content: str
        metadata: str  # Auxiliary column
        embedding: Annotated[
            NDArray[np.float32], VectorSchema(dtype=np.dtype(np.float32), size=2)
        ]

    rows: list[Vec0AuxRow] = []

    with sqlite.register_db("vec0_aux_db", managed_conn) as db:

        async def declare_aux_table() -> None:
            table = coco.mount_run(
                coco.component_subpath("setup", "table"),
                db.declare_table_target,
                table_name="vec0_with_aux",
                table_schema=await sqlite.TableSchema.from_class(
                    Vec0AuxRow,
                    primary_key=["id"],
                ),
                virtual_table_def=sqlite.Vec0TableDef(
                    auxiliary_columns=["metadata"],
                ),
            ).result()

            for row in rows:
                table.declare_row(row=row)

        app = coco.App(
            coco.AppConfig(name="Vec0AuxTest", environment=coco_env),
            declare_aux_table,
        )

        rows = [
            Vec0AuxRow(
                id=1,
                content="Doc",
                metadata="extra info",
                embedding=np.array([1.0, 2.0], dtype=np.float32),
            ),
        ]
        app.update()

        # Verify CREATE statement includes + prefix for auxiliary column
        with managed_conn.readonly() as conn:
            cursor = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='vec0_with_aux'"
            )
            sql = cursor.fetchone()[0]
            assert "+metadata" in sql

        data = read_table_data(managed_conn, "vec0_with_aux")
        assert len(data) == 1
        assert data[0]["metadata"] == "extra info"


@requires_sqlite_vec
def test_vec0_schema_change_forces_recreate(
    sqlite_db: tuple[sqlite.ManagedConnection, Path],
) -> None:
    """Test that schema changes to vec0 virtual tables trigger DROP+CREATE."""
    managed_conn, _ = sqlite_db

    @dataclass
    class Vec0RowV1:
        id: int
        content: str
        embedding: Annotated[
            NDArray[np.float32], VectorSchema(dtype=np.dtype(np.float32), size=2)
        ]

    @dataclass
    class Vec0RowV2:
        id: int
        content: str
        new_field: str  # Added column
        embedding: Annotated[
            NDArray[np.float32], VectorSchema(dtype=np.dtype(np.float32), size=2)
        ]

    row_schema: type[Vec0RowV1] | type[Vec0RowV2] = Vec0RowV1
    rows: list[Vec0RowV1 | Vec0RowV2] = []

    with sqlite.register_db("vec0_schema_db", managed_conn) as db:

        async def declare_evolving_table() -> None:
            table = coco.mount_run(
                coco.component_subpath("setup", "table"),
                db.declare_table_target,
                table_name="vec0_evolving",
                table_schema=await sqlite.TableSchema.from_class(
                    row_schema,
                    primary_key=["id"],
                ),
                virtual_table_def=sqlite.Vec0TableDef(),
            ).result()

            for row in rows:
                table.declare_row(row=row)

        app = coco.App(
            coco.AppConfig(name="Vec0SchemaChangeTest", environment=coco_env),
            declare_evolving_table,
        )

        # Initial schema
        rows = [
            Vec0RowV1(
                id=1, content="Doc 1", embedding=np.array([1.0, 2.0], dtype=np.float32)
            ),
        ]
        app.update()

        columns = get_table_columns(managed_conn, "vec0_evolving")
        assert "new_field" not in columns

        # Change schema (add column) - should trigger DROP+CREATE
        row_schema = Vec0RowV2
        rows = [
            Vec0RowV2(
                id=1,
                content="Doc 1",
                new_field="value",
                embedding=np.array([1.0, 2.0], dtype=np.float32),
            ),
        ]
        app.update()

        # Verify new column exists
        columns = get_table_columns(managed_conn, "vec0_evolving")
        assert "new_field" in columns

        data = read_table_data(managed_conn, "vec0_evolving")
        assert len(data) == 1
        assert data[0]["new_field"] == "value"


@requires_sqlite_vec
def test_vec0_without_vector_column_raises_error(
    sqlite_db: tuple[sqlite.ManagedConnection, Path],
) -> None:
    """Test that vec0 table without vector columns raises validation error."""
    managed_conn, _ = sqlite_db

    @dataclass
    class NoVectorRow:
        id: int
        content: str

    with sqlite.register_db("vec0_novector_db", managed_conn) as db:

        async def declare_invalid_table() -> None:
            db.declare_table_target(
                table_name="vec0_invalid",
                table_schema=await sqlite.TableSchema.from_class(
                    NoVectorRow,
                    primary_key=["id"],
                ),
                virtual_table_def=sqlite.Vec0TableDef(),
            )

        app = coco.App(
            coco.AppConfig(name="Vec0NoVectorTest", environment=coco_env),
            declare_invalid_table,
        )

        with pytest.raises(
            ValueError, match="require at least one float\\[N\\] vector column"
        ):
            app.update()


@requires_sqlite_vec
def test_vec0_with_composite_pk_raises_error(
    sqlite_db: tuple[sqlite.ManagedConnection, Path],
) -> None:
    """Test that vec0 table with composite primary key raises validation error."""
    managed_conn, _ = sqlite_db

    @dataclass
    class CompositePkRow:
        id1: int
        id2: int
        embedding: Annotated[
            NDArray[np.float32], VectorSchema(dtype=np.dtype(np.float32), size=2)
        ]

    with sqlite.register_db("vec0_composite_db", managed_conn) as db:

        async def declare_invalid_table() -> None:
            db.declare_table_target(
                table_name="vec0_composite_pk",
                table_schema=await sqlite.TableSchema.from_class(
                    CompositePkRow,
                    primary_key=["id1", "id2"],
                ),
                virtual_table_def=sqlite.Vec0TableDef(),
            )

        app = coco.App(
            coco.AppConfig(name="Vec0CompositePkTest", environment=coco_env),
            declare_invalid_table,
        )

        with pytest.raises(ValueError, match="require exactly one primary key column"):
            app.update()


@requires_sqlite_vec
def test_vec0_with_non_integer_pk_raises_error(
    sqlite_db: tuple[sqlite.ManagedConnection, Path],
) -> None:
    """Test that vec0 table with non-integer primary key raises validation error."""
    managed_conn, _ = sqlite_db

    @dataclass
    class StringPkRow:
        id: str
        embedding: Annotated[
            NDArray[np.float32], VectorSchema(dtype=np.dtype(np.float32), size=2)
        ]

    with sqlite.register_db("vec0_stringpk_db", managed_conn) as db:

        async def declare_invalid_table() -> None:
            db.declare_table_target(
                table_name="vec0_string_pk",
                table_schema=await sqlite.TableSchema.from_class(
                    StringPkRow,
                    primary_key=["id"],
                ),
                virtual_table_def=sqlite.Vec0TableDef(),
            )

        app = coco.App(
            coco.AppConfig(name="Vec0StringPkTest", environment=coco_env),
            declare_invalid_table,
        )

        with pytest.raises(ValueError, match="require INTEGER primary key"):
            app.update()


@requires_sqlite_vec
def test_vec0_without_extension_raises_error(
    sqlite_db: tuple[sqlite.ManagedConnection, Path],
) -> None:
    """Test that vec0 table without sqlite-vec extension raises error."""
    # Create a new connection without loading vec extension
    _, db_path = sqlite_db
    managed_conn_no_vec = sqlite.connect(str(db_path), load_vec=False)

    @dataclass
    class Vec0Row:
        id: int
        embedding: Annotated[
            NDArray[np.float32], VectorSchema(dtype=np.dtype(np.float32), size=2)
        ]

    with sqlite.register_db("vec0_noext_db", managed_conn_no_vec) as db:

        async def declare_table_without_ext() -> None:
            db.declare_table_target(
                table_name="vec0_needs_ext",
                table_schema=await sqlite.TableSchema.from_class(
                    Vec0Row,
                    primary_key=["id"],
                ),
                virtual_table_def=sqlite.Vec0TableDef(),
            )

        app = coco.App(
            coco.AppConfig(name="Vec0NoExtTest", environment=coco_env),
            declare_table_without_ext,
        )

        with pytest.raises(RuntimeError, match="sqlite-vec extension must be loaded"):
            app.update()

    managed_conn_no_vec.close()


@requires_sqlite_vec
def test_vec0_invalid_partition_key_raises_error(
    sqlite_db: tuple[sqlite.ManagedConnection, Path],
) -> None:
    """Test that vec0 table with invalid partition key column raises error."""
    managed_conn, _ = sqlite_db

    @dataclass
    class Vec0Row:
        id: int
        embedding: Annotated[
            NDArray[np.float32], VectorSchema(dtype=np.dtype(np.float32), size=2)
        ]

    with sqlite.register_db("vec0_badpartition_db", managed_conn) as db:

        async def declare_invalid_table() -> None:
            db.declare_table_target(
                table_name="vec0_bad_partition",
                table_schema=await sqlite.TableSchema.from_class(
                    Vec0Row,
                    primary_key=["id"],
                ),
                virtual_table_def=sqlite.Vec0TableDef(
                    partition_key_columns=["nonexistent_column"],
                ),
            )

        app = coco.App(
            coco.AppConfig(name="Vec0BadPartitionTest", environment=coco_env),
            declare_invalid_table,
        )

        with pytest.raises(ValueError, match="Partition key columns not in schema"):
            app.update()


@requires_sqlite_vec
def test_vec0_invalid_auxiliary_column_raises_error(
    sqlite_db: tuple[sqlite.ManagedConnection, Path],
) -> None:
    """Test that vec0 table with invalid auxiliary column raises error."""
    managed_conn, _ = sqlite_db

    @dataclass
    class Vec0Row:
        id: int
        embedding: Annotated[
            NDArray[np.float32], VectorSchema(dtype=np.dtype(np.float32), size=2)
        ]

    with sqlite.register_db("vec0_badaux_db", managed_conn) as db:

        async def declare_invalid_table() -> None:
            db.declare_table_target(
                table_name="vec0_bad_aux",
                table_schema=await sqlite.TableSchema.from_class(
                    Vec0Row,
                    primary_key=["id"],
                ),
                virtual_table_def=sqlite.Vec0TableDef(
                    auxiliary_columns=["nonexistent_column"],
                ),
            )

        app = coco.App(
            coco.AppConfig(name="Vec0BadAuxTest", environment=coco_env),
            declare_invalid_table,
        )

        with pytest.raises(ValueError, match="Auxiliary columns not in schema"):
            app.update()


@requires_sqlite_vec
def test_vec0_with_column_overrides(
    sqlite_db: tuple[sqlite.ManagedConnection, Path],
) -> None:
    """Test vec0 virtual table with VectorSchema in column_overrides."""
    managed_conn, _ = sqlite_db

    explicit_vector_schema = VectorSchema(dtype=np.dtype(np.float32), size=3)

    @dataclass
    class Vec0OverrideRow:
        id: int
        data: str
        vec: NDArray[np.float32]  # No annotation, use column_overrides

    rows: list[Vec0OverrideRow] = []

    with sqlite.register_db("vec0_override_db", managed_conn) as db:

        async def declare_vec0_override_table() -> None:
            table = coco.mount_run(
                coco.component_subpath("setup", "table"),
                db.declare_table_target,
                table_name="vec0_overrides",
                table_schema=await sqlite.TableSchema.from_class(
                    Vec0OverrideRow,
                    primary_key=["id"],
                    column_overrides={"vec": explicit_vector_schema},
                ),
                virtual_table_def=sqlite.Vec0TableDef(),
            ).result()

            for row in rows:
                table.declare_row(row=row)

        app = coco.App(
            coco.AppConfig(name="Vec0OverrideTest", environment=coco_env),
            declare_vec0_override_table,
        )

        rows = [
            Vec0OverrideRow(
                id=1,
                data="test data",
                vec=np.array([0.1, 0.2, 0.3], dtype=np.float32),
            ),
        ]
        app.update()

        data = read_table_data(managed_conn, "vec0_overrides")
        assert len(data) == 1
        row = data[0]
        assert row["data"] == "test data"
        vector = decode_vector(row["vec"], 3)
        assert vector == pytest.approx([0.1, 0.2, 0.3])


@requires_sqlite_vec
def test_regular_table_vs_vec0_switch(
    sqlite_db: tuple[sqlite.ManagedConnection, Path],
) -> None:
    """Test switching between regular table and vec0 virtual table triggers recreation."""
    managed_conn, _ = sqlite_db

    @dataclass
    class VectorRow:
        id: int
        content: str
        embedding: Annotated[
            NDArray[np.float32], VectorSchema(dtype=np.dtype(np.float32), size=2)
        ]

    use_virtual = False
    rows: list[VectorRow] = []

    with sqlite.register_db("switch_db", managed_conn) as db:

        async def declare_table() -> None:
            table = coco.mount_run(
                coco.component_subpath("setup", "table"),
                db.declare_table_target,
                table_name="switchable",
                table_schema=await sqlite.TableSchema.from_class(
                    VectorRow,
                    primary_key=["id"],
                ),
                virtual_table_def=sqlite.Vec0TableDef() if use_virtual else None,
            ).result()

            for row in rows:
                table.declare_row(row=row)

        app = coco.App(
            coco.AppConfig(name="SwitchTest", environment=coco_env),
            declare_table,
        )

        # Start with regular table
        rows = [
            VectorRow(
                id=1, content="Doc", embedding=np.array([1.0, 2.0], dtype=np.float32)
            ),
        ]
        app.update()

        # Verify it's a regular table
        with managed_conn.readonly() as conn:
            cursor = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='switchable'"
            )
            sql = cursor.fetchone()[0]
            assert "VIRTUAL TABLE" not in sql

        # Switch to virtual table
        use_virtual = True
        app.update()

        # Verify it's now a virtual table
        with managed_conn.readonly() as conn:
            cursor = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='switchable'"
            )
            sql = cursor.fetchone()[0]
            assert "VIRTUAL TABLE" in sql
            assert "USING vec0" in sql
        data = read_table_data(managed_conn, "switchable")
        assert len(data) == 1
        assert data[0]["id"] == 1

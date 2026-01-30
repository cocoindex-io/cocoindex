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


@pytest.fixture
def sqlite_db_with_vec() -> Iterator[tuple[sqlite.ManagedConnection, Path]]:
    """Create a temporary SQLite database with sqlite-vec extension loaded."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)

    managed_conn = sqlite.connect(db_path, load_vec=True)
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


# Vector dimension for tests
VECTOR_DIM = 4
_vector_schema = VectorSchema(dtype=np.dtype(np.float32), size=VECTOR_DIM)


@dataclass
class VectorRow:
    id: str
    content: str
    embedding: Annotated[NDArray[np.float32], _vector_schema]


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


def declare_table_and_rows() -> None:
    """Declare table and rows from global source data."""
    assert _sqlite_db is not None

    table = coco.mount_run(
        coco.component_subpath("setup", "table"),
        _sqlite_db.declare_table_target,
        _table_name,
        sqlite.TableSchema(_row_type, primary_key=["id"]),
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

        def declare_extended_table() -> None:
            table = coco.mount_run(
                coco.component_subpath("setup", "table"),
                db.declare_table_target,
                "extended_table",
                sqlite.TableSchema(ExtendedRow, primary_key=["id"]),
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

        def declare_table_conditionally() -> None:
            if _source_rows:  # Only declare if there are rows
                table = coco.mount_run(
                    coco.component_subpath("setup", "table"),
                    db.declare_table_target,
                    _table_name,
                    sqlite.TableSchema(_row_type, primary_key=["id"]),
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

        def declare_multiple_tables() -> None:
            table1 = coco.mount_run(
                coco.component_subpath("setup", "table1"),
                db.declare_table_target,
                "users",
                sqlite.TableSchema(SimpleRow, primary_key=["id"]),
            ).result()

            table2 = coco.mount_run(
                coco.component_subpath("setup", "table2"),
                db.declare_table_target,
                "products",
                sqlite.TableSchema(SimpleRow, primary_key=["id"]),
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

        def declare_user_managed_rows() -> None:
            table = coco.mount_run(
                coco.component_subpath("setup", "table"),
                db.declare_table_target,
                "user_managed",
                sqlite.TableSchema(SimpleRow, primary_key=["id"]),
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
# Vector test cases (require sqlite-vec)
# =============================================================================


@requires_sqlite_vec
def test_vector_insert_and_read(
    sqlite_db_with_vec: tuple[sqlite.ManagedConnection, Path],
) -> None:
    """Test inserting and reading rows with vector columns."""
    managed_conn, _ = sqlite_db_with_vec

    vector_rows: list[VectorRow] = []

    with sqlite.register_db("vector_test_db", managed_conn) as db:

        def declare_vector_table() -> None:
            table = coco.mount_run(
                coco.component_subpath("setup", "table"),
                db.declare_table_target,
                "documents",
                sqlite.TableSchema(VectorRow, primary_key=["id"]),
            ).result()
            for row in vector_rows:
                table.declare_row(row=row)

        app = coco.App(
            coco.AppConfig(name="test_vector_insert", environment=coco_env),
            declare_vector_table,
        )

        # Insert data with vectors
        vector_rows.extend(
            [
                VectorRow(
                    id="doc1",
                    content="Hello world",
                    embedding=np.array([1.0, 2.0, 3.0, 4.0], dtype=np.float32),
                ),
                VectorRow(
                    id="doc2",
                    content="Goodbye world",
                    embedding=np.array([5.0, 6.0, 7.0, 8.0], dtype=np.float32),
                ),
            ]
        )
        app.update()

        assert table_exists(managed_conn, "documents")
        columns = get_table_columns(managed_conn, "documents")
        assert "embedding" in columns
        assert columns["embedding"] == "BLOB"

        # Read and verify
        data = read_table_data(managed_conn, "documents")
        assert len(data) == 2

        # Find doc1 and verify its vector
        doc1 = next(row for row in data if row["id"] == "doc1")
        assert doc1["content"] == "Hello world"
        vector = decode_vector(doc1["embedding"], VECTOR_DIM)
        assert vector == pytest.approx([1.0, 2.0, 3.0, 4.0])


@requires_sqlite_vec
def test_vector_update(
    sqlite_db_with_vec: tuple[sqlite.ManagedConnection, Path],
) -> None:
    """Test updating rows with vector columns."""
    managed_conn, _ = sqlite_db_with_vec

    vector_rows: list[VectorRow] = []

    with sqlite.register_db("vector_update_db", managed_conn) as db:

        def declare_vector_table() -> None:
            table = coco.mount_run(
                coco.component_subpath("setup", "table"),
                db.declare_table_target,
                "docs_update",
                sqlite.TableSchema(VectorRow, primary_key=["id"]),
            ).result()
            for row in vector_rows:
                table.declare_row(row=row)

        app = coco.App(
            coco.AppConfig(name="test_vector_update", environment=coco_env),
            declare_vector_table,
        )

        # Initial insert
        vector_rows.append(
            VectorRow(
                id="doc1",
                content="Original content",
                embedding=np.array([1.0, 1.0, 1.0, 1.0], dtype=np.float32),
            )
        )
        app.update()

        data = read_table_data(managed_conn, "docs_update")
        assert len(data) == 1
        doc1 = data[0]
        assert decode_vector(doc1["embedding"], VECTOR_DIM) == pytest.approx(
            [1.0, 1.0, 1.0, 1.0]
        )

        # Update the vector
        vector_rows.clear()
        vector_rows.append(
            VectorRow(
                id="doc1",
                content="Updated content",
                embedding=np.array([9.0, 9.0, 9.0, 9.0], dtype=np.float32),
            )
        )
        app.update()

        data = read_table_data(managed_conn, "docs_update")
        assert len(data) == 1
        doc1 = data[0]
        assert doc1["content"] == "Updated content"
        assert decode_vector(doc1["embedding"], VECTOR_DIM) == pytest.approx(
            [9.0, 9.0, 9.0, 9.0]
        )


@requires_sqlite_vec
def test_vector_with_explicit_schema(
    sqlite_db_with_vec: tuple[sqlite.ManagedConnection, Path],
) -> None:
    """Test vector columns using explicit VectorSchema in column_overrides."""
    managed_conn, _ = sqlite_db_with_vec

    # Define a schema with explicit VectorSchema override
    explicit_vector_schema = VectorSchema(dtype=np.dtype(np.float32), size=3)

    @dataclass
    class ExplicitVectorRow:
        id: str
        data: str
        vec: NDArray[np.float32]  # No annotation here, use column_overrides instead

    explicit_rows: list[ExplicitVectorRow] = []

    with sqlite.register_db("vector_explicit_db", managed_conn) as db:

        def declare_explicit_vector_table() -> None:
            table = coco.mount_run(
                coco.component_subpath("setup", "table"),
                db.declare_table_target,
                "explicit_vectors",
                sqlite.TableSchema(
                    ExplicitVectorRow,
                    primary_key=["id"],
                    column_overrides={"vec": explicit_vector_schema},
                ),
            ).result()
            for row in explicit_rows:
                table.declare_row(row=row)

        app = coco.App(
            coco.AppConfig(name="test_vector_explicit", environment=coco_env),
            declare_explicit_vector_table,
        )

        explicit_rows.extend(
            [
                ExplicitVectorRow(
                    id="v1",
                    data="test data",
                    vec=np.array([0.1, 0.2, 0.3], dtype=np.float32),
                ),
            ]
        )
        app.update()

        data = read_table_data(managed_conn, "explicit_vectors")
        assert len(data) == 1
        row = data[0]
        assert row["data"] == "test data"
        vector = decode_vector(row["vec"], 3)
        assert vector == pytest.approx([0.1, 0.2, 0.3])


@requires_sqlite_vec
def test_vector_delete(
    sqlite_db_with_vec: tuple[sqlite.ManagedConnection, Path],
) -> None:
    """Test deleting rows with vector columns."""
    managed_conn, _ = sqlite_db_with_vec

    vector_rows: list[VectorRow] = []

    with sqlite.register_db("vector_delete_db", managed_conn) as db:

        def declare_vector_table() -> None:
            table = coco.mount_run(
                coco.component_subpath("setup", "table"),
                db.declare_table_target,
                "docs_delete",
                sqlite.TableSchema(VectorRow, primary_key=["id"]),
            ).result()
            for row in vector_rows:
                table.declare_row(row=row)

        app = coco.App(
            coco.AppConfig(name="test_vector_delete", environment=coco_env),
            declare_vector_table,
        )

        # Insert multiple rows
        vector_rows.extend(
            [
                VectorRow(
                    id="doc1",
                    content="First",
                    embedding=np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32),
                ),
                VectorRow(
                    id="doc2",
                    content="Second",
                    embedding=np.array([0.0, 1.0, 0.0, 0.0], dtype=np.float32),
                ),
                VectorRow(
                    id="doc3",
                    content="Third",
                    embedding=np.array([0.0, 0.0, 1.0, 0.0], dtype=np.float32),
                ),
            ]
        )
        app.update()

        data = read_table_data(managed_conn, "docs_delete")
        assert len(data) == 3

        # Delete one row
        vector_rows.pop(1)  # Remove doc2
        app.update()

        data = read_table_data(managed_conn, "docs_delete")
        assert len(data) == 2
        ids = [row["id"] for row in data]
        assert "doc1" in ids
        assert "doc2" not in ids
        assert "doc3" in ids


@requires_sqlite_vec
def test_vector_without_extension_raises_error(
    sqlite_db: tuple[sqlite.ManagedConnection, Path],
) -> None:
    """Test that using vector columns without sqlite-vec loaded raises an error."""
    # Use the regular sqlite_db fixture, then close it and create a new one without vec
    original_managed_conn, db_path = sqlite_db
    original_managed_conn.close()

    # Create a new connection explicitly without loading sqlite-vec
    managed_conn = sqlite.connect(db_path, load_vec=False)

    vector_rows: list[VectorRow] = []

    with sqlite.register_db("vector_no_ext_db", managed_conn) as db:

        def declare_vector_table() -> None:
            table = coco.mount_run(
                coco.component_subpath("setup", "table"),
                db.declare_table_target,
                "vectors_no_ext",
                sqlite.TableSchema(VectorRow, primary_key=["id"]),
            ).result()
            for row in vector_rows:
                table.declare_row(row=row)

        app = coco.App(
            coco.AppConfig(name="test_vector_no_ext", environment=coco_env),
            declare_vector_table,
        )

        vector_rows.append(
            VectorRow(
                id="doc1",
                content="Test",
                embedding=np.array([1.0, 2.0, 3.0, 4.0], dtype=np.float32),
            )
        )

        with pytest.raises(RuntimeError, match="sqlite-vec extension is not loaded"):
            app.update()

    managed_conn.close()

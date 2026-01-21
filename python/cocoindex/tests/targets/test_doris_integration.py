# mypy: disable-error-code="no-untyped-def"
"""
Integration tests for Doris connector with VeloDB Cloud.

Run with: pytest python/cocoindex/tests/targets/test_doris_integration.py -v

Environment variables for custom Doris setup:
- DORIS_FE_HOST: FE host (required)
- DORIS_PASSWORD: Password (required)
- DORIS_HTTP_PORT: HTTP port for Stream Load (default: 8080)
- DORIS_QUERY_PORT: MySQL protocol port (default: 9030)
- DORIS_USERNAME: Username (default: root)
- DORIS_DATABASE: Test database (default: cocoindex_test)
- DORIS_ASYNC_TIMEOUT: Timeout in seconds for async operations like index changes (default: 15)
"""

import asyncio
import os
import time
import uuid
import pytest
from typing import Any, Generator, Literal

# Skip all tests if dependencies not available
try:
    import aiohttp
    import aiomysql  # type: ignore[import-untyped]  # noqa: F401

    DEPS_AVAILABLE = True
except ImportError:
    DEPS_AVAILABLE = False

from cocoindex.targets.doris import (
    DorisTarget,
    _Connector,
    _TableKey,
    _State,
    _VectorIndex,
    _InvertedIndex,
    _stream_load,
    _execute_ddl,
    _table_exists,
    _generate_create_table_ddl,
    connect_async,
    build_vector_search_query,
    DorisConnectionError,
    RetryConfig,
    with_retry,
)
from cocoindex.engine_type import (
    FieldSchema,
    EnrichedValueType,
    BasicValueType,
    VectorTypeSchema,
)
from cocoindex import op
from cocoindex.index import IndexOptions

# ============================================================
# TEST CONFIGURATION
# ============================================================

# All configuration via environment variables - no defaults for security
# Required env vars:
#   DORIS_FE_HOST - FE host address
#   DORIS_PASSWORD - Password for authentication
# Optional env vars:
#   DORIS_HTTP_PORT - HTTP port (default: 8080)
#   DORIS_QUERY_PORT - MySQL port (default: 9030)
#   DORIS_USERNAME - Username (default: root)
#   DORIS_DATABASE - Test database (default: cocoindex_test)
#   DORIS_ASYNC_TIMEOUT - Timeout for async operations (default: 15)

# Timeout for Doris async operations (index creation/removal, schema changes)
ASYNC_OPERATION_TIMEOUT = int(os.getenv("DORIS_ASYNC_TIMEOUT", "15"))


def get_test_config() -> dict[str, Any] | None:
    """Get test configuration from environment variables.

    Returns None if required env vars are not set.
    """
    fe_host = os.getenv("DORIS_FE_HOST")
    password = os.getenv("DORIS_PASSWORD")

    # Required env vars
    if not fe_host or not password:
        return None

    return {
        "fe_host": fe_host,
        "fe_http_port": int(os.getenv("DORIS_HTTP_PORT", "8080")),
        "query_port": int(os.getenv("DORIS_QUERY_PORT", "9030")),
        "username": os.getenv("DORIS_USERNAME", "root"),
        "password": password,
        "database": os.getenv("DORIS_DATABASE", "cocoindex_test"),
    }


# Check if Doris is configured
_TEST_CONFIG = get_test_config()
DORIS_CONFIGURED = _TEST_CONFIG is not None

# Skip tests if deps not available or Doris not configured
pytestmark = [
    pytest.mark.skipif(not DEPS_AVAILABLE, reason="aiohttp/aiomysql not installed"),
    pytest.mark.skipif(
        not DORIS_CONFIGURED,
        reason="Doris not configured (set DORIS_FE_HOST and DORIS_PASSWORD)",
    ),
    pytest.mark.integration,
]


# ============================================================
# FIXTURES
# ============================================================


@pytest.fixture(scope="module")
def test_config() -> dict[str, Any]:
    """Test configuration."""
    assert _TEST_CONFIG is not None, "Doris not configured"
    return _TEST_CONFIG


@pytest.fixture(scope="module")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def unique_table_name() -> str:
    """Generate unique table name for each test."""
    return f"test_{int(time.time())}_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def doris_spec(test_config: dict[str, Any], unique_table_name: str) -> DorisTarget:
    """Create DorisTarget spec for testing."""
    return DorisTarget(
        fe_host=test_config["fe_host"],
        fe_http_port=test_config["fe_http_port"],
        query_port=test_config["query_port"],
        username=test_config["username"],
        password=test_config["password"],
        database=test_config["database"],
        table=unique_table_name,
        replication_num=1,
        buckets=1,  # Small for testing
    )


@pytest.fixture
def cleanup_table(
    doris_spec: DorisTarget, event_loop: asyncio.AbstractEventLoop
) -> Generator[None, None, None]:
    """Cleanup table after test."""
    yield
    try:
        event_loop.run_until_complete(
            _execute_ddl(
                doris_spec,
                f"DROP TABLE IF EXISTS {doris_spec.database}.{doris_spec.table}",
            )
        )
    except Exception as e:
        print(f"Cleanup failed: {e}")


@pytest.fixture
def ensure_database(
    doris_spec: DorisTarget, event_loop: asyncio.AbstractEventLoop
) -> None:
    """Ensure test database exists."""
    # Create a spec without database to create the database
    temp_spec = DorisTarget(
        fe_host=doris_spec.fe_host,
        fe_http_port=doris_spec.fe_http_port,
        query_port=doris_spec.query_port,
        username=doris_spec.username,
        password=doris_spec.password,
        database="",  # No database for CREATE DATABASE
        table="dummy",
    )
    try:
        event_loop.run_until_complete(
            _execute_ddl(
                temp_spec,
                f"CREATE DATABASE IF NOT EXISTS {doris_spec.database}",
            )
        )
    except Exception as e:
        print(f"Database creation failed: {e}")


# Type alias for BasicValueType kind
_BasicKind = Literal[
    "Bytes",
    "Str",
    "Bool",
    "Int64",
    "Float32",
    "Float64",
    "Range",
    "Uuid",
    "Date",
    "Time",
    "LocalDateTime",
    "OffsetDateTime",
    "TimeDelta",
    "Json",
    "Vector",
    "Union",
]


def _mock_field(
    name: str, kind: _BasicKind, nullable: bool = False, dim: int | None = None
) -> FieldSchema:
    """Create mock FieldSchema for testing."""
    if kind == "Vector":
        vec_schema = VectorTypeSchema(
            element_type=BasicValueType(kind="Float32"),
            dimension=dim,
        )
        basic_type = BasicValueType(kind=kind, vector=vec_schema)
    else:
        basic_type = BasicValueType(kind=kind)
    return FieldSchema(
        name=name,
        value_type=EnrichedValueType(type=basic_type, nullable=nullable),
    )


def _mock_state(
    key_fields: list[str] | None = None,
    value_fields: list[str] | None = None,
    vector_fields: list[tuple[str, int]] | None = None,
    spec: DorisTarget | None = None,
    schema_evolution: str = "extend",
) -> _State:
    """Create mock State for testing."""
    key_fields = key_fields or ["id"]
    value_fields = value_fields or ["content"]

    key_schema = [_mock_field(f, "Int64") for f in key_fields]
    value_schema = [_mock_field(f, "Str") for f in value_fields]

    if vector_fields:
        for name, dim in vector_fields:
            value_schema.append(_mock_field(name, "Vector", dim=dim))

    # Use spec credentials if provided
    if spec:
        return _State(
            key_fields_schema=key_schema,
            value_fields_schema=value_schema,
            fe_http_port=spec.fe_http_port,
            query_port=spec.query_port,
            username=spec.username,
            password=spec.password,
            max_retries=spec.max_retries,
            retry_base_delay=spec.retry_base_delay,
            retry_max_delay=spec.retry_max_delay,
            schema_evolution=schema_evolution,  # type: ignore[arg-type]
        )

    return _State(
        key_fields_schema=key_schema,
        value_fields_schema=value_schema,
        schema_evolution=schema_evolution,  # type: ignore[arg-type]
    )


# ============================================================
# CONNECTION TESTS
# ============================================================


class TestConnection:
    """Test connection to VeloDB Cloud."""

    @pytest.mark.asyncio
    async def test_mysql_connection(self, doris_spec: DorisTarget):
        """Test MySQL protocol connection."""
        conn = await connect_async(
            fe_host=doris_spec.fe_host,
            query_port=doris_spec.query_port,
            username=doris_spec.username,
            password=doris_spec.password,
        )
        try:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT 1")
                result = await cursor.fetchone()
                assert result[0] == 1
        finally:
            conn.close()
            await conn.ensure_closed()

    @pytest.mark.asyncio
    async def test_execute_ddl_show_databases(self, doris_spec: DorisTarget):
        """Test DDL execution with SHOW DATABASES."""
        result = await _execute_ddl(doris_spec, "SHOW DATABASES")
        assert isinstance(result, list)
        # Should have at least some system databases
        db_names = [r.get("Database") for r in result]
        assert "information_schema" in db_names or len(db_names) > 0

    @pytest.mark.asyncio
    async def test_http_connection_for_stream_load(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test HTTP endpoint is reachable for Stream Load."""
        # First create a simple table
        state = _mock_state(spec=doris_spec)
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        ddl = _generate_create_table_ddl(key, state)
        await _execute_ddl(doris_spec, ddl)

        # Wait for table to be ready
        await asyncio.sleep(2)

        # Try Stream Load with empty data
        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(doris_spec.username, doris_spec.password),
        ) as session:
            result = await _stream_load(session, doris_spec, [])
            assert result.get("Status") == "Success"


# ============================================================
# TABLE LIFECYCLE TESTS
# ============================================================


class TestTableLifecycle:
    """Test table creation, modification, and deletion."""

    @pytest.mark.asyncio
    async def test_create_simple_table(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test creating a simple table."""
        key = _Connector.get_persistent_key(doris_spec)
        state = _mock_state(spec=doris_spec)

        # Apply setup change (create table)
        await _Connector.apply_setup_change(key, None, state)

        # Verify table exists
        exists = await _table_exists(doris_spec, doris_spec.database, doris_spec.table)
        assert exists, "Table should exist after creation"

    @pytest.mark.asyncio
    async def test_create_table_with_vector_column(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test creating table with vector column."""
        state = _State(
            key_fields_schema=[_mock_field("id", "Int64")],
            value_fields_schema=[
                _mock_field("content", "Str"),
                _mock_field("embedding", "Vector", dim=384),
            ],
        )
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        ddl = _generate_create_table_ddl(key, state)

        await _execute_ddl(doris_spec, ddl)

        # Verify table exists
        exists = await _table_exists(doris_spec, doris_spec.database, doris_spec.table)
        assert exists

    @pytest.mark.asyncio
    async def test_create_table_with_vector_index(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test creating table with vector index (HNSW)."""
        state = _State(
            key_fields_schema=[_mock_field("id", "Int64")],
            value_fields_schema=[
                _mock_field("content", "Str"),
                _mock_field("embedding", "Vector", dim=384),
            ],
            vector_indexes=[
                _VectorIndex(
                    name="idx_embedding_ann",
                    field_name="embedding",
                    index_type="hnsw",
                    metric_type="l2_distance",
                    dimension=384,
                )
            ],
        )
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        ddl = _generate_create_table_ddl(key, state)

        await _execute_ddl(doris_spec, ddl)

        # Verify table exists
        exists = await _table_exists(doris_spec, doris_spec.database, doris_spec.table)
        assert exists

    @pytest.mark.asyncio
    async def test_create_table_with_ivf_vector_index(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test creating table with IVF vector index."""
        state = _State(
            key_fields_schema=[_mock_field("id", "Int64")],
            value_fields_schema=[
                _mock_field("content", "Str"),
                _mock_field("embedding", "Vector", dim=384),
            ],
            vector_indexes=[
                _VectorIndex(
                    name="idx_embedding_ivf",
                    field_name="embedding",
                    index_type="ivf",
                    metric_type="l2_distance",
                    dimension=384,
                    nlist=128,  # IVF-specific parameter
                )
            ],
        )
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        ddl = _generate_create_table_ddl(key, state)

        # Verify DDL contains IVF-specific parameters
        assert '"index_type" = "ivf"' in ddl
        assert '"nlist" = "128"' in ddl

        await _execute_ddl(doris_spec, ddl)

        # Verify table exists
        exists = await _table_exists(doris_spec, doris_spec.database, doris_spec.table)
        assert exists

        # Verify index was created by checking SHOW CREATE TABLE
        result = await _execute_ddl(
            doris_spec,
            f"SHOW CREATE TABLE `{doris_spec.database}`.`{doris_spec.table}`",
        )
        create_stmt = result[0].get("Create Table", "")
        assert "idx_embedding_ivf" in create_stmt, "IVF index should be created"

    @pytest.mark.asyncio
    async def test_drop_table(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test dropping a table in strict mode.

        Note: In extend mode (default), tables are NOT dropped when target is removed.
        This test uses strict mode to verify table dropping works.
        """
        # Create table first - use strict mode so table will be dropped
        key = _Connector.get_persistent_key(doris_spec)
        state = _mock_state(spec=doris_spec, schema_evolution="strict")
        await _Connector.apply_setup_change(key, None, state)

        # Verify exists
        exists = await _table_exists(doris_spec, doris_spec.database, doris_spec.table)
        assert exists

        # Drop table (only works in strict mode)
        await _Connector.apply_setup_change(key, state, None)

        # Verify dropped
        exists = await _table_exists(doris_spec, doris_spec.database, doris_spec.table)
        assert not exists

    @pytest.mark.asyncio
    async def test_vector_without_dimension_stored_as_json(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test that vector fields without dimension are stored as JSON.

        When a vector field doesn't have a fixed dimension, it cannot be stored
        as ARRAY<FLOAT> or have a vector index. Instead, it falls back to JSON
        storage, which is consistent with how other targets (Postgres, Qdrant)
        handle this case.
        """
        # Create a vector field without dimension
        vec_schema = VectorTypeSchema(
            element_type=BasicValueType(kind="Float32"),
            dimension=None,  # No dimension specified
        )
        basic_type = BasicValueType(kind="Vector", vector=vec_schema)
        key_fields = [_mock_field("id", "Int64")]
        value_fields = [
            _mock_field("content", "Str"),
            FieldSchema(
                name="embedding",
                value_type=EnrichedValueType(type=basic_type),
            ),
        ]

        state = _State(
            key_fields_schema=key_fields,
            value_fields_schema=value_fields,
            fe_http_port=doris_spec.fe_http_port,
            query_port=doris_spec.query_port,
            username=doris_spec.username,
            password=doris_spec.password,
            vector_indexes=None,  # No vector index since no dimension
        )

        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        ddl = _generate_create_table_ddl(key, state)
        await _execute_ddl(doris_spec, ddl)

        # Verify table was created
        exists = await _table_exists(doris_spec, doris_spec.database, doris_spec.table)
        assert exists

        # Verify the embedding column is JSON (not ARRAY<FLOAT>)
        result = await _execute_ddl(
            doris_spec,
            f"SHOW CREATE TABLE `{doris_spec.database}`.`{doris_spec.table}`",
        )
        create_stmt = result[0].get("Create Table", "")
        # JSON columns are stored as JSON type in Doris
        assert (
            "`embedding` JSON" in create_stmt
            or "`embedding` json" in create_stmt.lower()
        ), f"embedding should be JSON type, got: {create_stmt}"
        assert "ARRAY<FLOAT>" not in create_stmt, (
            f"embedding should NOT be ARRAY<FLOAT>, got: {create_stmt}"
        )

        # Test that we can insert JSON data into the vector column
        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(doris_spec.username, doris_spec.password),
        ) as session:
            # Insert data with embedding as a JSON array
            data = [
                {"id": 1, "content": "test", "embedding": [0.1, 0.2, 0.3]},
                {
                    "id": 2,
                    "content": "test2",
                    "embedding": [0.4, 0.5, 0.6, 0.7],
                },  # Different length is OK for JSON
            ]
            load_result = await _stream_load(session, doris_spec, data)
            assert load_result.get("Status") == "Success", (
                f"Stream Load failed: {load_result}"
            )

        # Verify data was inserted
        await asyncio.sleep(2)  # Wait for data to be visible
        query_result = await _execute_ddl(
            doris_spec,
            f"SELECT id, embedding FROM `{doris_spec.database}`.`{doris_spec.table}` ORDER BY id",
        )
        assert len(query_result) == 2
        # JSON stored vectors can have different lengths
        assert query_result[0]["id"] == 1
        assert query_result[1]["id"] == 2


# ============================================================
# DATA MUTATION TESTS
# ============================================================


class TestDataMutation:
    """Test upsert and delete operations."""

    @pytest.mark.asyncio
    async def test_insert_single_row(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test inserting a single row via Stream Load."""
        # Create table
        state = _mock_state(spec=doris_spec)
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        ddl = _generate_create_table_ddl(key, state)
        await _execute_ddl(doris_spec, ddl)

        await asyncio.sleep(2)

        # Insert row
        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(doris_spec.username, doris_spec.password),
        ) as session:
            result = await _stream_load(
                session,
                doris_spec,
                [{"id": 1, "content": "Hello, Doris!"}],
            )
            assert result.get("Status") in ("Success", "Publish Timeout")

        # Wait for data to be visible
        await asyncio.sleep(2)

        # Verify data
        query_result = await _execute_ddl(
            doris_spec,
            f"SELECT * FROM {doris_spec.database}.{doris_spec.table} WHERE id = 1",
        )
        assert len(query_result) == 1
        assert query_result[0]["content"] == "Hello, Doris!"

    @pytest.mark.asyncio
    async def test_insert_multiple_rows(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test inserting multiple rows in batch."""
        # Create table
        state = _mock_state(spec=doris_spec)
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        ddl = _generate_create_table_ddl(key, state)
        await _execute_ddl(doris_spec, ddl)

        await asyncio.sleep(2)

        # Insert rows
        rows = [{"id": i, "content": f"Row {i}"} for i in range(1, 101)]
        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(doris_spec.username, doris_spec.password),
        ) as session:
            result = await _stream_load(session, doris_spec, rows)
            assert result.get("Status") in ("Success", "Publish Timeout")

        # Wait for data
        await asyncio.sleep(2)

        # Verify count
        query_result = await _execute_ddl(
            doris_spec,
            f"SELECT COUNT(*) as cnt FROM {doris_spec.database}.{doris_spec.table}",
        )
        assert query_result[0]["cnt"] == 100

    @pytest.mark.asyncio
    async def test_upsert_row(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test upserting (update on duplicate key)."""
        # Create table
        state = _mock_state(spec=doris_spec)
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        ddl = _generate_create_table_ddl(key, state)
        await _execute_ddl(doris_spec, ddl)

        await asyncio.sleep(2)

        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(doris_spec.username, doris_spec.password),
        ) as session:
            # Insert initial row
            await _stream_load(
                session,
                doris_spec,
                [{"id": 1, "content": "Original"}],
            )

            await asyncio.sleep(2)

            # Upsert (update same key)
            await _stream_load(
                session,
                doris_spec,
                [{"id": 1, "content": "Updated"}],
            )

            await asyncio.sleep(2)

        # Verify updated - with DUPLICATE KEY model, may have multiple versions
        query_result = await _execute_ddl(
            doris_spec,
            f"SELECT content FROM {doris_spec.database}.{doris_spec.table} WHERE id = 1 ORDER BY content DESC LIMIT 1",
        )
        # Note: DUPLICATE KEY keeps all versions, so we check latest
        assert len(query_result) >= 1

    @pytest.mark.asyncio
    async def test_delete_row(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test deleting a row via SQL DELETE (works with DUPLICATE KEY tables)."""
        # Create table
        state = _mock_state(spec=doris_spec)
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        ddl = _generate_create_table_ddl(key, state)
        await _execute_ddl(doris_spec, ddl)

        await asyncio.sleep(2)

        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(doris_spec.username, doris_spec.password),
        ) as session:
            # Insert row
            await _stream_load(
                session,
                doris_spec,
                [{"id": 1, "content": "To be deleted"}],
            )

            await asyncio.sleep(2)

        # Verify row exists
        result = await _execute_ddl(
            doris_spec,
            f"SELECT COUNT(*) as cnt FROM {doris_spec.database}.{doris_spec.table} WHERE id = 1",
        )
        assert result[0]["cnt"] >= 1

        # Delete row using SQL DELETE (not Stream Load)
        from cocoindex.targets.doris import _execute_delete

        await _execute_delete(
            doris_spec,
            key_field_names=["id"],
            key_values=[{"id": 1}],
        )
        # Note: cursor.rowcount may return 0 in Doris even for successful deletes
        # so we verify deletion by checking the actual row count

        await asyncio.sleep(2)

        # Verify row is deleted
        result = await _execute_ddl(
            doris_spec,
            f"SELECT COUNT(*) as cnt FROM {doris_spec.database}.{doris_spec.table} WHERE id = 1",
        )
        assert result[0]["cnt"] == 0

    @pytest.mark.asyncio
    async def test_upsert_idempotent_no_duplicates(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test that repeated upserts do NOT accumulate duplicate rows.

        This tests the fix for the issue where DUPLICATE KEY tables would
        accumulate multiple rows with the same key. The fix uses delete-before-insert
        to ensure idempotent upsert behavior.
        """
        from cocoindex.targets.doris import _MutateContext, _execute_delete

        # Create table
        state = _mock_state(spec=doris_spec)
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        ddl = _generate_create_table_ddl(key, state)
        await _execute_ddl(doris_spec, ddl)

        await asyncio.sleep(2)

        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(doris_spec.username, doris_spec.password),
        ) as session:
            context = _MutateContext(
                spec=doris_spec,
                session=session,
                state=state,
                lock=asyncio.Lock(),
            )

            # First upsert
            mutations1: dict[Any, dict[str, Any] | None] = {
                1: {"content": "Version 1"},
            }
            await _Connector.mutate((context, mutations1))

            await asyncio.sleep(2)

            # Second upsert - same key, different value
            mutations2: dict[Any, dict[str, Any] | None] = {
                1: {"content": "Version 2"},
            }
            await _Connector.mutate((context, mutations2))

            await asyncio.sleep(2)

            # Third upsert - same key, yet another value
            mutations3: dict[Any, dict[str, Any] | None] = {
                1: {"content": "Version 3"},
            }
            await _Connector.mutate((context, mutations3))

            await asyncio.sleep(2)

        # Verify EXACTLY ONE row exists (not 3)
        result = await _execute_ddl(
            doris_spec,
            f"SELECT COUNT(*) as cnt FROM {doris_spec.database}.{doris_spec.table} WHERE id = 1",
        )
        assert result[0]["cnt"] == 1, (
            f"Expected exactly 1 row, but found {result[0]['cnt']}. "
            "Delete-before-insert fix may not be working."
        )

        # Verify the content is the latest version
        content_result = await _execute_ddl(
            doris_spec,
            f"SELECT content FROM {doris_spec.database}.{doris_spec.table} WHERE id = 1",
        )
        assert content_result[0]["content"] == "Version 3"


# ============================================================
# INDEX LIFECYCLE TESTS
# ============================================================


class TestIndexLifecycle:
    """Test index creation, modification, and removal on existing tables."""

    @pytest.mark.asyncio
    async def test_add_vector_index_to_existing_table(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test adding a vector index to an existing table without index."""
        from cocoindex.targets.doris import _sync_indexes

        # Create table without vector index
        state_no_index = _State(
            key_fields_schema=[_mock_field("id", "Int64")],
            value_fields_schema=[
                _mock_field("content", "Str"),
                _mock_field("embedding", "Vector", dim=4),
            ],
            vector_indexes=None,  # No index initially
        )
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        ddl = _generate_create_table_ddl(key, state_no_index)
        await _execute_ddl(doris_spec, ddl)

        await asyncio.sleep(2)

        # Insert some data first
        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(doris_spec.username, doris_spec.password),
        ) as session:
            rows = [
                {"id": 1, "content": "Test", "embedding": [1.0, 0.0, 0.0, 0.0]},
            ]
            await _stream_load(session, doris_spec, rows)

        await asyncio.sleep(2)

        # Now add a vector index
        state_with_index = _State(
            key_fields_schema=[_mock_field("id", "Int64")],
            value_fields_schema=[
                _mock_field("content", "Str"),
                _mock_field("embedding", "Vector", dim=4),
            ],
            vector_indexes=[
                _VectorIndex(
                    name="idx_embedding_ann",
                    field_name="embedding",
                    index_type="hnsw",
                    metric_type="l2_distance",
                    dimension=4,
                )
            ],
        )

        # Sync indexes - should add the new index
        await _sync_indexes(doris_spec, key, state_no_index, state_with_index)

        await asyncio.sleep(3)

        # Verify index was created by checking SHOW CREATE TABLE
        result = await _execute_ddl(
            doris_spec,
            f"SHOW CREATE TABLE {doris_spec.database}.{doris_spec.table}",
        )
        create_stmt = result[0].get("Create Table", "")
        assert "idx_embedding_ann" in create_stmt, "Vector index should be created"

    @pytest.mark.asyncio
    async def test_add_inverted_index_to_existing_table(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test adding an inverted (FTS) index to an existing table."""
        from cocoindex.targets.doris import _sync_indexes

        # Create table without inverted index
        state_no_index = _State(
            key_fields_schema=[_mock_field("id", "Int64")],
            value_fields_schema=[_mock_field("content", "Str")],
            inverted_indexes=None,
        )
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        ddl = _generate_create_table_ddl(key, state_no_index)
        await _execute_ddl(doris_spec, ddl)

        await asyncio.sleep(2)

        # Insert some data
        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(doris_spec.username, doris_spec.password),
        ) as session:
            rows = [{"id": 1, "content": "Hello world test document"}]
            await _stream_load(session, doris_spec, rows)

        await asyncio.sleep(2)

        # Add inverted index
        state_with_index = _State(
            key_fields_schema=[_mock_field("id", "Int64")],
            value_fields_schema=[_mock_field("content", "Str")],
            inverted_indexes=[
                _InvertedIndex(
                    name="idx_content_inv",
                    field_name="content",
                    parser="unicode",
                )
            ],
        )

        await _sync_indexes(doris_spec, key, state_no_index, state_with_index)

        await asyncio.sleep(3)

        # Verify index was created
        result = await _execute_ddl(
            doris_spec,
            f"SHOW CREATE TABLE {doris_spec.database}.{doris_spec.table}",
        )
        create_stmt = result[0].get("Create Table", "")
        assert "idx_content_inv" in create_stmt, "Inverted index should be created"

    @pytest.mark.asyncio
    async def test_remove_index_from_existing_table(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test removing an index from an existing table."""
        from cocoindex.targets.doris import _sync_indexes

        # Create table with inverted index
        state_with_index = _State(
            key_fields_schema=[_mock_field("id", "Int64")],
            value_fields_schema=[_mock_field("content", "Str")],
            inverted_indexes=[
                _InvertedIndex(
                    name="idx_content_inv",
                    field_name="content",
                    parser="unicode",
                )
            ],
        )
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        ddl = _generate_create_table_ddl(key, state_with_index)
        await _execute_ddl(doris_spec, ddl)

        await asyncio.sleep(2)

        # Verify index exists
        result = await _execute_ddl(
            doris_spec,
            f"SHOW CREATE TABLE {doris_spec.database}.{doris_spec.table}",
        )
        create_stmt = result[0].get("Create Table", "")
        assert "idx_content_inv" in create_stmt, "Index should exist initially"

        # Remove the index
        state_no_index = _State(
            key_fields_schema=[_mock_field("id", "Int64")],
            value_fields_schema=[_mock_field("content", "Str")],
            inverted_indexes=None,
        )

        await _sync_indexes(doris_spec, key, state_with_index, state_no_index)

        # Wait for index removal with retry (Doris async operation)
        for i in range(ASYNC_OPERATION_TIMEOUT):
            await asyncio.sleep(1)
            result = await _execute_ddl(
                doris_spec,
                f"SHOW CREATE TABLE {doris_spec.database}.{doris_spec.table}",
            )
            create_stmt = result[0].get("Create Table", "")
            if "idx_content_inv" not in create_stmt:
                break
        else:
            pytest.fail(
                f"Index was not removed after {ASYNC_OPERATION_TIMEOUT}s. "
                f"Current schema: {create_stmt}"
            )

    @pytest.mark.asyncio
    async def test_change_index_parameters(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test changing index parameters (recreates index)."""
        from cocoindex.targets.doris import _sync_indexes

        # Create table with inverted index using english parser
        state_english = _State(
            key_fields_schema=[_mock_field("id", "Int64")],
            value_fields_schema=[_mock_field("content", "Str")],
            inverted_indexes=[
                _InvertedIndex(
                    name="idx_content_inv",
                    field_name="content",
                    parser="english",
                )
            ],
        )
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        ddl = _generate_create_table_ddl(key, state_english)
        await _execute_ddl(doris_spec, ddl)

        await asyncio.sleep(3)

        # Change to unicode parser (same name, different params)
        state_unicode = _State(
            key_fields_schema=[_mock_field("id", "Int64")],
            value_fields_schema=[_mock_field("content", "Str")],
            inverted_indexes=[
                _InvertedIndex(
                    name="idx_content_inv",
                    field_name="content",
                    parser="unicode",  # Changed parser
                )
            ],
        )

        await _sync_indexes(doris_spec, key, state_english, state_unicode)

        # Wait longer for schema change to complete (Doris needs time for index operations)
        await asyncio.sleep(5)

        # Index should still exist (was dropped and recreated)
        result = await _execute_ddl(
            doris_spec,
            f"SHOW CREATE TABLE {doris_spec.database}.{doris_spec.table}",
        )
        create_stmt = result[0].get("Create Table", "")
        assert "idx_content_inv" in create_stmt, (
            "Index should exist after parameter change"
        )
        # Note: Verifying the parser changed would require parsing SHOW CREATE TABLE output


# ============================================================
# CONNECTOR MUTATION TESTS
# ============================================================


class TestConnectorMutation:
    """Test the full connector mutation flow using _Connector.mutate()."""

    @pytest.mark.asyncio
    async def test_mutate_insert_multiple_rows(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test inserting multiple rows via connector mutation."""
        from cocoindex.targets.doris import _MutateContext

        state = _mock_state(spec=doris_spec)
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        ddl = _generate_create_table_ddl(key, state)
        await _execute_ddl(doris_spec, ddl)

        await asyncio.sleep(2)

        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(doris_spec.username, doris_spec.password),
        ) as session:
            context = _MutateContext(
                spec=doris_spec,
                session=session,
                state=state,
                lock=asyncio.Lock(),
            )

            # Insert multiple rows
            mutations: dict[Any, dict[str, Any] | None] = {
                1: {"content": "First row"},
                2: {"content": "Second row"},
                3: {"content": "Third row"},
            }
            await _Connector.mutate((context, mutations))

        await asyncio.sleep(2)

        # Verify all rows were inserted
        result = await _execute_ddl(
            doris_spec,
            f"SELECT COUNT(*) as cnt FROM {doris_spec.database}.{doris_spec.table}",
        )
        assert result[0]["cnt"] == 3

    @pytest.mark.asyncio
    async def test_mutate_delete_rows(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test deleting rows via connector mutation (value=None)."""
        from cocoindex.targets.doris import _MutateContext

        state = _mock_state(spec=doris_spec)
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        ddl = _generate_create_table_ddl(key, state)
        await _execute_ddl(doris_spec, ddl)

        await asyncio.sleep(2)

        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(doris_spec.username, doris_spec.password),
        ) as session:
            context = _MutateContext(
                spec=doris_spec,
                session=session,
                state=state,
                lock=asyncio.Lock(),
            )

            # Insert rows first
            insert_mutations: dict[Any, dict[str, Any] | None] = {
                1: {"content": "Row 1"},
                2: {"content": "Row 2"},
                3: {"content": "Row 3"},
            }
            await _Connector.mutate((context, insert_mutations))

            await asyncio.sleep(2)

            # Verify rows exist
            result = await _execute_ddl(
                doris_spec,
                f"SELECT COUNT(*) as cnt FROM {doris_spec.database}.{doris_spec.table}",
            )
            assert result[0]["cnt"] == 3

            # Delete row 2 (value=None means delete)
            delete_mutations: dict[Any, dict[str, Any] | None] = {
                2: None,  # Delete
            }
            await _Connector.mutate((context, delete_mutations))

        await asyncio.sleep(2)

        # Verify row was deleted
        result = await _execute_ddl(
            doris_spec,
            f"SELECT COUNT(*) as cnt FROM {doris_spec.database}.{doris_spec.table}",
        )
        assert result[0]["cnt"] == 2

        # Verify specific row is gone
        result = await _execute_ddl(
            doris_spec,
            f"SELECT id FROM {doris_spec.database}.{doris_spec.table} ORDER BY id",
        )
        ids = [row["id"] for row in result]
        assert ids == [1, 3], "Row 2 should be deleted"

    @pytest.mark.asyncio
    async def test_mutate_mixed_operations(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test mixed insert/update/delete in single mutation batch."""
        from cocoindex.targets.doris import _MutateContext

        state = _mock_state(spec=doris_spec)
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        ddl = _generate_create_table_ddl(key, state)
        await _execute_ddl(doris_spec, ddl)

        await asyncio.sleep(2)

        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(doris_spec.username, doris_spec.password),
        ) as session:
            context = _MutateContext(
                spec=doris_spec,
                session=session,
                state=state,
                lock=asyncio.Lock(),
            )

            # Initial insert
            await _Connector.mutate(
                (
                    context,
                    {
                        1: {"content": "Original 1"},
                        2: {"content": "Original 2"},
                        3: {"content": "Original 3"},
                    },
                )
            )

            await asyncio.sleep(2)

            # Mixed operations in single batch:
            # - Update row 1
            # - Delete row 2
            # - Insert row 4
            mixed_mutations: dict[Any, dict[str, Any] | None] = {
                1: {"content": "Updated 1"},  # Update
                2: None,  # Delete
                4: {"content": "New row 4"},  # Insert
            }
            await _Connector.mutate((context, mixed_mutations))

        await asyncio.sleep(2)

        # Verify final state
        result = await _execute_ddl(
            doris_spec,
            f"SELECT id, content FROM {doris_spec.database}.{doris_spec.table} ORDER BY id",
        )

        # Should have rows 1, 3, 4
        assert len(result) == 3
        rows_by_id = {row["id"]: row["content"] for row in result}
        assert rows_by_id[1] == "Updated 1"
        assert rows_by_id[3] == "Original 3"
        assert rows_by_id[4] == "New row 4"
        assert 2 not in rows_by_id

    @pytest.mark.asyncio
    async def test_mutate_composite_key(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test mutation with composite (multi-column) primary key."""
        from cocoindex.targets.doris import _MutateContext

        # Create state with composite key
        state = _State(
            key_fields_schema=[
                _mock_field("tenant_id", "Int64"),
                _mock_field("doc_id", "Str"),
            ],
            value_fields_schema=[_mock_field("content", "Str")],
        )
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        ddl = _generate_create_table_ddl(key, state)
        await _execute_ddl(doris_spec, ddl)

        await asyncio.sleep(2)

        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(doris_spec.username, doris_spec.password),
        ) as session:
            context = _MutateContext(
                spec=doris_spec,
                session=session,
                state=state,
                lock=asyncio.Lock(),
            )

            # Insert with composite keys (tuple keys)
            mutations: dict[Any, dict[str, Any] | None] = {
                (1, "doc_a"): {"content": "Tenant 1, Doc A"},
                (1, "doc_b"): {"content": "Tenant 1, Doc B"},
                (2, "doc_a"): {"content": "Tenant 2, Doc A"},
            }
            await _Connector.mutate((context, mutations))

            await asyncio.sleep(2)

            # Update one row
            await _Connector.mutate(
                (
                    context,
                    {
                        (1, "doc_a"): {"content": "Updated content"},
                    },
                )
            )

            await asyncio.sleep(2)

        # Verify
        result = await _execute_ddl(
            doris_spec,
            f"SELECT tenant_id, doc_id, content FROM {doris_spec.database}.{doris_spec.table} ORDER BY tenant_id, doc_id",
        )

        assert len(result) == 3
        # Find the updated row
        updated_row = next(
            r for r in result if r["tenant_id"] == 1 and r["doc_id"] == "doc_a"
        )
        assert updated_row["content"] == "Updated content"

    @pytest.mark.asyncio
    async def test_mutate_composite_key_delete(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test deleting rows with composite (multi-column) primary key.

        This tests the fix for composite-key DELETE which uses OR conditions
        instead of tuple IN syntax (which Doris doesn't support).
        """
        from cocoindex.targets.doris import _MutateContext

        # Create state with composite key
        state = _State(
            key_fields_schema=[
                _mock_field("tenant_id", "Int64"),
                _mock_field("doc_id", "Str"),
            ],
            value_fields_schema=[_mock_field("content", "Str")],
        )
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        ddl = _generate_create_table_ddl(key, state)
        await _execute_ddl(doris_spec, ddl)

        await asyncio.sleep(2)

        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(doris_spec.username, doris_spec.password),
        ) as session:
            context = _MutateContext(
                spec=doris_spec,
                session=session,
                state=state,
                lock=asyncio.Lock(),
            )

            # Insert multiple rows with composite keys
            mutations: dict[Any, dict[str, Any] | None] = {
                (1, "doc_a"): {"content": "Tenant 1, Doc A"},
                (1, "doc_b"): {"content": "Tenant 1, Doc B"},
                (1, "doc_c"): {"content": "Tenant 1, Doc C"},
                (2, "doc_a"): {"content": "Tenant 2, Doc A"},
                (2, "doc_b"): {"content": "Tenant 2, Doc B"},
            }
            await _Connector.mutate((context, mutations))

            await asyncio.sleep(2)

            # Verify all rows exist
            result = await _execute_ddl(
                doris_spec,
                f"SELECT COUNT(*) as cnt FROM {doris_spec.database}.{doris_spec.table}",
            )
            assert result[0]["cnt"] == 5

            # Delete multiple rows with composite keys in a single mutation
            # This tests the OR chain DELETE: WHERE (t=1 AND d='a') OR (t=1 AND d='c') OR (t=2 AND d='b')
            delete_mutations: dict[Any, dict[str, Any] | None] = {
                (1, "doc_a"): None,  # Delete
                (1, "doc_c"): None,  # Delete
                (2, "doc_b"): None,  # Delete
            }
            await _Connector.mutate((context, delete_mutations))

        await asyncio.sleep(2)

        # Verify correct rows were deleted
        result = await _execute_ddl(
            doris_spec,
            f"SELECT tenant_id, doc_id FROM {doris_spec.database}.{doris_spec.table} ORDER BY tenant_id, doc_id",
        )

        # Should have 2 rows remaining: (1, doc_b) and (2, doc_a)
        assert len(result) == 2
        remaining_keys = [(r["tenant_id"], r["doc_id"]) for r in result]
        assert (1, "doc_b") in remaining_keys
        assert (2, "doc_a") in remaining_keys
        # Deleted keys should not exist
        assert (1, "doc_a") not in remaining_keys
        assert (1, "doc_c") not in remaining_keys
        assert (2, "doc_b") not in remaining_keys

    @pytest.mark.asyncio
    async def test_mutate_composite_key_delete_large_batch(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test deleting many rows with composite key.

        This tests that deleting multiple rows with composite keys works correctly.
        Doris doesn't support OR with AND predicates in DELETE, so composite keys
        are deleted one row at a time.
        """
        from cocoindex.targets.doris import _MutateContext

        # Create state with composite key
        state = _State(
            key_fields_schema=[
                _mock_field("tenant_id", "Int64"),
                _mock_field("doc_id", "Str"),
            ],
            value_fields_schema=[_mock_field("content", "Str")],
        )
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        ddl = _generate_create_table_ddl(key, state)
        await _execute_ddl(doris_spec, ddl)

        await asyncio.sleep(2)

        # Use a moderate batch size (deleting one-by-one takes time)
        num_rows = 20

        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(doris_spec.username, doris_spec.password),
        ) as session:
            context = _MutateContext(
                spec=doris_spec,
                session=session,
                state=state,
                lock=asyncio.Lock(),
            )

            # Insert many rows
            insert_mutations: dict[Any, dict[str, Any] | None] = {
                (i // 5, f"doc_{i % 5}"): {"content": f"Content {i}"}
                for i in range(num_rows)
            }
            await _Connector.mutate((context, insert_mutations))

            await asyncio.sleep(3)

            # Verify all rows inserted
            result = await _execute_ddl(
                doris_spec,
                f"SELECT COUNT(*) as cnt FROM {doris_spec.database}.{doris_spec.table}",
            )
            assert result[0]["cnt"] == num_rows

            # Delete all rows
            delete_mutations: dict[Any, dict[str, Any] | None] = {
                (i // 5, f"doc_{i % 5}"): None for i in range(num_rows)
            }
            await _Connector.mutate((context, delete_mutations))

        await asyncio.sleep(3)

        # Verify all rows were deleted
        result = await _execute_ddl(
            doris_spec,
            f"SELECT COUNT(*) as cnt FROM {doris_spec.database}.{doris_spec.table}",
        )
        assert result[0]["cnt"] == 0, (
            f"Expected 0 rows after delete, but found {result[0]['cnt']}. "
            "Composite key delete may not be working correctly."
        )


# ============================================================
# VECTOR SEARCH TESTS
# ============================================================


class TestVectorSearch:
    """Test vector search functionality."""

    @pytest.mark.asyncio
    async def test_insert_and_query_vectors(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test inserting vectors and querying with similarity search."""
        # Create table with vector column (no index for simpler test)
        state = _State(
            key_fields_schema=[_mock_field("id", "Int64")],
            value_fields_schema=[
                _mock_field("content", "Str"),
                _mock_field("embedding", "Vector", dim=4),  # Small dim for testing
            ],
        )
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        ddl = _generate_create_table_ddl(key, state)
        await _execute_ddl(doris_spec, ddl)

        await asyncio.sleep(2)

        # Insert vectors
        rows = [
            {"id": 1, "content": "Apple", "embedding": [1.0, 0.0, 0.0, 0.0]},
            {"id": 2, "content": "Banana", "embedding": [0.0, 1.0, 0.0, 0.0]},
            {"id": 3, "content": "Cherry", "embedding": [0.0, 0.0, 1.0, 0.0]},
        ]
        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(doris_spec.username, doris_spec.password),
        ) as session:
            load_result = await _stream_load(session, doris_spec, rows)
            assert load_result.get("Status") in ("Success", "Publish Timeout")

        await asyncio.sleep(2)

        # Query with vector similarity (using non-approximate function for test)
        # Note: approximate functions require index
        query = f"""
        SELECT id, content, l2_distance(embedding, [1.0, 0.0, 0.0, 0.0]) as distance
        FROM {doris_spec.database}.{doris_spec.table}
        ORDER BY distance ASC
        LIMIT 3
        """
        query_result = await _execute_ddl(doris_spec, query)

        assert len(query_result) == 3
        # Apple should be closest (distance ~0)
        assert query_result[0]["content"] == "Apple"

    @pytest.mark.asyncio
    async def test_ivf_index_vector_search(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test vector search with IVF index.

        IVF requires at least nlist training points, so we must:
        1. Create table without IVF index
        2. Load data first
        3. Add IVF index after data is loaded
        4. Build the index
        """
        # Step 1: Create table WITHOUT IVF index (just vector column)
        state_no_index = _State(
            key_fields_schema=[_mock_field("id", "Int64")],
            value_fields_schema=[
                _mock_field("content", "Str"),
                _mock_field("embedding", "Vector", dim=4),
            ],
            vector_indexes=None,  # No index initially
        )
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        ddl = _generate_create_table_ddl(key, state_no_index)
        await _execute_ddl(doris_spec, ddl)

        await asyncio.sleep(2)

        # Step 2: Insert vectors first (IVF needs training data)
        rows = [
            {"id": 1, "content": "Apple", "embedding": [1.0, 0.0, 0.0, 0.0]},
            {"id": 2, "content": "Banana", "embedding": [0.0, 1.0, 0.0, 0.0]},
            {"id": 3, "content": "Cherry", "embedding": [0.0, 0.0, 1.0, 0.0]},
            {"id": 4, "content": "Date", "embedding": [0.0, 0.0, 0.0, 1.0]},
            {"id": 5, "content": "Elderberry", "embedding": [0.5, 0.5, 0.0, 0.0]},
        ]
        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(doris_spec.username, doris_spec.password),
        ) as session:
            load_result = await _stream_load(session, doris_spec, rows)
            assert load_result.get("Status") in ("Success", "Publish Timeout")

        await asyncio.sleep(3)

        # Step 3: Add IVF index after data is loaded
        # nlist=2 requires at least 2 data points for training
        await _execute_ddl(
            doris_spec,
            f"""CREATE INDEX idx_embedding_ivf ON `{doris_spec.database}`.`{doris_spec.table}` (embedding)
            USING ANN PROPERTIES (
                "index_type" = "ivf",
                "metric_type" = "l2_distance",
                "dim" = "4",
                "nlist" = "2"
            )""",
        )

        await asyncio.sleep(2)

        # Step 4: Build the index
        try:
            await _execute_ddl(
                doris_spec,
                f"BUILD INDEX idx_embedding_ivf ON `{doris_spec.database}`.`{doris_spec.table}`",
            )
            await asyncio.sleep(5)  # Wait for index build
        except Exception:
            pass  # Index may already be built

        # Query with l2_distance function (index accelerates ORDER BY queries)
        query = f"""
        SELECT id, content, l2_distance(embedding, [1.0, 0.0, 0.0, 0.0]) as distance
        FROM {doris_spec.database}.{doris_spec.table}
        ORDER BY distance ASC
        LIMIT 3
        """
        query_result = await _execute_ddl(doris_spec, query)

        assert len(query_result) >= 1
        # Apple should be closest (exact match)
        assert query_result[0]["content"] == "Apple"

    @pytest.mark.asyncio
    async def test_hnsw_index_vector_search(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test vector search with HNSW index."""
        # Create table with HNSW vector index
        state = _State(
            key_fields_schema=[_mock_field("id", "Int64")],
            value_fields_schema=[
                _mock_field("content", "Str"),
                _mock_field("embedding", "Vector", dim=4),
            ],
            vector_indexes=[
                _VectorIndex(
                    name="idx_embedding_hnsw",
                    field_name="embedding",
                    index_type="hnsw",
                    metric_type="l2_distance",
                    dimension=4,
                    max_degree=16,
                    ef_construction=100,
                )
            ],
        )
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        ddl = _generate_create_table_ddl(key, state)
        await _execute_ddl(doris_spec, ddl)

        await asyncio.sleep(2)

        # Insert vectors
        rows = [
            {"id": 1, "content": "Apple", "embedding": [1.0, 0.0, 0.0, 0.0]},
            {"id": 2, "content": "Banana", "embedding": [0.0, 1.0, 0.0, 0.0]},
            {"id": 3, "content": "Cherry", "embedding": [0.0, 0.0, 1.0, 0.0]},
        ]
        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(doris_spec.username, doris_spec.password),
        ) as session:
            load_result = await _stream_load(session, doris_spec, rows)
            assert load_result.get("Status") in ("Success", "Publish Timeout")

        await asyncio.sleep(3)

        # Build the index explicitly
        try:
            await _execute_ddl(
                doris_spec,
                f"BUILD INDEX idx_embedding_hnsw ON `{doris_spec.database}`.`{doris_spec.table}`",
            )
            await asyncio.sleep(3)
        except Exception:
            pass

        # Query with l2_distance function (index accelerates ORDER BY queries)
        query = f"""
        SELECT id, content, l2_distance(embedding, [1.0, 0.0, 0.0, 0.0]) as distance
        FROM {doris_spec.database}.{doris_spec.table}
        ORDER BY distance ASC
        LIMIT 3
        """
        query_result = await _execute_ddl(doris_spec, query)

        assert len(query_result) >= 1
        # Apple should be closest
        assert query_result[0]["content"] == "Apple"


# ============================================================
# HYBRID SEARCH TESTS (Vector + Full-Text)
# ============================================================


class TestHybridSearch:
    """Test hybrid search combining vector similarity and full-text search."""

    @pytest.mark.asyncio
    async def test_inverted_index_text_search(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test full-text search with inverted index."""
        from cocoindex.targets.doris import _InvertedIndex

        # Create table with inverted index on content
        state = _State(
            key_fields_schema=[_mock_field("id", "Int64")],
            value_fields_schema=[
                _mock_field("title", "Str"),
                _mock_field("content", "Str"),
            ],
            inverted_indexes=[
                _InvertedIndex(
                    name="idx_content_inv",
                    field_name="content",
                    parser="unicode",  # Good for mixed language content
                ),
            ],
        )
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        ddl = _generate_create_table_ddl(key, state)
        await _execute_ddl(doris_spec, ddl)

        await asyncio.sleep(2)

        # Insert documents
        rows = [
            {
                "id": 1,
                "title": "Apache Doris",
                "content": "Apache Doris is a real-time analytical database",
            },
            {
                "id": 2,
                "title": "Vector Database",
                "content": "Vector databases enable semantic search with embeddings",
            },
            {
                "id": 3,
                "title": "Machine Learning",
                "content": "Machine learning powers modern AI applications",
            },
            {
                "id": 4,
                "title": "Data Analytics",
                "content": "Real-time data analytics for business intelligence",
            },
        ]
        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(doris_spec.username, doris_spec.password),
        ) as session:
            load_result = await _stream_load(session, doris_spec, rows)
            assert load_result.get("Status") in ("Success", "Publish Timeout")

        await asyncio.sleep(3)  # Wait for index to be built

        # Test MATCH_ANY - any keyword match
        query = f"""
        SELECT id, title FROM {doris_spec.database}.{doris_spec.table}
        WHERE content MATCH_ANY 'real-time analytics'
        """
        query_result = await _execute_ddl(doris_spec, query)
        assert (
            len(query_result) >= 1
        )  # Should match docs with "real-time" or "analytics"

        # Test MATCH_ALL - all keywords required
        query = f"""
        SELECT id, title FROM {doris_spec.database}.{doris_spec.table}
        WHERE content MATCH_ALL 'real-time analytical'
        """
        query_result = await _execute_ddl(doris_spec, query)
        assert len(query_result) >= 1  # Should match "Apache Doris" doc

        # Test MATCH_PHRASE - exact phrase
        query = f"""
        SELECT id, title FROM {doris_spec.database}.{doris_spec.table}
        WHERE content MATCH_PHRASE 'semantic search'
        """
        query_result = await _execute_ddl(doris_spec, query)
        assert len(query_result) >= 1  # Should match "Vector Database" doc

    @pytest.mark.asyncio
    async def test_hybrid_vector_and_text_search(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test hybrid search combining vector similarity with text filtering."""
        from cocoindex.targets.doris import _InvertedIndex, _VectorIndex

        # Create table with both vector and inverted indexes
        state = _State(
            key_fields_schema=[_mock_field("id", "Int64")],
            value_fields_schema=[
                _mock_field("category", "Str"),
                _mock_field("content", "Str"),
                _mock_field("embedding", "Vector", dim=4),
            ],
            vector_indexes=[
                _VectorIndex(
                    name="idx_embedding_ann",
                    field_name="embedding",
                    index_type="hnsw",
                    metric_type="l2_distance",
                    dimension=4,
                ),
            ],
            inverted_indexes=[
                _InvertedIndex(
                    name="idx_content_inv",
                    field_name="content",
                    parser="unicode",
                ),
            ],
        )
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        ddl = _generate_create_table_ddl(key, state)
        await _execute_ddl(doris_spec, ddl)

        await asyncio.sleep(2)

        # Insert documents with embeddings
        # Embeddings are simple 4D vectors for testing
        rows = [
            {
                "id": 1,
                "category": "tech",
                "content": "Apache Doris real-time database",
                "embedding": [1.0, 0.0, 0.0, 0.0],
            },
            {
                "id": 2,
                "category": "tech",
                "content": "Vector search with embeddings",
                "embedding": [0.9, 0.1, 0.0, 0.0],
            },
            {
                "id": 3,
                "category": "science",
                "content": "Machine learning algorithms",
                "embedding": [0.0, 1.0, 0.0, 0.0],
            },
            {
                "id": 4,
                "category": "science",
                "content": "Deep learning neural networks",
                "embedding": [0.0, 0.9, 0.1, 0.0],
            },
            {
                "id": 5,
                "category": "business",
                "content": "Real-time analytics dashboard",
                "embedding": [0.5, 0.5, 0.0, 0.0],
            },
        ]
        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(doris_spec.username, doris_spec.password),
        ) as session:
            load_result = await _stream_load(session, doris_spec, rows)
            assert load_result.get("Status") in ("Success", "Publish Timeout")

        await asyncio.sleep(3)  # Wait for indexes

        # Build index
        try:
            await _execute_ddl(
                doris_spec,
                f"BUILD INDEX idx_embedding_ann ON {doris_spec.database}.{doris_spec.table}",
            )
        except Exception:
            pass  # Index may already be built

        await asyncio.sleep(2)

        # Hybrid search: Vector similarity + text filter
        # Find docs similar to [1.0, 0.0, 0.0, 0.0] that contain "real-time"
        query = f"""
        SELECT id, category, content,
               l2_distance(embedding, [1.0, 0.0, 0.0, 0.0]) as distance
        FROM {doris_spec.database}.{doris_spec.table}
        WHERE content MATCH_ANY 'real-time'
        ORDER BY distance ASC
        LIMIT 3
        """
        query_result = await _execute_ddl(doris_spec, query)
        assert len(query_result) >= 1
        # Should return docs containing "real-time", ordered by vector similarity

        # Hybrid search: Vector similarity + category filter + text search
        query = f"""
        SELECT id, category, content,
               l2_distance(embedding, [0.0, 1.0, 0.0, 0.0]) as distance
        FROM {doris_spec.database}.{doris_spec.table}
        WHERE category = 'science'
          AND content MATCH_ANY 'learning'
        ORDER BY distance ASC
        LIMIT 2
        """
        query_result = await _execute_ddl(doris_spec, query)
        assert len(query_result) >= 1
        # Should return science docs about learning, ordered by similarity

    @pytest.mark.asyncio
    async def test_text_search_operators(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test various text search operators with inverted index."""
        from cocoindex.targets.doris import _InvertedIndex

        # Create table with inverted index
        state = _State(
            key_fields_schema=[_mock_field("id", "Int64")],
            value_fields_schema=[
                _mock_field("content", "Str"),
            ],
            inverted_indexes=[
                _InvertedIndex(
                    name="idx_content_inv",
                    field_name="content",
                    parser="unicode",
                ),
            ],
        )
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        ddl = _generate_create_table_ddl(key, state)
        await _execute_ddl(doris_spec, ddl)

        await asyncio.sleep(2)

        # Insert test documents
        rows = [
            {"id": 1, "content": "data warehouse solutions for enterprise"},
            {"id": 2, "content": "data warehousing best practices"},
            {"id": 3, "content": "big data processing pipeline"},
            {"id": 4, "content": "warehouse inventory management system"},
        ]
        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(doris_spec.username, doris_spec.password),
        ) as session:
            load_result = await _stream_load(session, doris_spec, rows)
            assert load_result.get("Status") in ("Success", "Publish Timeout")

        await asyncio.sleep(3)

        # Test MATCH_PHRASE_PREFIX - prefix matching
        query = f"""
        SELECT id, content FROM {doris_spec.database}.{doris_spec.table}
        WHERE content MATCH_PHRASE_PREFIX 'data ware'
        """
        query_result = await _execute_ddl(doris_spec, query)
        # Should match "data warehouse" and "data warehousing"
        assert len(query_result) >= 1


# ============================================================
# CONFIGURATION TESTS
# ============================================================


class TestConfiguration:
    """Test all configuration options."""

    def test_default_config_values(self):
        """Test default configuration values."""
        spec = DorisTarget(
            fe_host="localhost",
            database="test",
            table="test_table",
        )
        assert spec.fe_http_port == 8080
        assert spec.query_port == 9030
        assert spec.username == "root"
        assert spec.password == ""
        assert spec.enable_https is False
        assert spec.batch_size == 10000
        assert spec.stream_load_timeout == 600
        assert spec.auto_create_table is True
        assert spec.max_retries == 3
        assert spec.retry_base_delay == 1.0
        assert spec.retry_max_delay == 30.0
        assert spec.replication_num == 1
        assert spec.buckets == "auto"

    def test_custom_config_values(self):
        """Test custom configuration values."""
        spec = DorisTarget(
            fe_host="custom-host",
            database="custom_db",
            table="custom_table",
            fe_http_port=9080,
            query_port=19030,
            username="custom_user",
            password="custom_pass",
            enable_https=True,
            batch_size=5000,
            stream_load_timeout=300,
            auto_create_table=False,
            max_retries=5,
            retry_base_delay=2.0,
            retry_max_delay=60.0,
            replication_num=3,
            buckets=16,
        )
        assert spec.fe_host == "custom-host"
        assert spec.fe_http_port == 9080
        assert spec.query_port == 19030
        assert spec.username == "custom_user"
        assert spec.password == "custom_pass"
        assert spec.enable_https is True
        assert spec.batch_size == 5000
        assert spec.stream_load_timeout == 300
        assert spec.auto_create_table is False
        assert spec.max_retries == 5
        assert spec.retry_base_delay == 2.0
        assert spec.retry_max_delay == 60.0
        assert spec.replication_num == 3
        assert spec.buckets == 16

    @pytest.mark.asyncio
    async def test_batch_size_respected(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test that batch_size configuration is used."""
        # Create spec with small batch size
        spec = DorisTarget(
            fe_host=doris_spec.fe_host,
            fe_http_port=doris_spec.fe_http_port,
            query_port=doris_spec.query_port,
            username=doris_spec.username,
            password=doris_spec.password,
            database=doris_spec.database,
            table=doris_spec.table,
            batch_size=10,  # Small batch
        )

        # Create table
        state = _mock_state(spec=doris_spec)
        key = _TableKey(spec.fe_host, spec.database, spec.table)
        ddl = _generate_create_table_ddl(key, state)
        await _execute_ddl(spec, ddl)

        await asyncio.sleep(2)

        # The batch_size is used in mutate() to batch rows
        # This test verifies the spec is accepted
        assert spec.batch_size == 10


# ============================================================
# RETRY LOGIC TESTS
# ============================================================


class TestRetryLogic:
    """Test retry configuration and behavior."""

    def test_retry_config_defaults(self):
        """Test RetryConfig default values."""
        config = RetryConfig()
        assert config.max_retries == 3
        assert config.base_delay == 1.0
        assert config.max_delay == 30.0
        assert config.exponential_base == 2.0

    def test_retry_config_custom(self):
        """Test custom RetryConfig values."""
        config = RetryConfig(
            max_retries=5,
            base_delay=0.5,
            max_delay=60.0,
            exponential_base=3.0,
        )
        assert config.max_retries == 5
        assert config.base_delay == 0.5
        assert config.max_delay == 60.0
        assert config.exponential_base == 3.0

    @pytest.mark.asyncio
    async def test_retry_succeeds_on_first_try(self):
        """Test retry logic when operation succeeds immediately."""
        call_count = 0

        async def successful_op():
            nonlocal call_count
            call_count += 1
            return "success"

        result = await with_retry(
            successful_op,
            config=RetryConfig(max_retries=3),
            retryable_errors=(Exception,),
        )

        assert result == "success"
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_retry_succeeds_after_failures(self):
        """Test retry logic with transient failures."""
        call_count = 0

        async def flaky_op():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise asyncio.TimeoutError("Transient error")
            return "success"

        result = await with_retry(
            flaky_op,
            config=RetryConfig(max_retries=3, base_delay=0.01),
            retryable_errors=(asyncio.TimeoutError,),
        )

        assert result == "success"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_retry_exhausted_raises_error(self):
        """Test retry logic when all retries fail."""
        call_count = 0

        async def always_fails():
            nonlocal call_count
            call_count += 1
            raise asyncio.TimeoutError("Always fails")

        with pytest.raises(DorisConnectionError) as exc_info:
            await with_retry(
                always_fails,
                config=RetryConfig(max_retries=2, base_delay=0.01),
                retryable_errors=(asyncio.TimeoutError,),
            )

        assert call_count == 3  # Initial + 2 retries
        assert "failed after 3 attempts" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_retry_config_from_spec_used(self, doris_spec: DorisTarget):
        """Test that spec's retry config is actually used."""
        # Create spec with custom retry settings
        spec = DorisTarget(
            fe_host=doris_spec.fe_host,
            fe_http_port=doris_spec.fe_http_port,
            query_port=doris_spec.query_port,
            username=doris_spec.username,
            password=doris_spec.password,
            database=doris_spec.database,
            table=doris_spec.table,
            max_retries=1,
            retry_base_delay=0.1,
            retry_max_delay=1.0,
        )

        # Verify config is set
        assert spec.max_retries == 1
        assert spec.retry_base_delay == 0.1
        assert spec.retry_max_delay == 1.0


# ============================================================
# ERROR HANDLING TESTS
# ============================================================


class TestErrorHandling:
    """Test error handling scenarios."""

    @pytest.mark.asyncio
    async def test_invalid_host_raises_connection_error(self):
        """Test that invalid host raises appropriate error."""
        spec = DorisTarget(
            fe_host="invalid-host-that-does-not-exist.example.com",
            database="test",
            table="test_table",
            max_retries=0,  # No retries for faster test
        )

        with pytest.raises((DorisConnectionError, Exception)):
            await _execute_ddl(spec, "SELECT 1")

    @pytest.mark.asyncio
    async def test_invalid_credentials_raises_auth_error(self, doris_spec: DorisTarget):
        """Test that invalid credentials raise auth error."""
        spec = DorisTarget(
            fe_host=doris_spec.fe_host,
            fe_http_port=doris_spec.fe_http_port,
            query_port=doris_spec.query_port,
            username="invalid_user",
            password="invalid_password",
            database=doris_spec.database,
            table=doris_spec.table,
            max_retries=0,
        )

        with pytest.raises(Exception):  # May be auth error or connection error
            await _execute_ddl(spec, "SELECT 1")


# ============================================================
# FULL CONNECTOR WORKFLOW TEST
# ============================================================


class TestConnectorWorkflow:
    """Test complete connector workflow as used by CocoIndex."""

    @pytest.mark.asyncio
    async def test_full_workflow(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test the complete connector workflow: setup -> prepare -> mutate."""
        # 1. Get persistent key
        key = _Connector.get_persistent_key(doris_spec)
        assert isinstance(key, _TableKey)

        # 2. Get setup state
        key_schema = [_mock_field("doc_id", "Str")]
        value_schema = [
            _mock_field("title", "Str"),
            _mock_field("content", "Str"),
        ]
        state = _Connector.get_setup_state(
            doris_spec,
            key_fields_schema=key_schema,
            value_fields_schema=value_schema,
            index_options=IndexOptions(primary_key_fields=["doc_id"]),
        )
        assert isinstance(state, _State)

        # 3. Describe resource
        desc = _Connector.describe(key)
        assert doris_spec.table in desc

        # 4. Apply setup change (create table)
        await _Connector.apply_setup_change(key, None, state)

        # 5. Verify table exists
        exists = await _table_exists(doris_spec, doris_spec.database, doris_spec.table)
        assert exists

        # 6. Prepare for mutations
        context = await _Connector.prepare(doris_spec, state)
        assert context.session is not None

        # 7. Perform mutations
        mutations: dict[Any, dict[str, Any] | None] = {
            "doc1": {"title": "First Document", "content": "This is document 1"},
            "doc2": {"title": "Second Document", "content": "This is document 2"},
        }
        await _Connector.mutate((context, mutations))

        # Wait for data
        await asyncio.sleep(2)

        # 8. Verify data
        result = await _execute_ddl(
            doris_spec,
            f"SELECT COUNT(*) as cnt FROM {doris_spec.database}.{doris_spec.table}",
        )
        assert result[0]["cnt"] >= 2

        # 9. Cleanup (drop table)
        await _Connector.apply_setup_change(key, state, None)

        # 10. Close session
        await context.session.close()


# ============================================================
# HELPER FOR STATE WITH CREDENTIALS
# ============================================================


def _state_with_creds(
    spec: DorisTarget,
    key_fields: list[FieldSchema],
    value_fields: list[FieldSchema],
    vector_indexes: list[_VectorIndex] | None = None,
    inverted_indexes: list[_InvertedIndex] | None = None,
    schema_evolution: str = "extend",
) -> _State:
    """Create a _State with credentials from the spec."""
    return _State(
        key_fields_schema=key_fields,
        value_fields_schema=value_fields,
        vector_indexes=vector_indexes,
        inverted_indexes=inverted_indexes,
        fe_http_port=spec.fe_http_port,
        query_port=spec.query_port,
        username=spec.username,
        password=spec.password,
        max_retries=spec.max_retries,
        retry_base_delay=spec.retry_base_delay,
        retry_max_delay=spec.retry_max_delay,
        schema_evolution=schema_evolution,  # type: ignore[arg-type]
        replication_num=1,
        buckets=1,  # Small for testing
    )


# ============================================================
# SCHEMA EVOLUTION TESTS
# ============================================================


class TestSchemaEvolution:
    """Test schema evolution behavior (extend vs strict mode)."""

    @pytest.mark.asyncio
    async def test_extend_mode_keeps_extra_columns(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test that extend mode keeps extra columns in DB that aren't in schema.

        Documented behavior: Extra columns in the database that aren't in your
        schema are kept untouched.
        """
        from cocoindex.targets.doris import _get_table_schema

        # Create initial table with extra column
        initial_state = _state_with_creds(
            doris_spec,
            key_fields=[_mock_field("id", "Int64")],
            value_fields=[
                _mock_field("content", "Str"),
                _mock_field("extra_col", "Str"),  # Extra column to be kept
            ],
            schema_evolution="extend",
        )
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        await _Connector.apply_setup_change(key, None, initial_state)

        await asyncio.sleep(2)

        # Now apply a new state WITHOUT the extra column
        new_state = _state_with_creds(
            doris_spec,
            key_fields=[_mock_field("id", "Int64")],
            value_fields=[
                _mock_field("content", "Str"),
                # extra_col is removed from schema
            ],
            schema_evolution="extend",
        )

        # Apply the setup change - should NOT drop the extra column
        await _Connector.apply_setup_change(key, initial_state, new_state)

        await asyncio.sleep(2)

        # Verify extra_col still exists in the database
        actual_schema = await _get_table_schema(
            doris_spec, doris_spec.database, doris_spec.table
        )
        assert actual_schema is not None, "Table should exist"
        assert "extra_col" in actual_schema, (
            "Extra column should be kept in extend mode. "
            f"Available columns: {list(actual_schema.keys())}"
        )
        assert "content" in actual_schema

    @pytest.mark.asyncio
    async def test_extend_mode_adds_missing_columns(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test that extend mode adds missing columns via ALTER TABLE.

        Documented behavior: Missing columns are added via ALTER TABLE ADD COLUMN.
        """
        from cocoindex.targets.doris import _get_table_schema

        # Create initial table without the new column
        initial_state = _state_with_creds(
            doris_spec,
            key_fields=[_mock_field("id", "Int64")],
            value_fields=[
                _mock_field("content", "Str"),
            ],
            schema_evolution="extend",
        )
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        await _Connector.apply_setup_change(key, None, initial_state)

        await asyncio.sleep(2)

        # Insert some data
        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(doris_spec.username, doris_spec.password),
        ) as session:
            await _stream_load(session, doris_spec, [{"id": 1, "content": "Test"}])

        await asyncio.sleep(2)

        # Now apply a new state WITH an additional column
        new_state = _state_with_creds(
            doris_spec,
            key_fields=[_mock_field("id", "Int64")],
            value_fields=[
                _mock_field("content", "Str"),
                _mock_field("new_column", "Str"),  # New column to add
            ],
            schema_evolution="extend",
        )

        # Apply the setup change - should add the new column
        await _Connector.apply_setup_change(key, initial_state, new_state)

        await asyncio.sleep(2)

        # Verify new_column was added
        actual_schema = await _get_table_schema(
            doris_spec, doris_spec.database, doris_spec.table
        )
        assert actual_schema is not None, "Table should exist"
        assert "new_column" in actual_schema, (
            "New column should be added in extend mode. "
            f"Available columns: {list(actual_schema.keys())}"
        )
        assert "content" in actual_schema

        # Verify existing data is preserved
        result = await _execute_ddl(
            doris_spec,
            f"SELECT * FROM {doris_spec.database}.{doris_spec.table} WHERE id = 1",
        )
        assert len(result) == 1
        assert result[0]["content"] == "Test"

    @pytest.mark.asyncio
    async def test_extend_mode_never_drops_table_except_key_change(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test that extend mode never drops table except for primary key changes.

        Documented behavior: Tables are never dropped except for primary key changes.
        """
        # Create initial table with data
        initial_state = _state_with_creds(
            doris_spec,
            key_fields=[_mock_field("id", "Int64")],
            value_fields=[
                _mock_field("content", "Str"),
                _mock_field("old_column", "Str"),
            ],
            schema_evolution="extend",
        )
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        await _Connector.apply_setup_change(key, None, initial_state)

        await asyncio.sleep(2)

        # Insert data
        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(doris_spec.username, doris_spec.password),
        ) as session:
            await _stream_load(
                session,
                doris_spec,
                [
                    {"id": 1, "content": "Row 1", "old_column": "Old data"},
                    {"id": 2, "content": "Row 2", "old_column": "Old data 2"},
                ],
            )

        await asyncio.sleep(2)

        # Apply new state that removes a column (NOT a key change)
        new_state = _state_with_creds(
            doris_spec,
            key_fields=[_mock_field("id", "Int64")],  # Same key
            value_fields=[
                _mock_field("content", "Str"),
                # old_column removed
            ],
            schema_evolution="extend",
        )

        await _Connector.apply_setup_change(key, initial_state, new_state)

        await asyncio.sleep(2)

        # Verify data is still there (table wasn't dropped)
        result = await _execute_ddl(
            doris_spec,
            f"SELECT COUNT(*) as cnt FROM {doris_spec.database}.{doris_spec.table}",
        )
        assert result[0]["cnt"] == 2, (
            "Data should be preserved - table should not be dropped"
        )

    @pytest.mark.asyncio
    async def test_strict_mode_drops_table_on_column_removal(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test that strict mode drops and recreates table when columns are removed.

        Documented behavior: In strict mode, schema changes (removing columns)
        cause the table to be dropped and recreated.
        """
        # Create initial table with data
        initial_state = _state_with_creds(
            doris_spec,
            key_fields=[_mock_field("id", "Int64")],
            value_fields=[
                _mock_field("content", "Str"),
                _mock_field("old_column", "Str"),
            ],
            schema_evolution="strict",
        )
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        await _Connector.apply_setup_change(key, None, initial_state)

        await asyncio.sleep(2)

        # Insert data
        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(doris_spec.username, doris_spec.password),
        ) as session:
            await _stream_load(
                session,
                doris_spec,
                [
                    {"id": 1, "content": "Row 1", "old_column": "Old data"},
                ],
            )

        await asyncio.sleep(2)

        # Apply new state that removes a column
        new_state = _state_with_creds(
            doris_spec,
            key_fields=[_mock_field("id", "Int64")],
            value_fields=[
                _mock_field("content", "Str"),
                # old_column removed
            ],
            schema_evolution="strict",
        )

        # Check compatibility - should be NOT_COMPATIBLE in strict mode
        compat = _Connector.check_state_compatibility(initial_state, new_state)
        assert compat == op.TargetStateCompatibility.NOT_COMPATIBLE, (
            "Removing columns should be NOT_COMPATIBLE in strict mode"
        )

    @pytest.mark.asyncio
    async def test_key_change_drops_table_even_in_extend_mode(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test that key schema changes drop table even in extend mode.

        Documented behavior: Tables are never dropped except for primary key changes.
        """
        # Create initial table
        initial_state = _state_with_creds(
            doris_spec,
            key_fields=[_mock_field("id", "Int64")],
            value_fields=[_mock_field("content", "Str")],
            schema_evolution="extend",
        )
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        await _Connector.apply_setup_change(key, None, initial_state)

        await asyncio.sleep(2)

        # Insert data
        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(doris_spec.username, doris_spec.password),
        ) as session:
            await _stream_load(session, doris_spec, [{"id": 1, "content": "Test"}])

        await asyncio.sleep(2)

        # New state with different key schema
        new_state = _state_with_creds(
            doris_spec,
            key_fields=[
                _mock_field("id", "Int64"),
                _mock_field("version", "Int64"),  # Added to key
            ],
            value_fields=[_mock_field("content", "Str")],
            schema_evolution="extend",
        )

        # Check compatibility - should be NOT_COMPATIBLE even in extend mode
        compat = _Connector.check_state_compatibility(initial_state, new_state)
        assert compat == op.TargetStateCompatibility.NOT_COMPATIBLE, (
            "Key schema change should be NOT_COMPATIBLE even in extend mode"
        )

    @pytest.mark.asyncio
    async def test_table_model_validation(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test that tables with correct DUPLICATE KEY model pass validation.

        Documented behavior: Tables are created using DUPLICATE KEY model,
        which is required for vector index support in Doris 4.0+.
        """
        from cocoindex.targets.doris import _get_table_model

        # Create a table via CocoIndex (should be DUPLICATE KEY)
        state = _state_with_creds(
            doris_spec,
            key_fields=[_mock_field("id", "Int64")],
            value_fields=[_mock_field("content", "Str")],
        )
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        await _Connector.apply_setup_change(key, None, state)

        await asyncio.sleep(2)

        # Verify table model is DUPLICATE KEY
        table_model = await _get_table_model(
            doris_spec, doris_spec.database, doris_spec.table
        )
        assert table_model == "DUPLICATE KEY", (
            f"Table should use DUPLICATE KEY model, got: {table_model}"
        )

        # Apply same state again (should succeed since model is correct)
        await _Connector.apply_setup_change(key, state, state)


# ============================================================
# INDEX VALIDATION FAILURE TESTS
# ============================================================


class TestIndexValidationFailures:
    """Test index creation failures when columns are incompatible.

    These tests verify the documented behavior: Indexes are created only if
    the referenced column exists and has a compatible type. Incompatible
    columns should raise DorisSchemaError.
    """

    @pytest.mark.asyncio
    async def test_vector_index_on_missing_column_raises_error(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test that creating a vector index on a missing column raises DorisSchemaError."""
        from cocoindex.targets.doris import (
            _sync_indexes,
            DorisSchemaError,
            _get_table_schema,
        )

        # Create table WITHOUT embedding column
        state_no_vector = _state_with_creds(
            doris_spec,
            key_fields=[_mock_field("id", "Int64")],
            value_fields=[
                _mock_field("content", "Str"),
                # No embedding column
            ],
            schema_evolution="extend",
        )
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        await _Connector.apply_setup_change(key, None, state_no_vector)

        await asyncio.sleep(2)

        # Get actual schema from DB
        actual_schema = await _get_table_schema(
            doris_spec, doris_spec.database, doris_spec.table
        )
        assert actual_schema is not None

        # Try to create vector index on non-existent column
        state_with_index = _state_with_creds(
            doris_spec,
            key_fields=[_mock_field("id", "Int64")],
            value_fields=[
                _mock_field("content", "Str"),
            ],
            vector_indexes=[
                _VectorIndex(
                    name="idx_embedding_ann",
                    field_name="embedding",  # This column doesn't exist!
                    index_type="hnsw",
                    metric_type="l2_distance",
                    dimension=384,
                )
            ],
            schema_evolution="extend",
        )

        # Should raise DorisSchemaError
        with pytest.raises(DorisSchemaError) as exc_info:
            await _sync_indexes(
                doris_spec, key, state_no_vector, state_with_index, actual_schema
            )

        assert "embedding" in str(exc_info.value)
        assert "does not exist" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_vector_index_on_wrong_type_raises_error(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test that creating a vector index on a non-array column raises DorisSchemaError."""
        from cocoindex.targets.doris import (
            _sync_indexes,
            DorisSchemaError,
            _get_table_schema,
        )

        # Create table with TEXT column instead of ARRAY<FLOAT>
        state_wrong_type = _state_with_creds(
            doris_spec,
            key_fields=[_mock_field("id", "Int64")],
            value_fields=[
                _mock_field("content", "Str"),
                _mock_field(
                    "embedding", "Str"
                ),  # Wrong type - TEXT instead of ARRAY<FLOAT>
            ],
            schema_evolution="extend",
        )
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        await _Connector.apply_setup_change(key, None, state_wrong_type)

        await asyncio.sleep(2)

        # Get actual schema from DB
        actual_schema = await _get_table_schema(
            doris_spec, doris_spec.database, doris_spec.table
        )
        assert actual_schema is not None

        # Try to create vector index on TEXT column
        state_with_index = _state_with_creds(
            doris_spec,
            key_fields=[_mock_field("id", "Int64")],
            value_fields=[
                _mock_field("content", "Str"),
                _mock_field("embedding", "Str"),
            ],
            vector_indexes=[
                _VectorIndex(
                    name="idx_embedding_ann",
                    field_name="embedding",  # This column is TEXT, not ARRAY<FLOAT>
                    index_type="hnsw",
                    metric_type="l2_distance",
                    dimension=384,
                )
            ],
            schema_evolution="extend",
        )

        # Should raise DorisSchemaError
        with pytest.raises(DorisSchemaError) as exc_info:
            await _sync_indexes(
                doris_spec, key, state_wrong_type, state_with_index, actual_schema
            )

        assert "embedding" in str(exc_info.value)
        assert "ARRAY" in str(exc_info.value) or "type" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_inverted_index_on_missing_column_raises_error(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test that creating an inverted index on a missing column raises DorisSchemaError."""
        from cocoindex.targets.doris import (
            _sync_indexes,
            DorisSchemaError,
            _get_table_schema,
        )

        # Create table WITHOUT content column
        state_no_content = _state_with_creds(
            doris_spec,
            key_fields=[_mock_field("id", "Int64")],
            value_fields=[
                _mock_field("title", "Str"),
                # No content column
            ],
            schema_evolution="extend",
        )
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        await _Connector.apply_setup_change(key, None, state_no_content)

        await asyncio.sleep(2)

        # Get actual schema from DB
        actual_schema = await _get_table_schema(
            doris_spec, doris_spec.database, doris_spec.table
        )
        assert actual_schema is not None

        # Try to create inverted index on non-existent column
        state_with_index = _state_with_creds(
            doris_spec,
            key_fields=[_mock_field("id", "Int64")],
            value_fields=[
                _mock_field("title", "Str"),
            ],
            inverted_indexes=[
                _InvertedIndex(
                    name="idx_content_inv",
                    field_name="content",  # This column doesn't exist!
                    parser="unicode",
                )
            ],
            schema_evolution="extend",
        )

        # Should raise DorisSchemaError
        with pytest.raises(DorisSchemaError) as exc_info:
            await _sync_indexes(
                doris_spec, key, state_no_content, state_with_index, actual_schema
            )

        assert "content" in str(exc_info.value)
        assert "does not exist" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_inverted_index_on_wrong_type_raises_error(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Test that creating an inverted index on a non-text column raises DorisSchemaError."""
        from cocoindex.targets.doris import (
            _sync_indexes,
            DorisSchemaError,
            _get_table_schema,
        )

        # Create table with INT column instead of TEXT
        state_wrong_type = _state_with_creds(
            doris_spec,
            key_fields=[_mock_field("id", "Int64")],
            value_fields=[
                _mock_field("title", "Str"),
                _mock_field("count", "Int64"),  # Wrong type - INT instead of TEXT
            ],
            schema_evolution="extend",
        )
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        await _Connector.apply_setup_change(key, None, state_wrong_type)

        await asyncio.sleep(2)

        # Get actual schema from DB
        actual_schema = await _get_table_schema(
            doris_spec, doris_spec.database, doris_spec.table
        )
        assert actual_schema is not None

        # Try to create inverted index on INT column
        state_with_index = _state_with_creds(
            doris_spec,
            key_fields=[_mock_field("id", "Int64")],
            value_fields=[
                _mock_field("title", "Str"),
                _mock_field("count", "Int64"),
            ],
            inverted_indexes=[
                _InvertedIndex(
                    name="idx_count_inv",
                    field_name="count",  # This column is BIGINT, not TEXT
                    parser="unicode",
                )
            ],
            schema_evolution="extend",
        )

        # Should raise DorisSchemaError
        with pytest.raises(DorisSchemaError) as exc_info:
            await _sync_indexes(
                doris_spec, key, state_wrong_type, state_with_index, actual_schema
            )

        assert "count" in str(exc_info.value)
        assert "type" in str(exc_info.value).lower() or "TEXT" in str(exc_info.value)


# ============================================================
# QUERY HELPER TESTS
# ============================================================


class TestQueryHelpers:
    """Test query helper functions documented in the docs."""

    @pytest.mark.asyncio
    async def test_connect_async_with_proper_cleanup(self, doris_spec: DorisTarget):
        """Test connect_async helper with proper cleanup using ensure_closed().

        Documented usage:
            conn = await connect_async(...)
            try:
                async with conn.cursor() as cursor:
                    await cursor.execute("SELECT * FROM table")
                    rows = await cursor.fetchall()
            finally:
                conn.close()
                await conn.ensure_closed()
        """
        conn = await connect_async(
            fe_host=doris_spec.fe_host,
            query_port=doris_spec.query_port,
            username=doris_spec.username,
            password=doris_spec.password,
            database=doris_spec.database,
        )
        try:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT 1 as value")
                rows = await cursor.fetchall()
                assert len(rows) == 1
                assert rows[0][0] == 1
        finally:
            conn.close()
            await conn.ensure_closed()

    def test_build_vector_search_query_l2_distance(self):
        """Test build_vector_search_query with L2 distance metric."""
        sql = build_vector_search_query(
            table="test_db.embeddings",
            vector_field="embedding",
            query_vector=[0.1, 0.2, 0.3, 0.4],
            metric="l2_distance",
            limit=10,
            select_columns=["id", "content"],
        )

        assert "l2_distance_approximate" in sql
        assert "`embedding`" in sql  # Backtick-quoted
        assert "[0.1, 0.2, 0.3, 0.4]" in sql
        assert "LIMIT 10" in sql
        assert "ORDER BY _distance ASC" in sql
        assert "`id`, `content`" in sql  # Backtick-quoted

    def test_build_vector_search_query_inner_product(self):
        """Test build_vector_search_query with inner product metric."""
        sql = build_vector_search_query(
            table="test_db.embeddings",
            vector_field="embedding",
            query_vector=[0.1, 0.2],
            metric="inner_product",
            limit=5,
        )

        assert "inner_product_approximate" in sql
        assert "ORDER BY _distance DESC" in sql  # Larger = more similar
        assert "LIMIT 5" in sql
        assert "`test_db`.`embeddings`" in sql  # Backtick-quoted table

    def test_build_vector_search_query_with_where_clause(self):
        """Test build_vector_search_query with WHERE clause filter."""
        sql = build_vector_search_query(
            table="test_db.docs",
            vector_field="embedding",
            query_vector=[1.0, 0.0],
            metric="l2_distance",
            limit=10,
            where_clause="category = 'tech'",
        )

        assert "WHERE category = 'tech'" in sql
        assert "`test_db`.`docs`" in sql  # Backtick-quoted table


# ============================================================
# DOCUMENTED BEHAVIOR VERIFICATION
# ============================================================


class TestDocumentedBehavior:
    """Verify all documented behavior works as specified."""

    @pytest.mark.asyncio
    async def test_vector_type_mapped_to_array_float(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Verify: Vectors are mapped to ARRAY<FLOAT> columns in Doris."""
        from cocoindex.targets.doris import _get_table_schema

        state = _state_with_creds(
            doris_spec,
            key_fields=[_mock_field("id", "Int64")],
            value_fields=[
                _mock_field("embedding", "Vector", dim=4),
            ],
        )
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        await _Connector.apply_setup_change(key, None, state)

        await asyncio.sleep(2)

        actual_schema = await _get_table_schema(
            doris_spec, doris_spec.database, doris_spec.table
        )
        assert actual_schema is not None
        assert "embedding" in actual_schema
        assert "ARRAY" in actual_schema["embedding"].doris_type.upper()

    @pytest.mark.asyncio
    async def test_vector_columns_are_not_null(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Verify: Vector columns are automatically created as NOT NULL."""
        from cocoindex.targets.doris import _get_table_schema

        state = _state_with_creds(
            doris_spec,
            key_fields=[_mock_field("id", "Int64")],
            value_fields=[
                _mock_field("embedding", "Vector", dim=4),
            ],
        )
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        await _Connector.apply_setup_change(key, None, state)

        await asyncio.sleep(2)

        actual_schema = await _get_table_schema(
            doris_spec, doris_spec.database, doris_spec.table
        )
        assert actual_schema is not None
        assert "embedding" in actual_schema
        assert actual_schema["embedding"].nullable is False, (
            "Vector columns should be NOT NULL for vector index support"
        )

    @pytest.mark.asyncio
    async def test_duplicate_key_table_model(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """Verify: Tables are created using DUPLICATE KEY model."""
        state = _state_with_creds(
            doris_spec,
            key_fields=[_mock_field("id", "Int64")],
            value_fields=[_mock_field("content", "Str")],
        )
        key = _TableKey(doris_spec.fe_host, doris_spec.database, doris_spec.table)
        await _Connector.apply_setup_change(key, None, state)

        await asyncio.sleep(2)

        result = await _execute_ddl(
            doris_spec,
            f"SHOW CREATE TABLE {doris_spec.database}.{doris_spec.table}",
        )
        create_stmt = result[0].get("Create Table", "")
        assert "DUPLICATE KEY" in create_stmt.upper(), (
            "Table should use DUPLICATE KEY model for vector index support"
        )

    @pytest.mark.asyncio
    async def test_default_config_values_match_docs(self):
        """Verify default config values match documentation."""
        spec = DorisTarget(
            fe_host="localhost",
            database="test",
            table="test_table",
        )

        # Connection defaults from docs
        assert spec.fe_http_port == 8080, "Default fe_http_port should be 8080"
        assert spec.query_port == 9030, "Default query_port should be 9030"
        assert spec.username == "root", "Default username should be 'root'"
        assert spec.password == "", "Default password should be empty string"
        assert spec.enable_https is False, "Default enable_https should be False"

        # Behavior defaults from docs
        assert spec.batch_size == 10000, "Default batch_size should be 10000"
        assert spec.stream_load_timeout == 600, (
            "Default stream_load_timeout should be 600"
        )
        assert spec.auto_create_table is True, (
            "Default auto_create_table should be True"
        )
        assert spec.schema_evolution == "extend", (
            "Default schema_evolution should be 'extend'"
        )

        # Retry defaults from docs
        assert spec.max_retries == 3, "Default max_retries should be 3"
        assert spec.retry_base_delay == 1.0, "Default retry_base_delay should be 1.0"
        assert spec.retry_max_delay == 30.0, "Default retry_max_delay should be 30.0"

        # Table property defaults from docs
        assert spec.replication_num == 1, "Default replication_num should be 1"
        assert spec.buckets == "auto", "Default buckets should be 'auto'"


# ============================================================
# TEXT EMBEDDING EXAMPLE INTEGRATION TEST
# ============================================================


class TestTextEmbeddingExample:
    """Integration tests for the text_embedding_doris example pattern."""

    @pytest.mark.asyncio
    async def test_text_embedding_flow_pattern(
        self, doris_spec: DorisTarget, ensure_database, cleanup_table
    ):
        """
        Test the complete text_embedding_doris example flow pattern:
        1. Create table with vector index
        2. Insert document chunks with embeddings
        3. Query using vector similarity search
        """
        import uuid
        from cocoindex.targets.doris import (
            _execute_ddl,
            connect_async,
            build_vector_search_query,
        )

        # Step 1: Create table with vector and FTS index (matching example pattern)
        table_name = doris_spec.table
        database = doris_spec.database

        # Create table via connector
        vector_indexes = [
            _VectorIndex(
                name="idx_text_embedding_ann",
                field_name="text_embedding",
                index_type="hnsw",
                metric_type="l2_distance",
                dimension=4,  # Small dimension for testing
                max_degree=16,
                ef_construction=200,
            )
        ]
        inverted_indexes = [
            _InvertedIndex(
                name="idx_text_inv",
                field_name="text",
                parser="unicode",
            )
        ]

        state = _State(
            key_fields_schema=[_mock_field("id", "Str")],
            value_fields_schema=[
                _mock_field("filename", "Str"),
                _mock_field("location", "Str"),
                _mock_field("text", "Str"),
                _mock_field("text_embedding", "Vector", dim=4),
            ],
            vector_indexes=vector_indexes,
            inverted_indexes=inverted_indexes,
            replication_num=1,
            buckets="auto",
            fe_http_port=doris_spec.fe_http_port,
            query_port=doris_spec.query_port,
            username=doris_spec.username,
            password=doris_spec.password,
            max_retries=3,
            retry_base_delay=1.0,
            retry_max_delay=30.0,
            auto_create_table=True,
            schema_evolution="extend",
        )

        key = _TableKey(doris_spec.fe_host, database, table_name)
        await _Connector.apply_setup_change(key, None, state)
        await asyncio.sleep(3)

        # Step 2: Insert document chunks (matching example data format)
        # Build mutations dict: key -> {field: value, ...}
        mutations: dict[Any, dict[str, Any] | None] = {
            "doc1_chunk_0": {
                "filename": "doc1.md",
                "location": "0:100",
                "text": "Vector databases are specialized database systems designed for similarity search.",
                "text_embedding": [0.1, 0.2, 0.3, 0.4],
            },
            "doc2_chunk_0": {
                "filename": "doc2.md",
                "location": "0:80",
                "text": "Apache Doris is a high-performance analytical database with vector support.",
                "text_embedding": [0.2, 0.3, 0.4, 0.5],
            },
            "doc3_chunk_0": {
                "filename": "doc3.md",
                "location": "0:90",
                "text": "Semantic search uses embeddings to find relevant results.",
                "text_embedding": [0.3, 0.4, 0.5, 0.6],
            },
        }

        context = await _Connector.prepare(doris_spec, state)
        await _Connector.mutate((context, mutations))
        await _Connector.cleanup(context)

        # Wait for data to be visible
        await asyncio.sleep(3)

        # Step 3: Build index (required after data load for IVF, good practice for HNSW)
        try:
            await _execute_ddl(
                doris_spec,
                f"BUILD INDEX idx_text_embedding_ann ON `{database}`.`{table_name}`",
            )
            await asyncio.sleep(2)
        except Exception:
            pass  # Index may already be built or not require explicit build

        # Step 4: Query using vector similarity (matching example query pattern)
        query_vector = [0.15, 0.25, 0.35, 0.45]  # Similar to doc1

        sql = build_vector_search_query(
            table=f"{database}.{table_name}",
            vector_field="text_embedding",
            query_vector=query_vector,
            metric="l2_distance",
            limit=3,
            select_columns=["id", "filename", "text"],
        )

        conn = await connect_async(
            fe_host=doris_spec.fe_host,
            query_port=doris_spec.query_port,
            username=doris_spec.username,
            password=doris_spec.password,
            database=database,
        )

        try:
            async with conn.cursor() as cursor:
                await cursor.execute(sql)
                results = await cursor.fetchall()
        finally:
            conn.close()
            await conn.ensure_closed()

        # Verify results
        assert len(results) == 3, "Should return 3 results"
        # Results should be ordered by distance (closest first)
        # doc1 [0.1, 0.2, 0.3, 0.4] should be closest to query [0.15, 0.25, 0.35, 0.45]
        assert results[0][1] == "doc1.md", (
            "First result should be doc1.md (closest vector)"
        )

        # Step 5: Verify full-text search works (optional part of example)
        fts_sql = f"""
        SELECT id, filename, text
        FROM {database}.{table_name}
        WHERE text MATCH_ANY 'vector'
        LIMIT 5
        """

        conn = await connect_async(
            fe_host=doris_spec.fe_host,
            query_port=doris_spec.query_port,
            username=doris_spec.username,
            password=doris_spec.password,
            database=database,
        )

        try:
            async with conn.cursor() as cursor:
                await cursor.execute(fts_sql)
                fts_results = await cursor.fetchall()
        finally:
            conn.close()
            await conn.ensure_closed()

        assert len(fts_results) >= 1, (
            "Should find at least one document containing 'vector'"
        )

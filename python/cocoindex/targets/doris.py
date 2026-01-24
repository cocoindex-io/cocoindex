"""
Apache Doris 4.0 target connector for CocoIndex.

Supports:
- Vector index (HNSW, IVF) with L2 distance and inner product metrics
- Inverted index for full-text search
- Stream Load for bulk data ingestion
- Incremental updates with upsert/delete operations

Requirements:
- Doris 4.0+ with vector index support
- DUPLICATE KEY table model (required for vector indexes)
"""

import asyncio
import dataclasses
import json
import logging
import math
import time
import uuid
import re
from typing import Any, Callable, Awaitable, Literal, TypeVar, TYPE_CHECKING

if TYPE_CHECKING:
    import aiohttp  # type: ignore[import-not-found]

from cocoindex import op
from cocoindex.engine_type import (
    FieldSchema,
    EnrichedValueType,
    BasicValueType,
    StructType,
    ValueType,
    TableType,
)
from cocoindex.index import (
    IndexOptions,
    VectorSimilarityMetric,
    HnswVectorIndexMethod,
    IvfFlatVectorIndexMethod,
)

_logger = logging.getLogger(__name__)

T = TypeVar("T")


def _get_aiohttp() -> Any:
    """Lazily import aiohttp to avoid import errors when not installed."""
    try:
        import aiohttp  # type: ignore[import-not-found]

        return aiohttp
    except ImportError:
        raise ImportError(
            "aiohttp is required for Doris connector. "
            "Install it with: pip install aiohttp"
        )


# ============================================================
# TYPE MAPPING: CocoIndex -> Doris SQL
# ============================================================

_DORIS_TYPE_MAPPING: dict[str, str] = {
    "Bytes": "STRING",
    "Str": "TEXT",
    "Bool": "BOOLEAN",
    "Int64": "BIGINT",
    "Float32": "FLOAT",
    "Float64": "DOUBLE",
    "Uuid": "VARCHAR(36)",
    "Date": "DATE",
    "Time": "VARCHAR(20)",  # HH:MM:SS.ffffff
    "LocalDateTime": "DATETIME(6)",
    "OffsetDateTime": "DATETIME(6)",
    "TimeDelta": "BIGINT",  # microseconds
    "Json": "JSON",
    "Range": "JSON",  # {"start": x, "end": y}
}

_DORIS_VECTOR_METRIC: dict[VectorSimilarityMetric, str] = {
    VectorSimilarityMetric.L2_DISTANCE: "l2_distance",
    VectorSimilarityMetric.INNER_PRODUCT: "inner_product",
    VectorSimilarityMetric.COSINE_SIMILARITY: "cosine_distance",
}


# ============================================================
# SPEC CLASSES
# ============================================================


class DorisTarget(op.TargetSpec):
    """Apache Doris target connector specification."""

    # Connection
    fe_host: str
    database: str
    table: str
    fe_http_port: int = 8080
    query_port: int = 9030
    username: str = "root"
    password: str = ""
    enable_https: bool = False

    # Behavior
    batch_size: int = 10000
    stream_load_timeout: int = 600
    auto_create_table: bool = True

    # Timeout configuration (seconds)
    schema_change_timeout: int = 60  # Timeout for ALTER TABLE operations
    index_build_timeout: int = 300  # Timeout for BUILD INDEX operations

    # Retry configuration
    max_retries: int = 3
    retry_base_delay: float = 1.0
    retry_max_delay: float = 30.0

    # Table properties
    replication_num: int = 1
    buckets: int | str = "auto"  # int for fixed count, or "auto" for automatic

    # Schema evolution strategy:
    # - "extend": Allow extra columns in DB, only add missing columns, never drop.
    #             Indexes are created only if referenced columns exist and are compatible.
    # - "strict": Require exact schema match; drop and recreate table if incompatible.
    schema_evolution: Literal["extend", "strict"] = "extend"


@dataclasses.dataclass
class _ColumnInfo:
    """Information about a column in the actual database table."""

    name: str
    doris_type: str  # "BIGINT", "TEXT", "ARRAY<FLOAT>", etc.
    nullable: bool
    is_key: bool
    dimension: int | None = None  # For vector columns (ARRAY<FLOAT>)


@dataclasses.dataclass
class _TableKey:
    """Unique identifier for a Doris table."""

    fe_host: str
    database: str
    table: str


@dataclasses.dataclass
class _VectorIndex:
    """Vector index configuration."""

    name: str
    field_name: str
    index_type: str  # "hnsw" or "ivf"
    metric_type: str  # "l2_distance" or "inner_product"
    dimension: int
    # HNSW params
    max_degree: int | None = None
    ef_construction: int | None = None
    # IVF params
    nlist: int | None = None


@dataclasses.dataclass
class _InvertedIndex:
    """Inverted index for text search."""

    name: str
    field_name: str
    parser: str | None = None  # "chinese", "english", etc.


@dataclasses.dataclass
class _State:
    """Setup state for Doris target."""

    key_fields_schema: list[FieldSchema]
    value_fields_schema: list[FieldSchema]
    vector_indexes: list[_VectorIndex] | None = None
    inverted_indexes: list[_InvertedIndex] | None = None
    replication_num: int = 1
    buckets: int | str = "auto"  # int for fixed count, or "auto" for automatic
    # Connection credentials (needed for apply_setup_change)
    fe_http_port: int = 8080
    query_port: int = 9030
    username: str = "root"
    password: str = ""
    max_retries: int = 3
    retry_base_delay: float = 1.0
    retry_max_delay: float = 30.0
    # Timeout configuration
    schema_change_timeout: int = 60
    index_build_timeout: int = 300
    # Table creation behavior
    auto_create_table: bool = True
    # Schema evolution mode
    schema_evolution: Literal["extend", "strict"] = "extend"


@dataclasses.dataclass
class _MutateContext:
    """Context for mutation operations."""

    spec: DorisTarget
    session: "aiohttp.ClientSession"
    state: _State
    lock: asyncio.Lock


# ============================================================
# ERROR CLASSES
# ============================================================


class DorisError(Exception):
    """Base class for Doris connector errors."""


class DorisConnectionError(DorisError):
    """Connection-related errors (network, auth, timeout)."""

    def __init__(
        self, message: str, host: str, port: int, cause: Exception | None = None
    ):
        self.host = host
        self.port = port
        self.cause = cause
        super().__init__(f"{message} (host={host}:{port})")


class DorisAuthError(DorisConnectionError):
    """Authentication failed."""


class DorisStreamLoadError(DorisError):
    """Stream Load operation failed."""

    def __init__(
        self,
        message: str,
        status: str,
        error_url: str | None = None,
        loaded_rows: int = 0,
        filtered_rows: int = 0,
    ):
        self.status = status
        self.error_url = error_url
        self.loaded_rows = loaded_rows
        self.filtered_rows = filtered_rows
        super().__init__(f"Stream Load {status}: {message}")


class DorisSchemaError(DorisError):
    """Schema-related errors (type mismatch, invalid column)."""

    def __init__(self, message: str, field_name: str | None = None):
        self.field_name = field_name
        super().__init__(message)


# ============================================================
# RETRY LOGIC
# ============================================================


@dataclasses.dataclass
class RetryConfig:
    """Retry configuration for Doris operations."""

    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 30.0
    exponential_base: float = 2.0


def _is_retryable_mysql_error(e: Exception) -> bool:
    """Check if a MySQL error is retryable (transient connection issue)."""
    try:
        import pymysql  # type: ignore

        if isinstance(e, pymysql.err.OperationalError):
            # Check error code - only retry connection-related errors
            if e.args and len(e.args) > 0:
                error_code = e.args[0]
                # Retryable error codes (connection issues):
                # 2003: Can't connect to MySQL server
                # 2006: MySQL server has gone away
                # 2013: Lost connection to MySQL server during query
                # 1040: Too many connections
                # 1205: Lock wait timeout
                retryable_codes = {2003, 2006, 2013, 1040, 1205}
                return error_code in retryable_codes
        if isinstance(e, pymysql.err.InterfaceError):
            return True  # Interface errors are usually connection issues
    except ImportError:
        pass
    return False


def _get_retryable_errors() -> tuple[type[Exception], ...]:
    """Get tuple of retryable error types including aiohttp errors when available."""
    base_errors: tuple[type[Exception], ...] = (
        asyncio.TimeoutError,
        ConnectionError,
        ConnectionResetError,
        ConnectionRefusedError,
    )
    try:
        aiohttp = _get_aiohttp()
        return base_errors + (
            aiohttp.ClientConnectorError,
            aiohttp.ServerDisconnectedError,
        )
    except ImportError:
        # aiohttp not installed - return only base network errors
        # MySQL-only paths will still work via _is_retryable_mysql_error
        return base_errors


async def with_retry(
    operation: Callable[[], Awaitable[T]],
    config: RetryConfig = RetryConfig(),
    operation_name: str = "operation",
    retryable_errors: tuple[type[Exception], ...] | None = None,
) -> T:
    """Execute operation with exponential backoff retry.

    Handles both aiohttp errors (via retryable_errors tuple) and MySQL/aiomysql
    connection errors (via _is_retryable_mysql_error helper).
    """
    if retryable_errors is None:
        retryable_errors = _get_retryable_errors()

    last_error: Exception | None = None

    for attempt in range(config.max_retries + 1):
        try:
            return await operation()
        except Exception as e:
            # Check if error is retryable (either aiohttp or MySQL error)
            is_retryable = isinstance(e, retryable_errors) or _is_retryable_mysql_error(
                e
            )
            if not is_retryable:
                raise  # Re-raise non-retryable errors immediately

            last_error = e
            if attempt < config.max_retries:
                delay = min(
                    config.base_delay * (config.exponential_base**attempt),
                    config.max_delay,
                )
                _logger.warning(
                    "%s failed (attempt %d/%d), retrying in %.1fs: %s",
                    operation_name,
                    attempt + 1,
                    config.max_retries + 1,
                    delay,
                    e,
                )
                await asyncio.sleep(delay)

    raise DorisConnectionError(
        f"{operation_name} failed after {config.max_retries + 1} attempts",
        host="",
        port=0,
        cause=last_error,
    )


# ============================================================
# TYPE CONVERSION
# ============================================================


def _convert_value_type_to_doris_type(value_type: EnrichedValueType) -> str:
    """Convert EnrichedValueType to Doris SQL type."""
    base_type: ValueType = value_type.type

    if isinstance(base_type, StructType):
        return "JSON"

    if isinstance(base_type, TableType):
        return "JSON"

    if isinstance(base_type, BasicValueType):
        kind: str = base_type.kind

        if kind == "Vector":
            # Only vectors with fixed dimension can be stored as ARRAY<FLOAT>
            # for index creation. Others fall back to JSON.
            if _is_vector_indexable(value_type):
                return "ARRAY<FLOAT>"
            else:
                return "JSON"

        if kind in _DORIS_TYPE_MAPPING:
            return _DORIS_TYPE_MAPPING[kind]

        # Fallback to JSON for unsupported types
        return "JSON"

    # Fallback to JSON for unknown value types
    return "JSON"


def _convert_value_for_doris(value: Any) -> Any:
    """Convert Python value to Doris-compatible format."""
    if value is None:
        return None

    if isinstance(value, uuid.UUID):
        return str(value)

    if isinstance(value, float) and math.isnan(value):
        return None

    if isinstance(value, (list, tuple)):
        return [_convert_value_for_doris(v) for v in value]

    if isinstance(value, dict):
        return {k: _convert_value_for_doris(v) for k, v in value.items()}

    if hasattr(value, "isoformat"):
        return value.isoformat()

    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")

    return value


def _get_vector_dimension(
    value_fields_schema: list[FieldSchema], field_name: str
) -> int | None:
    """Get the dimension of a vector field.

    Returns None if the field is not found, not a vector type, or doesn't have a dimension.
    This allows fallback to JSON storage for vectors without fixed dimensions.
    """
    for field in value_fields_schema:
        if field.name == field_name:
            base_type = field.value_type.type
            if isinstance(base_type, BasicValueType) and base_type.kind == "Vector":
                if (
                    base_type.vector is not None
                    and base_type.vector.dimension is not None
                ):
                    return base_type.vector.dimension
            # Field exists but is not a vector with dimension
            return None
    # Field not found
    return None


def _get_doris_metric_type(metric: VectorSimilarityMetric) -> str:
    """Convert CocoIndex metric to Doris metric type."""
    if metric not in _DORIS_VECTOR_METRIC:
        raise ValueError(f"Unsupported vector metric for Doris: {metric}")
    doris_metric = _DORIS_VECTOR_METRIC[metric]
    # Note: cosine_distance doesn't support index in Doris 4.0
    if doris_metric == "cosine_distance":
        _logger.warning(
            "Cosine distance does not support vector index in Doris 4.0. "
            "Queries will use full table scan. Consider using L2 distance or inner product."
        )
    return doris_metric


def _extract_vector_dimension(value_type: EnrichedValueType) -> int | None:
    """Extract dimension from a vector value type."""
    base_type = value_type.type
    if isinstance(base_type, BasicValueType) and base_type.kind == "Vector":
        if base_type.vector is not None:
            return base_type.vector.dimension
    return None


def _is_vector_indexable(value_type: EnrichedValueType) -> bool:
    """Check if a vector type can be indexed (has fixed dimension)."""
    return _extract_vector_dimension(value_type) is not None


def _extract_array_element_type(type_str: str) -> str | None:
    """Extract element type from ARRAY<ELEMENT> type string."""
    type_upper = type_str.upper().strip()
    if type_upper.startswith("ARRAY<") and type_upper.endswith(">"):
        return type_upper[6:-1].strip()
    if type_upper.startswith("ARRAY(") and type_upper.endswith(")"):
        return type_upper[6:-1].strip()
    return None


def _extract_varchar_length(type_str: str) -> int | None:
    """Extract length from VARCHAR(N) type string. Returns None if no length specified."""
    type_upper = type_str.upper().strip()
    if type_upper.startswith("VARCHAR(") and type_upper.endswith(")"):
        try:
            return int(type_upper[8:-1].strip())
        except ValueError:
            return None
    return None


def _types_compatible(expected: str, actual: str) -> bool:
    """Check if two Doris types are compatible.

    This performs strict type checking to avoid data corruption:
    - ARRAY types must have matching element types (ARRAY<FLOAT> != ARRAY<INT>)
    - VARCHAR lengths are checked to ensure actual can hold expected data
    - TEXT/STRING types are treated as interchangeable
    """
    # Normalize for comparison
    expected_norm = expected.upper().strip()
    actual_norm = actual.upper().strip()

    # Exact match
    if expected_norm == actual_norm:
        return True

    # Handle ARRAY types - must check element type
    expected_elem = _extract_array_element_type(expected_norm)
    actual_elem = _extract_array_element_type(actual_norm)
    if expected_elem is not None or actual_elem is not None:
        if expected_elem is None or actual_elem is None:
            # One is ARRAY, one is not
            return False
        # Both are ARRAY - check element types match
        # Allow FLOAT vs DOUBLE as they're commonly interchangeable in Doris
        float_types = {"FLOAT", "DOUBLE"}
        if expected_elem in float_types and actual_elem in float_types:
            return True
        return expected_elem == actual_elem

    # Handle VARCHAR - check length compatibility
    expected_len = _extract_varchar_length(expected_norm)
    actual_len = _extract_varchar_length(actual_norm)
    if expected_norm.startswith("VARCHAR") and actual_norm.startswith("VARCHAR"):
        if expected_len is not None and actual_len is not None:
            # Actual must be able to hold expected length
            return actual_len >= expected_len
        # If either has no explicit length, accept (Doris defaults to large)
        return True

    # Handle TEXT vs STRING (both are text types in Doris)
    # These are essentially unlimited text types
    text_types = {"TEXT", "STRING"}
    expected_base = expected_norm.split("(")[0]
    actual_base = actual_norm.split("(")[0]
    if expected_base in text_types and actual_base in text_types:
        return True

    # TEXT/STRING can hold any VARCHAR content
    if expected_norm.startswith("VARCHAR") and actual_base in text_types:
        return True
    if expected_base in text_types and actual_norm.startswith("VARCHAR"):
        # VARCHAR may truncate TEXT - this is a warning case but we allow it
        return True

    return False


# ============================================================
# SQL GENERATION
# ============================================================


def _validate_identifier(name: str) -> None:
    """Validate SQL identifier to prevent injection."""
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
        raise DorisSchemaError(f"Invalid identifier: {name}")


def _convert_to_key_column_type(doris_type: str) -> str:
    """Convert a Doris type to be compatible with key columns.

    Doris DUPLICATE KEY model doesn't allow TEXT or STRING as key columns.
    Convert them to VARCHAR with appropriate length.
    """
    if doris_type in ("TEXT", "STRING"):
        # Use VARCHAR(512) for key columns - reasonable default for identifiers
        return "VARCHAR(512)"
    return doris_type


def _build_vector_index_properties(idx: "_VectorIndex") -> list[str]:
    """Build PROPERTIES list for vector index DDL.

    This helper is shared between CREATE TABLE and CREATE INDEX statements.

    Args:
        idx: Vector index definition

    Returns:
        List of property strings like '"index_type" = "HNSW"'
    """
    props = [
        f'"index_type" = "{idx.index_type}"',
        f'"metric_type" = "{idx.metric_type}"',
        f'"dim" = "{idx.dimension}"',
    ]
    if idx.max_degree is not None:
        props.append(f'"max_degree" = "{idx.max_degree}"')
    if idx.ef_construction is not None:
        props.append(f'"ef_construction" = "{idx.ef_construction}"')
    if idx.nlist is not None:
        props.append(f'"nlist" = "{idx.nlist}"')
    return props


def _generate_create_table_ddl(key: _TableKey, state: _State) -> str:
    """Generate CREATE TABLE DDL for Doris."""
    _validate_identifier(key.database)
    _validate_identifier(key.table)

    columns = []
    key_column_names = []

    # Key columns - must use VARCHAR instead of TEXT/STRING
    for field in state.key_fields_schema:
        _validate_identifier(field.name)
        doris_type = _convert_value_type_to_doris_type(field.value_type)
        key_type = _convert_to_key_column_type(doris_type)
        columns.append(f"    {field.name} {key_type} NOT NULL")
        key_column_names.append(field.name)

    # Value columns
    for field in state.value_fields_schema:
        _validate_identifier(field.name)
        doris_type = _convert_value_type_to_doris_type(field.value_type)
        # Vector columns must be NOT NULL for index creation
        base_type = field.value_type.type
        if isinstance(base_type, BasicValueType) and base_type.kind == "Vector":
            nullable = "NOT NULL"
        else:
            nullable = "NULL" if field.value_type.nullable else "NOT NULL"
        columns.append(f"    {field.name} {doris_type} {nullable}")

    # Vector indexes (inline definition)
    for idx in state.vector_indexes or []:
        _validate_identifier(idx.name)
        _validate_identifier(idx.field_name)
        props = _build_vector_index_properties(idx)
        columns.append(
            f"    INDEX {idx.name} ({idx.field_name}) USING ANN PROPERTIES ({', '.join(props)})"
        )

    # Inverted indexes
    for inv_idx in state.inverted_indexes or []:
        _validate_identifier(inv_idx.name)
        _validate_identifier(inv_idx.field_name)
        if inv_idx.parser:
            columns.append(
                f'    INDEX {inv_idx.name} ({inv_idx.field_name}) USING INVERTED PROPERTIES ("parser" = "{inv_idx.parser}")'
            )
        else:
            columns.append(
                f"    INDEX {inv_idx.name} ({inv_idx.field_name}) USING INVERTED"
            )

    key_cols = ", ".join(key_column_names)

    # Handle "auto" buckets or fixed integer count
    buckets_clause = (
        "AUTO" if str(state.buckets).lower() == "auto" else str(state.buckets)
    )
    return f"""CREATE TABLE IF NOT EXISTS {key.database}.{key.table} (
{("," + chr(10)).join(columns)}
)
ENGINE = OLAP
DUPLICATE KEY({key_cols})
DISTRIBUTED BY HASH({key_cols}) BUCKETS {buckets_clause}
PROPERTIES (
    "replication_num" = "{state.replication_num}"
)"""


def _generate_stream_load_label() -> str:
    """Generate a unique label for Stream Load."""
    return f"cocoindex_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"


def _build_stream_load_headers(
    label: str, columns: list[str] | None = None
) -> dict[str, str]:
    """Build headers for Stream Load request."""
    headers = {
        "format": "json",
        "strip_outer_array": "true",
        "label": label,
        "Expect": "100-continue",
    }

    if columns:
        headers["columns"] = ", ".join(columns)

    return headers


# ============================================================
# STREAM LOAD
# ============================================================


async def _stream_load(
    session: "aiohttp.ClientSession",
    spec: DorisTarget,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Execute Stream Load for bulk data ingestion.

    Note: Deletes are handled via SQL DELETE (_execute_delete) instead of Stream Load
    because Doris 4.0 vector indexes only support DUPLICATE KEY tables, and Stream Load
    DELETE requires UNIQUE KEY model.
    """
    aiohttp = _get_aiohttp()

    if not rows:
        return {"Status": "Success", "NumberLoadedRows": 0}

    protocol = "https" if spec.enable_https else "http"
    url = f"{protocol}://{spec.fe_host}:{spec.fe_http_port}/api/{spec.database}/{spec.table}/_stream_load"

    label = _generate_stream_load_label()
    # Collect ALL unique columns across all rows to avoid data loss
    # (first row may not have all optional fields)
    all_columns: set[str] = set()
    for row in rows:
        all_columns.update(row.keys())
    columns = sorted(all_columns)  # Sort for consistent ordering
    headers = _build_stream_load_headers(label, columns)

    data = json.dumps(rows, ensure_ascii=False)

    async def do_stream_load() -> dict[str, Any]:
        async with session.put(
            url,
            data=data,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=spec.stream_load_timeout),
        ) as response:
            # Check for auth errors
            if response.status in (401, 403):
                raise DorisAuthError(
                    f"Authentication failed: HTTP {response.status}",
                    host=spec.fe_host,
                    port=spec.fe_http_port,
                )

            # Parse response - VeloDB/Doris may return wrong Content-Type
            text = await response.text()
            try:
                result: dict[str, Any] = json.loads(text)
            except json.JSONDecodeError:
                raise DorisStreamLoadError(
                    message=f"Invalid JSON response: {text[:200]}",
                    status="ParseError",
                )

            # Use case-insensitive status check for robustness
            # (different Doris versions may return different case)
            status = result.get("Status", "Unknown")
            status_upper = status.upper() if isinstance(status, str) else ""
            if status_upper not in ("SUCCESS", "PUBLISH TIMEOUT"):
                raise DorisStreamLoadError(
                    message=result.get("Message", "Unknown error"),
                    status=status,
                    error_url=result.get("ErrorURL"),
                    loaded_rows=result.get("NumberLoadedRows", 0),
                    filtered_rows=result.get("NumberFilteredRows", 0),
                )

            return result

    retry_config = RetryConfig(
        max_retries=spec.max_retries,
        base_delay=spec.retry_base_delay,
        max_delay=spec.retry_max_delay,
    )
    return await with_retry(
        do_stream_load,
        config=retry_config,
        operation_name="Stream Load",
    )


# ============================================================
# MYSQL CONNECTION (for DDL)
# ============================================================


async def _execute_ddl(
    spec: DorisTarget,
    sql: str,
) -> list[dict[str, Any]]:
    """Execute DDL via MySQL protocol using aiomysql."""
    try:
        import aiomysql  # type: ignore
    except ImportError:
        raise ImportError(
            "aiomysql is required for Doris DDL operations. "
            "Install it with: pip install aiomysql"
        )

    async def do_execute() -> list[dict[str, Any]]:
        conn = await aiomysql.connect(
            host=spec.fe_host,
            port=spec.query_port,
            user=spec.username,
            password=spec.password,
            db=spec.database if spec.database else None,
            autocommit=True,
        )
        try:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(sql)
                try:
                    result = await cursor.fetchall()
                    return list(result)
                except aiomysql.ProgrammingError as e:
                    # "no result set" error is expected for DDL statements
                    # that don't return results (CREATE, DROP, ALTER, etc.)
                    if "no result set" in str(e).lower():
                        return []
                    raise  # Re-raise other programming errors
        finally:
            conn.close()
            await conn.ensure_closed()

    retry_config = RetryConfig(
        max_retries=spec.max_retries,
        base_delay=spec.retry_base_delay,
        max_delay=spec.retry_max_delay,
    )
    return await with_retry(
        do_execute,
        config=retry_config,
        operation_name="DDL execution",
    )


async def _table_exists(spec: DorisTarget, database: str, table: str) -> bool:
    """Check if a table exists."""
    try:
        result = await _execute_ddl(spec, f"SHOW TABLES FROM {database} LIKE '{table}'")
        return len(result) > 0
    except Exception as e:  # pylint: disable=broad-except
        _logger.warning("Failed to check table existence: %s", e)
        return False


async def _get_table_schema(
    spec: DorisTarget, database: str, table: str
) -> dict[str, _ColumnInfo] | None:
    """
    Query the actual table schema from Doris using DESCRIBE.

    Returns a dict mapping column_name -> _ColumnInfo, or None if table doesn't exist.
    Raises DorisError for other query failures (connection errors, permission issues, etc.).
    """
    try:
        import aiomysql  # type: ignore
    except ImportError:
        raise ImportError(
            "aiomysql is required for Doris operations. "
            "Install it with: pip install aiomysql"
        )

    try:
        result = await _execute_ddl(spec, f"DESCRIBE `{database}`.`{table}`")
        if not result:
            return None

        columns: dict[str, _ColumnInfo] = {}
        for row in result:
            col_name = row.get("Field", "")
            col_type = row.get("Type", "")
            nullable = row.get("Null", "YES") == "YES"
            is_key = row.get("Key", "") == "true"

            # Extract dimension from ARRAY<FLOAT> type if present
            # Format: ARRAY<FLOAT> or ARRAY<FLOAT>(384)
            dimension: int | None = None
            if col_type.upper().startswith("ARRAY"):
                dim_match = re.search(r"\((\d+)\)", col_type)
                if dim_match:
                    dimension = int(dim_match.group(1))

            columns[col_name] = _ColumnInfo(
                name=col_name,
                doris_type=col_type,
                nullable=nullable,
                is_key=is_key,
                dimension=dimension,
            )
        return columns
    except aiomysql.Error as e:
        # MySQL error 1146: Table doesn't exist
        # MySQL error 1049: Unknown database
        # VeloDB also uses error 1105 with "Unknown table" message
        error_code = e.args[0] if e.args else 0
        error_msg = str(e.args[1]) if len(e.args) > 1 else ""
        if error_code in (1146, 1049):
            _logger.debug("Table not found: %s.%s", database, table)
            return None
        if error_code == 1105 and "unknown table" in error_msg.lower():
            _logger.debug("Table not found (VeloDB): %s.%s", database, table)
            return None
        # Re-raise other MySQL errors (connection issues, permissions, etc.)
        raise DorisError(f"Failed to get table schema: {e}") from e


async def _get_table_model(spec: DorisTarget, database: str, table: str) -> str | None:
    """
    Get the table model (DUPLICATE KEY, UNIQUE KEY, or AGGREGATE KEY) from
    SHOW CREATE TABLE.

    Returns the model type string or None if table doesn't exist or can't be determined.
    """
    try:
        result = await _execute_ddl(spec, f"SHOW CREATE TABLE `{database}`.`{table}`")
        if not result:
            return None

        create_stmt = result[0].get("Create Table", "")

        # Parse the table model from CREATE TABLE statement
        # Format: DUPLICATE KEY(`col1`, `col2`)
        # Format: UNIQUE KEY(`col1`)
        # Format: AGGREGATE KEY(`col1`)
        if "DUPLICATE KEY" in create_stmt.upper():
            return "DUPLICATE KEY"
        elif "UNIQUE KEY" in create_stmt.upper():
            return "UNIQUE KEY"
        elif "AGGREGATE KEY" in create_stmt.upper():
            return "AGGREGATE KEY"
        else:
            return None
    except Exception as e:  # pylint: disable=broad-except
        _logger.debug("Failed to get table model: %s", e)
        return None


async def _create_database_if_not_exists(spec: DorisTarget, database: str) -> None:
    """Create database if it doesn't exist."""
    _validate_identifier(database)
    # Create a spec with no database to execute CREATE DATABASE
    temp_spec = dataclasses.replace(spec, database="")
    await _execute_ddl(temp_spec, f"CREATE DATABASE IF NOT EXISTS {database}")


async def _execute_delete(
    spec: DorisTarget,
    key_field_names: list[str],
    key_values: list[dict[str, Any]],
) -> int:
    """
    Execute DELETE via SQL for DUPLICATE KEY tables.

    Stream Load DELETE requires UNIQUE KEY model, but Doris 4.0 vector indexes
    only support DUPLICATE KEY model. So we use standard SQL DELETE instead.

    Uses parameterized queries via aiomysql for safe value escaping.
    For single keys, uses efficient IN clause.
    For composite keys, deletes one row at a time (Doris doesn't support
    OR with AND predicates in DELETE WHERE clause).

    Args:
        spec: Doris connection spec
        key_field_names: Names of the key columns
        key_values: List of key dictionaries to delete

    Returns:
        Number of rows deleted
    """
    if not key_values:
        return 0

    try:
        import aiomysql  # type: ignore
    except ImportError:
        raise ImportError(
            "aiomysql is required for Doris delete operations. "
            "Install it with: pip install aiomysql"
        )

    # Validate identifiers to prevent SQL injection (values are parameterized)
    _validate_identifier(spec.database)
    _validate_identifier(spec.table)
    for field_name in key_field_names:
        _validate_identifier(field_name)

    total_deleted = 0
    is_composite_key = len(key_field_names) > 1

    retry_config = RetryConfig(
        max_retries=spec.max_retries,
        base_delay=spec.retry_base_delay,
        max_delay=spec.retry_max_delay,
    )

    if not is_composite_key:
        # Single key column - use efficient IN clause for batch delete
        async def do_single_key_delete() -> int:
            conn = await aiomysql.connect(
                host=spec.fe_host,
                port=spec.query_port,
                user=spec.username,
                password=spec.password,
                db=spec.database,
                autocommit=True,
            )
            try:
                async with conn.cursor() as cursor:
                    field_name = key_field_names[0]
                    placeholders = ", ".join(["%s"] * len(key_values))
                    sql = f"DELETE FROM `{spec.database}`.`{spec.table}` WHERE `{field_name}` IN ({placeholders})"
                    params = tuple(kv.get(field_name) for kv in key_values)
                    _logger.debug(
                        "Executing batched DELETE: %s with %d keys",
                        sql[:100],
                        len(key_values),
                    )
                    await cursor.execute(sql, params)
                    return int(cursor.rowcount) if cursor.rowcount else 0
            finally:
                conn.close()
                await conn.ensure_closed()

        total_deleted = await with_retry(
            do_single_key_delete,
            config=retry_config,
            operation_name="SQL DELETE",
        )
    else:
        # Composite key - Doris DELETE doesn't support OR with AND predicates,
        # so we must delete one row at a time. We reuse a single connection
        # for efficiency and wrap the whole batch in retry logic.
        condition_parts = [f"`{name}` = %s" for name in key_field_names]
        sql_template = (
            f"DELETE FROM `{spec.database}`.`{spec.table}` "
            f"WHERE {' AND '.join(condition_parts)}"
        )

        # Prepare all params upfront
        all_params = [
            tuple(kv.get(name) for name in key_field_names) for kv in key_values
        ]

        async def do_composite_deletes() -> int:
            """Execute all composite key deletes using a single connection."""
            conn = await aiomysql.connect(
                host=spec.fe_host,
                port=spec.query_port,
                user=spec.username,
                password=spec.password,
                db=spec.database,
                autocommit=True,
            )
            try:
                deleted_count = 0
                async with conn.cursor() as cursor:
                    for params in all_params:
                        _logger.debug("Executing DELETE for composite key: %s", params)
                        await cursor.execute(sql_template, params)
                        deleted_count += int(cursor.rowcount) if cursor.rowcount else 0
                return deleted_count
            finally:
                conn.close()
                await conn.ensure_closed()

        # Retry the entire batch - DELETE is idempotent so safe to retry
        total_deleted = await with_retry(
            do_composite_deletes,
            config=retry_config,
            operation_name=f"SQL DELETE (composite keys, {len(all_params)} rows)",
        )

    return total_deleted


async def _wait_for_schema_change(
    spec: DorisTarget,
    key: "_TableKey",
    timeout: int = 60,
) -> bool:
    """
    Wait for ALTER TABLE schema changes to complete.

    Doris tables go through SCHEMA_CHANGE state during DDL operations.
    We need to wait for the table to return to NORMAL before issuing
    another DDL command.

    Returns True if schema change completed successfully.
    Raises DorisSchemaError if the schema change was cancelled/failed.
    Returns False on timeout.
    """
    start_time = time.time()
    poll_interval = 1.0

    while time.time() - start_time < timeout:
        try:
            result = await _execute_ddl(
                spec,
                f"SHOW ALTER TABLE COLUMN FROM `{key.database}` "
                f"WHERE TableName = '{key.table}' ORDER BY CreateTime DESC LIMIT 1",
            )
            if not result:
                # No ongoing ALTER operations
                return True

            state = result[0].get("State", "FINISHED")
            if state == "FINISHED":
                return True
            elif state == "CANCELLED":
                msg = result[0].get("Msg", "Unknown reason")
                raise DorisSchemaError(
                    f"Schema change on table {key.table} was cancelled: {msg}"
                )

            _logger.debug("Waiting for schema change on %s: %s", key.table, state)
        except DorisSchemaError:
            raise
        except Exception as e:  # pylint: disable=broad-except
            _logger.debug("Error checking schema change state: %s", e)

        await asyncio.sleep(poll_interval)

    _logger.warning("Timeout waiting for schema change on table %s", key.table)
    return False


async def _wait_for_index_build(
    spec: DorisTarget,
    key: "_TableKey",
    index_name: str,
    timeout: int = 300,
) -> bool:
    """
    Wait for BUILD INDEX to complete using SHOW BUILD INDEX.

    Index builds can take significant time on large tables.
    This properly monitors the index build progress.

    Returns True if build completed successfully.
    Raises DorisSchemaError if the build was cancelled/failed.
    Returns False on timeout.
    """
    start_time = time.time()
    poll_interval = 2.0

    while time.time() - start_time < timeout:
        try:
            # SHOW BUILD INDEX shows the status of index build jobs
            result = await _execute_ddl(
                spec,
                f"SHOW BUILD INDEX FROM `{key.database}` "
                f"WHERE TableName = '{key.table}' ORDER BY CreateTime DESC LIMIT 5",
            )

            if not result:
                # No build jobs found - might have completed quickly
                return True

            # Check if any build job for our index is still running
            for row in result:
                # Check if this is our index (exact match to avoid idx_emb matching idx_emb_v2)
                row_index_name = str(row.get("IndexName", "")).strip()
                if row_index_name == index_name:
                    state = row.get("State", "FINISHED")
                    if state == "FINISHED":
                        return True
                    elif state in ("CANCELLED", "FAILED"):
                        msg = row.get("Msg", "Unknown reason")
                        raise DorisSchemaError(
                            f"Index build {index_name} failed with state {state}: {msg}"
                        )
                    else:
                        _logger.debug("Index build %s state: %s", index_name, state)
                        break
            else:
                # No matching index found in results, assume completed
                return True

        except DorisSchemaError:
            raise
        except Exception as e:  # pylint: disable=broad-except
            _logger.debug("Error checking index build state: %s", e)

        await asyncio.sleep(poll_interval)

    _logger.warning(
        "Timeout waiting for index build %s on table %s", index_name, key.table
    )
    return False


async def _sync_indexes(
    spec: DorisTarget,
    key: "_TableKey",
    previous: "_State | None",
    current: "_State",
    actual_schema: dict[str, "_ColumnInfo"] | None = None,
) -> None:
    """
    Synchronize indexes when table already exists.

    Handles adding/removing vector and inverted indexes.
    Waits for schema changes and index builds to complete before proceeding.

    Args:
        spec: Doris target specification
        key: Table identifier
        previous: Previous state (may be None)
        current: Current state
        actual_schema: Actual table schema from database (for validation in extend mode)
    """
    # Determine which indexes to drop and which to add
    prev_vec_idx = {
        idx.name: idx for idx in (previous.vector_indexes if previous else []) or []
    }
    curr_vec_idx = {idx.name: idx for idx in (current.vector_indexes or [])}

    prev_inv_idx = {
        idx.name: idx for idx in (previous.inverted_indexes if previous else []) or []
    }
    curr_inv_idx = {idx.name: idx for idx in (current.inverted_indexes or [])}

    # Find indexes to drop (in previous but not in current, or changed)
    vec_to_drop = set(prev_vec_idx.keys()) - set(curr_vec_idx.keys())
    inv_to_drop = set(prev_inv_idx.keys()) - set(curr_inv_idx.keys())

    # Also drop if index definition changed
    for name in set(prev_vec_idx.keys()) & set(curr_vec_idx.keys()):
        if prev_vec_idx[name] != curr_vec_idx[name]:
            vec_to_drop.add(name)

    for name in set(prev_inv_idx.keys()) & set(curr_inv_idx.keys()):
        if prev_inv_idx[name] != curr_inv_idx[name]:
            inv_to_drop.add(name)

    # Find indexes to add (in current but not in previous, or changed)
    vec_to_add = set(curr_vec_idx.keys()) - set(prev_vec_idx.keys())
    inv_to_add = set(curr_inv_idx.keys()) - set(prev_inv_idx.keys())

    # Also add if index definition changed
    for name in set(prev_vec_idx.keys()) & set(curr_vec_idx.keys()):
        if prev_vec_idx[name] != curr_vec_idx[name]:
            vec_to_add.add(name)

    for name in set(prev_inv_idx.keys()) & set(curr_inv_idx.keys()):
        if prev_inv_idx[name] != curr_inv_idx[name]:
            inv_to_add.add(name)

    # Drop old indexes
    dropped_any = False
    for idx_name in vec_to_drop | inv_to_drop:
        try:
            await _execute_ddl(
                spec,
                f"DROP INDEX `{idx_name}` ON `{key.database}`.`{key.table}`",
            )
            _logger.info("Dropped index %s", idx_name)
            dropped_any = True
        except Exception as e:  # pylint: disable=broad-except
            _logger.warning("Failed to drop index %s: %s", idx_name, e)

    # Wait for schema change to complete before creating new indexes
    if dropped_any and (vec_to_add or inv_to_add):
        if not await _wait_for_schema_change(
            spec, key, timeout=spec.schema_change_timeout
        ):
            raise DorisSchemaError(
                f"Timeout waiting for DROP INDEX to complete on table {key.table}"
            )

    # Add new vector indexes with column validation
    for idx_name in vec_to_add:
        idx = curr_vec_idx[idx_name]

        # Validate column compatibility if actual schema is provided
        if actual_schema is not None:
            _validate_vector_index_column(idx, actual_schema)

        try:
            # Wait for any pending schema changes before CREATE INDEX
            if not await _wait_for_schema_change(
                spec, key, timeout=spec.schema_change_timeout
            ):
                raise DorisSchemaError(
                    f"Timeout waiting for schema change before creating index {idx_name}"
                )

            # Create vector index
            props = _build_vector_index_properties(idx)
            await _execute_ddl(
                spec,
                f"CREATE INDEX `{idx.name}` ON `{key.database}`.`{key.table}` (`{idx.field_name}`) "
                f"USING ANN PROPERTIES ({', '.join(props)})",
            )

            # Build index and wait for completion
            await _execute_ddl(
                spec,
                f"BUILD INDEX `{idx.name}` ON `{key.database}`.`{key.table}`",
            )

            # Wait for index build to complete using SHOW BUILD INDEX
            if not await _wait_for_index_build(
                spec, key, idx.name, timeout=spec.index_build_timeout
            ):
                raise DorisSchemaError(
                    f"Timeout waiting for index build {idx.name} to complete"
                )

            _logger.info("Created and built vector index %s", idx.name)
        except DorisSchemaError:
            raise
        except Exception as e:  # pylint: disable=broad-except
            _logger.warning("Failed to create vector index %s: %s", idx.name, e)

    # Add new inverted indexes with column validation
    for idx_name in inv_to_add:
        inv_idx = curr_inv_idx[idx_name]

        # Validate column compatibility if actual schema is provided
        if actual_schema is not None:
            _validate_inverted_index_column(inv_idx, actual_schema)

        try:
            # Wait for any pending schema changes before CREATE INDEX
            if not await _wait_for_schema_change(
                spec, key, timeout=spec.schema_change_timeout
            ):
                raise DorisSchemaError(
                    f"Timeout waiting for schema change before creating index {idx_name}"
                )

            if inv_idx.parser:
                await _execute_ddl(
                    spec,
                    f"CREATE INDEX `{inv_idx.name}` ON `{key.database}`.`{key.table}` (`{inv_idx.field_name}`) "
                    f'USING INVERTED PROPERTIES ("parser" = "{inv_idx.parser}")',
                )
            else:
                await _execute_ddl(
                    spec,
                    f"CREATE INDEX `{inv_idx.name}` ON `{key.database}`.`{key.table}` (`{inv_idx.field_name}`) "
                    f"USING INVERTED",
                )
            _logger.info("Created inverted index %s", inv_idx.name)
        except DorisSchemaError:
            raise
        except Exception as e:  # pylint: disable=broad-except
            _logger.warning("Failed to create inverted index %s: %s", inv_idx.name, e)


def _validate_vector_index_column(
    idx: "_VectorIndex", actual_schema: dict[str, "_ColumnInfo"]
) -> None:
    """Validate that a column is compatible with a vector index.

    Raises DorisSchemaError if the column is missing or incompatible.
    """
    # Check 1: Column must exist
    if idx.field_name not in actual_schema:
        raise DorisSchemaError(
            f"Cannot create vector index '{idx.name}': "
            f"column '{idx.field_name}' does not exist in table. "
            f"Available columns: {list(actual_schema.keys())}"
        )

    col = actual_schema[idx.field_name]

    # Check 2: Must be ARRAY<FLOAT> type
    if not col.doris_type.upper().startswith("ARRAY"):
        raise DorisSchemaError(
            f"Cannot create vector index '{idx.name}': "
            f"column '{idx.field_name}' has type '{col.doris_type}', "
            f"expected ARRAY<FLOAT>. Vector indexes require array types."
        )

    # Check 3: Must be NOT NULL
    if col.nullable:
        raise DorisSchemaError(
            f"Cannot create vector index '{idx.name}': "
            f"column '{idx.field_name}' is nullable. "
            f"Vector indexes require NOT NULL columns. "
            f"Use ALTER TABLE to set the column to NOT NULL."
        )

    # Check 4: Dimension must match (if we know it)
    if col.dimension is not None and col.dimension != idx.dimension:
        raise DorisSchemaError(
            f"Cannot create vector index '{idx.name}': "
            f"dimension mismatch - column has {col.dimension} dimensions, "
            f"but index expects {idx.dimension} dimensions."
        )


def _validate_inverted_index_column(
    idx: "_InvertedIndex", actual_schema: dict[str, "_ColumnInfo"]
) -> None:
    """Validate that a column is compatible with an inverted index.

    Raises DorisSchemaError if the column is missing or incompatible.
    """
    # Check 1: Column must exist
    if idx.field_name not in actual_schema:
        raise DorisSchemaError(
            f"Cannot create inverted index '{idx.name}': "
            f"column '{idx.field_name}' does not exist in table. "
            f"Available columns: {list(actual_schema.keys())}"
        )

    col = actual_schema[idx.field_name]

    # Check 2: Must be a text-compatible type
    text_types = {"TEXT", "STRING", "VARCHAR", "CHAR"}
    col_type_upper = col.doris_type.upper()
    is_text_type = any(col_type_upper.startswith(t) for t in text_types)

    if not is_text_type:
        raise DorisSchemaError(
            f"Cannot create inverted index '{idx.name}': "
            f"column '{idx.field_name}' has type '{col.doris_type}', "
            f"expected TEXT, VARCHAR, or STRING. "
            f"Inverted indexes require text-compatible column types."
        )


# ============================================================
# CONNECTOR IMPLEMENTATION
# ============================================================


@op.target_connector(
    spec_cls=DorisTarget, persistent_key_type=_TableKey, setup_state_cls=_State
)
class _Connector:
    @staticmethod
    def get_persistent_key(spec: DorisTarget) -> _TableKey:
        return _TableKey(
            fe_host=spec.fe_host,
            database=spec.database,
            table=spec.table,
        )

    @staticmethod
    def get_setup_state(
        spec: DorisTarget,
        key_fields_schema: list[FieldSchema],
        value_fields_schema: list[FieldSchema],
        index_options: IndexOptions,
    ) -> _State:
        if len(key_fields_schema) == 0:
            raise ValueError("Doris requires at least one key field")

        # Extract vector indexes
        vector_indexes: list[_VectorIndex] | None = None
        if index_options.vector_indexes:
            vector_indexes = []
            for idx in index_options.vector_indexes:
                metric_type = _get_doris_metric_type(idx.metric)
                # Skip cosine similarity as it doesn't support index
                if metric_type == "cosine_distance":
                    continue

                dimension = _get_vector_dimension(value_fields_schema, idx.field_name)

                # Skip vector index if dimension is not available
                # The field will be stored as JSON instead
                if dimension is None:
                    _logger.warning(
                        "Field '%s' does not have a fixed vector dimension. "
                        "It will be stored as JSON and vector index will not be created. "
                        "Only vectors with fixed dimensions support ARRAY<FLOAT> storage "
                        "and vector indexing in Doris.",
                        idx.field_name,
                    )
                    continue

                # Determine index type and parameters from method
                index_type = "hnsw"  # Default to HNSW
                max_degree: int | None = None
                ef_construction: int | None = None
                nlist: int | None = None

                if idx.method is not None:
                    if isinstance(idx.method, HnswVectorIndexMethod):
                        index_type = "hnsw"
                        # m in HNSW corresponds to max_degree in Doris
                        max_degree = idx.method.m
                        ef_construction = idx.method.ef_construction
                    elif isinstance(idx.method, IvfFlatVectorIndexMethod):
                        index_type = "ivf"
                        # lists in IVFFlat corresponds to nlist in Doris
                        nlist = idx.method.lists

                vector_indexes.append(
                    _VectorIndex(
                        name=f"idx_{idx.field_name}_ann",
                        field_name=idx.field_name,
                        index_type=index_type,
                        metric_type=metric_type,
                        dimension=dimension,
                        max_degree=max_degree,
                        ef_construction=ef_construction,
                        nlist=nlist,
                    )
                )
            if not vector_indexes:
                vector_indexes = None

        # Extract FTS indexes
        inverted_indexes: list[_InvertedIndex] | None = None
        if index_options.fts_indexes:
            inverted_indexes = [
                _InvertedIndex(
                    name=f"idx_{idx.field_name}_inv",
                    field_name=idx.field_name,
                    parser=idx.parameters.get("parser") if idx.parameters else None,
                )
                for idx in index_options.fts_indexes
            ]

        return _State(
            key_fields_schema=key_fields_schema,
            value_fields_schema=value_fields_schema,
            vector_indexes=vector_indexes,
            inverted_indexes=inverted_indexes,
            replication_num=spec.replication_num,
            buckets=spec.buckets,
            # Store connection credentials for apply_setup_change
            fe_http_port=spec.fe_http_port,
            query_port=spec.query_port,
            username=spec.username,
            password=spec.password,
            max_retries=spec.max_retries,
            retry_base_delay=spec.retry_base_delay,
            retry_max_delay=spec.retry_max_delay,
            schema_change_timeout=spec.schema_change_timeout,
            index_build_timeout=spec.index_build_timeout,
            auto_create_table=spec.auto_create_table,
            schema_evolution=spec.schema_evolution,
        )

    @staticmethod
    def describe(key: _TableKey) -> str:
        return f"Doris table {key.database}.{key.table}@{key.fe_host}"

    @staticmethod
    def check_state_compatibility(
        previous: _State, current: _State
    ) -> op.TargetStateCompatibility:
        # Key schema change → always incompatible (requires table recreation)
        if previous.key_fields_schema != current.key_fields_schema:
            return op.TargetStateCompatibility.NOT_COMPATIBLE

        # Check schema evolution mode
        is_extend_mode = current.schema_evolution == "extend"

        # Value schema: check for removed columns
        prev_field_names = {f.name for f in previous.value_fields_schema}
        curr_field_names = {f.name for f in current.value_fields_schema}

        # Columns removed from schema (in previous but not in current)
        removed_columns = prev_field_names - curr_field_names
        if removed_columns:
            if is_extend_mode:
                # In extend mode: columns removed from schema are OK
                # (we'll keep them in DB, just won't manage them)
                _logger.info(
                    "Extend mode: columns removed from schema will be kept in DB: %s",
                    removed_columns,
                )
            else:
                # In strict mode: removing columns is incompatible
                return op.TargetStateCompatibility.NOT_COMPATIBLE

        # Check type changes for columns that exist in BOTH schemas
        prev_fields = {f.name: f for f in previous.value_fields_schema}
        for field in current.value_fields_schema:
            if field.name in prev_fields:
                # Type changes are always incompatible (can't ALTER column type)
                if prev_fields[field.name].value_type.type != field.value_type.type:
                    return op.TargetStateCompatibility.NOT_COMPATIBLE

        # Index changes (vector or inverted) don't require table recreation.
        # They are handled in apply_setup_change via _sync_indexes().

        return op.TargetStateCompatibility.COMPATIBLE

    @staticmethod
    async def apply_setup_change(
        key: _TableKey, previous: _State | None, current: _State | None
    ) -> None:
        if current is None and previous is None:
            return

        # Get a spec for DDL execution - use current or previous state
        state = current or previous
        if state is None:
            return

        is_extend_mode = state.schema_evolution == "extend"

        # Create a spec for DDL execution with credentials from state
        spec = DorisTarget(
            fe_host=key.fe_host,
            database=key.database,
            table=key.table,
            fe_http_port=state.fe_http_port,
            query_port=state.query_port,
            username=state.username,
            password=state.password,
            max_retries=state.max_retries,
            retry_base_delay=state.retry_base_delay,
            retry_max_delay=state.retry_max_delay,
        )

        # Handle target removal
        if current is None:
            # In extend mode, we don't drop tables on target removal
            if not is_extend_mode:
                try:
                    await _execute_ddl(
                        spec, f"DROP TABLE IF EXISTS `{key.database}`.`{key.table}`"
                    )
                except Exception as e:  # pylint: disable=broad-except
                    _logger.warning("Failed to drop table: %s", e)
            return

        # Check if we need to drop and recreate (key schema change)
        key_schema_changed = (
            previous is not None
            and previous.key_fields_schema != current.key_fields_schema
        )

        if key_schema_changed:
            # Key schema change always requires table recreation
            try:
                await _execute_ddl(
                    spec, f"DROP TABLE IF EXISTS `{key.database}`.`{key.table}`"
                )
            except Exception as e:  # pylint: disable=broad-except
                _logger.warning("Failed to drop table: %s", e)

        # Create database if not exists (only if auto_create_table is enabled)
        if current.auto_create_table:
            await _create_database_if_not_exists(spec, key.database)

        # Query actual table schema from database
        actual_schema = await _get_table_schema(spec, key.database, key.table)

        if actual_schema is None:
            # Table doesn't exist - create it
            if not current.auto_create_table:
                raise DorisSchemaError(
                    f"Table {key.database}.{key.table} does not exist and "
                    f"auto_create_table is disabled"
                )

            ddl = _generate_create_table_ddl(key, current)
            _logger.info("Creating table with DDL:\n%s", ddl)
            await _execute_ddl(spec, ddl)

            # Build vector indexes (async operation in Doris)
            for idx in current.vector_indexes or []:
                try:
                    await _execute_ddl(
                        spec,
                        f"BUILD INDEX {idx.name} ON `{key.database}`.`{key.table}`",
                    )
                except Exception as e:  # pylint: disable=broad-except
                    _logger.warning("Failed to build index %s: %s", idx.name, e)
            return

        # Table exists - validate table model first
        # Vector indexes require DUPLICATE KEY model in Doris 4.0+
        table_model = await _get_table_model(spec, key.database, key.table)
        if table_model and table_model != "DUPLICATE KEY":
            raise DorisSchemaError(
                f"Table {key.database}.{key.table} uses {table_model} model, "
                f"but Doris requires DUPLICATE KEY model for vector index support. "
                f"Please drop the table and recreate it with DUPLICATE KEY model."
            )

        # Validate key columns
        desired_key_names = {f.name for f in current.key_fields_schema}
        actual_key_names = {name for name, col in actual_schema.items() if col.is_key}

        # Check key column mismatch
        if desired_key_names != actual_key_names:
            raise DorisSchemaError(
                f"Key column mismatch for table {key.database}.{key.table}: "
                f"expected keys {sorted(desired_key_names)}, "
                f"but table has keys {sorted(actual_key_names)}. "
                f"To fix this, either update the schema to match or drop the table."
            )

        # Validate key column types
        for field in current.key_fields_schema:
            if field.name in actual_schema:
                expected_type = _convert_value_type_to_doris_type(field.value_type)
                expected_type = _convert_to_key_column_type(expected_type)
                actual_type = actual_schema[field.name].doris_type
                if not _types_compatible(expected_type, actual_type):
                    raise DorisSchemaError(
                        f"Key column '{field.name}' type mismatch: "
                        f"expected '{expected_type}', but table has '{actual_type}'"
                    )

        # Now handle value columns based on schema evolution mode
        actual_columns = set(actual_schema.keys())
        desired_columns = {
            f.name for f in current.key_fields_schema + current.value_fields_schema
        }

        # Check extra columns in DB
        extra_columns = actual_columns - desired_columns
        if extra_columns:
            if is_extend_mode:
                _logger.info(
                    "Extend mode: keeping extra columns in DB not in schema: %s",
                    extra_columns,
                )
            else:
                # Strict mode: extra columns are not allowed
                raise DorisSchemaError(
                    f"Strict mode: table {key.database}.{key.table} has extra columns "
                    f"not in schema: {sorted(extra_columns)}. "
                    f"Either add these columns to the schema or drop the table."
                )

        # Add missing columns (only value columns can be added)
        missing_columns = desired_columns - actual_columns
        missing_key_columns = missing_columns & desired_key_names
        if missing_key_columns:
            raise DorisSchemaError(
                f"Table {key.database}.{key.table} is missing key columns: "
                f"{sorted(missing_key_columns)}. Key columns cannot be added via ALTER TABLE."
            )

        for field in current.value_fields_schema:
            if field.name in missing_columns:
                _validate_identifier(field.name)
                doris_type = _convert_value_type_to_doris_type(field.value_type)
                base_type = field.value_type.type

                # Determine nullable and default value for ALTER TABLE
                # When adding columns to existing tables, NOT NULL columns need defaults
                default_clause = ""
                if isinstance(base_type, BasicValueType) and base_type.kind == "Vector":
                    # Vector columns must be NOT NULL for index creation
                    # Use empty array as default for existing rows
                    nullable = "NOT NULL"
                    default_clause = " DEFAULT '[]'"
                    _logger.warning(
                        "Adding vector column %s with empty default. "
                        "Existing rows will have empty vectors until data is populated.",
                        field.name,
                    )
                elif field.value_type.nullable:
                    nullable = "NULL"
                else:
                    # NOT NULL columns need DEFAULT for existing rows
                    nullable = "NOT NULL"
                    # Set appropriate defaults based on type
                    if doris_type in ("TEXT", "STRING"):
                        default_clause = " DEFAULT ''"
                    elif doris_type == "BIGINT":
                        default_clause = " DEFAULT 0"
                    elif doris_type in ("FLOAT", "DOUBLE"):
                        default_clause = " DEFAULT 0.0"
                    elif doris_type == "BOOLEAN":
                        default_clause = " DEFAULT FALSE"
                    elif doris_type == "JSON":
                        default_clause = " DEFAULT '{}'"
                    else:
                        # For complex types, use NULL instead
                        nullable = "NULL"

                try:
                    await _execute_ddl(
                        spec,
                        f"ALTER TABLE `{key.database}`.`{key.table}` "
                        f"ADD COLUMN `{field.name}` {doris_type} {nullable}{default_clause}",
                    )
                    _logger.info("Added column %s to table %s", field.name, key.table)

                    # Wait for schema change to complete before proceeding
                    # Doris tables go through SCHEMA_CHANGE state during ALTER TABLE
                    # and reject writes to newly added columns until complete
                    await _wait_for_schema_change(
                        spec, key, timeout=spec.schema_change_timeout
                    )

                    # Update actual_schema with the new column
                    actual_schema[field.name] = _ColumnInfo(
                        name=field.name,
                        doris_type=doris_type,
                        nullable=nullable == "NULL",
                        is_key=False,
                        dimension=_extract_vector_dimension(field.value_type),
                    )
                except Exception as e:  # pylint: disable=broad-except
                    _logger.warning("Failed to add column %s: %s", field.name, e)

        # Verify type compatibility for existing columns
        for field in current.value_fields_schema:
            if field.name in actual_schema and field.name not in missing_columns:
                expected_type = _convert_value_type_to_doris_type(field.value_type)
                actual_type = actual_schema[field.name].doris_type
                # Normalize types for comparison (basic check)
                if not _types_compatible(expected_type, actual_type):
                    raise DorisSchemaError(
                        f"Column '{field.name}' type mismatch: "
                        f"DB has '{actual_type}', schema expects '{expected_type}'"
                    )

        # Handle index changes with actual schema validation
        await _sync_indexes(spec, key, previous, current, actual_schema)

    @staticmethod
    async def prepare(
        spec: DorisTarget,
        setup_state: _State,
    ) -> _MutateContext:
        aiohttp = _get_aiohttp()
        session = aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(spec.username, spec.password),
        )
        return _MutateContext(
            spec=spec,
            session=session,
            state=setup_state,
            lock=asyncio.Lock(),
        )

    @staticmethod
    async def mutate(
        *all_mutations: tuple[_MutateContext, dict[Any, dict[str, Any] | None]],
    ) -> None:
        for context, mutations in all_mutations:
            upserts: list[dict[str, Any]] = []
            deletes: list[dict[str, Any]] = []

            key_field_names = [f.name for f in context.state.key_fields_schema]

            for key, value in mutations.items():
                # Build key dict
                if isinstance(key, tuple):
                    key_dict = {
                        name: _convert_value_for_doris(k)
                        for name, k in zip(key_field_names, key)
                    }
                else:
                    key_dict = {key_field_names[0]: _convert_value_for_doris(key)}

                if value is None:
                    deletes.append(key_dict)
                else:
                    # Build full row
                    row = {**key_dict}
                    for field in context.state.value_fields_schema:
                        if field.name in value:
                            row[field.name] = _convert_value_for_doris(
                                value[field.name]
                            )
                    upserts.append(row)

            async with context.lock:
                # For DUPLICATE KEY tables, we must delete existing rows before inserting
                # to prevent accumulating duplicate rows with the same key.
                # This ensures idempotent upsert behavior.
                #
                # WARNING: This is NOT atomic. If stream load fails after deletes,
                # the deleted rows are lost. Use smaller batch_size to minimize risk.
                if upserts:
                    # Extract keys from upserts for deletion
                    upsert_keys = [
                        {name: row[name] for name in key_field_names} for row in upserts
                    ]

                    # Delete existing rows first
                    deleted_count = 0
                    for i in range(0, len(upsert_keys), context.spec.batch_size):
                        batch = upsert_keys[i : i + context.spec.batch_size]
                        deleted_count += await _execute_delete(
                            context.spec, key_field_names, batch
                        )

                    # Process inserts in batches via Stream Load
                    # If this fails, deleted rows cannot be recovered
                    try:
                        for i in range(0, len(upserts), context.spec.batch_size):
                            batch = upserts[i : i + context.spec.batch_size]
                            await _stream_load(context.session, context.spec, batch)
                    except Exception as e:
                        if deleted_count > 0:
                            _logger.error(
                                "Stream Load failed after deleting %d rows. "
                                "Data loss may have occurred. Error: %s",
                                deleted_count,
                                e,
                            )
                        raise

                # Process explicit deletes in batches via SQL DELETE
                for i in range(0, len(deletes), context.spec.batch_size):
                    batch = deletes[i : i + context.spec.batch_size]
                    await _execute_delete(context.spec, key_field_names, batch)

    @staticmethod
    async def cleanup(context: _MutateContext) -> None:
        """Clean up resources used by the mutation context.

        This closes the aiohttp session that was created in prepare().
        """
        if context.session is not None:
            await context.session.close()


# ============================================================
# PUBLIC HELPERS
# ============================================================


async def connect_async(
    fe_host: str,
    query_port: int = 9030,
    username: str = "root",
    password: str = "",
    database: str | None = None,
) -> Any:
    """
    Helper function to connect to a Doris database via MySQL protocol.
    Returns an aiomysql connection for query operations.

    Usage:
        conn = await connect_async("localhost", database="my_db")
        try:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT * FROM my_table LIMIT 10")
                rows = await cursor.fetchall()
        finally:
            conn.close()
            await conn.ensure_closed()
    """
    try:
        import aiomysql  # type: ignore
    except ImportError:
        raise ImportError(
            "aiomysql is required for Doris connections. "
            "Install it with: pip install aiomysql"
        )

    return await aiomysql.connect(
        host=fe_host,
        port=query_port,
        user=username,
        password=password,
        db=database,
        autocommit=True,
    )


def build_vector_search_query(
    table: str,
    vector_field: str,
    query_vector: list[float],
    metric: str = "l2_distance",
    limit: int = 10,
    select_columns: list[str] | None = None,
    where_clause: str | None = None,
) -> str:
    """
    Build a vector search query for Doris.

    Args:
        table: Table name (database.table format supported). Names are
               validated and quoted with backticks to prevent SQL injection.
        vector_field: Name of the vector column (validated and quoted)
        query_vector: Query vector as a list of floats
        metric: Distance metric ("l2_distance" or "inner_product")
        limit: Number of results to return (must be positive integer)
        select_columns: Columns to select (validated and quoted, default: all)
        where_clause: Optional WHERE clause for filtering.
                      WARNING: This is NOT escaped. Caller must ensure proper
                      escaping of any user input to prevent SQL injection.

    Returns:
        SQL query string

    Raises:
        ValueError: If table, vector_field, or select_columns contain
                   invalid characters that could indicate SQL injection.

    Note:
        Uses _approximate suffix for functions to leverage vector index.
    """
    # Validate and quote table name (supports database.table format)
    table_parts = table.split(".")
    if len(table_parts) == 2:
        _validate_identifier(table_parts[0])
        _validate_identifier(table_parts[1])
        quoted_table = f"`{table_parts[0]}`.`{table_parts[1]}`"
    elif len(table_parts) == 1:
        _validate_identifier(table)
        quoted_table = f"`{table}`"
    else:
        raise ValueError(f"Invalid table name format: {table}")

    # Validate and quote vector field
    _validate_identifier(vector_field)
    quoted_vector_field = f"`{vector_field}`"

    # Validate limit
    if not isinstance(limit, int) or limit <= 0:
        raise ValueError(f"limit must be a positive integer, got: {limit}")

    # Use approximate functions to leverage index
    if metric == "l2_distance":
        distance_fn = "l2_distance_approximate"
        order = "ASC"  # Smaller distance = more similar
    elif metric == "inner_product":
        distance_fn = "inner_product_approximate"
        order = "DESC"  # Larger product = more similar
    else:
        # Validate metric for safety
        if not metric.isidentifier():
            raise ValueError(f"Invalid metric name: {metric}")
        distance_fn = metric
        order = "ASC" if "distance" in metric else "DESC"

    # Format vector as array literal (safe - only floats)
    vector_literal = "[" + ", ".join(str(float(v)) for v in query_vector) + "]"

    # Build SELECT clause
    if select_columns:
        # Validate and quote each column
        quoted_columns = []
        for col in select_columns:
            _validate_identifier(col)
            quoted_columns.append(f"`{col}`")
        select = ", ".join(quoted_columns)
    else:
        select = "*"

    # Build query
    query = f"""SELECT {select}, {distance_fn}({quoted_vector_field}, {vector_literal}) as _distance
FROM {quoted_table}"""

    if where_clause:
        # WARNING: where_clause is NOT escaped
        query += f"\nWHERE {where_clause}"

    query += f"\nORDER BY _distance {order}\nLIMIT {limit}"

    return query

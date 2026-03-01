"""
Apache Doris target connector for CocoIndex (v1 API).

Follows the same two-level TargetHandler pattern as the SQLite connector:
1. Table level (_TableHandler): manages DDL — CREATE/ALTER/DROP TABLE + indexes
2. Row level (_RowHandler): manages DML — upsert/delete via Stream Load + SQL DELETE

Supports:
- Vector index (HNSW, IVF) with L2 distance and inner product metrics
- Inverted index for full-text search
- Stream Load for bulk data ingestion
- Incremental updates with upsert/delete operations
- Schema evolution (extend / strict mode)

Requirements:
- Doris 4.0+ with vector index support
- DUPLICATE KEY table model (required for vector indexes)
- Python packages: aiohttp, aiomysql (pymysql for sync DDL)
"""

from __future__ import annotations

import asyncio
import datetime
import decimal
import json
import logging
import math
import re
import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import (
    Any,
    Awaitable,
    Callable,
    Collection,
    Generic,
    Literal,
    NamedTuple,
    TYPE_CHECKING,
)

import numpy as np
from typing_extensions import TypeVar

import cocoindex as coco
from cocoindex.connectorkits import connection, statediff
from cocoindex.connectorkits.fingerprint import fingerprint_object
from cocoindex._internal.datatype import (
    AnyType,
    MappingType,
    RecordType,
    SequenceType,
    TypeChecker,
    UnionType,
    analyze_type_info,
    is_record_type,
)
from cocoindex._internal.api import mount_target as _mount_target
from cocoindex.resources.schema import VectorSchemaProvider

if TYPE_CHECKING:
    import aiohttp  # type: ignore[import-not-found]

_logger = logging.getLogger(__name__)

# ============================================================
# Type aliases
# ============================================================

_RowKey = tuple[Any, ...]
_ROW_KEY_CHECKER = TypeChecker(tuple[Any, ...])
_RowValue = dict[str, Any]
_RowFingerprint = bytes
ValueEncoder = Callable[[Any], Any]
RowT = TypeVar("RowT", default=dict[str, Any])
T = TypeVar("T")


# ============================================================
# Lazy imports
# ============================================================


def _get_aiohttp() -> Any:
    """Lazily import aiohttp."""
    try:
        import aiohttp  # type: ignore[import-not-found]

        return aiohttp
    except ImportError:
        raise ImportError(
            "aiohttp is required for Doris connector. "
            "Install it with: pip install aiohttp"
        )


def _get_pymysql() -> Any:
    """Lazily import pymysql (sync MySQL client for DDL)."""
    try:
        import pymysql  # type: ignore[import-not-found]

        return pymysql
    except ImportError:
        raise ImportError(
            "pymysql is required for Doris connector DDL operations. "
            "Install it with: pip install pymysql"
        )


# ============================================================
# JSON encoder for numpy arrays
# ============================================================


class _NumpyEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if hasattr(obj, "tolist"):
            return obj.tolist()
        return super().default(obj)


# ============================================================
# Error classes
# ============================================================


class DorisError(Exception):
    """Base class for Doris connector errors."""


class DorisConnectionError(DorisError):
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
    def __init__(self, message: str, field_name: str | None = None):
        self.field_name = field_name
        super().__init__(message)


# ============================================================
# Retry logic
# ============================================================


@dataclass
class RetryConfig:
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 30.0
    exponential_base: float = 2.0


def _is_retryable_error(e: Exception) -> bool:
    """Check if an error is retryable (transient)."""
    if isinstance(
        e,
        (
            asyncio.TimeoutError,
            ConnectionError,
            ConnectionResetError,
            ConnectionRefusedError,
        ),
    ):
        return True
    try:
        aiohttp = _get_aiohttp()
        if isinstance(
            e, (aiohttp.ClientConnectorError, aiohttp.ServerDisconnectedError)
        ):
            return True
    except ImportError:
        pass
    try:
        pymysql = _get_pymysql()
        if isinstance(e, pymysql.err.OperationalError):
            if e.args and len(e.args) > 0:
                retryable_codes = {2003, 2006, 2013, 1040, 1205}
                return e.args[0] in retryable_codes
        if isinstance(e, pymysql.err.InterfaceError):
            return True
    except ImportError:
        pass
    return False


async def _with_retry(
    operation: Callable[[], Awaitable[T]],
    config: RetryConfig = RetryConfig(),
    operation_name: str = "operation",
) -> T:
    last_error: Exception | None = None
    for attempt in range(config.max_retries + 1):
        try:
            return await operation()
        except Exception as e:
            if not _is_retryable_error(e):
                raise
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
# Type mapping: Python -> Doris SQL
# ============================================================


class _TypeMapping(NamedTuple):
    doris_type: str
    encoder: ValueEncoder | None = None


_LEAF_TYPE_MAPPINGS: dict[type, _TypeMapping] = {
    bool: _TypeMapping("BOOLEAN"),
    int: _TypeMapping("BIGINT"),
    float: _TypeMapping("DOUBLE"),
    decimal.Decimal: _TypeMapping("TEXT", str),
    str: _TypeMapping("TEXT"),
    bytes: _TypeMapping("STRING"),
    uuid.UUID: _TypeMapping("VARCHAR(36)", str),
    datetime.date: _TypeMapping("DATE", lambda v: v.isoformat()),
    datetime.time: _TypeMapping("VARCHAR(20)", lambda v: v.isoformat()),
    datetime.datetime: _TypeMapping("DATETIME(6)", lambda v: v.isoformat()),
    datetime.timedelta: _TypeMapping(
        "BIGINT", lambda v: int(v.total_seconds() * 1_000_000)
    ),
}

_JSON_MAPPING = _TypeMapping("JSON", lambda v: json.dumps(v, default=str))


class DorisType(NamedTuple):
    """Annotation to override default Doris type mapping via typing.Annotated."""

    doris_type: str
    encoder: ValueEncoder | None = None


async def _get_type_mapping(
    python_type: Any, *, vector_schema_provider: VectorSchemaProvider | None = None
) -> _TypeMapping:
    type_info = analyze_type_info(python_type)

    for annotation in type_info.annotations:
        if isinstance(annotation, DorisType):
            return _TypeMapping(annotation.doris_type, annotation.encoder)

    base_type = type_info.base_type

    if base_type in _LEAF_TYPE_MAPPINGS:
        return _LEAF_TYPE_MAPPINGS[base_type]

    if base_type is np.ndarray:
        if vector_schema_provider is None:
            raise ValueError("VectorSchemaProvider is required for NumPy ndarray type.")
        schema = await vector_schema_provider.__coco_vector_schema__()
        if schema.size <= 0:
            raise ValueError(f"Invalid vector dimension: {schema.size}")
        return _TypeMapping(
            "ARRAY<FLOAT>",
            lambda v: v.tolist() if hasattr(v, "tolist") else list(v),
        )
    elif vector_schema_provider is not None:
        raise ValueError(
            f"VectorSchemaProvider only supported for ndarray. Got: {python_type}"
        )

    if isinstance(
        type_info.variant, (SequenceType, MappingType, RecordType, UnionType, AnyType)
    ):
        return _JSON_MAPPING

    return _JSON_MAPPING


# ============================================================
# Column / Table definitions
# ============================================================


class ColumnDef(NamedTuple):
    type: str  # Doris SQL type
    nullable: bool = True
    encoder: ValueEncoder | None = None
    is_vector: bool = False
    vector_dimension: int | None = None  # dimension for ARRAY<FLOAT> columns


@dataclass(slots=True)
class TableSchema(Generic[RowT]):
    columns: dict[str, ColumnDef]
    primary_key: list[str]
    row_type: type[RowT] | None

    def __init__(
        self,
        columns: dict[str, ColumnDef],
        primary_key: list[str],
        *,
        row_type: type[RowT] | None = None,
    ) -> None:
        self.columns = columns
        self.primary_key = primary_key
        self.row_type = row_type
        for pk in self.primary_key:
            if pk not in self.columns:
                raise ValueError(
                    f"PK column '{pk}' not in columns: {list(self.columns.keys())}"
                )

    @classmethod
    async def from_class(
        cls,
        record_type: type[RowT],
        primary_key: list[str],
        *,
        column_overrides: dict[str, DorisType | VectorSchemaProvider] | None = None,
    ) -> "TableSchema[RowT]":
        if not is_record_type(record_type):
            raise TypeError(
                f"record_type must be a record type, got {type(record_type)}"
            )
        columns = await cls._columns_from_record_type(record_type, column_overrides)
        return cls(columns, primary_key, row_type=record_type)

    @staticmethod
    async def _columns_from_record_type(
        record_type: type,
        column_overrides: dict[str, DorisType | VectorSchemaProvider] | None,
    ) -> dict[str, ColumnDef]:
        record_info = RecordType(record_type)
        columns: dict[str, ColumnDef] = {}

        for f in record_info.fields:
            override = column_overrides.get(f.name) if column_overrides else None
            type_info = analyze_type_info(f.type_hint)

            doris_type_annotation: DorisType | None = None
            vector_schema_provider: VectorSchemaProvider | None = None
            for ann in type_info.annotations:
                if isinstance(ann, DorisType):
                    doris_type_annotation = ann
                elif isinstance(ann, VectorSchemaProvider):
                    vector_schema_provider = ann

            if isinstance(override, DorisType):
                doris_type_annotation = override
            elif isinstance(override, VectorSchemaProvider):
                vector_schema_provider = override

            if doris_type_annotation is not None:
                mapping = _TypeMapping(
                    doris_type_annotation.doris_type, doris_type_annotation.encoder
                )
            else:
                mapping = await _get_type_mapping(
                    f.type_hint, vector_schema_provider=vector_schema_provider
                )

            dim: int | None = None
            if vector_schema_provider is not None:
                schema = await vector_schema_provider.__coco_vector_schema__()
                dim = schema.size

            columns[f.name] = ColumnDef(
                type=mapping.doris_type.strip(),
                nullable=type_info.nullable,
                encoder=mapping.encoder,
                is_vector=(vector_schema_provider is not None),
                vector_dimension=dim,
            )

        return columns


# ============================================================
# Doris connection
# ============================================================


@dataclass
class DorisConnectionConfig:
    """Configuration for a Doris connection."""

    fe_host: str
    database: str
    fe_http_port: int = 8080
    query_port: int = 9030
    username: str = "root"
    password: str = ""
    enable_https: bool = False
    be_load_host: str | None = None
    batch_size: int = 10000
    stream_load_timeout: int = 600
    max_retries: int = 3
    retry_base_delay: float = 1.0
    retry_max_delay: float = 30.0
    schema_change_timeout: int = 60
    index_build_timeout: int = 300
    replication_num: int = 1
    buckets: int | str = "auto"


@dataclass
class ManagedConnection:
    """A Doris connection wrapper with an aiohttp session and config."""

    config: DorisConnectionConfig
    _session: "aiohttp.ClientSession | None" = field(default=None, repr=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    def _ensure_session(self) -> "aiohttp.ClientSession":
        if self._session is None or self._session.closed:
            aiohttp = _get_aiohttp()
            self._session = aiohttp.ClientSession(
                auth=aiohttp.BasicAuth(self.config.username, self.config.password),
            )
        return self._session

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
            self._session = None


def connect(config: DorisConnectionConfig) -> ManagedConnection:
    """Create a ManagedConnection from a DorisConnectionConfig."""
    return ManagedConnection(config=config)


# ============================================================
# SQL helpers
# ============================================================


def _validate_identifier(name: str) -> None:
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
        raise DorisSchemaError(f"Invalid identifier: {name}")


def _convert_to_key_column_type(doris_type: str) -> str:
    if doris_type in ("TEXT", "STRING"):
        return "VARCHAR(512)"
    return doris_type


def _convert_value_for_doris(value: Any) -> Any:
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
    if hasattr(value, "tolist"):
        return value.tolist()
    return value


def _execute_ddl_sync(config: DorisConnectionConfig, sql: str) -> None:
    """Execute a DDL statement via MySQL protocol (synchronous)."""
    pymysql = _get_pymysql()
    conn = pymysql.connect(
        host=config.fe_host,
        port=config.query_port,
        user=config.username,
        password=config.password,
        database=config.database,
        autocommit=True,
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
    finally:
        conn.close()


def _query_sync(config: DorisConnectionConfig, sql: str) -> list[tuple[Any, ...]]:
    """Execute a query via MySQL protocol (synchronous)."""
    pymysql = _get_pymysql()
    conn = pymysql.connect(
        host=config.fe_host,
        port=config.query_port,
        user=config.username,
        password=config.password,
        database=config.database,
        autocommit=True,
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            return list(cursor.fetchall())
    finally:
        conn.close()


# ============================================================
# Stream Load
# ============================================================


def _generate_stream_load_label() -> str:
    return f"cocoindex_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"


async def _stream_load(
    managed_conn: ManagedConnection,
    table_name: str,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Execute Stream Load for bulk data ingestion."""
    aiohttp_mod = _get_aiohttp()
    config = managed_conn.config

    if not rows:
        return {"Status": "Success", "NumberLoadedRows": 0}

    session = managed_conn._ensure_session()
    protocol = "https" if config.enable_https else "http"
    url = f"{protocol}://{config.fe_host}:{config.fe_http_port}/api/{config.database}/{table_name}/_stream_load"

    label = _generate_stream_load_label()
    all_columns: set[str] = set()
    for row in rows:
        all_columns.update(row.keys())
    columns = sorted(all_columns)
    headers = {
        "format": "json",
        "strip_outer_array": "true",
        "label": label,
        "Expect": "100-continue",
    }
    if columns:
        headers["columns"] = ", ".join(columns)

    data = json.dumps(rows, ensure_ascii=False, cls=_NumpyEncoder)

    async def do_stream_load() -> dict[str, Any]:
        load_timeout = aiohttp_mod.ClientTimeout(total=config.stream_load_timeout)

        if config.be_load_host:

            async def _send(target_url: str) -> tuple[int, str, str]:
                async with session.put(
                    target_url,
                    data=data,
                    headers=headers,
                    timeout=load_timeout,
                    allow_redirects=False,
                ) as resp:
                    return (
                        resp.status,
                        resp.headers.get("Location", ""),
                        await resp.text(),
                    )

            status_code, location, text = await _send(url)
            if status_code == 307 and location:
                from urllib.parse import urlparse, urlunparse

                parsed = urlparse(location)
                rewritten = urlunparse(
                    parsed._replace(
                        netloc=f"{config.be_load_host}:{parsed.port or config.fe_http_port}"
                    )
                )
                status_code, _, text = await _send(rewritten)
        else:
            async with session.put(
                url, data=data, headers=headers, timeout=load_timeout
            ) as response:
                status_code = response.status
                text = await response.text()

        if status_code in (401, 403):
            raise DorisAuthError(
                f"Authentication failed: HTTP {status_code}",
                host=config.fe_host,
                port=config.fe_http_port,
            )

        try:
            result: dict[str, Any] = json.loads(text)
        except json.JSONDecodeError:
            raise DorisStreamLoadError(
                f"Invalid response: {text[:200]}", status="ParseError"
            )

        load_status = result.get("Status", "Unknown")
        if load_status not in ("Success", "Publish Timeout"):
            raise DorisStreamLoadError(
                result.get("Message", "Unknown error"),
                status=load_status,
                error_url=result.get("ErrorURL"),
                loaded_rows=result.get("NumberLoadedRows", 0),
                filtered_rows=result.get("NumberFilteredRows", 0),
            )
        return result

    retry_config = RetryConfig(
        max_retries=config.max_retries,
        base_delay=config.retry_base_delay,
        max_delay=config.retry_max_delay,
    )
    return await _with_retry(
        do_stream_load, retry_config, f"Stream Load to {table_name}"
    )


async def _execute_delete(
    config: DorisConnectionConfig,
    table_name: str,
    key_columns: list[str],
    key_rows: list[dict[str, Any]],
) -> int:
    """Delete rows by primary key via SQL DELETE."""
    if not key_rows:
        return 0

    _validate_identifier(config.database)
    _validate_identifier(table_name)

    conditions = []
    for row in key_rows:
        parts = []
        for col in key_columns:
            val = row[col]
            if val is None:
                parts.append(f"`{col}` IS NULL")
            elif isinstance(val, str):
                escaped = val.replace("'", "\\'")
                parts.append(f"`{col}` = '{escaped}'")
            else:
                parts.append(f"`{col}` = {val}")
        conditions.append("(" + " AND ".join(parts) + ")")

    sql = f"DELETE FROM `{config.database}`.`{table_name}` WHERE {' OR '.join(conditions)}"

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _execute_ddl_sync, config, sql)
    return len(key_rows)


# ============================================================
# DDL generation
# ============================================================


@dataclass
class VectorIndexDef:
    """Vector index configuration."""

    field_name: str
    index_type: str = "HNSW"  # "HNSW" or "IVF"
    metric_type: str = (
        "l2_distance"  # "l2_distance", "inner_product", "cosine_distance"
    )
    max_degree: int | None = None
    ef_construction: int | None = None
    nlist: int | None = None


@dataclass
class InvertedIndexDef:
    """Inverted index for full-text search."""

    field_name: str
    parser: str | None = None  # "chinese", "english", "unicode", etc.


def _generate_create_table_ddl(
    database: str,
    table_name: str,
    schema: TableSchema[Any],
    *,
    replication_num: int = 1,
    buckets: int | str = "auto",
    vector_indexes: list[VectorIndexDef] | None = None,
    inverted_indexes: list[InvertedIndexDef] | None = None,
) -> str:
    _validate_identifier(database)
    _validate_identifier(table_name)

    col_defs = []
    pk_cols = schema.primary_key

    for col_name, col_def in schema.columns.items():
        _validate_identifier(col_name)
        doris_type = col_def.type
        if col_name in pk_cols:
            doris_type = _convert_to_key_column_type(doris_type)
            col_defs.append(f"    `{col_name}` {doris_type} NOT NULL")
        else:
            if col_def.is_vector:
                col_defs.append(f"    `{col_name}` {doris_type} NOT NULL")
            else:
                nullable = "NULL" if col_def.nullable else "NOT NULL"
                col_defs.append(f"    `{col_name}` {doris_type} {nullable}")

    # Vector indexes
    for idx in vector_indexes or []:
        _validate_identifier(idx.field_name)
        idx_name = f"idx_vec_{idx.field_name}"
        _validate_identifier(idx_name)
        dim = schema.columns.get(idx.field_name)
        dim_val = dim.vector_dimension if dim else None
        props = [
            f'"index_type" = "{idx.index_type.lower()}"',
            f'"metric_type" = "{idx.metric_type.lower()}"',
        ]
        if dim_val:
            props.append(f'"dim" = "{dim_val}"')
        if idx.max_degree is not None:
            props.append(f'"max_degree" = "{idx.max_degree}"')
        if idx.ef_construction is not None:
            props.append(f'"ef_construction" = "{idx.ef_construction}"')
        if idx.nlist is not None:
            props.append(f'"nlist" = "{idx.nlist}"')
        col_defs.append(
            f"    INDEX {idx_name} (`{idx.field_name}`) USING ANN PROPERTIES ({', '.join(props)})"
        )

    # Inverted indexes
    for inv in inverted_indexes or []:
        _validate_identifier(inv.field_name)
        idx_name = f"idx_inv_{inv.field_name}"
        _validate_identifier(idx_name)
        if inv.parser:
            col_defs.append(
                f'    INDEX {idx_name} (`{inv.field_name}`) USING INVERTED PROPERTIES ("parser" = "{inv.parser}")'
            )
        else:
            col_defs.append(f"    INDEX {idx_name} (`{inv.field_name}`) USING INVERTED")

    pk_list = ", ".join(f"`{c}`" for c in pk_cols)
    buckets_clause = "AUTO" if str(buckets).lower() == "auto" else str(buckets)

    return f"""CREATE TABLE IF NOT EXISTS `{database}`.`{table_name}` (
{("," + chr(10)).join(col_defs)}
)
ENGINE = OLAP
DUPLICATE KEY({pk_list})
DISTRIBUTED BY HASH({pk_list}) BUCKETS {buckets_clause}
PROPERTIES (
    "replication_num" = "{replication_num}"
)"""


# ============================================================
# Row-level handler
# ============================================================


class _RowAction(NamedTuple):
    key: _RowKey
    value: _RowValue | None  # None = delete


class _RowHandler(coco.TargetHandler[_RowValue, _RowFingerprint]):
    """Handler for row-level target states within a Doris table."""

    _managed_conn: ManagedConnection
    _table_name: str
    _table_schema: TableSchema[Any]
    _sink: coco.TargetActionSink[_RowAction, None]

    def __init__(
        self,
        managed_conn: ManagedConnection,
        table_name: str,
        table_schema: TableSchema[Any],
    ) -> None:
        self._managed_conn = managed_conn
        self._table_name = table_name
        self._table_schema = table_schema
        self._sink = coco.TargetActionSink[_RowAction, None].from_fn(
            self._apply_actions
        )

    def _apply_actions(self, actions: Sequence[_RowAction]) -> None:
        """Apply row actions (upserts and deletes) to Doris."""
        if not actions:
            return

        upserts: list[_RowAction] = []
        deletes: list[_RowAction] = []

        for action in actions:
            if action.value is None:
                deletes.append(action)
            else:
                upserts.append(action)

        config = self._managed_conn.config
        pk_cols = self._table_schema.primary_key

        loop = asyncio.get_event_loop()

        if upserts:
            # Build rows for stream load
            rows_to_load: list[dict[str, Any]] = []
            upsert_keys: list[dict[str, Any]] = []
            for action in upserts:
                assert action.value is not None
                rows_to_load.append(action.value)
                upsert_keys.append({col: action.value[col] for col in pk_cols})

            # Delete existing rows first (DUPLICATE KEY needs delete-before-insert)
            batch_size = config.batch_size
            for i in range(0, len(upsert_keys), batch_size):
                batch = upsert_keys[i : i + batch_size]
                loop.run_until_complete(
                    _execute_delete(config, self._table_name, pk_cols, batch)
                )

            # Stream Load the new rows
            for i in range(0, len(rows_to_load), batch_size):
                batch = rows_to_load[i : i + batch_size]
                loop.run_until_complete(
                    _stream_load(self._managed_conn, self._table_name, batch)
                )

        if deletes:
            delete_keys = []
            for action in deletes:
                key_dict = {}
                for col_name, key_val in zip(pk_cols, action.key):
                    key_dict[col_name] = _convert_value_for_doris(key_val)
                delete_keys.append(key_dict)

            batch_size = config.batch_size
            for i in range(0, len(delete_keys), batch_size):
                batch = delete_keys[i : i + batch_size]
                loop.run_until_complete(
                    _execute_delete(config, self._table_name, pk_cols, batch)
                )

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: _RowValue | coco.NonExistenceType,
        prev_possible_states: Collection[_RowFingerprint],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_RowAction, _RowFingerprint] | None:
        key = _ROW_KEY_CHECKER.check(key)
        if coco.is_non_existence(desired_state):
            if not prev_possible_states and not prev_may_be_missing:
                return None
            return coco.TargetReconcileOutput(
                action=_RowAction(key=key, value=None),
                sink=self._sink,
                tracking_record=coco.NON_EXISTENCE,
            )

        target_fp = fingerprint_object(desired_state)
        if not prev_may_be_missing and all(
            prev == target_fp for prev in prev_possible_states
        ):
            return None

        return coco.TargetReconcileOutput(
            action=_RowAction(key=key, value=desired_state),
            sink=self._sink,
            tracking_record=target_fp,
        )


# ============================================================
# Table-level handler
# ============================================================


class _TableKey(NamedTuple):
    db_key: str
    table_name: str


_TABLE_KEY_CHECKER = TypeChecker(tuple[str, str])


@dataclass
class _TableSpec:
    table_schema: TableSchema[Any]
    managed_by: Literal["system", "user"] = "system"
    vector_indexes: list[VectorIndexDef] | None = None
    inverted_indexes: list[InvertedIndexDef] | None = None


class _PkColumnInfo(NamedTuple):
    name: str
    type: str


class _TablePrimaryTrackingRecord(NamedTuple):
    primary_key_columns: tuple[_PkColumnInfo, ...]
    vector_indexes: tuple[str, ...] | None = None
    inverted_indexes: tuple[str, ...] | None = None


class _NonPkColumnTrackingRecord(NamedTuple):
    type: str
    nullable: bool


_COL_SUBKEY_PREFIX: str = "col:"


def _col_subkey(col_name: str) -> str:
    return f"{_COL_SUBKEY_PREFIX}{col_name}"


_TableSubTrackingRecord = _NonPkColumnTrackingRecord | None


def _table_composite_tracking_record_from_spec(
    spec: _TableSpec,
) -> statediff.CompositeTrackingRecord[
    _TablePrimaryTrackingRecord, str, _TableSubTrackingRecord
]:
    schema = spec.table_schema
    col_by_name = schema.columns
    pk_sig = _TablePrimaryTrackingRecord(
        primary_key_columns=tuple(
            _PkColumnInfo(name=pk, type=col_by_name[pk].type)
            for pk in schema.primary_key
        ),
        vector_indexes=tuple(v.field_name for v in spec.vector_indexes)
        if spec.vector_indexes
        else None,
        inverted_indexes=tuple(v.field_name for v in spec.inverted_indexes)
        if spec.inverted_indexes
        else None,
    )
    sub: dict[str, _TableSubTrackingRecord] = {
        _col_subkey(col_name): _NonPkColumnTrackingRecord(
            type=col_def.type, nullable=col_def.nullable
        )
        for col_name, col_def in schema.columns.items()
        if col_name not in schema.primary_key
    }
    return statediff.CompositeTrackingRecord(main=pk_sig, sub=sub)


_TableTrackingRecord = statediff.MutualTrackingRecord[
    statediff.CompositeTrackingRecord[
        _TablePrimaryTrackingRecord, str, _TableSubTrackingRecord
    ]
]


class _TableAction(NamedTuple):
    key: _TableKey
    spec: _TableSpec | coco.NonExistenceType
    main_action: statediff.DiffAction | None
    column_actions: dict[str, statediff.DiffAction]


_db_registry: connection.ConnectionRegistry[ManagedConnection] = (
    connection.ConnectionRegistry("cocoindex/doris")
)


def _apply_table_actions(
    actions: Sequence[_TableAction],
) -> list[coco.ChildTargetDef["_RowHandler"] | None]:
    actions_list = list(actions)
    outputs: list[coco.ChildTargetDef[_RowHandler] | None] = [None] * len(actions_list)

    by_key: dict[_TableKey, list[int]] = {}
    for i, action in enumerate(actions_list):
        by_key.setdefault(action.key, []).append(i)

    for key, idxs in by_key.items():
        managed_conn = _db_registry.get(key.db_key)
        config = managed_conn.config

        for i in idxs:
            action = actions_list[i]
            assert action.key == key

            if action.main_action in ("replace", "delete"):
                # Drop table
                try:
                    _execute_ddl_sync(
                        config,
                        f"DROP TABLE IF EXISTS `{config.database}`.`{key.table_name}`",
                    )
                except Exception as e:
                    _logger.warning("Failed to drop table %s: %s", key.table_name, e)

            if coco.is_non_existence(action.spec):
                outputs[i] = None
                continue

            spec = action.spec
            outputs[i] = coco.ChildTargetDef(
                handler=_RowHandler(
                    managed_conn=managed_conn,
                    table_name=key.table_name,
                    table_schema=spec.table_schema,
                )
            )

            if action.main_action in ("insert", "upsert", "replace"):
                ddl = _generate_create_table_ddl(
                    config.database,
                    key.table_name,
                    spec.table_schema,
                    replication_num=config.replication_num,
                    buckets=config.buckets,
                    vector_indexes=spec.vector_indexes,
                    inverted_indexes=spec.inverted_indexes,
                )
                _execute_ddl_sync(config, ddl)
                continue

            # No main change: reconcile non-PK columns incrementally
            if action.column_actions:
                for sub_key, col_action in action.column_actions.items():
                    if not sub_key.startswith(_COL_SUBKEY_PREFIX):
                        continue
                    col_name = sub_key[len(_COL_SUBKEY_PREFIX) :]
                    if col_name in spec.table_schema.primary_key:
                        continue

                    col_def = spec.table_schema.columns.get(col_name)
                    if col_action == "delete":
                        try:
                            _execute_ddl_sync(
                                config,
                                f"ALTER TABLE `{config.database}`.`{key.table_name}` DROP COLUMN `{col_name}`",
                            )
                        except Exception:
                            pass
                    elif col_action in ("insert", "upsert") and col_def is not None:
                        nullable = "NULL" if col_def.nullable else "NOT NULL"
                        try:
                            _execute_ddl_sync(
                                config,
                                f"ALTER TABLE `{config.database}`.`{key.table_name}` "
                                f"ADD COLUMN `{col_name}` {col_def.type} {nullable}",
                            )
                        except Exception:
                            pass

    return outputs


_table_action_sink = coco.TargetActionSink[_TableAction, _RowHandler].from_fn(
    _apply_table_actions
)


class _TableHandler(coco.TargetHandler[_TableSpec, _TableTrackingRecord, _RowHandler]):
    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: _TableSpec | coco.NonExistenceType,
        prev_possible_states: Collection[_TableTrackingRecord],
        prev_may_be_missing: bool,
        /,
    ) -> (
        coco.TargetReconcileOutput[_TableAction, _TableTrackingRecord, _RowHandler]
        | None
    ):
        key = _TableKey(*_TABLE_KEY_CHECKER.check(key))
        tracking_record: _TableTrackingRecord | coco.NonExistenceType

        if coco.is_non_existence(desired_state):
            tracking_record = coco.NON_EXISTENCE
        else:
            tracking_record = statediff.MutualTrackingRecord(
                tracking_record=_table_composite_tracking_record_from_spec(
                    desired_state
                ),
                managed_by=desired_state.managed_by,
            )

        resolved = statediff.resolve_system_transition(
            statediff.TrackingRecordTransition(
                tracking_record,
                prev_possible_states,
                prev_may_be_missing,
            )
        )
        main_action, column_transitions = statediff.diff_composite(resolved)

        column_actions: dict[str, statediff.DiffAction] = {}
        if main_action is None:
            for sub_key, t in column_transitions.items():
                action = statediff.diff(t)
                if action is not None:
                    column_actions[sub_key] = action

        return coco.TargetReconcileOutput(
            action=_TableAction(
                key=key,
                spec=desired_state,
                main_action=main_action,
                column_actions=column_actions,
            ),
            sink=_table_action_sink,
            tracking_record=tracking_record,
        )


_table_provider = coco.register_root_target_states_provider(
    "cocoindex/doris/table", _TableHandler()
)


# ============================================================
# Public API
# ============================================================


class DorisDatabase(connection.KeyedConnection[ManagedConnection]):
    """Handle for a registered Doris database connection."""

    def declare_table_target(
        self,
        table_name: str,
        table_schema: TableSchema[RowT],
        *,
        managed_by: Literal["system", "user"] = "system",
        vector_indexes: list[VectorIndexDef] | None = None,
        inverted_indexes: list[InvertedIndexDef] | None = None,
    ) -> "DorisTableTarget[RowT, coco.PendingS]":
        provider = coco.declare_target_state_with_child(
            self.table_target(
                table_name,
                table_schema,
                managed_by=managed_by,
                vector_indexes=vector_indexes,
                inverted_indexes=inverted_indexes,
            )
        )
        return DorisTableTarget(provider, table_schema)

    def table_target(
        self,
        table_name: str,
        table_schema: TableSchema[RowT],
        *,
        managed_by: Literal["system", "user"] = "system",
        vector_indexes: list[VectorIndexDef] | None = None,
        inverted_indexes: list[InvertedIndexDef] | None = None,
    ) -> coco.TargetState[_RowHandler]:
        key = _TableKey(db_key=self.key, table_name=table_name)
        spec = _TableSpec(
            table_schema=table_schema,
            managed_by=managed_by,
            vector_indexes=vector_indexes,
            inverted_indexes=inverted_indexes,
        )
        return _table_provider.target_state(key, spec)

    async def mount_table_target(
        self,
        table_name: str,
        table_schema: TableSchema[RowT],
        *,
        managed_by: Literal["system", "user"] = "system",
        vector_indexes: list[VectorIndexDef] | None = None,
        inverted_indexes: list[InvertedIndexDef] | None = None,
    ) -> "DorisTableTarget[RowT]":
        provider = await _mount_target(
            self.table_target(
                table_name,
                table_schema,
                managed_by=managed_by,
                vector_indexes=vector_indexes,
                inverted_indexes=inverted_indexes,
            )
        )
        return DorisTableTarget(provider, table_schema)


class DorisTableTarget(
    Generic[RowT, coco.MaybePendingS], coco.ResolvesTo["DorisTableTarget[RowT]"]
):
    """A target for writing rows to a Doris table."""

    _provider: coco.TargetStateProvider[_RowValue, None, coco.MaybePendingS]
    _table_schema: TableSchema[RowT]

    def __init__(
        self,
        provider: coco.TargetStateProvider[_RowValue, None, coco.MaybePendingS],
        table_schema: TableSchema[RowT],
    ) -> None:
        self._provider = provider
        self._table_schema = table_schema

    def declare_row(self: "DorisTableTarget[RowT]", *, row: RowT) -> None:
        row_dict = self._row_to_dict(row)
        pk_values = tuple(row_dict[pk] for pk in self._table_schema.primary_key)
        coco.declare_target_state(self._provider.target_state(pk_values, row_dict))

    def _row_to_dict(self, row: RowT) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for col_name, col in self._table_schema.columns.items():
            if isinstance(row, dict):
                value = row.get(col_name)
            else:
                value = getattr(row, col_name)
            if value is not None and col.encoder is not None:
                value = col.encoder(value)
            out[col_name] = value
        return out

    def __coco_memo_key__(self) -> str:
        return self._provider.memo_key


def register_db(key: str, managed_conn: ManagedConnection) -> DorisDatabase:
    """
    Register a Doris database connection with a stable key.

    The key identifies the logical database and should be stable across runs.
    """
    _db_registry.register(key, managed_conn)
    return DorisDatabase(_db_registry.name, key, managed_conn, _db_registry)


# ============================================================
# Query helpers
# ============================================================


async def connect_async(
    fe_host: str,
    query_port: int = 9030,
    username: str = "root",
    password: str = "",
    database: str | None = None,
) -> Any:
    """Connect to Doris via MySQL protocol (async, using aiomysql)."""
    try:
        import aiomysql  # type: ignore
    except ImportError:
        raise ImportError(
            "aiomysql is required for async Doris connections. "
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
    """Build a vector search query for Doris."""
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

    _validate_identifier(vector_field)

    if metric == "l2_distance":
        distance_fn = "l2_distance_approximate"
        order = "ASC"
    elif metric == "inner_product":
        distance_fn = "inner_product_approximate"
        order = "DESC"
    else:
        if not metric.isidentifier():
            raise ValueError(f"Invalid metric name: {metric}")
        distance_fn = metric
        order = "ASC" if "distance" in metric else "DESC"

    vector_literal = "[" + ", ".join(str(float(v)) for v in query_vector) + "]"

    if select_columns:
        quoted_columns = []
        for col in select_columns:
            _validate_identifier(col)
            quoted_columns.append(f"`{col}`")
        select = ", ".join(quoted_columns)
    else:
        select = "*"

    query = f"""SELECT {select}, {distance_fn}(`{vector_field}`, {vector_literal}) as _distance
FROM {quoted_table}"""

    if where_clause:
        query += f"\nWHERE {where_clause}"

    query += f"\nORDER BY _distance {order}\nLIMIT {limit}"
    return query


__all__ = [
    "ColumnDef",
    "connect",
    "connect_async",
    "build_vector_search_query",
    "DorisConnectionConfig",
    "DorisDatabase",
    "DorisTableTarget",
    "DorisType",
    "InvertedIndexDef",
    "ManagedConnection",
    "register_db",
    "RetryConfig",
    "TableSchema",
    "ValueEncoder",
    "VectorIndexDef",
]

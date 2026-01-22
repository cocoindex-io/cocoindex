"""
LanceDB target for CocoIndex.

This module provides a two-level target state system for LanceDB:
1. Table level: Creates/drops tables in the database
2. Row level: Upserts/deletes rows within tables
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import threading
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Collection,
    Generic,
    Literal,
    NamedTuple,
    Sequence,
    overload,
)

from typing_extensions import TypeVar

try:
    import lancedb  # type: ignore
    import pyarrow as pa  # type: ignore
except ImportError as e:
    raise ImportError(
        "lancedb and pyarrow are required to use the LanceDB connector. Please install cocoindex[lancedb]."
    ) from e

import numpy as np

import cocoindex as coco
from cocoindex.connectorkits import statediff
from cocoindex._internal.datatype import (
    AnyType,
    MappingType,
    SequenceType,
    RecordType,
    UnionType,
    analyze_type_info,
    is_record_type,
)
from cocoindex.resources.schema import VectorSchema, VectorSchemaProvider, FtsSpec

# Type aliases
_RowKey = tuple[Any, ...]  # Primary key values as tuple
_RowValue = dict[str, Any]  # Column name -> value
_RowFingerprint = bytes
ValueEncoder = Callable[[Any], Any]

# Minimum rows required before creating indexes in LanceDB
_MIN_ROWS_FOR_INDEX = 256


class LanceType(NamedTuple):
    """
    Annotation to specify a LanceDB/PyArrow column type.

    Use with `typing.Annotated` to override the default type mapping:

    ```python
    from typing import Annotated
    from dataclasses import dataclass
    from cocoindex.connectors.lancedb import LanceType
    import pyarrow as pa

    @dataclass
    class MyRow:
        # Use int32 instead of default int64
        id: Annotated[int, LanceType(pa.int32())]
        # Use float32 instead of default float64
        value: Annotated[float, LanceType(pa.float32())]
    ```
    """

    pa_type: pa.DataType
    encoder: ValueEncoder | None = None


def _json_encoder(value: Any) -> str:
    """Encode a value to JSON string for LanceDB."""
    return json.dumps(value, default=str)


class _TypeMapping(NamedTuple):
    """Mapping from Python type to PyArrow type with optional encoder."""

    pa_type: pa.DataType
    encoder: ValueEncoder | None = None


# Global mapping for leaf types
# Maps Python types to PyArrow types based on LanceDB's supported types
_LEAF_TYPE_MAPPINGS: dict[type, _TypeMapping] = {
    # Boolean
    bool: _TypeMapping(pa.bool_()),
    # Numeric types
    int: _TypeMapping(pa.int64()),
    float: _TypeMapping(pa.float64()),
    # NumPy scalar integer types
    np.int8: _TypeMapping(pa.int8()),
    np.int16: _TypeMapping(pa.int16()),
    np.int32: _TypeMapping(pa.int32()),
    np.int64: _TypeMapping(pa.int64()),
    # NumPy scalar unsigned integer types
    np.uint8: _TypeMapping(pa.uint8()),
    np.uint16: _TypeMapping(pa.uint16()),
    np.uint32: _TypeMapping(pa.uint32()),
    np.uint64: _TypeMapping(pa.uint64()),
    # Platform-dependent aliases
    np.int_: _TypeMapping(pa.int64()),
    np.uint: _TypeMapping(pa.uint64()),
    # NumPy scalar float types
    np.float16: _TypeMapping(pa.float16()),
    np.float32: _TypeMapping(pa.float32()),
    np.float64: _TypeMapping(pa.float64()),
    # String types
    str: _TypeMapping(pa.string()),
    bytes: _TypeMapping(pa.binary()),
}

# Default mapping for complex types that need JSON encoding
_JSON_MAPPING = _TypeMapping(pa.string(), _json_encoder)


def _get_type_mapping(
    python_type: Any, *, vector_schema_provider: VectorSchemaProvider | None = None
) -> _TypeMapping:
    """
    Get the PyArrow type mapping for a Python type.

    For complex types that don't have direct PyArrow equivalents, we encode to JSON string.
    Use `LanceType` annotation with `typing.Annotated` to override the default.
    """
    type_info = analyze_type_info(python_type)

    # Check for LanceType annotation override
    for annotation in type_info.annotations:
        if isinstance(annotation, LanceType):
            return _TypeMapping(annotation.pa_type, annotation.encoder)

    base_type = type_info.base_type

    # Check direct leaf type mappings
    if base_type in _LEAF_TYPE_MAPPINGS:
        return _LEAF_TYPE_MAPPINGS[base_type]

    # NumPy ndarray: map to fixed-size list; dimension is handled at the schema layer
    if base_type is np.ndarray:
        if vector_schema_provider is None:
            raise ValueError("VectorSchemaProvider is required for NumPy ndarray type.")
        vector_schema = vector_schema_provider.__coco_vector_schema__()

        if vector_schema.size <= 0:
            raise ValueError(f"Invalid vector dimension: {vector_schema.size}")

        # Default to float32 for vectors; use float16 for half-precision
        pa_elem = (
            pa.float16()
            if vector_schema.dtype in (np.half, np.float16)
            else pa.float32()
        )
        # Create fixed-size list type for vector
        return _TypeMapping(pa.list_(pa_elem, list_size=vector_schema.size))

    elif vector_schema_provider is not None:
        raise ValueError(
            f"VectorSchemaProvider is only supported for NumPy ndarray type. Got type: {python_type}"
        )

    # Complex types that need JSON encoding
    if isinstance(
        type_info.variant, (SequenceType, MappingType, RecordType, UnionType, AnyType)
    ):
        return _JSON_MAPPING

    # Default fallback
    return _JSON_MAPPING


class ColumnDef(NamedTuple):
    """Definition of a table column."""

    type: pa.DataType  # PyArrow type
    nullable: bool = True
    encoder: ValueEncoder | None = (
        None  # Optional encoder to convert value before sending to LanceDB
    )


# Type variable for row type
RowT = TypeVar("RowT", default=dict[str, Any])


@dataclass(slots=True)
class TableSchema(Generic[RowT]):
    """Schema definition for a LanceDB table."""

    columns: dict[str, ColumnDef]  # column name -> definition
    primary_key: list[str]  # Column names that form the primary key
    row_type: type[RowT] | None  # The row type, if provided
    vector_schemas: dict[
        str, VectorSchema
    ]  # Column name -> VectorSpec for vector columns
    fts_specs: dict[str, FtsSpec]  # Column name -> FtsSpec for FTS columns

    @overload
    def __init__(
        self: "TableSchema[dict[str, Any]]",
        columns: dict[str, ColumnDef],
        primary_key: list[str],
    ) -> None: ...

    @overload
    def __init__(
        self: "TableSchema[RowT]",
        columns: type[RowT],
        primary_key: list[str],
        *,
        column_specs: dict[str, ColumnDef | VectorSchemaProvider | FtsSpec]
        | None = None,
    ) -> None: ...

    def __init__(
        self,
        columns: type[RowT] | dict[str, ColumnDef],
        primary_key: list[str],
        *,
        column_specs: dict[str, ColumnDef | VectorSchemaProvider | FtsSpec]
        | None = None,
    ) -> None:
        """
        Create a TableSchema.

        Args:
            columns: Either a record type (dataclass, NamedTuple, or Pydantic model)
                     or a dict mapping column names to ColumnDef.
                     When a record type is provided, Python types are automatically
                     mapped to PyArrow types.
            primary_key: List of column names that form the primary key.
            column_specs: Optional dict mapping column names to ColumnDef, VectorSpec, or FtsSpec.
                         VectorSpec is used for vector columns to specify dimension and metric.
                         FtsSpec is used for full-text search columns to specify tokenizer.
        """
        self.vector_schemas = {}
        self.fts_specs = {}

        if isinstance(columns, dict):
            self.columns = columns
            self.row_type = None
        elif is_record_type(columns):
            self.columns = self._columns_from_record_type(columns, column_specs)
            self.row_type = columns
        else:
            raise TypeError(
                f"columns must be a record type (dataclass, NamedTuple, Pydantic model) "
                f"or a dict[str, ColumnDef], got {type(columns)}"
            )

        self.primary_key = primary_key

        # Validate primary key columns exist
        for pk in self.primary_key:
            if pk not in self.columns:
                raise ValueError(
                    f"Primary key column '{pk}' not found in columns: {list(self.columns.keys())}"
                )

    def _columns_from_record_type(
        self,
        record_type: type,
        column_specs: dict[str, ColumnDef | VectorSchemaProvider | FtsSpec] | None,
    ) -> dict[str, ColumnDef]:
        """Convert a record type to a dict of column name -> ColumnDef."""
        record_info = RecordType(record_type)
        columns: dict[str, ColumnDef] = {}

        for field in record_info.fields:
            spec = column_specs.get(field.name) if column_specs else None

            # Handle ColumnDef override
            if isinstance(spec, ColumnDef):
                columns[field.name] = spec
                continue

            type_info = analyze_type_info(field.type_hint)

            # Handle VectorSchemaProvider
            vector_schema_provider = None
            for annotation in type_info.annotations:
                if isinstance(annotation, VectorSchemaProvider):
                    vector_schema_provider = annotation
                    break
            if isinstance(spec, VectorSchemaProvider):
                vector_schema_provider = spec

            if vector_schema_provider is not None:
                vector_schema = vector_schema_provider.__coco_vector_schema__()
                self.vector_schemas[field.name] = vector_schema
                if vector_schema.size <= 0:
                    raise ValueError(f"Invalid vector dimension: {vector_schema.size}")

            # Handle FtsSpec
            if isinstance(spec, FtsSpec):
                self.fts_specs[field.name] = spec

            # Get type mapping with vector_schema_provider if applicable
            type_mapping = _get_type_mapping(
                field.type_hint, vector_schema_provider=vector_schema_provider
            )

            columns[field.name] = ColumnDef(
                type=type_mapping.pa_type,
                nullable=type_info.nullable,
                encoder=type_mapping.encoder,
            )

        return columns


class _RowAction(NamedTuple):
    """Action to perform on a row."""

    key: _RowKey
    value: _RowValue | None  # None means delete


class _RowHandler(coco.TargetHandler[_RowKey, _RowValue, _RowFingerprint]):
    """Handler for row-level target states within a table."""

    _db_key: str
    _table_name: str
    _table_schema: TableSchema
    _sink: coco.TargetActionSink[_RowAction]

    def __init__(
        self,
        db_key: str,
        table_name: str,
        table_schema: TableSchema,
    ) -> None:
        self._db_key = db_key
        self._table_name = table_name
        self._table_schema = table_schema
        self._sink = coco.TargetActionSink.from_async_fn(self._apply_actions)

    async def _apply_actions(self, actions: Sequence[_RowAction]) -> None:
        """Apply row actions (upserts and deletes) to the database."""

        if not actions:
            return

        upserts: list[_RowAction] = []
        deletes: list[_RowAction] = []

        for action in actions:
            if action.value is None:
                deletes.append(action)
            else:
                upserts.append(action)

        conn = _get_connection(self._db_key)
        table = await conn.open_table(self._table_name)

        # Process upserts
        if upserts:
            await self._execute_upserts(table, upserts)

        # Process deletes
        if deletes:
            await self._execute_deletes(table, deletes)

    async def _execute_upserts(
        self,
        table: lancedb.table.AsyncTable,
        upserts: list[_RowAction],
    ) -> None:
        """Execute upsert operations using LanceDB's merge_insert."""
        # Prepare data as PyArrow record batch
        columns_data: dict[str, list[Any]] = {
            col_name: [] for col_name in self._table_schema.columns.keys()
        }

        for action in upserts:
            assert action.value is not None
            for col_name in self._table_schema.columns.keys():
                columns_data[col_name].append(action.value.get(col_name))

        # Build PyArrow schema
        pa_schema = self._build_pyarrow_schema()

        # Convert to PyArrow arrays
        arrays = []
        for col_name in self._table_schema.columns.keys():
            col_def = self._table_schema.columns[col_name]
            arrays.append(pa.array(columns_data[col_name], type=col_def.type))

        # Create record batch
        record_batch = pa.RecordBatch.from_arrays(arrays, schema=pa_schema)

        # Use merge_insert for upsert behavior
        # Primary key columns are used for matching
        pk_columns = self._table_schema.primary_key

        # Build merge_insert: match on primary key, update all on match, insert if not matched
        builder = (
            table.merge_insert(pk_columns[0] if len(pk_columns) == 1 else pk_columns)
            .when_matched_update_all()
            .when_not_matched_insert_all()
        )

        await builder.execute(record_batch)

    async def _execute_deletes(
        self,
        table: lancedb.table.AsyncTable,
        deletes: list[_RowAction],
    ) -> None:
        """Execute delete operations using LanceDB's delete."""
        pk_cols = self._table_schema.primary_key

        # Build delete conditions for each row
        # LanceDB delete syntax: table.delete("column = value")
        for action in deletes:
            conditions = []
            for i, pk_col in enumerate(pk_cols):
                pk_value = action.key[i]
                # Handle different types appropriately
                if isinstance(pk_value, str):
                    conditions.append(f"{pk_col} = '{pk_value}'")
                else:
                    conditions.append(f"{pk_col} = {pk_value}")

            condition = " AND ".join(conditions)
            await table.delete(condition)

    def _build_pyarrow_schema(self) -> pa.Schema:
        """Build PyArrow schema from table schema."""
        fields = []
        for col_name, col_def in self._table_schema.columns.items():
            field = pa.field(col_name, col_def.type, nullable=col_def.nullable)
            fields.append(field)
        return pa.schema(fields)

    def _compute_fingerprint(self, value: _RowValue) -> _RowFingerprint:
        """Compute a fingerprint for row data."""
        # Serialize deterministically
        serialized = json.dumps(value, sort_keys=True, default=str)
        return hashlib.blake2b(serialized.encode()).digest()

    def reconcile(
        self,
        key: _RowKey,
        desired_state: _RowValue | coco.NonExistenceType,
        prev_possible_states: Collection[_RowFingerprint],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_RowAction, _RowFingerprint] | None:
        if coco.is_non_existence(desired_state):
            # Delete case - only if it might exist
            if not prev_possible_states and not prev_may_be_missing:
                return None
            return coco.TargetReconcileOutput(
                action=_RowAction(key=key, value=None),
                sink=self._sink,
                tracking_record=coco.NON_EXISTENCE,
            )

        # Upsert case
        target_fp = self._compute_fingerprint(desired_state)
        if not prev_may_be_missing and all(
            prev == target_fp for prev in prev_possible_states
        ):
            # No change needed
            return None

        return coco.TargetReconcileOutput(
            action=_RowAction(key=key, value=desired_state),
            sink=self._sink,
            tracking_record=target_fp,
        )


class _TableKey(NamedTuple):
    """Key identifying a table: (database_key, table_name)."""

    db_key: str  # Stable key for the database
    table_name: str


@dataclass
class _TableSpec:
    """Specification for a LanceDB table."""

    table_schema: TableSchema[Any]
    managed_by: Literal["system", "user"] = "system"


class _ColumnState(NamedTuple):
    """Per-column state used for table-level state tracking."""

    name: str
    type: str  # String representation of PyArrow type
    nullable: bool


class _IndexState(NamedTuple):
    """State for a vector or FTS index."""

    column_name: str
    index_type: Literal["vector", "fts"]


_VECTOR_INDEX_SUBKEY_PREFIX: str = "vec_idx:"
_FTS_INDEX_SUBKEY_PREFIX: str = "fts_idx:"


def _vector_index_subkey(col_name: str) -> str:
    return f"{_VECTOR_INDEX_SUBKEY_PREFIX}{col_name}"


def _fts_index_subkey(col_name: str) -> str:
    return f"{_FTS_INDEX_SUBKEY_PREFIX}{col_name}"


_TableSubTrackingRecord = _ColumnState | _IndexState | None


def _table_composite_tracking_record_from_spec(
    spec: _TableSpec,
) -> statediff.CompositeTrackingRecord[tuple[str, ...], str, _TableSubTrackingRecord]:
    """Build composite state from table spec."""
    schema = spec.table_schema

    # Main state: primary key column names (simplified - just names)
    pk_sig = tuple(schema.primary_key)

    # Sub-tracking-records: each column and each index
    sub: dict[str, _TableSubTrackingRecord] = {}

    # Add column states
    for col_name, col_def in schema.columns.items():
        sub_key = f"col:{col_name}"
        sub[sub_key] = _ColumnState(
            name=col_name,
            type=str(col_def.type),
            nullable=col_def.nullable,
        )

    # Add vector index states
    for col_name in schema.vector_schemas.keys():
        sub_key = _vector_index_subkey(col_name)
        sub[sub_key] = _IndexState(column_name=col_name, index_type="vector")

    # Add FTS index states
    for col_name in schema.fts_specs.keys():
        sub_key = _fts_index_subkey(col_name)
        sub[sub_key] = _IndexState(column_name=col_name, index_type="fts")

    return statediff.CompositeTrackingRecord(main=pk_sig, sub=sub)


_TableTrackingRecord = statediff.MutualTrackingRecord[
    statediff.CompositeTrackingRecord[tuple[str, ...], str, _TableSubTrackingRecord]
]


class _TableAction(NamedTuple):
    """Action to perform on a table."""

    key: _TableKey
    spec: _TableSpec | coco.NonExistenceType
    main_action: statediff.DiffAction | None
    sub_actions: dict[str, statediff.DiffAction]


# Database registry: maps stable keys to async connections
_db_registry: dict[str, lancedb.AsyncConnection] = {}
_db_registry_lock = threading.Lock()


def _get_connection(db_key: str) -> lancedb.AsyncConnection:
    """Get the async connection for the given database key."""
    with _db_registry_lock:
        conn = _db_registry.get(db_key)
    if conn is None:
        raise RuntimeError(
            f"No database registered with key '{db_key}'. Call register_db() first."
        )
    return conn


async def _register_db_async(key: str, uri: str, **options: Any) -> None:
    """Internal async function to register a LanceDB connection."""
    conn = await lancedb.connect_async(uri, **options)
    with _db_registry_lock:
        if key in _db_registry:
            raise ValueError(
                f"Database with key '{key}' is already registered. "
                f"Use a different key or unregister the existing one first."
            )
        _db_registry[key] = conn


def _unregister_db(key: str) -> None:
    """Unregister a database connection (internal, with lock)."""
    with _db_registry_lock:
        _db_registry.pop(key, None)


class _TableHandler(
    coco.TargetHandler[_TableKey, _TableSpec, _TableTrackingRecord, _RowHandler]
):
    """Handler for table-level target states."""

    _sink: coco.TargetActionSink[_TableAction, _RowHandler]

    def __init__(self) -> None:
        self._sink = coco.TargetActionSink.from_async_fn(self._apply_actions)

    async def _apply_actions(
        self, actions: Collection[_TableAction]
    ) -> list[coco.ChildTargetDef[_RowHandler] | None]:
        """Apply table actions (DDL) and return child row handlers."""
        actions_list = list(actions)
        outputs: list[coco.ChildTargetDef[_RowHandler] | None] = [None] * len(
            actions_list
        )

        # Group actions by table key
        by_key: dict[_TableKey, list[int]] = {}
        for i, action in enumerate(actions_list):
            by_key.setdefault(action.key, []).append(i)

        for key, idxs in by_key.items():
            conn = _get_connection(key.db_key)

            for i in idxs:
                action = actions_list[i]
                assert action.key == key

                if action.main_action in ("replace", "delete"):
                    await self._drop_table(conn, key.table_name)

                if coco.is_non_existence(action.spec):
                    outputs[i] = None
                    continue

                spec = action.spec
                outputs[i] = coco.ChildTargetDef(
                    handler=_RowHandler(
                        db_key=key.db_key,
                        table_name=key.table_name,
                        table_schema=spec.table_schema,
                    )
                )

                if action.main_action in ("insert", "upsert", "replace"):
                    await self._create_table(
                        conn,
                        key.table_name,
                        spec.table_schema,
                        if_not_exists=(action.main_action == "upsert"),
                    )
                    continue

                # No main change: reconcile sub-states (indexes)
                if action.sub_actions:
                    await self._apply_sub_actions(
                        conn, key.table_name, spec.table_schema, action.sub_actions
                    )

        return outputs

    async def _drop_table(
        self,
        conn: lancedb.AsyncConnection,
        table_name: str,
    ) -> None:
        """Drop a table if it exists."""
        try:
            await conn.drop_table(table_name)
        except Exception:
            # Table might not exist, ignore
            pass

    async def _create_table(
        self,
        conn: lancedb.AsyncConnection,
        table_name: str,
        schema: TableSchema[Any],
        *,
        if_not_exists: bool,
    ) -> None:
        """Create a table."""
        # Check if table exists
        table_names = await conn.table_names()
        table_exists = table_name in table_names

        if table_exists and if_not_exists:
            return

        if table_exists:
            # Drop and recreate
            await conn.drop_table(table_name)

        # Build PyArrow schema
        pa_schema = self._build_pyarrow_schema(schema)

        # Create empty table
        # LanceDB requires at least one row to create a table
        # Create an empty batch with the schema
        empty_data: dict[str, list[Any]] = {
            col_name: [] for col_name in schema.columns.keys()
        }
        arrays = [
            pa.array(empty_data[col_name], type=col_def.type)
            for col_name, col_def in schema.columns.items()
        ]
        empty_batch = pa.RecordBatch.from_arrays(arrays, schema=pa_schema)

        # Create table with empty data
        await conn.create_table(table_name, empty_batch, mode="overwrite")

    async def _apply_sub_actions(
        self,
        conn: lancedb.AsyncConnection,
        table_name: str,
        schema: TableSchema[Any],
        sub_actions: dict[str, statediff.DiffAction],
    ) -> None:
        """Apply sub-actions (index creation/deletion)."""
        table = await conn.open_table(table_name)

        # Check row count before creating indexes
        # LanceDB requires at least 256 rows to create indexes
        count = await table.count_rows()
        can_create_indexes = count >= _MIN_ROWS_FOR_INDEX

        for sub_key, action in sub_actions.items():
            # Handle vector indexes
            if sub_key.startswith(_VECTOR_INDEX_SUBKEY_PREFIX):
                col_name = sub_key[len(_VECTOR_INDEX_SUBKEY_PREFIX) :]
                if action == "delete":
                    # LanceDB doesn't have explicit index drop - recreate table if needed
                    continue
                elif action in ("insert", "upsert") and can_create_indexes:
                    vector_schema = schema.vector_schemas.get(col_name)
                    if vector_schema:
                        # Map VectorSpec metric to LanceDB metric
                        # VectorSpec currently just has dim, but we'll use L2 as default
                        # In the future, VectorSpec could include metric
                        await table.create_index(
                            vector_column_name=col_name,
                            metric="l2",  # Default to L2
                        )
                continue

            # Handle FTS indexes
            if sub_key.startswith(_FTS_INDEX_SUBKEY_PREFIX):
                col_name = sub_key[len(_FTS_INDEX_SUBKEY_PREFIX) :]
                if action == "delete":
                    # LanceDB doesn't have explicit index drop
                    continue
                elif action in ("insert", "upsert") and can_create_indexes:
                    fts_spec = schema.fts_specs.get(col_name)
                    if fts_spec:
                        # Map tokenizer names
                        # CocoIndex uses: "simple", "en_stem", "raw"
                        # LanceDB uses: "simple", "en_stem", "raw" (same)
                        await table.create_fts_index(
                            field_names=[col_name],
                            tokenizer_name=fts_spec.tokenizer,
                        )
                continue

    def _build_pyarrow_schema(self, schema: TableSchema[Any]) -> pa.Schema:
        """Build PyArrow schema from table schema."""
        fields = []
        for col_name, col_def in schema.columns.items():
            field = pa.field(col_name, col_def.type, nullable=col_def.nullable)
            fields.append(field)
        return pa.schema(fields)

    def reconcile(
        self,
        key: _TableKey,
        desired_state: _TableSpec | coco.NonExistenceType,
        prev_possible_states: Collection[_TableTrackingRecord],
        prev_may_be_missing: bool,
        /,
    ) -> (
        coco.TargetReconcileOutput[_TableAction, _TableTrackingRecord, _RowHandler]
        | None
    ):
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
        main_action, sub_transitions = statediff.diff_composite(resolved)

        sub_actions: dict[str, statediff.DiffAction] = {}
        if main_action is None:
            for sub_key, t in sub_transitions.items():
                action = statediff.diff(t)
                if action is not None:
                    sub_actions[sub_key] = action

        return coco.TargetReconcileOutput(
            action=_TableAction(
                key=key,
                spec=desired_state,
                main_action=main_action,
                sub_actions=sub_actions,
            ),
            sink=self._sink,
            tracking_record=tracking_record,
        )


# Register the root target states provider
_table_provider = coco.register_root_target_states_provider(
    "cocoindex.io/lancedb/table", _TableHandler()
)


class TableTarget(
    Generic[RowT, coco.MaybePendingS], coco.ResolvesTo["TableTarget[RowT]"]
):
    """
    A target for writing rows to a LanceDB table.

    The table is managed as a target state, with the scope used to scope the target state.

    Type Parameters:
        RowT: The type of row objects (dict, dataclass, NamedTuple, or Pydantic model).
    """

    _provider: coco.TargetStateProvider[_RowKey, _RowValue, None, coco.MaybePendingS]
    _table_schema: TableSchema[RowT]

    def __init__(
        self,
        provider: coco.TargetStateProvider[
            _RowKey, _RowValue, None, coco.MaybePendingS
        ],
        table_schema: TableSchema[RowT],
    ) -> None:
        self._provider = provider
        self._table_schema = table_schema

    def declare_row(self: "TableTarget[RowT]", scope: coco.Scope, *, row: RowT) -> None:
        """
        Declare a row to be upserted to this table.

        Args:
            scope: The scope for target state declaration.
            row: A row object (dict, dataclass, NamedTuple, or Pydantic model).
                 Must include all primary key columns.
        """
        row_dict = self._row_to_dict(row)
        # Extract primary key values
        pk_values = tuple(row_dict[pk] for pk in self._table_schema.primary_key)
        coco.declare_target_state(
            scope, self._provider.target_state(pk_values, row_dict)
        )

    def _row_to_dict(self, row: RowT) -> dict[str, Any]:
        """
        Convert a row (dict or object) into dict[str, Any] using the schema columns,
        and apply column encoders for both dict and object inputs.
        """
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


class LanceDatabase:
    """
    Handle for a registered LanceDB database.

    Use `register_db()` to create an instance. Can be used as a context manager
    to automatically unregister on exit.

    Example:
        ```python
        # Without context manager (manual lifecycle)
        db = register_db("my_db", "./lancedb_data")
        # ... use db ...

        # With context manager (auto-unregister on exit)
        with register_db("my_db", "./lancedb_data") as db:
            # ... use db ...
        # db is automatically unregistered here
        ```
    """

    _key: str

    def __init__(self, key: str) -> None:
        self._key = key

    @property
    def key(self) -> str:
        """The stable key for this database."""
        return self._key

    def __enter__(self) -> "LanceDatabase":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        _unregister_db(self._key)

    def declare_table_target(
        self,
        scope: coco.Scope,
        table_name: str,
        table_schema: TableSchema[RowT],
        *,
        managed_by: Literal["system", "user"] = "system",
    ) -> TableTarget[RowT, coco.PendingS]:
        """
        Create a TableTarget for writing rows to a LanceDB table.

        Args:
            scope: The scope for target state declaration.
            table_name: Name of the table.
            table_schema: Schema definition including columns and primary key.
            managed_by: Whether the table is managed by "system" (CocoIndex creates/drops it)
                        or "user" (table must exist, CocoIndex only manages rows).

        Returns:
            A TableTarget that can be used to declare rows.
        """
        key = _TableKey(db_key=self._key, table_name=table_name)
        spec = _TableSpec(
            table_schema=table_schema,
            managed_by=managed_by,
        )
        provider = coco.declare_target_state_with_child(
            scope, _table_provider.target_state(key, spec)
        )
        return TableTarget(provider, table_schema)

    def __coco_memo_key__(self) -> str:
        return self._key


def register_db(key: str, uri: str, **options: Any) -> LanceDatabase:
    """
    Register a LanceDB database connection with a stable key.

    The key should be stable across runs - it identifies the logical database.
    The URI can point to a local directory or cloud storage (s3://, gs://).

    Can be used as a context manager to automatically unregister on exit.

    Note: This function must be called from an async context. Typically you would
    call it from within a @coco_aio.lifespan decorated function after awaiting
    _register_db_async().

    Args:
        key: A stable identifier for this database (e.g., "main_db", "embeddings").
             Must be unique - raises ValueError if a database with this key
             is already registered.
        uri: LanceDB URI (local path like "./lancedb_data" or cloud URI like "s3://bucket/path").
        **options: Additional options to pass to lancedb.connect_async().

    Returns:
        A LanceDatabase handle that can be used to create table targets.

    Raises:
        ValueError: If a database with the given key is already registered.

    Example:
        ```python
        @coco_aio.lifespan
        async def coco_lifespan(builder: coco_aio.EnvironmentBuilder) -> AsyncIterator[None]:
            # Register LanceDB connection
            await lancedb._register_db_async("my_db", "./lancedb_data")
            state.db = lancedb.LanceDatabase("my_db")
            yield
        ```
    """
    # Note: The actual async registration must be done via _register_db_async()
    # This function just creates the handle after registration is complete
    return LanceDatabase(key)


__all__ = [
    "ColumnDef",
    "LanceDatabase",
    "LanceType",
    "TableSchema",
    "TableTarget",
    "ValueEncoder",
    "register_db",
    "_register_db_async",  # Exposed for use in lifespan contexts
    "_get_connection",  # Exposed for query operations
]

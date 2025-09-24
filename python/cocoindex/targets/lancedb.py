import dataclasses
import logging
import threading
import uuid
import weakref
import datetime

from typing import Any

import lancedb  # type: ignore
import pyarrow as pa  # type: ignore

from .. import op
from ..typing import (
    FieldSchema,
    EnrichedValueType,
    BasicValueType,
    StructType,
    ValueType,
    VectorTypeSchema,
    TableType,
)
from ..index import VectorIndexDef, IndexOptions, VectorSimilarityMetric

_logger = logging.getLogger(__name__)

_LANCEDB_VECTOR_METRIC: dict[VectorSimilarityMetric, str] = {
    VectorSimilarityMetric.COSINE_SIMILARITY: "cosine",
    VectorSimilarityMetric.L2_DISTANCE: "l2",
    VectorSimilarityMetric.INNER_PRODUCT: "dot",
}


class DatabaseOptions:
    storage_options: dict[str, Any] | None = None


class LanceDB(op.TargetSpec):
    db_uri: str
    table_name: str
    db_options: DatabaseOptions | None = None


@dataclasses.dataclass
class _VectorIndex:
    name: str
    field_name: str
    metric: VectorSimilarityMetric


@dataclasses.dataclass
class _State:
    key_field_schema: FieldSchema
    value_fields_schema: list[FieldSchema]
    vector_indexes: list[_VectorIndex] | None = None
    db_options: DatabaseOptions | None = None


@dataclasses.dataclass
class _TableKey:
    db_uri: str
    table_name: str


_DbConnectionsLock = threading.Lock()
_DbConnections: weakref.WeakValueDictionary[str, lancedb.AsyncConnection] = (
    weakref.WeakValueDictionary()
)


async def connect_async(
    db_uri: str,
    *,
    db_options: DatabaseOptions | None = None,
    read_consistency_interval: datetime.timedelta | None = None,
) -> lancedb.AsyncConnection:
    """
    Helper function to connect to a LanceDB database.
    It will reuse the connection if it already exists.
    The connection will be shared with the target used by cocoindex, so it achieves strong consistency.
    """
    with _DbConnectionsLock:
        conn = _DbConnections.get(db_uri)
        if conn is None:
            db_options = db_options or DatabaseOptions()
            _DbConnections[db_uri] = conn = await lancedb.connect_async(
                db_uri,
                storage_options=db_options.storage_options,
                read_consistency_interval=read_consistency_interval,
            )
        return conn


def make_pa_schema(
    key_field_schema: FieldSchema, value_fields_schema: list[FieldSchema]
) -> pa.Schema:
    """Convert FieldSchema list to PyArrow schema."""
    fields = [
        _convert_field_to_pa_field(field)
        for field in [key_field_schema] + value_fields_schema
    ]
    return pa.schema(fields)


def _convert_field_to_pa_field(field_schema: FieldSchema) -> pa.Field:
    """Convert a FieldSchema to a PyArrow Field."""
    pa_type = _convert_value_type_to_pa_type(field_schema.value_type)

    # Handle nullable fields
    nullable = field_schema.value_type.nullable

    return pa.field(field_schema.name, pa_type, nullable=nullable)


def _convert_value_type_to_pa_type(value_type: EnrichedValueType) -> pa.DataType:
    """Convert EnrichedValueType to PyArrow DataType."""
    base_type: ValueType = value_type.type

    if isinstance(base_type, StructType):
        # Handle struct types
        return _convert_struct_fields_to_pa_type(base_type.fields)
    elif isinstance(base_type, BasicValueType):
        # Handle basic types
        return _convert_basic_type_to_pa_type(base_type)
    elif isinstance(base_type, TableType):
        return pa.list_(_convert_struct_fields_to_pa_type(base_type.row.fields))

    assert False, f"Unhandled value type: {value_type}"


def _convert_struct_fields_to_pa_type(
    fields_schema: list[FieldSchema],
) -> pa.StructType:
    """Convert StructType to PyArrow StructType."""
    return pa.struct([_convert_field_to_pa_field(field) for field in fields_schema])


def _convert_basic_type_to_pa_type(basic_type: BasicValueType) -> pa.DataType:
    """Convert BasicValueType to PyArrow DataType."""
    kind: str = basic_type.kind

    # Map basic types to PyArrow types
    type_mapping = {
        "Bytes": pa.binary(),
        "Str": pa.string(),
        "Bool": pa.bool_(),
        "Int64": pa.int64(),
        "Float32": pa.float32(),
        "Float64": pa.float64(),
        "Uuid": pa.uuid(),
        "Date": pa.date32(),
        "Time": pa.time64("us"),
        "LocalDateTime": pa.timestamp("us"),
        "OffsetDateTime": pa.timestamp("us", tz="UTC"),
        "TimeDelta": pa.duration("us"),
        "Json": pa.json_(),
    }

    if kind in type_mapping:
        return type_mapping[kind]

    if kind == "Vector":
        vector_schema: VectorTypeSchema | None = basic_type.vector
        if vector_schema is None:
            raise ValueError("Vector type missing vector schema")
        element_type = _convert_basic_type_to_pa_type(vector_schema.element_type)

        if vector_schema.dimension is not None:
            return pa.list_(element_type, vector_schema.dimension)
        else:
            return pa.list_(element_type)

    if kind == "Range":
        # Range as a struct with start and end
        return pa.struct([pa.field("start", pa.int64()), pa.field("end", pa.int64())])

    assert False, f"Unsupported type kind for LanceDB: {kind}"


def _convert_key_value_to_sql(v: Any) -> str:
    if isinstance(v, str):
        escaped = v.replace("'", "''")
        return f"'{escaped}'"

    if isinstance(v, uuid.UUID):
        return f"x'{v.hex}'"

    return str(v)


def _convert_fields_to_pyarrow(fields: list[FieldSchema], v: Any) -> Any:
    if isinstance(v, dict):
        return {
            field.name: _convert_value_for_pyarrow(
                field.value_type.type, v.get(field.name)
            )
            for field in fields
        }
    elif isinstance(v, tuple):
        return {
            field.name: _convert_value_for_pyarrow(field.value_type.type, value)
            for field, value in zip(fields, v)
        }
    else:
        field = fields[0]
        return {field.name: _convert_value_for_pyarrow(field.value_type.type, v)}


def _convert_value_for_pyarrow(t: ValueType, v: Any) -> Any:
    if v is None:
        return None

    if isinstance(t, BasicValueType):
        if isinstance(v, uuid.UUID):
            return v.bytes

        if t.kind == "Range":
            return {"start": v[0], "end": v[1]}

        if t.vector is not None:
            return [_convert_value_for_pyarrow(t.vector.element_type, e) for e in v]

        return v

    elif isinstance(t, StructType):
        return _convert_fields_to_pyarrow(t.fields, v)

    elif isinstance(t, TableType):
        if isinstance(v, list):
            return [_convert_fields_to_pyarrow(t.row.fields, value) for value in v]
        else:
            key_fields = t.row.fields[: t.num_key_parts]
            value_fields = t.row.fields[t.num_key_parts :]
            return [
                _convert_fields_to_pyarrow(key_fields, value[0 : t.num_key_parts])
                | _convert_fields_to_pyarrow(value_fields, value[t.num_key_parts :])
                for value in v
            ]

    assert False, f"Unsupported value type: {t}"


@dataclasses.dataclass
class _MutateContext:
    table: lancedb.AsyncTable
    key_field_schema: FieldSchema
    value_fields_type: list[ValueType]
    pa_schema: pa.Schema


# Not used for now, because of https://github.com/lancedb/lance/issues/3443
#
# async def _update_table_schema(
#     table: lancedb.AsyncTable,
#     expected_schema: pa.Schema,
# ) -> None:
#     existing_schema = await table.schema()
#     unseen_existing_field_names = {field.name: field for field in existing_schema}
#     new_columns = []
#     updated_columns = []
#     for field in expected_schema:
#         existing_field = unseen_existing_field_names.pop(field.name, None)
#         if existing_field is None:
#             new_columns.append(field)
#         else:
#             if field.type != existing_field.type:
#                 updated_columns.append(
#                     {
#                         "path": field.name,
#                         "data_type": field.type,
#                         "nullable": field.nullable,
#                     }
#                 )
#     if new_columns:
#         table.add_columns(new_columns)
#     if updated_columns:
#         table.alter_columns(*updated_columns)
#     if unseen_existing_field_names:
#         table.drop_columns(unseen_existing_field_names.keys())


@op.target_connector(
    spec_cls=LanceDB, persistent_key_type=_TableKey, setup_state_cls=_State
)
class _Connector:
    @staticmethod
    def get_persistent_key(spec: LanceDB) -> _TableKey:
        return _TableKey(db_uri=spec.db_uri, table_name=spec.table_name)

    @staticmethod
    def get_setup_state(
        spec: LanceDB,
        key_fields_schema: list[FieldSchema],
        value_fields_schema: list[FieldSchema],
        index_options: IndexOptions,
    ) -> _State:
        if len(key_fields_schema) != 1:
            raise ValueError("LanceDB only supports a single key field")
        return _State(
            key_field_schema=key_fields_schema[0],
            value_fields_schema=value_fields_schema,
            db_options=spec.db_options,
            vector_indexes=(
                [
                    _VectorIndex(
                        name=f"__{index.field_name}__{_LANCEDB_VECTOR_METRIC[index.metric]}__idx",
                        field_name=index.field_name,
                        metric=index.metric,
                    )
                    for index in index_options.vector_indexes
                ]
                if index_options.vector_indexes is not None
                else None
            ),
        )

    @staticmethod
    def describe(key: _TableKey) -> str:
        return f"LanceDB table {key.table_name}@{key.db_uri}"

    @staticmethod
    def check_state_compatibility(
        previous: _State, current: _State
    ) -> op.TargetStateCompatibility:
        if (
            previous.key_field_schema != current.key_field_schema
            or previous.value_fields_schema != current.value_fields_schema
        ):
            return op.TargetStateCompatibility.NOT_COMPATIBLE

        return op.TargetStateCompatibility.COMPATIBLE

    @staticmethod
    async def apply_setup_change(
        key: _TableKey, previous: _State | None, current: _State | None
    ) -> None:
        latest_state = current or previous
        if not latest_state:
            return
        db_conn = await connect_async(key.db_uri, db_options=latest_state.db_options)

        reuse_table = (
            previous is not None
            and current is not None
            and previous.key_field_schema == current.key_field_schema
            and previous.value_fields_schema == current.value_fields_schema
        )
        if previous is not None:
            if not reuse_table:
                await db_conn.drop_table(key.table_name, ignore_missing=True)

        if current is None:
            return

        table: lancedb.AsyncTable | None = None
        if reuse_table:
            try:
                table = await db_conn.open_table(key.table_name)
            except Exception as e:  # pylint: disable=broad-exception-caught
                _logger.warning(
                    "Exception in opening table %s, creating it",
                    key.table_name,
                    exc_info=e,
                )
                table = None

        if table is None:
            table = await db_conn.create_table(
                key.table_name,
                schema=make_pa_schema(
                    current.key_field_schema, current.value_fields_schema
                ),
                mode="overwrite",
            )
            await table.create_index(
                current.key_field_schema.name, config=lancedb.index.BTree()
            )

        unseen_prev_vector_indexes = {
            index.name for index in (previous and previous.vector_indexes) or []
        }
        existing_vector_indexes = {index.name for index in await table.list_indices()}

        for index in current.vector_indexes or []:
            if index.name in unseen_prev_vector_indexes:
                unseen_prev_vector_indexes.remove(index.name)
            else:
                try:
                    await table.create_index(
                        index.field_name,
                        name=index.name,
                        config=lancedb.index.HnswPq(
                            distance_type=_LANCEDB_VECTOR_METRIC[index.metric]
                        ),
                    )
                except Exception as e:  # pylint: disable=broad-exception-caught
                    raise RuntimeError(
                        f"Exception in creating index on field {index.field_name}. "
                        f"This may be caused by a limitation of LanceDB, "
                        f"which requires data existing in the table to train the index. "
                        f"See: https://github.com/lancedb/lance/issues/4034",
                        index.name,
                    ) from e

        for vector_index_name in unseen_prev_vector_indexes:
            if vector_index_name in existing_vector_indexes:
                await table.drop_index(vector_index_name)

    @staticmethod
    async def prepare(
        spec: LanceDB,
        setup_state: _State,
    ) -> _MutateContext:
        db_conn = await connect_async(spec.db_uri, db_options=spec.db_options)
        table = await db_conn.open_table(spec.table_name)
        return _MutateContext(
            table=table,
            key_field_schema=setup_state.key_field_schema,
            value_fields_type=[
                field.value_type.type for field in setup_state.value_fields_schema
            ],
            pa_schema=make_pa_schema(
                setup_state.key_field_schema, setup_state.value_fields_schema
            ),
        )

    @staticmethod
    async def mutate(
        *all_mutations: tuple[_MutateContext, dict[Any, dict[str, Any] | None]],
    ) -> None:
        for context, mutations in all_mutations:
            key_name = context.key_field_schema.name
            value_types = context.value_fields_type

            rows_to_upserts = []
            keys_sql_to_deletes = []
            for key, value in mutations.items():
                if value is None:
                    keys_sql_to_deletes.append(_convert_key_value_to_sql(key))
                else:
                    fields = {
                        key_name: _convert_value_for_pyarrow(
                            context.key_field_schema.value_type.type, key
                        )
                    }
                    for (name, value), value_type in zip(value.items(), value_types):
                        fields[name] = _convert_value_for_pyarrow(value_type, value)
                    rows_to_upserts.append(fields)
            record_batch = pa.RecordBatch.from_pylist(
                rows_to_upserts, context.pa_schema
            )
            builder = (
                context.table.merge_insert(key_name)
                .when_matched_update_all()
                .when_not_matched_insert_all()
            )
            if keys_sql_to_deletes:
                delete_cond_sql = f"{key_name} IN ({','.join(keys_sql_to_deletes)})"
                builder = builder.when_not_matched_by_source_delete(delete_cond_sql)
            await builder.execute(record_batch)

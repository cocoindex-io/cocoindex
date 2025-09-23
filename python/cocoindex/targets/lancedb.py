import datetime
import dataclasses
import uuid
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
from ..index import IndexOptions


@dataclasses.dataclass
class DatabaseOptions:
    read_consistency_interval: datetime.timedelta | None = None
    storage_options: dict[str, Any] | None = None


class LanceDB(op.TargetSpec):
    db_uri: str
    table_name: str
    db_options: DatabaseOptions | None = None


@dataclasses.dataclass
class _State:
    key_field_schema: FieldSchema
    value_fields_schema: list[FieldSchema]
    index_options: IndexOptions
    db_options: DatabaseOptions | None = None


@dataclasses.dataclass
class _TableKey:
    db_uri: str
    table_name: str


async def _open_db(
    db_uri: str, db_options: DatabaseOptions | None
) -> lancedb.AsyncConnection:
    db_options = db_options or DatabaseOptions()

    # TODO: reuse cached connections
    return await lancedb.connect_async(
        db_uri,
        read_consistency_interval=db_options.read_consistency_interval,
        storage_options=db_options.storage_options,
    )


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
        "Union": pa.json_(),
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

    assert False, f"Unsupported type kind: {kind}"


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


def _convert_value_for_pyarrow(t: ValueType | None, v: Any) -> Any:
    if t is None or isinstance(t, BasicValueType):
        if isinstance(v, uuid.UUID):
            return v.bytes

        if isinstance(v, tuple) and len(v) == 2:
            return {"start": v[0], "end": v[1]}

        if isinstance(v, list):
            return [_convert_value_for_pyarrow(None, value) for value in v]

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
            index_options=index_options,
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
        db_conn = await _open_db(key.db_uri, latest_state.db_options)

        reuse_table = (
            previous is not None
            and current is not None
            and previous.key_field_schema == current.key_field_schema
            and previous.value_fields_schema == current.value_fields_schema
        )
        if previous is not None:
            if not reuse_table:
                await db_conn.drop_table(key.table_name, ignore_missing=True)

        if current is not None:
            if not reuse_table:
                await db_conn.create_table(
                    key.table_name,
                    schema=make_pa_schema(
                        current.key_field_schema, current.value_fields_schema
                    ),
                    exist_ok=True,
                )

            # TODO: deal with the index options

    @staticmethod
    async def prepare(
        spec: LanceDB,
        setup_state: _State,
    ) -> _MutateContext:
        db_conn = await _open_db(spec.db_uri, spec.db_options)
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

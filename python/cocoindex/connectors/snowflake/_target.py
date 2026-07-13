"""
Snowflake target for CocoIndex.

This module provides a two-level target state system for Snowflake:
1. Table level: Creates/drops tables in Snowflake
2. Row level: Upserts/deletes rows within tables
"""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import json
import re
import uuid
from contextlib import contextmanager
from typing import (
    Any,
    Callable,
    Collection,
    Generic,
    Iterator,
    Literal,
    NamedTuple,
    Sequence,
)

import msgspec
from typing_extensions import TypeVar

import cocoindex as coco
from cocoindex._internal.context_keys import ContextKey, ContextProvider
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
from cocoindex.connectorkits import reject_sparse_vectors, statediff, target
from cocoindex.connectorkits.fingerprint import fingerprint_object

_RowKey = tuple[Any, ...]
_ROW_KEY_CHECKER = TypeChecker(tuple[Any, ...])
_RowValue = dict[str, Any]
_RowFingerprint = bytes
ValueEncoder = Callable[[Any], Any]

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclasses.dataclass(frozen=True)
class ConnectionConfig:
    """Connection information for the Snowflake Python connector."""

    account: str
    user: str
    password: str
    warehouse: str | None = None
    role: str | None = None


class SnowflakeType(NamedTuple):
    """
    Annotation to specify a Snowflake column type.

    Use with ``typing.Annotated`` to override the default type mapping.
    """

    snowflake_type: str
    encoder: ValueEncoder | None = None
    use_parse_json: bool = False


class _TypeMapping(NamedTuple):
    """Mapping from Python type to Snowflake type with optional encoder."""

    snowflake_type: str
    encoder: ValueEncoder | None = None
    use_parse_json: bool = False


class ColumnDef(NamedTuple):
    """Definition of a Snowflake table column."""

    type: str
    nullable: bool = True
    encoder: ValueEncoder | None = None
    use_parse_json: bool = False


RowT = TypeVar("RowT", default=dict[str, Any])


@dataclasses.dataclass(slots=True)
class TableSchema(Generic[RowT]):
    """Schema definition for a Snowflake table."""

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
                    f"Primary key column '{pk}' not found in columns: {list(self.columns.keys())}"
                )

    @classmethod
    async def from_class(
        cls,
        record_type: type[RowT],
        primary_key: list[str],
        *,
        column_overrides: dict[str, SnowflakeType] | None = None,
    ) -> "TableSchema[RowT]":
        """
        Create a TableSchema from a record type.

        Args:
            record_type: A dataclass, NamedTuple, or Pydantic model.
            primary_key: List of column names that form the primary key.
            column_overrides: Optional per-column SnowflakeType overrides.
        """
        if not is_record_type(record_type):
            raise TypeError(
                f"record_type must be a record type (dataclass, NamedTuple, Pydantic model), "
                f"got {type(record_type)}"
            )
        columns = await cls._columns_from_record_type(record_type, column_overrides)
        return cls(columns, primary_key, row_type=record_type)

    @staticmethod
    async def _columns_from_record_type(
        record_type: type,
        column_overrides: dict[str, SnowflakeType] | None,
    ) -> dict[str, ColumnDef]:
        """Convert a record type to a dict of column name -> ColumnDef."""
        record_info = RecordType(record_type)
        columns: dict[str, ColumnDef] = {}

        for field in record_info.fields:
            override = column_overrides.get(field.name) if column_overrides else None
            type_info = analyze_type_info(field.type_hint)

            all_annotations: list[Any] = []
            if override is not None:
                all_annotations.append(override)
            all_annotations.extend(type_info.annotations)

            reject_sparse_vectors(
                type_info.base_type,
                all_annotations,
                connector_name="Snowflake",
            )

            snowflake_type_annotation = next(
                (t for t in all_annotations if isinstance(t, SnowflakeType)), None
            )

            if snowflake_type_annotation is not None:
                type_mapping = _TypeMapping(
                    snowflake_type=snowflake_type_annotation.snowflake_type,
                    encoder=snowflake_type_annotation.encoder,
                    use_parse_json=snowflake_type_annotation.use_parse_json,
                )
            else:
                type_mapping = await _get_type_mapping(field.type_hint)

            columns[field.name] = ColumnDef(
                type=type_mapping.snowflake_type.strip(),
                nullable=type_info.nullable,
                encoder=type_mapping.encoder,
                use_parse_json=type_mapping.use_parse_json,
            )

        return columns


def _json_encoder(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


_LEAF_TYPE_MAPPINGS: dict[type, _TypeMapping] = {
    bool: _TypeMapping("BOOLEAN"),
    int: _TypeMapping("NUMBER"),
    float: _TypeMapping("FLOAT"),
    decimal.Decimal: _TypeMapping("NUMBER"),
    str: _TypeMapping("VARCHAR"),
    bytes: _TypeMapping("BINARY"),
    uuid.UUID: _TypeMapping("VARCHAR", str),
    datetime.date: _TypeMapping("DATE"),
    datetime.time: _TypeMapping("TIME"),
    datetime.datetime: _TypeMapping("TIMESTAMP_TZ"),
    datetime.timedelta: _TypeMapping("NUMBER", lambda v: v.total_seconds()),
}

_VARIANT_MAPPING = _TypeMapping("VARIANT", _json_encoder, True)


async def _get_type_mapping(python_type: Any) -> _TypeMapping:
    """Get the Snowflake type mapping for a Python type."""
    type_info = analyze_type_info(python_type)

    for annotation in type_info.annotations:
        if isinstance(annotation, SnowflakeType):
            return _TypeMapping(
                annotation.snowflake_type,
                annotation.encoder,
                annotation.use_parse_json,
            )

    base_type = type_info.base_type
    if base_type in _LEAF_TYPE_MAPPINGS:
        return _LEAF_TYPE_MAPPINGS[base_type]

    if isinstance(
        type_info.variant, (SequenceType, MappingType, RecordType, UnionType, AnyType)
    ):
        return _VARIANT_MAPPING

    return _VARIANT_MAPPING


def _validate_identifier(name: str, kind: str = "identifier") -> None:
    if not isinstance(name, str) or not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid Snowflake {kind}: {name!r}")


def _quote_ident(name: str) -> str:
    _validate_identifier(name)
    return f'"{name}"'


def _qualified_table_name(
    database: str | None, schema: str | None, table_name: str
) -> str:
    parts = []
    if database is not None:
        parts.append(_quote_ident(database))
    if schema is not None:
        parts.append(_quote_ident(schema))
    parts.append(_quote_ident(table_name))
    return ".".join(parts)


def _qualified_schema_name(database: str | None, schema: str) -> str:
    if database is not None:
        return f"{_quote_ident(database)}.{_quote_ident(schema)}"
    return _quote_ident(schema)


def _source_select_sql(table_schema: TableSchema[Any]) -> str:
    source_cols = []
    for col_name, col in table_schema.columns.items():
        value_expr = "PARSE_JSON(%s)" if col.use_parse_json else "%s"
        source_cols.append(f'{value_expr} AS "{col_name}"')
    return "SELECT " + ", ".join(source_cols)


def _merge_sql(qualified_table_name: str, table_schema: TableSchema[Any]) -> str:
    all_col_names = list(table_schema.columns.keys())
    pk_cols = table_schema.primary_key
    non_pk_cols = [c for c in all_col_names if c not in pk_cols]

    on_clause = " AND ".join(f'target."{c}" = source."{c}"' for c in pk_cols)
    insert_cols = ", ".join(f'"{c}"' for c in all_col_names)
    insert_values = ", ".join(f'source."{c}"' for c in all_col_names)

    sql_parts = [
        f"MERGE INTO {qualified_table_name} AS target",
        f"USING ({_source_select_sql(table_schema)}) AS source",
        f"ON {on_clause}",
    ]

    if non_pk_cols:
        update_list = ", ".join(f'"{c}" = source."{c}"' for c in non_pk_cols)
        sql_parts.append(f"WHEN MATCHED THEN UPDATE SET {update_list}")

    sql_parts.append(
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_values})"
    )
    return " ".join(sql_parts)


def _delete_sql(
    qualified_table_name: str, table_schema: TableSchema[Any], *, row_count: int
) -> str:
    pk_cols = table_schema.primary_key
    if row_count <= 0:
        raise ValueError("row_count must be positive")

    if len(pk_cols) == 1:
        markers = ", ".join("%s" for _ in range(row_count))
        return f'DELETE FROM {qualified_table_name} WHERE "{pk_cols[0]}" IN ({markers})'

    row_clauses = []
    for _ in range(row_count):
        and_clause = " AND ".join(f'"{pk}" = %s' for pk in pk_cols)
        row_clauses.append(f"({and_clause})")
    return f"DELETE FROM {qualified_table_name} WHERE {' OR '.join(row_clauses)}"


def _encode_value(col: ColumnDef, value: Any) -> Any:
    if value is None:
        return None
    if col.use_parse_json:
        if isinstance(value, str):
            return value
        return _json_encoder(value)
    if col.encoder is not None:
        return col.encoder(value)
    return value


def _encode_row(table_schema: TableSchema[Any], row: _RowValue) -> tuple[Any, ...]:
    return tuple(
        _encode_value(col, row.get(col_name))
        for col_name, col in table_schema.columns.items()
    )


@contextmanager
def _connect(config: ConnectionConfig) -> Iterator[Any]:
    try:
        import snowflake.connector  # type: ignore[import-not-found]
    except ImportError as e:
        raise ImportError(
            "snowflake-connector-python is required to use the Snowflake connector. "
            "Please install cocoindex[snowflake]."
        ) from e

    kwargs: dict[str, str] = {
        "account": config.account,
        "user": config.user,
        "password": config.password,
    }
    if config.warehouse is not None:
        kwargs["warehouse"] = config.warehouse
    if config.role is not None:
        kwargs["role"] = config.role

    conn = snowflake.connector.connect(**kwargs)
    try:
        yield conn
    finally:
        conn.close()


class _RowAction(NamedTuple):
    """Action to perform on a row."""

    key: _RowKey
    value: _RowValue | None


class _RowHandler(coco.TargetHandler[_RowValue, _RowFingerprint]):
    """Handler for row-level target states within a table."""

    _db_key: str
    _database: str | None
    _schema: str | None
    _table_name: str
    _table_schema: TableSchema[Any]
    _sink: coco.TargetActionSink[_RowAction, None]

    def __init__(
        self,
        db_key: str,
        database: str | None,
        schema: str | None,
        table_name: str,
        table_schema: TableSchema[Any],
    ) -> None:
        self._db_key = db_key
        self._database = database
        self._schema = schema
        self._table_name = table_name
        self._table_schema = table_schema
        self._sink = coco.TargetActionSink[_RowAction, None].from_fn(
            self._apply_actions
        )

    def _apply_actions(
        self, context_provider: ContextProvider, actions: Sequence[_RowAction]
    ) -> None:
        if not actions:
            return

        config = context_provider.get(self._db_key, ConnectionConfig)
        qualified_name = _qualified_table_name(
            self._database, self._schema, self._table_name
        )
        upserts = [action for action in actions if action.value is not None]
        deletes = [action for action in actions if action.value is None]

        with _connect(config) as conn:
            cursor = conn.cursor()
            try:
                if upserts:
                    merge_sql = _merge_sql(qualified_name, self._table_schema)
                    for action in upserts:
                        assert action.value is not None
                        cursor.execute(
                            merge_sql, _encode_row(self._table_schema, action.value)
                        )

                if deletes:
                    delete_sql = _delete_sql(
                        qualified_name,
                        self._table_schema,
                        row_count=len(deletes),
                    )
                    delete_params: list[Any] = []
                    for action in deletes:
                        delete_params.extend(action.key)
                    cursor.execute(delete_sql, tuple(delete_params))

                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cursor.close()

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: _RowValue | coco.NonExistenceType,
        prev_possible_records: Collection[_RowFingerprint],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_RowAction, _RowFingerprint] | None:
        key = _ROW_KEY_CHECKER.check(key)
        if coco.is_non_existence(desired_state):
            if not prev_possible_records and not prev_may_be_missing:
                return None
            return coco.TargetReconcileOutput(
                action=_RowAction(key=key, value=None),
                sink=self._sink,
                tracking_record=coco.NON_EXISTENCE,
            )

        target_fp = fingerprint_object(desired_state)
        if not prev_may_be_missing and all(
            prev == target_fp for prev in prev_possible_records
        ):
            return None

        return coco.TargetReconcileOutput(
            action=_RowAction(key=key, value=desired_state),
            sink=self._sink,
            tracking_record=target_fp,
        )


class _TableKey(NamedTuple):
    """Key identifying a Snowflake table."""

    db_key: str
    database: str | None
    schema: str | None
    table_name: str


_TABLE_KEY_CHECKER = TypeChecker(tuple[str, str | None, str | None, str])


@dataclasses.dataclass
class _TableSpec:
    """Specification for a Snowflake table."""

    table_schema: TableSchema[Any]
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM


class _PkColumnTrackingRecord(msgspec.Struct, frozen=True, array_like=True):
    """Primary-key column signature used for table-level main tracking record."""

    name: str
    type: str


class _NonPkColumnTrackingRecord(msgspec.Struct, frozen=True, array_like=True):
    """Per-non-PK column tracking record used for incremental ALTER TABLE operations."""

    type: str
    nullable: bool


_COL_SUBKEY_PREFIX: str = "col:"


def _col_subkey(col_name: str) -> str:
    return f"{_COL_SUBKEY_PREFIX}{col_name}"


_TableSubTrackingRecord = _NonPkColumnTrackingRecord | None


def _table_composite_tracking_record_from_spec(
    spec: _TableSpec,
) -> statediff.CompositeTrackingRecord[
    tuple[_PkColumnTrackingRecord, ...], str, _TableSubTrackingRecord
]:
    schema = spec.table_schema
    col_by_name = schema.columns
    pk_sig = tuple(
        _PkColumnTrackingRecord(name=pk, type=col_by_name[pk].type)
        for pk in schema.primary_key
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
        tuple[_PkColumnTrackingRecord, ...], str, _TableSubTrackingRecord
    ]
]


class _TableAction(NamedTuple):
    """Action to perform on a table."""

    key: _TableKey
    spec: _TableSpec | coco.NonExistenceType
    main_action: statediff.DiffAction | None
    column_actions: dict[str, statediff.DiffAction]


def _column_sql(col_name: str, col: ColumnDef, primary_key: set[str]) -> str:
    nullable = "" if col.nullable and col_name not in primary_key else " NOT NULL"
    return f'"{col_name}" {col.type}{nullable}'


def _drop_table(cursor: Any, key: _TableKey) -> None:
    qualified_name = _qualified_table_name(key.database, key.schema, key.table_name)
    cursor.execute(f"DROP TABLE IF EXISTS {qualified_name}")


def _create_table(
    cursor: Any,
    key: _TableKey,
    schema: TableSchema[Any],
    *,
    if_not_exists: bool,
) -> None:
    if key.database is not None:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {_quote_ident(key.database)}")
    if key.schema is not None:
        cursor.execute(
            f"CREATE SCHEMA IF NOT EXISTS "
            f"{_qualified_schema_name(key.database, key.schema)}"
        )

    primary_key = set(schema.primary_key)
    col_defs = [
        _column_sql(col_name, col, primary_key)
        for col_name, col in schema.columns.items()
    ]
    pk_cols = ", ".join(f'"{c}"' for c in schema.primary_key)
    col_defs.append(f"PRIMARY KEY ({pk_cols})")

    if_not_exists_sql = " IF NOT EXISTS" if if_not_exists else ""
    qualified_name = _qualified_table_name(key.database, key.schema, key.table_name)
    columns_sql = ", ".join(col_defs)
    cursor.execute(f"CREATE TABLE{if_not_exists_sql} {qualified_name} ({columns_sql})")


def _apply_column_actions(
    cursor: Any,
    key: _TableKey,
    schema: TableSchema[Any],
    column_actions: dict[str, statediff.DiffAction],
) -> None:
    qualified_name = _qualified_table_name(key.database, key.schema, key.table_name)
    pk_cols = set(schema.primary_key)
    non_pk_col_by_name = {n: c for n, c in schema.columns.items() if n not in pk_cols}

    for sub_key, action in column_actions.items():
        if not sub_key.startswith(_COL_SUBKEY_PREFIX):
            raise ValueError(
                f"Unexpected column subkey format: {sub_key!r}, expected to start with {_COL_SUBKEY_PREFIX!r}"
            )
        col_name = sub_key[len(_COL_SUBKEY_PREFIX) :]
        if col_name in pk_cols:
            continue

        if action == "delete":
            cursor.execute(
                f'ALTER TABLE {qualified_name} DROP COLUMN IF EXISTS "{col_name}"'
            )
            continue

        desired_col = non_pk_col_by_name.get(col_name)
        if desired_col is None:
            continue

        if action == "insert":
            cursor.execute(
                f"ALTER TABLE {qualified_name} "
                f"ADD COLUMN {_column_sql(col_name, desired_col, pk_cols)}"
            )
            continue

        if action == "upsert":
            cursor.execute(
                f"ALTER TABLE {qualified_name} "
                f"ADD COLUMN IF NOT EXISTS {_column_sql(col_name, desired_col, pk_cols)}"
            )
            continue

        if action == "replace":
            cursor.execute(
                f'ALTER TABLE {qualified_name} DROP COLUMN IF EXISTS "{col_name}"'
            )
            cursor.execute(
                f"ALTER TABLE {qualified_name} "
                f"ADD COLUMN {_column_sql(col_name, desired_col, pk_cols)}"
            )


class _TableHandler(coco.TargetHandler[_TableSpec, _TableTrackingRecord, _RowHandler]):
    """Handler for table-level target states."""

    _sink: coco.TargetActionSink[_TableAction, _RowHandler]

    def __init__(self) -> None:
        self._sink = coco.TargetActionSink[_TableAction, _RowHandler].from_fn(
            self._apply_actions
        )

    def _apply_actions(
        self, context_provider: ContextProvider, actions: Collection[_TableAction]
    ) -> list[coco.ChildTargetDef[_RowHandler] | None]:
        actions_list = list(actions)
        outputs: list[coco.ChildTargetDef[_RowHandler] | None] = [None] * len(
            actions_list
        )

        by_key: dict[_TableKey, list[int]] = {}
        for i, action in enumerate(actions_list):
            by_key.setdefault(action.key, []).append(i)

        for key, idxs in by_key.items():
            config = context_provider.get(key.db_key, ConnectionConfig)
            with _connect(config) as conn:
                cursor = conn.cursor()
                try:
                    for i in idxs:
                        action = actions_list[i]
                        assert action.key == key

                        if action.main_action in ("replace", "delete"):
                            _drop_table(cursor, key)

                        if coco.is_non_existence(action.spec):
                            outputs[i] = None
                            continue

                        spec = action.spec
                        outputs[i] = coco.ChildTargetDef(
                            handler=_RowHandler(
                                db_key=key.db_key,
                                database=key.database,
                                schema=key.schema,
                                table_name=key.table_name,
                                table_schema=spec.table_schema,
                            )
                        )

                        if action.main_action in ("insert", "upsert", "replace"):
                            _create_table(
                                cursor,
                                key,
                                spec.table_schema,
                                if_not_exists=(action.main_action == "upsert"),
                            )
                            continue

                        if action.column_actions:
                            _apply_column_actions(
                                cursor, key, spec.table_schema, action.column_actions
                            )

                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise
                finally:
                    cursor.close()

        return outputs

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: _TableSpec | coco.NonExistenceType,
        prev_possible_records: Collection[_TableTrackingRecord],
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
                prev_possible_records,
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

        child_invalidation: Literal["destructive", "lossy"] | None = None
        if main_action == "replace":
            child_invalidation = "destructive"
        elif main_action is None and any(
            a != "insert" for a in column_actions.values()
        ):
            child_invalidation = "lossy"

        return coco.TargetReconcileOutput(
            action=_TableAction(
                key=key,
                spec=desired_state,
                main_action=main_action,
                column_actions=column_actions,
            ),
            sink=self._sink,
            tracking_record=tracking_record,
            child_invalidation=child_invalidation,
        )


_table_provider = coco.register_root_target_states_provider(
    "cocoindex/snowflake/table", _TableHandler()
)


class TableTarget(
    Generic[RowT, coco.MaybePendingS], coco.ResolvesTo["TableTarget[RowT]"]
):
    """A target for writing rows to a Snowflake table."""

    _provider: coco.TargetStateProvider[_RowValue, None, coco.MaybePendingS]
    _table_schema: TableSchema[RowT]

    def __init__(
        self,
        provider: coco.TargetStateProvider[_RowValue, None, coco.MaybePendingS],
        table_schema: TableSchema[RowT],
    ) -> None:
        self._provider = provider
        self._table_schema = table_schema

    def declare_row(self: "TableTarget[RowT]", *, row: RowT) -> None:
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
            out[col_name] = _encode_value(col, value)
        return out

    def __coco_memo_key__(self) -> str:
        return self._provider.memo_key


def table_target(
    db: ContextKey[ConnectionConfig],
    table_name: str,
    table_schema: TableSchema[RowT],
    *,
    database: str | None = None,
    schema: str | None = None,
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> coco.TargetState[_RowHandler]:
    """
    Create a TargetState for a Snowflake table target.

    Use with ``coco.mount_target()`` or the convenience wrappers below.
    """
    _validate_identifier(table_name, "table name")
    if database is not None:
        _validate_identifier(database, "database name")
    if schema is not None:
        _validate_identifier(schema, "schema name")
    for col_name in table_schema.columns:
        _validate_identifier(col_name, "column name")

    key = _TableKey(
        db_key=db.key,
        database=database,
        schema=schema,
        table_name=table_name,
    )
    spec = _TableSpec(
        table_schema=table_schema,
        managed_by=managed_by,
    )
    return _table_provider.target_state(key, spec)


def declare_table_target(
    db: ContextKey[ConnectionConfig],
    table_name: str,
    table_schema: TableSchema[RowT],
    *,
    database: str | None = None,
    schema: str | None = None,
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> TableTarget[RowT, coco.PendingS]:
    """Declare a Snowflake table target and return a pending TableTarget."""
    provider = coco.declare_target_state_with_child(
        table_target(
            db,
            table_name,
            table_schema,
            database=database,
            schema=schema,
            managed_by=managed_by,
        )
    )
    return TableTarget(provider, table_schema)


async def mount_table_target(
    db: ContextKey[ConnectionConfig],
    table_name: str,
    table_schema: TableSchema[RowT],
    *,
    database: str | None = None,
    schema: str | None = None,
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> TableTarget[RowT]:
    """Mount a Snowflake table target and return a ready-to-use TableTarget."""
    provider = await coco.mount_target(
        table_target(
            db,
            table_name,
            table_schema,
            database=database,
            schema=schema,
            managed_by=managed_by,
        )
    )
    return TableTarget(provider, table_schema)


__all__ = [
    "ColumnDef",
    "ConnectionConfig",
    "SnowflakeType",
    "TableSchema",
    "TableTarget",
    "ValueEncoder",
    "declare_table_target",
    "mount_table_target",
    "table_target",
]

"""Shared reconcile logic for container target handlers (tables, collections,
namespaces, indexes): a container dropped outside CocoIndex, sub-state
actions that invalidate the rows a container holds, and a NOT NULL column that
needs the table rebuilt."""

from __future__ import annotations

from typing import Collection, Generic, Mapping, NamedTuple, Protocol, TypeVar

from cocoindex._internal.typing import NonExistenceType, is_non_existence
from cocoindex.connectorkits.statediff import DiffAction
from cocoindex.connectorkits.target import ManagedBy


class ContainerSpec(Protocol):
    """The fields of a container's desired state that `recreate_if_dropped` reads.

    `missing` is set by the connector's `mount_*_target` when it finds the
    container is not in the external system. It is not part of the container's
    tracking record.
    """

    @property
    def missing(self) -> bool: ...

    @property
    def managed_by(self) -> ManagedBy: ...


class _Managed(Protocol):
    @property
    def managed_by(self) -> ManagedBy: ...


_RecordT = TypeVar("_RecordT", bound=_Managed)


class PrevRecords(NamedTuple, Generic[_RecordT]):
    """The previous records to reconcile against, adjusted by `recreate_if_dropped`."""

    prev_possible_records: Collection[_RecordT]
    prev_may_be_missing: bool
    # The container was dropped outside CocoIndex: everything it held is gone,
    # so its children are invalidated destructively.
    dropped: bool


def was_dropped(
    desired_state: ContainerSpec | NonExistenceType,
    prev_possible_records: Collection[_Managed],
) -> bool:
    """Whether a container CocoIndex created on an earlier run is gone now,
    i.e. it was dropped outside CocoIndex.

    Only a container CocoIndex manages counts: a user-managed container that is
    missing is the user's to create.
    """
    return (
        not is_non_existence(desired_state)
        and desired_state.missing
        and desired_state.managed_by == ManagedBy.SYSTEM
        and any(p.managed_by == ManagedBy.SYSTEM for p in prev_possible_records)
    )


def recreate_if_dropped(
    desired_state: ContainerSpec | NonExistenceType,
    prev_possible_records: Collection[_RecordT],
    prev_may_be_missing: bool,
) -> PrevRecords[_RecordT]:
    """Plan a container dropped outside CocoIndex (see `was_dropped`) as new.

    When it was dropped, the previous records are discarded and the previous
    state may be missing, so the container is created again as if new; the
    caller invalidates its children destructively (`dropped`), so all its rows
    are written again. Otherwise the previous records pass through unchanged.
    """
    if was_dropped(desired_state, prev_possible_records):
        return PrevRecords([], True, True)
    return PrevRecords(prev_possible_records, prev_may_be_missing, False)


def may_lose_data(action: DiffAction, /) -> bool:
    """Whether applying `action` to a sub-state (a column) may lose data that
    rows already written hold: a "replace" (a retype) or a "delete" may.

    An "insert" adds what no row has written yet, and an "upsert" only ensures a
    sub-state that the tracked rows were written with (it is planned when the
    previous state may be missing, e.g. in a full reprocess or after an
    interrupted run), so neither invalidates the rows.
    """
    return action in ("replace", "delete")


class _NullableColumn(Protocol):
    @property
    def nullable(self) -> bool: ...


def _declared_not_null(
    columns: Mapping[str, _NullableColumn], sub_key_prefix: str, sub_key: str
) -> bool:
    """Whether the column of `sub_key` is declared NOT NULL."""
    if not sub_key.startswith(sub_key_prefix):
        return False
    col = columns.get(sub_key[len(sub_key_prefix) :])
    return col is not None and not col.nullable


def adds_not_null_column(
    column_actions: Mapping[str, DiffAction],
    columns: Mapping[str, _NullableColumn],
    sub_key_prefix: str,
) -> bool:
    """Whether `column_actions` add a column declared NOT NULL, or change one.

    For a database that can't add a NOT NULL column to a table with rows, nor
    give an existing column the constraint (SQLite, BigQuery): when this holds,
    the handler rebuilds the table instead ("replace"), and the rows are written
    again. All of them change anyway: a new column changes every row, and a
    changed one is lossy. Otherwise the change would fail on every run.

    `columns` are the declared columns by name; a column's sub key is its name
    after `sub_key_prefix`.
    """
    return any(
        action in ("insert", "replace")
        and _declared_not_null(columns, sub_key_prefix, sub_key)
        for sub_key, action in column_actions.items()
    )

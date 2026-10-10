"""A handler that sets ``tracks_value_fingerprint`` is not asked to reconcile
target states whose declared value is unchanged."""

from __future__ import annotations

from typing import Any, Collection, Iterator, Literal, Mapping, NamedTuple

import pytest

import cocoindex as coco
from cocoindex._internal.memo_fingerprint import (
    register_memo_key_function,
    unregister_memo_key_function,
)
from cocoindex.connectorkits.fingerprint import fingerprint_object

from tests import common

coco_env = common.create_test_env(__file__)

_Action = tuple[str, Any | coco.NonExistenceType]


class _ReconcileCall(NamedTuple):
    key: str
    prev_count: int
    prev_may_be_missing: bool


class _FingerprintStore(coco.TargetHandler[Any, bytes, None]):
    """Tracks the fingerprint of the declared value, the way row handlers do."""

    tracks_value_fingerprint = True

    data: dict[str, Any]
    reconciles: list[_ReconcileCall]
    sink_exception: bool = False

    def __init__(self) -> None:
        self.data = {}
        self.reconciles = []
        self._sink = coco.TargetActionSink.from_fn(self._apply)

    def _apply(
        self, context_provider: coco.ContextProvider, actions: Collection[_Action], /
    ) -> None:
        if self.sink_exception:
            raise ValueError("injected sink exception")
        for key, value in actions:
            if coco.is_non_existence(value):
                del self.data[key]
            else:
                self.data[key] = value

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: Any | coco.NonExistenceType,
        prev_possible_records: Collection[bytes],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_Action, bytes] | None:
        assert isinstance(key, str)
        self.reconciles.append(
            _ReconcileCall(key, len(prev_possible_records), prev_may_be_missing)
        )
        if coco.is_non_existence(desired_state):
            if not prev_possible_records and not prev_may_be_missing:
                return None
            return coco.TargetReconcileOutput(
                action=(key, coco.NON_EXISTENCE),
                sink=self._sink,
                tracking_record=coco.NON_EXISTENCE,
            )
        target_fp = fingerprint_object(desired_state)
        if not prev_may_be_missing and all(
            prev == target_fp for prev in prev_possible_records
        ):
            return None
        return coco.TargetReconcileOutput(
            action=(key, desired_state),
            sink=self._sink,
            tracking_record=target_fp,
        )

    def take_reconciles(self) -> list[_ReconcileCall]:
        calls = sorted(self.reconciles)
        self.reconciles = []
        return calls

    def clear(self) -> None:
        self.data.clear()
        self.reconciles.clear()


class _UntrackedFingerprintStore(_FingerprintStore):
    tracks_value_fingerprint = False


class _TableStore(coco.TargetHandler[None, bytes, _FingerprintStore]):
    """A container whose child rows are tracked by `_FingerprintStore`.

    It opts in too, yet must be reconciled on every run to fulfill its child slot.
    """

    tracks_value_fingerprint = True

    rows: _FingerprintStore
    child_invalidation: Literal["lossy"] | None = None

    def __init__(self) -> None:
        self.rows = _FingerprintStore()
        self._sink = coco.TargetActionSink.from_fn_with_children(self._apply)

    def _apply(
        self,
        context_provider: coco.ContextProvider,
        actions: Collection[None],
        child_slots: Mapping[int, coco.ChildSlot[_FingerprintStore]],
        /,
    ) -> None:
        for slot in child_slots.values():
            slot.fulfill(self.rows)

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: None | coco.NonExistenceType,
        prev_possible_records: Collection[bytes],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[None, bytes, _FingerprintStore] | None:
        return coco.TargetReconcileOutput(
            action=None,
            sink=self._sink,
            tracking_record=coco.NON_EXISTENCE
            if coco.is_non_existence(desired_state)
            else fingerprint_object(desired_state),
            child_invalidation=self.child_invalidation,
        )


_store = _FingerprintStore()
_provider = coco.register_root_target_states_provider(
    "test_value_fingerprint_tracking/tracked", _store
)
_untracked_store = _UntrackedFingerprintStore()
_untracked_provider = coco.register_root_target_states_provider(
    "test_value_fingerprint_tracking/untracked", _untracked_store
)
_table_store = _TableStore()
_table_provider = coco.register_root_target_states_provider(
    "test_value_fingerprint_tracking/table", _table_store
)

_source_data: dict[str, Any] = {}


def _declare_entries() -> None:
    for key, value in _source_data.items():
        coco.declare_target_state(_provider.target_state(key, value))


def _declare_untracked_entries() -> None:
    for key, value in _source_data.items():
        coco.declare_target_state(_untracked_provider.target_state(key, value))


async def _declare_table_rows() -> None:
    rows_provider = await coco.use_mount(
        coco.component_subpath("setup"), _declare_table
    )
    for key, value in _source_data.items():
        coco.declare_target_state(rows_provider.target_state(key, value))


def _declare_table() -> coco.PendingTargetStateProvider[Any, None]:
    return coco.declare_target_state_with_child(
        _table_provider.target_state("table", None)
    )


@pytest.fixture(autouse=True)
def _reset() -> Iterator[None]:
    for store in (_store, _untracked_store, _table_store.rows):
        store.clear()
    _table_store.child_invalidation = None
    _source_data.clear()
    yield


def _new_app(name: str, main_fn: Any = _declare_entries) -> coco.App[[], None]:
    return coco.App(coco.AppConfig(name=name, environment=coco_env), main_fn)


def test_reconcile_runs_only_for_new_changed_and_removed_values() -> None:
    app = _new_app("test_reconcile_runs_only_for_new_changed_and_removed_values")

    _source_data.update(
        a={"id": 1, "tags": ["x", "y"]}, b={"id": 2, "tags": []}, c="plain str"
    )
    app.update_blocking()
    assert _store.take_reconciles() == [
        _ReconcileCall("a", 0, True),
        _ReconcileCall("b", 0, True),
        _ReconcileCall("c", 0, True),
    ]

    # Equal values, rebuilt as new objects.
    _source_data.update(a={"tags": ["x", "y"], "id": 1}, b={"id": 2, "tags": []})
    app.update_blocking()
    assert _store.take_reconciles() == []

    _source_data["b"] = {"id": 2, "tags": ["z"]}
    del _source_data["c"]
    _source_data["d"] = 4
    app.update_blocking()
    assert _store.take_reconciles() == [
        _ReconcileCall("b", 1, False),
        _ReconcileCall("c", 1, False),
        _ReconcileCall("d", 0, True),
    ]
    assert _store.data == {
        "a": {"id": 1, "tags": ["x", "y"]},
        "b": {"id": 2, "tags": ["z"]},
        "d": 4,
    }

    app.update_blocking()
    assert _store.take_reconciles() == []


def test_handler_without_the_opt_in_reconciles_unchanged_values() -> None:
    app = _new_app(
        "test_handler_without_the_opt_in_reconciles_unchanged_values",
        _declare_untracked_entries,
    )

    _source_data["a"] = {"id": 1}
    app.update_blocking()
    _untracked_store.take_reconciles()

    app.update_blocking()
    assert _untracked_store.take_reconciles() == [_ReconcileCall("a", 1, False)]


class _Point(NamedTuple):
    x: int
    y: int


def test_value_that_is_not_plain_data_is_reconciled_by_the_handler() -> None:
    app = _new_app("test_value_that_is_not_plain_data_is_reconciled_by_the_handler")

    _source_data["a"] = _Point(1, 2)
    app.update_blocking()
    _store.take_reconciles()

    # The handler sees the unchanged value and decides there is nothing to do.
    _store.sink_exception = True
    try:
        app.update_blocking()
    finally:
        _store.sink_exception = False
    assert _store.take_reconciles() == [_ReconcileCall("a", 1, False)]


def test_full_reprocess_reconciles_unchanged_values() -> None:
    app = _new_app("test_full_reprocess_reconciles_unchanged_values")

    _source_data["a"] = {"id": 1}
    app.update_blocking()
    _store.take_reconciles()
    _store.data.clear()

    app.update_blocking(full_reprocess=True)
    assert _store.take_reconciles() == [_ReconcileCall("a", 1, True)]
    assert _store.data == {"a": {"id": 1}}


def test_value_matching_only_some_previous_records_is_reconciled() -> None:
    app = _new_app("test_value_matching_only_some_previous_records_is_reconciled")

    _source_data["a"] = {"id": 1}
    app.update_blocking()

    # A failed sink call leaves both the old and the new record as possible.
    _source_data["a"] = {"id": 2}
    _store.sink_exception = True
    try:
        with pytest.raises(Exception):
            app.update_blocking()
    finally:
        _store.sink_exception = False
    _store.take_reconciles()

    app.update_blocking()
    assert _store.take_reconciles() == [_ReconcileCall("a", 2, False)]
    assert _store.data == {"a": {"id": 2}}

    app.update_blocking()
    assert _store.take_reconciles() == []


def test_lossy_parent_change_reconciles_unchanged_children() -> None:
    app = _new_app(
        "test_lossy_parent_change_reconciles_unchanged_children", _declare_table_rows
    )
    rows = _table_store.rows

    _source_data.update(a={"id": 1}, b={"id": 2})
    app.update_blocking()
    rows.take_reconciles()

    app.update_blocking()
    assert rows.take_reconciles() == []

    _table_store.child_invalidation = "lossy"
    rows.data.clear()
    app.update_blocking()
    assert rows.take_reconciles() == [
        _ReconcileCall("a", 1, True),
        _ReconcileCall("b", 1, True),
    ]
    assert rows.data == {"a": {"id": 1}, "b": {"id": 2}}


def test_memo_key_function_for_a_plain_type_keeps_reconcile_in_charge() -> None:
    app = _new_app("test_memo_key_function_for_a_plain_type_keeps_reconcile_in_charge")

    # With `tuple` keyed by length only, the handler's fingerprints for these
    # two values are equal, so its record must keep deciding what changed.
    register_memo_key_function(tuple, lambda t: len(t))
    try:
        _source_data["a"] = (1, 2)
        app.update_blocking()
        _store.take_reconciles()

        _source_data["a"] = (3, 4)
        app.update_blocking()
        assert _store.take_reconciles() == [_ReconcileCall("a", 1, False)]
        assert _store.data == {"a": (1, 2)}
    finally:
        unregister_memo_key_function(tuple)

    # Back to the default canonical form: the record no longer matches.
    app.update_blocking()
    assert _store.take_reconciles() == [_ReconcileCall("a", 1, False)]
    assert _store.data == {"a": (3, 4)}

    app.update_blocking()
    assert _store.take_reconciles() == []

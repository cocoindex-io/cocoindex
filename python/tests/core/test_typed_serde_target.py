"""End-to-end tests for typed serde with target state tracking records.

Tests cover:
- Tracking records round-trip through serialize/deserialize across updates
- _TypedTargetHandlerWrapper correctly deserializes prev_possible_records
"""

from dataclasses import dataclass
from typing import Any, Collection, NamedTuple

import cocoindex as coco
from cocoindex import (
    ContextProvider,
    NonExistenceType,
    NON_EXISTENCE,
    StableKey,
    TargetReconcileOutput,
    TargetActionSink,
    is_non_existence,
)

from tests import common
from tests.common.target_states import (
    DictDataWithPrev,
    Metrics,
)

coco_env = common.create_test_env(__file__)


# ============================================================================
# Tracking record round-trip via pickle serialization
# ============================================================================
#
# The standard GlobalDictTarget stores arbitrary values as tracking records.
# Here we create a custom target store that uses a typed tracking record
# and verifies that reconcile() receives properly typed records on run 2.


@dataclass(frozen=True)
class TrackingMeta:
    version: int
    label: str


class _RecordWithPrev(NamedTuple):
    data: Any
    prev_records: list[TrackingMeta]
    prev_may_be_missing: bool


class _TypedTrackingStore:
    """Target store whose reconcile receives typed TrackingMeta records."""

    data: dict[str, _RecordWithPrev]
    metrics: Metrics

    def __init__(self) -> None:
        self.data = {}
        self.metrics = Metrics()

    def _sink(
        self,
        context_provider: ContextProvider,
        actions: Collection[tuple[str, _RecordWithPrev | NonExistenceType]],
        /,
    ) -> None:
        for key, value in actions:
            if is_non_existence(value):
                del self.data[key]
                self.metrics.increment("delete")
            else:
                self.data[key] = value
                self.metrics.increment("upsert")
        self.metrics.increment("sink")

    def reconcile(
        self,
        key: StableKey,
        desired_state: Any | NonExistenceType,
        prev_possible_records: Collection[TrackingMeta],
        prev_may_be_missing: bool,
    ) -> (
        TargetReconcileOutput[
            tuple[str, _RecordWithPrev | NonExistenceType], TrackingMeta
        ]
        | None
    ):
        assert isinstance(key, str)
        # Record the types of prev_possible_records for verification
        for rec in prev_possible_records:
            _received_record_types.append(type(rec))

        if is_non_existence(desired_state):
            if len(prev_possible_records) == 0:
                return None
            return TargetReconcileOutput(
                action=(key, NON_EXISTENCE),
                sink=TargetActionSink.from_fn(self._sink),
                tracking_record=NON_EXISTENCE,
            )

        # Short-circuit no-change
        if not prev_may_be_missing and all(
            prev == desired_state for prev in prev_possible_records
        ):
            return None

        version, label = desired_state
        tracking = TrackingMeta(version=version, label=label)
        new_value = _RecordWithPrev(
            data=desired_state,
            prev_records=list(prev_possible_records),
            prev_may_be_missing=prev_may_be_missing,
        )
        return TargetReconcileOutput(
            action=(key, new_value),
            sink=TargetActionSink.from_fn(self._sink),
            tracking_record=tracking,
        )

    def clear(self) -> None:
        self.data.clear()
        self.metrics.clear()


_typed_tracking_store = _TypedTrackingStore()
_typed_tracking_provider = coco.register_root_target_states_provider(
    "test_target_state/typed_tracking", _typed_tracking_store
)
_received_record_types: list[type] = []

_tracking_source_data: dict[str, tuple[int, str]] = {}


@coco.fn
def _declare_tracking() -> None:
    for key, value in _tracking_source_data.items():
        coco.declare_target_state(_typed_tracking_provider.target_state(key, value))


def test_tracking_record_serialize_by_pickle() -> None:
    """Tracking records round-trip: run 1 creates TrackingMeta, run 2 reconcile
    receives deserialized TrackingMeta instances."""
    _typed_tracking_store.clear()
    _tracking_source_data.clear()
    _received_record_types.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_tracking_record_serialize_by_pickle", environment=coco_env
        ),
        _declare_tracking,
    )

    # Run 1: no prev records
    _tracking_source_data["A"] = (1, "alpha")
    app.update_blocking()
    assert _typed_tracking_store.data == {
        "A": _RecordWithPrev(
            data=(1, "alpha"), prev_records=[], prev_may_be_missing=True
        ),
    }

    # Run 2: same data -- reconcile receives prev_possible_records with TrackingMeta
    _received_record_types.clear()
    _tracking_source_data["A"] = (1, "alpha")
    app.update_blocking()
    # The prev_possible_records should contain TrackingMeta instances
    assert all(t is TrackingMeta for t in _received_record_types), (
        f"Expected TrackingMeta types, got {_received_record_types}"
    )

    # Run 3: change value -- verify reconcile still gets typed prev records
    _received_record_types.clear()
    _tracking_source_data["A"] = (2, "beta")
    app.update_blocking()
    assert all(t is TrackingMeta for t in _received_record_types), (
        f"Expected TrackingMeta types on update, got {_received_record_types}"
    )
    assert _typed_tracking_store.data == {
        "A": _RecordWithPrev(
            data=(2, "beta"),
            prev_records=[TrackingMeta(version=1, label="alpha")],
            prev_may_be_missing=False,
        ),
    }


# ============================================================================
# E2E test verifying _TypedTargetHandlerWrapper deserializes records
# ============================================================================
#
# Since PyValue cannot be constructed from Python, we test this E2E:
# a custom handler with typed reconcile, run the app twice, and verify
# the handler receives properly typed TrackingMeta on the second run.


@dataclass(frozen=True)
class MyRecord:
    seq: int
    tag: str


class _TypedRecordStore:
    """Handler whose reconcile() expects Collection[MyRecord]."""

    data: dict[str, Any]
    metrics: Metrics
    received_types: list[type]

    def __init__(self) -> None:
        self.data = {}
        self.metrics = Metrics()
        self.received_types = []

    def _sink(
        self,
        context_provider: ContextProvider,
        actions: Collection[tuple[str, Any]],
        /,
    ) -> None:
        for key, value in actions:
            if is_non_existence(value):
                del self.data[key]
                self.metrics.increment("delete")
            else:
                self.data[key] = value
                self.metrics.increment("upsert")
        self.metrics.increment("sink")

    def reconcile(
        self,
        key: StableKey,
        desired_state: Any | NonExistenceType,
        prev_possible_records: Collection[MyRecord],
        prev_may_be_missing: bool,
    ) -> TargetReconcileOutput[tuple[str, Any], MyRecord] | None:
        assert isinstance(key, str)
        for rec in prev_possible_records:
            self.received_types.append(type(rec))

        if is_non_existence(desired_state):
            if len(prev_possible_records) == 0:
                return None
            return TargetReconcileOutput(
                action=(key, NON_EXISTENCE),
                sink=TargetActionSink.from_fn(self._sink),
                tracking_record=NON_EXISTENCE,
            )

        if not prev_may_be_missing and all(
            isinstance(prev, MyRecord) and prev.seq == desired_state
            for prev in prev_possible_records
        ):
            return None

        record = MyRecord(seq=desired_state, tag=f"tag_{desired_state}")
        return TargetReconcileOutput(
            action=(key, desired_state),
            sink=TargetActionSink.from_fn(self._sink),
            tracking_record=record,
        )

    def clear(self) -> None:
        self.data.clear()
        self.metrics.clear()
        self.received_types.clear()


_typed_record_store = _TypedRecordStore()
_typed_record_provider = coco.register_root_target_states_provider(
    "test_target_state/typed_record", _typed_record_store
)

_typed_record_source_data: dict[str, int] = {}


@coco.fn
def _declare_typed_record() -> None:
    for key, value in _typed_record_source_data.items():
        coco.declare_target_state(_typed_record_provider.target_state(key, value))


def test_typed_handler_wrapper_deserializes() -> None:
    """_TypedTargetHandlerWrapper extracts MyRecord type from reconcile() signature
    and deserializes prev_possible_records to MyRecord instances on run 2."""
    _typed_record_store.clear()
    _typed_record_source_data.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_typed_handler_wrapper_deserializes", environment=coco_env
        ),
        _declare_typed_record,
    )

    # Run 1: no prev records
    _typed_record_source_data["X"] = 42
    app.update_blocking()
    assert _typed_record_store.data == {"X": 42}
    assert _typed_record_store.received_types == []  # no prev records on first run

    # Run 2: prev records should be deserialized MyRecord instances
    _typed_record_store.received_types.clear()
    _typed_record_source_data["X"] = 99  # change value so reconcile runs with prev
    app.update_blocking()
    assert _typed_record_store.data == {"X": 99}
    assert all(t is MyRecord for t in _typed_record_store.received_types), (
        f"Expected MyRecord types, got {_typed_record_store.received_types}"
    )

    # Verify the prev record has correct values
    # Run 3: add another key to verify independent tracking
    _typed_record_store.received_types.clear()
    _typed_record_source_data["Y"] = 7
    app.update_blocking()
    assert _typed_record_store.data == {"X": 99, "Y": 7}
    # X had prev records (from run 2), Y had none
    # Only X's prev records contribute to received_types
    assert all(t is MyRecord for t in _typed_record_store.received_types)

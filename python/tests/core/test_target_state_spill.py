"""End-to-end tests of spilling declared target states to disk.

Under `COCOINDEX_TARGET_STATE_SPILL_THRESHOLD=0`, every declared value and
every action that can spill does, and is read back in chunks of 1 MiB: the
sinks see a component's actions over several calls, in declaration order. The
commit results must be those of the in-memory path, which the same scenarios
check under a threshold nothing reaches.
"""

from __future__ import annotations

import dataclasses
import os
from typing import Any, Collection, NamedTuple

import pytest

import cocoindex as coco
from cocoindex.connectorkits.fingerprint import fingerprint_object

from tests import common
from tests.common.target_states import AtMost, DictDataWithPrev, DictsTarget

_THRESHOLD_ENV = "COCOINDEX_TARGET_STATE_SPILL_THRESHOLD"


def _create_env(threshold: str, suffix: str) -> coco.Environment:
    """An environment created under the given spill threshold, which is read
    from the variable once, when the environment is created."""
    saved = os.environ.get(_THRESHOLD_ENV)
    os.environ[_THRESHOLD_ENV] = threshold
    try:
        return common.create_test_env(__file__, suffix=suffix)
    finally:
        if saved is None:
            del os.environ[_THRESHOLD_ENV]
        else:
            os.environ[_THRESHOLD_ENV] = saved


spill_env = _create_env("0", "spill")
resident_env = _create_env(str(1 << 40), "resident")
_ENVS = {"spill": spill_env, "resident": resident_env}


class _Action(NamedTuple):
    key: str
    value: Any


@dataclasses.dataclass
class _Opaque:
    """Not plain data: cannot spill, so it reaches the handler and the sink as
    the declared object."""

    name: str


class _Store(coco.TargetHandler[Any, bytes, None]):
    """Tracks each value by its fingerprint and records what `reconcile()` and
    the sink observe, including how many actions each sink call carried."""

    tracks_value_fingerprint: bool
    data: dict[str, Any]
    reconciled: dict[str, Any]
    sink_calls: list[int]
    sink_exception: bool

    def __init__(self, tracks_value_fingerprint: bool) -> None:
        self.tracks_value_fingerprint = tracks_value_fingerprint
        self._sink = coco.TargetActionSink.from_fn(self._apply)
        self.data = {}
        self.clear_observations()

    def clear_observations(self) -> None:
        self.reconciled = {}
        self.sink_calls = []
        self.sink_exception = False

    def clear(self) -> None:
        self.data = {}
        self.clear_observations()

    def _apply(
        self, context_provider: coco.ContextProvider, actions: Collection[_Action], /
    ) -> None:
        if self.sink_exception:
            raise ValueError("injected sink exception")
        self.sink_calls.append(len(actions))
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
        if coco.is_non_existence(desired_state):
            if not prev_possible_records:
                return None
            return coco.TargetReconcileOutput(
                action=_Action(key, coco.NON_EXISTENCE),
                sink=self._sink,
                tracking_record=coco.NON_EXISTENCE,
            )
        self.reconciled[key] = desired_state
        record = fingerprint_object(desired_state)
        if not prev_may_be_missing and all(
            prev == record for prev in prev_possible_records
        ):
            return None
        return coco.TargetReconcileOutput(
            action=_Action(key, desired_state),
            sink=self._sink,
            tracking_record=record,
        )


_store = _Store(tracks_value_fingerprint=False)
_provider = coco.register_root_target_states_provider(
    "test_target_state_spill/store", _store
)
_fingerprint_store = _Store(tracks_value_fingerprint=True)
_fingerprint_provider = coco.register_root_target_states_provider(
    "test_target_state_spill/fingerprint_store", _fingerprint_store
)

_declared: dict[str, Any] = {}


@coco.fn
def _declare_all() -> None:
    for key, value in _declared.items():
        coco.declare_target_state(_provider.target_state(key, value))


@coco.fn
def _declare_all_fingerprinted() -> None:
    for key, value in _declared.items():
        coco.declare_target_state(_fingerprint_provider.target_state(key, value))


def _rows(count: int) -> dict[str, dict[str, Any]]:
    """Rows of about 650 bytes encoded: 3000 of them spill past the 1 MiB
    chunk, so the sink gets them over several calls."""
    return {
        f"r{i}": {"id": i, "text": f"{i:06d}" * 100, "tags": ["a", "b"], "n": i}
        for i in range(count)
    }


def _assert_exact(actual: Any, expected: Any) -> None:
    """`actual == expected`, with the same types all the way down."""
    assert type(actual) is type(expected), (actual, expected)
    if isinstance(expected, dict):
        assert list(actual) == list(expected)
        for key in expected:
            _assert_exact(actual[key], expected[key])
    elif isinstance(expected, (list, tuple)):
        assert len(actual) == len(expected)
        for a, e in zip(actual, expected):
            _assert_exact(a, e)
    else:
        assert actual == expected


def _assert_store_matches_declared(store: _Store) -> None:
    assert list(store.data) == list(_declared), "actions reach the sink in order"
    for key, value in _declared.items():
        _assert_exact(store.data[key], value)


@pytest.mark.parametrize("env_name", ["spill", "resident"])
def test_rows_insert_update_delete(env_name: str) -> None:
    _store.clear()
    _declared.clear()
    _declared.update(_rows(3000))
    app = coco.App(
        coco.AppConfig(
            name="test_rows_insert_update_delete", environment=_ENVS[env_name]
        ),
        _declare_all,
    )

    app.update_blocking()
    _assert_store_matches_declared(_store)
    assert set(_store.reconciled) == set(_declared)
    assert sum(_store.sink_calls) == 3000
    if env_name == "spill":
        assert len(_store.sink_calls) > 1, "spilled actions come in chunks"
    else:
        assert _store.sink_calls == [3000]

    # Nothing changed: every value is read back for `reconcile`, which then
    # plans nothing.
    _store.clear_observations()
    app.update_blocking()
    assert _store.sink_calls == []
    assert set(_store.reconciled) == set(_declared)
    for key, value in _declared.items():
        _assert_exact(_store.reconciled[key], value)

    for i in range(10):
        _declared[f"r{i}"] = {**_declared[f"r{i}"], "n": -1}
    for i in range(10, 20):
        del _declared[f"r{i}"]
    _store.clear_observations()
    app.update_blocking()
    assert sum(_store.sink_calls) == 20
    _assert_store_matches_declared(_store)


def test_fingerprints_kept_in_memory_skip_reading_unchanged_values_back() -> None:
    _fingerprint_store.clear()
    _declared.clear()
    _declared.update(_rows(3000))
    app = coco.App(
        coco.AppConfig(
            name="test_fingerprints_kept_in_memory_skip_reading_unchanged_values_back",
            environment=spill_env,
        ),
        _declare_all_fingerprinted,
    )

    app.update_blocking()
    _assert_store_matches_declared(_fingerprint_store)
    assert len(_fingerprint_store.sink_calls) > 1

    # The fingerprint of each spilled value stays in memory, so an unchanged
    # value is known unchanged without `reconcile` (nor the spill file).
    _fingerprint_store.clear_observations()
    app.update_blocking()
    assert _fingerprint_store.reconciled == {}
    assert _fingerprint_store.sink_calls == []

    _declared["r7"] = {**_declared["r7"], "n": -1}
    _fingerprint_store.clear_observations()
    app.update_blocking()
    assert set(_fingerprint_store.reconciled) == {"r7"}
    assert _fingerprint_store.sink_calls == [1]
    _assert_store_matches_declared(_fingerprint_store)


_OPAQUE = _Opaque("kept")


def test_values_of_every_kind_round_trip_through_the_spill() -> None:
    """Scalars and oversized plain data spill (encoded for the spill, whatever
    the codec holds in memory); what is not plain data stays as the object."""
    _store.clear()
    _declared.clear()
    _declared.update(
        {
            "row": {"id": 1, "point": (1.5, 2.5), "tags": ["a"]},
            "bytes": b"payload" * 100,
            "text": "text" * 100,
            "int": 12345,
            "none": None,
            "oversized": {"text": "x" * 100_000, "n": 1},
            "opaque": _OPAQUE,
            "nested_opaque": {"inner": _OPAQUE},
        }
    )
    app = coco.App(
        coco.AppConfig(
            name="test_values_of_every_kind_round_trip_through_the_spill",
            environment=spill_env,
        ),
        _declare_all,
    )
    app.update_blocking()
    _assert_store_matches_declared(_store)
    for key, value in _declared.items():
        _assert_exact(_store.reconciled[key], value)
    assert _store.reconciled["opaque"] is _OPAQUE
    assert _store.data["opaque"] is _OPAQUE
    assert _store.data["nested_opaque"]["inner"] is _OPAQUE

    _store.clear_observations()
    app.update_blocking()
    assert _store.sink_calls == []
    assert set(_store.reconciled) == set(_declared)


def test_spilled_actions_are_applied_after_a_sink_failure_is_fixed() -> None:
    _store.clear()
    _declared.clear()
    _declared.update(_rows(3000))
    app = coco.App(
        coco.AppConfig(
            name="test_spilled_actions_are_applied_after_a_sink_failure_is_fixed",
            environment=spill_env,
        ),
        _declare_all,
    )
    _store.sink_exception = True
    with pytest.raises(Exception, match="injected sink exception"):
        app.update_blocking()
    assert _store.data == {}

    _store.clear_observations()
    app.update_blocking()
    _assert_store_matches_declared(_store)
    assert sum(_store.sink_calls) == 3000


async def _declare_dicts_with_rows() -> None:
    with coco.component_subpath("dict"):
        for name, data in _declared.items():
            single_dict_provider = await coco.use_mount(
                coco.component_subpath(name),
                DictsTarget.declare_dict_target,
                name,
            )
            for key, value in data.items():
                coco.declare_target_state(single_dict_provider.target_state(key, value))


def test_container_actions_and_their_child_slots_spill() -> None:
    """A container's action spills too; the slot for its child provider is
    minted when the chunk holding the action reaches the sink."""
    DictsTarget.store.clear()
    _declared.clear()
    _declared["D1"] = {f"k{i}": i for i in range(50)}
    _declared["D2"] = {"a": "x"}
    app = coco.App(
        coco.AppConfig(
            name="test_container_actions_and_their_child_slots_spill",
            environment=spill_env,
        ),
        _declare_dicts_with_rows,
    )
    app.update_blocking()
    assert DictsTarget.store.data == {
        name: {
            key: DictDataWithPrev(data=value, prev=[], prev_may_be_missing=True)
            for key, value in data.items()
        }
        for name, data in _declared.items()
    }
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(2), "insert": 2}
    assert DictsTarget.store.collect_child_metrics() == {
        "sink": AtMost(2),
        "upsert": 51,
    }

    del _declared["D2"]
    _declared["D1"]["k0"] = -1
    app.update_blocking()
    assert set(DictsTarget.store.data) == {"D1"}
    assert DictsTarget.store.data["D1"]["k0"] == DictDataWithPrev(
        data=-1, prev=[0], prev_may_be_missing=False
    )
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(2), "delete": 1}
    assert DictsTarget.store.collect_child_metrics() == {
        "sink": AtMost(1),
        "upsert": 1,
    }


def test_unparsable_threshold_fails_environment_creation() -> None:
    with pytest.raises(Exception, match=_THRESHOLD_ENV):
        _create_env("lots", "unparsable")

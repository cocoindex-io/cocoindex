"""End-to-end tests of how the engine holds declared target-state values.

Plain-data values are held encoded from declaration on: `reconcile()` and the
sink get equal objects of the same types back. The engine releases every
declared value once pre-commit has reconciled it, before any sink runs.
"""

import dataclasses
import datetime
import decimal
import gc
import ipaddress
import uuid
import weakref
from typing import Any, Callable, Collection, NamedTuple

import numpy as np

import cocoindex as coco
from cocoindex import (
    ContextProvider,
    NonExistenceType,
    StableKey,
    TargetActionSink,
    TargetReconcileOutput,
    is_non_existence,
)

from tests import common

coco_env = common.create_test_env(__file__)


class _Point(NamedTuple):
    x: int
    y: int


@dataclasses.dataclass
class _Opaque:
    """Not plain data: held as the declared object itself."""

    name: str


class _Action(NamedTuple):
    key: str
    value: Any


class _RecordingStore:
    """Records what `reconcile()` and the sink observe; never skips.

    With `retains_values` off, neither the record nor the action keeps the
    value `reconcile()` got.
    """

    reconciled: dict[str, Any]
    applied: dict[str, Any]
    retains_values: bool
    before_sink: list[Callable[[], None]]

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.reconciled = {}
        self.applied = {}
        self.retains_values = True
        self.before_sink = []

    def _sink(
        self, context_provider: ContextProvider, actions: Collection[_Action], /
    ) -> None:
        for hook in self.before_sink:
            hook()
        for action in actions:
            self.applied[action.key] = action.value

    def reconcile(
        self,
        key: StableKey,
        desired_state: Any | NonExistenceType,
        prev_possible_records: Collection[None],
        prev_may_be_missing: bool,
        /,
    ) -> TargetReconcileOutput[_Action, None] | None:
        assert isinstance(key, str)
        if is_non_existence(desired_state):
            return None
        kept = desired_state if self.retains_values else None
        self.reconciled[key] = kept
        return TargetReconcileOutput(
            action=_Action(key, kept),
            sink=TargetActionSink.from_fn(self._sink),
            tracking_record=None,
        )


_store = _RecordingStore()
_provider = coco.register_root_target_states_provider(
    "test_target_state_values/recording", _store
)


def _assert_exact(actual: Any, expected: Any) -> None:
    """`actual == expected`, with the same types all the way down."""
    assert type(actual) is type(expected), (actual, expected)
    if isinstance(expected, np.ndarray):
        assert actual.dtype == expected.dtype
        assert (actual == expected).all()
    elif isinstance(expected, dict):
        assert list(actual) == list(expected)
        for key in expected:
            _assert_exact(actual[key], expected[key])
    elif isinstance(expected, (list, tuple)):
        assert len(actual) == len(expected)
        for a, e in zip(actual, expected):
            _assert_exact(a, e)
    else:
        assert actual == expected


_OPAQUE = _Opaque("kept")

_DECLARED: dict[str, Any] = {
    "row": {
        "id": 7,
        "tags": ["a", "b"],
        "point": (1.5, 2.5),
        "price": decimal.Decimal("9.90"),
        "at": datetime.datetime(2024, 1, 2, tzinfo=datetime.timezone.utc),
        "uid": uuid.UUID("12345678-1234-5678-1234-567812345678"),
        "blob": b"\x00\x01",
        "vec": np.arange(3, dtype=np.float32),
        "missing": None,
    },
    "nested": {"meta": {"k": [1, (2, 3)]}, "ip": ipaddress.ip_address("10.0.0.1")},
    "tuple": (1, [2, 3]),
    "named": _Point(1, 2),
    "opaque": _OPAQUE,
}


@coco.fn
def _declare_all() -> None:
    for key, value in _DECLARED.items():
        coco.declare_target_state(_provider.target_state(key, value))


def test_reconcile_and_sink_get_exact_values() -> None:
    _store.reset()
    app = coco.App(
        coco.AppConfig(
            name="test_reconcile_and_sink_get_exact_values", environment=coco_env
        ),
        _declare_all,
    )
    app.update_blocking()

    assert set(_store.reconciled) == set(_DECLARED)
    assert set(_store.applied) == set(_DECLARED)
    for key, declared in _DECLARED.items():
        _assert_exact(_store.reconciled[key], declared)
        _assert_exact(_store.applied[key], declared)
    # A value that is not plain data reaches the handler as the declared object.
    assert _store.reconciled["opaque"] is _OPAQUE
    assert _store.applied["opaque"] is _OPAQUE


_vector_refs: list[weakref.ref[np.ndarray]] = []


@coco.fn
def _declare_vector_row() -> None:
    vector = np.arange(4.0)
    _vector_refs.append(weakref.ref(vector))
    coco.declare_target_state(_provider.target_state("v", {"id": 1, "vec": vector}))


def test_plain_data_value_is_not_kept_as_declared() -> None:
    """The engine holds the value's encoding, not the declared objects."""
    _store.reset()
    _vector_refs.clear()
    observed: list[bool] = []

    def check_released() -> None:
        gc.collect()
        observed.append(all(ref() is None for ref in _vector_refs))

    # Until the sink decodes the action, nothing holds the declared vector.
    _store.before_sink.append(check_released)
    app = coco.App(
        coco.AppConfig(
            name="test_plain_data_value_is_not_kept_as_declared", environment=coco_env
        ),
        _declare_vector_row,
    )
    app.update_blocking()
    assert observed == [True]
    _assert_exact(_store.applied["v"], {"id": 1, "vec": np.arange(4.0)})


_opaque_refs: list[weakref.ref[_Opaque]] = []


@coco.fn
def _declare_opaque() -> None:
    value = _Opaque("released")
    _opaque_refs.append(weakref.ref(value))
    coco.declare_target_state(_provider.target_state("o", value))


def test_declared_values_are_released_before_sinks_run() -> None:
    """Pre-commit is the last reader of the declared values: the engine drops
    them before applying actions, even those it holds as objects."""
    _store.reset()
    _store.retains_values = False
    _opaque_refs.clear()
    observed: list[bool] = []

    def check_released() -> None:
        gc.collect()
        observed.append(all(ref() is None for ref in _opaque_refs))

    _store.before_sink.append(check_released)
    app = coco.App(
        coco.AppConfig(
            name="test_declared_values_are_released_before_sinks_run",
            environment=coco_env,
        ),
        _declare_opaque,
    )
    app.update_blocking()
    assert observed == [True]
    assert set(_store.applied) == {"o"}

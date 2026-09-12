"""Tests for value-based ``TargetActionSink`` identity.

Sinks built from callbacks of the same type that compare equal share one
batching identity: their actions land in the same engine batch and are applied
in one call, even when each ``reconcile()`` constructs a fresh (equal)
callback. An idle identity — no live sink object, no pending actions — is
released rather than pinned for the process lifetime, which is why a callback
must support weak references.
"""

from __future__ import annotations

import gc
import threading
import weakref
from dataclasses import dataclass
from typing import Any, Collection, NamedTuple

import pytest

import cocoindex as coco
from tests import common

coco_env = common.create_test_env(__file__)

_batches: list[list[tuple[Any, Any]]] = []
_batches_lock = threading.Lock()


@dataclass(frozen=True)
class _RecordingSink:
    """Value-keyed sink callback: equal instances must share one sink identity."""

    group: str

    async def __call__(
        self,
        context_provider: coco.ContextProvider,
        actions: Collection[tuple[Any, Any]],
        /,
    ) -> None:
        with _batches_lock:
            _batches.append(list(actions))


@dataclass(frozen=True)
class _OtherRecordingSink:
    """A distinct frozen-dataclass type with the same fields as ``_RecordingSink``."""

    group: str

    async def __call__(
        self,
        context_provider: coco.ContextProvider,
        actions: Collection[tuple[Any, Any]],
        /,
    ) -> None:
        pass


class _PermissiveSink:
    """Weakref-able callback whose ``__eq__`` is not type-safe: it equals any
    object carrying an equal ``group``, whatever that object's class."""

    def __init__(self, group: str) -> None:
        self.group = group

    def __eq__(self, other: object) -> bool:
        return getattr(other, "group", None) == self.group

    def __hash__(self) -> int:
        return hash(self.group)

    async def __call__(
        self,
        context_provider: coco.ContextProvider,
        actions: Collection[tuple[Any, Any]],
        /,
    ) -> None:
        pass


class _OtherPermissiveSink(_PermissiveSink):
    """A distinct type whose instances compare equal to ``_PermissiveSink``'s."""


class _NtSink(NamedTuple):
    """NamedTuple callback: equal to any same-shaped tuple and not weakref-able,
    so the framework must reject it rather than canonicalize it."""

    group: str

    async def __call__(
        self,
        context_provider: coco.ContextProvider,
        actions: Collection[tuple[Any, Any]],
        /,
    ) -> None:
        pass


class _GroupedHandler:
    """Routes each target state to a per-group sink built fresh per reconcile.

    The key is a ``(group, n)`` tuple; the sink callback is keyed by the group
    alone, so all of one group's actions must batch together purely through
    callback value equality.
    """

    def reconcile(
        self,
        key: Any,
        desired_state: Any | coco.NonExistenceType,
        prev_possible_records: Collection[Any],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[tuple[Any, Any], Any] | None:
        if coco.is_non_existence(desired_state):
            return None
        return coco.TargetReconcileOutput(
            action=(key, desired_state),
            sink=coco.TargetActionSink.from_async_fn(_RecordingSink(key[0])),
            tracking_record=desired_state,
        )


_provider = coco.register_root_target_states_provider(
    "test_target_sink_identity/records", _GroupedHandler()
)


def test_equal_callback_sinks_batch_together() -> None:
    _batches.clear()

    def declare() -> None:
        for i in range(4):
            coco.declare_target_state(_provider.target_state(("g", i), f"v{i}"))

    app = coco.App(
        coco.AppConfig(name="test_sink_identity_batch", environment=coco_env),
        declare,
    )
    app.update_blocking()

    # All four actions share one sink identity (equal callbacks), so the
    # engine must apply them in a single batch.
    assert sorted(len(b) for b in _batches) == [4]


def test_unequal_callback_sinks_stay_separate() -> None:
    _batches.clear()

    def declare() -> None:
        coco.declare_target_state(_provider.target_state(("x", 0), "v0"))
        coco.declare_target_state(_provider.target_state(("x", 1), "v1"))
        coco.declare_target_state(_provider.target_state(("y", 0), "v0"))

    app = coco.App(
        coco.AppConfig(name="test_sink_identity_separate", environment=coco_env),
        declare,
    )
    app.update_blocking()

    assert sorted(len(b) for b in _batches) == [1, 2]


def test_sink_identity_by_callback_value() -> None:
    async def _noop(
        context_provider: coco.ContextProvider,
        actions: Collection[Any],
        /,
    ) -> None:
        pass

    # Same function object: same identity.
    assert (
        coco.TargetActionSink.from_async_fn(_noop)._core
        == coco.TargetActionSink.from_async_fn(_noop)._core
    )

    # Equal frozen-dataclass callables: same identity; different values: distinct.
    a1 = coco.TargetActionSink.from_async_fn(_RecordingSink("a"))
    a2 = coco.TargetActionSink.from_async_fn(_RecordingSink("a"))
    b = coco.TargetActionSink.from_async_fn(_RecordingSink("b"))
    assert a1._core == a2._core
    assert hash(a1._core) == hash(a2._core)
    assert a1._core != b._core


def test_sink_identity_is_keyed_by_callback_type() -> None:
    # Distinct frozen-dataclass types with equal field values: distinct identities.
    assert (
        coco.TargetActionSink.from_async_fn(_RecordingSink("a"))._core
        != coco.TargetActionSink.from_async_fn(_OtherRecordingSink("a"))._core
    )

    # A permissive ``__eq__`` that holds across types must not merge identities
    # either — the engine would otherwise route one type's actions to the
    # other type's ``__call__``. Within one type, value equality still shares.
    assert _PermissiveSink("a") == _OtherPermissiveSink("a")
    p1 = coco.TargetActionSink.from_async_fn(_PermissiveSink("a"))
    p2 = coco.TargetActionSink.from_async_fn(_PermissiveSink("a"))
    other = coco.TargetActionSink.from_async_fn(_OtherPermissiveSink("a"))
    assert p1._core == p2._core
    assert p1._core != other._core


def test_non_weakrefable_callback_is_rejected() -> None:
    with pytest.raises(TypeError, match="must support weak references"):
        coco.TargetActionSink.from_async_fn(_NtSink("a"))

    @dataclass(frozen=True, slots=True)
    class _SlotsSink:
        group: str

        def __call__(
            self,
            context_provider: coco.ContextProvider,
            actions: Collection[tuple[Any, Any]],
            /,
        ) -> None:
            pass

    with pytest.raises(TypeError, match="weakref_slot=True"):
        coco.TargetActionSink.from_fn(_SlotsSink("a"))

    @dataclass(frozen=True, slots=True, weakref_slot=True)
    class _WeakrefSlotsSink:
        group: str

        def __call__(
            self,
            context_provider: coco.ContextProvider,
            actions: Collection[tuple[Any, Any]],
            /,
        ) -> None:
            pass

    assert (
        coco.TargetActionSink.from_fn(_WeakrefSlotsSink("a"))._core
        == coco.TargetActionSink.from_fn(_WeakrefSlotsSink("a"))._core
    )


def test_sync_sink_identity_and_async_separation() -> None:
    def _sync_noop(
        context_provider: coco.ContextProvider,
        actions: Collection[Any],
        /,
    ) -> None:
        pass

    # Sync sinks intern by callback value like async ones.
    assert (
        coco.TargetActionSink.from_fn(_sync_noop)._core
        == coco.TargetActionSink.from_fn(_sync_noop)._core
    )

    # The same callback object registered as sync vs async must NOT share an
    # identity: the identity fixes how the callback is invoked.
    async def _dual(
        context_provider: coco.ContextProvider,
        actions: Collection[Any],
        /,
    ) -> None:
        pass

    assert (
        coco.TargetActionSink.from_fn(_dual)._core  # type: ignore[arg-type]
        != coco.TargetActionSink.from_async_fn(_dual)._core
    )


def test_idle_sink_identity_is_released() -> None:
    callback = _RecordingSink("gc-probe")
    callback_ref = weakref.ref(callback)
    sink = coco.TargetActionSink.from_async_fn(callback)
    del callback
    gc.collect()
    # The sink keeps its callback (the canonical object) alive.
    assert callback_ref() is not None

    del sink
    for _ in range(3):
        gc.collect()
    # With the sink gone and nothing pending, nothing pins the callback: the
    # deduper and the keeper registry hold only weak references.
    assert callback_ref() is None


def test_dead_canonical_is_replaced() -> None:
    from cocoindex._internal.target_state import _ObjectDeduper

    deduper = _ObjectDeduper()
    a1 = _RecordingSink("turnover")
    assert deduper.get_canonical(a1) is a1
    a2 = _RecordingSink("turnover")
    assert deduper.get_canonical(a2) is a1

    del a1
    gc.collect()
    a3 = _RecordingSink("turnover")
    assert deduper.get_canonical(a3) is a3

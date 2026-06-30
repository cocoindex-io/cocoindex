"""In-memory live key-value map.

``LiveMap[K, V]`` bridges live data-*producing* logic and live data-*consuming* logic within a
single CocoIndex session. The producing side declares ``(key, value)`` entries as **target
states** via :meth:`LiveMap.declare_entry`; the consuming side reads it as a
``coco.LiveMapView`` via ``coco.mount_each``. All data is held in an in-process ``dict`` that
the engine keeps in sync through normal target-state ownership, and that same ``dict`` is
exposed as a live source for downstream components.

Designed for live mode (``app.update(live=True)``). See ``specs/live_map`` for the full design.

Example::

    lm: LiveMap[str, str] = await LiveMap.create()   # inside the app's component tree
    lm.declare_entry(key, value)                      # producer: inside any component
    await coco.mount_each(process_entry, lm)          # consumer: one component per entry
"""

from __future__ import annotations

import asyncio as _asyncio
import uuid as _uuid
import weakref as _weakref
from collections.abc import AsyncIterator as _AsyncIterator
from collections.abc import Collection as _Collection
from collections.abc import Sequence as _Sequence
from dataclasses import dataclass as _dataclass
from typing import Any as _Any
from typing import Generic as _Generic
from typing import NamedTuple as _NamedTuple
from typing import TypeVar as _TypeVar

import msgspec as _msgspec

import cocoindex as _coco

__all__ = ["LiveMap"]

# K becomes both the entry target-state key and the consumer's per-entry component subpath,
# so it must be a StableKey. V is any `==`-comparable value (no hashability required).
_K = _TypeVar("_K", bound="_coco.StableKey")
_V = _TypeVar("_V")

# Sentinel distinguishing "key absent" from "present with value None" in the == gate.
_MISSING: _Any = object()

# Maps a LiveMap's UUID to its live instance so the shared, stateless container sink can
# recover the Python object during apply. Weak so instances aren't retained past their
# container's teardown; a bound `_EntryHandler` holds a strong ref, keeping an in-use map
# alive (see specs/live_map/design.md).
_REGISTRY: "_weakref.WeakValueDictionary[_uuid.UUID, LiveMap[_Any, _Any]]" = (
    _weakref.WeakValueDictionary()
)


# ============================================================================
# Private target-state types
# ============================================================================


@_dataclass(frozen=True, slots=True)
class _ContainerSpec:
    """Marker value for the per-map container target state (keyed by the map's UUID)."""


class _ContainerRecord(_msgspec.Struct, frozen=True):
    """Minimal existence marker persisted for the container target state."""


class _EntryRecord(_msgspec.Struct, frozen=True):
    """Minimal existence marker persisted for an entry target state.

    Never read — its only job is to be a non-``NON_EXISTENCE`` value so the engine records
    "this key exists", which lets a later run drive a delete when the producer stops declaring
    it. Change detection for the consumer is done by ``==`` in the sink, not from this record.
    """


class _ContainerAction(_NamedTuple):
    live_map: "LiveMap[_Any, _Any] | None"
    deleted: bool


class _EntryAction(_NamedTuple):
    live_map: "LiveMap[_Any, _Any]"
    key: _coco.StableKey
    value: _Any
    deleted: bool


class _Change(_NamedTuple):
    key: _Any
    value: _Any
    deleted: bool


# ============================================================================
# Handlers and sinks (producer side)
# ============================================================================


class _EntryHandler:
    """Per-instance target handler for entries; closes over one ``LiveMap``.

    Constructed by the container sink (bound to the instance recovered from the registry) and
    reused for every entry declared into that map.
    """

    __slots__ = ("_live_map",)

    def __init__(self, live_map: "LiveMap[_Any, _Any]") -> None:
        self._live_map = live_map

    def reconcile(
        self,
        key: _coco.StableKey,
        desired_state: _Any | _coco.NonExistenceType,
        prev_possible_records: _Collection[_EntryRecord],
        prev_may_be_missing: bool,
        /,
    ) -> "_coco.TargetReconcileOutput[_EntryAction, _EntryRecord] | None":
        # Never skip: applying to an in-memory dict is cheap, and the `==` gate in the sink
        # decides whether to notify the consumer. `prev_possible_records` is intentionally
        # unused (no fingerprint compare).
        if _coco.is_non_existence(desired_state):
            return _coco.TargetReconcileOutput(
                action=_EntryAction(self._live_map, key, None, True),
                sink=_ENTRY_SINK,
                tracking_record=_coco.NON_EXISTENCE,
            )
        return _coco.TargetReconcileOutput(
            action=_EntryAction(self._live_map, key, desired_state, False),
            sink=_ENTRY_SINK,
            tracking_record=_EntryRecord(),
        )


class _ContainerHandler:
    """Root handler: binds a per-instance ``_EntryHandler`` via the UUID→instance registry."""

    def reconcile(
        self,
        key: _coco.StableKey,
        desired_state: "_ContainerSpec | _coco.NonExistenceType",
        prev_possible_records: _Collection[_ContainerRecord],
        prev_may_be_missing: bool,
        /,
    ) -> "_coco.TargetReconcileOutput[_ContainerAction, _ContainerRecord, _EntryHandler] | None":
        if _coco.is_non_existence(desired_state):
            return _coco.TargetReconcileOutput(
                action=_ContainerAction(None, True),
                sink=_CONTAINER_SINK,
                tracking_record=_coco.NON_EXISTENCE,
            )
        assert isinstance(key, _uuid.UUID)
        return _coco.TargetReconcileOutput(
            action=_ContainerAction(_REGISTRY.get(key), False),
            sink=_CONTAINER_SINK,
            tracking_record=_ContainerRecord(),
        )


async def _apply_entry_actions(
    context_provider: _coco.ContextProvider,
    actions: _Sequence[_EntryAction],
    /,
) -> None:
    # Runs on the app event loop (async sink), so it mutates `_entries` and notifies the
    # watcher queue directly — no locks, no cross-thread marshalling.
    for action in actions:
        live_map = action.live_map
        if action.deleted:
            if action.key in live_map._entries:
                del live_map._entries[action.key]
                live_map._emit(_Change(action.key, None, True))
        else:
            prev = live_map._entries.get(action.key, _MISSING)
            if prev is _MISSING or prev != action.value:
                live_map._entries[action.key] = action.value
                live_map._emit(_Change(action.key, action.value, False))


async def _apply_container_actions(
    context_provider: _coco.ContextProvider,
    actions: _Sequence[_ContainerAction],
    /,
) -> "list[_coco.ChildTargetDef[_EntryHandler] | None]":
    out: "list[_coco.ChildTargetDef[_EntryHandler] | None]" = []
    for action in actions:
        if action.deleted or action.live_map is None:
            out.append(None)
        else:
            out.append(_coco.ChildTargetDef(handler=_EntryHandler(action.live_map)))
    return out


_ENTRY_SINK: _coco.TargetActionSink[_EntryAction, None] = (
    _coco.TargetActionSink.from_async_fn(_apply_entry_actions)
)
_CONTAINER_SINK: _coco.TargetActionSink[_ContainerAction, _EntryHandler] = (
    _coco.TargetActionSink.from_async_fn(_apply_container_actions)
)
_CONTAINER_PROVIDER = _coco.register_root_target_states_provider(
    "cocoindex/livemap", _ContainerHandler()
)


# ============================================================================
# Public API
# ============================================================================


class LiveMap(_Generic[_K, _V]):
    """An in-memory, keyed collection that is both a target and a ``coco.LiveMapView``.

    Create with ``await LiveMap.create()`` from inside the app's component tree. Producers add
    entries with :meth:`declare_entry` from inside any component; consumers pass the map to
    ``coco.mount_each`` to process one component per entry, kept in sync as entries appear,
    change, and disappear. An entry exists as long as some live component declares it; when its
    declaring component stops declaring it (or disappears), the entry is removed.

    Single active ``watch()`` at a time. ``K`` must be a ``coco.StableKey``; ``V`` is any
    ``==``-comparable value (no hashability required).
    """

    __slots__ = (
        "_uuid",
        "_entries",
        "_entry_provider",
        "_watcher_queue",
        "__weakref__",
    )

    _uuid: _uuid.UUID
    _entries: dict[_K, _V]
    _entry_provider: "_coco.TargetStateProvider[_Any, None]"
    _watcher_queue: "_asyncio.Queue[_Change] | None"

    def __init__(self) -> None:
        # Only sets up in-memory state — `create()` mounts the container and sets
        # `_entry_provider`; the map isn't usable until then.
        self._uuid = _uuid.uuid4()
        self._entries = {}
        self._watcher_queue = None

    @classmethod
    async def create(cls) -> "LiveMap[_K, _V]":
        """Create a LiveMap and mount its backing target. Call inside a component context."""
        self = cls()
        _REGISTRY[self._uuid] = self
        container = _CONTAINER_PROVIDER.target_state(self._uuid, _ContainerSpec())
        self._entry_provider = await _coco.mount_target(container)
        return self

    def declare_entry(self, key: _K, value: _V) -> None:
        """Declare an entry, owned by the calling component. Call inside a component context."""
        _coco.declare_target_state(self._entry_provider.target_state(key, value))

    def __aiter__(self) -> _AsyncIterator[tuple[_K, _V]]:
        return self._scan()

    async def _scan(self) -> _AsyncIterator[tuple[_K, _V]]:
        # Snapshot synchronously so a sink firing between yields can't mutate mid-iteration.
        for item in list(self._entries.items()):
            yield item

    async def watch(self, subscriber: "_coco.LiveMapSubscriber[_K, _V]") -> None:
        """Deliver an initial snapshot then incremental changes. Drives one consumer."""
        if self._watcher_queue is not None:
            raise RuntimeError("LiveMap supports a single active watch() at a time.")
        queue: _asyncio.Queue[_Change] = _asyncio.Queue()
        self._watcher_queue = (
            queue  # arm before the scan so concurrent changes aren't lost
        )
        try:
            await subscriber.update_all()
            await subscriber.mark_ready()
            while True:
                change = await queue.get()
                if change.deleted:
                    handle = await subscriber.delete(change.key)
                else:
                    handle = await subscriber.update(change.key, change.value)
                await handle.ready()
        finally:
            self._watcher_queue = None

    def _emit(self, change: _Change) -> None:
        queue = self._watcher_queue
        if queue is not None:
            queue.put_nowait(change)

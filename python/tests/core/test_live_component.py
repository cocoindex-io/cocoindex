from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest

import cocoindex as coco

from tests import common
from tests.common.target_states import GlobalDictTarget, DictDataWithPrev

coco_env = common.create_test_env(__file__)

_source_data: dict[str, int] = {}


# ============================================================================
# Rejection tests
# ============================================================================


class _MinimalLiveComponent:
    async def process(self) -> None:
        pass

    async def process_live(self, operator: coco.LiveComponentOperator) -> None:
        pass


def test_live_component_rejected_in_use_mount() -> None:
    async def _main() -> None:
        await coco.use_mount(coco.component_subpath("x"), _MinimalLiveComponent)

    app = coco.App(
        coco.AppConfig(name="test_rejected_use_mount", environment=coco_env),
        _main,
    )
    with pytest.raises(TypeError, match="cannot be used with use_mount"):
        app.update_blocking()


def test_live_component_rejected_in_mount_each() -> None:
    async def _main() -> None:
        await coco.mount_each(  # type: ignore[call-overload]
            coco.component_subpath("x"),
            _MinimalLiveComponent,
            [("a",), ("b",)],
        )

    app = coco.App(
        coco.AppConfig(name="test_rejected_mount_each", environment=coco_env),
        _main,
    )
    with pytest.raises(TypeError, match="cannot be used with mount_each"):
        app.update_blocking()


# ============================================================================
# Basic lifecycle tests
# ============================================================================


def _declare_source_entries() -> None:
    for key, value in _source_data.items():
        coco.declare_target_state(GlobalDictTarget.target_state(key, value))


class _BasicLiveComponent:
    """LiveComponent that processes _source_data into GlobalDictTarget."""

    async def process(self) -> None:
        _declare_source_entries()

    async def process_live(self, operator: coco.LiveComponentOperator) -> None:
        await operator.update_full()
        await operator.mark_ready()


def test_live_component_basic_full_update() -> None:
    GlobalDictTarget.store.clear()
    _source_data.clear()

    _source_data["a"] = 1
    _source_data["b"] = 2

    async def _main() -> None:
        await coco.mount(coco.component_subpath("live"), _BasicLiveComponent)

    app = coco.App(
        coco.AppConfig(name="test_live_basic_full_update", environment=coco_env),
        _main,
    )
    app.update_blocking(live=True)

    assert GlobalDictTarget.store.data == {
        "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
        "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }


class _CatchUpLiveComponent:
    """LiveComponent that loops forever after mark_ready (tests catch-up termination)."""

    async def process(self) -> None:
        _declare_source_entries()

    async def process_live(self, operator: coco.LiveComponentOperator) -> None:
        await operator.update_full()
        await operator.mark_ready()
        # In catch-up mode, mark_ready should terminate process_live.
        # This infinite loop should never execute.
        while True:
            await asyncio.sleep(1)


def test_live_component_catch_up_mode() -> None:
    GlobalDictTarget.store.clear()
    _source_data.clear()

    _source_data["x"] = 10

    async def _main() -> None:
        await coco.mount(coco.component_subpath("live"), _CatchUpLiveComponent)

    app = coco.App(
        coco.AppConfig(name="test_live_catch_up_mode", environment=coco_env),
        _main,
    )
    # Catch-up mode (default): should complete without hanging
    app.update_blocking()

    assert GlobalDictTarget.store.data == {
        "x": DictDataWithPrev(data=10, prev=[], prev_may_be_missing=True),
    }


class _AutoReadyLiveComponent:
    """LiveComponent where process_live returns without calling mark_ready."""

    async def process(self) -> None:
        _declare_source_entries()

    async def process_live(self, operator: coco.LiveComponentOperator) -> None:
        await operator.update_full()
        # Deliberately NOT calling mark_ready — ensure_mark_ready should auto-call it


def test_live_component_mark_ready_auto_on_return() -> None:
    GlobalDictTarget.store.clear()
    _source_data.clear()

    _source_data["auto"] = 42

    async def _main() -> None:
        await coco.mount(coco.component_subpath("live"), _AutoReadyLiveComponent)

    app = coco.App(
        coco.AppConfig(name="test_live_mark_ready_auto", environment=coco_env),
        _main,
    )
    app.update_blocking(live=True)

    assert GlobalDictTarget.store.data == {
        "auto": DictDataWithPrev(data=42, prev=[], prev_may_be_missing=True),
    }


# ============================================================================
# Incremental operations
# ============================================================================


def _declare_item(key: str, value: int) -> None:
    coco.declare_target_state(GlobalDictTarget.target_state(key, value))


class _IncrementalUpdateLiveComponent:
    """LiveComponent that does a full update then an incremental update."""

    async def process(self) -> None:
        _declare_source_entries()

    async def process_live(self, operator: coco.LiveComponentOperator) -> None:
        await operator.update_full()
        # Incremental: add a new item not in _source_data
        handle = await operator.update(
            coco.component_subpath("new_item"), _declare_item, "new_key", 99
        )
        await handle.ready()
        # mark_ready called AFTER incremental operations so update_blocking waits
        await operator.mark_ready()


def test_live_component_incremental_update() -> None:
    GlobalDictTarget.store.clear()
    _source_data.clear()

    _source_data["a"] = 1
    _source_data["b"] = 2

    async def _main() -> None:
        await coco.mount(
            coco.component_subpath("live"), _IncrementalUpdateLiveComponent
        )

    app = coco.App(
        coco.AppConfig(name="test_live_incremental_update", environment=coco_env),
        _main,
    )
    app.update_blocking(live=True)

    assert "a" in GlobalDictTarget.store.data
    assert "b" in GlobalDictTarget.store.data
    assert "new_key" in GlobalDictTarget.store.data
    assert GlobalDictTarget.store.data["new_key"].data == 99


class _IncrementalDeleteDirectLiveComponent:
    """LiveComponent that tests direct deletion via operator.delete().

    process() mounts child components for each key in _source_data.
    process_live() does a full update, then directly deletes one child.
    """

    async def process(self) -> None:
        for key, value in _source_data.items():
            await coco.mount(coco.component_subpath(key), _declare_item, key, value)

    async def process_live(self, operator: coco.LiveComponentOperator) -> None:
        await operator.update_full()
        # Directly delete the child that was mounted by process() for key "b"
        handle = await operator.delete(coco.component_subpath("b"))
        await handle.ready()
        await operator.mark_ready()


def test_live_component_incremental_delete_direct() -> None:
    """operator.delete() removes a child originally created by update_full()."""
    GlobalDictTarget.store.clear()
    _source_data.clear()

    _source_data["a"] = 1
    _source_data["b"] = 2

    async def _main() -> None:
        await coco.mount(
            coco.component_subpath("live"), _IncrementalDeleteDirectLiveComponent
        )

    app = coco.App(
        coco.AppConfig(
            name="test_live_incremental_delete_direct", environment=coco_env
        ),
        _main,
    )
    app.update_blocking(live=True)

    # "a" should remain, "b" should be deleted
    assert "a" in GlobalDictTarget.store.data
    assert "b" not in GlobalDictTarget.store.data


class _IncrementalDeleteNoStaleComponent:
    """LiveComponent for testing that incremental delete doesn't leave stale tombstones.

    First run: process() mounts "a" and "b", process_live() deletes "b".
    Second run: process() mounts only "a", process_live() does nothing extra.
    The second run should NOT produce an "<unknown>" deletion for "b".
    """

    async def process(self) -> None:
        for key, value in _source_data.items():
            await coco.mount(coco.component_subpath(key), _declare_item, key, value)

    async def process_live(self, operator: coco.LiveComponentOperator) -> None:
        await operator.update_full()
        # Delete "b" only if it's in the current source data
        if "b" in _source_data:
            handle = await operator.delete(coco.component_subpath("b"))
            await handle.ready()
        await operator.mark_ready()


@pytest.mark.asyncio
async def test_live_component_incremental_delete_no_stale_tombstone() -> None:
    """After incremental delete, a second run should not produce '<unknown>' deletions."""
    GlobalDictTarget.store.clear()
    _source_data.clear()

    _source_data["a"] = 1
    _source_data["b"] = 2

    async def _main() -> None:
        await coco.mount(
            coco.component_subpath("live"), _IncrementalDeleteNoStaleComponent
        )

    app = coco.App(
        coco.AppConfig(name="test_live_incr_delete_no_stale", environment=coco_env),
        _main,
    )

    # First run: deletes "b" incrementally
    app.update_blocking(live=True)
    assert "a" in GlobalDictTarget.store.data
    assert "b" not in GlobalDictTarget.store.data

    # Second run: "b" no longer in source, should be a clean run
    _source_data.pop("b", None)
    handle = app.update(live=True)
    await handle.result()
    stats = handle.stats()
    assert stats is not None

    # There should be no "<unknown>" component in the stats
    assert "<unknown>" not in stats.by_component, (
        f"Stale tombstone caused '<unknown>' deletion: {stats.by_component}"
    )


class _IncrementalDeleteViaGCLiveComponent:
    """LiveComponent that tests deletion via update_full GC.

    The update_full GC mechanism is the primary way children get cleaned up:
    any children mounted by the previous update_full but NOT mounted by the
    current update_full are garbage collected.
    """

    async def process(self) -> None:
        _declare_source_entries()

    async def process_live(self, operator: coco.LiveComponentOperator) -> None:
        # First full update: adds "a" and "b" from _source_data
        await operator.update_full()
        # Incrementally add "extra"
        handle = await operator.update(
            coco.component_subpath("extra"), _declare_item, "extra_key", 42
        )
        await handle.ready()
        # Second full update: process() still only declares "a" and "b".
        # The incremental "extra" child was not mounted by process(), so it
        # gets GC'd by the second update_full.
        await operator.update_full()
        # mark_ready auto-called on return


def test_live_component_incremental_delete() -> None:
    GlobalDictTarget.store.clear()
    _source_data.clear()

    _source_data["a"] = 1
    _source_data["b"] = 2

    async def _main() -> None:
        await coco.mount(
            coco.component_subpath("live"), _IncrementalDeleteViaGCLiveComponent
        )

    app = coco.App(
        coco.AppConfig(name="test_live_incremental_delete", environment=coco_env),
        _main,
    )
    app.update_blocking(live=True)

    # a and b should remain from both full updates
    assert "a" in GlobalDictTarget.store.data
    assert "b" in GlobalDictTarget.store.data
    # extra_key should have been GC'd by the second update_full
    assert "extra_key" not in GlobalDictTarget.store.data


# State for GC test — controlled by a flag to change behavior between update_full calls
_gc_phase: int = 0


def _declare_gc_entries() -> None:
    """Declares entries based on _gc_phase:
    Phase 0: A, B, C
    Phase 1+: A, B only
    """
    if _gc_phase == 0:
        coco.declare_target_state(GlobalDictTarget.target_state("gc_a", 1))
        coco.declare_target_state(GlobalDictTarget.target_state("gc_b", 2))
        coco.declare_target_state(GlobalDictTarget.target_state("gc_c", 3))
    else:
        coco.declare_target_state(GlobalDictTarget.target_state("gc_a", 1))
        coco.declare_target_state(GlobalDictTarget.target_state("gc_b", 2))


class _GCLiveComponent:
    """LiveComponent that tests GC via two update_full calls."""

    async def process(self) -> None:
        _declare_gc_entries()

    async def process_live(self, operator: coco.LiveComponentOperator) -> None:
        global _gc_phase
        # First full update: A, B, C
        _gc_phase = 0
        await operator.update_full()

        # Incremental add D
        handle = await operator.update(
            coco.component_subpath("d_item"), _declare_item, "gc_d", 4
        )
        await handle.ready()

        # Second full update: only A, B (C removed from process, D was incremental)
        _gc_phase = 1
        await operator.update_full()
        # mark_ready auto-called on return


def test_live_component_update_full_gc() -> None:
    GlobalDictTarget.store.clear()
    _source_data.clear()

    async def _main() -> None:
        await coco.mount(coco.component_subpath("live"), _GCLiveComponent)

    app = coco.App(
        coco.AppConfig(name="test_live_update_full_gc", environment=coco_env),
        _main,
    )
    app.update_blocking(live=True)

    # After second update_full: only A and B should exist. C and D should be GC'd.
    assert "gc_a" in GlobalDictTarget.store.data
    assert "gc_b" in GlobalDictTarget.store.data
    assert "gc_c" not in GlobalDictTarget.store.data
    assert "gc_d" not in GlobalDictTarget.store.data


# ============================================================================
# LiveMapView + mount_each tests
# ============================================================================


class _TestLiveMapView:
    """A simple LiveMapView for testing.

    Yields (key, key) pairs — the value is the same as the key string.
    The per-item function receives the value (a str) as first arg.
    """

    def __init__(self, data: dict[str, int]) -> None:
        self._data = data
        self._watch_fn: (
            Callable[[coco.LiveMapSubscriber[str, str]], Awaitable[None]] | None
        ) = None

    def set_watch_fn(
        self,
        fn: Callable[[coco.LiveMapSubscriber[str, str]], Awaitable[None]],
    ) -> None:
        self._watch_fn = fn

    def __aiter__(self) -> AsyncIterator[tuple[str, str]]:
        return self._aiter_impl()

    async def _aiter_impl(self) -> AsyncIterator[tuple[str, str]]:
        for k in self._data:
            yield (k, k)

    async def watch(self, subscriber: coco.LiveMapSubscriber[str, str]) -> None:
        await subscriber.update_all()
        if self._watch_fn is not None:
            await self._watch_fn(subscriber)
        await subscriber.mark_ready()


_live_source: dict[str, int] = {}


def _declare_live_item(key: str) -> None:
    """Per-item function for LiveMapView tests. Looks up value from _live_source."""
    value = _live_source[key]
    coco.declare_target_state(GlobalDictTarget.target_state(key, value))


def test_mount_each_live_items_view_basic() -> None:
    GlobalDictTarget.store.clear()
    _live_source.clear()
    _live_source.update({"a": 1, "b": 2, "c": 3})

    items = _TestLiveMapView(_live_source)

    async def _main() -> None:
        await coco.mount_each(_declare_live_item, items)  # type: ignore[call-overload]

    app = coco.App(
        coco.AppConfig(name="test_live_items_basic", environment=coco_env),
        _main,
    )
    app.update_blocking(live=True)

    assert GlobalDictTarget.store.data == {
        "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
        "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
    }


def test_mount_each_live_items_view_catch_up_mode() -> None:
    GlobalDictTarget.store.clear()
    _live_source.clear()
    _live_source["x"] = 10

    items = _TestLiveMapView(_live_source)

    async def _main() -> None:
        await coco.mount_each(_declare_live_item, items)  # type: ignore[call-overload]

    app = coco.App(
        coco.AppConfig(name="test_live_items_catch_up", environment=coco_env),
        _main,
    )
    # Catch-up mode: mark_ready terminates watch(), app completes
    app.update_blocking()

    assert GlobalDictTarget.store.data == {
        "x": DictDataWithPrev(data=10, prev=[], prev_may_be_missing=True),
    }


def test_mount_each_live_items_view_incremental_update() -> None:
    GlobalDictTarget.store.clear()
    _live_source.clear()
    _live_source["a"] = 1

    items = _TestLiveMapView(_live_source)

    async def _after_ready(subscriber: coco.LiveMapSubscriber[str, str]) -> None:
        _live_source["new_key"] = 99
        handle = await subscriber.update("new_key", "new_key")
        await handle.ready()

    items.set_watch_fn(_after_ready)

    async def _main() -> None:
        await coco.mount_each(_declare_live_item, items)  # type: ignore[call-overload]

    app = coco.App(
        coco.AppConfig(name="test_live_items_incr_update", environment=coco_env),
        _main,
    )
    app.update_blocking(live=True)

    assert "a" in GlobalDictTarget.store.data
    assert "new_key" in GlobalDictTarget.store.data
    assert GlobalDictTarget.store.data["new_key"].data == 99


def test_mount_each_live_items_view_update_all_rescan() -> None:
    GlobalDictTarget.store.clear()
    _live_source.clear()
    _live_source.update({"a": 1, "b": 2, "c": 3})

    items = _TestLiveMapView(_live_source)

    async def _after_ready(subscriber: coco.LiveMapSubscriber[str, str]) -> None:
        # Mutate backing data, then trigger rescan
        _live_source.clear()
        _live_source.update({"a": 1, "d": 4})
        items._data = _live_source
        await subscriber.update_all()

    items.set_watch_fn(_after_ready)

    async def _main() -> None:
        await coco.mount_each(_declare_live_item, items)  # type: ignore[call-overload]

    app = coco.App(
        coco.AppConfig(name="test_live_items_rescan", environment=coco_env),
        _main,
    )
    app.update_blocking(live=True)

    # After rescan: a and d should exist, b and c should be GC'd
    assert "a" in GlobalDictTarget.store.data
    assert "d" in GlobalDictTarget.store.data
    assert "b" not in GlobalDictTarget.store.data
    assert "c" not in GlobalDictTarget.store.data


def test_mount_each_auto_subpath() -> None:
    GlobalDictTarget.store.clear()
    _live_source.clear()
    _live_source["k1"] = 1

    async def _main() -> None:
        await coco.mount_each(_declare_live_item, [("k1", "k1")])  # type: ignore[call-overload]

    app = coco.App(
        coco.AppConfig(name="test_mount_each_auto_subpath", environment=coco_env),
        _main,
    )
    app.update_blocking()

    assert "k1" in GlobalDictTarget.store.data


def test_mount_each_no_name_raises() -> None:
    """Callables without __name__ require an explicit ComponentSubpath."""

    class _NoName:
        def __call__(self, x: Any) -> None:
            pass

    fn = _NoName()  # callable instance without __name__

    async def _main() -> None:
        await coco.mount_each(fn, [("a", 1)])  # type: ignore[arg-type]

    app = coco.App(
        coco.AppConfig(name="test_mount_each_no_name", environment=coco_env),
        _main,
    )
    with pytest.raises(TypeError, match="requires a ComponentSubpath"):
        app.update_blocking()

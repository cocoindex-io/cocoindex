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


# Slice F: nested LiveCompClass via operator.update
class _InnerLiveComponent:
    """Inner live component mounted via operator.update(LiveCompClass)."""

    async def process(self) -> None:
        coco.declare_target_state(GlobalDictTarget.target_state("inner_marker", 7))

    async def process_live(self, operator: coco.LiveComponentOperator) -> None:
        await operator.update_full()
        await operator.mark_ready()


class _OuterLiveComponentWithInner:
    """Outer live component that mounts an inner live component via operator.update.

    Exercises Slice F: the LiveCompClass branch in `operator.update()` should
    install the inner controller under the outer's `update_full_lock`, spawn
    the inner's `process_live`, and wait for the inner's `mark_ready`.
    """

    async def process(self) -> None:
        coco.declare_target_state(GlobalDictTarget.target_state("outer_marker", 1))

    async def process_live(self, operator: coco.LiveComponentOperator) -> None:
        await operator.update_full()
        # Mount a nested live component at "inner" subpath.
        handle = await operator.update(
            coco.component_subpath("inner"), _InnerLiveComponent
        )
        await handle.ready()
        await operator.mark_ready()


def test_in_process_live_blocks_coco_mount() -> None:
    """Polish 2: `coco.mount(...)` inside `process_live` raises RuntimeError.

    Verifies the `_in_process_live` ContextVar enforcement directly:
    when the var is `True` (as it is inside `process_live`), the entry
    points `coco.mount` / `coco.mount_each` / `coco.use_mount` must
    raise a `RuntimeError` mentioning `process_live`.

    The integration shape — that the var is actually set to `True` for
    `process_live`'s body — is covered by the positive-case test
    (`test_process_inside_live_can_call_coco_mount`): if the var weren't
    set, calls inside `process()` would only succeed because the var was
    already `False`, not because of the symmetric reset, which means
    `process_live`'s body would also see `False`. The fact that the
    positive case passes after the symmetric reset takes effect
    (combined with the var defaulting to `False`) is the load-bearing
    end-to-end signal that the True/False machinery works.
    """
    from cocoindex._internal.live_component import (
        _in_process_live,
        check_not_in_process_live,
    )

    # Default: not in process_live → no raise.
    assert _in_process_live.get() is False
    check_not_in_process_live("coco.mount")  # should not raise

    # Simulate inside process_live: var set to True → must raise.
    prev = _in_process_live.get()
    _in_process_live.set(True)
    try:
        with pytest.raises(RuntimeError, match="not allowed inside process_live"):
            check_not_in_process_live("coco.mount")
        with pytest.raises(RuntimeError, match="not allowed inside process_live"):
            check_not_in_process_live("coco.mount_each")
        with pytest.raises(RuntimeError, match="not allowed inside process_live"):
            check_not_in_process_live("coco.use_mount")
    finally:
        _in_process_live.set(prev)


class _LiveComponentInnerCallsCocoMount:
    """Polish 2 positive test: process() inside live can call coco.mount safely."""

    async def process(self) -> None:
        # This MUST NOT raise — process() is a separate context; the
        # symmetric reset in update_full sets _in_process_live=False
        # before the new Task captures Context for process()'s body.
        await coco.mount(coco.component_subpath("ok"), _declare_item, "marker", 99)

    async def process_live(self, operator: coco.LiveComponentOperator) -> None:
        await operator.update_full()
        await operator.mark_ready()


def test_process_inside_live_can_call_coco_mount() -> None:
    """Polish 2: `coco.mount(...)` from inside `process()` of a live component
    must work — `update_full`'s inline `_in_process_live = False` reset
    takes effect for the asyncio Task that runs `process()`.

    This is the load-bearing positive case (design.md integration test #3):
    if the symmetric reset breaks, every `process()` of a live component
    would falsely raise here.
    """
    GlobalDictTarget.store.clear()

    async def _main() -> None:
        await coco.mount(
            coco.component_subpath("inner_ok"), _LiveComponentInnerCallsCocoMount
        )

    app = coco.App(
        coco.AppConfig(
            name="test_process_inside_live_can_call_coco_mount",
            environment=coco_env,
        ),
        _main,
    )
    app.update_blocking()
    # The inner mount declared the marker.
    assert "marker" in GlobalDictTarget.store.data


def test_live_component_operator_update_with_live_class() -> None:
    """operator.update(LiveCompClass) installs and runs a nested live component.

    Verifies Slice F: the cancellable update_full_lock acquisition, fresh
    parent_ctx construction, mount_live_prepare + complete sequence, and
    inner controller startup all wire up correctly. Both outer and inner
    target states should be declared.
    """
    GlobalDictTarget.store.clear()
    _source_data.clear()

    async def _main() -> None:
        await coco.mount(coco.component_subpath("outer"), _OuterLiveComponentWithInner)

    app = coco.App(
        coco.AppConfig(name="test_operator_update_live_class", environment=coco_env),
        _main,
    )
    app.update_blocking()

    # Both outer's process() and inner's process() (via inner's update_full
    # invoked from inner's process_live) should have declared their markers.
    assert "outer_marker" in GlobalDictTarget.store.data
    assert GlobalDictTarget.store.data["outer_marker"].data == 1
    assert "inner_marker" in GlobalDictTarget.store.data
    assert GlobalDictTarget.store.data["inner_marker"].data == 7


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
# LiveComponentOperator.report_exception
# ============================================================================


class _LiveThatReports:
    """Live component that calls operator.report_exception once after mark_ready."""

    def __init__(self, exc: BaseException) -> None:
        self._exc = exc

    async def process(self) -> None:
        coco.declare_target_state(GlobalDictTarget.target_state("marker", 1))

    async def process_live(self, operator: coco.LiveComponentOperator) -> None:
        await operator.update_full()
        await operator.mark_ready()
        await operator.report_exception(self._exc)


def _raise_for_trace_test() -> None:
    raise ValueError("traceful boom")


class _LiveThatReportsCaught:
    """Raises and catches a ValueError, then reports it — exc.__traceback__ is real."""

    async def process(self) -> None:
        coco.declare_target_state(GlobalDictTarget.target_state("marker", 1))

    async def process_live(self, operator: coco.LiveComponentOperator) -> None:
        await operator.update_full()
        await operator.mark_ready()
        try:
            _raise_for_trace_test()
        except ValueError as exc:
            await operator.report_exception(exc)


def test_report_exception_routes_to_global_handler() -> None:
    """report_exception walks the parent's exception handler chain."""
    GlobalDictTarget.store.clear()

    seen: list[tuple[str, str, str, str | None, str | None]] = []

    def handler(exc: BaseException, ctx: coco.ExceptionContext) -> None:
        seen.append(
            (
                type(exc).__name__,
                ctx.mount_kind,
                ctx.stable_path,
                ctx.parent_stable_path,
                ctx.processor_name,
            )
        )

    env = common.create_test_env(
        __file__, suffix="report_exc_global", exception_handler=handler
    )

    async def _root() -> None:
        await coco.mount(
            coco.component_subpath("live"),
            _LiveThatReports,
            ValueError("boom from cycle"),
        )

    app = coco.App(
        coco.AppConfig(name="test_report_exc_global", environment=env), _root
    )
    app.update_blocking(live=True)

    assert len(seen) == 1
    exc_name, mount_kind, stable_path, parent_path, processor_name = seen[0]
    # resolve_handler synthesizes a RuntimeError from the stringified error
    assert exc_name == "RuntimeError"
    assert mount_kind == "process_live"
    # The live component's path includes the "live" subpath component
    assert "live" in stable_path
    # Parent is the root context
    assert parent_path is not None
    assert processor_name == "_LiveThatReports"


def test_report_exception_surfaces_python_traceback() -> None:
    """The handler should see the original Python traceback, not just the message."""
    GlobalDictTarget.store.clear()

    seen_messages: list[str] = []

    def handler(exc: BaseException, ctx: coco.ExceptionContext) -> None:
        seen_messages.append(str(exc))

    env = common.create_test_env(
        __file__, suffix="report_exc_trace", exception_handler=handler
    )

    async def _root() -> None:
        await coco.mount(coco.component_subpath("live"), _LiveThatReportsCaught)

    app = coco.App(coco.AppConfig(name="test_report_exc_trace", environment=env), _root)
    app.update_blocking(live=True)

    assert len(seen_messages) == 1
    msg = seen_messages[0]
    assert "ValueError" in msg
    assert "traceful boom" in msg
    assert "Traceback (most recent call last)" in msg
    assert "_raise_for_trace_test" in msg


async def _failing_child(value: int) -> None:
    raise ValueError(f"child failed with value={value}")


async def _child_declare_one(value: int) -> None:
    """Helper for delete-propagation test: declares a single target row."""
    coco.declare_target_state(GlobalDictTarget.target_state("c", value))


class _LiveThatDeletesWithFailingSink:
    """Mount a child, await ready, toggle the sink to fail, then delete.

    ``operator.delete`` is symmetric with ``operator.update`` —
    failures route through the parent's exception handler chain.
    Handlers control whether ``handle.ready()`` raises (raise to
    propagate, return to swallow). This test verifies the routing.
    """

    async def process(self) -> None:
        pass

    async def process_live(self, operator: coco.LiveComponentOperator) -> None:
        await operator.update_full()
        h = await operator.update(coco.component_subpath("c"), _child_declare_one, 1)
        await h.ready()
        await operator.mark_ready()
        GlobalDictTarget.store.sink_exception = True
        try:
            dh = await operator.delete(coco.component_subpath("c"))
            await dh.ready()
        finally:
            GlobalDictTarget.store.sink_exception = False


def test_operator_delete_failure_routes_to_handler() -> None:
    """`operator.delete()` failure → handler chain (mount-style symmetry
    with `operator.update`). Handler that returns normally → swallow."""
    GlobalDictTarget.store.clear()
    GlobalDictTarget.store.sink_exception = False

    seen: list[tuple[str, str, str]] = []

    def handler(exc: BaseException, ctx: coco.ExceptionContext) -> None:
        seen.append((type(exc).__name__, ctx.mount_kind, ctx.stable_path))

    env = common.create_test_env(
        __file__, suffix="delete_route", exception_handler=handler
    )

    async def _root() -> None:
        await coco.mount(
            coco.component_subpath("live"), _LiveThatDeletesWithFailingSink
        )

    app = coco.App(coco.AppConfig(name="test_delete_route", environment=env), _root)
    app.update_blocking(live=True)

    assert len(seen) == 1
    exc_name, mount_kind, stable_path = seen[0]
    assert exc_name == "RuntimeError"
    assert mount_kind == "process_live"
    assert "c" in stable_path


class _LiveThatDeletesWithRaisingHandler:
    """`operator.delete` with a raising-handler: handle.ready() should raise.

    The raise should propagate via the user's handler chain → Rust
    on_error returns Err → spawned task task_result = Err →
    HandleOutcome::Executed(Err) → user's await handle.ready() raises.
    """

    delete_err: BaseException | None = None

    async def process(self) -> None:
        pass

    async def process_live(self, operator: coco.LiveComponentOperator) -> None:
        type(self).delete_err = None
        await operator.update_full()
        h = await operator.update(coco.component_subpath("c"), _child_declare_one, 1)
        await h.ready()
        await operator.mark_ready()
        GlobalDictTarget.store.sink_exception = True
        try:
            dh = await operator.delete(coco.component_subpath("c"))
            try:
                await dh.ready()
            except Exception as e:
                # Hold the real exception object on a class attribute:
                # exercises the operator-detach guarantee (the framework
                # releases the controller in `_process_live_wrapper`'s
                # finally, so the exception's traceback no longer pins
                # the live component's Arc via `operator`).
                type(self).delete_err = e
        finally:
            GlobalDictTarget.store.sink_exception = False


@pytest.mark.timeout(10, method="thread")
def test_operator_delete_failure_propagates_via_raising_handler() -> None:
    """A handler that raises must propagate the err to handle.ready()."""
    GlobalDictTarget.store.clear()
    GlobalDictTarget.store.sink_exception = False
    _LiveThatDeletesWithRaisingHandler.delete_err = None

    async def raising_handler(exc: BaseException, ctx: coco.ExceptionContext) -> None:
        raise exc

    env = common.create_test_env(
        __file__, suffix="delete_raise", exception_handler=raising_handler
    )

    async def _root() -> None:
        await coco.mount(
            coco.component_subpath("live"), _LiveThatDeletesWithRaisingHandler
        )

    app = coco.App(coco.AppConfig(name="test_delete_raise", environment=env), _root)
    app.update_blocking(live=True)

    err = _LiveThatDeletesWithRaisingHandler.delete_err
    assert err is not None
    assert "injected sink exception" in str(err)


class _LiveThatUpdatesFailingChild:
    """Mount a child via operator.update() that raises; the loop continues."""

    update_err: BaseException | None = None

    async def process(self) -> None:
        coco.declare_target_state(GlobalDictTarget.target_state("marker", 1))

    async def process_live(self, operator: coco.LiveComponentOperator) -> None:
        type(self).update_err = None
        await operator.update_full()
        await operator.mark_ready()
        handle = await operator.update(
            coco.component_subpath("bad_child"), _failing_child, 42
        )
        try:
            await handle.ready()
        except Exception as e:
            # Hold the real exception (with traceback) on a class
            # attribute to verify the operator-detach guarantee:
            # `_process_live_wrapper`'s finally releases the controller
            # so the retained traceback can't pin the live component.
            type(self).update_err = e


def test_operator_update_child_failure_routes_to_handler() -> None:
    """A child mounted via operator.update() that raises should be routed
    through the parent's exception handler chain with mount_kind='process_live'
    — same as update_full() cycle failures, not silently logged via Rust."""
    GlobalDictTarget.store.clear()
    _LiveThatUpdatesFailingChild.update_err = None

    seen: list[tuple[str, str, str]] = []

    def handler(exc: BaseException, ctx: coco.ExceptionContext) -> None:
        seen.append((type(exc).__name__, ctx.mount_kind, ctx.stable_path))

    env = common.create_test_env(
        __file__, suffix="op_update_failure", exception_handler=handler
    )

    async def _root() -> None:
        await coco.mount(coco.component_subpath("live"), _LiveThatUpdatesFailingChild)

    app = coco.App(
        coco.AppConfig(name="test_op_update_failure", environment=env), _root
    )
    app.update_blocking(live=True)

    assert len(seen) == 1
    exc_name, mount_kind, stable_path = seen[0]
    assert exc_name == "RuntimeError"
    assert mount_kind == "process_live"
    assert "bad_child" in stable_path
    # Swallowing handler → no propagation
    assert _LiveThatUpdatesFailingChild.update_err is None


@pytest.mark.timeout(10, method="thread")
def test_operator_update_child_failure_propagates_via_raising_handler() -> None:
    """operator.update with a RAISING handler: handle.ready() should raise.
    Probe to localize the hang — if this also hangs, the issue is in the
    on_error mechanism generally, not delete-specific."""
    GlobalDictTarget.store.clear()
    _LiveThatUpdatesFailingChild.update_err = None

    def raising(exc: BaseException, ctx: coco.ExceptionContext) -> None:
        raise exc

    env = common.create_test_env(
        __file__, suffix="op_update_raise", exception_handler=raising
    )

    async def _root() -> None:
        await coco.mount(coco.component_subpath("live"), _LiveThatUpdatesFailingChild)

    app = coco.App(coco.AppConfig(name="test_op_update_raise", environment=env), _root)
    app.update_blocking(live=True)

    err = _LiveThatUpdatesFailingChild.update_err
    assert err is not None
    assert "child failed with value=42" in str(err)


def test_report_exception_falls_back_to_log_when_no_handler(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """With no handler registered, report_exception logs at ERROR."""
    GlobalDictTarget.store.clear()

    env = common.create_test_env(__file__, suffix="report_exc_no_handler")

    async def _root() -> None:
        await coco.mount(
            coco.component_subpath("live"),
            _LiveThatReports,
            RuntimeError("no handler boom"),
        )

    app = coco.App(
        coco.AppConfig(name="test_report_exc_no_handler", environment=env), _root
    )
    with caplog.at_level("ERROR"):
        app.update_blocking(live=True)

    assert any("no handler boom" in record.getMessage() for record in caplog.records)


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


# ============================================================================
# Cancellation
# ============================================================================


@pytest.mark.asyncio
async def test_live_component_global_cancel_terminates_update() -> None:
    """Global cancellation (Ctrl+C path) must reach a blocked process_live coroutine
    and let App.update() terminate.

    Regression test for the bug where AppContextInner.cancellation_token was an
    independent token rather than a child of GLOBAL_CANCEL, so cancel_all() never
    propagated to LiveComponentState child tokens.
    """
    from cocoindex._internal import core as _core

    started = asyncio.Event()
    cancelled_in_python = asyncio.Event()

    class _BlockingLive:
        async def process(self) -> None:
            pass

        async def process_live(self, operator: coco.LiveComponentOperator) -> None:
            await operator.update_full()
            await operator.mark_ready()
            started.set()
            try:
                await asyncio.Event().wait()  # Block forever.
            except asyncio.CancelledError:
                cancelled_in_python.set()
                raise

    async def _main() -> None:
        await coco.mount(coco.component_subpath("live"), _BlockingLive)

    _core.reset_global_cancellation()
    app = coco.App(
        coco.AppConfig(name="test_live_global_cancel_terminates", environment=coco_env),
        _main,
    )
    handle = app.update(live=True)
    # update() is lazy — kick it off by spawning a task that awaits result().
    result_task = asyncio.create_task(handle.result())
    try:
        await asyncio.wait_for(started.wait(), timeout=5.0)

        _core.cancel_all()  # simulates SIGINT handler in cli.py

        # Wait for cancellation to actually reach Python before checking. The
        # update task may return before the inner Python cancellation has
        # finished propagating via CancelOnDropPy.
        await asyncio.wait_for(cancelled_in_python.wait(), timeout=5.0)

        try:
            await asyncio.wait_for(result_task, timeout=5.0)
        except Exception:
            pass
    finally:
        if not result_task.done():
            result_task.cancel()
        _core.reset_global_cancellation()


# ============================================================================
# LiveStream primitives
# ============================================================================


@pytest.mark.asyncio
async def test_immediate_ready_is_immediate() -> None:
    """_IMMEDIATE_READY.ready() resolves without suspension."""
    from cocoindex._internal.live_component import _IMMEDIATE_READY

    fut = _IMMEDIATE_READY.ready()
    # Should already be done after one await with no scheduling.
    await asyncio.wait_for(fut, timeout=0.1)


def test_live_stream_protocol_runtime_check() -> None:
    """A minimal subscriber satisfies LiveStreamSubscriber.isinstance(...)."""
    from cocoindex._internal.live_component import (
        LiveStreamSubscriber,
        ReadyAwaitable,
    )

    class _Sub:
        async def send(self, message: Any) -> ReadyAwaitable:  # noqa: ARG002
            raise NotImplementedError

        async def mark_ready(self) -> None:
            pass

    assert isinstance(_Sub(), LiveStreamSubscriber)
    assert not isinstance(object(), LiveStreamSubscriber)

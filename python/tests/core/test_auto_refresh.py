"""Tests for ``coco.auto_refresh`` and ``LiveComponentOperator.report_exception``."""

from __future__ import annotations

import asyncio
import datetime

import pytest

import cocoindex as coco
from cocoindex._internal import core as _core

from tests import common
from tests.common.target_states import DictDataWithPrev, GlobalDictTarget

coco_env = common.create_test_env(__file__)


# ============================================================================
# auto_refresh — catch-up mode
# ============================================================================


def test_auto_refresh_catch_up_runs_once() -> None:
    """Catch-up mode: process_fn runs exactly once and the app completes."""
    GlobalDictTarget.store.clear()

    counter = {"calls": 0}

    async def fn(value: int) -> None:
        counter["calls"] += 1
        coco.declare_target_state(GlobalDictTarget.target_state("k", value))

    AutoRefresh = coco.auto_refresh(fn, interval=datetime.timedelta(milliseconds=10))

    async def _main() -> None:
        await coco.mount(coco.component_subpath("ar"), AutoRefresh, 7)

    app = coco.App(
        coco.AppConfig(name="test_auto_refresh_catch_up", environment=coco_env),
        _main,
    )
    app.update_blocking()

    assert counter["calls"] == 1
    assert GlobalDictTarget.store.data == {
        "k": DictDataWithPrev(data=7, prev=[], prev_may_be_missing=True),
    }


def test_auto_refresh_forwards_args_and_kwargs() -> None:
    """__init__'s args/kwargs are forwarded to process_fn on each call."""
    GlobalDictTarget.store.clear()

    received: list[tuple[tuple[int, ...], dict[str, int]]] = []

    async def fn(*args: int, **kwargs: int) -> None:
        received.append((args, dict(kwargs)))
        coco.declare_target_state(GlobalDictTarget.target_state("k", 0))

    AutoRefresh = coco.auto_refresh(fn, interval=datetime.timedelta(milliseconds=10))

    async def _main() -> None:
        await coco.mount(coco.component_subpath("ar"), AutoRefresh, 1, 2, x=3)

    app = coco.App(
        coco.AppConfig(name="test_auto_refresh_forward_args", environment=coco_env),
        _main,
    )
    app.update_blocking()

    assert received == [((1, 2), {"x": 3})]


# ============================================================================
# auto_refresh — live mode (periodic loop)
# ============================================================================


@pytest.mark.asyncio
async def test_auto_refresh_live_runs_periodically() -> None:
    """Live mode: process_fn is called multiple times across the interval."""
    GlobalDictTarget.store.clear()
    _core.reset_global_cancellation()

    counter = {"calls": 0}
    cycle_event = asyncio.Event()

    async def fn() -> None:
        counter["calls"] += 1
        if counter["calls"] >= 3:
            cycle_event.set()
        coco.declare_target_state(GlobalDictTarget.target_state("k", counter["calls"]))

    AutoRefresh = coco.auto_refresh(fn, interval=datetime.timedelta(milliseconds=20))

    async def _main() -> None:
        await coco.mount(coco.component_subpath("ar"), AutoRefresh)

    app = coco.App(
        coco.AppConfig(name="test_auto_refresh_live_periodic", environment=coco_env),
        _main,
    )
    handle = app.update(live=True)
    result_task = asyncio.create_task(handle.result())
    try:
        # Wait until at least 3 cycles have happened
        await asyncio.wait_for(cycle_event.wait(), timeout=5.0)
        assert counter["calls"] >= 3

        _core.cancel_all()
        try:
            await asyncio.wait_for(result_task, timeout=5.0)
        except Exception:
            pass
    finally:
        if not result_task.done():
            result_task.cancel()
        _core.reset_global_cancellation()


# ============================================================================
# auto_refresh + exception routing
# ============================================================================


@pytest.mark.asyncio
async def test_auto_refresh_cycle_exception_reported_and_loop_continues() -> None:
    """A cycle that raises is auto-routed through the parent's exception handler chain
    (via ``update_full``'s internal on_error wiring), and the loop keeps going."""
    GlobalDictTarget.store.clear()
    _core.reset_global_cancellation()

    counter = {"calls": 0}
    reports: list[str] = []
    after_failure_event = asyncio.Event()

    def handler(exc: BaseException, ctx: coco.ExceptionContext) -> None:
        reports.append(f"{ctx.mount_kind}:{type(exc).__name__}")

    env = common.create_test_env(
        __file__, suffix="cycle_err", exception_handler=handler
    )

    async def fn() -> None:
        counter["calls"] += 1
        # 2nd cycle (after the initial one) fails. The 3rd cycle should still run.
        if counter["calls"] == 2:
            raise ValueError("cycle 2 fails")
        if counter["calls"] >= 3:
            after_failure_event.set()
        coco.declare_target_state(GlobalDictTarget.target_state("k", counter["calls"]))

    AutoRefresh = coco.auto_refresh(fn, interval=datetime.timedelta(milliseconds=20))

    async def _main() -> None:
        await coco.mount(coco.component_subpath("ar"), AutoRefresh)

    app = coco.App(
        coco.AppConfig(name="test_auto_refresh_cycle_err", environment=env),
        _main,
    )
    handle = app.update(live=True)
    result_task = asyncio.create_task(handle.result())
    try:
        await asyncio.wait_for(after_failure_event.wait(), timeout=5.0)
        assert counter["calls"] >= 3
        # The 2nd cycle's failure must have been routed via the handler chain
        # with mount_kind="process_live" (set by update_full's on_error resolver).
        assert any(r.startswith("process_live:") for r in reports), reports

        _core.cancel_all()
        try:
            await asyncio.wait_for(result_task, timeout=5.0)
        except Exception:
            pass
    finally:
        if not result_task.done():
            result_task.cancel()
        _core.reset_global_cancellation()

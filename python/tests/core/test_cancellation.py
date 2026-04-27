"""Tests for the cancellation pipeline (see specs/core/cancellation.md).

Verifies that `_core.cancel_all()` (the same call the CLI's SIGINT handler
makes) propagates from the global token through the AppContext token, into
the per-component spawned tasks, and ultimately reaches Python coroutines
via CancelOnDropPy.

The live-component variant lives alongside other live-component tests in
test_live_component.py.
"""

from __future__ import annotations

import asyncio

import pytest

import cocoindex as coco

from tests import common

coco_env = common.create_test_env(__file__)


@pytest.mark.asyncio
async def test_non_live_global_cancel_terminates_update() -> None:
    """Global cancellation must reach a non-live component's Python coroutine.

    Regression test for the case where Component::run / run_in_background
    spawned detached tokio tasks that did not watch any cancellation token,
    so dropping the outer App::update future left the spawned task running
    and the Python `process()` coroutine never received CancelledError.
    """
    from cocoindex._internal import core as _core

    started = asyncio.Event()
    cancelled_in_python = asyncio.Event()

    async def _blocking_main() -> None:
        started.set()
        try:
            await asyncio.Event().wait()  # block forever
        except asyncio.CancelledError:
            cancelled_in_python.set()
            raise

    _core.reset_global_cancellation()
    app = coco.App(
        coco.AppConfig(
            name="test_non_live_global_cancel_terminates", environment=coco_env
        ),
        _blocking_main,
    )
    handle = app.update()
    result_task = asyncio.create_task(handle.result())
    try:
        await asyncio.wait_for(started.wait(), timeout=5.0)

        _core.cancel_all()  # simulates SIGINT handler in cli.py

        # Wait for cancellation to actually reach Python. The outer App::update
        # task may return Err immediately when the app token fires, but the
        # inner spawned task that drops the work future and triggers
        # CancelOnDropPy runs async — we need to wait for that propagation.
        await asyncio.wait_for(cancelled_in_python.wait(), timeout=5.0)

        # And the update task itself should also terminate quickly.
        try:
            await asyncio.wait_for(result_task, timeout=5.0)
        except Exception:
            pass
    finally:
        if not result_task.done():
            result_task.cancel()
        _core.reset_global_cancellation()


@pytest.mark.asyncio
async def test_app_drop_interrupts_in_flight_update() -> None:
    """App.drop() must interrupt a concurrent update.

    The app token is shared between update and drop_app. drop_app cancels
    it, which fires the cancel arm in App::update and the per-component
    spawned tasks, propagating CancelledError into Python.
    """
    from cocoindex._internal import core as _core

    started = asyncio.Event()
    cancelled_in_python = asyncio.Event()

    async def _blocking_main() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled_in_python.set()
            raise

    _core.reset_global_cancellation()
    app = coco.App(
        coco.AppConfig(name="test_app_drop_interrupts_update", environment=coco_env),
        _blocking_main,
    )
    handle = app.update()
    result_task = asyncio.create_task(handle.result())
    try:
        await asyncio.wait_for(started.wait(), timeout=5.0)

        # drop_app cancels the app token, which interrupts the running update.
        # Run drop concurrently — it should complete after the update terminates.
        await asyncio.wait_for(app.drop(), timeout=5.0)

        try:
            await asyncio.wait_for(result_task, timeout=5.0)
        except Exception:
            pass

        assert cancelled_in_python.is_set(), (
            "process coroutine never received CancelledError — "
            "App.drop did not interrupt the in-flight update"
        )
    finally:
        if not result_task.done():
            result_task.cancel()
        _core.reset_global_cancellation()

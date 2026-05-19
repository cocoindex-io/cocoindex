"""Tests for App.drop() method."""

import pytest

import cocoindex as coco
import cocoindex.inspect as coco_inspect

from typing import Any

from tests import common
from tests.common.target_states import DictsTarget, DictDataWithPrev

coco_env = common.create_test_env(__file__)

_source_data: dict[str, dict[str, Any]] = {}


async def _declare_dicts() -> None:
    """Create dict target states for testing."""
    with coco.component_subpath("dict"):
        for name, data in _source_data.items():
            single_dict_provider = await coco.use_mount(
                coco.component_subpath(name),
                DictsTarget.declare_dict_target,
                name,
            )
            for key, value in data.items():
                coco.declare_target_state(single_dict_provider.target_state(key, value))


def test_drop_blocking() -> None:
    """Test that drop_blocking() reverts target states and clears the database."""
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        coco.AppConfig(name="test_drop_blocking", environment=coco_env),
        _declare_dicts,
    )

    # Run app to create target states
    _source_data["D1"] = {"a": 1, "b": 2}
    app.update_blocking()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
    }

    # Drop the app
    app.drop_blocking()

    # Verify target states were reverted and database is cleared
    assert DictsTarget.store.data == {}
    assert coco_inspect.list_stable_paths_sync(app) == []


@pytest.mark.asyncio
async def test_drop_reverts_target_states() -> None:
    """Test that drop() reverts all target states created by the app."""
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        coco.AppConfig(name="test_drop_reverts_target_states", environment=coco_env),
        _declare_dicts,
    )

    # Run app to create target states
    _source_data["D1"] = {"a": 1, "b": 2}
    _source_data["D2"] = {"c": 3}
    await app.update()

    # Verify target states were created
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
    }

    # Drop the app
    await app.drop()

    # Verify target states were reverted
    assert DictsTarget.store.data == {}

    # Verify database is cleared
    assert await coco_inspect.list_stable_paths(app) == []


@pytest.mark.asyncio
async def test_drop_clears_database() -> None:
    """Test that drop() clears the app's database."""
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        coco.AppConfig(name="test_drop_clears_database", environment=coco_env),
        _declare_dicts,
    )

    # Run app
    _source_data["D1"] = {"a": 1}
    await app.update()

    # Verify app has state
    paths_before = await coco_inspect.list_stable_paths(app)
    assert len(paths_before) > 0

    # Drop the app
    await app.drop()

    # Verify database is cleared
    paths_after = await coco_inspect.list_stable_paths(app)
    assert paths_after == []


@pytest.mark.asyncio
async def test_drop_allows_rerun() -> None:
    """Test that an app can be run again after being dropped."""
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        coco.AppConfig(name="test_drop_allows_rerun", environment=coco_env),
        _declare_dicts,
    )

    # First run
    _source_data["D1"] = {"a": 1}
    await app.update()
    assert DictsTarget.store.data == {
        "D1": {"a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True)},
    }

    # Drop
    await app.drop()
    assert DictsTarget.store.data == {}

    # Run again with different data
    _source_data.clear()
    _source_data["D2"] = {"b": 2}
    await app.update()
    assert DictsTarget.store.data == {
        "D2": {"b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True)},
    }


@pytest.mark.asyncio
async def test_drop_empty_app() -> None:
    """Test that drop() works on an app that hasn't been run yet."""
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        coco.AppConfig(name="test_drop_empty_app", environment=coco_env),
        _declare_dicts,
    )

    # Drop without running - should not error
    await app.drop()

    # Verify no paths exist
    assert await coco_inspect.list_stable_paths(app) == []


# ============================================================================
# Slice D: drop_app live-component drain integration
# ============================================================================


class _CatchUpLiveComponent:
    """Catch-up live component: process is a no-op, then process_live exits via mark_ready.

    Empty `process()` is fine — declares no target states.
    """

    async def process(self) -> None:
        pass

    async def process_live(self, operator: coco.LiveComponentOperator) -> None:
        await operator.update_full()
        await operator.mark_ready()


def test_drop_with_live_component_in_registry() -> None:
    """Slice D: after a live-mode update mounts a live component, calling
    drop_app should walk the live-components registry and drain each (with
    30s per-component timeout). For a quiesced live component (catch-up
    mode that mark_ready'd cleanly), the drain is a no-op and drop_app
    completes promptly without timing out.
    """
    DictsTarget.store.clear()
    _source_data.clear()

    async def _main() -> None:
        await coco.mount(coco.component_subpath("live"), _CatchUpLiveComponent)

    app = coco.App(
        coco.AppConfig(name="test_drop_live_component_drain", environment=coco_env),
        _main,
    )
    # Run in catch-up mode (live=False is the default; the component's own
    # mark_ready in non-live mode triggers process_live to suspend then be
    # dropped via the controller's biased select).
    app.update_blocking()

    # The drain path must not hang here — registry walk + 30s-bounded drain.
    app.drop_blocking()

    # Verify state is fully cleaned up after drop.
    assert DictsTarget.store.data == {}
    assert coco_inspect.list_stable_paths_sync(app) == []


def test_drop_failure_preserves_tracking_for_retry() -> None:
    """When a delete fails during `app.drop()`, the tracking record must
    survive so a follow-up drop (or next update) can retry the cleanup.

    A failed sink call short-circuits `submit()` BEFORE
    `cleanup_tombstone()`, so the tombstone/tracking record stays in the
    DB. The next `app.drop_blocking()` (with the sink healthy) finds
    the surviving records and cleans up — no leak.

    Note: today this test does NOT assert `app.drop_blocking()` raises
    on failure, because failed sub-component deletes happen via the GC
    sweep which doesn't propagate err to the root. Surfacing that to
    `app.drop()` as a raise is a follow-up. The preservation contract
    (this test) is what guarantees no data leaks regardless.
    """
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_drop_failure_preserves_tracking", environment=coco_env
        ),
        _declare_dicts,
    )

    _source_data["D1"] = {"a": 1}
    app.update_blocking()
    assert "D1" in DictsTarget.store.data
    paths_before_failed_drop = coco_inspect.list_stable_paths_sync(app)
    assert paths_before_failed_drop, (
        "tracking records should exist after the initial update"
    )

    # First drop attempt with a failing sink. Whether or not it raises,
    # the tracking record for the failed delete MUST survive.
    DictsTarget.store.sink_exception = True
    try:
        try:
            app.drop_blocking()
        except Exception:
            pass  # tolerate either propagation outcome (see note above)
    finally:
        DictsTarget.store.sink_exception = False

    # Tracking record must survive a failed delete — otherwise the
    # target state would be leaked (no one to clean it up later).
    paths_after_failed_drop = coco_inspect.list_stable_paths_sync(app)
    assert paths_after_failed_drop, (
        "tracking records must survive a failed drop so retry can find them; "
        f"got empty list — target state would leak"
    )

    # Healthy retry cleans up properly.
    app.drop_blocking()
    assert DictsTarget.store.data == {}
    assert coco_inspect.list_stable_paths_sync(app) == []

"""Tests for App.drop() and App.drop_async() methods."""

import pytest

import cocoindex as coco
import cocoindex.asyncio as coco_aio
import cocoindex.inspect as coco_inspect

from typing import Any

from tests import common
from tests.common.target_states import DictsTarget, DictDataWithPrev

coco_env = common.create_test_env(__file__)

_source_data: dict[str, dict[str, Any]] = {}


def _declare_dicts(scope: coco.Scope) -> None:
    """Create dict target states for testing."""
    for name, data in _source_data.items():
        single_dict_provider = coco.mount_run(
            DictsTarget.declare_dict_target,
            scope / "dict" / name,
            name,
        ).result()
        for key, value in data.items():
            coco.declare_target_state(
                scope, single_dict_provider.target_state(key, value)
            )


# === Sync Drop Tests ===


def test_drop_reverts_target_states() -> None:
    """Test that drop() reverts all target states created by the app."""
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        _declare_dicts,
        coco.AppConfig(name="test_drop_reverts_target_states", environment=coco_env),
    )

    # Run app to create target states
    _source_data["D1"] = {"a": 1, "b": 2}
    _source_data["D2"] = {"c": 3}
    app.update()

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
    assert coco_inspect.list_stable_paths_sync(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "dict",
        coco.ROOT_PATH / "dict" / "D1",
        coco.ROOT_PATH / "dict" / "D2",
    ]

    # Drop the app
    app.drop()

    # Verify target states were reverted (dicts deleted)
    assert DictsTarget.store.data == {}

    # Verify database is cleared (no stable paths)
    assert coco_inspect.list_stable_paths_sync(app) == []


def test_drop_clears_database() -> None:
    """Test that drop() clears the app's database."""
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        _declare_dicts,
        coco.AppConfig(name="test_drop_clears_database", environment=coco_env),
    )

    # Run app
    _source_data["D1"] = {"a": 1}
    app.update()

    # Verify app has state
    paths_before = coco_inspect.list_stable_paths_sync(app)
    assert len(paths_before) > 0

    # Drop the app
    app.drop()

    # Verify database is cleared
    paths_after = coco_inspect.list_stable_paths_sync(app)
    assert paths_after == []


def test_drop_allows_rerun() -> None:
    """Test that an app can be run again after being dropped."""
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        _declare_dicts,
        coco.AppConfig(name="test_drop_allows_rerun", environment=coco_env),
    )

    # First run
    _source_data["D1"] = {"a": 1}
    app.update()
    assert DictsTarget.store.data == {
        "D1": {"a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True)},
    }

    # Drop
    app.drop()
    assert DictsTarget.store.data == {}

    # Run again with different data
    _source_data.clear()
    _source_data["D2"] = {"b": 2}
    app.update()
    assert DictsTarget.store.data == {
        "D2": {"b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True)},
    }


def test_drop_empty_app() -> None:
    """Test that drop() works on an app that hasn't been run yet."""
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        _declare_dicts,
        coco.AppConfig(name="test_drop_empty_app", environment=coco_env),
    )

    # Drop without running - should not error
    app.drop()

    # Verify no paths exist
    assert coco_inspect.list_stable_paths_sync(app) == []


# === Async Drop Tests ===


@pytest.mark.asyncio
async def test_drop_async_reverts_target_states() -> None:
    """Test that drop_async() reverts all target states created by the app."""
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco_aio.App(
        _declare_dicts,
        coco.AppConfig(
            name="test_drop_async_reverts_target_states", environment=coco_env
        ),
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
async def test_drop_async_allows_rerun() -> None:
    """Test that an async app can be run again after being dropped."""
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco_aio.App(
        _declare_dicts,
        coco.AppConfig(name="test_drop_async_allows_rerun", environment=coco_env),
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
async def test_drop_async_empty_app() -> None:
    """Test that drop_async() works on an app that hasn't been run yet."""
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco_aio.App(
        _declare_dicts,
        coco.AppConfig(name="test_drop_async_empty_app", environment=coco_env),
    )

    # Drop without running - should not error
    await app.drop()

    # Verify no paths exist
    assert await coco_inspect.list_stable_paths(app) == []

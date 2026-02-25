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

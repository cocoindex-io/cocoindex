"""Tests for component_selector parameter on App.update()."""

from __future__ import annotations

import pytest

import cocoindex as coco
from cocoindex._internal import core
from cocoindex._internal.component_ctx import get_component_selector
from cocoindex._internal.stable_path import (
    build_selector_path,
    stable_path_to_selector,
)

from tests import common
from tests.common.target_states import GlobalDictTarget

_SHARED_ENV = common.create_test_env(__file__)


def _make_env(test_name: str) -> coco.Environment:
    """Create a test-isolated environment."""
    return common.create_test_env(__file__, suffix=test_name)


@coco.fn
async def process_item(value: str) -> None:
    key = stable_path_to_selector(coco.get_component_context()._core_path)
    coco.declare_target_state(GlobalDictTarget.target_state(key, value))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sel(*parts: coco.StableKey) -> core.StablePath:
    """Shorthand to build a selector ``core.StablePath``."""
    return build_selector_path(*parts)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_component_selector_basic(request: pytest.FixtureRequest) -> None:
    """All components are always mounted; selector identifies which items may
    have changed. Unselected items whose inputs haven't changed are preserved."""
    GlobalDictTarget.store.clear()
    env = _make_env(request.node.name)

    items = {"a": "value_a", "b": "value_b", "c": "value_c"}

    @coco.fn
    async def app_main() -> None:
        await coco.mount_each(
            coco.component_subpath("process"),
            process_item,
            items.items(),
        )

    app = coco.App(
        coco.AppConfig(name="test_selector_basic", environment=env),
        app_main,
    )

    # First run without selector — all items processed.
    await app.update()
    assert set(GlobalDictTarget.store.data.keys()) == {
        "process/a",
        "process/b",
        "process/c",
    }

    # Change only the selected item's input.
    items["a"] = "new_value_a"

    # Second run with selector.
    await app.update(component_selector=[_sel(coco.Symbol("process"), "a")])

    # Selected item updated; unselected items preserved.
    assert GlobalDictTarget.store.data["process/a"].data == "new_value_a"
    assert GlobalDictTarget.store.data["process/b"].data == "value_b"
    assert GlobalDictTarget.store.data["process/c"].data == "value_c"


@pytest.mark.asyncio
async def test_component_selector_multiple_patterns(
    request: pytest.FixtureRequest,
) -> None:
    """Multiple selector paths match multiple items."""
    GlobalDictTarget.store.clear()
    env = _make_env(request.node.name)

    items = {"a": "va", "b": "vb", "c": "vc", "d": "vd"}

    @coco.fn
    async def app_main() -> None:
        await coco.mount_each(
            coco.component_subpath("proc"),
            process_item,
            items.items(),
        )

    app = coco.App(
        coco.AppConfig(name="test_selector_multi", environment=env),
        app_main,
    )

    # First run without selector.
    await app.update()

    # Change selected items.
    items["a"] = "va_new"
    items["c"] = "vc_new"

    await app.update(
        component_selector=[
            _sel(coco.Symbol("proc"), "a"),
            _sel(coco.Symbol("proc"), "c"),
        ]
    )

    assert GlobalDictTarget.store.data["proc/a"].data == "va_new"
    assert GlobalDictTarget.store.data["proc/b"].data == "vb"
    assert GlobalDictTarget.store.data["proc/c"].data == "vc_new"
    assert GlobalDictTarget.store.data["proc/d"].data == "vd"


@pytest.mark.asyncio
async def test_component_selector_glob(request: pytest.FixtureRequest) -> None:
    """Glob patterns match items by extension."""
    GlobalDictTarget.store.clear()
    env = _make_env(request.node.name)

    items = {"doc.md": "md", "doc.txt": "txt", "readme.md": "md2"}

    @coco.fn
    async def app_main() -> None:
        await coco.mount_each(
            coco.component_subpath("files"),
            process_item,
            items.items(),
        )

    app = coco.App(
        coco.AppConfig(name="test_selector_glob", environment=env),
        app_main,
    )

    # First run without selector.
    await app.update()
    assert set(GlobalDictTarget.store.data.keys()) == {
        "files/doc.md",
        "files/doc.txt",
        "files/readme.md",
    }

    # Change only .md files.
    items["doc.md"] = "md_new"
    items["readme.md"] = "md2_new"

    await app.update(component_selector=[_sel(coco.Symbol("files"), "*.md")])

    # .md files updated; .txt preserved.
    assert GlobalDictTarget.store.data["files/doc.md"].data == "md_new"
    assert GlobalDictTarget.store.data["files/doc.txt"].data == "txt"
    assert GlobalDictTarget.store.data["files/readme.md"].data == "md2_new"


@pytest.mark.asyncio
async def test_component_selector_no_match(request: pytest.FixtureRequest) -> None:
    """Selector matching nothing still preserves all existing output."""
    GlobalDictTarget.store.clear()
    env = _make_env(request.node.name)

    items = {"a": "va", "b": "vb"}

    @coco.fn
    async def app_main() -> None:
        await coco.mount_each(
            coco.component_subpath("process"),
            process_item,
            items.items(),
        )

    app = coco.App(
        coco.AppConfig(name="test_selector_nomatch", environment=env),
        app_main,
    )

    # First run — all items processed.
    await app.update()
    assert set(GlobalDictTarget.store.data.keys()) == {"process/a", "process/b"}

    # Second run with non-matching selector — all items still mounted,
    # output preserved since inputs haven't changed.
    await app.update(component_selector=[_sel(coco.Symbol("process"), "nonexistent")])

    assert GlobalDictTarget.store.data["process/a"].data == "va"
    assert GlobalDictTarget.store.data["process/b"].data == "vb"


@pytest.mark.asyncio
async def test_component_selector_none(request: pytest.FixtureRequest) -> None:
    """Selector=None processes all items (default behavior)."""
    GlobalDictTarget.store.clear()
    env = _make_env(request.node.name)

    items = {"a": "va", "b": "vb"}

    @coco.fn
    async def app_main() -> None:
        await coco.mount_each(
            coco.component_subpath("p"),
            process_item,
            items.items(),
        )

    app = coco.App(
        coco.AppConfig(name="test_selector_none", environment=env),
        app_main,
    )

    await app.update()  # No selector → all items processed

    assert set(GlobalDictTarget.store.data.keys()) == {"p/a", "p/b"}


@pytest.mark.asyncio
async def test_component_selector_empty_list(request: pytest.FixtureRequest) -> None:
    """Empty selector list is normalized to None (run everything)."""
    GlobalDictTarget.store.clear()
    env = _make_env(request.node.name)

    items = {"a": "va"}

    @coco.fn
    async def app_main() -> None:
        await coco.mount_each(
            coco.component_subpath("p"),
            process_item,
            items.items(),
        )

    app = coco.App(
        coco.AppConfig(name="test_selector_empty", environment=env),
        app_main,
    )

    await app.update(component_selector=[])  # Empty → all items

    assert "p/a" in GlobalDictTarget.store.data


@pytest.mark.asyncio
async def test_use_mount_not_affected(request: pytest.FixtureRequest) -> None:
    """use_mount components always run regardless of selector."""
    GlobalDictTarget.store.clear()
    env = _make_env(request.node.name)

    side_effect_called: list[str] = []

    @coco.fn
    async def resolve_config() -> str:
        side_effect_called.append("config")
        return "resolved_config"

    @coco.fn
    async def process_with_config(value: str, config: str) -> None:
        key = stable_path_to_selector(coco.get_component_context()._core_path)
        coco.declare_target_state(
            GlobalDictTarget.target_state(key, f"{value}:{config}")
        )

    items = {"a": "va", "b": "vb"}

    @coco.fn
    async def app_main() -> None:
        config = await coco.use_mount(resolve_config)
        await coco.mount_each(
            coco.component_subpath("proc"),
            process_with_config,
            items.items(),
            config,
        )

    app = coco.App(
        coco.AppConfig(name="test_selector_usemount", environment=env),
        app_main,
    )

    # First run without selector.
    await app.update()
    assert side_effect_called == ["config"]
    assert set(GlobalDictTarget.store.data.keys()) == {"proc/a", "proc/b"}

    # Change only selected item.
    items["a"] = "va_new"

    await app.update(component_selector=[_sel(coco.Symbol("proc"), "a")])

    # use_mount should have run again.
    assert side_effect_called == ["config", "config"]
    # Selected item updated; unselected item preserved.
    assert GlobalDictTarget.store.data["proc/a"].data == "va_new:resolved_config"
    assert GlobalDictTarget.store.data["proc/b"].data == "vb:resolved_config"


@pytest.mark.asyncio
async def test_component_selector_cleaned() -> None:
    """After update() completes, get_component_selector() returns None."""
    GlobalDictTarget.store.clear()
    env = _SHARED_ENV

    @coco.fn
    async def app_main() -> None:
        pass

    app = coco.App(
        coco.AppConfig(name="test_selector_clean", environment=env),
        app_main,
    )

    await app.update(component_selector=[_sel(coco.Symbol("something"))])

    assert get_component_selector() is None


@pytest.mark.asyncio
async def test_component_selector_wildcard(request: pytest.FixtureRequest) -> None:
    """Wildcard pattern matches all items under a mount_each."""
    GlobalDictTarget.store.clear()
    env = _make_env(request.node.name)

    items = {"f1": "v1", "f2": "v2", "f3": "v3"}

    @coco.fn
    async def app_main() -> None:
        await coco.mount_each(
            coco.component_subpath("batch"),
            process_item,
            items.items(),
        )

    app = coco.App(
        coco.AppConfig(name="test_selector_wildcard", environment=env),
        app_main,
    )

    # First run without selector.
    await app.update()

    # Change all items.
    items["f1"] = "v1_new"
    items["f2"] = "v2_new"
    items["f3"] = "v3_new"

    await app.update(component_selector=[_sel(coco.Symbol("batch"), "*")])

    assert GlobalDictTarget.store.data["batch/f1"].data == "v1_new"
    assert GlobalDictTarget.store.data["batch/f2"].data == "v2_new"
    assert GlobalDictTarget.store.data["batch/f3"].data == "v3_new"


@pytest.mark.asyncio
async def test_unselected_paths_preserved(
    request: pytest.FixtureRequest,
) -> None:
    """When a selector is provided, unselected paths are not touched.

    Sequence:
    1. Run app.update() without selector → all items processed.
    2. Update an input item.
    3. Run app.update() with a selector → only the selected item's output
       should change; unselected items' outputs are preserved.
    """
    GlobalDictTarget.store.clear()
    env = _make_env(request.node.name)

    items = {"a": "value_a", "b": "value_b"}

    @coco.fn
    async def app_main() -> None:
        await coco.mount_each(
            coco.component_subpath("process"),
            process_item,
            items.items(),
        )

    app = coco.App(
        coco.AppConfig(name="test_unselected_preserved", environment=env),
        app_main,
    )

    # Step 1: Run without selector.
    await app.update()
    assert GlobalDictTarget.store.data["process/a"].data == "value_a"
    assert GlobalDictTarget.store.data["process/b"].data == "value_b"

    # Step 2: Update input item "a".
    items["a"] = "new_value_a"

    # Step 3: Run with selector selecting only "process/a".
    await app.update(component_selector=[_sel(coco.Symbol("process"), "a")])

    # Selected item updated.
    assert GlobalDictTarget.store.data["process/a"].data == "new_value_a"
    # Unselected item preserved — NOT deleted and NOT changed.
    assert GlobalDictTarget.store.data["process/b"].data == "value_b"

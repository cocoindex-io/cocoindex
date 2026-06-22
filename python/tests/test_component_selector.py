"""Tests for component_selector parameter on App.update()."""

from __future__ import annotations

import pytest

import cocoindex as coco
from cocoindex._internal.component_ctx import get_component_selector
from cocoindex._internal.stable_path import stable_path_to_selector

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


@pytest.mark.asyncio
async def test_component_selector_basic(request: pytest.FixtureRequest) -> None:
    """Selector matching one specific key processes only that item."""
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

    await app.update(component_selector=["process/a"])

    assert set(GlobalDictTarget.store.data.keys()) == {"process/a"}
    assert GlobalDictTarget.store.data["process/a"].data == "value_a"


@pytest.mark.asyncio
async def test_component_selector_multiple_patterns(
    request: pytest.FixtureRequest,
) -> None:
    """Multiple selector patterns match multiple items."""
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

    await app.update(component_selector=["proc/a", "proc/c"])

    assert set(GlobalDictTarget.store.data.keys()) == {"proc/a", "proc/c"}


@pytest.mark.asyncio
async def test_component_selector_glob(request: pytest.FixtureRequest) -> None:
    """Glob patterns match files by extension."""
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

    await app.update(component_selector=["files/*.md"])

    assert set(GlobalDictTarget.store.data.keys()) == {
        "files/doc.md",
        "files/readme.md",
    }


@pytest.mark.asyncio
async def test_component_selector_no_match(request: pytest.FixtureRequest) -> None:
    """Selector matching nothing results in zero children mounted. App completes normally."""
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

    # Should not raise — just mounts zero children
    await app.update(component_selector=["nonexistent"])

    assert GlobalDictTarget.store.data == {}


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

    @coco.fn
    async def app_main() -> None:
        config = await coco.use_mount(resolve_config)
        await coco.mount_each(
            coco.component_subpath("proc"),
            process_with_config,
            {"a": "va", "b": "vb"}.items(),
            config,
        )

    app = coco.App(
        coco.AppConfig(name="test_selector_usemount", environment=env),
        app_main,
    )

    await app.update(component_selector=["proc/a"])

    # use_mount should have run
    assert side_effect_called == ["config"]
    # Only selected item processed
    assert set(GlobalDictTarget.store.data.keys()) == {"proc/a"}
    assert GlobalDictTarget.store.data["proc/a"].data == "va:resolved_config"


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

    await app.update(component_selector=["something"])

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

    await app.update(component_selector=["batch/*"])

    assert set(GlobalDictTarget.store.data.keys()) == {
        "batch/f1",
        "batch/f2",
        "batch/f3",
    }

"""Sliced execution: a container target change would take unselected rows with it."""

from __future__ import annotations

from typing import Any

import pytest

import cocoindex as coco

from tests import common
from tests.common.target_states import DictsTarget

coco_env = common.create_test_env(__file__)
_rows: dict[str, int] = {}


@coco.fn
async def _row(provider: Any, key: str, value: int) -> None:
    coco.declare_target_state(provider.target_state(key, value))


@coco.fn(memo=True)
async def _memo_row(provider: Any, key: str, value: int) -> None:
    coco.declare_target_state(provider.target_state(key, value))


def _main_for(fn: Any) -> Any:
    @coco.fn
    async def main() -> None:
        provider = await coco.use_mount(
            coco.component_subpath("setup"), DictsTarget.declare_dict_target, "D"
        )
        with coco.component_subpath("rows"):
            for key, value in sorted(_rows.items()):
                await coco.mount(coco.component_subpath(key), fn, provider, key, value)

    return main


def _data() -> dict[str, Any]:
    return {k: v.data for k, v in DictsTarget.store.data.get("D", {}).items()}


@pytest.mark.parametrize("invalidation", ["destructive", "lossy"])
@pytest.mark.parametrize("memo", [False, True])
def test_container_change_under_a_selector_is_refused(
    invalidation: Any, memo: bool
) -> None:
    DictsTarget.store.clear()
    _rows.clear()
    _rows.update(a=1, b=1, c=1)
    app = coco.App(
        coco.AppConfig(
            name=f"selector_container_{invalidation}_{memo}", environment=coco_env
        ),
        _main_for(_memo_row if memo else _row),
    )
    app.update_blocking()
    assert _data() == {"a": 1, "b": 1, "c": 1}

    DictsTarget.store.child_invalidation = invalidation
    selector = [coco.ROOT_PATH / "rows" / "a"]
    try:
        # The preview of a page and its update agree: both refuse, alike.
        with pytest.raises(Exception) as preview_refusal:
            app.update_blocking(preview=True, component_selector=selector)
        with pytest.raises(Exception) as update_refusal:
            app.update_blocking(component_selector=selector)
        assert type(preview_refusal.value) is type(update_refusal.value)
        assert str(preview_refusal.value) == str(update_refusal.value)
        message = str(update_refusal.value)
        assert "container target state" in message
        assert '"D"' in message  # The container's path.
        assert "A full update is needed" in message
        assert _data() == {"a": 1, "b": 1, "c": 1}

        # A plain update declares every row again.
        app.update_blocking()
    finally:
        DictsTarget.store.child_invalidation = None
    assert _data() == {"a": 1, "b": 1, "c": 1}


def test_container_removal_under_a_selector_is_refused_alike_in_preview() -> None:
    declares = {"on": True}

    @coco.fn
    async def setup() -> Any:
        if declares["on"]:
            return DictsTarget.declare_dict_target("D")
        return None

    @coco.fn
    async def main() -> None:
        provider = await coco.use_mount(coco.component_subpath("setup"), setup)
        if provider is None:
            return
        with coco.component_subpath("rows"):
            for key, value in sorted(_rows.items()):
                await coco.mount(coco.component_subpath(key), _row, provider, key, value)

    DictsTarget.store.clear()
    _rows.clear()
    _rows.update(a=1, b=1)
    app = coco.App(
        coco.AppConfig(name="selector_container_removal", environment=coco_env), main
    )
    app.update_blocking()
    assert _data() == {"a": 1, "b": 1}

    # The setup component still runs (a use_mount child) and no longer
    # declares the container, which would take row "b" with it.
    declares["on"] = False
    selector = [coco.ROOT_PATH / "rows" / "a"]
    with pytest.raises(Exception) as preview_refusal:
        app.update_blocking(preview=True, component_selector=selector)
    with pytest.raises(Exception) as update_refusal:
        app.update_blocking(component_selector=selector)
    assert type(preview_refusal.value) is type(update_refusal.value)
    assert str(preview_refusal.value) == str(update_refusal.value)
    assert "needs a removal" in str(update_refusal.value)
    assert "A full update is needed" in str(update_refusal.value)
    assert _data() == {"a": 1, "b": 1}

    app.update_blocking()
    assert DictsTarget.store.data == {}

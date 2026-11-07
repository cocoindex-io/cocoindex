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
    try:
        with pytest.raises(Exception, match="without component_selector"):
            app.update_blocking(component_selector=[coco.ROOT_PATH / "rows" / "a"])
        assert _data() == {"a": 1, "b": 1, "c": 1}

        # A preview applies nothing, so it can show the change.
        app.update_blocking(
            preview=True, component_selector=[coco.ROOT_PATH / "rows" / "a"]
        )
        assert _data() == {"a": 1, "b": 1, "c": 1}

        # A plain update declares every row again.
        app.update_blocking()
    finally:
        DictsTarget.store.child_invalidation = None
    assert _data() == {"a": 1, "b": 1, "c": 1}

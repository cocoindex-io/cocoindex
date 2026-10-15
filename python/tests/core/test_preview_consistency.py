"""A preview plans what the next real update does, in cases where the update
relies on state it writes while it runs (and a preview does not write)."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

import cocoindex as coco

from tests import common
from tests.common.target_states import DictDataWithPrev, GlobalDictTarget

coco_env = common.create_test_env(__file__)


def _data() -> dict[str, Any]:
    return {k: v.data for k, v in GlobalDictTarget.store.data.items()}


##################################################################################
# A target moving to a component that plans after the still-mounted old owner
##################################################################################


@pytest.mark.parametrize("changed", [False, True])
@pytest.mark.parametrize("new_owner", ["parent", "sibling"])
def test_preview_target_moving_from_a_still_mounted_owner(
    new_owner: str, changed: bool
) -> None:
    """The old owner stays mounted but stops declaring the target, and a
    component that plans after it starts declaring it. An update deletes the
    target through the old owner, then creates it through the new one; a
    preview plans the same, not a lone delete."""
    GlobalDictTarget.store.clear()
    moved = False
    value = 1

    async def old_owner() -> None:
        if not moved:
            coco.declare_target_state(GlobalDictTarget.target_state("moved", value))

    async def sibling() -> None:
        if moved and new_owner == "sibling":
            # Plan after the old owner, as a parent always does.
            await asyncio.sleep(0.2)
            coco.declare_target_state(GlobalDictTarget.target_state("moved", value))

    async def main() -> None:
        await asyncio.gather(
            coco.use_mount(coco.component_subpath("old"), old_owner),
            coco.use_mount(coco.component_subpath("sibling"), sibling),
        )
        if moved and new_owner == "parent":
            coco.declare_target_state(GlobalDictTarget.target_state("moved", value))

    app = coco.App(
        coco.AppConfig(
            name=f"preview_move_from_mounted_{new_owner}_{changed}",
            environment=coco_env,
        ),
        main,
    )
    app.update_blocking()
    GlobalDictTarget.store.metrics.clear()

    moved = True
    value = 2 if changed else 1
    created = DictDataWithPrev(data=value, prev=[], prev_may_be_missing=True)
    for _ in range(2):
        assert app.update_blocking(preview=True) == [
            ("moved", coco.NON_EXISTENCE),
            ("moved", created),
        ]
        assert _data() == {"moved": 1}
        assert GlobalDictTarget.store.metrics.collect() == {}

    app.update_blocking()
    assert GlobalDictTarget.store.data["moved"] == created
    metrics = GlobalDictTarget.store.metrics.collect()
    assert (metrics.get("delete", 0), metrics.get("upsert", 0)) == (1, 1)


##################################################################################
# A directory of components refactored into a component mounting them
##################################################################################

_files: list[str] = []
_files_in_a_component = False


async def _process_file(name: str) -> None:
    coco.declare_target_state(GlobalDictTarget.target_state(name, 1))


async def _process_files() -> None:
    for name in _files:
        await coco.use_mount(coco.component_subpath(name), _process_file, name)


async def _main_files() -> None:
    if _files_in_a_component:
        await coco.use_mount(coco.component_subpath("files"), _process_files)
    else:
        with coco.component_subpath("files"):
            await _process_files()


def test_preview_directory_refactored_into_a_component() -> None:
    """`files/<name>` stay the same component paths when `files` turns from a
    directory into a component: an update keeps every file component, and only
    deletes the one no longer mounted. A preview plans the same, each delete
    once."""
    global _files_in_a_component
    GlobalDictTarget.store.clear()
    _files[:] = ["a", "b", "c"]
    _files_in_a_component = False

    app = coco.App(
        coco.AppConfig(name="preview_directory_to_component", environment=coco_env),
        _main_files,
    )
    app.update_blocking()
    GlobalDictTarget.store.metrics.clear()

    _files_in_a_component = True
    try:
        assert app.update_blocking(preview=True) == []
        _files[:] = ["a", "c"]
        for _ in range(2):
            assert app.update_blocking(preview=True) == [("b", coco.NON_EXISTENCE)]
        assert _data() == {"a": 1, "b": 1, "c": 1}
        assert GlobalDictTarget.store.metrics.collect() == {}

        app.update_blocking()
        assert _data() == {"a": 1, "c": 1}
        assert GlobalDictTarget.store.metrics.collect() == {"delete": 1, "sink": 1}
    finally:
        _files_in_a_component = False


##################################################################################
# A live delete, then a full update that no longer mounts the child
##################################################################################

_live_items: dict[str, int] = {}


async def _declare_live_item(key: str, value: int) -> None:
    coco.declare_target_state(GlobalDictTarget.target_state(key, value))


class _DeleteThenUpdateFullLiveComponent:
    """Deletes child `b` through `operator.delete()` when `delete_b` is set,
    then runs a full update."""

    delete_b = False

    async def process(self) -> None:
        for key, value in _live_items.items():
            await coco.mount(
                coco.component_subpath(key), _declare_live_item, key, value
            )

    async def process_live(self, operator: coco.LiveComponentOperator) -> None:
        if _DeleteThenUpdateFullLiveComponent.delete_b:
            handle = await operator.delete(coco.component_subpath("b"))
            await handle.ready()
        await operator.update_full()
        await operator.mark_ready()


def test_preview_live_delete_then_full_update_plans_one_delete() -> None:
    """`b` is deleted live and is gone from the full update's source too: an
    update deletes it once, and a preview plans it once."""
    GlobalDictTarget.store.clear()
    _live_items.clear()
    _live_items.update({"a": 1, "b": 2})

    async def main() -> None:
        await coco.mount(
            coco.component_subpath("live"), _DeleteThenUpdateFullLiveComponent
        )

    app = coco.App(
        coco.AppConfig(name="preview_live_delete_then_full", environment=coco_env),
        main,
    )
    app.update_blocking()
    GlobalDictTarget.store.metrics.clear()

    del _live_items["b"]
    _DeleteThenUpdateFullLiveComponent.delete_b = True
    try:
        assert app.update_blocking(preview=True) == [("b", coco.NON_EXISTENCE)]
        assert _data() == {"a": 1, "b": 2}
        assert GlobalDictTarget.store.metrics.collect() == {}
    finally:
        _DeleteThenUpdateFullLiveComponent.delete_b = False

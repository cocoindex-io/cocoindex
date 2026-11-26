"""A preview lists what an update does for the components it no longer mounts:
the deletion of their target states, down their whole subtree. It applies none
of it and writes nothing, and the update that follows does exactly that."""

from __future__ import annotations

from typing import Any

import pytest

import cocoindex as coco
import cocoindex.inspect as coco_inspect

from tests import common
from tests.common.target_states import (
    DictDataWithPrev,
    DictsTarget,
    GlobalDictTarget,
    split_dicts_actions,
)

coco_env = common.create_test_env(__file__)

# Item name -> {child key -> value}.
_source: dict[str, dict[str, int]] = {}


@pytest.fixture(autouse=True)
def _reset() -> None:
    GlobalDictTarget.store.clear()
    GlobalDictTarget.store.sink_exception = False
    DictsTarget.store.clear()
    _source.clear()


@coco.fn
async def _child(key: str, value: int) -> None:
    coco.declare_target_state(GlobalDictTarget.target_state(key, value))


@coco.fn
async def _item(name: str, children: dict[str, int]) -> None:
    coco.declare_target_state(GlobalDictTarget.target_state(name, len(children)))
    for key, value in children.items():
        await coco.mount(coco.component_subpath(key), _child, f"{name}/{key}", value)


@coco.fn
async def _main() -> None:
    for name, children in _source.items():
        await coco.mount(coco.component_subpath("items", name), _item, name, children)


def _app(name: str, main: Any = _main) -> coco.App[Any, Any]:
    return coco.App(
        coco.AppConfig(name=f"test_preview_deletes_{name}", environment=coco_env),
        main,
    )


def _stored() -> dict[str, Any]:
    return {k: v.data for k, v in GlobalDictTarget.store.data.items()}


def _planned(actions: Any) -> dict[str, Any]:
    """Each planned action by key: the new value, or `None` for a delete."""
    planned: dict[str, Any] = {}
    for key, value in actions:
        assert key not in planned
        planned[key] = None if coco.is_non_existence(value) else value.data
    return planned


def _snapshot(app: coco.App[Any, Any]) -> tuple[Any, ...]:
    """What a preview must leave as it is: the target and CocoIndex's records."""
    return (
        _stored(),
        sorted(str(p) for p in coco_inspect.list_stable_paths_sync(app)),
        {k: str(v) for k, v in common.list_target_state_owners_sync(app).items()},
    )


def _applies_exactly(app: coco.App[Any, Any], planned: dict[str, Any]) -> None:
    """The next update applies exactly the planned actions."""
    before = _stored()
    GlobalDictTarget.store.metrics.clear()
    app.update_blocking()
    metrics = GlobalDictTarget.store.metrics.collect()
    deletes = {k for k, v in planned.items() if v is None}
    upserts = {k: v for k, v in planned.items() if v is not None}
    assert metrics.get("delete", 0) == len(deletes)
    assert metrics.get("upsert", 0) == len(upserts)
    expected = {k: v for k, v in before.items() if k not in deletes} | upserts
    assert _stored() == expected


def test_preview_lists_the_deletes_of_a_removed_item_and_its_subtree() -> None:
    _source.update(a={"x": 1}, b={"x": 2, "y": 3})
    app = _app("removed_item")
    app.update_blocking()
    snapshot = _snapshot(app)
    GlobalDictTarget.store.metrics.clear()

    del _source["b"]
    planned = _planned(app.update_blocking(preview=True))

    assert planned == {"b": None, "b/x": None, "b/y": None}
    # Nothing applied, nothing recorded.
    assert GlobalDictTarget.store.metrics.collect() == {}
    assert _snapshot(app) == snapshot
    # So a second preview plans the same deletes again.
    assert _planned(app.update_blocking(preview=True)) == planned

    _applies_exactly(app, planned)
    assert _stored() == {"a": 1, "a/x": 1}


@pytest.mark.asyncio
async def test_async_preview_lists_the_deletes_of_a_removed_item() -> None:
    _source.update(a={}, b={"x": 2})
    app = _app("removed_item_async")
    await app.update()

    del _source["b"]
    planned = _planned(await app.update(preview=True))

    assert planned == {"b": None, "b/x": None}
    assert _stored() == {"a": 0, "b": 1, "b/x": 2}


def test_preview_lists_the_subtree_of_a_renamed_child_key() -> None:
    _source.update(a={"x": 1, "y": 2})
    app = _app("renamed_key")
    app.update_blocking()
    snapshot = _snapshot(app)

    _source["a"] = {"x": 1, "z": 2}
    planned = _planned(app.update_blocking(preview=True))

    assert planned == {"a/y": None, "a/z": 2}
    assert _snapshot(app) == snapshot

    _applies_exactly(app, planned)


def test_preview_lists_the_deletes_of_everything_when_nothing_is_mounted() -> None:
    _source.update(a={"x": 1}, b={})
    app = _app("all_removed")
    app.update_blocking()
    snapshot = _snapshot(app)

    _source.clear()
    planned = _planned(app.update_blocking(preview=True))

    assert planned == {"a": None, "a/x": None, "b": None}
    assert _snapshot(app) == snapshot

    _applies_exactly(app, planned)
    assert coco_inspect.list_stable_paths_sync(app) == [coco.ROOT_PATH]


def test_preview_lists_the_delete_of_a_removed_container() -> None:
    @coco.fn
    async def container(name: str, entries: dict[str, int]) -> None:
        provider = await coco.mount_target(DictsTarget.dict_target(name))
        for key, value in entries.items():
            coco.declare_target_state(provider.target_state(key, value))

    containers: dict[str, dict[str, int]] = {"D1": {"a": 1}, "D2": {"b": 2}}

    @coco.fn
    async def main() -> None:
        for name, entries in containers.items():
            await coco.mount(coco.component_subpath(name), container, name, entries)

    app = _app("removed_container", main)
    app.update_blocking()
    DictsTarget.store.metrics.clear()

    del containers["D2"]
    planned = split_dicts_actions(app.update_blocking(preview=True))

    # A declared container's action is listed on every run (see
    # `test_preview_child_target_providers`); the gone one's is a delete.
    assert {n: a.action for n, a in planned.containers.items()} == {
        "D1": None,
        "D2": "delete",
    }
    # Its entries go with it: no action of their own.
    assert planned.entries == {}
    assert set(DictsTarget.store.data) == {"D1", "D2"}
    assert DictsTarget.store.metrics.collect() == {}

    app.update_blocking()
    assert DictsTarget.store.data == {
        "D1": {"a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True)}
    }


def test_preview_of_a_delete_that_fails_only_in_the_sink_succeeds() -> None:
    # The stated limit: a failure that happens only when a sink applies its
    # actions shows up only in the update.
    _source.update(a={}, b={})
    app = _app("sink_failure")
    app.update_blocking()

    del _source["b"]
    GlobalDictTarget.store.sink_exception = True
    try:
        assert _planned(app.update_blocking(preview=True)) == {"b": None}
        app.update_blocking()
        assert _stored() == {"a": 0, "b": 0}
    finally:
        GlobalDictTarget.store.sink_exception = False

    # The delete the update owes is previewed again, then applied.
    assert _planned(app.update_blocking(preview=True)) == {"b": None}
    app.update_blocking()
    assert _stored() == {"a": 0}

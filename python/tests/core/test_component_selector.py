"""Tests for sliced execution: ``App.update(component_selector=...)``.

An unselected component is neither run nor treated as gone. The root, ``use_mount``
children and the ancestors of a selected path still run, and a run that may have
left children unrun stores no memo.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

import cocoindex as coco
from cocoindex._internal.inspect_api import list_stable_paths_sync
from cocoindex.resources.live_map import LiveMap

from tests import common
from tests.common.target_states import GlobalDictTarget

coco_env = common.create_test_env(__file__)

_source: dict[Any, Any] = {}
_runs: list[str] = []


@pytest.fixture(autouse=True)
def _reset() -> None:
    GlobalDictTarget.store.clear()
    GlobalDictTarget.store.sink_exception = False
    _source.clear()
    _runs.clear()


def _stored() -> dict[str, Any]:
    return {k: v.data for k, v in GlobalDictTarget.store.data.items()}


def _planned(actions: Any) -> dict[str, Any]:
    return {key: value.data for key, value in actions}


def _declare(key: Any, value: Any) -> None:
    coco.declare_target_state(GlobalDictTarget.target_state(str(key), value))


def _entries() -> list[tuple[Any, tuple[Any, Any]]]:
    return [(k, (k, v)) for k, v in _source.items()]


@coco.fn
async def _item(entry: tuple[Any, Any]) -> None:
    key, value = entry
    _runs.append(str(key))
    _declare(key, value)


@coco.fn(memo=True)
async def _memo_item(entry: tuple[Any, Any]) -> None:
    key, value = entry
    _runs.append(str(key))
    _declare(key, value)


def _items_path(key: Any) -> coco.StablePath:
    path: coco.StablePath = coco.ROOT_PATH / "items" / key
    return path


@coco.fn
async def _mount_each_main() -> None:
    _runs.append("root")
    await coco.mount_each(coco.component_subpath("items"), _item, _entries())


def _app(name: str, main: Any = _mount_each_main, *args: Any) -> coco.App[Any, Any]:
    return coco.App(
        coco.AppConfig(name=f"test_selector_{name}", environment=coco_env), main, *args
    )


def _run_all_then_clear_log(app: coco.App[Any, Any]) -> None:
    app.update_blocking()
    _runs.clear()


# ---------------------------------------------------------------------------
# The acceptance scenarios
# ---------------------------------------------------------------------------


def test_unselected_source_changed_is_not_run_and_keeps_its_output() -> None:
    _source.update(a=1, b=1)
    app = _app("unselected_changed")
    _run_all_then_clear_log(app)

    _source.update(a=2, b=2)
    app.update_blocking(component_selector=[_items_path("a")])

    assert _runs == ["root", "a"]
    assert _stored() == {"a": 2, "b": 1}


def test_unselected_source_deleted_is_not_run_and_keeps_its_target_states() -> None:
    _source.update(a=1, b=1)
    app = _app("unselected_deleted")
    _run_all_then_clear_log(app)
    paths_before = {str(p) for p in list_stable_paths_sync(app)}

    del _source["b"]
    app.update_blocking(component_selector=[_items_path("a")])

    assert _runs == ["root", "a"]
    assert _stored() == {"a": 1, "b": 1}
    assert {str(p) for p in list_stable_paths_sync(app)} == paths_before

    # A later update that covers it sees it gone.
    app.update_blocking()
    assert _stored() == {"a": 1}


def test_unselected_new_item_leaves_no_trace() -> None:
    _source.update(a=1)
    app = _app("unselected_new")
    _run_all_then_clear_log(app)
    paths_before = {str(p) for p in list_stable_paths_sync(app)}

    _source["new"] = 1
    app.update_blocking(component_selector=[_items_path("a")])

    assert _runs == ["root", "a"]
    assert _stored() == {"a": 1}
    assert {str(p) for p in list_stable_paths_sync(app)} == paths_before


def test_selected_new_item_runs() -> None:
    _source.update(a=1)
    app = _app("selected_new")
    _run_all_then_clear_log(app)

    _source["new"] = 1
    app.update_blocking(component_selector=[_items_path("new")])

    assert _runs == ["root", "new"]
    assert _stored() == {"a": 1, "new": 1}


def test_selected_changed_updates_and_selected_unchanged_memo_hits() -> None:
    @coco.fn
    async def main() -> None:
        await coco.mount_each(coco.component_subpath("items"), _memo_item, _entries())

    _source.update(a=1, b=1, c=1)
    app = _app("selected_memo", main)
    _run_all_then_clear_log(app)

    _source.update(a=2, c=2)
    handle = app.update(component_selector=[_items_path("a"), _items_path("b")])
    asyncio.run(_finish(handle))

    assert _runs == ["a"]
    assert _stored() == {"a": 2, "b": 1, "c": 1}
    stats = handle.stats()
    assert stats is not None
    assert stats.total.num_unchanged == 1


async def _finish(handle: coco.UpdateHandle[Any]) -> None:
    await handle.result()


def test_selected_source_deleted_is_deleted() -> None:
    _source.update(a=1, b=1)
    app = _app("selected_deleted")
    _run_all_then_clear_log(app)

    del _source["a"]
    del _source["b"]
    app.update_blocking(component_selector=[_items_path("a")])

    assert _stored() == {"b": 1}


def test_select_by_several_paths() -> None:
    _source.update(a=1, b=1, c=1)
    app = _app("several")
    _run_all_then_clear_log(app)

    _source.update(a=2, b=2, c=2)
    app.update_blocking(component_selector=[_items_path("a"), _items_path("c")])

    assert sorted(_runs) == ["a", "c", "root"]
    assert _stored() == {"a": 2, "b": 1, "c": 2}


def test_select_by_subtree_prefix() -> None:
    @coco.fn
    async def main() -> None:
        await coco.mount_each(coco.component_subpath("items"), _item, _entries())
        await coco.mount(
            coco.component_subpath("other"), _item, ("other", _source["a"])
        )

    _source.update(a=1, b=1)
    app = _app("prefix", main)
    _run_all_then_clear_log(app)

    _source.update(a=2, b=2)
    app.update_blocking(component_selector=[coco.ROOT_PATH / "items"])

    assert sorted(_runs) == ["a", "b"]
    assert _stored() == {"a": 2, "b": 2, "other": 1}


def test_selector_takes_any_iterable_of_paths() -> None:
    _source.update(a=1, b=1)
    app = _app("iterable")
    _run_all_then_clear_log(app)

    _source.update(a=2, b=2)
    app.update_blocking(component_selector=(_items_path(k) for k in ["a"]))
    assert _stored() == {"a": 2, "b": 1}


def test_paths_from_inspection_select_the_components_they_name() -> None:
    _source.update(a=1, b=1)
    app = _app("inspection")
    _run_all_then_clear_log(app)

    listed = [p for p in list_stable_paths_sync(app) if str(p).endswith('"a"')]
    assert len(listed) == 1

    _source.update(a=2, b=2)
    app.update_blocking(component_selector=listed)
    assert _stored() == {"a": 2, "b": 1}


@pytest.mark.parametrize(
    "key",
    [7, True, ("tuple", 1), b"bytes", None],
    ids=["int", "bool", "tuple", "bytes", "none"],
)
def test_keys_of_any_type_select_by_value(key: Any) -> None:
    other = "other"
    _source.update({key: 1, other: 1})
    app = _app(f"key_{type(key).__name__}")
    _run_all_then_clear_log(app)

    _source.update({key: 2, other: 2})
    app.update_blocking(component_selector=[_items_path(key)])

    assert _stored() == {str(key): 2, other: 1}


def test_a_key_that_prints_the_same_is_not_selected() -> None:
    _source.update({1: 1, "1x": 1})
    app = _app("key_type_distinct")
    _run_all_then_clear_log(app)

    _source.update({1: 2})
    app.update_blocking(component_selector=[_items_path("1")])

    assert _stored() == {"1": 1, "1x": 1}


def test_default_subpath_is_a_symbol_key() -> None:
    @coco.fn
    async def main() -> None:
        await coco.mount_each(_item, _entries())

    _source.update(a=1, b=1)
    app = _app("symbol", main)
    _run_all_then_clear_log(app)

    _source.update(a=2, b=2)
    app.update_blocking(
        component_selector=[coco.ROOT_PATH / coco.Symbol("_item") / "a"]
    )
    assert _stored() == {"a": 2, "b": 1}


def test_rejects_anything_but_paths_early() -> None:
    app = _app("reject")
    with pytest.raises(TypeError, match="takes StablePath values"):
        app.update_blocking(component_selector="items/a")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="takes StablePath values"):
        app.update(component_selector=["items/a"])  # type: ignore[list-item]
    with pytest.raises(TypeError, match="not iterable"):
        app.update(component_selector=_items_path("a"))  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# mount() and nesting
# ---------------------------------------------------------------------------


@coco.fn
async def _parent() -> None:
    _runs.append("parent")
    await coco.mount_each(coco.component_subpath("kids"), _item, _entries())


@coco.fn
async def _nested_main() -> None:
    _runs.append("root")
    await coco.mount(coco.component_subpath("parent"), _parent)
    await coco.mount(
        coco.component_subpath("sibling"), _item, ("sibling", _source["a"])
    )


def test_plain_mount_honours_the_selector() -> None:
    _source.update(a=1)
    app = _app("plain_mount", _nested_main)
    _run_all_then_clear_log(app)

    _source.update(a=2)
    app.update_blocking(component_selector=[coco.ROOT_PATH / "sibling"])

    assert sorted(_runs) == ["root", "sibling"]
    assert _stored() == {"a": 1, "sibling": 2}


def test_selected_child_under_unselected_parent_runs_the_parent_to_reach_it() -> None:
    _source.update(a=1, b=1)
    app = _app("nested", _nested_main)
    _run_all_then_clear_log(app)

    _source.update(a=2, b=2)
    app.update_blocking(
        component_selector=[coco.ROOT_PATH / "parent" / "kids" / "a"],
    )

    assert sorted(_runs) == ["a", "parent", "root"]
    assert _stored() == {"a": 2, "b": 1, "sibling": 1}


def test_selecting_a_parent_selects_its_subtree() -> None:
    _source.update(a=1, b=1)
    app = _app("parent_subtree", _nested_main)
    _run_all_then_clear_log(app)

    _source.update(a=2, b=2)
    app.update_blocking(component_selector=[coco.ROOT_PATH / "parent"])

    assert sorted(_runs) == ["a", "b", "parent", "root"]
    assert _stored() == {"a": 2, "b": 2, "sibling": 1}


def test_ancestor_no_longer_mounted_keeps_its_whole_subtree() -> None:
    mounted = [True]

    @coco.fn
    async def main() -> None:
        if mounted[0]:
            await coco.mount(coco.component_subpath("parent"), _parent)

    _source.update(a=1, b=1)
    app = _app("ancestor_gone", main)
    _run_all_then_clear_log(app)

    mounted[0] = False
    app.update_blocking(
        component_selector=[coco.ROOT_PATH / "parent" / "kids" / "a"],
    )
    assert _runs == []
    assert _stored() == {"a": 1, "b": 1}

    app.update_blocking()
    assert _stored() == {}


def test_selected_gone_under_a_directory_is_deleted_and_its_sibling_kept() -> None:
    @coco.fn
    async def main() -> None:
        with coco.component_subpath("dir", "deep"):
            for key, value in _source.items():
                await coco.mount(coco.component_subpath(key), _item, (key, value))

    _source.update(a=1, b=1)
    app = _app("directory", main)
    _run_all_then_clear_log(app)

    _source.clear()
    app.update_blocking(component_selector=[coco.ROOT_PATH / "dir" / "deep" / "a"])

    assert _stored() == {"b": 1}

    app.update_blocking()
    assert _stored() == {}


def test_unselected_component_replaced_by_a_directory_stays_until_covered() -> None:
    @coco.fn
    async def as_component() -> None:
        _declare("x", 1)

    use_directory = [False]

    @coco.fn
    async def main() -> None:
        if use_directory[0]:
            await coco.mount(coco.component_subpath("x", "y"), _item, ("y", 1))
        else:
            await coco.mount(coco.component_subpath("x"), as_component)

    app = _app("demote", main)
    _run_all_then_clear_log(app)
    assert _stored() == {"x": 1}

    use_directory[0] = True
    app.update_blocking(component_selector=[coco.ROOT_PATH / "x" / "y"])
    assert _stored() == {"x": 1, "y": 1}

    app.update_blocking()
    assert _stored() == {"y": 1}


# ---------------------------------------------------------------------------
# use_mount and the root always run
# ---------------------------------------------------------------------------


@coco.fn
async def _helper() -> int:
    _runs.append("helper")
    await coco.mount_each(coco.component_subpath("kids"), _item, _entries())
    return 7


def test_use_mount_runs_for_its_value_and_its_mounted_children_stay_filtered() -> None:
    received: list[int] = []

    @coco.fn
    async def main() -> None:
        received.append(await coco.use_mount(coco.component_subpath("helper"), _helper))
        await coco.mount(coco.component_subpath("sibling"), _item, ("sibling", 1))

    _source.update(a=1)
    app = _app("use_mount", main)
    _run_all_then_clear_log(app)
    received.clear()

    _source.update(a=2)
    app.update_blocking(component_selector=[coco.ROOT_PATH / "sibling"])

    assert received == [7]
    assert sorted(_runs) == ["helper", "sibling"]
    assert _stored() == {"a": 1, "sibling": 1}


def test_selected_under_use_mount_child_runs() -> None:
    @coco.fn
    async def main() -> None:
        await coco.use_mount(coco.component_subpath("helper"), _helper)

    _source.update(a=1, b=1)
    app = _app("use_mount_selected", main)
    _run_all_then_clear_log(app)

    _source.update(a=2, b=2)
    app.update_blocking(
        component_selector=[coco.ROOT_PATH / "helper" / "kids" / "a"],
    )

    assert sorted(_runs) == ["a", "helper"]
    assert _stored() == {"a": 2, "b": 1}


def test_memoized_use_mount_child_without_children_stays_reusable_when_unchanged() -> (
    None
):
    @coco.fn(memo=True)
    async def memo_helper() -> int:
        _runs.append("memo_helper")
        return 7

    @coco.fn
    async def main() -> None:
        await coco.use_mount(coco.component_subpath("helper"), memo_helper)
        await coco.mount_each(coco.component_subpath("items"), _item, _entries())

    _source.update(a=1)
    app = _app("memo_use_mount_hit", main)
    _run_all_then_clear_log(app)

    app.update_blocking(component_selector=[_items_path("a")])
    assert _runs == ["a"]


def test_fan_in_after_a_skipped_mount_gets_a_ready_handle() -> None:
    collected: list[str] = []

    @coco.fn
    async def fan_in(values: list[Any]) -> None:
        collected.append(f"fan_in:{values}")

    @coco.fn
    async def main() -> None:
        handle = await coco.mount_each(
            coco.component_subpath("items"), _item, _entries()
        )
        await handle.ready()
        await coco.use_mount(coco.component_subpath("fan_in"), fan_in, sorted(_source))

    _source.update(a=1, b=1)
    app = _app("fan_in", main)
    _run_all_then_clear_log(app)
    collected.clear()

    app.update_blocking(component_selector=[_items_path("a")])

    assert _runs == ["a"]
    assert collected == ["fan_in:['a', 'b']"]


def test_root_path_selects_everything_like_no_selector() -> None:
    _source.update(a=1, b=1)
    app = _app("root_selected")
    _run_all_then_clear_log(app)

    _source.update(a=2)
    del _source["b"]
    app.update_blocking(component_selector=[coco.ROOT_PATH])

    assert sorted(_runs) == ["a", "root"]
    assert _stored() == {"a": 2}


def test_empty_selector_runs_only_the_root_and_touches_nothing() -> None:
    _source.update(a=1, b=1)
    app = _app("empty")
    _run_all_then_clear_log(app)

    _source.update(a=2)
    del _source["b"]
    app.update_blocking(component_selector=[])

    assert _runs == ["root"]
    assert _stored() == {"a": 1, "b": 1}


def test_path_that_does_not_exist_selects_nothing_and_is_no_error() -> None:
    _source.update(a=1, b=1)
    app = _app("no_such_path")
    _run_all_then_clear_log(app)

    _source.update(a=2)
    app.update_blocking(
        component_selector=[_items_path("nope"), coco.ROOT_PATH / "nope" / "x"],
    )

    assert _runs == ["root"]
    assert _stored() == {"a": 1, "b": 1}


def test_none_selector_is_a_plain_update() -> None:
    _source.update(a=1, b=1)
    app = _app("none")
    _run_all_then_clear_log(app)

    _source.update(a=2)
    del _source["b"]
    app.update_blocking(component_selector=None)

    assert sorted(_runs) == ["a", "root"]
    assert _stored() == {"a": 2}


# ---------------------------------------------------------------------------
# Memo: a run that may have left children unrun must not claim its subtree
# ---------------------------------------------------------------------------


_version = [1]


@coco.fn(memo=True)
async def _memo_parent(version: int) -> None:
    _runs.append("memo_parent")
    await coco.mount_each(coco.component_subpath("kids"), _item, _entries())


@coco.fn
async def _memo_parent_main() -> None:
    await coco.mount(coco.component_subpath("parent"), _memo_parent, _version[0])


def test_partial_run_of_a_memoized_parent_stores_no_memo() -> None:
    _version[0] = 1
    _source.update(a=1, b=1)
    app = _app("memo_parent_no_store", _memo_parent_main)
    _run_all_then_clear_log(app)

    _version[0] = 2
    _source.update(a=2, b=2)
    app.update_blocking(
        component_selector=[coco.ROOT_PATH / "parent" / "kids" / "a"],
    )
    assert _stored() == {"a": 2, "b": 1}

    # Same input as the partial run. A memo from it would hide "b" for good.
    _runs.clear()
    app.update_blocking()
    assert "memo_parent" in _runs
    assert _stored() == {"a": 2, "b": 2}

    # The full update stored a memo again.
    _runs.clear()
    app.update_blocking()
    assert _runs == []


def test_partial_run_drops_the_memo_of_an_unchanged_input() -> None:
    _version[0] = 1
    _source.update(a=1, b=1)
    app = _app("memo_parent_drop", _memo_parent_main)
    _run_all_then_clear_log(app)

    # `full_reprocess` runs the parent under its stored memo key, so only the
    # partial run itself can retire that memo.
    _source.update(a=2, b=2)
    app.update_blocking(
        full_reprocess=True,
        component_selector=[coco.ROOT_PATH / "parent" / "kids" / "a"],
    )
    assert _stored() == {"a": 2, "b": 1}

    _runs.clear()
    app.update_blocking()
    assert "memo_parent" in _runs
    assert _stored() == {"a": 2, "b": 2}


def test_memo_hit_on_an_ancestor_stands_in_for_its_selected_descendants() -> None:
    _version[0] = 1
    _source.update(a=1, b=1)
    app = _app("memo_parent_hit", _memo_parent_main)
    _run_all_then_clear_log(app)

    # As in a plain update: the memo says the subtree is up to date.
    app.update_blocking(
        component_selector=[coco.ROOT_PATH / "parent" / "kids" / "a"],
    )
    assert _runs == []


def test_selected_subtree_of_a_memoized_parent_stores_its_memo() -> None:
    _version[0] = 1
    _source.update(a=1, b=1)
    app = _app("memo_parent_selected", _memo_parent_main)
    _run_all_then_clear_log(app)

    _version[0] = 2
    _source.update(a=2, b=2)
    app.update_blocking(component_selector=[coco.ROOT_PATH / "parent"])
    assert _stored() == {"a": 2, "b": 2}

    _runs.clear()
    app.update_blocking()
    assert _runs == []


def test_memoized_function_that_mounts_is_rejected_even_if_its_mounts_are_skipped() -> (
    None
):
    @coco.fn(memo=True)
    async def mounting_fn() -> None:
        await coco.mount_each(coco.component_subpath("items"), _item, _entries())

    @coco.fn
    async def main() -> None:
        await mounting_fn()

    _source.update(a=1)
    app = _app("memo_fn", main)
    with pytest.raises(ValueError, match="mounted child components"):
        app.update_blocking(component_selector=[_items_path("nope")])


def test_full_reprocess_covers_only_what_is_selected() -> None:
    @coco.fn
    async def main() -> None:
        await coco.mount_each(coco.component_subpath("items"), _memo_item, _entries())

    _source.update(a=1, b=1)
    app = _app("full_reprocess", main)
    _run_all_then_clear_log(app)

    app.update_blocking(full_reprocess=True, component_selector=[_items_path("a")])
    assert _runs == ["a"]

    # "b" was not reprocessed, so its memo still holds.
    _runs.clear()
    app.update_blocking()
    assert _runs == []


# ---------------------------------------------------------------------------
# Preview
# ---------------------------------------------------------------------------


def test_preview_returns_actions_of_selected_paths_and_applies_nothing() -> None:
    _source.update(a=1, b=1, c=1)
    app = _app("preview")
    _run_all_then_clear_log(app)
    paths_before = {str(p) for p in list_stable_paths_sync(app)}

    _source.update(a=2, b=2, c=2)
    actions = app.update_blocking(
        preview=True,
        component_selector=[_items_path("a"), _items_path("c")],
    )

    assert _planned(actions) == {"a": 2, "c": 2}
    assert sorted(_runs) == ["a", "c", "root"]
    assert _stored() == {"a": 1, "b": 1, "c": 1}
    assert {str(p) for p in list_stable_paths_sync(app)} == paths_before

    # A preview leaves nothing behind: the real update still sees the changes.
    app.update_blocking()
    assert _stored() == {"a": 2, "b": 2, "c": 2}


@pytest.mark.asyncio
async def test_preview_async_composes_with_the_selector() -> None:
    _source.update(a=1, b=1)
    app = _app("preview_async")
    await app.update()

    _source.update(a=2, b=2)
    actions = await app.update(preview=True, component_selector=[_items_path("b")])

    assert _planned(actions) == {"b": 2}
    assert _stored() == {"a": 1, "b": 1}


def test_preview_of_an_unselected_deleted_item_plans_nothing() -> None:
    _source.update(a=1, b=1)
    app = _app("preview_unselected_deleted")
    _run_all_then_clear_log(app)

    del _source["b"]
    actions = app.update_blocking(preview=True, component_selector=[_items_path("a")])

    assert actions == []
    assert _stored() == {"a": 1, "b": 1}


def test_preview_does_not_plan_deletes_of_selected_gone_components() -> None:
    # As in a preview without a selector: the deletes of the components a run
    # no longer mounts are not previewed.
    _source.update(a=1)
    app = _app("preview_selected_deleted")
    _run_all_then_clear_log(app)

    del _source["a"]
    actions = app.update_blocking(preview=True, component_selector=[_items_path("a")])

    assert actions == []
    assert _stored() == {"a": 1}


def test_preview_includes_the_own_changes_of_the_ancestors_that_run() -> None:
    @coco.fn
    async def main() -> None:
        _declare("root_own", _source["a"])
        await coco.mount_each(coco.component_subpath("items"), _item, _entries())

    _source.update(a=1, b=1)
    app = _app("preview_ancestor", main)
    _run_all_then_clear_log(app)

    _source.update(a=2, b=2)
    actions = app.update_blocking(preview=True, component_selector=[_items_path("a")])

    assert _planned(actions) == {"a": 2, "root_own": 2}


# ---------------------------------------------------------------------------
# Stats groups, failures, concurrent updates, shared target states
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stats_groups_split_and_skip_cleanly() -> None:
    group_stats: dict[str, Any] = {}

    @coco.fn
    async def main() -> None:
        with coco.stats_group("first") as first:
            await coco.mount_each(coco.component_subpath("first"), _item, _entries())
        with coco.stats_group("second") as second:
            await coco.mount_each(
                coco.component_subpath("second"), _item, [("b", ("second_b", 1))]
            )
        group_stats["first"] = first
        group_stats["second"] = second

    _source.update(a=1)
    app = _app("stats_groups", main)
    await app.update()
    _runs.clear()

    await app.update(component_selector=[coco.ROOT_PATH / "second" / "b"])

    assert _runs == ["second_b"]
    first_stats = group_stats["first"].stats()
    second_stats = group_stats["second"].stats()
    assert first_stats is None or first_stats.total.num_execution_starts == 0
    assert second_stats is not None
    assert second_stats.total.num_execution_starts == 1


def test_failing_selected_component_leaves_the_rest_and_recovers() -> None:
    failing = [True]

    @coco.fn
    async def flaky(entry: tuple[str, int]) -> None:
        key, value = entry
        _runs.append(key)
        if key == "a" and failing[0]:
            raise ValueError("injected")
        _declare(key, value)

    @coco.fn
    async def main() -> None:
        await coco.mount_each(coco.component_subpath("items"), flaky, _entries())

    failing[0] = False
    _source.update(a=1, b=1)
    app = _app("failure", main)
    _run_all_then_clear_log(app)

    failing[0] = True
    _source.update(a=2, b=2)
    app.update_blocking(component_selector=[_items_path("a")])
    assert _runs == ["a"]
    assert _stored() == {"a": 1, "b": 1}

    failing[0] = False
    app.update_blocking()
    assert _stored() == {"a": 2, "b": 2}


def test_interrupted_selected_commit_is_finished_by_the_next_run() -> None:
    _source.update(a=1, b=1)
    app = _app("interrupted")
    _run_all_then_clear_log(app)

    # The sink fails after pre-commit: "a" keeps its staged, uncommitted state.
    _source.update(a=2, b=2)
    GlobalDictTarget.store.sink_exception = True
    app.update_blocking(component_selector=[_items_path("a")])
    GlobalDictTarget.store.sink_exception = False
    assert _stored() == {"a": 1, "b": 1}

    app.update_blocking(component_selector=[_items_path("a")])
    assert _stored() == {"a": 2, "b": 1}


def test_tombstone_left_by_a_failed_delete_waits_for_a_run_that_covers_it() -> None:
    _source.update(a=1, b=1)
    app = _app("tombstone")
    _run_all_then_clear_log(app)

    # A plain update tombstones "b", and its delete fails.
    del _source["b"]
    GlobalDictTarget.store.sink_exception = True
    app.update_blocking()
    GlobalDictTarget.store.sink_exception = False
    assert _stored() == {"a": 1, "b": 1}

    # This run's GC sweep leaves the tombstone: "b" is not selected.
    app.update_blocking(component_selector=[_items_path("a")])
    assert _stored() == {"a": 1, "b": 1}

    app.update_blocking(component_selector=[_items_path("b")])
    assert _stored() == {"a": 1}


@pytest.mark.asyncio
async def test_parallel_updates_keep_their_own_selectors() -> None:
    _source.update(a=1, b=1, c=1)
    app = _app("parallel")
    await app.update()
    _runs.clear()

    _source.update(a=2, b=2, c=2)
    await asyncio.gather(
        app.update(component_selector=[_items_path("a")]).result(),
        app.update(component_selector=[_items_path("b")]).result(),
    )

    assert _stored() == {"a": 2, "b": 2, "c": 1}


def test_selected_component_takes_over_a_target_state_of_an_unselected_one() -> None:
    owned: dict[str, dict[str, int]] = {"c1": {"x": 1, "c1_own": 1}, "c2": {}}

    @coco.fn
    async def declare_owned(name: str) -> None:
        _runs.append(name)
        for key, value in owned[name].items():
            _declare(key, value)

    @coco.fn
    async def main() -> None:
        for name in owned:
            await coco.mount(coco.component_subpath(name), declare_owned, name)

    app = _app("takeover", main)
    _run_all_then_clear_log(app)

    # The declaration of the selected component wins, as it would in a plain
    # update. The rest of the unselected component stays.
    owned["c2"] = {"x": 2}
    app.update_blocking(component_selector=[coco.ROOT_PATH / "c2"])
    assert _runs == ["c2"]
    assert _stored() == {"x": 2, "c1_own": 1}
    assert common.list_target_state_owners_sync(app)[
        '/@test_target_state/global_dict/"x"'
    ] == (coco.ROOT_PATH / "c2")

    owned["c1"] = {"c1_own": 1}
    app.update_blocking()
    assert _stored() == {"x": 2, "c1_own": 1}


# ---------------------------------------------------------------------------
# Live components
# ---------------------------------------------------------------------------


@coco.fn
async def _live_item(key: str, value: int) -> None:
    _runs.append(key)
    _declare(key, value)


_live_starts: list[str] = []


class _LiveWithEvents:
    """Scans `_source` in `process`, then replays `events` as live updates."""

    events: list[tuple[str, str, int]] = []

    async def process(self) -> None:
        _runs.append("live_process")
        for key, value in _source.items():
            await coco.mount(coco.component_subpath(key), _live_item, key, value)

    async def process_live(self, operator: coco.LiveComponentOperator) -> None:
        _live_starts.append("live")
        await operator.update_full()
        await operator.mark_ready()
        for kind, key, value in self.events:
            subpath = coco.component_subpath(key)
            if kind == "update":
                handle = await operator.update(subpath, _live_item, key, value)
            else:
                handle = await operator.delete(subpath)
            await handle.ready()


@coco.fn
async def _live_main() -> None:
    await coco.mount(coco.component_subpath("live"), _LiveWithEvents)


def test_live_update_full_and_events_honour_the_selector() -> None:
    _LiveWithEvents.events = []
    _source.update(a=1, b=1, c=1, d=1)
    app = _app("live_events", _live_main)
    _run_all_then_clear_log(app)

    _LiveWithEvents.events = [
        ("update", "a", 3),
        ("update", "b", 3),
        ("delete", "c", 0),
        ("delete", "d", 0),
    ]
    _source.update(a=2, b=2)
    app.update_blocking(
        live=True,
        component_selector=[
            coco.ROOT_PATH / "live" / "a",
            coco.ROOT_PATH / "live" / "c",
        ],
    )

    assert sorted(_runs) == ["a", "a", "c", "live_process"]
    assert _stored() == {"a": 3, "b": 1, "d": 1}

    _LiveWithEvents.events = []
    app.update_blocking()
    assert _stored() == {"a": 2, "b": 2, "c": 1, "d": 1}


def test_live_delete_of_an_ancestor_of_a_selected_path_is_skipped() -> None:
    class Live:
        async def process(self) -> None:
            await coco.mount(
                coco.component_subpath("group", "a"), _live_item, "a", _source["a"]
            )
            await coco.mount(
                coco.component_subpath("group", "b"), _live_item, "b", _source["b"]
            )

        async def process_live(self, operator: coco.LiveComponentOperator) -> None:
            await operator.update_full()
            await operator.mark_ready()
            handle = await operator.delete(coco.component_subpath("group"))
            await handle.ready()

    @coco.fn
    async def main() -> None:
        await coco.mount(coco.component_subpath("live"), Live)

    _source.update(a=1, b=1)
    app = _app("live_delete_ancestor", main)
    _run_all_then_clear_log(app)

    app.update_blocking(
        live=True,
        component_selector=[coco.ROOT_PATH / "live" / "group" / "a"],
    )
    assert _stored() == {"a": 1, "b": 1}


def test_unselected_live_component_does_not_start() -> None:
    _LiveWithEvents.events = []
    _source.update(a=1)

    @coco.fn
    async def main() -> None:
        await coco.mount(coco.component_subpath("live"), _LiveWithEvents)
        await coco.mount(coco.component_subpath("other"), _item, ("other", 1))

    app = _app("live_not_started", main)
    _run_all_then_clear_log(app)
    _live_starts.clear()

    _source.update(a=2)
    app.update_blocking(component_selector=[coco.ROOT_PATH / "other"])

    assert _live_starts == []
    assert _stored() == {"a": 1, "other": 1}


@pytest.mark.asyncio
async def test_unselected_live_component_of_a_running_update_keeps_running(
    request: pytest.FixtureRequest,
) -> None:
    # An environment on this test's loop, which the events below belong to.
    env = common.create_test_env(__file__, suffix=request.node.name)
    release = asyncio.Event()
    started = asyncio.Event()

    class Live:
        async def process(self) -> None:
            pass

        async def process_live(self, operator: coco.LiveComponentOperator) -> None:
            await operator.mark_ready()
            started.set()
            await release.wait()
            handle = await operator.update(
                coco.component_subpath("late"), _live_item, "late", 1
            )
            await handle.ready()

    @coco.fn
    async def main() -> None:
        await coco.mount(coco.component_subpath("live"), Live)
        await coco.mount(
            coco.component_subpath("other"), _item, ("other", _source["a"])
        )

    _source.update(a=1)
    app = coco.App(
        coco.AppConfig(name="test_selector_live_keeps_running", environment=env), main
    )
    running = asyncio.ensure_future(app.update(live=True).result())
    await asyncio.wait_for(started.wait(), timeout=10)

    _source.update(a=2)
    await app.update(component_selector=[coco.ROOT_PATH / "other"])
    assert _stored() == {"other": 2}

    release.set()
    await asyncio.wait_for(running, timeout=10)
    assert _stored() == {"other": 2, "late": 1}


def test_selected_live_component_runs_as_without_a_selector() -> None:
    _LiveWithEvents.events = []
    _source.update(a=1, b=1)
    app = _app("live_selected", _live_main)
    _run_all_then_clear_log(app)

    _source.update(a=2, b=2)
    del _source["b"]
    app.update_blocking(component_selector=[coco.ROOT_PATH / "live"])

    assert _stored() == {"a": 2}


def test_nested_live_component_honours_the_selector() -> None:
    nested_starts: list[str] = []

    def nested(name: str) -> type:
        class Nested:
            async def process(self) -> None:
                _runs.append(name)
                _declare(name, _source["a"])

            async def process_live(self, operator: coco.LiveComponentOperator) -> None:
                nested_starts.append(name)
                await operator.update_full()
                await operator.mark_ready()

        return Nested

    class Outer:
        async def process(self) -> None:
            pass

        async def process_live(self, operator: coco.LiveComponentOperator) -> None:
            await operator.update_full()
            for name in ["one", "two"]:
                handle = await operator.update(
                    coco.component_subpath(name), nested(name)
                )
                await handle.ready()
            await operator.mark_ready()

    @coco.fn
    async def main() -> None:
        await coco.mount(coco.component_subpath("outer"), Outer)

    _source.update(a=1)
    app = _app("live_nested", main)
    _run_all_then_clear_log(app)
    nested_starts.clear()

    _source.update(a=2)
    app.update_blocking(component_selector=[coco.ROOT_PATH / "outer" / "one"])

    assert nested_starts == ["one"]
    assert _stored() == {"one": 2, "two": 1}


def test_mount_each_over_a_live_map_honours_the_selector() -> None:
    @coco.fn
    async def declare_entry(value: tuple[str, int]) -> None:
        _runs.append(value[0])
        _declare(*value)

    @coco.fn
    async def produce(live_map: LiveMap[str, tuple[str, int]]) -> None:
        for key, value in _source.items():
            live_map.declare_entry(key, (key, value))

    @coco.fn
    async def main() -> None:
        live_map: LiveMap[str, tuple[str, int]] = await LiveMap.create()
        producer = await coco.mount(
            coco.component_subpath("producer"), produce, live_map
        )
        await producer.ready()
        await coco.mount_each(coco.component_subpath("items"), declare_entry, live_map)

    _source.update(a=1, b=1, c=1)
    app = _app("live_map", main)
    _run_all_then_clear_log(app)

    _source.update(a=2, b=2)
    del _source["c"]
    app.update_blocking(
        component_selector=[coco.ROOT_PATH / "producer", _items_path("a")],
    )

    assert _runs == ["a"]
    assert _stored() == {"a": 2, "b": 1, "c": 1}


def test_preview_of_live_component_composes_with_the_selector() -> None:
    _LiveWithEvents.events = []
    _source.update(a=1, b=1)
    app = _app("live_preview", _live_main)
    _run_all_then_clear_log(app)

    _source.update(a=2, b=2)
    actions = app.update_blocking(
        preview=True,
        component_selector=[coco.ROOT_PATH / "live" / "a"],
    )

    assert _planned(actions) == {"a": 2}
    assert _stored() == {"a": 1, "b": 1}

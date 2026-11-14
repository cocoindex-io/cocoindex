"""Sliced execution: target states, memoization and tree shape changes.

Each test drives an app through selective updates and then checks, where the
contract says so, that one plain update brings it to what plain updates alone
give.
"""

from __future__ import annotations

from typing import Any

import pytest

import cocoindex as coco
from cocoindex._internal.inspect_api import list_stable_paths_sync

from tests import common
from tests.common.target_states import (
    AttachmentDictsTarget,
    DictsTarget,
    GlobalDictTarget,
)

coco_env = common.create_test_env(__file__)

_src: dict[str, Any] = {}
_runs: list[str] = []


@pytest.fixture(autouse=True)
def _reset() -> None:
    GlobalDictTarget.store.clear()
    GlobalDictTarget.store.sink_exception = False
    DictsTarget.store.clear()
    DictsTarget.store.sink_exception = False
    _src.clear()
    _runs.clear()


def _stored() -> dict[str, Any]:
    return {k: v.data for k, v in GlobalDictTarget.store.data.items()}


def _app(name: str, main: Any, *args: Any) -> coco.App[Any, Any]:
    return coco.App(
        coco.AppConfig(name=f"selector_{name}", environment=coco_env), main, *args
    )


def _paths(app: coco.App[Any, Any]) -> set[str]:
    return {str(p) for p in list_stable_paths_sync(app)}


# ---------------------------------------------------------------------------
# Mount shape: what a plain update rejects, a selective one must not accept
# ---------------------------------------------------------------------------


@coco.fn
async def _leaf(key: str, value: Any) -> None:
    _runs.append(key)
    coco.declare_target_state(GlobalDictTarget.target_state(key, value))


def test_duplicate_mount_is_rejected_with_or_without_a_selector() -> None:
    @coco.fn
    async def main() -> None:
        await coco.mount(coco.component_subpath("x"), _leaf, "x", 1)
        await coco.mount(coco.component_subpath("x"), _leaf, "x", 2)
        await coco.mount(coco.component_subpath("y"), _leaf, "y", 1)

    app = _app("duplicate", main)
    with pytest.raises(Exception):
        app.update_blocking()
    # "x" is unselected: is the duplicate still an error?
    with pytest.raises(Exception):
        app.update_blocking(component_selector=[coco.ROOT_PATH / "y"])


def test_component_and_its_own_subpath_both_mounted_is_rejected_either_way() -> None:
    @coco.fn
    async def main() -> None:
        await coco.mount(coco.component_subpath("x"), _leaf, "x", 1)
        await coco.mount(coco.component_subpath("x", "y"), _leaf, "xy", 1)

    app = _app("component_and_dir", main)
    with pytest.raises(Exception):
        app.update_blocking()
    with pytest.raises(Exception):
        app.update_blocking(component_selector=[coco.ROOT_PATH / "x" / "y"])


# ---------------------------------------------------------------------------
# Tombstones of a failed delete, and the component mounted again
# ---------------------------------------------------------------------------


@coco.fn
async def _mount_src_main() -> None:
    for key, value in _src.items():
        await coco.mount(coco.component_subpath("items", key), _leaf, key, value)


@pytest.mark.parametrize("selected_first", [False, True])
def test_remounted_after_a_failed_delete_keeps_its_target_states(
    selected_first: bool,
) -> None:
    _src.update(a=1, b=1)
    app = _app(f"remount_after_failed_delete_{selected_first}", _mount_src_main)
    app.update_blocking()

    # The delete of "a" fails in the sink: its tombstone stays.
    del _src["a"]
    GlobalDictTarget.store.sink_exception = True
    app.update_blocking()
    GlobalDictTarget.store.sink_exception = False
    assert _stored() == {"a": 1, "b": 1}

    # "a" comes back, with a new value.
    _src["a"] = 2
    if selected_first:
        # A run that does not cover "a": "a" is not mounted, its tombstone stays.
        app.update_blocking(component_selector=[coco.ROOT_PATH / "items" / "b"])
        assert _stored() == {"a": 1, "b": 1}
    _runs.clear()
    app.update_blocking(component_selector=[coco.ROOT_PATH / "items" / "a"])
    assert _runs == ["a"]
    assert _stored() == {"a": 2, "b": 1}
    owners = common.list_target_state_owners_sync(app)
    assert owners['/@test_target_state/global_dict/"a"'] == (
        coco.ROOT_PATH / "items" / "a"
    )

    # And a plain update agrees.
    app.update_blocking()
    assert _stored() == {"a": 2, "b": 1}
    app.update_blocking()
    assert _stored() == {"a": 2, "b": 1}


# ---------------------------------------------------------------------------
# Containers: mount_target, child rows, provider generations
# ---------------------------------------------------------------------------


@coco.fn(memo=True)
async def _row(entry: tuple[str, int], provider: Any) -> None:
    key, value = entry
    _runs.append(key)
    coco.declare_target_state(provider.target_state(key, value))


@coco.fn
async def _container_main() -> None:
    provider = await coco.mount_target(DictsTarget.dict_target("t"))
    await coco.mount_each(
        coco.component_subpath("rows"),
        _row,
        [(k, (k, v)) for k, v in _src.items()],
        provider,
    )


def _rows() -> dict[str, Any]:
    return {k: v.data for k, v in DictsTarget.store.data.get("t", {}).items()}


def _row_path(key: str) -> coco.StablePath:
    path: coco.StablePath = coco.ROOT_PATH / "rows" / key
    return path


def test_container_dropped_while_its_rows_are_unselected() -> None:
    declare = {"on": True}

    @coco.fn
    async def main() -> None:
        if not declare["on"]:
            return
        await _container_main()

    _src.update(a=1, b=1)
    app = _app("container_dropped", main)
    app.update_blocking()
    assert _rows() == {"a": 1, "b": 1}

    declare["on"] = False
    # The root runs and no longer declares the container (a use_mount child
    # of the root through mount_target), while the rows are unselected.
    app.update_blocking(component_selector=[_row_path("a")])
    after_selective = (dict(DictsTarget.store.data), _paths(app))

    app.update_blocking()
    assert DictsTarget.store.data == {}, after_selective
    assert common.list_target_state_owners_sync(app) == {}
    assert _paths(app) == {"/"}


def test_first_ever_update_is_selective_and_creates_the_container() -> None:
    _src.update(a=1, b=1)
    app = _app("container_first_selective", _container_main)
    app.update_blocking(component_selector=[_row_path("a")])
    assert _rows() == {"a": 1}
    app.update_blocking()
    assert _rows() == {"a": 1, "b": 1}


@coco.fn(memo=True)
async def _memo_container_parent(items: dict[str, int]) -> None:
    _runs.append("parent")
    provider = await coco.mount_target(DictsTarget.dict_target("t"))
    await coco.mount_each(
        coco.component_subpath("rows"),
        _row,
        [(k, (k, v)) for k, v in items.items()],
        provider,
    )


@coco.fn
async def _memo_parent_main() -> None:
    await coco.mount(coco.component_subpath("p"), _memo_container_parent, dict(_src))


def test_selected_row_under_a_memoized_container_parent() -> None:
    _src.update(a=1, b=1)
    app = _app("memo_container_parent", _memo_parent_main)
    app.update_blocking()

    # Nothing changed: the parent memo-hits and stands in for the row.
    _runs.clear()
    app.update_blocking(component_selector=[coco.ROOT_PATH / "p" / "rows" / "a"])
    assert _runs == []

    # Both rows change; only "a" is selected.
    _src.update(a=2, b=2)
    _runs.clear()
    app.update_blocking(component_selector=[coco.ROOT_PATH / "p" / "rows" / "a"])
    assert sorted(_runs) == ["a", "parent"]
    assert _rows() == {"a": 2, "b": 1}

    # The parent stored no memo: the next plain update reaches "b".
    _runs.clear()
    app.update_blocking()
    assert _rows() == {"a": 2, "b": 2}
    assert "b" in _runs


# ---------------------------------------------------------------------------
# Memo and dependencies
# ---------------------------------------------------------------------------

_CTX = coco.ContextKey[int]("selector_ctx", detect_change=True)


@coco.fn(memo=True)
async def _ctx_leaf(key: str) -> None:
    _runs.append(key)
    coco.declare_target_state(
        GlobalDictTarget.target_state(key, coco.use_context(_CTX))
    )


@coco.fn
async def _ctx_main() -> None:
    for key in _src:
        await coco.mount(coco.component_subpath(key), _ctx_leaf, key)


def test_tracked_context_change_reaches_an_unselected_memo_later() -> None:
    env = common.create_test_env(__file__, suffix="ctx")
    env.context_provider.provide(_CTX, 1)
    _src.update(a=None, b=None)
    app = coco.App(coco.AppConfig(name="selector_ctx", environment=env), _ctx_main)
    app.update_blocking()
    assert _stored() == {"a": 1, "b": 1}

    env.context_provider.provide(_CTX, 2)
    _runs.clear()
    app.update_blocking(component_selector=[coco.ROOT_PATH / "a"])
    assert _runs == ["a"]
    assert _stored() == {"a": 2, "b": 1}

    _runs.clear()
    app.update_blocking()
    assert _runs == ["b"]
    assert _stored() == {"a": 2, "b": 2}


@coco.fn(memo=True)
async def _double(x: int) -> int:
    _runs.append(f"double({x})")
    return x * 2


@coco.fn
async def _map_main() -> None:
    doubled = await coco.map(_double, sorted(_src.values()))
    coco.declare_target_state(GlobalDictTarget.target_state("sum", sum(doubled)))
    for key, value in _src.items():
        await coco.mount(coco.component_subpath(key), _leaf, key, value)


def test_map_and_function_memo_in_the_root_under_a_selector() -> None:
    _src.update(a=1, b=2)
    app = _app("map_root", _map_main)
    app.update_blocking()
    assert _stored() == {"sum": 6, "a": 1, "b": 2}

    _src.update(b=3)
    _runs.clear()
    app.update_blocking(component_selector=[coco.ROOT_PATH / "a"])
    # The root runs: its function memos are its own (it is always run).
    assert "double(3)" in _runs and "b" not in _runs
    assert _stored() == {"sum": 8, "a": 1, "b": 2}

    _runs.clear()
    app.update_blocking()
    assert _stored() == {"sum": 8, "a": 1, "b": 3}
    # The memo of double(1) survived the selective run of the root.
    assert "double(1)" not in _runs


@coco.fn(memo=True)
async def _um_inner(items: dict[str, int]) -> int:
    _runs.append("inner")
    for key, value in items.items():
        await coco.mount(coco.component_subpath(key), _leaf, key, value)
    return len(items)


@coco.fn(memo=True)
async def _um_outer(items: dict[str, int]) -> int:
    _runs.append("outer")
    return await coco.use_mount(  # type: ignore[no-any-return]
        coco.component_subpath("inner"), _um_inner, items
    )


@coco.fn
async def _um_chain_main() -> None:
    n = await coco.use_mount(coco.component_subpath("outer"), _um_outer, dict(_src))
    coco.declare_target_state(GlobalDictTarget.target_state("n", n))


def test_memoized_use_mount_chain_under_a_selector() -> None:
    _src.update(a=1, b=1)
    app = _app("um_chain", _um_chain_main)
    app.update_blocking()
    sel = [coco.ROOT_PATH / "outer" / "inner" / "a"]

    _src.update(a=2, b=2, c=1)
    _runs.clear()
    app.update_blocking(component_selector=sel)
    assert sorted(_runs) == ["a", "inner", "outer"]
    assert _stored() == {"n": 3, "a": 2, "b": 1}

    # Neither link of the chain may memo-hit now: "b" and "c" are behind.
    _runs.clear()
    app.update_blocking()
    assert _stored() == {"n": 3, "a": 2, "b": 2, "c": 1}

    _runs.clear()
    app.update_blocking()
    assert _runs == []


@coco.fn
async def _state_leaf(key: str, value: int) -> None:
    state = coco.use_state("seen", 0)
    state.value = state.value + 1
    _runs.append(key)
    coco.declare_target_state(GlobalDictTarget.target_state(key, (value, state.value)))


@coco.fn
async def _state_main() -> None:
    for key, value in _src.items():
        await coco.mount(coco.component_subpath(key), _state_leaf, key, value)


def test_use_state_of_an_unselected_component_is_not_advanced() -> None:
    _src.update(a=1, b=1)
    app = _app("use_state", _state_main)
    app.update_blocking()
    app.update_blocking(component_selector=[coco.ROOT_PATH / "a"])
    app.update_blocking(component_selector=[coco.ROOT_PATH / "a"])
    assert _stored() == {"a": (1, 3), "b": (1, 1)}
    # A preview does not advance it either.
    app.update_blocking(component_selector=[coco.ROOT_PATH / "b"], preview=True)
    app.update_blocking(component_selector=[coco.ROOT_PATH / "b"])
    assert _stored() == {"a": (1, 3), "b": (1, 2)}


# ---------------------------------------------------------------------------
# Tree shape
# ---------------------------------------------------------------------------


@coco.fn
async def _deep(depth: int, max_depth: int) -> None:
    _runs.append(str(depth))
    coco.declare_target_state(GlobalDictTarget.target_state(f"d{depth}", _src["v"]))
    if depth < max_depth:
        await coco.mount(coco.component_subpath("n"), _deep, depth + 1, max_depth)


def test_very_deep_path_selects_only_the_deepest_and_runs_its_ancestors() -> None:
    depth = 60

    @coco.fn
    async def main() -> None:
        await coco.mount(coco.component_subpath("n"), _deep, 1, depth)

    _src["v"] = 1
    app = _app("deep", main)
    app.update_blocking()

    _src["v"] = 2
    deepest = coco.ROOT_PATH
    for _ in range(depth):
        deepest = deepest / "n"
    _runs.clear()
    app.update_blocking(component_selector=[deepest])
    assert _runs == [str(i) for i in range(1, depth + 1)]
    # Ancestors run, so they redeclare with the new value too.
    assert all(v == 2 for v in _stored().values())


def test_selected_delete_with_nested_children_deletes_the_subtree() -> None:
    @coco.fn
    async def group(name: str, keys: list[str]) -> None:
        for key in keys:
            await coco.mount(coco.component_subpath(key), _leaf, f"{name}/{key}", 1)

    @coco.fn
    async def main() -> None:
        for name, keys in _src.items():
            await coco.mount(coco.component_subpath(name), group, name, keys)

    _src.update(g=["a", "b"], h=["a"])
    app = _app("nested_delete", main)
    app.update_blocking()
    del _src["g"]
    del _src["h"]
    app.update_blocking(component_selector=[coco.ROOT_PATH / "g"])
    assert _stored() == {"h/a": 1}
    assert not any(p.startswith('/"g"') for p in _paths(app)), _paths(app)
    assert '/"h"/"a"' in _paths(app)
    app.update_blocking()
    assert _stored() == {}
    assert _paths(app) == {"/"}


def test_component_becomes_a_directory_with_the_same_child_keys() -> None:
    """`x` was a component mounting `x/a`; now the root mounts `x/a` itself."""
    shape = {"nested": True}

    @coco.fn
    async def x_component() -> None:
        await coco.mount(coco.component_subpath("a"), _leaf, "a", _src["a"])
        coco.declare_target_state(GlobalDictTarget.target_state("x", 1))

    @coco.fn
    async def main() -> None:
        if shape["nested"]:
            await coco.mount(coco.component_subpath("x"), x_component)
        else:
            await coco.mount(coco.component_subpath("x", "a"), _leaf, "a", _src["a"])

    for selective in (False, True):
        GlobalDictTarget.store.clear()
        shape["nested"] = True
        _src["a"] = 1
        app = _app(f"component_to_dir_{selective}", main)
        app.update_blocking()
        assert _stored() == {"a": 1, "x": 1}

        shape["nested"] = False
        _src["a"] = 2
        if selective:
            app.update_blocking(component_selector=[coco.ROOT_PATH / "x" / "a"])
            # "x" is unselected: its own target state stays.
            assert _stored() == {"a": 2, "x": 1}
        app.update_blocking()
        assert _stored() == {"a": 2}, f"selective={selective}"
        app.update_blocking()
        assert _stored() == {"a": 2}, f"selective={selective}, second plain"
        assert common.list_target_state_owners_sync(app) == {
            '/@test_target_state/global_dict/"a"': coco.ROOT_PATH / "x" / "a"
        }

        # And back: "x" a component again.
        shape["nested"] = True
        _src["a"] = 3
        if selective:
            app.update_blocking(component_selector=[coco.ROOT_PATH / "x"])
        app.update_blocking()
        assert _stored() == {"a": 3, "x": 1}, f"selective={selective}, back"


def test_selected_child_removed_after_a_selective_demotion_is_deleted() -> None:
    """`x` was a component mounting `x/k` and `x/m`; a selective run of `x/k`
    makes `x` a directory; then `x/k` goes, and a selective run of `x/k` must
    delete it, as it would under a directory that was always one."""
    nested = [True]
    keys = ["k", "m"]

    @coco.fn
    async def x_component() -> None:
        for key in keys:
            await coco.mount(coco.component_subpath(key), _leaf, key, 1)

    @coco.fn
    async def main() -> None:
        if nested[0]:
            await coco.mount(coco.component_subpath("x"), x_component)
        else:
            for key in keys:
                await coco.mount(coco.component_subpath("x", key), _leaf, key, 1)

    app = _app("selective_demotion_then_delete", main)
    app.update_blocking()
    assert _stored() == {"k": 1, "m": 1}

    selected = [coco.ROOT_PATH / "x" / "k"]
    nested[0] = False
    app.update_blocking(component_selector=selected)
    keys.remove("k")
    app.update_blocking(component_selector=selected)
    assert _stored() == {"m": 1}


def test_selected_gone_child_is_deleted_when_its_component_parent_becomes_a_directory() -> (
    None
):
    nested = [True]
    keys = ["k", "b"]

    @coco.fn
    async def x_component() -> None:
        for key in keys:
            await coco.mount(coco.component_subpath(key), _leaf, key, 1)

    @coco.fn
    async def main() -> None:
        if nested[0]:
            await coco.mount(coco.component_subpath("x"), x_component)
        else:
            for key in keys:
                await coco.mount(coco.component_subpath("x", key), _leaf, key, 1)

    app = _app("component_parent_becomes_directory", main)
    app.update_blocking()
    assert _stored() == {"k": 1, "b": 1}

    # One change: `x` becomes a directory and `b` goes. Selecting `x/b` must
    # delete it, as selecting a gone item under a directory does.
    nested[0] = False
    keys.remove("b")
    app.update_blocking(component_selector=[coco.ROOT_PATH / "x" / "b"])
    assert _stored() == {"k": 1}


# ---------------------------------------------------------------------------
# Ownership transfer from an unselected component
# ---------------------------------------------------------------------------


@coco.fn(memo=True)
async def _memo_leaf(key: str, value: int) -> None:
    _runs.append(key)
    coco.declare_target_state(GlobalDictTarget.target_state(key, value))


def test_owner_taken_over_while_unselected_redeclares_when_mounted_again() -> None:
    where = {"group": "items"}

    @coco.fn
    async def main() -> None:
        await coco.mount(
            coco.component_subpath(where["group"], "c"), _memo_leaf, "c", 5
        )

    app = _app("takeover_then_memo_hit", main)
    app.update_blocking()
    assert _stored() == {"c": 5}

    # The item moves under another group. A selective run of that group runs
    # the new component, which takes "c" over; the old one is unselected, so it
    # is neither run nor deleted.
    where["group"] = "other"
    app.update_blocking(component_selector=[coco.ROOT_PATH / "other"])
    assert _stored() == {"c": 5}

    # The item moves back. A plain update must give what a plain update gives:
    # "c" declared by items/c.
    where["group"] = "items"
    app.update_blocking()
    assert _stored() == {"c": 5}
    assert common.list_target_state_owners_sync(app) == {
        '/@test_target_state/global_dict/"c"': coco.ROOT_PATH / "items" / "c"
    }


@coco.fn(memo=True)
async def _memo_group(name: str, keys: tuple[str, ...]) -> None:
    # Declares nothing of its own: only its child owns a target state.
    _runs.append(name)
    for key in keys:
        await coco.mount(coco.component_subpath(key), _memo_leaf, key, 5)


def test_owner_taken_over_under_a_memoized_parent_redeclares() -> None:
    where = {"group": "items"}

    @coco.fn
    async def main() -> None:
        await coco.mount(
            coco.component_subpath(where["group"]), _memo_group, "grp", ("c",)
        )

    app = _app("takeover_under_memo_parent", main)
    app.update_blocking()
    assert _stored() == {"c": 5}

    where["group"] = "other"
    app.update_blocking(component_selector=[coco.ROOT_PATH / "other"])
    where["group"] = "items"
    app.update_blocking()
    assert _stored() == {"c": 5}


# ---------------------------------------------------------------------------
# Pre-existing (reproduces with plain updates only): a failed cascaded delete
# ---------------------------------------------------------------------------


def _tolerant_global_dict() -> None:
    store = GlobalDictTarget.store

    def sink(ctx: Any, actions: Any, /) -> None:
        if store.sink_exception:
            raise ValueError("injected sink exception")
        for key, value in actions:
            if coco.is_non_existence(value):
                store.data.pop(key, None)
            else:
                store.data[key] = value

    store._action_sink = coco.TargetActionSink.from_fn(sink)


def test_failed_delete_under_a_deleted_parent_is_retried() -> None:
    _tolerant_global_dict()
    mounted = {"m": True}

    @coco.fn
    async def leaf() -> None:
        coco.declare_target_state(GlobalDictTarget.target_state("x", 1))

    @coco.fn
    async def mid() -> None:
        # Declares nothing of its own: its delete has nothing to sink.
        await coco.mount(coco.component_subpath("l"), leaf)

    @coco.fn
    async def main() -> None:
        if mounted["m"]:
            await coco.mount(coco.component_subpath("m"), mid)

    try:
        app = _app("orphan_tombstone", main)
        app.update_blocking()
        assert _stored() == {"x": 1}

        mounted["m"] = False
        GlobalDictTarget.store.sink_exception = True
        app.update_blocking()
        GlobalDictTarget.store.sink_exception = False

        app.update_blocking()
        app.update_blocking()
        assert _stored() == {}
        assert common.list_target_state_owners_sync(app) == {}
    finally:
        GlobalDictTarget.store._action_sink = coco.TargetActionSink.from_fn(
            GlobalDictTarget.store._sink
        )


# ---------------------------------------------------------------------------
# A memoized parent that a selective run leaves partly unrun
# ---------------------------------------------------------------------------


def _memo_parent_app(name: str) -> tuple[coco.App[Any, Any], dict[str, bool]]:
    """`p` is memoized and mounts `a` and `b` while `_src` has them; a child
    whose key is in `fail` raises."""
    fail: dict[str, bool] = {}

    @coco.fn
    async def child(key: str) -> None:
        _runs.append(key)
        if fail.get(key):
            raise ValueError(f"injected failure in {key}")
        coco.declare_target_state(GlobalDictTarget.target_state(key, _src[key]))

    @coco.fn(memo=True)
    async def parent() -> None:
        _runs.append("p")
        for key in ("a", "b"):
            if key in _src:
                await coco.mount(coco.component_subpath(key), child, key)

    @coco.fn
    async def main() -> None:
        await coco.mount(coco.component_subpath("p"), parent)

    return _app(name, main), fail


def _update_tolerating_failure(app: coco.App[Any, Any], **kwargs: Any) -> None:
    try:
        app.update_blocking(**kwargs)
    except Exception:
        pass


def test_a_selective_run_with_a_failed_child_still_drops_the_parent_memo() -> None:
    app, fail = _memo_parent_app("memo_failed_child")
    _src.update(a=1, b=1)
    app.update_blocking()
    assert _stored() == {"a": 1, "b": 1}

    del _src["b"]
    fail["a"] = True
    _update_tolerating_failure(
        app,
        full_reprocess=True,
        component_selector=[coco.ROOT_PATH / "p" / "a"],
    )
    # `b` is left out, so it keeps its target state for now.
    assert _stored() == {"a": 1, "b": 1}

    fail["a"] = False
    _runs.clear()
    app.update_blocking()
    assert "p" in _runs
    assert _stored() == {"a": 1}


def test_a_tombstone_the_selector_keeps_is_no_failed_delete() -> None:
    store = GlobalDictTarget.store
    fail_delete = {"b": False}

    def sink(ctx: Any, actions: Any, /) -> None:
        for key, value in actions:
            if coco.is_non_existence(value) and fail_delete.get(key):
                raise ValueError(f"injected delete failure for {key}")
        for key, value in actions:
            if coco.is_non_existence(value):
                store.data.pop(key, None)
            else:
                store.data[key] = value

    store._action_sink = coco.TargetActionSink.from_fn(sink)
    try:
        app, _ = _memo_parent_app("memo_kept_tombstone")
        _src.update(a=1, b=1)
        app.update_blocking()

        # `b`'s delete fails, so its tombstone stays under `p`.
        del _src["b"]
        fail_delete["b"] = True
        _update_tolerating_failure(app, full_reprocess=True)
        assert _stored() == {"a": 1, "b": 1}
        fail_delete["b"] = False

        # A selective run that leaves the tombstone alone succeeds.
        app.update_blocking(
            full_reprocess=True, component_selector=[coco.ROOT_PATH / "p" / "a"]
        )
        assert _stored() == {"a": 1, "b": 1}

        # The next plain update runs `p` and deletes `b`.
        _runs.clear()
        app.update_blocking()
        assert "p" in _runs
        assert _stored() == {"a": 1}
    finally:
        store._action_sink = coco.TargetActionSink.from_fn(store._sink)


def test_a_failed_child_delete_under_full_reprocess_is_retried() -> None:
    store = GlobalDictTarget.store
    fail_delete = {"b": False}

    def sink(ctx: Any, actions: Any, /) -> None:
        for key, value in actions:
            if coco.is_non_existence(value) and fail_delete.get(key):
                raise ValueError(f"injected delete failure for {key}")
        for key, value in actions:
            if coco.is_non_existence(value):
                store.data.pop(key, None)
            else:
                store.data[key] = value

    store._action_sink = coco.TargetActionSink.from_fn(sink)
    try:
        app, _ = _memo_parent_app("memo_failed_delete_full_reprocess")
        _src.update(a=1, b=1)
        app.update_blocking()

        del _src["b"]
        fail_delete["b"] = True
        _update_tolerating_failure(app, full_reprocess=True)
        fail_delete["b"] = False

        # Same memo key as the stored memo: only a dropped memo makes `p`
        # run, and only a run of `p` sweeps `b`'s tombstone again.
        app.update_blocking()
        assert _stored() == {"a": 1}
    finally:
        store._action_sink = coco.TargetActionSink.from_fn(store._sink)


# ---------------------------------------------------------------------------
# Attachments declared by children, some unselected
# ---------------------------------------------------------------------------


@coco.fn(memo=True)
async def _attach_child(entry: tuple[str, int], provider: Any) -> None:
    key, value = entry
    _runs.append(key)
    coco.declare_target_state(provider.attachment("items").target_state(key, value))


@coco.fn
async def _attachment_main() -> None:
    provider = await coco.use_mount(
        coco.component_subpath("setup"), AttachmentDictsTarget.declare_dict_target, "D"
    )
    await coco.mount_each(
        coco.component_subpath("kids"),
        _attach_child,
        [(k, (k, v)) for k, v in _src.items()],
        provider,
    )


def _attachments() -> dict[str, Any]:
    data = AttachmentDictsTarget.store.attachment_data.get("D", {}).get("items", {})
    return {k: v.data for k, v in data.items()}


def test_attachments_of_unselected_children_are_kept_and_converge() -> None:
    AttachmentDictsTarget.store.clear()
    _src.update(a=1, b=1, c=1)
    app = _app("attachments", _attachment_main)
    app.update_blocking()
    assert _attachments() == {"a": 1, "b": 1, "c": 1}

    _src.update(a=2, b=2)
    del _src["c"]
    _runs.clear()
    app.update_blocking(component_selector=[coco.ROOT_PATH / "kids" / "a"])
    assert _runs == ["a"]
    assert _attachments() == {"a": 2, "b": 1, "c": 1}

    app.update_blocking(component_selector=[coco.ROOT_PATH / "kids" / "c"])
    assert _attachments() == {"a": 2, "b": 1}

    app.update_blocking()
    assert _attachments() == {"a": 2, "b": 2}

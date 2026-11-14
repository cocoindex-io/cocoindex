"""A failed delete below a deleted component is retried by a later update.

When a component is no longer mounted, its delete cascades: the delete of each
child component runs from its parent's delete, and the child's tombstone is
keyed under that parent. A child delete that fails keeps its tombstone, so the
parent must keep its own tombstone too: otherwise nothing finds the child's
tombstone again, and the child's target states stay for good.
"""

from __future__ import annotations

import asyncio
from typing import Any, Collection, Iterator

import cocoindex as coco
import cocoindex.inspect as coco_inspect
import pytest

from tests import common
from tests.common.target_states import DictDataWithPrev, GlobalDictTarget

coco_env = common.create_test_env(__file__)

# Keys whose actions make the sink fail. A sink call can batch the actions of
# several components, so a failure can also fail other keys in the same call.
_failing_keys: set[str] = set()


@pytest.fixture(autouse=True)
def _failing_sink() -> Iterator[None]:
    store = GlobalDictTarget.store
    store.clear()
    _failing_keys.clear()

    def sink(
        ctx: coco.ContextProvider,
        actions: Collection[tuple[str, DictDataWithPrev | coco.NonExistenceType]],
        /,
    ) -> None:
        if any(key in _failing_keys for key, _ in actions):
            raise ValueError("injected sink exception")
        store._sink(ctx, actions)

    store._action_sink = coco.TargetActionSink.from_fn(sink)
    try:
        yield
    finally:
        store._action_sink = coco.TargetActionSink.from_fn(store._sink)
        _failing_keys.clear()


def _stored() -> dict[str, Any]:
    return {k: v.data for k, v in GlobalDictTarget.store.data.items()}


def _assert_all_gone(app: coco.App[Any, Any]) -> None:
    assert _stored() == {}
    assert common.list_target_state_owners_sync(app) == {}
    assert coco_inspect.list_stable_paths_sync(app) == [coco.ROOT_PATH]


##################################################################################
# A parent that declares nothing itself, with one child that declares `x`.

_mounted: dict[str, int | None] = {}


@coco.fn
async def _leaf(value: int) -> None:
    coco.declare_target_state(GlobalDictTarget.target_state("x", value))


@coco.fn
async def _mid(leaf_value: int | None) -> None:
    if leaf_value is not None:
        await coco.mount(coco.component_subpath("l"), _leaf, leaf_value)


@coco.fn
async def _mid_main() -> None:
    if "m" in _mounted:
        await coco.mount(coco.component_subpath("m"), _mid, _mounted["m"])


def _mid_app(name: str) -> coco.App[Any, Any]:
    _mounted.clear()
    _mounted["m"] = 1
    app = coco.App(coco.AppConfig(name=name, environment=coco_env), _mid_main)
    app.update_blocking()
    assert _stored() == {"x": 1}
    return app


def _fail_delete_of_m(app: coco.App[Any, Any]) -> None:
    """Unmount `m` while the delete of `x` fails."""
    del _mounted["m"]
    _failing_keys.add("x")
    try:
        app.update_blocking()
    finally:
        _failing_keys.clear()
    assert _stored() == {"x": 1}
    assert common.list_target_state_owners_sync(app) == {
        '/@test_target_state/global_dict/"x"': coco.ROOT_PATH / "m" / "l",
    }


def test_failed_delete_under_a_deleted_parent_is_retried() -> None:
    app = _mid_app("failed_delete_under_deleted_parent")
    _fail_delete_of_m(app)

    app.update_blocking()
    _assert_all_gone(app)


def test_failed_delete_is_retried_until_it_succeeds() -> None:
    app = _mid_app("failed_delete_retried_until_success")
    _fail_delete_of_m(app)

    # The retry fails too: the delete stays owed.
    _failing_keys.add("x")
    try:
        app.update_blocking()
    finally:
        _failing_keys.clear()
    assert _stored() == {"x": 1}

    app.update_blocking()
    _assert_all_gone(app)
    # Nothing is left to retry.
    GlobalDictTarget.store.metrics.clear()
    app.update_blocking()
    assert GlobalDictTarget.store.metrics.collect() == {}


def test_remount_before_retry_keeps_the_remounted_child() -> None:
    app = _mid_app("remount_before_retry_keeps_child")
    _fail_delete_of_m(app)

    # `m` comes back, with `l` declaring a new value, before any retry. The
    # owed deletes must not remove what the re-mounted components declare.
    _mounted["m"] = 2
    app.update_blocking()
    assert _stored() == {"x": 2}
    assert common.list_target_state_owners_sync(app) == {
        '/@test_target_state/global_dict/"x"': coco.ROOT_PATH / "m" / "l",
    }
    app.update_blocking()
    assert _stored() == {"x": 2}

    del _mounted["m"]
    app.update_blocking()
    _assert_all_gone(app)


def test_remount_before_retry_without_the_child_deletes_it() -> None:
    app = _mid_app("remount_before_retry_drops_child")
    _fail_delete_of_m(app)

    # `m` comes back without `l`: the re-mounted `m` retries the delete of `l`.
    _mounted["m"] = None
    app.update_blocking()
    assert _stored() == {}
    assert common.list_target_state_owners_sync(app) == {}

    del _mounted["m"]
    app.update_blocking()
    _assert_all_gone(app)


@pytest.mark.asyncio
async def test_concurrent_updates_retry_a_failed_delete_once() -> None:
    app = await asyncio.to_thread(_mid_app, "concurrent_updates_retry")
    await asyncio.to_thread(_fail_delete_of_m, app)

    GlobalDictTarget.store.metrics.clear()
    await asyncio.gather(app.update(), app.update())
    assert GlobalDictTarget.store.metrics.collect()["delete"] == 1
    assert _stored() == {}
    assert await common.list_target_state_owners(app) == {}


def test_preview_after_a_failed_delete_changes_nothing() -> None:
    app = _mid_app("preview_after_failed_delete")
    _fail_delete_of_m(app)

    GlobalDictTarget.store.metrics.clear()
    app.update_blocking(preview=True)
    assert _stored() == {"x": 1}
    assert GlobalDictTarget.store.metrics.collect() == {}

    app.update_blocking()
    _assert_all_gone(app)


def test_drop_after_a_failed_delete() -> None:
    app = _mid_app("drop_after_failed_delete")
    _fail_delete_of_m(app)

    app.drop_blocking()
    assert _stored() == {}
    assert coco_inspect.list_stable_paths_sync(app) == []
    assert common.list_target_state_owners_sync(app) == {}


def test_failed_drop_of_a_nested_child_is_retried() -> None:
    app = _mid_app("failed_drop_of_nested_child")

    _failing_keys.add("x")
    try:
        with pytest.raises(Exception, match="injected sink exception"):
            app.drop_blocking()
    finally:
        _failing_keys.clear()
    assert _stored() == {"x": 1}

    app.drop_blocking()
    assert _stored() == {}
    assert coco_inspect.list_stable_paths_sync(app) == []
    assert common.list_target_state_owners_sync(app) == {}


##################################################################################
# A chain c0 → c0/c1 → c0/c1/c2 → c0/c1/c2/c3; level `i` declares `k{i}`.

_CHAIN_DEPTH = 4
_chain_mounted: list[bool] = [True]


@coco.fn
async def _chain_level(level: int) -> None:
    coco.declare_target_state(GlobalDictTarget.target_state(f"k{level}", level))
    if level + 1 < _CHAIN_DEPTH:
        await coco.mount(
            coco.component_subpath(f"c{level + 1}"), _chain_level, level + 1
        )


@coco.fn
async def _chain_main() -> None:
    if _chain_mounted[0]:
        await coco.mount(coco.component_subpath("c0"), _chain_level, 0)


@pytest.mark.parametrize("failing_level", range(_CHAIN_DEPTH))
def test_failed_delete_at_any_depth_is_retried(failing_level: int) -> None:
    _chain_mounted[0] = True
    app = coco.App(
        coco.AppConfig(
            name=f"failed_delete_at_depth_{failing_level}", environment=coco_env
        ),
        _chain_main,
    )
    app.update_blocking()
    assert _stored() == {f"k{i}": i for i in range(_CHAIN_DEPTH)}

    _chain_mounted[0] = False
    failing_key = f"k{failing_level}"
    _failing_keys.add(failing_key)
    try:
        app.update_blocking()
    finally:
        _failing_keys.clear()
    assert _stored()[failing_key] == failing_level

    app.update_blocking()
    _assert_all_gone(app)


@pytest.mark.parametrize("failing_level", range(_CHAIN_DEPTH))
def test_failed_delete_at_any_depth_then_drop(failing_level: int) -> None:
    _chain_mounted[0] = True
    app = coco.App(
        coco.AppConfig(
            name=f"failed_delete_at_depth_{failing_level}_then_drop",
            environment=coco_env,
        ),
        _chain_main,
    )
    app.update_blocking()

    _chain_mounted[0] = False
    _failing_keys.add(f"k{failing_level}")
    try:
        app.update_blocking()
    finally:
        _failing_keys.clear()

    app.drop_blocking()
    assert _stored() == {}
    assert coco_inspect.list_stable_paths_sync(app) == []
    assert common.list_target_state_owners_sync(app) == {}


##################################################################################
# A memoized parent whose own run deletes a child it no longer mounts.

_memo_runs: list[str] = []


@coco.fn(memo=True)
async def _memo_parent(names: tuple[str, ...]) -> None:
    _memo_runs.append("parent")
    for name in names:
        await coco.mount(coco.component_subpath(name), _named_leaf, name)


@coco.fn
async def _named_leaf(name: str) -> None:
    coco.declare_target_state(GlobalDictTarget.target_state(name, 1))


@coco.fn(memo=True)
async def _memo_grandparent(names: tuple[str, ...]) -> None:
    await coco.mount(coco.component_subpath("p"), _memo_parent, names)


_memo_names: list[tuple[str, ...]] = []


@coco.fn
async def _memo_main() -> None:
    await coco.mount(coco.component_subpath("g"), _memo_grandparent, _memo_names[0])


def test_memoized_parent_retries_a_failed_child_delete() -> None:
    _memo_runs.clear()
    _memo_names[:] = [("a", "b")]
    app = coco.App(
        coco.AppConfig(
            name="memoized_parent_retries_child_delete", environment=coco_env
        ),
        _memo_main,
    )
    app.update_blocking()
    assert _stored() == {"a": 1, "b": 1}

    # The parent runs with new arguments and no longer mounts `b`; the delete
    # of `b` fails.
    _memo_names[:] = [("a",)]
    _failing_keys.add("b")
    try:
        app.update_blocking()
    finally:
        _failing_keys.clear()
    assert _stored() == {"a": 1, "b": 1}

    # Same arguments: the parent stored no memo from the run that left the
    # delete owed (and nor did the grandparent), so it runs and retries it.
    _memo_runs.clear()
    app.update_blocking()
    assert _memo_runs == ["parent"]
    assert _stored() == {"a": 1}
    assert common.list_target_state_owners_sync(app) == {
        '/@test_target_state/global_dict/"a"': coco.ROOT_PATH / "g" / "p" / "a",
    }

    # Now the memo holds.
    _memo_runs.clear()
    app.update_blocking()
    assert _memo_runs == []
    assert _stored() == {"a": 1}


##################################################################################
# `operator.delete` of a live component's child whose own child declares `y`.

_live_children: dict[str, int] = {}
_live_delete: list[str] = []


@coco.fn
async def _live_child(value: int) -> None:
    await coco.mount(coco.component_subpath("g"), _named_value, "y", value)


@coco.fn
async def _named_value(name: str, value: int) -> None:
    coco.declare_target_state(GlobalDictTarget.target_state(name, value))


class _LiveParent:
    async def process(self) -> None:
        for key, value in _live_children.items():
            await coco.mount(coco.component_subpath(key), _live_child, value)

    async def process_live(self, operator: coco.LiveComponentOperator) -> None:
        await operator.update_full()
        for key in _live_delete:
            handle = await operator.delete(coco.component_subpath(key))
            await handle.ready()
        await operator.mark_ready()


@coco.fn
async def _live_main() -> None:
    await coco.mount(coco.component_subpath("live"), _LiveParent)


def test_failed_live_delete_of_a_nested_child_is_retried() -> None:
    _live_children.clear()
    _live_delete.clear()
    _live_children["c"] = 1
    app = coco.App(
        coco.AppConfig(name="failed_live_delete_nested", environment=coco_env),
        _live_main,
    )
    app.update_blocking(live=True)
    assert _stored() == {"y": 1}

    # `operator.delete` of `c` cascades to `c/g`, whose delete fails.
    _live_delete.append("c")
    _failing_keys.add("y")
    try:
        app.update_blocking(live=True)
    finally:
        _failing_keys.clear()
    assert _stored() == {"y": 1}

    # The source no longer has `c`: the live component's next full update
    # retries the delete.
    _live_delete.clear()
    del _live_children["c"]
    app.update_blocking(live=True)
    assert _stored() == {}
    assert common.list_target_state_owners_sync(app) == {}

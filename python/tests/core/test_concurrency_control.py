"""Tests for concurrency control (max_inflight_components).

The limit is a pool of slots. A parent keeps its slot while it waits on its
children and lends it to one child at a time, so a hierarchy never deadlocks
and the components in flight stay bounded; see
docs/src/content/docs/advanced_topics/concurrency_control.mdx.
"""

from __future__ import annotations

import asyncio
import datetime
import os
import threading
import time
from collections.abc import AsyncIterator, Awaitable, Callable

import pytest

import cocoindex as coco
from cocoindex._internal import core as _core

from tests.common import create_test_env
from tests.common.target_states import GlobalDictTarget

coco_env = create_test_env(__file__)


# ── Concurrency tracking ────────────────────────────────────────────────


class ConcurrencyTracker:
    """Thread-safe tracker for peak concurrent execution."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._current = 0
        self._peak = 0
        self._total = 0

    def enter(self) -> None:
        with self._lock:
            self._current += 1
            self._total += 1
            self._peak = max(self._peak, self._current)

    def exit(self) -> None:
        with self._lock:
            self._current -= 1

    @property
    def peak(self) -> int:
        with self._lock:
            return self._peak

    @property
    def total(self) -> int:
        with self._lock:
            return self._total

    def reset(self) -> None:
        with self._lock:
            self._current = 0
            self._peak = 0
            self._total = 0


_tracker = ConcurrencyTracker()
_SLEEP = 0.1  # 100ms — enough to guarantee overlap between concurrent children


# ── Component functions ─────────────────────────────────────────────────


@coco.fn
def _slow_leaf() -> None:
    """Leaf component that sleeps to create overlapping execution windows."""
    _tracker.enter()
    try:
        time.sleep(_SLEEP)
    finally:
        _tracker.exit()


@coco.fn
def _noop() -> None:
    pass


@coco.fn
async def _child_mounts_grandchild() -> None:
    """Child that mounts a grandchild — tests permit release on first child mount."""
    await coco.mount(coco.component_subpath("gc"), _noop)


# ── Root functions ───────────────────────────────────────────────────────


async def _main_flat(count: int) -> None:
    """Mount *count* independent slow children."""
    for i in range(count):
        await coco.mount(coco.component_subpath(str(i)), _slow_leaf)


async def _main_nested() -> None:
    """Root → child → grandchild nesting."""
    for i in range(4):
        await coco.mount(coco.component_subpath(str(i)), _child_mounts_grandchild)


# ── Test 1: Quota enforcement ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_quota_limits_concurrency() -> None:
    """With max_inflight_components=2, at most 2 leaf components run at once."""
    _tracker.reset()
    app = coco.App(
        coco.AppConfig(
            name="test_quota_limits_concurrency",
            environment=coco_env,
            max_inflight_components=2,
        ),
        _main_flat,
        6,
    )
    await app.update()
    assert _tracker.total == 6
    assert _tracker.peak <= 2


def test_quota_one_serializes() -> None:
    """With max_inflight_components=1, components execute one at a time."""
    _tracker.reset()
    app = coco.App(
        coco.AppConfig(
            name="test_quota_one_serializes",
            environment=coco_env,
            max_inflight_components=1,
        ),
        _main_flat,
        4,
    )
    app.update_blocking()
    assert _tracker.total == 4
    assert _tracker.peak == 1


# ── Test 2: Deadlock prevention ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_deadlock_prevention() -> None:
    """Nested mount (parent → child → grandchild) with quota=2 completes without deadlock."""
    app = coco.App(
        coco.AppConfig(
            name="test_deadlock_prevention",
            environment=coco_env,
            max_inflight_components=2,
        ),
        _main_nested,
    )
    # If permit release on first child mount is broken, this deadlocks (test timeout fires).
    await app.update()


def test_deadlock_prevention_quota_one() -> None:
    """Even with quota=1, nested mount completes because parent releases permit."""
    app = coco.App(
        coco.AppConfig(
            name="test_deadlock_prevention_quota_one",
            environment=coco_env,
            max_inflight_components=1,
        ),
        _main_nested,
    )
    app.update_blocking()


# ── Test 3: Default limit (1024) ──────────────────────────────────────────


def test_default_limit() -> None:
    """Without max_inflight_components, the default limit of 1024 applies."""
    _tracker.reset()
    app = coco.App(
        coco.AppConfig(name="test_default_limit", environment=coco_env),
        _main_flat,
        6,
    )
    app.update_blocking()
    assert _tracker.total == 6
    # Default limit is 1024, far above 6, so all 6 children overlap → peak should exceed 2
    assert _tracker.peak > 2


# ── Test 4: Env var fallback ─────────────────────────────────────────────


def test_env_var_fallback() -> None:
    """COCOINDEX_MAX_INFLIGHT_COMPONENTS env var is used when AppConfig omits it."""
    _tracker.reset()
    original = os.environ.get("COCOINDEX_MAX_INFLIGHT_COMPONENTS")
    try:
        os.environ["COCOINDEX_MAX_INFLIGHT_COMPONENTS"] = "2"
        app = coco.App(
            coco.AppConfig(name="test_env_var_fallback", environment=coco_env),
            _main_flat,
            6,
        )
        app.update_blocking()
    finally:
        if original is None:
            os.environ.pop("COCOINDEX_MAX_INFLIGHT_COMPONENTS", None)
        else:
            os.environ["COCOINDEX_MAX_INFLIGHT_COMPONENTS"] = original

    assert _tracker.total == 6
    assert _tracker.peak <= 2


# ── Bounded hierarchies ─────────────────────────────────────────────────
#
# Async bodies run on the environment's loop, so the helpers below share
# state through plain locks and polling rather than asyncio primitives.


async def _hold_until_full(tracker: ConcurrencyTracker, size: int) -> None:
    """Keep a body in flight until ``size`` bodies are in flight together (or
    a second passes). The first ``size`` bodies wait for each other, every
    later one passes straight through, so the peak is deterministic: exactly
    ``size`` when the engine admits that many, less when it admits fewer."""
    deadline = time.monotonic() + 1.0
    while tracker.peak < size and time.monotonic() < deadline:
        await asyncio.sleep(0.001)


@coco.fn
async def _leaf_until_full(size: int) -> None:
    _tracker.enter()
    try:
        await _hold_until_full(_tracker, size)
    finally:
        _tracker.exit()


async def _main_gather(count: int, size: int) -> None:
    """A gathered fan-out: every child is asked for at once."""
    await asyncio.gather(
        *(
            coco.use_mount(coco.component_subpath(str(i)), _leaf_until_full, size)
            for i in range(count)
        )
    )


async def _main_map(count: int, size: int) -> None:
    """The same fan-out through ``coco.map``."""

    async def one(i: int) -> None:
        await coco.use_mount(coco.component_subpath(str(i)), _leaf_until_full, size)

    await coco.map(one, range(count))


_FAN_OUT_POOL = 32
_FAN_OUT = 3000


@pytest.mark.parametrize("main", [_main_gather, _main_map], ids=["gather", "map"])
def test_fan_out_peaks_at_the_pool_size(
    main: Callable[[int, int], Awaitable[None]],
) -> None:
    """Several thousand children asked for at once run at most pool-size at a
    time: one on the parent's lent slot, the rest on pool slots."""
    _tracker.reset()
    app = coco.App(
        coco.AppConfig(
            name=f"test_fan_out_{main.__name__}",
            environment=coco_env,
            max_inflight_components=_FAN_OUT_POOL,
        ),
        main,
        _FAN_OUT,
        _FAN_OUT_POOL,
    )
    app.update_blocking()
    assert _tracker.total == _FAN_OUT
    assert _tracker.peak == _FAN_OUT_POOL


_mount_progress = {"mounted": 0, "mounted_at_peak": -1}


@coco.fn
async def _leaf_records_mount_progress(size: int) -> None:
    _tracker.enter()
    try:
        if _tracker.peak >= size and _mount_progress["mounted_at_peak"] < 0:
            _mount_progress["mounted_at_peak"] = _mount_progress["mounted"]
        await _hold_until_full(_tracker, size)
    finally:
        _tracker.exit()


async def _main_sequential_mounts(count: int, size: int) -> None:
    for i in range(count):
        await coco.mount(
            coco.component_subpath(str(i)), _leaf_records_mount_progress, size
        )
        _mount_progress["mounted"] += 1


def test_sequential_mounts_wait_for_a_slot() -> None:
    """A parent mounting children one by one waits in ``mount`` while the pool
    is empty, instead of running ahead through its loop."""
    _tracker.reset()
    _mount_progress.update(mounted=0, mounted_at_peak=-1)
    pool = 4
    app = coco.App(
        coco.AppConfig(
            name="test_sequential_mounts_wait",
            environment=coco_env,
            max_inflight_components=pool,
        ),
        _main_sequential_mounts,
        200,
        pool,
    )
    app.update_blocking()
    assert _tracker.total == 200
    assert _tracker.peak == pool
    # When the pool filled up, the loop had not got past its first `pool`
    # mounts: the next one was waiting for a slot.
    assert 0 <= _mount_progress["mounted_at_peak"] <= pool


_nodes = ConcurrencyTracker()
_leaves = ConcurrencyTracker()


@coco.fn
async def _tree_node(depth: int, fan_out: int) -> None:
    _nodes.enter()
    try:
        if depth == 0:
            _leaves.enter()
            try:
                await asyncio.sleep(0.002)
            finally:
                _leaves.exit()
            return
        await asyncio.gather(
            *(
                coco.use_mount(
                    coco.component_subpath(str(i)), _tree_node, depth - 1, fan_out
                )
                for i in range(fan_out)
            )
        )
    finally:
        _nodes.exit()


async def _main_tree(depth: int, fan_out: int) -> None:
    await coco.use_mount(coco.component_subpath("tree"), _tree_node, depth, fan_out)


def test_deep_tree_completes_under_a_pool_of_two() -> None:
    """A binary tree six levels deep completes with two slots: leaves run two
    at a time, and the nodes in flight stay within pool size times depth."""
    _nodes.reset()
    _leaves.reset()
    depth, fan_out, pool = 5, 2, 2
    app = coco.App(
        coco.AppConfig(
            name="test_deep_tree_pool_two",
            environment=coco_env,
            max_inflight_components=pool,
        ),
        _main_tree,
        depth,
        fan_out,
    )
    app.update_blocking()
    assert _nodes.total == 2 ** (depth + 1) - 1
    assert _leaves.peak <= pool
    assert _nodes.peak <= pool * (depth + 1)


def test_deep_chain_completes_under_a_pool_of_one() -> None:
    """Every level of a chain of mounts runs on its parent's lent slot."""
    _nodes.reset()
    _leaves.reset()
    depth = 20
    app = coco.App(
        coco.AppConfig(
            name="test_deep_chain_pool_one",
            environment=coco_env,
            max_inflight_components=1,
        ),
        _main_tree,
        depth,
        1,
    )
    app.update_blocking()
    assert _nodes.total == depth + 1
    assert _nodes.peak == depth + 1
    assert _leaves.peak == 1


# ── Batched calls ───────────────────────────────────────────────────────


_batch_sizes: list[int] = []


@coco.fn.as_async(batching=True)
async def _batched_double(inputs: list[int]) -> list[int]:
    _batch_sizes.append(len(inputs))
    await asyncio.sleep(0.02)
    return [x * 2 for x in inputs]


@coco.fn
async def _calls_batched(i: int) -> int:
    _tracker.enter()
    try:
        return await _batched_double(i)
    finally:
        _tracker.exit()


async def _main_batched(count: int) -> list[int]:
    return list(
        await asyncio.gather(
            *(
                coco.use_mount(coco.component_subpath(str(i)), _calls_batched, i)
                for i in range(count)
            )
        )
    )


def test_batched_call_under_a_full_pool() -> None:
    """A component awaiting a batched call keeps its slot: the batcher drains
    the callers the pool admits, and no more than pool-size are ever inside
    the call."""
    _tracker.reset()
    _batch_sizes.clear()
    pool = 2
    app = coco.App(
        coco.AppConfig(
            name="test_batched_call_full_pool",
            environment=coco_env,
            max_inflight_components=pool,
        ),
        _main_batched,
        6,
    )
    assert app.update_blocking() == [0, 2, 4, 6, 8, 10]
    assert _tracker.total == 6
    assert _tracker.peak <= pool
    assert max(_batch_sizes) <= pool


# ── A parent blocked on an application-level primitive ─────────────────
#
# The shape of an application admission gate: a file component mounts a quick
# first child (its body), waits on an asyncio.Semaphore that admits a few files
# at a time, then fans out its chunks. Files waiting on the gate hold their
# slots, so the gate must not need any free slot to make progress: a file's
# first chunk runs on the file's own lent slot.


_gate: dict[str, asyncio.Semaphore] = {}


@coco.fn
async def _quick_leaf() -> None:
    pass


@coco.fn
async def _chunk_leaf(i: int, j: int) -> None:
    _tracker.enter()
    try:
        await asyncio.sleep(0.002)
    finally:
        _tracker.exit()


async def _chunks(i: int) -> None:
    await asyncio.gather(
        *(
            coco.use_mount(coco.component_subpath("chunk", j), _chunk_leaf, i, j)
            for j in range(4)
        )
    )


@coco.fn
async def _file_gate_after_first_child(i: int) -> None:
    await coco.use_mount(coco.component_subpath("body"), _quick_leaf)
    async with _gate["gate"]:
        await _chunks(i)


@coco.fn
async def _file_gate_before_first_child(i: int) -> None:
    async with _gate["gate"]:
        await coco.use_mount(coco.component_subpath("body"), _quick_leaf)
        await _chunks(i)


async def _main_gated(
    file_fn: Callable[[int], Awaitable[None]], count: int, gate_size: int
) -> None:
    # Created here so it binds to the loop the file components run on.
    _gate["gate"] = asyncio.Semaphore(gate_size)
    await asyncio.gather(
        *(
            coco.use_mount(coco.component_subpath("file", i), file_fn, i)
            for i in range(count)
        )
    )


@pytest.mark.parametrize(
    "file_fn",
    [_file_gate_after_first_child, _file_gate_before_first_child],
    ids=["gate_after_first_child", "gate_before_first_child"],
)
@pytest.mark.asyncio
async def test_parent_blocked_on_an_asyncio_primitive_keeps_lending(
    file_fn: Callable[[int], Awaitable[None]],
) -> None:
    _tracker.reset()
    pool, files, gate_size = 8, 40, 2
    app = coco.App(
        coco.AppConfig(
            name=f"test_gated_{file_fn.__name__}",
            environment=coco_env,
            max_inflight_components=pool,
        ),
        _main_gated,
        file_fn,
        files,
        gate_size,
    )
    # A stall would hang forever; bound it.
    await asyncio.wait_for(app.update(), timeout=60)
    assert _tracker.total == files * 4
    assert _tracker.peak <= pool


# ── Live components ─────────────────────────────────────────────────────


class _LiveMap:
    """A live map that yields ``(key, key)`` for each key, then is ready."""

    def __init__(self, keys: list[str]) -> None:
        self._keys = keys

    def __aiter__(self) -> AsyncIterator[tuple[str, str]]:
        return self._aiter()

    async def _aiter(self) -> AsyncIterator[tuple[str, str]]:
        for key in self._keys:
            yield (key, key)

    async def watch(self, subscriber: coco.LiveMapSubscriber[str, str]) -> None:
        await subscriber.update_all()
        await subscriber.mark_ready()


@coco.fn
async def _live_item(key: str) -> None:
    _tracker.enter()
    try:
        await asyncio.sleep(0.002)
    finally:
        _tracker.exit()


async def _main_live_map(keys: list[str]) -> None:
    await coco.mount_each(_live_item, _LiveMap(keys))  # type: ignore[call-overload]


@pytest.mark.parametrize("live", [False, True], ids=["catch_up", "live"])
def test_mount_each_over_a_live_map_under_a_pool_of_one(live: bool) -> None:
    """The live map's item updates run on the slot of the component that
    mounted it, one at a time."""
    _tracker.reset()
    keys = [f"k{i}" for i in range(12)]
    app = coco.App(
        coco.AppConfig(
            name=f"test_live_map_pool_one_{live}",
            environment=coco_env,
            max_inflight_components=1,
        ),
        _main_live_map,
        keys,
    )
    app.update_blocking(live=live)
    assert _tracker.total == len(keys)
    assert _tracker.peak == 1


_refresh_calls = {"calls": 0}


async def _refreshed() -> None:
    _refresh_calls["calls"] += 1
    _tracker.enter()
    try:
        await asyncio.sleep(0.002)
    finally:
        _tracker.exit()


_AutoRefresh = coco.auto_refresh(
    _refreshed, interval=datetime.timedelta(milliseconds=5)
)


async def _main_auto_refresh() -> None:
    await coco.mount(coco.component_subpath("ar"), _AutoRefresh)


def test_auto_refresh_under_a_pool_of_one_catch_up() -> None:
    """The first cycle runs on the mounting component's lent slot."""
    _tracker.reset()
    _refresh_calls["calls"] = 0
    app = coco.App(
        coco.AppConfig(
            name="test_auto_refresh_pool_one_catch_up",
            environment=coco_env,
            max_inflight_components=1,
        ),
        _main_auto_refresh,
    )
    app.update_blocking()
    assert _refresh_calls["calls"] == 1
    assert _tracker.peak == 1


@pytest.mark.asyncio
async def test_auto_refresh_under_a_pool_of_one_live() -> None:
    """Cycles after the mounting component finished take pool slots."""
    _tracker.reset()
    _refresh_calls["calls"] = 0
    _core.reset_global_cancellation()
    app = coco.App(
        coco.AppConfig(
            name="test_auto_refresh_pool_one_live",
            environment=coco_env,
            max_inflight_components=1,
        ),
        _main_auto_refresh,
    )
    handle = app.update(live=True)
    result_task = asyncio.create_task(handle.result())
    try:
        deadline = time.monotonic() + 5.0
        while _refresh_calls["calls"] < 3 and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        assert _refresh_calls["calls"] >= 3
        assert _tracker.peak == 1
        _core.cancel_all()
        try:
            await asyncio.wait_for(result_task, timeout=5.0)
        except Exception:
            pass
    finally:
        if not result_task.done():
            result_task.cancel()
        _core.reset_global_cancellation()


# ── Delete-mode processing ──────────────────────────────────────────────


_subtree_count = {"n": 0}


@coco.fn
async def _declares(key: str) -> None:
    coco.declare_target_state(GlobalDictTarget.target_state(key, 1))


@coco.fn
async def _subtree(i: int, n_children: int) -> None:
    await asyncio.gather(
        *(
            coco.use_mount(coco.component_subpath(str(j)), _declares, f"{i}/{j}")
            for j in range(n_children)
        )
    )


async def _main_subtrees(n_children: int) -> None:
    for i in range(_subtree_count["n"]):
        await coco.mount(coco.component_subpath(str(i)), _subtree, i, n_children)


@pytest.mark.asyncio
async def test_delete_mode_processing_under_a_pool_of_one() -> None:
    """Orphan cleanup during an update and ``App.drop()`` delete whole
    subtrees with one slot: child deletes run on their parent's lent slot."""
    GlobalDictTarget.store.clear()
    app = coco.App(
        coco.AppConfig(
            name="test_delete_mode_pool_one",
            environment=coco_env,
            max_inflight_components=1,
        ),
        _main_subtrees,
        3,
    )
    _subtree_count["n"] = 5
    await asyncio.wait_for(app.update(), timeout=60)
    assert len(GlobalDictTarget.store.data) == 15

    # The subtrees disappear from the root: cleaned up during its commit.
    _subtree_count["n"] = 0
    await asyncio.wait_for(app.update(), timeout=60)
    assert GlobalDictTarget.store.data == {}

    _subtree_count["n"] = 5
    await asyncio.wait_for(app.update(), timeout=60)
    assert len(GlobalDictTarget.store.data) == 15
    await asyncio.wait_for(app.drop(), timeout=60)
    assert GlobalDictTarget.store.data == {}

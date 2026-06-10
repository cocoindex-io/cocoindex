"""End-to-end tests for LiveMap (producer target + LiveMapView consumer bridge)."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest

import cocoindex as coco
from cocoindex.resources.live_map import LiveMap

from tests import common
from tests.common.target_states import AsyncGlobalDictTarget, GlobalDictTarget


# A value that carries its own key, so the consumer (which only receives the value from
# mount_each) can key the output target. Intentionally an unhashable, non-frozen dataclass:
# LiveMap requires only `==` on V, never hashing/fingerprinting.
@dataclass
class Val:
    key: str
    payload: Any


# How many times each entry was processed by the consumer (isolates LiveMap's `==` gate).
_call_counts: dict[str, int] = {}


@coco.fn
async def process_entry(v: Val) -> None:
    _call_counts[v.key] = _call_counts.get(v.key, 0) + 1
    coco.declare_target_state(GlobalDictTarget.target_state(v.key, v.payload))


@coco.fn
async def process_entry_b(v: Val) -> None:
    coco.declare_target_state(AsyncGlobalDictTarget.target_state(v.key, v.payload))


@coco.fn
async def produce(lm: LiveMap[str, Val], desired: dict[str, Any]) -> None:
    """One-shot producer: declare the given entries (owned by this component)."""
    for k, payload in desired.items():
        lm.declare_entry(k, Val(k, payload))


# --- Live producer driven by module-level control (so no unpicklable args are passed) ---

_live_desired: dict[str, dict[str, Any]] = {}
_live_signals: dict[str, asyncio.Queue[None]] = {}


class MapProducer:
    """A live producer: each `update_full` re-declares the current desired set into the map."""

    def __init__(self, lm: LiveMap[str, Val], name: str) -> None:
        self._lm = lm
        self._name = name

    async def process(self) -> None:
        for k, payload in list(_live_desired[self._name].items()):
            self._lm.declare_entry(k, Val(k, payload))

    async def process_live(self, operator: Any) -> None:
        await operator.update_full()
        await operator.mark_ready()
        signal = _live_signals[self._name]
        while True:
            await signal.get()
            await operator.update_full()


# --- Polling helpers (modeled on test_localfs_live.py) ---


async def _wait_for_target_keys(
    expected_keys: set[str], *, timeout: float = 20.0, poll_interval: float = 0.05
) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if set(GlobalDictTarget.store.data.keys()) == expected_keys:
            return
        await asyncio.sleep(poll_interval)
    raise AssertionError(
        f"Timed out. Expected {expected_keys}, got {set(GlobalDictTarget.store.data.keys())}"
    )


async def _wait_for_value(
    key: str, expected: Any, *, timeout: float = 20.0, poll_interval: float = 0.05
) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        entry = GlobalDictTarget.store.data.get(key)
        if entry is not None and entry.data == expected:
            return
        await asyncio.sleep(poll_interval)
    raise AssertionError(
        f"Timed out waiting for {key!r}=={expected!r}; got {GlobalDictTarget.store.data.get(key)}"
    )


def _data() -> dict[str, Any]:
    return {k: v.data for k, v in GlobalDictTarget.store.data.items()}


# ============================================================================
# One-shot, ordered (deterministic)
# ============================================================================


def _run_oneshot(name: str, app_main: Any) -> None:
    env = common.create_test_env(__file__, suffix=name)
    app = coco.App(
        coco.AppConfig(name=f"test_live_map_{name}", environment=env), app_main
    )
    app.update_blocking()


def test_basic_produce_consume() -> None:
    GlobalDictTarget.store.clear()
    _call_counts.clear()

    @coco.fn
    async def app_main() -> None:
        lm: LiveMap[str, Val] = await LiveMap.create()
        handle = await coco.mount(produce, lm, {"a": "1", "b": "2", "c": "3"})
        await handle.ready()
        await coco.mount_each(process_entry, lm)

    _run_oneshot("basic", app_main)
    assert _data() == {"a": "1", "b": "2", "c": "3"}
    assert _call_counts == {"a": 1, "b": 1, "c": 1}


def test_multiple_producers_one_map() -> None:
    GlobalDictTarget.store.clear()
    _call_counts.clear()

    @coco.fn
    async def app_main() -> None:
        lm: LiveMap[str, Val] = await LiveMap.create()
        h1 = await coco.mount(
            coco.component_subpath("p1"), produce, lm, {"a": "1", "b": "2"}
        )
        h2 = await coco.mount(
            coco.component_subpath("p2"), produce, lm, {"c": "3", "d": "4"}
        )
        await h1.ready()
        await h2.ready()
        await coco.mount_each(process_entry, lm)

    _run_oneshot("multi_producer", app_main)
    assert _data() == {"a": "1", "b": "2", "c": "3", "d": "4"}


def test_multiple_livemaps_isolated() -> None:
    GlobalDictTarget.store.clear()
    AsyncGlobalDictTarget.store.clear()

    @coco.fn
    async def app_main() -> None:
        lm1: LiveMap[str, Val] = await LiveMap.create()
        lm2: LiveMap[str, Val] = await LiveMap.create()
        h1 = await coco.mount(
            coco.component_subpath("p1"), produce, lm1, {"a": "1", "b": "2"}
        )
        h2 = await coco.mount(
            coco.component_subpath("p2"), produce, lm2, {"c": "3", "d": "4"}
        )
        await h1.ready()
        await h2.ready()
        await coco.mount_each(coco.component_subpath("c1"), process_entry, lm1)
        await coco.mount_each(coco.component_subpath("c2"), process_entry_b, lm2)

    _run_oneshot("isolation", app_main)
    assert {k: v.data for k, v in GlobalDictTarget.store.data.items()} == {
        "a": "1",
        "b": "2",
    }
    assert {k: v.data for k, v in AsyncGlobalDictTarget.store.data.items()} == {
        "c": "3",
        "d": "4",
    }


def test_unhashable_value() -> None:
    GlobalDictTarget.store.clear()

    @coco.fn
    async def app_main() -> None:
        lm: LiveMap[str, Val] = await LiveMap.create()
        # payloads are unhashable lists — proves no hashability/fingerprintability needed.
        handle = await coco.mount(produce, lm, {"a": [1, 2], "b": [3]})
        await handle.ready()
        await coco.mount_each(process_entry, lm)

    _run_oneshot("unhashable", app_main)
    assert _data() == {"a": [1, 2], "b": [3]}


def test_aiter_snapshot() -> None:
    GlobalDictTarget.store.clear()
    collected: dict[str, Any] = {}

    @coco.fn
    async def app_main() -> None:
        lm: LiveMap[str, Val] = await LiveMap.create()
        handle = await coco.mount(produce, lm, {"a": "1", "b": "2"})
        await handle.ready()
        async for key, value in lm:
            collected[key] = value.payload

    _run_oneshot("aiter", app_main)
    assert collected == {"a": "1", "b": "2"}


def test_restart_refill_and_cross_run_delete() -> None:
    GlobalDictTarget.store.clear()
    _call_counts.clear()
    env = common.create_test_env(__file__, suffix="restart")

    def run(desired: dict[str, Any]) -> None:
        @coco.fn
        async def app_main() -> None:
            lm: LiveMap[str, Val] = await LiveMap.create()
            handle = await coco.mount(produce, lm, desired)
            await handle.ready()
            await coco.mount_each(process_entry, lm)

        app = coco.App(
            coco.AppConfig(name="test_live_map_restart", environment=env), app_main
        )
        app.update_blocking()

    # Run 1.
    run({"a": "1", "b": "2"})
    assert _data() == {"a": "1", "b": "2"}

    # Run 2 (same env + app name = restart): fresh LiveMap (new UUID) refills the empty dict;
    # `a` unchanged still reappears, `b` dropped is removed, `c` added.
    run({"a": "1", "c": "3"})
    assert _data() == {"a": "1", "c": "3"}


def test_restart_same_inputs_refill() -> None:
    # The hard case: identical producer inputs across a restart. The in-memory dict is gone, so
    # the producer MUST re-run to refill it — even though its inputs are unchanged. If it were
    # allowed to memoize and skip, the fresh map would stay empty and the consumer would tear
    # down its (now-unbacked) entries.
    GlobalDictTarget.store.clear()
    env = common.create_test_env(__file__, suffix="restart_same")

    def run() -> None:
        @coco.fn
        async def app_main() -> None:
            lm: LiveMap[str, Val] = await LiveMap.create()
            handle = await coco.mount(produce, lm, {"a": "1", "b": "2"})
            await handle.ready()
            await coco.mount_each(process_entry, lm)

        app = coco.App(
            coco.AppConfig(name="test_live_map_restart_same", environment=env), app_main
        )
        app.update_blocking()

    run()
    assert _data() == {"a": "1", "b": "2"}
    run()  # same inputs, fresh LiveMap: must still refill, not empty out.
    assert _data() == {"a": "1", "b": "2"}


def test_single_watcher_raises() -> None:
    env = common.create_test_env(__file__, suffix="single_watcher")
    result: list[bool] = []

    class _FakeSub:
        async def update_all(self) -> None: ...
        async def mark_ready(self) -> None: ...
        async def update(self, key: Any, value: Any) -> Any:
            raise AssertionError("unreached")

        async def delete(self, key: Any) -> Any:
            raise AssertionError("unreached")

    @coco.fn
    async def app_main() -> None:
        lm: LiveMap[str, Val] = await LiveMap.create()
        task = asyncio.create_task(lm.watch(_FakeSub()))  # type: ignore[arg-type]
        await asyncio.sleep(0)  # let the first watch arm its queue
        try:
            await lm.watch(_FakeSub())  # type: ignore[arg-type]
            result.append(False)
        except RuntimeError:
            result.append(True)
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    app = coco.App(
        coco.AppConfig(name="test_live_map_single_watcher", environment=env), app_main
    )
    app.update_blocking()
    assert result == [True]


# ============================================================================
# Live mode
# ============================================================================


def _make_live_app(name: str) -> coco.App[[], None]:
    env = common.create_test_env(__file__, suffix=name)

    @coco.fn
    async def app_main() -> None:
        lm: LiveMap[str, Val] = await LiveMap.create()
        await coco.mount_each(process_entry, lm)
        await coco.mount(coco.component_subpath("producer"), MapProducer, lm, name)

    return coco.App(
        coco.AppConfig(name=f"test_live_map_{name}", environment=env), app_main
    )


async def _run_live(name: str, body: Any) -> None:
    GlobalDictTarget.store.clear()
    _call_counts.clear()
    _live_signals[name] = asyncio.Queue()
    app = _make_live_app(name)
    handle = app.update(live=True)
    task = asyncio.create_task(handle.result())
    await asyncio.sleep(0.3)
    try:
        await body()
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_live_initial_scan_then_add() -> None:
    name = "live_add"
    _live_desired[name] = {"a": "1", "b": "2"}

    async def body() -> None:
        await _wait_for_target_keys({"a", "b"})
        _live_desired[name]["c"] = "3"
        _live_signals[name].put_nowait(None)
        await _wait_for_target_keys({"a", "b", "c"})
        assert GlobalDictTarget.store.data["c"].data == "3"

    await _run_live(name, body)


@pytest.mark.asyncio
async def test_live_update_value() -> None:
    name = "live_update"
    _live_desired[name] = {"a": "1"}

    async def body() -> None:
        await _wait_for_value("a", "1")
        assert _call_counts.get("a") == 1
        _live_desired[name]["a"] = "2"
        _live_signals[name].put_nowait(None)
        await _wait_for_value("a", "2")
        assert _call_counts.get("a") == 2

    await _run_live(name, body)


@pytest.mark.asyncio
async def test_live_change_gating() -> None:
    name = "live_gating"
    _live_desired[name] = {"a": "1"}

    async def body() -> None:
        await _wait_for_value("a", "1")
        assert _call_counts.get("a") == 1
        # Re-declare the SAME value: the == gate must suppress the emit (no re-trigger).
        _live_signals[name].put_nowait(None)
        await asyncio.sleep(0.4)
        assert _call_counts.get("a") == 1
        # Change the value: must re-trigger.
        _live_desired[name]["a"] = "2"
        _live_signals[name].put_nowait(None)
        await _wait_for_value("a", "2")
        assert _call_counts.get("a") == 2

    await _run_live(name, body)


@pytest.mark.asyncio
async def test_live_delete_via_ownership() -> None:
    name = "live_delete"
    _live_desired[name] = {"a": "1", "b": "2"}

    async def body() -> None:
        await _wait_for_target_keys({"a", "b"})
        _live_desired[name] = {"a": "1"}  # drop b
        _live_signals[name].put_nowait(None)
        await _wait_for_target_keys({"a"})
        assert "b" not in GlobalDictTarget.store.data

    await _run_live(name, body)

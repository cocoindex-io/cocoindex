"""Sliced execution in live mode: live events, and a running live update beside a slice."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest

import cocoindex as coco
from cocoindex._internal.live_component import LiveMapSubscriber

from tests import common
from tests.common.target_states import GlobalDictTarget

_source: dict[str, int] = {}
_runs: list[str] = []


@pytest.fixture(autouse=True)
def _reset() -> None:
    GlobalDictTarget.store.clear()
    GlobalDictTarget.store.sink_exception = False
    _source.clear()
    _runs.clear()


def _stored() -> dict[str, Any]:
    return {k: v.data for k, v in GlobalDictTarget.store.data.items()}


def _declare(key: str, value: Any) -> None:
    coco.declare_target_state(GlobalDictTarget.target_state(str(key), value))


@coco.fn
async def _live_item(key: str, value: int) -> None:
    _runs.append(key)
    _declare(key, value)


def _env(request: pytest.FixtureRequest) -> coco.Environment:
    return common.create_test_env(__file__, suffix=request.node.name)


def _app(env: coco.Environment, name: str, main: Any) -> coco.App[Any, Any]:
    return coco.App(coco.AppConfig(name=f"selector_live_{name}", environment=env), main)


class _QueueLive:
    """`process` mounts `_source`; `process_live` replays events from a queue."""

    queue: asyncio.Queue[tuple[str, str, int]]
    started: asyncio.Event
    cancelled: list[str] = []

    async def process(self) -> None:
        _runs.append("live_process")
        for key, value in _source.items():
            await coco.mount(coco.component_subpath(key), _live_item, key, value)

    async def process_live(self, operator: coco.LiveComponentOperator) -> None:
        await operator.update_full()
        await operator.mark_ready()
        _QueueLive.started.set()
        try:
            while True:
                kind, key, value = await _QueueLive.queue.get()
                subpath = coco.component_subpath(key)
                if kind == "update":
                    handle = await operator.update(subpath, _live_item, key, value)
                else:
                    handle = await operator.delete(subpath)
                await handle.ready()
                _runs.append(f"event_done:{key}")
        except asyncio.CancelledError:
            _QueueLive.cancelled.append("cancelled")
            raise


@coco.fn
async def _queue_main() -> None:
    await coco.mount(coco.component_subpath("live"), _QueueLive)
    await coco.mount(coco.component_subpath("other"), _live_item, "other", 1)


async def _wait_for(cond: Any, timeout: float = 5.0) -> bool:
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while loop.time() < end:
        if cond():
            return True
        await asyncio.sleep(0.02)
    return bool(cond())


async def _start_running_live(
    request: pytest.FixtureRequest, name: str
) -> tuple[coco.App[Any, Any], asyncio.Future[Any]]:
    _QueueLive.queue = asyncio.Queue()
    _QueueLive.started = asyncio.Event()
    _QueueLive.cancelled = []
    _source.update(a=1, b=1)
    app = _app(_env(request), name, _queue_main)
    running = asyncio.ensure_future(app.update(live=True).result())
    await asyncio.wait_for(_QueueLive.started.wait(), timeout=10)
    return app, running


async def _stop(app: coco.App[Any, Any], running: asyncio.Future[Any]) -> None:
    running.cancel()
    try:
        await asyncio.wait_for(running, timeout=10)
    except (asyncio.CancelledError, Exception):
        pass


# ---------------------------------------------------------------------------
# 1. A parallel update that runs THROUGH a live component of a running live
#    update. Contract: parallel updates do not interfere.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind",
    ["selective_through_live", "selective_preview_through"],
)
async def test_running_live_update_survives_a_parallel_selective_update(
    request: pytest.FixtureRequest, kind: str
) -> None:
    app, running = await _start_running_live(request, kind)
    try:
        _source.update(a=2)
        if kind == "selective_through_live":
            await app.update(component_selector=[coco.ROOT_PATH / "live" / "a"])
        else:
            await app.update(
                preview=True, component_selector=[coco.ROOT_PATH / "live" / "a"]
            )
        # The running live update gets a new event: does it still process it?
        _QueueLive.queue.put_nowait(("update", "late", 7))
        ok = await _wait_for(lambda: "event_done:late" in _runs, timeout=3)
        assert ok, (
            f"running live update lost its live component; cancelled={_QueueLive.cancelled}, "
            f"running.done()={running.done()}, stored={_stored()}"
        )
    finally:
        await _stop(app, running)


@pytest.mark.asyncio
async def test_running_live_update_survives_a_parallel_plain_preview(
    request: pytest.FixtureRequest,
) -> None:
    app, running = await _start_running_live(request, "baseline_preview")
    try:
        await app.update(preview=True)
        _QueueLive.queue.put_nowait(("update", "late", 7))
        ok = await _wait_for(lambda: "event_done:late" in _runs, timeout=3)
        assert ok, (
            f"a plain preview ended the live update; cancelled={_QueueLive.cancelled}, "
            f"running.done()={running.done()}"
        )
    finally:
        await _stop(app, running)


# ---------------------------------------------------------------------------
# 2. Live events under a selective live update.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_selective_live_events_for_keys_appearing_after_start_and_readd(
    request: pytest.FixtureRequest,
) -> None:
    _QueueLive.queue = asyncio.Queue()
    _QueueLive.started = asyncio.Event()
    _source.update(a=1, b=1)
    app = _app(_env(request), "events_after_start", _queue_main)
    await app.update()
    _runs.clear()

    running = asyncio.ensure_future(
        app.update(
            live=True,
            component_selector=[
                coco.ROOT_PATH / "live" / "a",
                coco.ROOT_PATH / "live" / "new_sel",
            ],
        ).result()
    )
    try:
        await asyncio.wait_for(_QueueLive.started.wait(), timeout=10)
        q = _QueueLive.queue
        for ev in [
            ("update", "new_sel", 1),
            ("update", "new_unsel", 1),
            ("delete", "a", 0),
            ("update", "a", 5),  # re-add
            ("delete", "b", 0),  # unselected delete: skipped
            ("update", "b", 9),  # unselected update: skipped
            ("delete", "new_sel", 0),
            ("update", "new_sel", 3),
        ]:
            q.put_nowait(ev)
        assert await _wait_for(lambda: _runs.count("event_done:new_sel") == 3)
        assert _stored() == {"a": 5, "b": 1, "other": 1, "new_sel": 3}
        assert "new_unsel" not in _runs and "b" not in _runs
    finally:
        await _stop(app, running)

    # A plain update catches up and does not lose anything.
    await app.update()
    assert _stored() == {"a": 1, "b": 1, "other": 1}


@pytest.mark.asyncio
async def test_selective_live_coalescing_on_selected_and_skipped_paths(
    request: pytest.FixtureRequest,
) -> None:
    gate = asyncio.Event()
    handles: dict[str, list[Any]] = {"sel": [], "unsel": []}

    @coco.fn
    async def slow_item(key: str, value: int) -> None:
        await gate.wait()
        _runs.append(f"{key}={value}")
        _declare(key, value)

    class Live:
        async def process(self) -> None:
            pass

        async def process_live(self, operator: coco.LiveComponentOperator) -> None:
            await operator.update_full()
            await operator.mark_ready()
            for i in range(5):
                for key in ("sel", "unsel"):
                    handles[key].append(
                        await operator.update(
                            coco.component_subpath(key), slow_item, key, i
                        )
                    )
            gate.set()
            for key in ("sel", "unsel"):
                for h in handles[key]:
                    await h.ready()
            _runs.append("all_ready")

    @coco.fn
    async def main() -> None:
        await coco.mount(coco.component_subpath("live"), Live)

    app = _app(_env(request), "coalescing", main)
    running = asyncio.ensure_future(
        app.update(
            live=True, component_selector=[coco.ROOT_PATH / "live" / "sel"]
        ).result()
    )
    try:
        assert await _wait_for(lambda: "all_ready" in _runs)
        sel_runs = [r for r in _runs if r.startswith("sel=")]
        assert sel_runs[-1] == "sel=4" and len(sel_runs) <= 3, _runs
        assert not [r for r in _runs if r.startswith("unsel")]
        assert _stored() == {"sel": 4}
    finally:
        await _stop(app, running)


# ---------------------------------------------------------------------------
# 3. Termination: catch-up and live=True when everything is skipped.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("live", [False, True])
async def test_selective_update_terminates_when_every_live_child_is_skipped(
    request: pytest.FixtureRequest, live: bool
) -> None:
    class Live:
        async def process(self) -> None:
            for key in ["a", "b"]:
                await coco.mount(coco.component_subpath(key), _live_item, key, 1)

        async def process_live(self, operator: coco.LiveComponentOperator) -> None:
            await operator.update_full()
            await operator.mark_ready()

    @coco.fn
    async def main() -> None:
        await coco.mount(coco.component_subpath("live"), Live)
        await coco.mount(coco.component_subpath("live2"), Live)

    app = _app(_env(request), f"terminates_{live}", main)
    await app.update()
    for selector in (
        [coco.ROOT_PATH / "nothing"],
        [coco.ROOT_PATH / "live" / "nothing"],
        [],
    ):
        handle = app.update(live=live, component_selector=selector)
        await asyncio.wait_for(handle.result(), timeout=10)
        assert handle.stats() is not None


@pytest.mark.asyncio
async def test_watch_and_stats_on_a_selective_handle(
    request: pytest.FixtureRequest,
) -> None:
    @coco.fn
    async def main() -> None:
        for key in ["a", "b", "c"]:
            await coco.mount(coco.component_subpath(key), _live_item, key, 1)

    app = _app(_env(request), "watch_stats", main)
    await app.update()
    handle = app.update(component_selector=[coco.ROOT_PATH / "b"])
    snapshots = []
    async for snap in handle.watch():
        snapshots.append(snap)
    stats = handle.stats()
    assert snapshots, "watch() yielded nothing"
    assert stats is not None
    text = str(stats)
    # Only one item component ran.
    assert sorted(_runs) == ["a", "b", "b", "c"], _runs
    print(text)


# ---------------------------------------------------------------------------
# 4. Committed live state under a selector: a partial scan must not be
#    recorded as a bootstrap (OCI connector pattern, oci_object_storage/_source.py:423-440).
# ---------------------------------------------------------------------------

_logic_version = ["v1"]


class _BootstrapView:
    """A LiveMapView that skips its startup scan when the stored version matches."""

    def __aiter__(self) -> AsyncIterator[tuple[str, int]]:
        async def gen() -> AsyncIterator[tuple[str, int]]:
            for k, v in list(_source.items()):
                yield k, v

        return gen()

    async def watch(self, subscriber: LiveMapSubscriber[str, int]) -> None:
        stored = await subscriber.read_committed_state("scan_version")
        if stored != _logic_version[0]:
            await subscriber.update_all()
        await subscriber.mark_ready()
        if stored != _logic_version[0]:
            await subscriber.write_committed_state("scan_version", _logic_version[0])


@coco.fn
async def _versioned_item(value: int, key: str) -> None:
    _runs.append(key)
    _declare(key, f"{_logic_version[0]}:{value}")


@coco.fn
async def _bootstrap_main() -> None:
    await coco.mount_each(  # type: ignore[call-overload]
        coco.component_subpath("items"),
        lambda value, key: _versioned_item(value, key),
        _BootstrapView(),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("preview", [False, True])
async def test_selective_run_does_not_record_a_partial_scan_as_bootstrap(
    request: pytest.FixtureRequest, preview: bool
) -> None:
    @coco.fn
    async def item(value: int) -> None:
        key = f"k{value}"
        _runs.append(key)
        _declare(key, f"{_logic_version[0]}:{value}")

    @coco.fn
    async def main() -> None:
        await coco.mount_each(coco.component_subpath("items"), item, _BootstrapView())

    _logic_version[0] = "v1"
    _source.update(k1=1, k2=2, k3=3)
    app = _app(_env(request), f"bootstrap_{preview}", main)
    await app.update(live=True)
    assert _stored() == {"k1": "v1:1", "k2": "v1:2", "k3": "v1:3"}

    # The logic changes. A selective run processes one item.
    _logic_version[0] = "v2"
    await app.update(
        live=True,
        preview=preview,
        component_selector=[coco.ROOT_PATH / "items" / "k1"],
    )
    # The next plain run must still catch up the rest.
    await app.update(live=True)
    assert _stored() == {"k1": "v2:1", "k2": "v2:2", "k3": "v2:3"}


# ---------------------------------------------------------------------------
# 5. auto_refresh, LiveMapFeed / LiveStream style feeds, nested live.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_auto_refresh_under_a_selector(request: pytest.FixtureRequest) -> None:
    import datetime

    cycles: list[int] = []

    @coco.fn
    async def refresh_all() -> None:
        cycles.append(1)
        for key, value in _source.items():
            await coco.mount(coco.component_subpath(key), _live_item, key, value)

    @coco.fn
    async def main() -> None:
        await coco.mount(
            coco.component_subpath("refresh"),
            coco.auto_refresh(refresh_all, interval=datetime.timedelta(seconds=0.05)),
        )

    _source.update(a=1, b=1)
    app = _app(_env(request), "auto_refresh", main)
    await app.update()
    _runs.clear()
    _source.update(a=2, b=2)
    running = asyncio.ensure_future(
        app.update(
            live=True, component_selector=[coco.ROOT_PATH / "refresh" / "a"]
        ).result()
    )
    try:
        assert await _wait_for(lambda: len(cycles) >= 4)
        _source.update(a=3, b=3, c=3)
        assert await _wait_for(lambda: _stored().get("a") == 3)
        await asyncio.sleep(0.2)
        assert _stored() == {"a": 3, "b": 1}, _stored()
        assert "b" not in _runs and "c" not in _runs
    finally:
        await _stop(app, running)
    await app.update()
    assert _stored() == {"a": 3, "b": 3, "c": 3}


class _Feed:
    """A LiveMapFeed (watch only), like Kafka."""

    events: asyncio.Queue[tuple[str, str, int]]

    async def watch(self, subscriber: LiveMapSubscriber[str, int]) -> None:
        await subscriber.mark_ready()
        while True:
            kind, key, value = await _Feed.events.get()
            if kind == "update":
                h = await subscriber.update(key, value)
            else:
                h = await subscriber.delete(key)
            await h.ready()
            _runs.append(f"feed_done:{key}")


@pytest.mark.asyncio
async def test_mount_each_over_a_live_map_feed_under_a_selector(
    request: pytest.FixtureRequest,
) -> None:
    @coco.fn
    async def item(value: int, key_holder: list[str]) -> None:
        pass

    @coco.fn
    async def feed_item(value: tuple[str, int]) -> None:
        _runs.append(value[0])
        _declare(*value)

    class Feed:
        async def watch(self, subscriber: LiveMapSubscriber[str, Any]) -> None:
            await subscriber.mark_ready()
            while True:
                kind, key, value = await _Feed.events.get()
                if kind == "update":
                    h = await subscriber.update(key, (key, value))
                else:
                    h = await subscriber.delete(key)
                await h.ready()
                _runs.append(f"feed_done:{key}")

    @coco.fn
    async def main() -> None:
        await coco.mount_each(coco.component_subpath("feed"), feed_item, Feed())  # type: ignore[call-overload]

    _Feed.events = asyncio.Queue()
    app = _app(_env(request), "feed", main)
    running = asyncio.ensure_future(app.update(live=True).result())
    for k in ["a", "b"]:
        _Feed.events.put_nowait(("update", k, 1))
    assert await _wait_for(lambda: "feed_done:b" in _runs)
    await _stop(app, running)
    _runs.clear()

    _Feed.events = asyncio.Queue()
    running = asyncio.ensure_future(
        app.update(
            live=True, component_selector=[coco.ROOT_PATH / "feed" / "a"]
        ).result()
    )
    try:
        for ev in [
            ("update", "a", 2),
            ("update", "b", 2),
            ("delete", "b", 0),
            ("update", "c", 2),
        ]:
            _Feed.events.put_nowait(ev)
        assert await _wait_for(lambda: "feed_done:c" in _runs)
        assert _stored() == {"a": 2, "b": 1}
    finally:
        await _stop(app, running)


@pytest.mark.asyncio
async def test_update_that_selects_a_running_live_component_replaces_it(
    request: pytest.FixtureRequest,
) -> None:
    # As a second plain update does. The replaced update ends without an error.
    app, running = await _start_running_live(request, "replaced")
    await app.update(component_selector=[coco.ROOT_PATH / "live"])
    await asyncio.wait_for(running, 5)

    _QueueLive.started = asyncio.Event()
    running2 = asyncio.ensure_future(app.update(live=True).result())
    try:
        await asyncio.wait_for(_QueueLive.started.wait(), 10)
        _QueueLive.queue.put_nowait(("update", "late", 3))
        assert await _wait_for(lambda: "event_done:late" in _runs)
        assert _stored()["late"] == 3
    finally:
        await _stop(app, running2)


@pytest.mark.asyncio
async def test_update_through_a_live_component_queues_behind_its_running_cycle(
    request: pytest.FixtureRequest,
) -> None:
    gate = asyncio.Event()

    @coco.fn
    async def slow(key: str, value: int) -> None:
        if key == "slow":
            await gate.wait()
        _runs.append(key)
        _declare(key, value)

    class Live:
        async def process(self) -> None:
            for key in ["a", "slow"]:
                await coco.mount(coco.component_subpath(key), slow, key, 1)

        async def process_live(self, operator: coco.LiveComponentOperator) -> None:
            await operator.update_full()
            await operator.mark_ready()
            await asyncio.Event().wait()

    @coco.fn
    async def main() -> None:
        await coco.mount(coco.component_subpath("live"), Live)

    app = _app(_env(request), "queues_behind_cycle", main)
    running = asyncio.ensure_future(app.update(live=True).result())
    await asyncio.sleep(0.3)  # The running cycle is blocked on "slow".
    selective = asyncio.ensure_future(
        app.update(component_selector=[coco.ROOT_PATH / "live" / "a"]).result()
    )
    await asyncio.sleep(0.3)
    assert not selective.done()

    gate.set()
    await asyncio.wait_for(selective, 20)
    assert not running.done()
    assert _stored() == {"a": 1, "slow": 1}
    await _stop(app, running)


_constructed: list[str] = []


class _SideEffectLive:
    def __init__(self, name: str) -> None:
        _constructed.append(name)

    async def process(self) -> None:
        pass

    async def process_live(self, operator: coco.LiveComponentOperator) -> None:
        await operator.update_full()
        await operator.mark_ready()


@pytest.mark.asyncio
async def test_unselected_live_component_constructor_is_not_run(
    request: pytest.FixtureRequest,
) -> None:
    class Outer:
        async def process(self) -> None:
            pass

        async def process_live(self, operator: coco.LiveComponentOperator) -> None:
            await operator.update_full()
            for n in ["inner_sel", "inner_unsel"]:
                h = await operator.update(coco.component_subpath(n), _SideEffectLive, n)
                await h.ready()
            await operator.mark_ready()

    @coco.fn
    async def main() -> None:
        await coco.mount(coco.component_subpath("sel"), _SideEffectLive, "sel")
        await coco.mount(coco.component_subpath("unsel"), _SideEffectLive, "unsel")
        await coco.mount(coco.component_subpath("outer"), Outer)

    app = _app(_env(request), "ctor", main)
    await app.update()
    _constructed.clear()
    await app.update(
        component_selector=[
            coco.ROOT_PATH / "sel",
            coco.ROOT_PATH / "outer" / "inner_sel",
        ]
    )
    assert _constructed == ["sel", "inner_sel"], _constructed

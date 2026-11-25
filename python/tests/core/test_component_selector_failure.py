"""Sliced execution: failure, interruption and API surface."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import pytest

import cocoindex as coco

from tests import common
from tests.common.target_states import GlobalDictTarget

_source: dict[str, int] = {}
_runs: list[str] = []
_fail: set[str] = set()
_slow: dict[str, float] = {}


@pytest.fixture(autouse=True)
def _reset() -> None:
    GlobalDictTarget.store.clear()
    GlobalDictTarget.store.sink_exception = False
    _source.clear()
    _runs.clear()
    _fail.clear()
    _slow.clear()


def _stored() -> dict[str, Any]:
    return {k: v.data for k, v in GlobalDictTarget.store.data.items()}


@coco.fn
async def _item(key: str, value: int) -> None:
    _runs.append(key)
    if key in _slow:
        await asyncio.sleep(_slow[key])
    if key in _fail:
        raise ValueError(f"boom {key}")
    coco.declare_target_state(GlobalDictTarget.target_state(key, value))


@coco.fn
async def _group(name: str) -> None:
    _runs.append(f"group:{name}")
    for key, value in sorted(_source.items()):
        if key.startswith(name):
            await coco.mount(coco.component_subpath(key), _item, key, value)


@coco.fn
async def _main() -> None:
    for name in ["g1", "g2"]:
        await coco.mount(coco.component_subpath(name), _group, name)


def _app(env: coco.Environment, name: str, main: Any = _main) -> coco.App[Any, Any]:
    return coco.App(coco.AppConfig(name=f"selector_fail_{name}", environment=env), main)


def _seed() -> None:
    _source.update({"g1a": 1, "g1b": 1, "g1c": 1, "g2a": 1, "g2b": 1})


def _expected() -> dict[str, int]:
    return dict(_source)


@pytest.mark.asyncio
async def test_deadline_during_selective_run_then_recovers(
    request: pytest.FixtureRequest,
) -> None:
    env = common.create_test_env(__file__, suffix=request.node.name)
    _seed()
    app = _app(env, "deadline")
    await app.update()
    for k in list(_source):
        _source[k] = 2
    _slow["g1b"] = 1.0
    with coco.timeout(timedelta(seconds=0.2)):
        with pytest.raises(Exception) as ei:
            await app.update(component_selector=[coco.ROOT_PATH / "g1"])
    print("deadline error:", type(ei.value).__name__, ei.value)
    # unselected g2 untouched; g1b not updated.
    assert _stored()["g2a"] == 1 and _stored()["g2b"] == 1
    _slow.clear()
    await app.update(component_selector=[coco.ROOT_PATH / "g1"])
    assert {k: v for k, v in _stored().items() if k.startswith("g1")} == {
        "g1a": 2,
        "g1b": 2,
        "g1c": 2,
    }
    assert _stored()["g2a"] == 1
    await app.update()
    assert _stored() == _expected()


@pytest.mark.asyncio
async def test_cancel_a_selective_update_task_then_recovers(
    request: pytest.FixtureRequest,
) -> None:
    env = common.create_test_env(__file__, suffix=request.node.name)
    _seed()
    app = _app(env, "cancel")
    await app.update()
    for k in list(_source):
        _source[k] = 3
    del _source["g1c"]
    for delay in [0.0, 0.01, 0.05, 0.1, 0.3]:
        _slow["g1b"] = 0.2
        task = asyncio.ensure_future(
            app.update(component_selector=[coco.ROOT_PATH / "g1"]).result()
        )
        await asyncio.sleep(delay)
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass
        assert _stored()["g2a"] == 1, f"delay {delay}: unselected changed"
    _slow.clear()
    # Let any background work drain.
    await asyncio.sleep(0.5)
    await app.update(component_selector=[coco.ROOT_PATH / "g1"])
    assert {k: v for k, v in _stored().items() if k.startswith("g1")} == {
        "g1a": 3,
        "g1b": 3,
    }
    await app.update()
    assert _stored() == _expected()


@pytest.mark.asyncio
async def test_exception_handler_sees_only_selected_failures(
    request: pytest.FixtureRequest,
) -> None:
    env = common.create_test_env(__file__, suffix=request.node.name)
    seen: list[str] = []

    async def handler(exc: BaseException, ctx: Any) -> None:
        seen.append(ctx.stable_path)

    @coco.fn
    async def main() -> None:
        async with coco.exception_handler(handler):
            for key, value in sorted(_source.items()):
                await coco.mount(coco.component_subpath(key), _item, key, value)

    _source.update(a=1, b=1, c=1)
    app = _app(env, "handler", main)
    await app.update()
    _fail.update({"a", "b"})
    _source.update(a=2, b=2, c=2)
    await app.update(component_selector=[coco.ROOT_PATH / "b", coco.ROOT_PATH / "c"])
    assert len(seen) == 1 and seen[0].endswith('"b"'), seen
    # A failed selected component keeps its previous target state.
    assert _stored() == {"a": 1, "b": 1, "c": 2}
    _fail.clear()
    await app.update(component_selector=[coco.ROOT_PATH / "b"])
    assert _stored() == {"a": 1, "b": 2, "c": 2}


@pytest.mark.asyncio
async def test_failing_selected_child_does_not_delete_unselected_siblings(
    request: pytest.FixtureRequest,
) -> None:
    env = common.create_test_env(__file__, suffix=request.node.name)
    _seed()
    app = _app(env, "failing_child")
    await app.update()
    _fail.add("g1b")
    del _source["g1c"]  # unselected deletion: must stay
    _source["g1b"] = 5
    _source["g1a"] = 5
    try:
        await app.update(
            component_selector=[
                coco.ROOT_PATH / "g1" / "g1b",
                coco.ROOT_PATH / "g1" / "g1a",
            ]
        )
    except Exception as e:  # noqa: BLE001
        print("selective with failing child raised:", type(e).__name__)
    assert _stored() == {"g1a": 5, "g1b": 1, "g1c": 1, "g2a": 1, "g2b": 1}
    _fail.clear()
    await app.update()
    assert _stored() == _expected()


@pytest.mark.asyncio
async def test_failing_ancestor_use_mount_under_selector(
    request: pytest.FixtureRequest,
) -> None:
    env = common.create_test_env(__file__, suffix=request.node.name)
    fail_parent = [False]

    @coco.fn
    async def setup() -> int:
        if fail_parent[0]:
            raise ValueError("parent fails")
        return 10

    @coco.fn
    async def main() -> None:
        base = await coco.use_mount(coco.component_subpath("setup"), setup)
        for key, value in sorted(_source.items()):
            await coco.mount(coco.component_subpath(key), _item, key, value + base)

    _source.update(a=1, b=1)
    app = _app(env, "failing_use_mount", main)
    await app.update()
    fail_parent[0] = True
    _source.update(a=2, b=2)
    with pytest.raises(Exception):
        await app.update(component_selector=[coco.ROOT_PATH / "a"])
    assert _stored() == {"a": 11, "b": 11}
    fail_parent[0] = False
    await app.update(component_selector=[coco.ROOT_PATH / "a"])
    assert _stored() == {"a": 12, "b": 11}


@pytest.mark.asyncio
async def test_sink_failure_on_selected_then_retry(
    request: pytest.FixtureRequest,
) -> None:
    env = common.create_test_env(__file__, suffix=request.node.name)
    _seed()
    app = _app(env, "sink_fail")
    await app.update()
    for k in list(_source):
        _source[k] = 4
    GlobalDictTarget.store.sink_exception = True
    try:
        await app.update(component_selector=[coco.ROOT_PATH / "g2"])
    except Exception as e:  # noqa: BLE001
        print("sink failure raised:", type(e).__name__)
    GlobalDictTarget.store.sink_exception = False
    await app.update(component_selector=[coco.ROOT_PATH / "g2"])
    assert _stored() == {"g1a": 1, "g1b": 1, "g1c": 1, "g2a": 4, "g2b": 4}
    await app.update()
    assert _stored() == _expected()


def test_update_blocking_with_report_to_stdout_and_show_progress(
    capsys: pytest.CaptureFixture[str],
) -> None:
    env = common.create_test_env(__file__, suffix="stdout")
    _seed()
    app = _app(env, "stdout")
    app.update_blocking()
    _runs.clear()
    for k in list(_source):
        _source[k] = 6
    app.update_blocking(
        report_to_stdout=True, component_selector=[coco.ROOT_PATH / "g2" / "g2a"]
    )
    out = capsys.readouterr().out
    print(out)
    assert _stored()["g2a"] == 6 and _stored()["g1a"] == 1
    assert sorted(_runs) == ["g2a", "group:g2"], _runs


@pytest.mark.asyncio
async def test_show_progress_and_duplicates(request: pytest.FixtureRequest) -> None:
    env = common.create_test_env(__file__, suffix=request.node.name)
    _seed()
    app = _app(env, "progress")
    await app.update()
    _runs.clear()
    for k in list(_source):
        _source[k] = 7
    p = coco.ROOT_PATH / "g1" / "g1a"
    handle = app.update(
        component_selector=[p, p, coco.ROOT_PATH / "g1" / "g1a" / "deeper", p]
    )
    await coco.show_progress(handle)
    assert _runs.count("g1a") == 1, _runs
    assert _stored()["g1a"] == 7 and _stored()["g1b"] == 1


@pytest.mark.asyncio
async def test_selector_iterable_consumed_lazily_or_eagerly(
    request: pytest.FixtureRequest,
) -> None:
    env = common.create_test_env(__file__, suffix=request.node.name)
    _seed()
    app = _app(env, "lazy")
    await app.update()
    for k in list(_source):
        _source[k] = 8
    paths = [coco.ROOT_PATH / "g1" / "g1a"]
    handle = app.update(component_selector=paths)
    paths.append(coco.ROOT_PATH / "g2")  # mutation after the call must not matter
    await handle
    assert _stored()["g2a"] == 1 and _stored()["g1a"] == 8


@pytest.mark.asyncio
async def test_memoized_use_mount_leaf_keeps_its_memo_under_a_selector(
    request: pytest.FixtureRequest,
) -> None:
    env = common.create_test_env(__file__, suffix=request.node.name)
    loads: list[int] = []

    @coco.fn(memo=True)
    async def load_model(version: int) -> int:
        loads.append(version)
        return version * 10

    @coco.fn
    async def main() -> None:
        base = await coco.use_mount(
            coco.component_subpath("model"), load_model, _source["v"]
        )
        for key in ["a", "b"]:
            await coco.mount(coco.component_subpath(key), _item, key, base)

    _source["v"] = 1
    app = _app(env, "use_mount_memo", main)
    await app.update()
    _source["v"] = 2
    await app.update(component_selector=[coco.ROOT_PATH / "a"])
    assert loads == [1, 2]
    await app.update()
    # Nothing changed since the selective run: the expensive leaf should not rerun.
    assert loads == [1, 2], f"memoized use_mount leaf recomputed: {loads}"


@pytest.mark.asyncio
async def test_full_reprocess_under_a_selector_does_not_rerun_unselected_use_mount(
    request: pytest.FixtureRequest,
) -> None:
    env = common.create_test_env(__file__, suffix=request.node.name)
    calls: list[str] = []

    @coco.fn(memo=True)
    async def embed(key: str) -> int:
        calls.append(f"embed:{key}")
        return 1

    @coco.fn
    async def per_file(key: str) -> int:
        calls.append(f"use_mount:{key}")
        return await embed(key)

    @coco.fn
    async def main() -> None:
        for key in ["f1", "f2"]:
            await coco.use_mount(coco.component_subpath("files", key), per_file, key)
        await coco.mount(coco.component_subpath("target"), _item, "target", 1)

    app = _app(env, "full_reprocess_use_mount", main)
    await app.update()
    calls.clear()
    await app.update(
        full_reprocess=True, component_selector=[coco.ROOT_PATH / "target"]
    )
    assert [c for c in calls if c.startswith("embed")] == [], calls

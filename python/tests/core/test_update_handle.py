import asyncio

import pytest

import cocoindex as coco
from tests.common import create_test_env
from tests.common.target_states import GlobalDictTarget

coco_env = create_test_env(__file__)

_source_data: dict[str, int] = {}


@coco.fn()
async def _process_items() -> None:
    for key, value in _source_data.items():
        coco.declare_target_state(GlobalDictTarget.target_state(key, value))


@coco.fn()
async def _trivial_fn(s: str) -> str:
    return s


async def _failing_main() -> None:
    raise ValueError("intentional test error")


# --- F1: handle.result() and handle.stats() ---


@pytest.mark.asyncio
async def test_handle_result() -> None:
    app = coco.App(
        coco.AppConfig(name="test_handle_result", environment=coco_env),
        _trivial_fn,
        "hello",
    )
    handle = app.update()
    result = await handle.result()
    assert result == "hello"


@pytest.mark.asyncio
async def test_handle_stats_after_completion() -> None:
    GlobalDictTarget.store.clear()
    _source_data.clear()
    _source_data["a"] = 1
    _source_data["b"] = 2

    app = coco.App(
        coco.AppConfig(name="test_handle_stats_after_completion", environment=coco_env),
        _process_items,
    )
    handle = app.update()
    await handle.result()

    stats = handle.stats()
    assert stats is not None
    assert len(stats.by_component) > 0
    total = stats.total
    assert total.num_finished == total.num_processed + total.num_errors
    assert total.num_in_progress == 0


# --- F2: watch() tests ---


@pytest.mark.asyncio
async def test_watch_yields_running_then_done() -> None:
    GlobalDictTarget.store.clear()
    _source_data.clear()
    _source_data["x"] = 10
    _source_data["y"] = 20

    app = coco.App(
        coco.AppConfig(
            name="test_watch_yields_running_then_done", environment=coco_env
        ),
        _process_items,
    )
    handle = app.update()
    snapshots = []
    async for snapshot in handle.watch():
        snapshots.append(snapshot)

    assert len(snapshots) >= 1
    # Last snapshot should be DONE
    assert snapshots[-1].status == coco.UpdateStatus.DONE
    assert snapshots[-1].stats is not None
    # All snapshots should have stats
    for snap in snapshots:
        assert snap.stats is not None


@pytest.mark.asyncio
async def test_watch_raises_on_error() -> None:
    app = coco.App(
        coco.AppConfig(name="test_watch_raises_on_error", environment=coco_env),
        _failing_main,
    )
    handle = app.update()
    with pytest.raises(Exception, match="intentional test error"):
        async for _ in handle.watch():
            pass


@pytest.mark.asyncio
async def test_watch_with_throttle() -> None:
    GlobalDictTarget.store.clear()
    _source_data.clear()
    for i in range(10):
        _source_data[f"key_{i}"] = i

    app = coco.App(
        coco.AppConfig(name="test_watch_with_throttle", environment=coco_env),
        _process_items,
    )
    handle = app.update()
    snapshots = []
    async for snapshot in handle.watch():
        snapshots.append(snapshot)
        if snapshot.status == coco.UpdateStatus.RUNNING:
            await asyncio.sleep(0.05)

    assert len(snapshots) >= 1
    assert snapshots[-1].status == coco.UpdateStatus.DONE


# --- F3: report_to_stdout with watch ---


@pytest.mark.asyncio
async def test_report_to_stdout_with_watch() -> None:
    GlobalDictTarget.store.clear()
    _source_data.clear()
    _source_data["a"] = 1

    app = coco.App(
        coco.AppConfig(name="test_report_to_stdout_with_watch", environment=coco_env),
        _process_items,
    )
    handle = app.update(report_to_stdout=True)
    snapshots = []
    async for snapshot in handle.watch():
        snapshots.append(snapshot)

    assert len(snapshots) >= 1
    assert snapshots[-1].status == coco.UpdateStatus.DONE

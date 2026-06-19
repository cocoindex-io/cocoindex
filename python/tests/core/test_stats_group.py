"""E2E tests for `coco.stats_group(...)` — see specs/scoped_stats_report."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import pytest

import cocoindex as coco
from cocoindex._internal.update_stats import _resolve_report_to_stdout
from tests.common import create_test_env
from tests.common.target_states import GlobalDictTarget

coco_env = create_test_env(__file__)

# Per-test capture of the StatsGroup handle yielded inside the app main, plus
# coordination primitives. Reset at the start of each test.
_captured: dict[str, Any] = {}


def _reset() -> None:
    GlobalDictTarget.store.clear()
    _captured.clear()


@coco.fn()
async def _emit_root(key: str, value: int) -> None:
    coco.declare_target_state(GlobalDictTarget.target_state(key, value))


@coco.fn()
async def _emit_grp(key: str, value: int) -> None:
    coco.declare_target_state(GlobalDictTarget.target_state(key, value))


@coco.fn()
async def _emit_inner(key: str, value: int) -> None:
    coco.declare_target_state(GlobalDictTarget.target_state(key, value))


def _has(keys: Any, needle: str) -> bool:
    return any(needle in k for k in keys)


async def _drive_to_ready(
    handle: coco.UpdateHandle[None] | coco.StatsGroupHandle,
) -> list[Any]:
    snaps = []
    async for snap in handle.watch():
        snaps.append(snap)
    return snaps


# --- 2. split-out aggregation ---


@coco.fn()
async def _main_split() -> None:
    await coco.mount(coco.component_subpath("root_item"), _emit_root, "r0", 1)
    with coco.stats_group("grp") as sg:
        _captured["sg"] = sg
        for k in ("a", "b", "c"):
            await coco.mount(coco.component_subpath(f"g_{k}"), _emit_grp, k, ord(k))


@pytest.mark.asyncio
async def test_stats_group_splits_out_stats() -> None:
    _reset()
    app = coco.App(
        coco.AppConfig(name="test_sg_split", environment=coco_env), _main_split
    )
    handle = app.update()
    await handle.result()

    sg: coco.StatsGroupHandle = _captured["sg"]
    root_keys = set((handle.stats() or coco.UpdateStats(by_component={})).by_component)
    grp_keys = set((sg.stats() or coco.UpdateStats(by_component={})).by_component)

    # Group's processor is reported in the group, not the root; and vice versa.
    assert _has(grp_keys, "_emit_grp")
    assert not _has(root_keys, "_emit_grp")
    assert _has(root_keys, "_emit_root")
    assert not _has(grp_keys, "_emit_root")

    # Identity tree intact — all rows applied regardless of grouping.
    assert GlobalDictTarget.store.data.keys() >= {"r0", "a", "b", "c"}


# --- 3. watch() reaches READY ---


@pytest.mark.asyncio
async def test_stats_group_watch_ready() -> None:
    _reset()
    app = coco.App(
        coco.AppConfig(name="test_sg_watch", environment=coco_env), _main_split
    )
    handle = app.update()
    # Start the update concurrently so the group is created, then watch it.
    task = asyncio.create_task(handle.result())
    # Wait until the app main has entered the group and captured the handle.
    for _ in range(200):
        if "sg" in _captured:
            break
        await asyncio.sleep(0.01)
    sg: coco.StatsGroupHandle = _captured["sg"]
    snaps = await _drive_to_ready(sg)
    await task

    assert snaps, "expected at least one snapshot"
    assert snaps[-1].status == coco.UpdateStatus.READY
    assert snaps[-1].stats is not None


# --- 4. non-blocking exit ---


@coco.fn()
async def _slow_child() -> None:
    # Stays in-progress for a while so the test can observe that the
    # `with` block exited before this member became ready.
    await asyncio.sleep(2.0)
    coco.declare_target_state(GlobalDictTarget.target_state("blk", 1))


@coco.fn()
async def _main_nonblock() -> None:
    with coco.stats_group("g") as sg:
        _captured["sg"] = sg
        await coco.mount(coco.component_subpath("blk"), _slow_child)
    _captured["exited"] = True


@pytest.mark.asyncio
async def test_stats_group_nonblocking_exit() -> None:
    _reset()
    app = coco.App(
        coco.AppConfig(name="test_sg_nonblock", environment=coco_env), _main_nonblock
    )
    handle = app.update()
    task = asyncio.create_task(handle.result())

    # The `with` block exits immediately even though the mounted child is still
    # sleeping (2s) — exit is non-blocking. Observe the exit flag well
    # before the child completes.
    exited_fast = False
    for _ in range(50):  # up to ~0.5s
        if _captured.get("exited"):
            exited_fast = True
            break
        await asyncio.sleep(0.01)
    assert exited_fast, "stats_group exit should be non-blocking"
    assert not task.done(), "update should still be running (child in progress)"

    await task
    assert GlobalDictTarget.store.data["blk"].data == 1


# --- 5. nested groups ---


@coco.fn()
async def _main_nested() -> None:
    with coco.stats_group("outer") as og:
        _captured["og"] = og
        await coco.mount(coco.component_subpath("a"), _emit_root, "a", 1)
        with coco.stats_group("inner") as ig:
            _captured["ig"] = ig
            await coco.mount(coco.component_subpath("b"), _emit_inner, "b", 2)


@pytest.mark.asyncio
async def test_stats_group_nested() -> None:
    _reset()
    app = coco.App(
        coco.AppConfig(name="test_sg_nested", environment=coco_env), _main_nested
    )
    handle = app.update()
    await handle.result()

    og: coco.StatsGroupHandle = _captured["og"]
    ig: coco.StatsGroupHandle = _captured["ig"]
    root_keys = set((handle.stats() or coco.UpdateStats(by_component={})).by_component)
    og_keys = set((og.stats() or coco.UpdateStats(by_component={})).by_component)
    ig_keys = set((ig.stats() or coco.UpdateStats(by_component={})).by_component)

    # Stats go to the innermost group only.
    assert _has(ig_keys, "_emit_inner")
    assert not _has(og_keys, "_emit_inner")
    assert _has(og_keys, "_emit_root")
    assert not _has(root_keys, "_emit_root")
    assert not _has(root_keys, "_emit_inner")
    assert GlobalDictTarget.store.data.keys() >= {"a", "b"}


# --- 6. empty group ---


@coco.fn()
async def _main_empty() -> None:
    with coco.stats_group("empty") as sg:
        _captured["sg"] = sg


@pytest.mark.asyncio
async def test_stats_group_empty() -> None:
    _reset()
    app = coco.App(
        coco.AppConfig(name="test_sg_empty", environment=coco_env), _main_empty
    )
    handle = app.update()
    task = asyncio.create_task(handle.result())
    for _ in range(200):
        if "sg" in _captured:
            break
        await asyncio.sleep(0.01)
    sg: coco.StatsGroupHandle = _captured["sg"]
    # Should terminate cleanly (no hang); may yield zero snapshots.
    snaps = await asyncio.wait_for(_drive_to_ready(sg), timeout=10)
    await task
    assert sg.stats() is None or len(sg.stats().by_component) == 0  # type: ignore[union-attr]
    assert all(s.status == coco.UpdateStatus.READY for s in snaps)


# --- 7. body exception ---


@coco.fn()
async def _emit_then_ok(key: str, value: int) -> None:
    coco.declare_target_state(GlobalDictTarget.target_state(key, value))


@coco.fn()
async def _main_raises() -> None:
    with coco.stats_group("g"):
        await coco.mount(coco.component_subpath("x"), _emit_then_ok, "x", 1)
        raise ValueError("boom in body")


@pytest.mark.asyncio
async def test_stats_group_body_exception() -> None:
    _reset()
    app = coco.App(
        coco.AppConfig(name="test_sg_exc", environment=coco_env), _main_raises
    )
    handle = app.update()
    with pytest.raises(Exception, match="boom in body"):
        await handle.result()


# --- 8. foreground use_mount only ---


@coco.fn()
async def _produce(value: int) -> int:
    coco.declare_target_state(GlobalDictTarget.target_state(f"u{value}", value))
    return value * 2


@coco.fn()
async def _main_use_mount() -> None:
    with coco.stats_group("fg") as sg:
        _captured["sg"] = sg
        r = await coco.use_mount(coco.component_subpath("u"), _produce, 5)
        _captured["use_mount_result"] = r


@pytest.mark.asyncio
async def test_stats_group_use_mount_foreground() -> None:
    _reset()
    app = coco.App(
        coco.AppConfig(name="test_sg_usemount", environment=coco_env), _main_use_mount
    )
    handle = app.update()
    await handle.result()
    sg: coco.StatsGroupHandle = _captured["sg"]
    assert _captured["use_mount_result"] == 10
    grp_keys = set((sg.stats() or coco.UpdateStats(by_component={})).by_component)
    assert _has(grp_keys, "_produce")
    assert GlobalDictTarget.store.data["u5"].data == 5


# --- 9. report_to_stdout plain (non-TTY under pytest) ---


@coco.fn()
async def _main_report() -> None:
    with coco.stats_group("Indexing", report_to_stdout=True) as sg:
        _captured["sg"] = sg
        for k in ("a", "b"):
            await coco.mount(coco.component_subpath(f"r_{k}"), _emit_grp, k, ord(k))


@pytest.mark.asyncio
async def test_stats_group_report_to_stdout_plain(
    capfd: pytest.CaptureFixture[str],
) -> None:
    _reset()
    app = coco.App(
        coco.AppConfig(name="test_sg_report", environment=coco_env), _main_report
    )
    handle = app.update()
    await handle.result()
    # Give the detached plain reporter a tick to flush its final block.
    await asyncio.sleep(0.2)
    out = capfd.readouterr().out
    # Title-prefixed header (#3) and an explicit terminated marker (#4).
    assert "[Stats: Indexing]" in out
    assert "(terminated)" in out


# --- 11. live member: group readies + splits out, no hang ---


class _GroupLiveComponent:
    """A live component that catches up and marks ready, then returns."""

    async def process(self) -> None:
        coco.declare_target_state(GlobalDictTarget.target_state("live", 1))

    async def process_live(self, operator: coco.LiveComponentOperator) -> None:
        await operator.update_full()
        await operator.mark_ready()


@coco.fn()
async def _main_live_group() -> None:
    with coco.stats_group("livegrp") as sg:
        _captured["sg"] = sg
        await coco.mount(coco.component_subpath("live"), _GroupLiveComponent)


@pytest.mark.asyncio
async def test_stats_group_live_member_termination() -> None:
    _reset()
    app = coco.App(
        coco.AppConfig(name="test_sg_live", environment=coco_env), _main_live_group
    )
    handle = app.update(live=True)
    # Must complete (group READY does not deadlock on the live member, and
    # termination fires once the member goes inactive).
    await asyncio.wait_for(handle.result(), timeout=20)

    sg: coco.StatsGroupHandle = _captured["sg"]
    root_keys = set((handle.stats() or coco.UpdateStats(by_component={})).by_component)
    grp_keys = set((sg.stats() or coco.UpdateStats(by_component={})).by_component)

    # Live member's stats are split out into the group, not the root.
    assert grp_keys, "live member should report into the group"
    assert not (grp_keys & root_keys)
    assert GlobalDictTarget.store.data["live"].data == 1


# --- 12. report_to_stdout: bool | timedelta ---


def test_resolve_report_to_stdout() -> None:
    assert _resolve_report_to_stdout(False) == (False, None)
    assert _resolve_report_to_stdout(True) == (True, None)
    assert _resolve_report_to_stdout(timedelta(seconds=5)) == (True, 5.0)
    assert _resolve_report_to_stdout(timedelta(milliseconds=250)) == (True, 0.25)
    for bad in (timedelta(0), timedelta(seconds=-1)):
        with pytest.raises(ValueError, match="positive duration"):
            _resolve_report_to_stdout(bad)


@coco.fn()
async def _main_report_interval() -> None:
    with coco.stats_group(
        "Indexing", report_to_stdout=timedelta(milliseconds=50)
    ) as sg:
        _captured["sg"] = sg
        for k in ("a", "b"):
            await coco.mount(coco.component_subpath(f"ri_{k}"), _emit_grp, k, ord(k))


@pytest.mark.asyncio
async def test_stats_group_report_to_stdout_interval(
    capfd: pytest.CaptureFixture[str],
) -> None:
    _reset()
    app = coco.App(
        coco.AppConfig(name="test_sg_report_interval", environment=coco_env),
        _main_report_interval,
    )
    handle = app.update()
    await handle.result()
    await asyncio.sleep(0.2)
    out = capfd.readouterr().out
    # A custom refresh interval still produces the labeled group report.
    assert "[Stats: Indexing]" in out
    assert "(terminated)" in out

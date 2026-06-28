"""End-to-end tests for localfs source connector in live mode."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

import cocoindex as coco
from cocoindex.connectors import localfs

from tests import common
from tests.common.target_states import GlobalDictTarget

coco_env = common.create_test_env(__file__)


@coco.fn
async def process_file(file: localfs.File) -> None:
    content = await file.read_text()
    # Use the filename as the target state key
    key = file.file_path.path.name
    coco.declare_target_state(GlobalDictTarget.target_state(key, content))


async def _wait_for_target_keys(
    expected_keys: set[str],
    *,
    timeout: float = 30.0,
    poll_interval: float = 0.2,
) -> None:
    """Poll until GlobalDictTarget.store.data has exactly the expected keys."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if set(GlobalDictTarget.store.data.keys()) == expected_keys:
            return
        await asyncio.sleep(poll_interval)
    actual = set(GlobalDictTarget.store.data.keys())
    raise AssertionError(
        f"Timed out waiting for target keys.\n"
        f"  Expected: {expected_keys}\n"
        f"  Actual:   {actual}"
    )


async def _wait_for_value(
    key: str,
    expected_value: str,
    *,
    timeout: float = 20.0,
    poll_interval: float = 0.1,
) -> None:
    """Poll until GlobalDictTarget.store.data[key].data == expected_value."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        entry = GlobalDictTarget.store.data.get(key)
        if entry is not None and entry.data == expected_value:
            return
        await asyncio.sleep(poll_interval)
    actual = GlobalDictTarget.store.data.get(key)
    raise AssertionError(
        f"Timed out waiting for {key!r} to have value {expected_value!r}.\n"
        f"  Actual entry: {actual}"
    )


@pytest.mark.asyncio
async def test_localfs_live_add_edit_delete(tmp_path: Path) -> None:
    """Full lifecycle: initial scan, add file, edit file, delete file."""
    GlobalDictTarget.store.clear()

    # --- Initial files ---
    (tmp_path / "file1.txt").write_text("content1")
    (tmp_path / "file2.txt").write_text("content2")

    @coco.fn
    async def app_main() -> None:
        files = localfs.walk_dir(tmp_path, live=True)
        await coco.mount_each(process_file, files.items())

    app = coco.App(
        coco.AppConfig(name="test_localfs_live", environment=coco_env),
        app_main,
    )

    handle = app.update(live=True)
    # Drive the update in the background
    update_task = asyncio.create_task(handle.result())
    # Give the task a chance to start
    await asyncio.sleep(0.5)

    try:
        # Keys are relative paths (what DirWalker.items() yields as keys)
        file1_key = "file1.txt"
        file2_key = "file2.txt"

        # Wait for initial state: 2 files
        await _wait_for_target_keys({file1_key, file2_key})
        assert GlobalDictTarget.store.data[file1_key].data == "content1"
        assert GlobalDictTarget.store.data[file2_key].data == "content2"

        # --- Add a new file ---
        (tmp_path / "file3.txt").write_text("content3")
        file3_key = "file3.txt"
        await _wait_for_target_keys({file1_key, file2_key, file3_key})
        assert GlobalDictTarget.store.data[file3_key].data == "content3"

        # --- Edit an existing file ---
        (tmp_path / "file1.txt").write_text("content1_edited")
        await _wait_for_value(file1_key, "content1_edited")
        assert GlobalDictTarget.store.data[file1_key].data == "content1_edited"

        # --- Delete a file ---
        (tmp_path / "file2.txt").unlink()
        await _wait_for_target_keys({file1_key, file3_key})
        assert file2_key not in GlobalDictTarget.store.data

        # Final state verification
        assert GlobalDictTarget.store.data[file1_key].data == "content1_edited"
        assert GlobalDictTarget.store.data[file3_key].data == "content3"
    finally:
        update_task.cancel()
        try:
            await update_task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_localfs_live_rescan_interval(tmp_path: Path) -> None:
    """Periodic rescan detects files even with a short rescan interval."""
    GlobalDictTarget.store.clear()

    (tmp_path / "file1.txt").write_text("content1")

    @coco.fn
    async def app_main() -> None:
        files = localfs.walk_dir(tmp_path, live=True, rescan_interval=2.0)
        await coco.mount_each(process_file, files.items())

    app = coco.App(
        coco.AppConfig(name="test_localfs_live_rescan", environment=coco_env),
        app_main,
    )

    handle = app.update(live=True)
    update_task = asyncio.create_task(handle.result())
    await asyncio.sleep(0.5)

    try:
        file1_key = "file1.txt"
        await _wait_for_target_keys({file1_key})
        assert GlobalDictTarget.store.data[file1_key].data == "content1"

        # Add a file — picked up by events or the periodic rescan.
        (tmp_path / "file2.txt").write_text("content2")
        file2_key = "file2.txt"
        await _wait_for_target_keys({file1_key, file2_key})
        assert GlobalDictTarget.store.data[file2_key].data == "content2"

        # Edit after a rescan cycle should still be detected.
        await asyncio.sleep(2.5)
        (tmp_path / "file1.txt").write_text("content1_v2")
        await _wait_for_value(file1_key, "content1_v2")
    finally:
        update_task.cancel()
        try:
            await update_task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_localfs_live_rescan_none_disables(tmp_path: Path) -> None:
    """Setting rescan_interval=None disables periodic rescanning."""
    GlobalDictTarget.store.clear()

    (tmp_path / "a.txt").write_text("aaa")

    @coco.fn
    async def app_main() -> None:
        files = localfs.walk_dir(tmp_path, live=True, rescan_interval=None)
        await coco.mount_each(process_file, files.items())

    app = coco.App(
        coco.AppConfig(name="test_localfs_live_rescan_none", environment=coco_env),
        app_main,
    )

    handle = app.update(live=True)
    update_task = asyncio.create_task(handle.result())
    await asyncio.sleep(0.5)

    try:
        await _wait_for_target_keys({"a.txt"})
        assert GlobalDictTarget.store.data["a.txt"].data == "aaa"

        # Events still work when periodic rescan is disabled.
        (tmp_path / "b.txt").write_text("bbb")
        await _wait_for_target_keys({"a.txt", "b.txt"})
        assert GlobalDictTarget.store.data["b.txt"].data == "bbb"
    finally:
        update_task.cancel()
        try:
            await update_task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_localfs_live_single_watcher(tmp_path: Path) -> None:
    """A second concurrent watch() on one live view fails loudly."""
    items: Any = localfs.walk_dir(tmp_path, live=True).items()

    class _DummySub:
        async def update_all(self) -> None: ...
        async def mark_ready(self) -> None: ...
        async def update(self, key: Any, value: Any) -> Any:
            raise AssertionError("unreached")

        async def delete(self, key: Any) -> Any:
            raise AssertionError("unreached")

    first = asyncio.create_task(items.watch(_DummySub()))
    await asyncio.sleep(0.1)  # let the first watch enter the guard
    try:
        with pytest.raises(RuntimeError, match="single active watch"):
            await items.watch(_DummySub())
    finally:
        first.cancel()
        try:
            await first
        except asyncio.CancelledError:
            pass

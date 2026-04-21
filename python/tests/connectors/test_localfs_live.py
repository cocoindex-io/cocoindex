"""End-to-end tests for localfs source connector in live mode."""

from __future__ import annotations

import asyncio
from pathlib import Path

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

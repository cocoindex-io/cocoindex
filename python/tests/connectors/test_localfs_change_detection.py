"""End-to-end change detection for local files, through `coco.App`.

A file is reused when its mtime is unchanged. Filesystems stamp mtimes in ticks (NTFS
follows the system timer, HFS+ and ext3 keep whole seconds, FAT two), so a rewrite in
the same tick as the read keeps the old mtime. These tests make that happen on any
filesystem by putting the old mtime back after the rewrite.
"""

from __future__ import annotations

import os
import pathlib
import time
from collections.abc import Iterator
from typing import Any

import pytest

import cocoindex as coco
from cocoindex.connectors import localfs
from cocoindex.connectors.localfs import _source as localfs_source
from cocoindex.resources import file as file_module

from tests import common

coco_env = common.create_test_env(__file__)

_outputs: dict[str, str] = {}
_runs: list[str] = []
_content_reads: list[str] = []


class _Clock:
    """The wall clock `file_module` reads, which a test can freeze or move forward."""

    def __init__(self) -> None:
        self.frozen_at: float | None = None
        self.offset = 0.0

    def time(self) -> float:
        now = self.frozen_at if self.frozen_at is not None else time.time()
        return now + self.offset


_clock = _Clock()


@coco.fn(memo=True)
async def _process(file: localfs.File) -> None:
    text = await file.read_text()
    _runs.append(text)
    _outputs[file.file_path.path.name] = text


@coco.fn
async def _main(src: pathlib.Path) -> None:
    await coco.mount_each(_process, localfs.walk_dir(src).items())


@coco.fn
async def _main_reading_first(src: pathlib.Path) -> None:
    """Read each file, then mount it 5 s later: a slow pipeline, on the test clock."""
    async for key, file in localfs.walk_dir(src).items():
        await file.read()
        _clock.offset += 5.0
        await coco.mount(coco.component_subpath(key), _process, file)


@pytest.fixture(autouse=True)
def _test_state(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Reset the outputs and the clock, and record every read of a file's content."""
    _outputs.clear()
    _runs.clear()
    _content_reads.clear()
    _clock.frozen_at = None
    _clock.offset = 0.0
    monkeypatch.setattr(file_module, "_time", _clock)
    read_impl = localfs_source.File._read_impl

    async def counting_read_impl(self: localfs_source.File, size: int = -1) -> bytes:
        _content_reads.append(self.file_path.path.name)
        return await read_impl(self, size)

    monkeypatch.setattr(localfs_source.File, "_read_impl", counting_read_impl)
    yield


def _app(name: str, src: pathlib.Path, main: Any = _main) -> coco.App[Any, Any]:
    return coco.App(coco.AppConfig(name=name, environment=coco_env), main, src)


def _set_mtime(path: pathlib.Path, seconds_since_epoch: float) -> None:
    mtime = int(seconds_since_epoch * 1e9)
    os.utime(path, ns=(mtime, mtime))


def _rewrite_in_the_same_tick(path: pathlib.Path, content: str) -> None:
    """Rewrite `path` the way a write landing in the mtime's tick does: the mtime stays."""
    st = os.stat(path)
    path.write_text(content)
    os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns))


def test_a_rewrite_in_the_same_mtime_tick_is_reprocessed(
    tmp_path: pathlib.Path,
) -> None:
    a = tmp_path / "a.txt"
    a.write_text("version-1")
    # Read in the mtime's own tick, however slow the runner is.
    _clock.frozen_at = os.stat(a).st_mtime
    app = _app("same_tick_rewrite", tmp_path)
    app.update_blocking()
    assert _outputs == {"a.txt": "version-1"}

    _rewrite_in_the_same_tick(a, "version-2")
    app.update_blocking()
    assert _outputs == {"a.txt": "version-2"}
    assert _runs == ["version-1", "version-2"]


def test_a_rewrite_of_a_file_dated_in_the_future_is_reprocessed(
    tmp_path: pathlib.Path,
) -> None:
    """A clock skew can date a file ahead of now; its mtime is not settled either."""
    a = tmp_path / "a.txt"
    a.write_text("version-1")
    _set_mtime(a, time.time() + 3600)
    app = _app("future_mtime_rewrite", tmp_path)
    app.update_blocking()

    _rewrite_in_the_same_tick(a, "version-2")
    app.update_blocking()
    assert _outputs == {"a.txt": "version-2"}


def test_the_mtime_age_counts_from_when_the_content_was_read(
    tmp_path: pathlib.Path,
) -> None:
    """The content is read, and the memo check runs 5 s later.

    The file was fresh when its content was read, so a rewrite in its tick must still
    be caught. Dated from the memo check instead, it would pass as settled.
    """
    a = tmp_path / "a.txt"
    a.write_text("version-1")
    _clock.frozen_at = os.stat(a).st_mtime
    app = _app("read_before_check", tmp_path, _main_reading_first)
    app.update_blocking()

    _rewrite_in_the_same_tick(a, "version-2")
    app.update_blocking()
    assert _outputs == {"a.txt": "version-2"}


def test_a_settled_file_is_not_read_again(tmp_path: pathlib.Path) -> None:
    """A file last modified well before it was read is reused on its mtime alone."""
    a = tmp_path / "a.txt"
    a.write_text("settled")
    _set_mtime(a, time.time() - 3600)
    app = _app("settled_file", tmp_path)
    app.update_blocking()
    _content_reads.clear()

    app.update_blocking()
    assert _content_reads == []
    assert _runs == ["settled"]


def test_a_file_dated_near_1970_is_reused(tmp_path: pathlib.Path) -> None:
    """Some tools stamp files near the epoch, which Windows cannot convert to local time."""
    a = tmp_path / "a.txt"
    a.write_text("old")
    _set_mtime(a, 100)
    app = _app("near_epoch_file", tmp_path)
    app.update_blocking()
    _content_reads.clear()

    app.update_blocking()
    assert _content_reads == []
    assert _runs == ["old"]


def test_a_fresh_unchanged_file_is_not_reprocessed(tmp_path: pathlib.Path) -> None:
    """A just-written file is checked by content on the next run, and reused."""
    a = tmp_path / "a.txt"
    a.write_text("fresh")
    app = _app("fresh_file", tmp_path)
    app.update_blocking()
    app.update_blocking()
    assert _runs == ["fresh"]


def test_a_fresh_file_is_reused_on_its_mtime_once_settled(
    tmp_path: pathlib.Path,
) -> None:
    a = tmp_path / "a.txt"
    a.write_text("fresh")
    app = _app("fresh_then_settled", tmp_path)
    app.update_blocking()

    # Later, the same content with an mtime that has long since settled: checked by
    # content once more, and from then on reused on the mtime alone.
    _set_mtime(a, time.time() - 3600)
    app.update_blocking()
    _content_reads.clear()
    app.update_blocking()
    assert _content_reads == []
    assert _runs == ["fresh"]

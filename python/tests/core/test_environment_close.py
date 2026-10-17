"""Releasing an environment's database, so its db path can be opened again.

`Environment.close()` releases it even while something else still references
the environment. On free-threaded CPython 3.14+, a thread inherits the context
it was started in, so a thread started inside a component keeps that
component's context — and through it the environment — alive for as long as
the thread runs. The first test reproduces that reference on every Python
version by running such a thread inside a copy of the component's context. The
second takes the path real apps take: localfs walks, stats and reads files in
the event loop's default executor, whose worker threads hold the reference
until the loop closes.

The last test drops the environment instead of closing it. On the background
loop cocoindex owns, the default executor's worker threads start in an empty
context, so garbage collection alone releases the database. It runs in a fresh
interpreter: there, the app's own file reads are what start the workers, inside
the component. In this process, idle workers left behind by earlier tests would
serve the reads, and the test would pass with or without the empty context.
"""

from __future__ import annotations

import contextvars
import gc
import os
import pathlib
import subprocess
import sys
import threading
import pytest

import cocoindex as coco
from cocoindex.connectors import localfs

from tests import common

_num_squares = 0


@coco.fn(memo=True)
async def _square(x: int) -> int:
    global _num_squares
    _num_squares += 1
    return x * x


_release = threading.Event()
_threads: list[threading.Thread] = []


@coco.fn
async def _main(x: int) -> int:
    context = contextvars.copy_context()
    thread = threading.Thread(target=context.run, args=(_release.wait,), daemon=True)
    thread.start()
    _threads.append(thread)
    return await _square(x)


def test_reopen_after_close_while_thread_holds_component_context() -> None:
    try:
        _check_reopen_after_close()
    finally:
        _release.set()
        for thread in _threads:
            thread.join()


def _check_reopen_after_close() -> None:
    env = common.create_test_env(__file__, suffix="reopen")
    app = coco.App(coco.AppConfig(name="reopen", environment=env), _main, 3)
    assert app.update_blocking() == 9
    assert _num_squares == 1
    assert _threads[0].is_alive()

    db_path = env.settings.db_path
    assert db_path is not None
    # Opening a path that is still open is a caller mistake, so it is a
    # `ValueError` that names the path and the fix, not heed's bare message.
    with pytest.raises(ValueError, match="already open") as exc_info:
        coco.Environment(env.settings)
    assert os.fspath(db_path) in str(exc_info.value)

    env.close()
    env.close()
    with pytest.raises(Exception, match="environment is closed"):
        app.update_blocking()

    reopened = coco.Environment(env.settings)
    try:
        app = coco.App(coco.AppConfig(name="reopen", environment=reopened), _main, 3)
        assert app.update_blocking() == 9
        # The memo written before the close is still there.
        assert _num_squares == 1
    finally:
        reopened.close()


_num_file_reads = 0


@coco.fn(memo=True)
async def _read_file(file: localfs.File) -> str:
    global _num_file_reads
    _num_file_reads += 1
    return await file.read_text()


@coco.fn
async def _read_dir(sourcedir: pathlib.Path) -> list[str]:
    return sorted(await coco.map(_read_file, localfs.walk_dir(sourcedir)))


@pytest.mark.asyncio
async def test_reopen_after_close_once_localfs_read_files_on_the_loop(
    tmp_path: pathlib.Path,
) -> None:
    (tmp_path / "a.txt").write_text("alpha")
    (tmp_path / "b.txt").write_text("beta")

    # Created inside the test, the environment runs its apps on the test's loop.
    num_reads_before = _num_file_reads
    env = common.create_test_env(__file__, suffix="localfs")
    app = coco.App(coco.AppConfig(name="localfs", environment=env), _read_dir, tmp_path)
    assert await app.update() == ["alpha", "beta"]
    assert _num_file_reads == num_reads_before + 2

    env.close()

    reopened = coco.Environment(env.settings)
    try:
        app = coco.App(
            coco.AppConfig(name="localfs", environment=reopened), _read_dir, tmp_path
        )
        assert await app.update() == ["alpha", "beta"]
        # The memos written before the close are still there.
        assert _num_file_reads == num_reads_before + 2
    finally:
        reopened.close()


_PYTHON_DIR = pathlib.Path(__file__).resolve().parents[2]


def test_reopen_after_gc_once_localfs_read_files_on_the_background_loop(
    tmp_path: pathlib.Path,
) -> None:
    (tmp_path / "a.txt").write_text("alpha")
    (tmp_path / "b.txt").write_text("beta")
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys\n"
            "from tests.core.test_environment_close import _check_reopen_after_gc\n"
            "_check_reopen_after_gc(sys.argv[1])",
            str(tmp_path),
        ],
        check=True,
        cwd=_PYTHON_DIR,
    )


def _check_reopen_after_gc(sourcedir: str) -> None:
    """The body of the test above, run in a fresh interpreter."""
    settings = _update_with_dropped_env(pathlib.Path(sourcedir))
    assert _num_file_reads == 2
    gc.collect()

    reopened = coco.Environment(settings)
    try:
        app = coco.App(
            coco.AppConfig(name="gc", environment=reopened),
            _read_dir,
            pathlib.Path(sourcedir),
        )
        assert app.update_blocking() == ["alpha", "beta"]
        # The memos written before the environment was dropped are still there.
        assert _num_file_reads == 2
    finally:
        reopened.close()


def _update_with_dropped_env(sourcedir: pathlib.Path) -> coco.Settings:
    """Updates an app in a new environment, and leaves no reference to either."""
    # Created outside a running loop, the environment runs its apps on the
    # background loop cocoindex owns.
    env = common.create_test_env(__file__, suffix="gc")
    app = coco.App(coco.AppConfig(name="gc", environment=env), _read_dir, sourcedir)
    assert app.update_blocking() == ["alpha", "beta"]
    return env.settings

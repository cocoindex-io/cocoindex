"""Ctrl+C while a script is blocked in `App.update_blocking()` or
`App.drop_blocking()`.

While the main thread waits on the engine, it runs no Python code: CPython's
SIGINT handler only records the signal, and the `KeyboardInterrupt` it raises
waits for the main thread to run Python again. A live update never returns, so
the blocking wait has to run the pending signal handlers itself, stop the
operation, and raise what the handler raised.

The tests send SIGINT to a fresh interpreter, since what they check is how the
process ends.
"""

from __future__ import annotations

import asyncio
import contextlib
import pathlib
import queue
import signal
import subprocess
import sys
import threading
from collections.abc import Collection, Iterator
from typing import IO, Any

import pytest

import cocoindex as coco
from cocoindex.connectors import localfs

from tests import common

pytestmark = [
    pytest.mark.skipif(
        sys.platform == "win32",
        reason="sends SIGINT to a child process; Windows has CTRL_C_EVENT for a "
        "console process group instead",
    ),
    # Starting a fresh interpreter and its first update takes a few seconds.
    pytest.mark.timeout(120),
]

_PYTHON_DIR = pathlib.Path(__file__).resolve().parents[2]
_PROCESSED = "processed "
_DELETING = "deleting"
_LINE_TIMEOUT_SECS = 30
_EXIT_TIMEOUT_SECS = 10

# Runs one of the `_child_*` functions below. The child raises
# `KeyboardInterrupt` on SIGINT even if it inherited SIGINT ignored, as a
# process a shell starts in the background does.
_CHILD = """\
import signal, sys
signal.signal(signal.SIGINT, signal.default_int_handler)
from tests.core import test_blocking_update_interrupt as tests
getattr(tests, sys.argv[1])(*sys.argv[2:])
"""


class _ChildOutput:
    """The child's output lines, read on a thread as they come, so the child
    never blocks on a full pipe."""

    def __init__(self, stream: IO[str]) -> None:
        self.lines: list[str] = []
        self._pending: queue.Queue[str | None] = queue.Queue()
        threading.Thread(target=self._read, args=(stream,), daemon=True).start()

    def _read(self, stream: IO[str]) -> None:
        for line in stream:
            self._pending.put(line)
        self._pending.put(None)

    def wait_for(self, expected: str) -> None:
        while True:
            try:
                line = self._pending.get(timeout=_LINE_TIMEOUT_SECS)
            except queue.Empty:
                raise AssertionError(
                    f"no {expected!r} line from the child:\n{self.text()}"
                ) from None
            if line is None:
                raise AssertionError(
                    f"the child ended before printing {expected!r}:\n{self.text()}"
                )
            self.lines.append(line)
            if line.rstrip("\n") == expected:
                return

    def read_to_end(self) -> str:
        while (line := self._pending.get(timeout=_LINE_TIMEOUT_SECS)) is not None:
            self.lines.append(line)
        return self.text()

    def text(self) -> str:
        return "".join(self.lines)


@contextlib.contextmanager
def _child(
    child_fn: str, *args: str
) -> Iterator[tuple[subprocess.Popen[str], _ChildOutput]]:
    child = subprocess.Popen(
        [sys.executable, "-c", _CHILD, child_fn, *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=_PYTHON_DIR,
    )
    try:
        assert child.stdout is not None
        yield child, _ChildOutput(child.stdout)
    finally:
        child.kill()
        child.wait()


def _wait_for_exit(child: subprocess.Popen[str], output: _ChildOutput) -> str:
    try:
        child.wait(timeout=_EXIT_TIMEOUT_SECS)
    except subprocess.TimeoutExpired:
        raise AssertionError(
            f"the child kept running after SIGINT:\n{output.text()}"
        ) from None
    return output.read_to_end()


def _assert_ended_by_keyboard_interrupt(
    child: subprocess.Popen[str], text: str, call: str
) -> None:
    assert f"{call} raised KeyboardInterrupt" in text, text
    # Uncaught, the `KeyboardInterrupt` ends the process the way SIGINT does,
    # and nothing is left half-stopped to complain at exit.
    assert child.returncode == -signal.SIGINT, text
    assert "Exception ignored" not in text, text
    assert "Task was destroyed but it is pending" not in text, text


def test_ctrl_c_stops_a_blocking_live_update(tmp_path: pathlib.Path) -> None:
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    (source_dir / "a.txt").write_text("a")

    with _child("_child_live_update", str(source_dir)) as (child, output):
        output.wait_for(_PROCESSED + "a.txt")
        # A file processed after the initial pass shows that the update has
        # reached its live phase, the one that never returns.
        (source_dir / "b.txt").write_text("b")
        output.wait_for(_PROCESSED + "b.txt")

        child.send_signal(signal.SIGINT)
        text = _wait_for_exit(child, output)

    _assert_ended_by_keyboard_interrupt(child, text, "update_blocking")


def test_a_second_ctrl_c_stops_waiting_for_a_drop() -> None:
    """A drop has nothing to cancel: the first Ctrl+C waits for it to finish,
    and a second one stops waiting."""
    with _child("_child_drop") as (child, output):
        output.wait_for(_DELETING)

        child.send_signal(signal.SIGINT)
        with pytest.raises(subprocess.TimeoutExpired):
            child.wait(timeout=1)

        child.send_signal(signal.SIGINT)
        text = _wait_for_exit(child, output)

    _assert_ended_by_keyboard_interrupt(child, text, "drop_blocking")


def _child_live_update(source_dir: str) -> None:
    """Block in a live update until interrupted."""
    env = common.create_test_env(__file__)

    @coco.fn
    async def process_file(file: localfs.File) -> None:
        print(_PROCESSED + file.file_path.path.name, flush=True)

    @coco.fn
    async def app_main() -> None:
        files = localfs.walk_dir(pathlib.Path(source_dir), live=True)
        await coco.mount_each(process_file, files.items())

    app = coco.App(coco.AppConfig(name="live_update", environment=env), app_main)
    try:
        app.update_blocking(live=True)
    except KeyboardInterrupt:
        print("update_blocking raised KeyboardInterrupt", flush=True)
        raise


class _DeleteNeverEnds(coco.TargetHandler[Any, Any, None]):
    """Applies upserts, and blocks forever on a delete."""

    def __init__(self) -> None:
        self._sink = coco.TargetActionSink.from_async_fn(self._apply)

    async def _apply(
        self, context_provider: coco.ContextProvider, actions: Collection[Any], /
    ) -> None:
        if any(coco.is_non_existence(action) for action in actions):
            print(_DELETING, flush=True)
            await asyncio.Event().wait()

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: Any | coco.NonExistenceType,
        prev_possible_records: Collection[Any],
        prev_may_be_missing: bool,
    ) -> coco.TargetReconcileOutput[Any, Any] | None:
        if coco.is_non_existence(desired_state) and not prev_possible_records:
            return None
        return coco.TargetReconcileOutput(
            action=desired_state, sink=self._sink, tracking_record=desired_state
        )


def _child_drop() -> None:
    """Block in a drop that never ends until interrupted twice."""
    env = common.create_test_env(__file__)
    provider = coco.register_root_target_states_provider(
        "test_blocking_update_interrupt/delete_never_ends", _DeleteNeverEnds()
    )

    @coco.fn
    async def app_main() -> None:
        coco.declare_target_state(provider.target_state("key", "value"))

    app = coco.App(coco.AppConfig(name="drop", environment=env), app_main)
    app.update_blocking()
    try:
        app.drop_blocking()
    except KeyboardInterrupt:
        print("drop_blocking raised KeyboardInterrupt", flush=True)
        raise

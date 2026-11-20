"""A live update still running when the interpreter exits.

Cancelling the task that awaits a live update's `result()` does not stop the
update: it runs until the process exits. At exit, the Tokio runtime shutdown
cancels its `process_live` task, and that cancellation runs on the event loop
the environment uses, here the background loop cocoindex owns. The loop has to
run the cancellation to its end before it stops; stopped first, it closes with
the task pending, and garbage collection later destroys the task ("Task was
destroyed but it is pending!") and runs its cleanup outside its own context,
where resetting a context variable fails.

The test runs in a fresh interpreter, since what it checks happens at exit.
"""

from __future__ import annotations

import asyncio
import contextlib
import pathlib
import re
import subprocess
import sys
import threading

import cocoindex as coco

from tests import common

_PYTHON_DIR = pathlib.Path(__file__).resolve().parents[2]
_CLEANED_UP = "process_live cleaned up"

# The handler registered before cocoindex is imported runs after cocoindex's
# own exit handler: `atexit` runs handlers last in, first out.
_CHILD = """\
import atexit, time
main_returned = []
atexit.register(
    lambda: print(f"exit took {time.monotonic() - main_returned[0]:.3f}s", flush=True)
)
from tests.core.test_live_update_at_exit import _leave_live_update_running
_leave_live_update_running()
main_returned.append(time.monotonic())
"""


def test_exit_finishes_the_cancellation_of_a_running_live_update() -> None:
    child = subprocess.run(
        [sys.executable, "-c", _CHILD],
        capture_output=True,
        text=True,
        timeout=60,
        cwd=_PYTHON_DIR,
    )
    output = child.stdout + child.stderr
    assert child.returncode == 0, output
    assert "Task was destroyed but it is pending" not in output, output
    assert "was created in a different Context" not in output, output
    # The cleanup ran to its end, `await` included.
    assert _CLEANED_UP in child.stdout, output
    # With the GIL held through the runtime shutdown, the worker cancelling the
    # task waited for it, and the shutdown took its whole 5 s timeout.
    took = re.search(r"exit took ([0-9.]+)s", child.stdout)
    assert took is not None, output
    assert float(took.group(1)) < 3.0, output


def _leave_live_update_running() -> None:
    """The body of the test above, run in a fresh interpreter."""
    # Created outside a running loop, the environment uses the background loop.
    env = common.create_test_env(__file__)
    started = threading.Event()

    class _Live:
        async def process(self) -> None:
            pass

        async def process_live(self, operator: coco.LiveComponentOperator) -> None:
            await operator.update_full()
            await operator.mark_ready()
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                # A cleanup that takes more than one turn of the loop, as
                # closing a watcher does.
                await asyncio.sleep(0.2)
                print(_CLEANED_UP, flush=True)

    async def _main() -> None:
        await coco.mount(coco.component_subpath("live"), _Live)

    app = coco.App(
        coco.AppConfig(name="test_live_update_at_exit", environment=env), _main
    )

    async def _start_then_stop_awaiting() -> None:
        awaiting = asyncio.create_task(app.update(live=True).result())
        for _ in range(1000):
            if started.is_set():
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("process_live did not start")
        awaiting.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await awaiting

    asyncio.run(_start_then_stop_awaiting())

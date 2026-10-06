"""`Environment.close()` releases the environment's database even while
something else still references the environment.

On free-threaded CPython 3.14+, a thread inherits the context it was started
in, so a thread started inside a component keeps that component's context —
and through it the environment — alive for as long as the thread runs. The
tests reproduce that reference on every Python version by running such a
thread inside a copy of the component's context.
"""

from __future__ import annotations

import contextvars
import threading
import pytest

import cocoindex as coco

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

    with pytest.raises(Exception, match="already open"):
        coco.Environment(env.settings)

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

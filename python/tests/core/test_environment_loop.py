"""The background event loop is running by the time it is handed out."""

import threading

import cocoindex as coco
from cocoindex._internal.environment import _LoopRunner
from tests.common import get_env_db_path


def test_new_running_loop_is_running_on_return() -> None:
    """`Thread.start` only waits for the thread to exist, not for `run_forever`
    to begin. On a free-threaded interpreter the starter would otherwise read
    the loop as not running about half the time."""
    for _ in range(50):
        runner = _LoopRunner.create_new_running()
        thread = runner.thread
        assert thread is not None
        try:
            assert runner.loop.is_running()
        finally:
            runner.loop.call_soon_threadsafe(runner.loop.stop)
            thread.join(timeout=5)
            runner.loop.close()


def test_environment_rides_the_background_loop() -> None:
    """An environment created outside a running loop uses the shared background
    loop as is, without starting a runner of its own that would die with "This
    event loop is already running". (Only the first environment of a process
    can hit that race: it is the one that creates the background loop.)"""
    thread_errors: list[BaseException | None] = []

    def record(args: threading.ExceptHookArgs) -> None:
        thread_errors.append(args.exc_value)

    previous_hook = threading.excepthook
    threading.excepthook = record
    try:
        before = set(threading.enumerate())
        env = coco.Environment(
            coco.Settings.from_env(db_path=get_env_db_path("_environment_loop"))
        )
        assert env.event_loop.is_running()
        # A healthy runner outlives the join; a runner that failed to start
        # has reported to the hook by the time its thread is gone.
        for thread in threading.enumerate():
            if thread not in before:
                thread.join(timeout=0.5)
        env.close()
    finally:
        threading.excepthook = previous_hook
    assert thread_errors == []

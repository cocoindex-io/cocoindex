import pytest
import typing
import os
import signal
import sys


@pytest.fixture(scope="session", autouse=True)
def _cocoindex_windows_env_fixture(
    request: pytest.FixtureRequest,
) -> typing.Generator[None, None, None]:
    """Shutdown the subprocess pool at exit on Windows."""

    yield

    if not sys.platform.startswith("win"):
        return

    try:
        import cocoindex.subprocess_exec

        original_sigint_handler = signal.getsignal(signal.SIGINT)
        try:
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            cocoindex.subprocess_exec.shutdown_pool_at_exit()

            # If any test failed, let pytest exit normally with nonzero code
            if request.session.testsfailed == 0:
                os._exit(0)  # immediate success exit (skips atexit/teardown)

        finally:
            try:
                signal.signal(signal.SIGINT, original_sigint_handler)
            except ValueError:  # noqa: BLE001
                pass

    except (ImportError, AttributeError):  # noqa: BLE001
        pass

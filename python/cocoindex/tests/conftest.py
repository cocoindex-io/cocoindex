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

    print("Platform: ", sys.platform)

    yield

    print("Test done.")
    sys.stdout.flush()
    if not sys.platform.startswith("win"):
        return

    try:
        print("Shutdown the subprocess pool at exit in hook.")
        sys.stdout.flush()

        import cocoindex.subprocess_exec

        original_sigint_handler = signal.getsignal(signal.SIGINT)
        try:
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            cocoindex.subprocess_exec.shutdown_pool_at_exit()

            # If any test failed, let pytest exit normally with nonzero code
            if request.session.testsfailed == 0:
                print("Exit with success.")
                sys.stdout.flush()

                sys.exit(0)

        finally:
            try:
                signal.signal(signal.SIGINT, original_sigint_handler)
            except ValueError:  # noqa: BLE001
                pass

    except (ImportError, AttributeError):  # noqa: BLE001
        pass

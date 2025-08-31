import pytest
import typing
import os
import signal


@pytest.fixture(scope="session", autouse=True)
def _cocoindex_env_fixture() -> typing.Generator[None, None, None]:
    """Shutdown the subprocess pool at exit."""

    yield

    try:
        print("Shutdown the subprocess pool at exit in hook.")
        import cocoindex.subprocess_exec

        if os.name == "nt":
            original_sigint_handler = signal.getsignal(signal.SIGINT)
            try:
                signal.signal(signal.SIGINT, signal.SIG_IGN)
                cocoindex.subprocess_exec.shutdown_pool_at_exit()
            finally:
                try:
                    signal.signal(signal.SIGINT, original_sigint_handler)
                except ValueError:  # noqa: BLE001
                    pass
        else:
            cocoindex.subprocess_exec.shutdown_pool_at_exit()
    except (ImportError, AttributeError):  # noqa: BLE001
        pass

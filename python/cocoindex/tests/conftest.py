import pytest
import typing


@pytest.fixture(scope="session", autouse=True)
def _cocoindex_env_fixture() -> typing.Generator[None, None, None]:
    """Shutdown the subprocess pool at exit."""
    yield
    try:
        print("Shutdown the subprocess pool at exit in hook.")
        import cocoindex.subprocess_exec

        cocoindex.subprocess_exec.shutdown_pool_at_exit()
    except Exception:
        pass

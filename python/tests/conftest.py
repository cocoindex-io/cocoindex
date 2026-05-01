import contextlib
import os
import time

import pytest

from cocoindex._internal.serde import enable_strict_serialize

os.environ.setdefault("PYTHONASYNCIODEBUG", "1")
enable_strict_serialize()


def _is_docker_available() -> bool:
    """Check whether a Docker daemon is reachable.

    Used to skip (rather than error) tests that need testcontainers when no
    Docker socket exists — e.g. macOS GitHub runners that ship without a
    running daemon.
    """
    try:
        import docker  # type: ignore[import-untyped]
    except ImportError:
        return False
    try:
        client = docker.from_env(timeout=2)
        client.ping()
        return True
    except Exception:
        return False


_DOCKER_AVAILABLE = _is_docker_available()


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "requires_docker: test needs a running Docker daemon (e.g. testcontainers)",
    )


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    if _DOCKER_AVAILABLE:
        return
    skip_marker = pytest.mark.skip(reason="Docker daemon not available")
    for item in items:
        if item.get_closest_marker("requires_docker") is not None:
            item.add_marker(skip_marker)


def _install_testcontainers_reaper_retry() -> None:
    """Retry testcontainers' ryuk setup on port-mapping races.

    Docker (especially on macOS) occasionally has not yet populated a
    container's port mapping by the time ``docker port <id> 8080`` is queried
    right after ``docker run`` returns. That makes ``Reaper._create_instance``
    raise ``ConnectionError: Port mapping ... and port 8080 is not available``
    intermittently, failing any test that uses testcontainers.

    Wrap it with bounded exponential backoff, tearing down the partially-started
    ryuk container between attempts so we don't leak it.
    """
    try:
        from testcontainers.core.container import Reaper  # type: ignore[import-untyped]
    except ImportError:
        return

    original = Reaper._create_instance

    def _create_instance_with_retry() -> Reaper:
        last_exc: Exception | None = None
        for attempt in range(5):
            try:
                return original()
            except Exception as e:
                last_exc = e
                if Reaper._container is not None:
                    with contextlib.suppress(Exception):
                        Reaper._container.stop()
                    Reaper._container = None
                time.sleep(min(0.1 * (2**attempt), 1.0))
        assert last_exc is not None
        raise last_exc

    Reaper._create_instance = _create_instance_with_retry  # type: ignore[method-assign]


_install_testcontainers_reaper_retry()

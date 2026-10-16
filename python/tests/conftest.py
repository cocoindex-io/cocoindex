import contextlib
import os
import time
import warnings

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


def _is_statically_skipped(item: pytest.Item) -> bool:
    """Whether a ``skip``/``skipif(<bool>)`` marker already rules the item out.

    Only bool conditions are evaluated; string conditions count as not skipped.
    """
    if item.get_closest_marker("skip") is not None:
        return True
    return any(
        isinstance(cond, bool) and cond
        for marker in item.iter_markers("skipif")
        for cond in (*marker.args, marker.kwargs.get("condition", False))
    )


def _pull_missing_docker_images(session: pytest.Session) -> None:
    """Pull the images that selected ``requires_docker`` tests need, up front.

    testcontainers pulls a missing image inside the fixture that starts the
    container, so on a runner with a cold image cache the pull is charged to
    the first test's ``pytest-timeout`` budget; a slow registry then errors
    every test in the module. Pulling here, between collection and the first
    test, keeps download time out of every per-test timeout while still
    bounding container startup itself.
    """
    images = {
        image
        for item in session.items
        if not _is_statically_skipped(item)
        for marker in item.iter_markers("requires_docker")
        for image in marker.args
    }
    if not images:
        return

    import docker  # type: ignore[import-untyped]
    from docker.errors import ImageNotFound  # type: ignore[import-untyped]

    client = docker.from_env()
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    for image in sorted(images):
        try:
            client.images.get(image)
            continue
        except ImageNotFound:
            pass
        if reporter is not None:
            reporter.write_line(f"Pulling docker image {image} ...")
        start = time.monotonic()
        try:
            client.images.pull(image)
        except Exception as e:
            # Leave it to the fixture: it retries the pull and fails only its
            # own tests, rather than this aborting the whole session.
            warnings.warn(f"Failed to pre-pull docker image {image}: {e}")
            continue
        if reporter is not None:
            reporter.write_line(
                f"Pulled docker image {image} in {time.monotonic() - start:.1f}s"
            )


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "requires_docker(*images): test needs a running Docker daemon "
        "(e.g. testcontainers); the listed images are pulled before any test runs",
    )


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    if _DOCKER_AVAILABLE:
        return
    skip_marker = pytest.mark.skip(reason="Docker daemon not available")
    for item in items:
        if item.get_closest_marker("requires_docker") is not None:
            item.add_marker(skip_marker)


def pytest_collection_finish(session: pytest.Session) -> None:
    # Runs after deselection (`-k`, `-m`), so only selected tests' images are pulled.
    if _DOCKER_AVAILABLE and not session.config.option.collectonly:
        _pull_missing_docker_images(session)


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
        from testcontainers.core.container import Reaper  # type: ignore[import-not-found, import-untyped]
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

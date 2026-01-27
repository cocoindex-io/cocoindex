from contextlib import contextmanager
import os
from typing import Iterator
import pytest

import cocoindex as coco
from cocoindex._internal.environment import reset_default_env_for_tests
from tests.common import get_env_db_path

_env_db_path = get_env_db_path("_default")
_env_db_path_from_env_var = get_env_db_path("_default_from_env_var")


class _Resource:
    pass


_RESOURCE_KEY = coco.ContextKey[_Resource]("test_default_env/resource")

_num_active_resources = 0


@contextmanager
def _acquire_resource() -> Iterator[_Resource]:
    global _num_active_resources
    _num_active_resources += 1
    yield _Resource()
    _num_active_resources -= 1


@pytest.fixture(scope="module")
def _default_env() -> Iterator[None]:
    try:

        @coco.lifespan
        def default_lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
            builder.settings.db_path = _env_db_path
            builder.provide_with(_RESOURCE_KEY, _acquire_resource())
            yield

        yield
    finally:
        reset_default_env_for_tests()


def test_default_env(_default_env: None) -> None:
    assert not _env_db_path.exists()
    with coco.runtime():
        coco.default_env()
    assert _env_db_path.exists()


def _trivial_fn(_scope: coco.Scope, s: str, i: int) -> str:
    assert isinstance(_scope.use(_RESOURCE_KEY), _Resource)
    return f"{s} {i}"


def test_app(_default_env: None) -> None:
    app = coco.App(
        _trivial_fn,
        coco.AppConfig(name="trivial_app"),
        "Hello",
        1,
    )

    assert _num_active_resources == 0
    with coco.runtime():
        assert app.update() == "Hello 1"
        assert _num_active_resources == 1
    assert _num_active_resources == 0


def test_app_implicit_startup(_default_env: None) -> None:
    app = coco.App(
        _trivial_fn,
        coco.AppConfig(name="trivial_app_implicit_startup"),
        "Hello",
        1,
    )

    assert _num_active_resources == 0
    assert app.update() == "Hello 1"
    assert _num_active_resources == 1


# =============================================================================
# Test: Default DB path from COCOINDEX_DB environment variable
# =============================================================================


@pytest.fixture(scope="function")
def _default_env_from_env_var() -> Iterator[None]:
    """
    Fixture that sets COCOINDEX_DB env var and uses a lifespan that does NOT
    set db_path explicitly.
    """
    # Reset any previously initialized default environment
    reset_default_env_for_tests()

    old_env = os.environ.get("COCOINDEX_DB")
    os.environ["COCOINDEX_DB"] = str(_env_db_path_from_env_var)

    try:
        # Lifespan that does NOT set db_path - relies on COCOINDEX_DB env variable
        @coco.lifespan
        def lifespan_without_db_path(
            _builder: coco.EnvironmentBuilder,
        ) -> Iterator[None]:
            yield

        yield
    finally:
        reset_default_env_for_tests()
        if old_env is not None:
            os.environ["COCOINDEX_DB"] = old_env
        else:
            os.environ.pop("COCOINDEX_DB", None)


def _simple_fn(_scope: coco.Scope, s: str) -> str:
    return f"result: {s}"


def test_default_env_uses_cocoindex_db_env_var(_default_env_from_env_var: None) -> None:
    """Test that default env uses COCOINDEX_DB when lifespan doesn't set db_path."""
    assert not _env_db_path_from_env_var.exists()
    with coco.runtime():
        env = coco.default_env()._get_env_sync()
        assert env.settings.db_path == _env_db_path_from_env_var
    assert _env_db_path_from_env_var.exists()


def test_app_uses_cocoindex_db_env_var(_default_env_from_env_var: None) -> None:
    """Test that app works when using COCOINDEX_DB env var for db_path."""
    app = coco.App(
        _simple_fn,
        coco.AppConfig(name="app_with_env_var_db"),
        "test",
    )

    with coco.runtime():
        result = app.update()
        assert result == "result: test"

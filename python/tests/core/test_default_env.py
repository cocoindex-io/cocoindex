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
    def __init__(self, generation: int) -> None:
        self.generation = generation
        self.closed = False


_RESOURCE_KEY = coco.ContextKey[_Resource]("test_default_env/resource")

_num_active_resources = 0
_num_resource_generations = 0


@contextmanager
def _acquire_resource() -> Iterator[_Resource]:
    global _num_active_resources, _num_resource_generations
    _num_active_resources += 1
    _num_resource_generations += 1
    resource = _Resource(_num_resource_generations)
    try:
        yield resource
    finally:
        resource.closed = True
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
        pass
    assert _env_db_path.exists()


def _trivial_fn(s: str, i: int) -> str:
    resource = coco.use_context(_RESOURCE_KEY)
    assert isinstance(resource, _Resource)
    assert not resource.closed
    return f"{s} {i}"


def _resource_generation() -> int:
    resource = coco.use_context(_RESOURCE_KEY)
    assert not resource.closed
    return resource.generation


def test_app(_default_env: None) -> None:
    app = coco.App(
        coco.AppConfig(name="trivial_app"),
        _trivial_fn,
        "Hello",
        1,
    )

    assert _num_active_resources == 0
    with coco.runtime():
        assert app.update_blocking() == "Hello 1"
        assert _num_active_resources == 1
    assert _num_active_resources == 0


def test_app_implicit_startup(_default_env: None) -> None:
    app = coco.App(
        coco.AppConfig(name="trivial_app_implicit_startup"),
        _trivial_fn,
        "Hello",
        1,
    )

    assert _num_active_resources == 0
    try:
        assert app.update_blocking() == "Hello 1"
        assert _num_active_resources == 1
    finally:
        coco.stop_blocking()


def test_retained_app_across_runtime_cycles(_default_env: None) -> None:
    app = coco.App("retained_sync_app", _resource_generation)
    generations: list[int] = []

    for _ in range(3):
        with coco.runtime():
            generation = app.update_blocking()
            assert isinstance(generation, int)
            generations.append(generation)
            assert _num_active_resources == 1
        assert _num_active_resources == 0

    assert generations[1] == generations[0] + 1
    assert generations[2] == generations[1] + 1


def test_retained_app_implicitly_restarts_after_stop(_default_env: None) -> None:
    app = coco.App("retained_sync_implicit_restart", _resource_generation)

    with coco.runtime():
        first_generation = app.update_blocking()
        assert isinstance(first_generation, int)

    second_generation = app.update_blocking()
    try:
        assert isinstance(second_generation, int)
        assert second_generation == first_generation + 1
        assert _num_active_resources == 1
    finally:
        coco.stop_blocking()


def test_stop_edge_cases_and_registration_survive_restart(
    _default_env: None,
) -> None:
    coco.stop_blocking()
    coco.stop_blocking()

    app = coco.App("retained_sync_registration", _resource_generation)
    never_initialized = coco.App(
        "retained_sync_never_initialized", _resource_generation
    )
    with coco.runtime():
        app.update_blocking()

    with pytest.raises(ValueError, match="already registered"):
        coco.App("retained_sync_registration", _resource_generation)

    with coco.runtime():
        app_generation = app.update_blocking()
        generation = never_initialized.update_blocking()
        assert isinstance(app_generation, int)
        assert isinstance(generation, int)
        assert app_generation == generation
        assert generation > 0


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


def _simple_fn(s: str) -> str:
    return f"result: {s}"


@pytest.mark.asyncio
async def test_default_env_uses_cocoindex_db_env_var(
    _default_env_from_env_var: None,
) -> None:
    """Test that default env uses COCOINDEX_DB when lifespan doesn't set db_path."""
    assert not _env_db_path_from_env_var.exists()
    async with coco.runtime():
        env = await coco.default_env()
        assert env.settings.db_path == _env_db_path_from_env_var
    assert _env_db_path_from_env_var.exists()


def test_app_uses_cocoindex_db_env_var(_default_env_from_env_var: None) -> None:
    """Test that app works when using COCOINDEX_DB env var for db_path."""
    app = coco.App(
        coco.AppConfig(name="app_with_env_var_db"),
        _simple_fn,
        "test",
    )

    with coco.runtime():
        result = app.update_blocking()
        assert result == "result: test"

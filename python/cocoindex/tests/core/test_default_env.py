from contextlib import contextmanager
from typing import Iterator
import pytest

import cocoindex as coco
from cocoindex._internal.environment import reset_default_lifespan_for_tests
from ..common import get_env_db_path

_env_db_path = get_env_db_path("_default")


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
        reset_default_lifespan_for_tests()


def test_default_env(_default_env: None) -> None:
    assert not _env_db_path.exists()
    with coco.runtime():
        coco.default_env()
    assert _env_db_path.exists()


@coco.function()
def _trivial_fn(_scope: coco.Scope, s: str, i: int) -> str:
    assert isinstance(_scope.use(_RESOURCE_KEY), _Resource)
    return f"{s} {i}"


def test_app_in_default_env(_default_env: None) -> None:
    app = coco.App(
        _trivial_fn,
        coco.AppConfig(name="trivial_app"),
        "Hello",
        1,
    )

    assert _num_active_resources == 0
    with coco.runtime():
        assert app.run() == "Hello 1"
        assert _num_active_resources == 1
    assert _num_active_resources == 0

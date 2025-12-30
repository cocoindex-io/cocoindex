from typing import Iterator
import pytest

import cocoindex as coco
from cocoindex._internal.environment import clear_default_env
from .common import get_env_db_path

_env_db_path = get_env_db_path("_default")


@pytest.fixture(scope="module")
def _default_env() -> Iterator[None]:
    try:

        @coco.lifespan
        def default_lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
            builder.settings.db_path = _env_db_path
            yield

        yield
    finally:
        clear_default_env()


def test_default_env(_default_env: None) -> None:
    assert not _env_db_path.exists()
    coco.default_env()
    assert _env_db_path.exists()


@coco.function()
def trivial_fn(scope: coco.Scope, s: str, i: int) -> str:
    return f"{s} {i}"


def test_app_in_default_env(_default_env: None) -> None:
    app = coco.App("trivial_app", trivial_fn)
    assert app.run("Hello", 1) == "Hello 1"

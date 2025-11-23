import cocoindex
from .environment import get_env_db_path
from typing import Iterator

_env_db_path = get_env_db_path("_default")


@cocoindex.lifespan
def default_lifespan(builder: cocoindex.EnvironmentBuilder) -> Iterator[None]:
    builder.settings.db_path = _env_db_path
    yield


def test_default_env() -> None:
    assert not _env_db_path.exists()
    cocoindex.default_env()
    assert _env_db_path.exists()


@cocoindex.function()
def trivial_fn(csp: cocoindex.StatePath, s: str, i: int) -> str:
    return f"{s} {i}"


def test_trivial_app() -> None:
    app = cocoindex.App(trivial_fn, "trivial_app", "Hello", 1)
    assert app.update() == "Hello 1"

import cocoindex
import tempfile
import pathlib

from typing import Iterator

tmp_db_path = pathlib.Path(tempfile.mkdtemp()) / "cocoindex_test"


@cocoindex.lifespan
def lifespan(builder: cocoindex.EnvironmentBuilder) -> Iterator[None]:
    builder.settings.db_path = tmp_db_path
    yield


def test_init() -> None:
    assert not tmp_db_path.exists()
    cocoindex.default_env()
    assert tmp_db_path.exists()

import pathlib
import tempfile
from typing import Iterator
from logging import getLogger

import cocoindex

_logger = getLogger(__name__)

_tmp_db_path_base = pathlib.Path(tempfile.mkdtemp()) / "cocoindex_test"
_logger.info(f"Temporary database path base: {_tmp_db_path_base}")


def get_env_db_path(name: str) -> pathlib.Path:
    return _tmp_db_path_base / name


def create_test_env(name: str) -> cocoindex.Environment:
    def lifespan(builder: cocoindex.EnvironmentBuilder) -> Iterator[None]:
        builder.settings.db_path = get_env_db_path(name)
        yield

    return cocoindex.Environment(lifespan)

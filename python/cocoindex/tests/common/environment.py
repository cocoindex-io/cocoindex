import pathlib
import tempfile
from typing import Iterator
from logging import getLogger

import cocoindex

_logger = getLogger(__name__)

_tmp_db_path_base = pathlib.Path(tempfile.mkdtemp()) / "cocoindex_test"
_logger.info("Temporary database path base: %s", _tmp_db_path_base)


def get_env_db_path(name: str) -> pathlib.Path:
    return _tmp_db_path_base / name


_PATH_PREFIX = str(pathlib.Path(__file__).parent.parent) + "/"


def create_test_env(test_file_path: str) -> cocoindex.Environment:
    base_name = (
        test_file_path.removeprefix(_PATH_PREFIX).removesuffix(".py").replace("/", "__")
    )

    def lifespan(builder: cocoindex.EnvironmentBuilder) -> Iterator[None]:
        builder.settings.db_path = get_env_db_path(base_name)
        yield

    return cocoindex.Environment(lifespan)

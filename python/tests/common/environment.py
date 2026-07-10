import pathlib
import tempfile
from logging import getLogger

import cocoindex

_logger = getLogger(__name__)

_tmp_db_path_base = pathlib.Path(tempfile.mkdtemp()) / "cocoindex_test"
_logger.info("Temporary database path base: %s", _tmp_db_path_base)


def get_env_db_path(name: str) -> pathlib.Path:
    return _tmp_db_path_base / name


def create_test_env(
    test_file_path: str,
    suffix: str | None = None,
    *,
    exception_handler: cocoindex.ExceptionHandler | None = None,
) -> cocoindex.Environment:
    tests_dir = pathlib.Path(__file__).parent.parent.resolve()
    test_path = pathlib.Path(test_file_path).resolve()
    try:
        rel_path = test_path.relative_to(tests_dir)
        base_name = str(rel_path.with_suffix("")).replace("\\", "__").replace("/", "__")
    except ValueError:
        base_name = test_path.name.removesuffix(".py")

    if suffix is not None:
        base_name = f"{base_name}__{suffix}"
    settings = cocoindex.Settings.from_env(db_path=get_env_db_path(base_name))
    return cocoindex.Environment(settings, exception_handler=exception_handler)

"""Test module with same app name in different environments.

This tests that apps with the same name can coexist in different environments,
one using an explicit named environment and one using the default environment:
  alpha (./db_alpha/cocoindex.db):
    MyApp
  default (cocoindex.db):
    MyApp

Apps can be disambiguated using the @env_name syntax:
  cocoindex update ./same_name_diff_env.py:MyApp@alpha
  cocoindex update ./same_name_diff_env.py:MyApp@default
"""

from __future__ import annotations

import pathlib
from typing import Iterator

import cocoindex as coco
from cocoindex.connectors.localfs import declare_dir_target

_HERE = pathlib.Path(__file__).resolve().parent

# Explicit environment with a name
DB_DIR_ALPHA = _HERE / "db_alpha"
DB_PATH_ALPHA = DB_DIR_ALPHA / "cocoindex.db"
OUT_DIR_ALPHA = _HERE / "out_alpha"
OUT_DIR_DEFAULT = _HERE / "out_default"

# Create directory for alpha env
DB_DIR_ALPHA.mkdir(exist_ok=True)

# Named environment
env_alpha = coco.Environment(
    coco.Settings.from_env(db_path=DB_PATH_ALPHA), name="alpha"
)


# Configure the default environment via lifespan
@coco.lifespan
def _lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
    builder.settings.db_path = _HERE / "cocoindex.db"
    yield


@coco.function
def build_alpha() -> None:
    dir_target = coco.mount_run(
        coco.component_subpath("out"),
        declare_dir_target,
        OUT_DIR_ALPHA,
    ).result()
    dir_target.declare_file("output.txt", "From Alpha env\n")


@coco.function
def build_default() -> None:
    dir_target = coco.mount_run(
        coco.component_subpath("out"),
        declare_dir_target,
        OUT_DIR_DEFAULT,
    ).result()
    dir_target.declare_file("output.txt", "From Default env\n")


# Two apps with THE SAME NAME but in different environments
# One uses explicit named environment, one uses default environment
app_alpha = coco.App(coco.AppConfig(name="MyApp", environment=env_alpha), build_alpha)
app_default = coco.App("MyApp", build_default)  # Uses default environment

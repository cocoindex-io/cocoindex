"""Test module with multiple environments using same db filename in different directories.

This tests the ls output grouping when both envs use 'cocoindex.db' but in different paths:
  ./db1/cocoindex.db:
    App1
  ./db2/cocoindex.db:
    App2
"""

from __future__ import annotations

import pathlib

import cocoindex as coco
from cocoindex.connectors.localfs import declare_dir_target

_HERE = pathlib.Path(__file__).resolve().parent

# Two different directories, both using cocoindex.db as the filename
DB_DIR_1 = _HERE / "db1"
DB_DIR_2 = _HERE / "db2"
DB_PATH_1 = DB_DIR_1 / "cocoindex.db"
DB_PATH_2 = DB_DIR_2 / "cocoindex.db"
OUT_DIR_1 = _HERE / "out_db1"
OUT_DIR_2 = _HERE / "out_db2"

# Create directories if they don't exist
DB_DIR_1.mkdir(exist_ok=True)
DB_DIR_2.mkdir(exist_ok=True)

env1 = coco.Environment(coco.Settings.from_env(db_path=DB_PATH_1))
env2 = coco.Environment(coco.Settings.from_env(db_path=DB_PATH_2))


@coco.function
def build1() -> None:
    dir_target = coco.mount_run(
        coco.component_subpath("out"),
        declare_dir_target,
        OUT_DIR_1,
    ).result()
    dir_target.declare_file("db1.txt", "Hello from DB1App\n")


@coco.function
def build2() -> None:
    dir_target = coco.mount_run(
        coco.component_subpath("out"),
        declare_dir_target,
        OUT_DIR_2,
    ).result()
    dir_target.declare_file("db2.txt", "Hello from DB2App\n")


# Two apps in different environments (different directories, same db filename)
app1 = coco.App(coco.AppConfig(name="DB1App", environment=env1), build1)
app2 = coco.App(coco.AppConfig(name="DB2App", environment=env2), build2)

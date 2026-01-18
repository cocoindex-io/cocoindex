"""Test module with multiple apps to demonstrate app specifier syntax."""

from __future__ import annotations

import pathlib

import cocoindex as coco
from cocoindex.connectors.localfs import declare_dir_target

# Shared database path for all apps
_HERE = pathlib.Path(__file__).resolve().parent
DB_PATH = _HERE / "cocoindex.db"
OUT_DIR_1 = _HERE / "out_multi_1"
OUT_DIR_2 = _HERE / "out_multi_2"

env = coco.Environment(coco.Settings.from_env(db_path=DB_PATH))


@coco.function
def build1(scope: coco.Scope) -> None:
    dir_target = coco.mount_run(
        declare_dir_target,
        scope / "out",
        OUT_DIR_1,
        stable_key="out_dir",
        managed_by="system",
    ).result()
    dir_target.declare_file(
        scope, filename="hello.txt", content="Hello from MultiApp1\n"
    )


@coco.function
def build2(scope: coco.Scope) -> None:
    dir_target = coco.mount_run(
        declare_dir_target,
        scope / "out",
        OUT_DIR_2,
        stable_key="out_dir",
        managed_by="system",
    ).result()
    dir_target.declare_file(
        scope, filename="world.txt", content="Hello from MultiApp2\n"
    )


# Two apps in the same module
app1 = coco.App(build1, coco.AppConfig(name="MultiApp1", environment=env))
app2 = coco.App(build2, coco.AppConfig(name="MultiApp2", environment=env))

# Default app (what gets run if you don't specify :app_name)
app = app1

"""Test module with a SINGLE app - should auto-select without specifier."""

from __future__ import annotations

import pathlib

import cocoindex as coco
from cocoindex.connectors.localfs import declare_dir_target

_HERE = pathlib.Path(__file__).resolve().parent
DB_PATH = _HERE / "cocoindex.db"
OUT_DIR = _HERE / "out_single"

env = coco.Environment(coco.Settings.from_env(db_path=DB_PATH))


@coco.function
def build(scope: coco.Scope) -> None:
    dir_target = coco.mount_run(
        declare_dir_target,
        scope / "out",
        OUT_DIR,
        stable_key="out_dir",
        managed_by="system",
    ).result()
    dir_target.declare_file(
        scope, filename="single.txt", content="Hello from SingleApp\n"
    )


# Single app - should be auto-selected even without :app_name specifier
only_app = coco.App(build, coco.AppConfig(name="SingleApp", environment=env))

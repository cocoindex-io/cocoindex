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
def build() -> None:
    dir_target = coco.mount_run(
        coco.component_subpath("out"),
        declare_dir_target,
        OUT_DIR,
        stable_key="out_dir",
        managed_by="system",
    ).result()
    dir_target.declare_file(filename="single.txt", content="Hello from SingleApp\n")


# Single app - should be auto-selected even without :app_name specifier
only_app = coco.App(coco.AppConfig(name="SingleApp", environment=env), build)

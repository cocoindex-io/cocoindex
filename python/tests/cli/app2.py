"""Simple test app 2 - shares database with app1."""

from __future__ import annotations

import pathlib

import cocoindex as coco
from cocoindex.connectors.localfs import declare_dir_target

# Shared database path for both apps (same as app1)
_HERE = pathlib.Path(__file__).resolve().parent
DB_PATH = _HERE / "cocoindex.db"
OUT_DIR = _HERE / "out_app2"

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
    dir_target.declare_file(scope, filename="world.txt", content="Hello from App2\n")


app = coco.App(build, coco.AppConfig(name="TestApp2", environment=env))

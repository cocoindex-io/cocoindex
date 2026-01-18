"""Simple test app 1 - shares database with app2."""

from __future__ import annotations

import pathlib

import cocoindex as coco
from cocoindex.connectors.localfs import declare_dir_target

# Shared database path for both apps
_HERE = pathlib.Path(__file__).resolve().parent
DB_PATH = _HERE / "cocoindex.db"
OUT_DIR = _HERE / "out_app1"

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
    dir_target.declare_file(scope, filename="hello.txt", content="Hello from App1\n")


app = coco.App(build, coco.AppConfig(name="TestApp1", environment=env))

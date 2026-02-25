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
async def build() -> None:
    dir_target = await coco.use_mount(
        coco.component_subpath("out"),
        declare_dir_target,
        OUT_DIR,
    )
    dir_target.declare_file("hello.txt", "Hello from App1\n")


app = coco.App(coco.AppConfig(name="TestApp1", environment=env), build)

"""Test module that uses default db path from COCOINDEX_DB environment variable."""

from __future__ import annotations

import pathlib

import cocoindex as coco
from cocoindex.connectors.localfs import declare_dir_target

_HERE = pathlib.Path(__file__).resolve().parent
OUT_DIR = _HERE / "out_default_db"


@coco.function
def build() -> None:
    dir_target = coco.mount_run(
        coco.component_subpath("out"),
        declare_dir_target,
        OUT_DIR,
    ).result()
    dir_target.declare_file("default_db.txt", "Hello from DefaultDbApp\n")


app = coco.App(coco.AppConfig(name="DefaultDbApp"), build)

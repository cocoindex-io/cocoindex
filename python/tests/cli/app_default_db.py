"""Test module that uses default db path from COCOINDEX_DB environment variable."""

from __future__ import annotations

import pathlib

import cocoindex as coco
from cocoindex.connectors.localfs import declare_dir_target

_HERE = pathlib.Path(__file__).resolve().parent
OUT_DIR = _HERE / "out_default_db"


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
        scope, filename="default_db.txt", content="Hello from DefaultDbApp\n"
    )


app = coco.App(build, coco.AppConfig(name="DefaultDbApp"))

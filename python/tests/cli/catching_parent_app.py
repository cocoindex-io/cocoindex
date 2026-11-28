"""Test module with an app whose parent catches a use_mount failure."""

from __future__ import annotations

import pathlib

import cocoindex as coco

_HERE = pathlib.Path(__file__).resolve().parent
DB_PATH = _HERE / "cocoindex_catching_parent.db"

env = coco.Environment(coco.Settings.from_env(db_path=DB_PATH))


@coco.fn
async def boom(i: int) -> None:
    raise ValueError(f"item {i} is poison")


@coco.fn
async def app_main() -> None:
    try:
        await coco.use_mount(coco.component_subpath("boom"), boom, 0)
    except ValueError:
        pass


app = coco.App(coco.AppConfig(name="CatchingParentApp", environment=env), app_main)

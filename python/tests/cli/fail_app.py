"""Test app where every mounted component fails — for exit-code testing."""

from __future__ import annotations

import pathlib

import cocoindex as coco

_HERE = pathlib.Path(__file__).resolve().parent
DB_PATH = _HERE / "cocoindex_failapp.db"

env = coco.Environment(coco.Settings.from_env(db_path=DB_PATH))


@coco.fn
async def boom(i: int) -> None:
    raise ValueError(f"item {i} is poison")


@coco.fn
async def app_main() -> None:
    for i in range(3):
        await coco.mount(coco.component_subpath("boom", str(i)), boom, i)


app = coco.App(coco.AppConfig(name="FailApp", environment=env), app_main)

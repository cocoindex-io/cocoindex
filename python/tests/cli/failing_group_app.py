"""Test module with an app whose background components fail inside a stats group."""

from __future__ import annotations

import pathlib

import cocoindex as coco

_HERE = pathlib.Path(__file__).resolve().parent
DB_PATH = _HERE / "cocoindex_failing_group.db"

env = coco.Environment(coco.Settings.from_env(db_path=DB_PATH))


@coco.fn
async def boom(i: int) -> None:
    raise ValueError(f"item {i} is poison")


@coco.fn
async def app_main() -> None:
    with coco.stats_group("grp"):
        for i in range(3):
            await coco.mount(coco.component_subpath("boom", str(i)), boom, i)


app = coco.App(coco.AppConfig(name="FailingGroupApp", environment=env), app_main)

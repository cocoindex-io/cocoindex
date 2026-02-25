"""Test module with a memoized app for testing --full-reprocess flag."""

from __future__ import annotations

import pathlib
from datetime import datetime, timezone
from typing import Iterator

import cocoindex as coco
from cocoindex.connectors.localfs import declare_dir_target, DirTarget


@coco.lifespan
def coco_lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
    builder.settings.db_path = pathlib.Path("./cocoindex.db")
    yield


@coco.function(memo=True)
def write_timestamp(target: DirTarget) -> None:
    # If memoization hits, this function won't re-run and the file won't change.
    now = datetime.now(timezone.utc).isoformat()
    target.declare_file("stamp.txt", now)


@coco.function
async def app_main() -> None:
    target = await coco.use_mount(
        coco.component_subpath("setup"),
        declare_dir_target,
        pathlib.Path("./out_memo"),
    )
    await coco.mount(coco.component_subpath("write"), write_timestamp, target)


app = coco.App(
    coco.AppConfig(name="MemoApp"),
    app_main,
)

"""Test module for full reprocess behavior."""

from __future__ import annotations

import pathlib
from typing import Iterator

import cocoindex as coco
from cocoindex.connectors.localfs import declare_dir_target, DirTarget


@coco.lifespan
def coco_lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
    builder.settings.db_path = pathlib.Path("./cocoindex_full_reprocess.db")
    yield


@coco.function
def create_targets(target: DirTarget, create_b: bool) -> None:
    """Create target files A and optionally B."""
    target.declare_file("target_a.txt", "content_a")
    if create_b:
        target.declare_file("target_b.txt", "content_b")


@coco.function
async def app_main(create_b: bool = True) -> None:
    """Main app function that creates targets A and optionally B."""
    target = await coco.use_mount(
        coco.component_subpath("setup"),
        declare_dir_target,
        pathlib.Path("./out_full_reprocess"),
    )
    await coco.mount(coco.component_subpath("create"), create_targets, target, create_b)


app = coco.App(
    coco.AppConfig(name="FullReprocessApp"),
    app_main,
)

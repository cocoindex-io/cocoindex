"""Test module with multiple apps to demonstrate app specifier syntax."""

from __future__ import annotations

import pathlib
from typing import AsyncGenerator

import cocoindex as coco
from cocoindex.connectors.localfs import declare_dir_target


_ROOT_PATH = coco.ContextKey[pathlib.Path]("root_path")


@coco.lifespan
async def lifespan(builder: coco.EnvironmentBuilder) -> AsyncGenerator[None]:
    root_path = pathlib.Path(__file__).resolve().parent

    builder.provide(_ROOT_PATH, root_path)
    builder.settings.db_path = root_path / "cocoindex.db"
    yield


@coco.function
async def build1() -> None:
    dir_target = await coco.use_mount(
        coco.component_subpath("out"),
        declare_dir_target,
        coco.use_context(_ROOT_PATH) / "out_multi_1",
    )
    dir_target.declare_file("hello.txt", "Hello from MultiApp1\n")


@coco.function
async def build2() -> None:
    dir_target = await coco.use_mount(
        coco.component_subpath("out"),
        declare_dir_target,
        coco.use_context(_ROOT_PATH) / "out_multi_2",
    )
    dir_target.declare_file("world.txt", "Hello from MultiApp2\n")


# Two apps in the same module
app1 = coco.App("MultiApp1", build1)
app2 = coco.App("MultiApp2", build2)

# Default app (what gets run if you don't specify :app_name)
app = app1

"""Test app for tree display with non-component intermediate nodes.

This app creates a tree structure with various nodes:
- Root (/)
- /"group"
- /"group"/"item1"
- /"group"/"item2"
- /"direct"
"""

from __future__ import annotations

import pathlib

import cocoindex as coco
from cocoindex.connectors.localfs import declare_dir_target, DirTarget

_HERE = pathlib.Path(__file__).resolve().parent
DB_PATH = _HERE / "cocoindex.db"
OUT_DIR = _HERE / "out_tree_test"


@coco.function
def process_item(item_name: str, target: DirTarget) -> None:
    """Process a single item - this creates a component at /"group"/item_name."""
    target.declare_file(f"{item_name}.txt", f"Content for {item_name}\n")


@coco.function
def app_main() -> None:
    """Main app function that creates a tree with non-component intermediate nodes."""
    # Create output directory target
    dir_target = coco.mount_run(
        coco.component_subpath("setup"),
        declare_dir_target,
        OUT_DIR,
    ).result()

    coco.mount(
        coco.component_subpath("group", "item1"), process_item, "item1", dir_target
    )
    coco.mount(
        coco.component_subpath("group", "item2"), process_item, "item2", dir_target
    )

    coco.mount(coco.component_subpath("direct"), process_item, "direct", dir_target)


app = coco.App(
    coco.AppConfig(
        name="TreeTestApp",
        environment=coco.Environment(coco.Settings.from_env(db_path=DB_PATH)),
    ),
    app_main,
)

"""Test app for tree display with non-component intermediate nodes.

This app creates a tree structure similar to the papers example:
- Root (/) - component
- /"files" - intermediate node (NOT a component)
- /"files"/"file1.txt" - component
- /"files"/"file2.txt" - component
- /"direct" - component (direct child of root)
- /"setup" - component (from mount_run)
"""

from __future__ import annotations

import pathlib

import cocoindex as coco
from cocoindex.connectors.localfs import declare_dir_target, DirTarget

_HERE = pathlib.Path(__file__).resolve().parent
DB_PATH = _HERE / "cocoindex.db"
OUT_DIR = _HERE / "out_tree_test"


@coco.function
def process_file(file_name: str, target: DirTarget) -> None:
    """Process a single file - this creates a component at /"files"/file_name."""
    target.declare_file(file_name, f"Content for {file_name}\n")


@coco.function
async def app_main() -> None:
    """Main app function that creates a tree with non-component intermediate nodes."""
    # Create output directory target (use_mount returns the result directly)
    dir_target = await coco.use_mount(
        coco.component_subpath("setup"),
        declare_dir_target,
        OUT_DIR,
    )

    # Mount file components using path composition
    # Try using the / operator to construct paths, similar to scope / 'files' / file.name
    # The "files" part should be just part of the path, not a component itself
    files_subpath = coco.component_subpath("files")
    await coco.mount(files_subpath / "file1.txt", process_file, "file1.txt", dir_target)
    await coco.mount(files_subpath / "file2.txt", process_file, "file2.txt", dir_target)

    # Also mount a direct child of root (not under "files")
    await coco.mount(
        coco.component_subpath("direct"), process_file, "direct.txt", dir_target
    )


app = coco.App(
    coco.AppConfig(
        name="TreeTestApp",
        environment=coco.Environment(coco.Settings.from_env(db_path=DB_PATH)),
    ),
    app_main,
)

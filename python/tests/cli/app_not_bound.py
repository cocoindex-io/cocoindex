"""Test module where App is NOT bound to a module-level variable.

This tests the WeakValueDictionary registry approach - apps created inside
functions should still be discoverable via the registry.
"""

from __future__ import annotations

import pathlib

import cocoindex as coco
from cocoindex.connectors.localfs import declare_dir_target

_HERE = pathlib.Path(__file__).resolve().parent
DB_PATH = _HERE / "cocoindex_unbound.db"
OUT_DIR = _HERE / "out_unbound"

env = coco.Environment(coco.Settings.from_env(db_path=DB_PATH))


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
        scope, filename="unbound.txt", content="Hello from UnboundApp\n"
    )


def create_app() -> coco.App[[], None]:
    """Factory function that creates an app without binding to module-level variable."""
    return coco.App(build, coco.AppConfig(name="UnboundApp", environment=env))


# Create the app but DON'T bind it to a simple module-level name.
# The app should still be discoverable via the registry.
_internal_app_ref = create_app()

# Note: We keep _internal_app_ref to prevent garbage collection.
# In a real scenario, the app would be kept alive by being used somewhere.

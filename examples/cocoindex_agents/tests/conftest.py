from __future__ import annotations

import pathlib
import sys

import pytest

import cocoindex as coco

# Let `import cocoindex_agents` work when running from the repository root
# (`uv run pytest examples/cocoindex_agents/tests`) without installing the example.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))


@pytest.fixture
def coco_env(tmp_path: pathlib.Path) -> coco.Environment:
    """A fresh Environment per test, with its own LMDB under tmp_path."""
    return coco.Environment(coco.Settings.from_env(db_path=tmp_path / "coco_db"))

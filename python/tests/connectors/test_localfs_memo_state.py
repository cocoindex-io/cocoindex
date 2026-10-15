"""End-to-end: a memoized per-file function writing into a localfs directory
target keeps its output file when the source file's mtime changes but its
content does not.

`FileLike.__coco_memo_state__` reports such a file as still valid with a
refreshed memo state. The function body does not run, and the file it
declared on the previous run must not be reconciled away.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import cocoindex as coco
from cocoindex.connectors import localfs

from tests import common
from tests.common.target_states import Metrics

coco_env = common.create_test_env(__file__)
_metrics = Metrics()


@coco.fn(memo=True)
async def _upper_file(file: localfs.File, target: localfs.DirTarget) -> None:
    _metrics.increment("call.upper_file")
    text = await file.read_text()
    target.declare_file(file.file_path.path.name + ".out", text.upper())


@coco.fn
async def _app_main(src_dir: Path, out_dir: Path) -> None:
    target = await coco.use_mount(localfs.declare_dir_target, out_dir)
    files = localfs.walk_dir(src_dir)
    async for _key, f in files.items():
        await _upper_file(f, target)


def _outputs(out_dir: Path) -> list[str]:
    return sorted(p.name for p in out_dir.glob("*.out")) if out_dir.exists() else []


def test_memo_hit_with_refreshed_file_state_keeps_output(tmp_path: Path) -> None:
    src_dir = tmp_path / "src"
    out_dir = tmp_path / "out"
    src_dir.mkdir()
    (src_dir / "a.txt").write_text("alpha")
    (src_dir / "b.txt").write_text("beta")
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(name="test_localfs_memo_state", environment=coco_env),
        _app_main,
        src_dir=src_dir,
        out_dir=out_dir,
    )

    # Run 1: both files processed
    app.update_blocking()
    assert _metrics.collect() == {"call.upper_file": 2}
    assert _outputs(out_dir) == ["a.txt.out", "b.txt.out"]
    assert (out_dir / "a.txt.out").read_text() == "ALPHA"

    # Run 2: nothing changed → plain hits
    app.update_blocking()
    assert _metrics.collect() == {}
    assert _outputs(out_dir) == ["a.txt.out", "b.txt.out"]

    # Run 3: a.txt's mtime moves, content unchanged → hit with refreshed state;
    # the body does not run and a.txt.out must still exist
    later = time.time() + 3600
    os.utime(src_dir / "a.txt", (later, later))
    app.update_blocking()
    assert _metrics.collect() == {}
    assert _outputs(out_dir) == ["a.txt.out", "b.txt.out"]

    # Run 4: nothing changed → still there
    app.update_blocking()
    assert _metrics.collect() == {}
    assert _outputs(out_dir) == ["a.txt.out", "b.txt.out"]

    # Run 5: real edit → re-executes that file only, output updated
    (src_dir / "a.txt").write_text("alpha2")
    os.utime(src_dir / "a.txt", (later + 10, later + 10))
    app.update_blocking()
    assert _metrics.collect() == {"call.upper_file": 1}
    assert _outputs(out_dir) == ["a.txt.out", "b.txt.out"]
    assert (out_dir / "a.txt.out").read_text() == "ALPHA2"

    # Run 6: a.txt removed → its output is cleaned up, b untouched
    (src_dir / "a.txt").unlink()
    app.update_blocking()
    assert _metrics.collect() == {}
    assert _outputs(out_dir) == ["b.txt.out"]

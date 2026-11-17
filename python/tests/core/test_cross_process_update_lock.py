"""An update of an app holds a lock other processes see.

Two processes that open the same database must not both run an update (or a
drop) of the same app: the second one fails at once, naming the app, and reads
still work while the first one runs.
"""

import pathlib
import subprocess
import sys
import textwrap

import pytest

import cocoindex as coco
import cocoindex.inspect as coco_inspect

_APP_NAME = "cross_process_locked"

# The child process runs an update of the app whose main function blocks until
# the parent creates `release`.
_CHILD = textwrap.dedent(
    """
    import pathlib, sys, time
    import cocoindex as coco

    db_path, release = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])

    def main() -> None:
        print("started", flush=True)
        while not release.exists():
            time.sleep(0.02)

    env = coco.Environment(coco.Settings.from_env(db_path=db_path))
    app = coco.App(coco.AppConfig(name=sys.argv[3], environment=env), main)
    app.update_blocking()
    print("done", flush=True)
    """
)


def _main() -> None:
    pass


def test_second_process_update_of_the_same_app_fails_fast(
    tmp_path: pathlib.Path,
) -> None:
    db_path = tmp_path / "db"
    release = tmp_path / "release"
    child = subprocess.Popen(
        [sys.executable, "-c", _CHILD, str(db_path), str(release), _APP_NAME],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout is not None
        assert child.stdout.readline().strip() == "started"

        env = coco.Environment(coco.Settings.from_env(db_path=db_path))
        app = coco.App(coco.AppConfig(name=_APP_NAME, environment=env), _main)
        with pytest.raises(RuntimeError, match=f"app '{_APP_NAME}'.*another process"):
            app.update_blocking()
        with pytest.raises(RuntimeError, match="another process"):
            app.drop_blocking()
        # Reads work while the other process updates.
        assert coco_inspect.list_stable_paths_sync(app) is not None

        # Another app of the same database is not locked.
        other = coco.App(
            coco.AppConfig(name=f"{_APP_NAME}_other", environment=env), _main
        )
        other.update_blocking()

        release.touch()
        assert child.wait(timeout=60) == 0
        assert child.stdout.readline().strip() == "done"

        # Once the other process is done, this one can update.
        app.update_blocking()
    finally:
        release.touch()
        child.kill()
        child.wait()

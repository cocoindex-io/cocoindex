"""Ctrl+C during `cocoindex update` stops the update and exits cleanly."""

import os
import pathlib
import signal
import subprocess
import sys
import textwrap

import pytest

_APP = textwrap.dedent(
    """
    import asyncio
    import sys
    import cocoindex as coco

    async def main() -> None:
        print("main started", file=sys.stderr, flush=True)
        await asyncio.Event().wait()

    app = coco.App(coco.AppConfig(name="CtrlCApp"), main)
    """
)


@pytest.mark.skipif(sys.platform == "win32", reason="sends SIGINT")
@pytest.mark.timeout(90)
def test_ctrl_c_during_an_update_is_a_clean_stop(tmp_path: pathlib.Path) -> None:
    app_file = tmp_path / "ctrl_c_app.py"
    app_file.write_text(_APP)
    child = subprocess.Popen(
        [sys.executable, "-m", "cocoindex.cli", "update", str(app_file)],
        # Only stderr is read; an undrained stdout pipe could block the child.
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        env=dict(os.environ, COCOINDEX_DB=str(tmp_path / "db")),
    )
    try:
        assert child.stderr is not None
        seen = ""
        for line in child.stderr:
            seen += line
            if "main started" in line:
                break
        assert "main started" in seen, seen
        child.send_signal(signal.SIGINT)
        _, rest = child.communicate(timeout=60)
    finally:
        child.kill()
        child.wait()
    assert child.returncode == 0, rest
    assert "CancelledError" not in rest, rest

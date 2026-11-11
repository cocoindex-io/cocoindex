"""What a command prints on a real terminal, run in a pseudo-terminal: the code
paths that check for one do not run under a pipe."""

from __future__ import annotations

import os
import select
import subprocess
import sys
from pathlib import Path

import pytest

if sys.platform != "win32":
    import pty

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="pseudo-terminals are Unix-only"
)

_APP = Path(__file__).resolve().parent / "terminal_output_app.py"


def _run(args: list[str], tmp_path: Path, **extra_env: str) -> bytes:
    """Run `args` with a pseudo-terminal as stdout and stderr, and return what
    the terminal received."""
    env = {
        **os.environ,
        "TERMINAL_OUTPUT_DB": str(tmp_path / "db"),
        "TERM": "xterm-256color",
        **extra_env,
    }
    master, slave = pty.openpty()
    with subprocess.Popen(
        args,
        cwd=tmp_path,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=slave,
        stderr=slave,
    ) as process:
        os.close(slave)
        output = bytearray()
        while True:
            # Poll: a helper that inherited the terminal can keep it open after
            # the process exits, so end of file may never come.
            exited = process.poll() is not None
            if not select.select([master], [], [], 0.1)[0]:
                if exited:
                    break
                continue
            try:
                chunk = os.read(master, 65536)
            except OSError:  # EIO: every writer closed the terminal.
                break
            if not chunk:
                break
            output += chunk
        os.close(master)
        assert process.wait() == 0, output.decode(errors="replace")
    return bytes(output)


def test_a_cancelled_display_leaves_no_region_for_later_groups(tmp_path: Path) -> None:
    output = _run([sys.executable, str(_APP)], tmp_path, NO_COLOR="1")
    assert "✓ LaterGroup · Done in".encode() in output


def test_no_color_also_covers_engine_logs(tmp_path: Path) -> None:
    target = f"{_APP}:Items"
    _run(["cocoindex", "update", target], tmp_path)
    output = _run(
        ["cocoindex", "drop", target, "-f"],
        tmp_path,
        NO_COLOR="1",
        RUST_LOG="cocoindex_core=debug",
    )
    logs = [line for line in output.splitlines() if b" DEBUG " in line]
    assert logs, output
    assert not any(b"\x1b[" in line for line in logs), logs


def test_debug_logs_keep_their_time_and_logger_name(tmp_path: Path) -> None:
    output = _run(
        ["cocoindex", "update", f"{_APP}:Failures"],
        tmp_path,
        COCOINDEX_LOG_LEVEL="DEBUG",
    )
    assert b" ERROR cocoindex: " in output
    assert b" DEBUG cocoindex: traceback of " in output

"""A program that embeds cocoindex keeps stdout for its own output, such as an
MCP stdio server's messages: engine logs go to stderr, with no color codes when
stderr is not a terminal.

The program runs in a fresh interpreter: the engine sets up its log output once
per process, from the `RUST_LOG` the process starts with.
"""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys

import cocoindex as coco

from tests import common

_PYTHON_DIR = pathlib.Path(__file__).resolve().parents[2]

# What the program prints: a JSON-RPC response, as an MCP stdio server sends.
_OUTPUT = '{"jsonrpc": "2.0", "id": 1, "result": {}}'


@coco.fn
async def _main() -> None:
    pass


def _drop_an_app_then_print() -> None:
    """The program the test runs. Dropping an app logs a line at DEBUG."""
    env = common.create_test_env(__file__)
    app = coco.App(coco.AppConfig(name="engine_logs", environment=env), _main)
    app.update_blocking()
    app.drop_blocking()
    print(_OUTPUT)


def test_engine_logs_stay_off_a_piped_stdout() -> None:
    # Without `NO_COLOR`, only the terminal check keeps color codes out.
    env = {k: v for k, v in os.environ.items() if k != "NO_COLOR"}
    env["RUST_LOG"] = "cocoindex_core=debug"
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from tests.core.test_engine_logs import _drop_an_app_then_print\n"
            "_drop_an_app_then_print()",
        ],
        cwd=_PYTHON_DIR,
        env=env,
        capture_output=True,
        encoding="utf-8",
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == _OUTPUT + "\n"
    assert "App dropped successfully" in result.stderr
    assert "\x1b[" not in result.stderr

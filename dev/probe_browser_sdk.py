#!/usr/bin/env python3
"""Record offline wasm32 feasibility evidence without installing a toolchain.

Uses a fresh output directory, two Cargo jobs, no incremental/debug artifacts,
and a bounded build budget. This probes compilation, not browser execution.
"""

import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time

_TARGET = "wasm32-unknown-unknown"
_MIN_FREE = 5_000_000_000
_MAX_OUTPUT = 200_000_000
_TIMEOUT = 120


def _size(path: Path) -> int:
    total = 0
    for entry in path.rglob("*"):
        try:
            if entry.is_file():
                total += entry.stat().st_size
        except FileNotFoundError:
            # Cargo can rename/remove its temporary artifacts during a scan.
            continue
    return total


def _run(name: str, command: list[str], out: Path, root: Path) -> dict[str, object]:
    record: dict[str, object] = {"command": command, "cwd": str(root)}
    # Copy for subprocess configuration only. Never serialize environment values.
    env = os.environ.copy()
    env.update(
        CARGO_TARGET_DIR=str(out / "target"),
        CARGO_BUILD_JOBS="2",
        CARGO_INCREMENTAL="0",
        CARGO_PROFILE_DEV_DEBUG="0",
        CARGO_PROFILE_TEST_DEBUG="0",
    )
    started = time.monotonic()
    try:
        with (out / f"{name}.log").open("w") as log:
            with subprocess.Popen(
                command,
                cwd=root,
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            ) as process:
                while process.poll() is None:
                    reason = None
                    if shutil.disk_usage(out).free < _MIN_FREE:
                        reason = "less than 5GB disk free"
                    elif _size(out) > _MAX_OUTPUT:
                        reason = "probe output exceeded 200MB"
                    elif time.monotonic() - started > _TIMEOUT:
                        reason = "probe command exceeded 120 seconds"
                    if reason:
                        # Only this probe's newly created process group is owned.
                        os.killpg(process.pid, signal.SIGTERM)
                        try:
                            process.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            os.killpg(process.pid, signal.SIGKILL)
                        record["stopped"] = reason
                        break
                    time.sleep(0.25)
                record["exit_code"] = process.wait()
    except OSError as error:
        record["launch_error"] = str(error)
    record["seconds"] = round(time.monotonic() - started, 3)
    return record


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out",
        required=True,
        type=Path,
        help="new evidence directory (must not exist)",
    )
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    out = args.out.resolve()
    if out.exists() or not out.parent.is_dir():
        parser.error("output must not exist and its parent directory must exist")
    if shutil.disk_usage(out.parent).free < _MIN_FREE:
        parser.error("less than 5GB disk free")
    out.mkdir()  # Never overwrite evidence or reuse another task's target dir.
    canary = out / "std_canary.rs"
    canary.write_text(
        'pub fn canary() -> String { String::from("wasm std prerequisite") }\n'
    )
    commands = [
        ("revision", ["git", "rev-parse", "HEAD"]),
        ("rustc", ["rustc", "--version", "--verbose"]),
        ("cargo", ["cargo", "--version"]),
        ("installed-targets", ["rustup", "target", "list", "--installed"]),
        (
            "sdk-dependencies",
            [
                "cargo",
                "tree",
                "--offline",
                "--locked",
                "-p",
                "cocoindex",
                "--no-default-features",
                "--target",
                _TARGET,
                "--edges",
                "normal",
            ],
        ),
        (
            "std-canary",
            [
                "rustc",
                "--edition=2024",
                "--crate-type=lib",
                "--target",
                _TARGET,
                str(canary),
                "--out-dir",
                str(out),
            ],
        ),
        (
            "sdk-check",
            [
                "cargo",
                "check",
                "--offline",
                "--locked",
                "-p",
                "cocoindex",
                "--no-default-features",
                "--lib",
                "--target",
                _TARGET,
            ],
        ),
    ]
    records = {}
    for name, command in commands:
        records[name] = _run(name, command, out, root)
        if "stopped" in records[name]:
            break
    summary = {
        "target": _TARGET,
        "checks": records,
        "output_bytes": _size(out),
        "free_bytes": shutil.disk_usage(out).free,
        "scope": "compile probe only; no JS bindings or browser execution",
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))
    if records.get("std-canary", {}).get("exit_code") != 0:
        print(
            "Wasm standard-library prerequisite failed; SDK portability is not established."
        )
        return 2
    if records.get("sdk-check", {}).get("exit_code") != 0:
        print(
            "SDK compile did not succeed; inspect sdk-check.log for the first concrete failure."
        )
        return 1
    if records.get("sdk-dependencies", {}).get("exit_code") != 0:
        print("Dependency inventory failed; the probe is incomplete.")
        return 1
    print(
        "SDK cargo check succeeded; JS bindings, browser runtime and persistence still untested."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""
State-store benchmark runner.

For each (N, M) cell, runs three phases via the cocoindex CLI against a
fresh LMDB state store in a temp directory:

    cold   — fresh state, first cocoindex update (creates app + memo entries)
    warm   — second cocoindex update against the populated state (all memo hits)
    drop   — cocoindex drop -f on the populated state (cascade + clear)

Times are wall-clock from subprocess.run, matching what the user observes.
A failed phase stops its cell and makes the complete run exit unsuccessfully.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal


BENCH_DIR = Path(__file__).resolve().parent
MAIN_PY = BENCH_DIR / "main.py"

DEFAULT_NS = (100, 1_000, 10_000)
PHASES = ("cold", "warm", "drop")


@dataclass
class PhaseResult:
    name: str
    status: Literal["ok", "error", "not_run"]
    seconds: float | None = None
    error: str | None = None


@dataclass
class CellResult:
    n: int
    m: int
    phases: list[PhaseResult]

    @property
    def ok(self) -> bool:
        return all(phase.status == "ok" for phase in self.phases)

    def timing(self, index: int) -> float:
        seconds = self.phases[index].seconds
        assert seconds is not None
        return seconds


def _run(cmd: list[str], env: dict[str, str], cwd: Path) -> float:
    """Run a subprocess to completion and return its wall-clock seconds."""
    t = time.perf_counter()
    proc = subprocess.run(cmd, env=env, cwd=cwd, capture_output=True, text=True)
    elapsed = time.perf_counter() - t
    if proc.returncode != 0:
        print(f"  ! {' '.join(cmd)} failed (rc={proc.returncode})", file=sys.stderr)
        if proc.stdout:
            print(proc.stdout, file=sys.stderr)
        if proc.stderr:
            print(proc.stderr, file=sys.stderr)
        raise RuntimeError(f"command exited with status {proc.returncode}")
    return elapsed


_COCOINDEX_CMD = [sys.executable, "-m", "cocoindex.cli"]


def _cell(n: int, m: int) -> CellResult:
    update_cmd = [*_COCOINDEX_CMD, "update", str(MAIN_PY)]
    drop_cmd = [*_COCOINDEX_CMD, "drop", "-f", str(MAIN_PY)]
    phases = [PhaseResult(name, "not_run") for name in PHASES]
    with tempfile.TemporaryDirectory(prefix="coco-bench-lmdb-") as tmp:
        env = {
            **os.environ,
            "COCOINDEX_DB": str(Path(tmp) / "db"),
            "BENCH_N": str(n),
            "BENCH_M": str(m),
        }
        for index, command in enumerate((update_cmd, update_cmd, drop_cmd)):
            try:
                seconds = _run(command, env, BENCH_DIR)
            except (OSError, RuntimeError) as exc:
                phases[index] = PhaseResult(PHASES[index], "error", error=str(exc))
                break
            phases[index] = PhaseResult(PHASES[index], "ok", seconds=seconds)
    return CellResult(n=n, m=m, phases=phases)


def _print_table(results: list[CellResult]) -> None:
    hdr = f"{'N':>8}  {'M':>4}  {'cold':>8}  {'warm':>8}  {'drop':>8}"
    print(hdr)
    print("-" * len(hdr))
    for r in sorted(results, key=lambda r: (r.n, r.m)):
        print(
            f"{r.n:>8}  {r.m:>4}  {r.timing(0):>8.3f}  {r.timing(1):>8.3f}  {r.timing(2):>8.3f}"
        )
    print()
    print("All numbers in seconds. Lower is better.")


def _print_json(results: list[CellResult]) -> None:
    # Preserve the existing successful-cell shape for benchmark consumers.
    print(
        json.dumps(
            [
                {
                    "n": r.n,
                    "m": r.m,
                    **{f"{phase.name}_s": phase.seconds for phase in r.phases},
                }
                for r in results
            ],
            indent=2,
            allow_nan=False,
        )
    )


def _counts(value: str, minimum: int) -> list[int]:
    values = [int(s) for s in value.split(",") if s.strip()]
    if not values or any(n < minimum for n in values):
        raise ValueError(f"counts must be a nonempty list of integers >= {minimum}")
    if len(values) != len(set(values)):
        raise ValueError("counts must not contain duplicates")
    return values


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--n",
        default=",".join(str(n) for n in DEFAULT_NS),
        help=f"Comma-separated list of component counts. Default: {','.join(str(n) for n in DEFAULT_NS)}",
    )
    parser.add_argument(
        "--m",
        default="0",
        help=(
            "Comma-separated list of per-component target-state counts. "
            "Default: 0 (no target states). Each non-zero M exercises the "
            "target-state write path on every component."
        ),
    )
    parser.add_argument("--format", choices=("table", "json"), default="table")
    parser.add_argument(
        "--report",
        type=Path,
        help="Write a versioned JSON report containing every requested cell and phase, including failures.",
    )
    args = parser.parse_args()
    try:
        ns = _counts(args.n, minimum=0)
        ms = _counts(args.m, minimum=0)
    except ValueError as exc:
        parser.error(str(exc))

    results: list[CellResult] = []
    for n in ns:
        for m in ms:
            label = f"N={n}/M={m}"
            print(f"running {label} …", file=sys.stderr, flush=True)
            r = _cell(n, m)
            results.append(r)
            if not r.ok:
                failed = next(phase for phase in r.phases if phase.status == "error")
                print(
                    f"  ! {label} failed in {failed.name}: {failed.error}",
                    file=sys.stderr,
                )
                continue
            print(
                f"  cold {r.timing(0):.3f}s   warm {r.timing(1):.3f}s   drop {r.timing(2):.3f}s",
                file=sys.stderr,
                flush=True,
            )

    if args.report is not None:
        args.report.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "requested": {"n": ns, "m": ms, "phases": list(PHASES)},
                    "python": sys.executable,
                    "cells": [
                        {"status": "ok" if result.ok else "error", **asdict(result)}
                        for result in results
                    ],
                },
                indent=2,
                allow_nan=False,
            )
            + "\n"
        )
    successful = [result for result in results if result.ok]
    if args.format == "json":
        _print_json(successful)
    else:
        _print_table(successful)
    return 0 if len(successful) == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())

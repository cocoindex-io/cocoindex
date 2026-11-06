"""
State-store benchmark runner.

For each (N, M) cell, runs three phases via the cocoindex CLI against a
fresh LMDB state store in a temp directory:

    cold   — fresh state, first `cocoindex update` (creates app + memo entries)
    warm   — second `cocoindex update` against the populated state (all memo hits)
    drop   — `cocoindex drop -f` on the populated state (cascade + clear)

Times are wall-clock from subprocess.run, matching what the user observes.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path


BENCH_DIR = Path(__file__).resolve().parent
MAIN_PY = BENCH_DIR / "main.py"

DEFAULT_NS = (100, 1_000, 10_000)


@dataclass
class CellResult:
    n: int
    m: int
    cold_s: float
    warm_s: float
    drop_s: float


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
        raise RuntimeError(f"{cmd[0]} failed")
    return elapsed


_COCOINDEX_CMD = [sys.executable, "-m", "cocoindex.cli"]


def _cell(n: int, m: int) -> CellResult:
    update_cmd = [*_COCOINDEX_CMD, "update", str(MAIN_PY)]
    drop_cmd = [*_COCOINDEX_CMD, "drop", "-f", str(MAIN_PY)]
    with tempfile.TemporaryDirectory(prefix="coco-bench-lmdb-") as tmp:
        env = {
            **os.environ,
            "COCOINDEX_DB": str(Path(tmp) / "db"),
            "BENCH_N": str(n),
            "BENCH_M": str(m),
        }
        cold = _run(update_cmd, env, BENCH_DIR)
        warm = _run(update_cmd, env, BENCH_DIR)
        drop = _run(drop_cmd, env, BENCH_DIR)
    return CellResult(n=n, m=m, cold_s=cold, warm_s=warm, drop_s=drop)


def _print_table(results: list[CellResult]) -> None:
    hdr = f"{'N':>8}  {'M':>4}  {'cold':>8}  {'warm':>8}  {'drop':>8}"
    print(hdr)
    print("-" * len(hdr))
    for r in sorted(results, key=lambda r: (r.n, r.m)):
        print(
            f"{r.n:>8}  {r.m:>4}  {r.cold_s:>8.3f}  {r.warm_s:>8.3f}  {r.drop_s:>8.3f}"
        )
    print()
    print("All numbers in seconds. Lower is better.")


def _print_json(results: list[CellResult]) -> None:
    print(
        json.dumps(
            [
                {
                    "n": r.n,
                    "m": r.m,
                    "cold_s": r.cold_s,
                    "warm_s": r.warm_s,
                    "drop_s": r.drop_s,
                }
                for r in results
            ],
            indent=2,
        )
    )


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
    parser.add_argument(
        "--format",
        choices=("table", "json"),
        default="table",
    )
    args = parser.parse_args()

    ns = [int(s) for s in args.n.split(",") if s.strip()]
    ms = [int(s) for s in args.m.split(",") if s.strip()]

    results: list[CellResult] = []
    for n in ns:
        for m in ms:
            label = f"N={n}/M={m}"
            print(f"running {label} …", flush=True)
            try:
                r = _cell(n, m)
            except Exception as exc:
                print(f"  ! {label} failed: {exc}", file=sys.stderr)
                continue
            print(
                f"  cold {r.cold_s:.3f}s   warm {r.warm_s:.3f}s   drop {r.drop_s:.3f}s",
                flush=True,
            )
            results.append(r)

    print()
    if args.format == "json":
        _print_json(results)
    else:
        _print_table(results)
    return 0


if __name__ == "__main__":
    sys.exit(main())

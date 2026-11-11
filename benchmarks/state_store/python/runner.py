"""
State-store benchmark runner.

For each (N, M) cell and each SDK, runs three phases against a fresh LMDB
state store in a temp directory:

    cold   — fresh state, first update (creates app + memo entries)
    warm   — second update against the populated state (all memo hits)
    drop   — drop on the populated state (cascade + clear)

The Python SDK runs through the cocoindex CLI (`python -m cocoindex.cli
update|drop main.py`); the Rust SDK runs the `rust/` crate's binary
(`state_store update|drop`). Both read `COCOINDEX_DB`, `BENCH_N` and
`BENCH_M` from the environment. Times are wall-clock from subprocess.run,
matching what the user observes, so the Python numbers include interpreter
and import start-up and the Rust numbers include process start-up.
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


PY_DIR = Path(__file__).resolve().parent
BENCH_ROOT = PY_DIR.parent
MAIN_PY = PY_DIR / "main.py"
RUST_MANIFEST = BENCH_ROOT / "rust" / "Cargo.toml"

DEFAULT_NS = (100, 1_000, 10_000)
LANGUAGES = ("rust", "python")


@dataclass
class CellResult:
    language: str
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


def _rust_binary() -> Path:
    """Build the Rust crate in release mode and return its binary path."""
    subprocess.run(
        ["cargo", "build", "--release", "--manifest-path", str(RUST_MANIFEST)],
        cwd=BENCH_ROOT,
        check=True,
    )
    target_dir = Path(
        os.environ.get("CARGO_TARGET_DIR", RUST_MANIFEST.parent / "target")
    )
    return target_dir / "release" / "state_store"


def _commands(language: str, rust_bin: Path | None) -> tuple[list[str], list[str]]:
    """The (update, drop) commands of one SDK."""
    if language == "python":
        return (
            [*_COCOINDEX_CMD, "update", str(MAIN_PY)],
            [*_COCOINDEX_CMD, "drop", "-f", str(MAIN_PY)],
        )
    if rust_bin is None:
        raise RuntimeError("the Rust binary was not built")
    return ([str(rust_bin), "update"], [str(rust_bin), "drop"])


def _cell(language: str, rust_bin: Path | None, n: int, m: int) -> CellResult:
    update_cmd, drop_cmd = _commands(language, rust_bin)
    with tempfile.TemporaryDirectory(prefix="coco-bench-lmdb-") as tmp:
        env = {
            **os.environ,
            "COCOINDEX_DB": str(Path(tmp) / "db"),
            "BENCH_N": str(n),
            "BENCH_M": str(m),
        }
        cold = _run(update_cmd, env, PY_DIR)
        warm = _run(update_cmd, env, PY_DIR)
        drop = _run(drop_cmd, env, PY_DIR)
    return CellResult(
        language=language, n=n, m=m, cold_s=cold, warm_s=warm, drop_s=drop
    )


def _print_table(results: list[CellResult]) -> None:
    hdr = f"{'N':>8}  {'M':>4}  {'lang':<6}  {'cold':>8}  {'warm':>8}  {'drop':>8}"
    print(hdr)
    print("-" * len(hdr))
    for r in sorted(results, key=lambda r: (r.n, r.m, LANGUAGES.index(r.language))):
        print(
            f"{r.n:>8}  {r.m:>4}  {r.language:<6}  "
            f"{r.cold_s:>8.3f}  {r.warm_s:>8.3f}  {r.drop_s:>8.3f}"
        )
    print()
    print("All numbers in seconds. Lower is better.")


def _print_json(results: list[CellResult]) -> None:
    print(
        json.dumps(
            [
                {
                    "language": r.language,
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
        "--lang",
        choices=(*LANGUAGES, "all"),
        default="all",
        help="Which SDK to run. Default: all (Rust, then Python, per cell).",
    )
    parser.add_argument(
        "--format",
        choices=("table", "json"),
        default="table",
    )
    args = parser.parse_args()

    ns = [int(s) for s in args.n.split(",") if s.strip()]
    ms = [int(s) for s in args.m.split(",") if s.strip()]
    languages = list(LANGUAGES) if args.lang == "all" else [args.lang]
    rust_bin = _rust_binary() if "rust" in languages else None

    results: list[CellResult] = []
    for n in ns:
        for m in ms:
            for language in languages:
                label = f"N={n}/M={m}/{language}"
                print(f"running {label} …", flush=True)
                try:
                    r = _cell(language, rust_bin, n, m)
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

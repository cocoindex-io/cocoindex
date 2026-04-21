from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import statistics
import subprocess
import shutil
from typing import Any

from common import (
    BENCHMARK_SEED,
    PROFILE_ORDER,
    apply_edit_mutation,
    apply_shape_mutation,
    generate_dataset,
)


BASE_DIR = Path(__file__).resolve().parent
RUST_MANIFEST = BASE_DIR / "benchmark_rust" / "Cargo.toml"
RUST_BIN = BASE_DIR / "benchmark_rust" / "target" / "release" / "benchmark_rust"
PYTHON_BIN = BASE_DIR / "benchmark_python.py"
WORK_ROOT = BASE_DIR / ".work"
PHASES = ("cold", "warm", "edit", "shape")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run CocoIndex cross-language benchmarks."
    )
    parser.add_argument(
        "--scenario",
        choices=["codebase", "docs", "all"],
        default="all",
        help="Benchmark scenario to run.",
    )
    parser.add_argument(
        "--scale",
        choices=["tiny", "medium", "large", "xlarge"],
        default="tiny",
        help="Dataset size profile.",
    )
    parser.add_argument(
        "--profile",
        choices=["io", "cpu", "mixed", "all"],
        default="mixed",
        help="Workload profile to run.",
    )
    parser.add_argument(
        "--trials",
        type=int,
        default=1,
        help="Number of fresh trials to execute.",
    )
    parser.add_argument(
        "--format",
        choices=["table", "json"],
        default="table",
        help="Output format for aggregated results.",
    )
    return parser.parse_args()


def build_rust_binary() -> None:
    subprocess.run(
        ["cargo", "build", "--release", "--manifest-path", str(RUST_MANIFEST)],
        cwd=BASE_DIR,
        check=True,
    )


def run_language(
    language: str,
    *,
    scenario: str,
    profile: str,
    dataset_dir: Path,
    state_dir: Path,
    output_dir: Path,
    metrics_path: Path,
    phase: str,
) -> dict[str, Any]:
    env = os.environ.copy()
    if language == "python":
        cmd = [
            "uv",
            "run",
            "--project",
            str(BASE_DIR),
            "python",
            str(PYTHON_BIN),
            "--scenario",
            scenario,
            "--profile",
            profile,
            "--dataset",
            str(dataset_dir),
            "--state",
            str(state_dir),
            "--output",
            str(output_dir),
            "--metrics",
            str(metrics_path),
            "--phase",
            phase,
        ]
    else:
        cmd = [
            str(RUST_BIN),
            "--scenario",
            scenario,
            "--profile",
            profile,
            "--dataset",
            str(dataset_dir),
            "--state",
            str(state_dir),
            "--output",
            str(output_dir),
            "--metrics",
            str(metrics_path),
            "--phase",
            phase,
        ]

    subprocess.run(cmd, cwd=BASE_DIR, env=env, check=True)
    return json.loads(metrics_path.read_text(encoding="utf-8"))


def run_trial(
    scenario: str, profile: str, scale: str, trial_index: int
) -> list[dict[str, Any]]:
    trial_root = WORK_ROOT / scenario / profile / scale / f"trial_{trial_index:02d}"
    dataset_dir = trial_root / "dataset"
    rust_state = trial_root / "rust_state"
    python_state = trial_root / "python_state"
    rust_output = trial_root / "rust_output"
    python_output = trial_root / "python_output"
    rust_metrics = trial_root / "rust_metrics.json"
    python_metrics = trial_root / "python_metrics.json"

    generate_dataset(dataset_dir, scenario, scale, profile)
    for path in (rust_state, python_state, rust_output, python_output):
        if path.exists():
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
        path.mkdir(parents=True, exist_ok=True)

    phase_results: list[dict[str, Any]] = []
    for phase in PHASES:
        mutation: dict[str, Any] | None = None
        if phase == "edit":
            mutation = apply_edit_mutation(dataset_dir, scenario, profile)
        elif phase == "shape":
            mutation = apply_shape_mutation(dataset_dir, scenario, profile)

        rust_result = run_language(
            "rust",
            scenario=scenario,
            profile=profile,
            dataset_dir=dataset_dir,
            state_dir=rust_state,
            output_dir=rust_output,
            metrics_path=rust_metrics,
            phase=phase,
        )
        python_result = run_language(
            "python",
            scenario=scenario,
            profile=profile,
            dataset_dir=dataset_dir,
            state_dir=python_state,
            output_dir=python_output,
            metrics_path=python_metrics,
            phase=phase,
        )

        if rust_result["output_hash"] != python_result["output_hash"]:
            raise RuntimeError(
                f"Output mismatch in {scenario}:{profile}:{phase}: "
                f"rust={rust_result['output_hash']} python={python_result['output_hash']}"
            )

        rust_result["trial"] = trial_index
        python_result["trial"] = trial_index
        if mutation is not None:
            rust_result["mutation"] = mutation
            python_result["mutation"] = mutation
        phase_results.extend([rust_result, python_result])

    return phase_results


def aggregate_results(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    aggregated: list[dict[str, Any]] = []
    keys = sorted(
        {
            (row["scenario"], row["profile"], row["phase"], row["language"])
            for row in rows
        }
    )
    for scenario, profile, phase, language in keys:
        group = [
            row
            for row in rows
            if row["scenario"] == scenario
            and row["profile"] == profile
            and row["phase"] == phase
            and row["language"] == language
        ]
        aggregated.append(
            {
                "scenario": scenario,
                "profile": profile,
                "phase": phase,
                "language": language,
                "trials": len(group),
                "elapsed_ms_median": statistics.median(
                    row["elapsed_ms"] for row in group
                ),
                "cache_misses_median": statistics.median(
                    row["cache_misses"] for row in group
                ),
                "batch_calls_median": statistics.median(
                    row["batch_calls"] for row in group
                ),
                "output_files_rebuilt_median": statistics.median(
                    row["output_files_rebuilt"] for row in group
                ),
                "output_file_count_median": statistics.median(
                    row["output_file_count"] for row in group
                ),
                "output_bytes_median": statistics.median(
                    row["output_bytes"] for row in group
                ),
                "sections_total_median": statistics.median(
                    row["sections_total"] for row in group
                ),
            }
        )
    return aggregated


def print_table(rows: list[dict[str, Any]]) -> None:
    profile_rank = {name: index for index, name in enumerate(PROFILE_ORDER)}
    groups = sorted(
        {(row["scenario"], row["profile"]) for row in rows},
        key=lambda item: (item[0], profile_rank[item[1]]),
    )
    for scenario, profile in groups:
        print(f"\nScenario: {scenario} | Profile: {profile}")
        print("phase    rust_ms  py_ms  ratio  rust_miss  py_miss  rust_out  py_out")
        for phase in PHASES:
            rust_row = next(
                row
                for row in rows
                if row["scenario"] == scenario
                and row["profile"] == profile
                and row["phase"] == phase
                and row["language"] == "rust"
            )
            py_row = next(
                row
                for row in rows
                if row["scenario"] == scenario
                and row["profile"] == profile
                and row["phase"] == phase
                and row["language"] == "python"
            )
            ratio = py_row["elapsed_ms_median"] / rust_row["elapsed_ms_median"]
            print(
                f"{phase:<8} "
                f"{rust_row['elapsed_ms_median']:>7.1f} "
                f"{py_row['elapsed_ms_median']:>6.1f} "
                f"{ratio:>5.2f} "
                f"{rust_row['cache_misses_median']:>10.0f} "
                f"{py_row['cache_misses_median']:>8.0f} "
                f"{rust_row['output_files_rebuilt_median']:>8.0f} "
                f"{py_row['output_files_rebuilt_median']:>6.0f}"
            )


def main() -> None:
    args = parse_args()
    scenarios = ["codebase", "docs"] if args.scenario == "all" else [args.scenario]
    profiles = list(PROFILE_ORDER) if args.profile == "all" else [args.profile]

    build_rust_binary()

    all_rows: list[dict[str, Any]] = []
    for scenario in scenarios:
        for profile in profiles:
            for trial_index in range(1, args.trials + 1):
                all_rows.extend(run_trial(scenario, profile, args.scale, trial_index))

    aggregated = aggregate_results(all_rows)
    if args.format == "json":
        print(json.dumps({"seed": BENCHMARK_SEED, "results": aggregated}, indent=2))
    else:
        print_table(aggregated)


if __name__ == "__main__":
    main()

"""Compare two Linux builds of the CocoIndex Python extension on the same pipeline."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

from common import apply_edit_mutation, generate_dataset

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]


def stage_extension(extension: Path, stage: Path) -> None:
    package = stage / "cocoindex"
    shutil.copytree(
        ROOT / "python" / "cocoindex",
        package,
        ignore=shutil.ignore_patterns("__pycache__", "core*.so"),
    )
    shutil.copyfile(extension, package / "_internal" / "core.abi3.so")


def run_phase(
    stage: Path, run: Path, dataset: Path, scenario: str, profile: str, phase: str
) -> tuple[float, str]:
    metrics = run / "metrics.json"
    subprocess.run(
        [
            sys.executable,
            str(HERE / "benchmark.py"),
            "--scenario",
            scenario,
            "--profile",
            profile,
            "--phase",
            phase,
            "--dataset",
            str(dataset),
            "--state",
            str(run / "state"),
            "--output",
            str(run / "output"),
            "--metrics",
            str(metrics),
        ],
        env={**os.environ, "PYTHONPATH": str(stage)},
        stdout=subprocess.DEVNULL,
        check=True,
    )
    result = json.loads(metrics.read_text(encoding="utf-8"))
    return float(result["elapsed_ms"]), str(result["output_hash"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "extension", type=Path, help="Baseline or instrumented extension"
    )
    parser.add_argument("--candidate", type=Path, help="Profile-optimized extension")
    parser.add_argument("--scenario", choices=("codebase", "docs"), default="codebase")
    parser.add_argument("--profile", choices=("cpu", "io", "mixed"), default="mixed")
    parser.add_argument(
        "--scale", choices=("tiny", "medium", "large"), default="medium"
    )
    parser.add_argument("--trials", type=int, default=7)
    args = parser.parse_args()
    if args.trials < 1:
        parser.error("--trials must be at least 1")

    extensions = {"baseline": args.extension}
    if args.candidate is not None:
        extensions["pgo"] = args.candidate
    samples: dict[tuple[str, str], list[float]] = defaultdict(list)
    with tempfile.TemporaryDirectory(prefix="cocoindex-pgo-") as work:
        work_path = Path(work)
        for name, extension in extensions.items():
            stage_extension(extension, work_path / name)

        for trial in range(args.trials):
            trial_path = work_path / str(trial)
            dataset = trial_path / "dataset"
            generate_dataset(dataset, args.scenario, args.scale, args.profile)
            order = list(extensions)
            if trial % 2:
                order.reverse()
            for phase in ("cold", "warm", "edit"):
                if phase == "edit":
                    apply_edit_mutation(dataset, args.scenario, args.profile)
                hashes = set()
                for name in order:
                    elapsed, output_hash = run_phase(
                        work_path / name,
                        trial_path / name,
                        dataset,
                        args.scenario,
                        args.profile,
                        phase,
                    )
                    samples[name, phase].append(elapsed)
                    hashes.add(output_hash)
                if len(hashes) > 1:
                    raise RuntimeError(
                        f"Outputs differ on trial {trial + 1}, phase {phase}"
                    )

    print(
        json.dumps(
            {
                "scenario": args.scenario,
                "profile": args.profile,
                "scale": args.scale,
                "results": {
                    f"{name}/{phase}": {
                        "median_ms": round(statistics.median(values), 3),
                        "samples_ms": values,
                    }
                    for (name, phase), values in samples.items()
                },
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

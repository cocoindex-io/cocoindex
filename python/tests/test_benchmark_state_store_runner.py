"""Exercise the benchmark wrapper with real child processes and isolated fixtures."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


RUNNER = Path(__file__).resolve().parents[2] / "benchmarks/state_store/runner.py"
FAKE_CLI = """
import json
import os
import sys
from pathlib import Path

state = Path(os.environ["COCOINDEX_DB"])
state.mkdir(parents=True, exist_ok=True)
marker = state / "updated"
phase = "drop" if sys.argv[1] == "drop" else ("warm" if marker.exists() else "cold")
n = int(os.environ["BENCH_N"])
with Path(os.environ["BENCH_TRACE"]).open("a") as trace:
    trace.write(json.dumps({"n": n, "phase": phase, "db": str(state)}) + "\\n")
if os.environ.get("BENCH_FAIL") == f"{n}:{phase}":
    print("controlled benchmark child failure", file=sys.stderr)
    sys.exit(7)
marker.touch()
"""


class StateStoreRunnerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name)
        package = self.directory / "cocoindex"
        package.mkdir()
        (package / "__init__.py").write_text("")
        (package / "cli.py").write_text(FAKE_CLI)
        self.trace = self.directory / "trace.jsonl"
        self.env = {
            **os.environ,
            "PYTHONPATH": str(self.directory),
            "BENCH_TRACE": str(self.trace),
        }
        self.env.pop("BENCH_FAIL", None)

    def run_benchmark(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(RUNNER), *args],
            env=self.env,
            capture_output=True,
            text=True,
            timeout=20,
        )

    def test_failed_cell_returns_nonzero_even_when_other_cells_succeed(self) -> None:
        self.env["BENCH_FAIL"] = "2:warm"
        result = self.run_benchmark("--n", "1,2,3")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        trace = [json.loads(line) for line in self.trace.read_text().splitlines()]
        self.assertEqual(
            [(row["n"], row["phase"]) for row in trace],
            [
                (1, "cold"),
                (1, "warm"),
                (1, "drop"),
                (2, "cold"),
                (2, "warm"),
                (3, "cold"),
                (3, "warm"),
                (3, "drop"),
            ],
        )
        self.assertEqual(len({row["db"] for row in trace}), 3)

    def test_json_output_contains_no_progress_text(self) -> None:
        result = self.run_benchmark("--n", "1", "--format", "json")
        self.assertEqual(result.returncode, 0, result.stderr)
        data = json.loads(result.stdout)
        self.assertEqual(len(data), 1)
        self.assertEqual(set(data[0]), {"n", "m", "cold_s", "warm_s", "drop_s"})
        self.assertIn("running N=1/M=0", result.stderr)

    def test_failure_report_keeps_every_requested_cell_and_phase(self) -> None:
        self.env["BENCH_FAIL"] = "2:warm"
        report = self.directory / "report.json"
        result = self.run_benchmark(
            "--n", "1,2", "--format", "json", "--report", str(report)
        )
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        data = json.loads(report.read_text())
        self.assertEqual(data["schema_version"], 1)
        self.assertEqual(
            data["requested"],
            {"n": [1, 2], "m": [0], "phases": ["cold", "warm", "drop"]},
        )
        self.assertEqual(len(data["cells"]), 2)
        self.assertEqual([p["status"] for p in data["cells"][0]["phases"]], ["ok"] * 3)
        self.assertEqual(
            [p["status"] for p in data["cells"][1]["phases"]],
            ["ok", "error", "not_run"],
        )
        self.assertEqual(data["cells"][1]["phases"][1]["name"], "warm")
        self.assertIsNone(data["cells"][1]["phases"][1]["seconds"])
        self.assertEqual([row["n"] for row in json.loads(result.stdout)], [1])

    def test_all_failed_cells_do_not_look_like_success(self) -> None:
        self.env["BENCH_FAIL"] = "1:cold"
        result = self.run_benchmark("--n", "1", "--format", "json")
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(json.loads(result.stdout), [])

    def test_invalid_or_duplicate_counts_fail_before_launch(self) -> None:
        for arguments in [
            ["--n", ""],
            ["--n", "1,1"],
            ["--n", "-1"],
            ["--n", "wat"],
            ["--n", "1", "--m", "-1"],
        ]:
            with self.subTest(arguments=arguments):
                result = self.run_benchmark(*arguments)
                self.assertEqual(result.returncode, 2)
                self.assertFalse(self.trace.exists())


if __name__ == "__main__":
    unittest.main()

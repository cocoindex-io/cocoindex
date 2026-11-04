"""Benchmark: a ``for_each`` fan-out over N files, re-run after small changes.

    uv run maturin develop --release                      # the numbers mean little in a debug build
    uv run python examples/coco_harness/benchmarks/fan_out.py --files 10000

One orchestrator opens a localfs source and fans out one closed worker per
file; every worker reads its file and answers. A scripted model stands in for
inference (no API key), so what is measured is the harness and the engine:
wall time, tool executions and model calls for a cold run and then five
incremental cases: an unchanged rerun, one file touched (mtime only), one
file edited, one file added, one file removed.

Two shapes (DESIGN.md §7, *Open vs closed*):

- ``S1``: the orchestrator has no open child. Its own records (the walk, the
  source card) cover everything beneath it, so an unchanged rerun skips it
  whole.
- ``S2``: the orchestrator first calls a planner child that samples the source
  (an open child, as in the ``table`` flow). The planner records a read, so
  the orchestrator's memo is flagged ``has_open_descendants``.

Not part of the test suite: it takes minutes at 10,000 files. The numbers on
record are in DESIGN.md §7.
"""

from __future__ import annotations

import argparse
import os
import pathlib
import shutil
import sys
import tempfile
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass

import cocoindex as coco

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from coco_harness import (  # noqa: E402
    HARNESS,
    MODEL_CLIENT,
    HarnessConfig,
    RoleSpec,
    SessionResult,
    run_open_session,
)
from coco_harness.hints import RunHints  # noqa: E402
from coco_harness.spec import SessionSpec  # noqa: E402
from coco_harness.testing import CountingRegistry, ScriptedModel  # noqa: E402
from coco_harness.tools import agents, sources  # noqa: E402
from coco_harness.transcript import ModelOutput, ToolCall, Transcript  # noqa: E402

ORCHESTRATOR = RoleSpec(
    "orchestrator", "bench", ("open_source", "call", "for_each", "call_agent")
)
WORKER = RoleSpec("worker", "bench", ("read_input",))
PLANNER = RoleSpec("planner", "bench", ("call",))

SHAPES = ("S1", "S2")
CASES = ("cold", "unchanged", "touch one", "edit one", "add one", "remove one")


def _call(tool: str, **args: object) -> ToolCall:
    return ToolCall(tool, dict(args))


def _script(docs: pathlib.Path, shape: str) -> ScriptedModel:
    """The orchestrator's plan, as the model would make it, with no inference."""

    def script(spec: SessionSpec, prefix: Transcript, hints: RunHints) -> ModelOutput:
        n = len(prefix.steps)
        if spec.role.name == "worker":
            if n == 0:
                return ModelOutput(calls=(_call("read_input"),))
            text = prefix.steps[0].results[0].value["text"]
            return ModelOutput(answer=f"{len(text)} chars")
        if spec.role.name == "planner":
            if n == 0:
                return ModelOutput(
                    calls=(
                        _call(
                            "call", handle="src:docs", method="sample", args={"k": 3}
                        ),
                    )
                )
            return ModelOutput(answer="one worker per file")
        plan: list[ToolCall] = [
            _call(
                "open_source",
                kind="localfs",
                name="docs",
                path=str(docs),
                patterns=["*.md"],
            )
        ]
        if shape == "S2":
            plan.append(
                _call(
                    "call_agent",
                    key="plan",
                    task="look at a sample and propose the task",
                    role="planner",
                    inputs=["src:docs"],
                )
            )
        plan.append(
            _call(
                "for_each",
                source="src:docs",
                key="items",
                task="count the characters",
            )
        )
        if n < len(plan):
            return ModelOutput(calls=(plan[n],))
        return ModelOutput(answer=str(prefix.steps[-1].results[0].value))

    return ScriptedModel(script)


@dataclass
class Measurement:
    shape: str
    case: str
    seconds: float
    model_calls: int
    tool_executions: dict[str, int]

    def row(self) -> str:
        tools = ", ".join(f"{k}={v}" for k, v in sorted(self.tool_executions.items()))
        return (
            f"| {self.shape} | {self.case} | {self.seconds:.2f} | {self.model_calls} "
            f"| {tools or '—'} |"
        )


def _bump_mtime(path: pathlib.Path) -> None:
    """Edits within one process can be faster than the filesystem's mtime tick."""
    st = os.stat(path)
    os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000))


def _write(path: pathlib.Path, text: str) -> None:
    path.write_text(text)
    _bump_mtime(path)


@contextmanager
def _workspace(files: int) -> Iterator[tuple[pathlib.Path, pathlib.Path]]:
    root = pathlib.Path(tempfile.mkdtemp(prefix="coco_harness_bench_"))
    try:
        docs = root / "docs"
        docs.mkdir()
        for i in range(files):
            (docs / f"{i:06d}.md").write_text(f"document {i}\n" * 4)
        yield docs, root / "coco_db"
    finally:
        shutil.rmtree(root, ignore_errors=True)


def run_shape(shape: str, files: int) -> list[Measurement]:
    with _workspace(files) as (docs, db_path):
        env = coco.Environment(coco.Settings.from_env(db_path=db_path))
        model = _script(docs, shape)
        tools = CountingRegistry(
            [
                sources.open_source,
                sources.call_method,
                sources.read_input,
                agents.for_each,
                agents.call_agent,
            ]
        )
        config = HarnessConfig(
            roles={r.name: r for r in (ORCHESTRATOR, WORKER, PLANNER)}, tools=tools
        )
        env.context_provider.provide(MODEL_CLIENT, model)
        env.context_provider.provide(HARNESS, config)
        app = coco.App(
            coco.AppConfig(name=f"fan_out_{shape}", environment=env),
            run_open_session,
            config.spec("orchestrator", "count the characters of every doc"),
        )

        def measure(case: str, mutate: Callable[[], None]) -> Measurement:
            mutate()
            model.reset()
            tools.reset()
            started = time.perf_counter()
            result = app.update_blocking()
            seconds = time.perf_counter() - started
            assert isinstance(result, SessionResult)
            m = Measurement(shape, case, seconds, model.calls, dict(tools.executed))
            print(m.row(), flush=True)
            return m

        mutations: dict[str, Callable[[], None]] = {
            "cold": lambda: None,
            "unchanged": lambda: None,
            "touch one": lambda: _bump_mtime(docs / "000001.md"),
            "edit one": lambda: _write(docs / "000002.md", "edited\n"),
            "add one": lambda: _write(docs / f"{files:06d}.md", "added\n"),
            "remove one": lambda: (docs / "000003.md").unlink(),
        }
        measurements = [measure(case, mutations[case]) for case in CASES]
        env.close()
        return measurements


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--files", type=int, default=10_000)
    parser.add_argument("--shapes", nargs="+", choices=SHAPES, default=list(SHAPES))
    args = parser.parse_args(argv)
    print(f"files: {args.files}; cocoindex {coco.__version__}", flush=True)
    print("| shape | case | seconds | model calls | tool executions |")
    print("|---|---|---|---|---|")
    for shape in args.shapes:
        run_shape(shape, args.files)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

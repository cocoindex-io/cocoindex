"""The run report: cache-usage stats and the plan, from scripted flows of two shapes.

Shape A (``summaries``: map → targets) and shape C (``table``: observe → plan
→ map) of DESIGN.md §2, with the file counts of ``flows/``. The traces are
built from what each session did, and a skipped session's stored trace is
credited as its producer ran it; so the stats line on a first run, an
unchanged rerun and a one-edit rerun must agree with the counts measured in
``flows/README.md``. Every run also pins the derived model-call count to the
scripted model's own count, and the unchanged reruns pin that nothing here
changed what is memoized.
"""

from __future__ import annotations

import os
import pathlib
from dataclasses import dataclass

import pytest

import cocoindex as coco

from coco_harness import (
    HARNESS,
    MODEL_CLIENT,
    HarnessConfig,
    RoleSpec,
    SessionResult,
    ToolRegistry,
    run_open_session,
)
from coco_harness.hints import REPLAY_TOOL, RunHints
from coco_harness.report import RunReport, render_plan, render_stats_line
from coco_harness.spec import SessionSpec
from coco_harness.testing import ScriptedModel
from coco_harness.tools import agents, sources, targets
from coco_harness.trace import SessionScratch, Usage
from coco_harness.transcript import ModelOutput, ToolCall, Transcript
from helpers import explode_tool

ORCHESTRATOR = RoleSpec(
    "orchestrator", "test", ("open_source", "open_target", "observe", "for_each")
)
WORKER = RoleSpec("worker", "test", ("read_input", "declare"))
PLANNER = RoleSpec("planner", "test", ("call",), volatile_ok=True)

PLAN = "invoice — the number\nvendor — who billed\ntotal — the amount due"


def call(tool: str, **args: object) -> ToolCall:
    return ToolCall(tool, dict(args))


@dataclass
class Scenario:
    """The orchestrator of shape A, or of shape C when ``observe`` is set."""

    src: pathlib.Path
    out: pathlib.Path
    observe: bool

    def script(
        self, spec: SessionSpec, prefix: Transcript, hints: RunHints
    ) -> ModelOutput:
        n = len(prefix.steps)
        role = spec.role.name
        if role == "orchestrator":
            if n == 0:
                return ModelOutput(
                    calls=(
                        call(
                            "open_source",
                            kind="localfs",
                            name="src",
                            path=str(self.src),
                        ),
                        call(
                            "open_target",
                            kind="localfs_dir",
                            name="out",
                            path=str(self.out),
                        ),
                    )
                )
            if n == 1 and self.observe:
                return ModelOutput(
                    calls=(
                        call(
                            "observe",
                            key="plan",
                            question="Which columns does the data have?",
                            inputs=["src:src"],
                        ),
                    )
                )
            if n == 1 or (n == 2 and self.observe):
                plan = prefix.steps[1].results[0].value["plan"] if self.observe else ""
                return ModelOutput(
                    calls=(
                        call(
                            "for_each",
                            source="src:src",
                            key="items",
                            task=f"One output per file. {plan}".strip(),
                            inputs=["tgt:out"],
                        ),
                    )
                )
            return ModelOutput(answer="done")
        if role == "planner":
            if n == 0:
                return ModelOutput(
                    calls=(
                        call("call", handle="src:src", method="sample", args={"k": 5}),
                    )
                )
            if hints.dump is not None:
                # The sample moved; the previous plan is in the [replay] block: keep it.
                return ModelOutput(calls=(call(REPLAY_TOOL, from_step=1),))
            return ModelOutput(answer=PLAN)
        # worker: read, declare, answer
        if n == 0:
            return ModelOutput(calls=(call("read_input"),))
        item = prefix.steps[0].results[0].value
        if n == 1:
            return ModelOutput(
                calls=(
                    call(
                        "declare",
                        target="tgt:out",
                        key=item["key"] + ".json",
                        value="json:" + item["text"],
                    ),
                )
            )
        return ModelOutput(answer="ok:" + item["key"])


@dataclass
class Fixture:
    scenario: Scenario
    model: ScriptedModel
    app: coco.App[..., SessionResult]
    scratch: SessionScratch

    def run(self, usage: Usage | None = None) -> RunReport:
        self.model.reset()
        self.scratch.ran = False
        result = self.app.update_blocking()
        assert isinstance(result, SessionResult)
        report = RunReport.from_result(
            "flow",
            result.trace,
            ran=self.scratch.ran,
            model="scripted",
            wall_seconds=0.0,
            usage=usage,
        )
        # The derived count is pinned to the model's own count on every run.
        assert report.spent.model_calls == self.model.calls
        return report

    def edit(self, name: str) -> None:
        path = self.scenario.src / name
        path.write_text(path.read_text() + " (edited)")
        st = os.stat(path)
        os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000))


def setup(
    coco_env: coco.Environment,
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    observe: bool,
    files: int,
) -> Fixture:
    # Paths relative to the flow directory, as the sample flows use them.
    monkeypatch.chdir(tmp_path)
    src = pathlib.Path("src")
    src.mkdir()
    for i in range(files):
        (src / f"{i}.txt").write_text(f"item {i}")
    scenario = Scenario(src, pathlib.Path("out"), observe)
    model = ScriptedModel(scenario.script)
    config = HarnessConfig(
        roles={r.name: r for r in (ORCHESTRATOR, WORKER, PLANNER)},
        tools=ToolRegistry(
            [
                sources.open_source,
                sources.call_method,
                sources.read_input,
                targets.open_target,
                targets.declare,
                agents.observe,
                agents.for_each,
            ]
        ),
    )
    coco_env.context_provider.provide(MODEL_CLIENT, model)
    coco_env.context_provider.provide(HARNESS, config)
    scratch = SessionScratch()
    app = coco.App(
        coco.AppConfig(name="observability", environment=coco_env),
        run_open_session,
        config.spec("orchestrator", "one output per file"),
        scratch=scratch,
    )
    return Fixture(scenario, model, app, scratch)


def test_shape_a_stats_first_unchanged_and_one_edit(
    coco_env: coco.Environment, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    f = setup(coco_env, tmp_path, monkeypatch, observe=False, files=3)

    first = f.run(usage=Usage(1200, 300, 800))
    assert first.root.status == "new"
    assert render_stats_line(first) == (
        "stats: model calls 12, sessions 4, tool calls 9, "
        "spent tokens 1200 in (provider-cached 800 / 67%), 300 out, files 3, 0.0s"
    )
    assert first.model_calls_by_role() == {"orchestrator": 3, "worker": 9}

    # Skipped whole: the root's scratch is untouched and the stored trace is
    # credited as its producer ran it. The tokens segment is omitted: no call.
    unchanged = f.run()
    assert f.model.calls == 0 and unchanged.root.status == "skipped"
    assert render_stats_line(unchanged) == (
        "stats: model calls 12 (reused 12 / 100%), sessions 4 (skipped 4 / 100%), "
        "tool calls 9 (reused 9 / 100%), files 3 (unchanged 3 / 100%), 0.0s"
    )

    # One edit: the fan-out's session re-runs its body (its turns hit) and
    # that file's worker re-infers after read_input; the other two are skipped.
    f.edit("1.txt")
    edited = f.run()  # no usage: the client reported none
    assert render_stats_line(edited) == (
        "stats: model calls 12 (reused 10 / 83%), sessions 4 (skipped 2 / 50%), "
        "tool calls 9 (reused 4 / 44%), spent tokens ?, files 3 (unchanged 2 / 67%), 0.0s"
    )
    assert edited.root.reason == "walk of src:src changed"
    fan_out = edited.root.steps[1].actions[0].child
    assert fan_out is not None and (fan_out.counts.ran, fan_out.counts.skipped) == (
        1,
        2,
    )
    (item,) = fan_out.items
    assert item.key == "1.txt" and item.session.reason == "input changed"
    assert [s.served for s in item.session.steps] == ["hit", "inferred", "inferred"]
    assert [d.written for d in item.session.declared] == [True]


def test_shape_c_stats_and_plan(
    coco_env: coco.Environment, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    f = setup(coco_env, tmp_path, monkeypatch, observe=True, files=4)

    first = f.run()
    assert render_stats_line(first) == (
        "stats: model calls 18, sessions 6, tool calls 13, spent tokens ?, files 4, 0.0s"
    )

    # Skipped whole: the planner's recorded sample validates from its own
    # entry (transitive validation, engine PR #2671), so the orchestrator is
    # skipped too, and the stored plan shows the planner as reused.
    unchanged = f.run()
    assert f.model.calls == 0 and unchanged.root.status == "skipped"
    assert render_stats_line(unchanged) == (
        "stats: model calls 18 (reused 18 / 100%), sessions 6 (skipped 6 / 100%), "
        "tool calls 13 (reused 13 / 100%), files 4 (unchanged 4 / 100%), 0.0s"
    )
    assert "     └─ planner                                  reused · 2 turns" in (
        render_plan(unchanged)
    )

    # One edit: the sample covers every file, so the planner re-runs, sees the
    # diff and picks its previous plan (one call); the plan is byte-identical,
    # so only the edited file's worker re-infers.
    f.edit("3.txt")
    edited = f.run()
    assert render_stats_line(edited) == (
        "stats: model calls 18 (reused 15 / 83%), sessions 6 (skipped 3 / 50%), "
        "tool calls 13 (reused 6 / 46%), spent tokens ?, files 4 (unchanged 3 / 75%), 0.0s"
    )
    assert render_plan(edited) == "\n".join(
        [
            "plan: flow · scripted · 0.0s",
            "orchestrator                                     re-ran: walk of src:src changed · 4 turns: 4 hit · 0 calls",
            "   0  open_source src:src src                    hit",
            "      open_target tgt:out out",
            "   1  observe plan ← src:src                     hit",
            '       "Which columns does the data have?"',
            "     └─ planner                                  re-ran: sample of src:src changed · 2 turns: 1 hit, 1 picked · 1 call",
            "        0  call src:src sample                   hit · result differs from last run",
            "        1  answer                                picked [1]",
            "          plan: invoice — the number",
            "                vendor — who billed",
            "                total — the amount due",
            "   2  for_each items: src:src → tgt:out          hit · worker × 4: 1 re-ran, 3 skipped",
            '       "One output per file. invoice — the number vendor — who billed total — the amount due"',
            "     └─ 3.txt                                    re-ran: input changed · 3 turns: 1 hit, 2 inferred · 2 calls",
            "          declare 3.txt.json                     written",
            "   3  answer                                     hit",
        ]
    )


def test_a_stored_plan_is_rendered_without_live_detail(
    coco_env: coco.Environment, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    f = setup(coco_env, tmp_path, monkeypatch, observe=False, files=3)
    f.run()
    unchanged = f.run()
    assert render_plan(unchanged) == "\n".join(
        [
            "plan: flow · scripted · 0.0s",
            "orchestrator                                     skipped: nothing it read changed · 3 turns · 9 tool calls reused",
            "   0  open_source src:src src",
            "      open_target tgt:out out",
            "   1  for_each items: src:src → tgt:out          worker × 3: all reused",
            '       "One output per file."',
            "   2  answer",
        ]
    )


def test_a_contained_failure_is_reported_as_a_rerun_next_run(
    coco_env: coco.Environment, tmp_path: pathlib.Path
) -> None:
    """An absorbed environment error re-runs the session next run; the stats line says so."""

    def script(spec: SessionSpec, prefix: Transcript, hints: RunHints) -> ModelOutput:
        if not prefix.steps:
            return ModelOutput(calls=(call("explode", key="k"),))
        return ModelOutput(answer="done")

    model = ScriptedModel(script)
    errors: dict[str, Exception] = {"k": RuntimeError("disk on fire")}
    config = HarnessConfig(
        roles={"orchestrator": RoleSpec("orchestrator", "test", ("explode",))},
        tools=ToolRegistry([explode_tool(errors)]),
    )
    coco_env.context_provider.provide(MODEL_CLIENT, model)
    coco_env.context_provider.provide(HARNESS, config)
    scratch = SessionScratch()
    app = coco.App(
        coco.AppConfig(name="failures", environment=coco_env),
        run_open_session,
        config.spec("orchestrator", "poke"),
        scratch=scratch,
    )

    def run() -> RunReport:
        model.reset()
        result = app.update_blocking()
        assert isinstance(result, SessionResult)
        return RunReport.from_result(
            "flow",
            result.trace,
            ran=scratch.ran,
            model="scripted",
            wall_seconds=0.0,
            usage=None,
        )

    first = run()
    assert first.failures() == ["tool:explode"]
    assert render_stats_line(first) == (
        "stats: model calls 2, sessions 1, tool calls 1, spent tokens ?, files 0, "
        "re-runs next run: 1 contained tool failure, 0.0s"
    )
    # Fixed: the body re-runs for the recorded failure, the changed result
    # costs one inference, and nothing re-runs after that.
    errors.clear()
    healed = run()
    assert healed.root.reason == "absorbed a failure last run"
    assert healed.failures() == [] and healed.spent.model_calls == 1

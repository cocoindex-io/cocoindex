"""The reduce tool end to end: group slots, the oversize-output retry, and reuse across runs."""

from __future__ import annotations

import pathlib

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
from coco_harness.hints import RunHints
from coco_harness.spec import SessionSpec
from coco_harness.testing import ScriptedModel
from coco_harness.tools import agents, sources
from coco_harness.transcript import ModelOutput, ToolCall, Transcript

ORCHESTRATOR = RoleSpec(
    "orchestrator", "test", ("open_source", "for_each", "reduce", "call")
)
WORKER = RoleSpec("worker", "test", ("read_input",))
REDUCER = RoleSpec("reducer", "test", ("read_input",))

# Each worker answers "sum:doc N" (9 characters, 2 estimated tokens). With a
# threshold of 6 tokens a group holds at most three items and the output bound
# is 3 tokens (12 characters), so a reducer that echoes its members is too long
# and is re-asked at attempt slot 1.
THRESHOLD = 6


def call(tool: str, **args: object) -> ToolCall:
    return ToolCall(tool, dict(args))


def script(docs: pathlib.Path) -> ScriptedModel:
    def run(spec: SessionSpec, prefix: Transcript, hints: RunHints) -> ModelOutput:
        n = len(prefix.steps)
        if spec.role.name == "orchestrator":
            if n == 0:
                return ModelOutput(
                    calls=(
                        call(
                            "open_source",
                            kind="localfs",
                            name="docs",
                            path=str(docs),
                            patterns=["*.md"],
                        ),
                    )
                )
            if n == 1:
                return ModelOutput(
                    calls=(
                        call(
                            "for_each", source="src:docs", key="notes", task="summarize"
                        ),
                    )
                )
            if n == 2:
                return ModelOutput(
                    calls=(
                        call(
                            "reduce",
                            collection="coll:notes",
                            key="report",
                            task="combine",
                            threshold_tokens=THRESHOLD,
                        ),
                    )
                )
            if n == 3:
                return ModelOutput(
                    calls=(call("call", handle="res:report", method="read"),)
                )
            return ModelOutput(answer=str(prefix.steps[3].results[0].value))
        if n == 0:
            return ModelOutput(calls=(call("read_input"),))
        value = prefix.steps[0].results[0].value
        if spec.role.name == "worker":
            return ModelOutput(answer="sum:" + value["text"])
        # reducer: echo the members (too long) unless told to be brief
        if "too long" in spec.task:
            return ModelOutput(answer="ok")
        return ModelOutput(answer="+".join(value))

    return ScriptedModel(run)


def test_reduce_tool_groups_retries_oversize_outputs_and_reuses_groups(
    coco_env: coco.Environment, tmp_path: pathlib.Path
) -> None:
    docs = tmp_path / "docs"
    docs.mkdir()
    for i in range(5):
        (docs / f"{i}.md").write_text(f"doc {i}")

    model = script(docs)
    config = HarnessConfig(
        roles={r.name: r for r in (ORCHESTRATOR, WORKER, REDUCER)},
        tools=ToolRegistry(
            [
                sources.open_source,
                sources.read_input,
                sources.call_method,
                agents.for_each,
                agents.reduce,
            ]
        ),
    )
    coco_env.context_provider.provide(MODEL_CLIENT, model)
    coco_env.context_provider.provide(HARNESS, config)
    app = coco.App(
        coco.AppConfig(name="reduce_tool", environment=coco_env),
        run_open_session,
        config.spec("orchestrator", "report on the docs"),
    )

    def run() -> SessionResult:
        model.reset()
        result = app.update_blocking()
        assert isinstance(result, SessionResult)
        return result

    first = run()
    # Every leaf group echoed its members first and was re-asked; the final answer
    # is a reducer answer, not raw member text.
    assert first.answer in {"ok", "ok+ok", "ok+ok+ok"}
    assert (
        model.calls_for("reducer") >= 2 * 2
    )  # at least one group: echo attempt + brief attempt
    reducer_calls = model.calls_for("reducer")
    assert model.calls_for("orchestrator") == 5

    # Unchanged: every group hits at its slot.
    assert run() == first
    assert model.calls == 0

    # Editing one file re-runs its worker and only the reducer calls on its path.
    (docs / "1.md").write_text("doc 1 (edited)")
    run()
    assert 0 < model.calls_for("reducer") < reducer_calls
    assert model.calls_for("worker") == 1

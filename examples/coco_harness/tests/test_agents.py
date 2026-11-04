"""Open children: call_agent, start_agent, observe; result handles crossing mounts; slot keys."""

from __future__ import annotations

import pathlib
from dataclasses import dataclass

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
from coco_harness.tools import agents, sources, targets
from coco_harness.transcript import ModelOutput, ToolCall, Transcript

ORCHESTRATOR = RoleSpec(
    "orchestrator",
    "test",
    ("open_target", "declare", "call_agent", "start_agent", "observe", "call"),
)
WORKER = RoleSpec("worker", "test", ("declare",))
PLANNER = RoleSpec("planner", "test", ())


def call(tool: str, **args: object) -> ToolCall:
    return ToolCall(tool, dict(args))


def _child(spec: SessionSpec, prefix: Transcript) -> ModelOutput:
    """Workers declare into a target if given one, then answer; planners answer a plan."""
    if spec.role.name == "planner":
        return ModelOutput(answer="plan: two columns")
    targets_given = [c.handle for c in spec.inputs if c.kind == "localfs_dir"]
    if not prefix.steps and targets_given:
        return ModelOutput(
            calls=(
                call("declare", target=targets_given[0], key="bg.txt", value="from bg"),
            )
        )
    return ModelOutput(answer=f"child answer to: {spec.task}")


@dataclass
class Fixture:
    model: ScriptedModel
    app: coco.App[..., SessionResult]
    out: pathlib.Path

    def run(self) -> SessionResult:
        self.model.reset()
        result = self.app.update_blocking()
        assert isinstance(result, SessionResult)
        return result


def setup(
    coco_env: coco.Environment, tmp_path: pathlib.Path, orchestrator: ScriptedModel
) -> Fixture:
    config = HarnessConfig(
        roles={r.name: r for r in (ORCHESTRATOR, WORKER, PLANNER)},
        tools=ToolRegistry(
            [
                targets.open_target,
                targets.declare,
                sources.call_method,
                agents.call_agent,
                agents.start_agent,
                agents.observe,
            ]
        ),
    )
    coco_env.context_provider.provide(MODEL_CLIENT, orchestrator)
    coco_env.context_provider.provide(HARNESS, config)
    app = coco.App(
        coco.AppConfig(name="agents", environment=coco_env),
        run_open_session,
        config.spec("orchestrator", "delegate"),
    )
    return Fixture(orchestrator, app, tmp_path / "out")


def test_result_handles_cross_the_mount_and_resolve_or_fail_in_declare(
    coco_env: coco.Environment, tmp_path: pathlib.Path
) -> None:
    out = tmp_path / "out"

    def script(spec: SessionSpec, prefix: Transcript, hints: RunHints) -> ModelOutput:
        if spec.role.name != "orchestrator":
            return _child(spec, prefix)
        n = len(prefix.steps)
        if n == 0:
            return ModelOutput(
                calls=(
                    call("open_target", kind="localfs_dir", name="out", path=str(out)),
                )
            )
        if n == 1:
            return ModelOutput(
                calls=(call("call_agent", key="k1", task="answer please"),)
            )
        if n == 2:
            return ModelOutput(
                calls=(
                    call("declare", target="tgt:out", key="answer.txt", value="res:k1"),
                    call(
                        "declare",
                        target="tgt:out",
                        key="missing.txt",
                        value="res:missing",
                    ),
                    call("declare", target="tgt:out", key="wrong.txt", value="tgt:out"),
                    call("call", handle="res:k1", method="head", args={"chars": 5}),
                )
            )
        card = prefix.steps[1].results[0].value
        return ModelOutput(
            answer=str(card)
            + "|"
            + str([(r.ok, r.value) for r in prefix.steps[2].results])
        )

    f = setup(coco_env, tmp_path, ScriptedModel(script))
    result = f.run()
    assert f.model.calls_for("worker") == 1
    card, oks = (result.answer or "").split("|", 1)
    # The call_agent result card carries the handle and a widened size, not the text.
    assert card == "{'chars': '16–32', 'result': 'res:k1'}"
    assert "(True, {'declared': 'answer.txt'})" in oks
    assert "(False, \"unknown handle 'res:missing'" in oks
    assert "(False, \"'tgt:out' is a localfs_dir handle, not a result\")" in oks
    assert "(True, 'child')" in oks
    assert (out / "answer.txt").read_text() == "child answer to: answer please"
    assert not (out / "missing.txt").exists() and not (out / "wrong.txt").exists()

    f.run()
    assert f.model.calls == 0


def test_duplicate_child_keys_are_tool_errors_not_engine_errors(
    coco_env: coco.Environment, tmp_path: pathlib.Path
) -> None:
    def script(spec: SessionSpec, prefix: Transcript, hints: RunHints) -> ModelOutput:
        if spec.role.name != "orchestrator":
            return _child(spec, prefix)
        n = len(prefix.steps)
        same = call("call_agent", key="same", task="t")
        if n == 0:
            return ModelOutput(calls=(same, same))  # both fail before anything mounts
        if n == 1:
            return ModelOutput(calls=(call("call_agent", key="k2", task="t"),))
        if n == 2:
            return ModelOutput(
                calls=(call("call_agent", key="k2", task="again"),)
            )  # reused key
        if n == 3:  # rejected before mounting: the key is not consumed
            return ModelOutput(
                calls=(call("call_agent", key="k3", task="t", role="nope"),)
            )
        if n == 4:
            return ModelOutput(calls=(call("call_agent", key="k3", task="t"),))
        return ModelOutput(
            answer=str([[r.ok for r in s.results] for s in prefix.steps])
        )

    f = setup(coco_env, tmp_path, ScriptedModel(script))
    result = f.run()
    assert result.answer == "[[False, False], [True], [False], [False], [True]]"
    assert f.model.calls_for("worker") == 2


def test_background_agents_act_through_inputs_and_planners_return_plans(
    coco_env: coco.Environment, tmp_path: pathlib.Path
) -> None:
    out = tmp_path / "out"

    def script(spec: SessionSpec, prefix: Transcript, hints: RunHints) -> ModelOutput:
        if spec.role.name != "orchestrator":
            return _child(spec, prefix)
        n = len(prefix.steps)
        if n == 0:
            return ModelOutput(
                calls=(
                    call("open_target", kind="localfs_dir", name="out", path=str(out)),
                )
            )
        if n == 1:
            return ModelOutput(
                calls=(
                    call("start_agent", key="bg", task="write", inputs=["tgt:out"]),
                    call("observe", key="plan", question="what columns?"),
                )
            )
        return ModelOutput(answer=str([r.value for r in prefix.steps[1].results]))

    f = setup(coco_env, tmp_path, ScriptedModel(script))
    result = f.run()
    assert result.answer == "[{'job': 'job:bg'}, {'plan': 'plan: two columns'}]"
    assert f.model.calls_for("worker") == 2  # declare, then answer (never returned)
    assert f.model.calls_for("planner") == 1
    assert (out / "bg.txt").read_text() == "from bg"

    f.run()
    assert f.model.calls == 0

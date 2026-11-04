"""The replay policy end to end, with a scripted model and a probe tool.

Each test counts model calls per run. A probe tool returns values the test
mutates between runs, which is how a tool result "changes" and forces a
divergence.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace

import pytest

import cocoindex as coco

from coco_harness import (
    HARNESS,
    MODEL_CLIENT,
    HarnessConfig,
    RoleSpec,
    SessionResult,
    ToolError,
    ToolRegistry,
    run_open_session,
)
from coco_harness.hints import RunHints
from coco_harness.spec import SessionSpec
from coco_harness.testing import ScriptedModel
from coco_harness.tools import SessionContext, tool
from coco_harness.transcript import ModelOutput, ToolCall, Transcript
from helpers import probe_tool

ROLE = RoleSpec(name="orchestrator", system_prompt="test", tool_names=("probe",))


def probe(name: object) -> ModelOutput:
    return ModelOutput(calls=(ToolCall("probe", {"name": name}),))


def replay(from_step: object, steps: int | None = None) -> ModelOutput:
    args: dict[str, object] = {"from_step": from_step}
    if steps is not None:
        args["steps"] = steps
    return ModelOutput(calls=(ToolCall("replay", args),))


def finish(prefix: Transcript, tag: str = "done") -> ModelOutput:
    return ModelOutput(answer=f"{tag}:{prefix.steps[0].results[0].value}")


def basic_script(spec: SessionSpec, prefix: Transcript, hints: RunHints) -> ModelOutput:
    """probe a; probe b; finish with a's value."""
    n = len(prefix.steps)
    if n == 0:
        return probe("a")
    if n == 1:
        return probe("b")
    return finish(prefix)


def replaying_script(
    spec: SessionSpec, prefix: Transcript, hints: RunHints
) -> ModelOutput:
    """A model that trusts the dump: fast-forward from it, finish when told it reached the end."""
    if hints.notes:
        return finish(prefix)
    if hints.dump is not None and hints.dump.at_step == len(prefix.steps):
        return replay(hints.dump.first_step)
    return basic_script(spec, prefix, hints)


@dataclass
class Harness:
    env: coco.Environment
    task: str = "task"
    values: dict[str, object] = field(
        default_factory=lambda: {"a": "A", "b": "B", "c": "C"}
    )

    def __post_init__(self) -> None:
        self.model = ScriptedModel(basic_script)
        self.config = HarnessConfig(
            roles={ROLE.name: ROLE}, tools=ToolRegistry([probe_tool(self.values)])
        )
        self.env.context_provider.provide(MODEL_CLIENT, self.model)
        self.env.context_provider.provide(HARNESS, self.config)

        async def main() -> SessionResult:
            # The spec is built here, each run, so a test can edit the task, the
            # tools or the version between runs: all three are in every turn's key.
            return await coco.use_mount(
                run_open_session, self.config.spec(ROLE.name, self.task)
            )

        self.app = coco.App(coco.AppConfig(name="replay", environment=self.env), main)

    def run(self) -> SessionResult:
        self.model.reset()
        result = self.app.update_blocking()
        assert isinstance(result, SessionResult)
        return result

    def reconfigure(self, **changes: object) -> None:
        """A config is frozen once provided: an edit is a new config, provided anew."""
        self.config = replace(self.config, **changes)
        self.env.context_provider.provide(HARNESS, self.config)


def test_unchanged_run_makes_no_model_calls(coco_env: coco.Environment) -> None:
    h = Harness(coco_env)
    first = h.run()
    assert h.model.calls == 3
    assert first == SessionResult(answer="done:A", num_steps=3)

    second = h.run()
    assert h.model.calls == 0
    assert second == first


def test_changed_result_shows_dump_once_then_fast_forwards(
    coco_env: coco.Environment,
) -> None:
    h = Harness(coco_env)
    h.run()

    h.values["a"] = "A2"  # the probe at step 0 now answers differently
    h.model.script = replaying_script
    result = h.run()

    # Turn 0 hit (empty prefix) and re-executed probe(a); its result differed, so
    # turn 1 was the first model call, with the dump placed after the changed step.
    assert h.model.calls == 2
    first, second = h.model.seen
    assert first.step == 1
    assert first.hints.dump is not None
    assert (first.hints.dump.at_step, first.hints.dump.first_step) == (1, 1)
    assert [o.answer is not None for o in first.hints.dump.outputs] == [False, True]
    assert [(d.at_step, d.before, d.after) for d in first.hints.diffs] == [
        (0, "A", "A2")
    ]
    # Fast-forward replayed probe(b) with no model call, stopped before the final
    # answer and asked the model to confirm; the dump did not move.
    assert second.step == 2 and second.hints.notes
    assert second.hints.dump is not None and second.hints.dump.at_step == 1
    assert result == SessionResult(answer="done:A2", num_steps=3)

    # The fast-forwarded turn wrote its memo entry: a third run is free.
    assert h.run() == result
    assert h.model.calls == 0


def test_task_edit_dumps_at_turn_zero_and_fast_forwards_everything(
    coco_env: coco.Environment,
) -> None:
    h = Harness(coco_env)
    h.run()

    h.task = "task v2"  # input change: every turn misses, data unchanged
    h.model.script = replaying_script
    result = h.run()

    assert h.model.calls == 2
    first, second = h.model.seen
    assert first.step == 0
    assert first.hints.dump is not None
    assert (first.hints.dump.at_step, first.hints.dump.first_step) == (0, 0)
    assert len(first.hints.dump.outputs) == 3
    assert second.step == 2 and second.hints.notes
    assert result.answer == "done:A"

    assert h.run() == result
    assert h.model.calls == 0


def test_version_bump_invalidates_every_turn(coco_env: coco.Environment) -> None:
    h = Harness(coco_env)
    h.run()
    h.reconfigure(version=2)
    h.run()
    assert h.model.calls == 3


def test_dump_is_frozen_where_the_model_first_saw_it(
    coco_env: coco.Environment,
) -> None:
    """A model that ignores the dump and repeats its actions still sees one dump, in one place."""
    h = Harness(coco_env)
    h.run()

    h.task = "task v2"
    h.run()  # basic_script: every turn infers and reproduces last run's actions

    assert h.model.calls == 3
    assert [s.step for s in h.model.seen] == [0, 1, 2]
    assert all(
        s.hints.dump is not None and s.hints.dump.at_step == 0 for s in h.model.seen
    )


def test_model_may_pick_last_runs_final_answer(coco_env: coco.Environment) -> None:
    h = Harness(coco_env)
    h.run()

    h.values["b"] = "B2"  # changes a result the final answer does not depend on
    h.model.script = replaying_script
    result = h.run()

    # Turns 0 and 1 hit; probe(b) differed, so the dump holds only the final
    # answer. The script picks it; a pick of a final answer is honoured as is.
    assert h.model.calls == 1
    (only,) = h.model.seen
    assert only.step == 2
    assert only.hints.dump is not None
    assert (only.hints.dump.at_step, only.hints.dump.first_step) == (2, 2)
    assert [(d.at_step, d.before, d.after) for d in only.hints.diffs] == [
        (1, "B", "B2")
    ]
    assert result.answer == "done:A"


def test_pick_keeps_its_occurrence_among_repeated_actions(
    coco_env: coco.Environment,
) -> None:
    """Previous run: a, b, a, c, finish. Picking the second `a` continues with `c`, not `b`."""
    h = Harness(coco_env)

    def first_run(
        spec: SessionSpec, prefix: Transcript, hints: RunHints
    ) -> ModelOutput:
        n = len(prefix.steps)
        return (
            [probe("a"), probe("b"), probe("a"), probe("c")][n]
            if n < 4
            else ModelOutput(answer="x")
        )

    h.model.script = first_run
    assert h.run().num_steps == 5

    def picker(spec: SessionSpec, prefix: Transcript, hints: RunHints) -> ModelOutput:
        if hints.notes:
            return ModelOutput(answer="y")
        assert hints.dump is not None and len(prefix.steps) == 0
        return replay(from_step=2)

    h.task = "task v2"
    h.model.script = picker
    result = h.run()

    assert result == SessionResult(answer="y", num_steps=3)  # a, c, finish
    assert h.model.calls == 2
    assert h.model.seen[1].step == 2 and h.model.seen[1].hints.notes


def test_bounded_pick_stops_after_its_steps(coco_env: coco.Environment) -> None:
    h = Harness(coco_env)
    h.run()

    def picker(spec: SessionSpec, prefix: Transcript, hints: RunHints) -> ModelOutput:
        if len(prefix.steps) == 0:
            return replay(from_step=0, steps=2)
        return ModelOutput(answer="after two")

    h.task = "task v2"
    h.model.script = picker
    result = h.run()

    # a (picked) and b (replayed) ran without further inference; then the model was
    # asked again — no note, because the budget, not the final answer, stopped it.
    assert result == SessionResult(answer="after two", num_steps=3)
    assert h.model.calls == 2
    assert h.model.seen[1].step == 2 and not h.model.seen[1].hints.notes


def test_invalid_pick_is_shown_back_once_and_the_note_stays(
    coco_env: coco.Environment,
) -> None:
    h = Harness(coco_env)
    h.run()

    attempts: list[RunHints] = []

    def bad_then_good(
        spec: SessionSpec, prefix: Transcript, hints: RunHints
    ) -> ModelOutput:
        if len(prefix.steps) == 0:
            attempts.append(hints)
            if len(attempts) == 1:
                return replay(from_step=99)
        return basic_script(spec, prefix, hints)

    h.task = "task v2"
    h.model.script = bad_then_good
    assert h.run().answer == "done:A"
    assert len(attempts) == 2
    assert attempts[1].notes and "invalid replay" in attempts[1].notes[0].text
    # The note the model saw on retry is still in the history at the next turn.
    assert any("invalid replay" in n.text for n in h.model.seen[-1].hints.notes)

    h.task = "task v3"
    h.model.script = lambda spec, prefix, hints: replay(from_step=99)
    with pytest.raises(Exception, match="unresolvable replay"):
        h.run()

    # Arguments that are not an object are an invalid pick too, not a crash.
    h.task = "task v4"
    seen_shapes: list[int] = []

    def list_args_then_good(
        spec: SessionSpec, prefix: Transcript, hints: RunHints
    ) -> ModelOutput:
        if len(prefix.steps) == 0 and not seen_shapes:
            seen_shapes.append(1)
            return ModelOutput(calls=(ToolCall("replay", []),))  # type: ignore[arg-type]
        return basic_script(spec, prefix, hints)

    h.model.script = list_args_then_good
    assert h.run().answer == "done:A"


def test_cached_decision_from_a_failed_run_wins_over_a_requested_replay(
    coco_env: coco.Environment,
) -> None:
    """A fast-forward request met by an existing turn memo executes the memo, not the request.

    The failed runs here hit their deadline inside a tool: deadline expiry is
    never contained, so the run fails and `prev` keeps the last successful
    transcript while the completed turns keep their memos.
    """
    h = Harness(coco_env)

    def v1(spec: SessionSpec, prefix: Transcript, hints: RunHints) -> ModelOutput:
        n = len(prefix.steps)
        return (
            [probe("a"), probe("b"), probe("c")][n]
            if n < 3
            else ModelOutput(answer="v1")
        )

    h.model.script = v1
    assert h.run().num_steps == 4  # prev = a, b, c, finish

    # Task v2 decides `x` at turn 1 and fails there: turn 1's memo (x) is committed.
    h.task = "task v2"
    h.values["x"] = coco.DeadlineExceededError("x is down")
    h.model.script = (
        lambda spec, prefix, hints: probe("a") if not prefix.steps else probe("x")
    )
    with pytest.raises(Exception, match="x is down"):
        h.run()

    # Task v3 fails at turn 0, overwriting only turn 0's memo.
    h.task = "task v3"
    h.values["y"] = coco.DeadlineExceededError("y is down")
    h.model.script = lambda spec, prefix, hints: probe("y")
    with pytest.raises(Exception, match="y is down"):
        h.run()

    # Back on v2 with x repaired. Turn 0 misses and the model asks to replay from
    # v1's step 0. After `a`, the harness requests `b`, but the v2 memo for this
    # exact prefix answers `x`: the session's own earlier decision wins, it runs,
    # fast-forward stops, and the model is told.
    h.task = "task v2"
    h.values["x"] = "B"

    def resume(spec: SessionSpec, prefix: Transcript, hints: RunHints) -> ModelOutput:
        if hints.notes:
            return ModelOutput(answer="resumed")
        assert not prefix.steps
        return replay(from_step=0)

    h.model.script = resume
    result = h.run()
    assert result == SessionResult(answer="resumed", num_steps=3)  # a, x, finish
    assert h.model.calls == 2
    note = h.model.seen[1].hints.notes[0].text
    assert "interrupted" in note and "[1]" in note


def test_environment_errors_are_results_in_open_sessions_and_heal_next_run(
    coco_env: coco.Environment,
) -> None:
    """An open session reads an environment error as a result; the tool runs again next run."""
    h = Harness(coco_env)
    h.values["b"] = RuntimeError("disk on fire")

    def script(spec: SessionSpec, prefix: Transcript, hints: RunHints) -> ModelOutput:
        n = len(prefix.steps)
        if n == 0:
            return probe("a")
        if n == 1:
            return probe("b")
        return ModelOutput(answer=str(prefix.steps[1].results[0]))

    h.model.script = script
    first = h.run()
    assert (
        first.answer
        == "ToolResult(tool='probe', ok=False, value='RuntimeError: disk on fire')"
    )
    assert h.model.calls == 3

    # The cause is fixed: probe(b) runs again, its result differs, and only the
    # turn that read it needs the model. The transcript heals without a reset.
    h.values["b"] = "B"
    assert h.run().answer == "ToolResult(tool='probe', ok=True, value='B')"
    assert h.model.calls == 1

    # A ToolError reaches the model the same way, as the model's own error.
    h.values["a"] = ToolError("no such probe today")
    h.model.script = basic_script
    assert h.run().answer == "done:no such probe today"

    # Deadline expiry is never contained: it fails the run.
    h.values["a"] = coco.DeadlineExceededError("out of time")
    with pytest.raises(Exception, match="out of time"):
        h.run()


def test_argument_validation_is_the_models_error_and_internal_errors_are_reported(
    coco_env: coco.Environment,
) -> None:
    h = Harness(coco_env)

    # Missing required argument and wrong type: ok=False results, the session goes on.
    def bad_args(spec: SessionSpec, prefix: Transcript, hints: RunHints) -> ModelOutput:
        n = len(prefix.steps)
        if n == 0:
            return ModelOutput(calls=(ToolCall("probe", {}),))
        if n == 1:
            return probe(5)
        if n == 2:
            return ModelOutput(calls=(ToolCall("probe", {"name": "a", "extra": 1}),))
        return ModelOutput(answer=str([s.results[0].value for s in prefix.steps]))

    h.model.script = bad_args
    answer = h.run().answer
    assert "missing argument 'name'" in answer
    assert "must be of type string" in answer
    assert "unknown argument 'extra'" in answer

    # A KeyError inside a handler with valid arguments is an environment error: in
    # an open session it is reported to the model, named, not hidden.
    @tool("broken", "Looks something up internally.")
    async def broken(ctx: SessionContext, args: dict[str, object]) -> object:
        cache: dict[str, str] = {}
        return cache["entry"]

    h.reconfigure(
        tools=ToolRegistry([probe_tool(h.values), broken]),
        roles={ROLE.name: replace(ROLE, tool_names=("probe", "broken"))},
    )
    h.task = "task v2"
    h.model.script = lambda spec, prefix, hints: (
        ModelOutput(calls=(ToolCall("broken", {}),))
        if not prefix.steps
        else ModelOutput(answer=str(prefix.steps[0].results[0].value))
    )
    assert h.run().answer == "KeyError: 'entry'"


def test_non_object_arguments_are_the_models_error_and_survive_the_run(
    coco_env: coco.Environment,
) -> None:
    h = Harness(coco_env)

    def script(spec: SessionSpec, prefix: Transcript, hints: RunHints) -> ModelOutput:
        if not prefix.steps:
            return ModelOutput(calls=(ToolCall("probe", ["a"]),))  # type: ignore[arg-type]
        return ModelOutput(answer=str(prefix.steps[0].results[0].value))

    h.model.script = script
    first = h.run()
    assert first.answer == 'arguments must be a JSON object; got ["a"]'

    # The stored transcript and the turn memo are dict-typed: the next run loads
    # them and replays without inference.
    assert h.run() == first
    assert h.model.calls == 0

    # After a task edit the canonical invalid call is in the dump the model sees,
    # and picking it reproduces the same model-facing error.
    h.task = "task v2"
    h.model.script = replaying_script
    result = h.run()
    dump = h.model.seen[0].hints.dump
    assert dump is not None and dump.outputs[0].calls[0].args == {"$invalid": ["a"]}
    assert result.answer == "done:" + first.answer and h.model.calls == 2


def test_result_values_are_normalized_json(coco_env: coco.Environment) -> None:
    h = Harness(coco_env)
    h.values["a"] = {"z": (1, 2), "y": "t"}

    def script(spec: SessionSpec, prefix: Transcript, hints: RunHints) -> ModelOutput:
        if not prefix.steps:
            return probe("a")
        return ModelOutput(answer=str(prefix.steps[0].results[0].value))

    h.model.script = script
    assert h.run().answer == "{'y': 't', 'z': [1, 2]}"
    assert (
        h.run().answer == "{'y': 't', 'z': [1, 2]}"
    )  # the stored transcript compares equal
    assert h.model.calls == 0

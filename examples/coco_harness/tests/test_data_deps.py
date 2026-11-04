"""Open sessions are memoized on what they read (DESIGN.md §7, data dependencies).

Each test counts model calls (inference) and tool executions (whether a
session's body ran at all): a skipped session executes no tool and calls no
model. A source read is recorded when it happens, so a session is skipped
while every file it read, and every observation it made, is unchanged; a
change to something it read re-runs its body, and only the changed read
diverges. A child that read something flags its parent (the parent's body
re-runs, the child is skipped on its own entry), and a session that absorbed
an environment failure re-runs until a run absorbs none.
"""

from __future__ import annotations

import os
import pathlib
from dataclasses import dataclass, field

import cocoindex as coco

from coco_harness import (
    HARNESS,
    MODEL_CLIENT,
    HarnessConfig,
    RoleSpec,
    SessionResult,
    run_open_session,
)
from coco_harness.hints import RunHints
from coco_harness.spec import SessionSpec
from coco_harness.testing import ScriptedModel
from coco_harness.tools import agents, sources, targets
from coco_harness.transcript import ModelOutput, ToolCall, Transcript
from helpers import CountingRegistry, explode_tool

ORCHESTRATOR = RoleSpec(
    "orchestrator",
    "test",
    ("open_source", "open_target", "call", "for_each", "call_agent", "explode"),
)
WORKER = RoleSpec("worker", "test", ("read_input",))
READER = RoleSpec("reader", "test", ("call",))
QUIET = RoleSpec("quiet", "test", ())


def call(tool: str, **args: object) -> ToolCall:
    return ToolCall(tool, dict(args))


def read(handle: str, key: str) -> ToolCall:
    return call("call", handle=handle, method="read", args={"key": key})


@dataclass
class Fixture:
    docs: pathlib.Path
    model: ScriptedModel
    tools: CountingRegistry
    app: coco.App[..., SessionResult]
    errors: dict[str, Exception] = field(default_factory=dict)

    def run(self) -> SessionResult:
        self.model.reset()
        self.tools.reset()
        result = self.app.update_blocking()
        assert isinstance(result, SessionResult)
        return result

    def executed(self) -> dict[str, int]:
        return dict(self.tools.executed)

    def write(self, name: str, text: str) -> None:
        path = self.docs / name
        path.write_text(text)
        _bump_mtime(path)

    def touch(self, name: str) -> None:
        _bump_mtime(self.docs / name)


def _bump_mtime(path: pathlib.Path) -> None:
    """Edits within one test are faster than some filesystems' mtime resolution."""
    st = os.stat(path)
    os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000))


def setup(
    coco_env: coco.Environment,
    tmp_path: pathlib.Path,
    script: ScriptedModel,
    *,
    files: int = 5,
) -> Fixture:
    docs = tmp_path / "docs"
    docs.mkdir()
    for i in range(files):
        (docs / f"{i}.md").write_text(f"doc {i}")
    errors: dict[str, Exception] = {}
    tools = CountingRegistry(
        [
            sources.open_source,
            sources.call_method,
            sources.read_input,
            targets.open_target,
            agents.for_each,
            agents.call_agent,
            explode_tool(errors),
        ]
    )
    config = HarnessConfig(
        roles={r.name: r for r in (ORCHESTRATOR, WORKER, READER, QUIET)}, tools=tools
    )
    coco_env.context_provider.provide(MODEL_CLIENT, script)
    coco_env.context_provider.provide(HARNESS, config)
    app = coco.App(
        coco.AppConfig(name="data_deps", environment=coco_env),
        run_open_session,
        config.spec("orchestrator", "read the docs"),
    )
    return Fixture(docs, script, tools, app, errors)


def open_docs(docs: pathlib.Path) -> ToolCall:
    return call(
        "open_source", kind="localfs", name="docs", path=str(docs), patterns=["*.md"]
    )


def test_a_session_is_skipped_while_what_it_read_is_unchanged(
    coco_env: coco.Environment, tmp_path: pathlib.Path
) -> None:
    def script(spec: SessionSpec, prefix: Transcript, hints: RunHints) -> ModelOutput:
        n = len(prefix.steps)
        if n == 0:
            return ModelOutput(calls=(open_docs(tmp_path / "docs"),))
        if n == 1:
            return ModelOutput(calls=(read("src:docs", "1.md"),))
        return ModelOutput(answer=str(prefix.steps[1].results[0].value))

    f = setup(coco_env, tmp_path, ScriptedModel(script))
    assert f.run().answer == "doc 1"
    assert f.model.calls == 3
    assert f.executed() == {"open_source": 1, "call": 1}

    # Unchanged: the session is skipped whole. No tool runs, no turn is probed.
    f.run()
    assert f.model.calls == 0 and f.executed() == {}

    # The file it read changed: the body re-runs, its tools execute again, the
    # two turns before the read hit, and the model sees exactly that diff.
    f.write("1.md", "doc 1 (edited)")
    assert f.run().answer == "doc 1 (edited)"
    assert f.executed() == {"open_source": 1, "call": 1}
    assert f.model.calls == 1
    (seen,) = f.model.seen
    assert seen.step == 2
    assert [(d.at_step, d.before, d.after) for d in seen.hints.diffs] == [
        (1, "doc 1", "doc 1 (edited)")
    ]

    # A file it did not read changed, inside the same widened count: skipped.
    f.write("3.md", "doc 3 (edited)")
    f.run()
    assert f.model.calls == 0 and f.executed() == {}

    # Touched, not changed: the read's mtime moved, its digest did not. Skipped,
    # and the record is refreshed so the next probe takes the mtime fast path.
    f.touch("1.md")
    f.run()
    assert f.model.calls == 0 and f.executed() == {}
    f.run()
    assert f.model.calls == 0 and f.executed() == {}

    # Enough new files to widen the count: the open_source observation differs,
    # so the body re-runs and the card diverges at step 0.
    for i in range(5, 9):
        f.write(f"{i}.md", f"doc {i}")
    f.run()
    assert f.executed() == {"open_source": 1, "call": 1}
    assert f.model.calls == 2
    first = f.model.seen[0]
    assert first.step == 1 and [d.at_step for d in first.hints.diffs] == [0]


def test_a_missing_file_is_a_recorded_read_too(
    coco_env: coco.Environment, tmp_path: pathlib.Path
) -> None:
    def script(spec: SessionSpec, prefix: Transcript, hints: RunHints) -> ModelOutput:
        n = len(prefix.steps)
        if n == 0:
            return ModelOutput(calls=(open_docs(tmp_path / "docs"),))
        if n == 1:
            return ModelOutput(calls=(read("src:docs", "9.md"),))
        r = prefix.steps[1].results[0]
        return ModelOutput(answer=f"{r.ok}:{r.value}")

    f = setup(coco_env, tmp_path, ScriptedModel(script))
    assert f.run().answer.startswith("False:no file '9.md'")
    f.run()
    assert f.model.calls == 0 and f.executed() == {}

    # The file appears: the failed read is a read whose state changed.
    f.write("9.md", "doc 9")
    assert f.run().answer == "True:doc 9"
    assert f.model.calls == 1


def test_a_file_edit_under_for_each_reruns_its_worker_and_the_fan_out_body_only(
    coco_env: coco.Environment, tmp_path: pathlib.Path
) -> None:
    def script(spec: SessionSpec, prefix: Transcript, hints: RunHints) -> ModelOutput:
        n = len(prefix.steps)
        if spec.role.name == "worker":
            if n == 0:
                return ModelOutput(calls=(call("read_input"),))
            return ModelOutput(answer="sum:" + prefix.steps[0].results[0].value["text"])
        if n == 0:
            return ModelOutput(
                calls=(
                    open_docs(tmp_path / "docs"),
                    call(
                        "open_target",
                        kind="localfs_dir",
                        name="out",
                        path=str(tmp_path / "out"),
                    ),
                )
            )
        if n == 1:
            return ModelOutput(
                calls=(call("for_each", source="src:docs", key="notes", task="sum"),)
            )
        return ModelOutput(answer=str(prefix.steps[1].results[0].value))

    f = setup(coco_env, tmp_path, ScriptedModel(script))
    first = f.run()
    assert "'items': '4–8'" in first.answer
    assert f.model.calls_for("orchestrator") == 3
    assert f.model.calls_for("worker") == 10

    # Unchanged: the orchestrator is skipped whole, workers included.
    f.run()
    assert f.model.calls == 0 and f.executed() == {}

    # One file edited: the walk the orchestrator recorded differs, so its body
    # re-runs — tools execute, turns hit, no inference — and the workers are
    # probed: the edited one re-runs its content turn, the others hit.
    f.write("2.md", "doc 2 (edited)")
    f.run()
    assert f.model.calls_for("worker") == 1
    assert f.model.calls_for("orchestrator") == 0
    assert f.executed() == {
        "open_source": 1,
        "open_target": 1,
        "for_each": 1,
        "read_input": 1,
    }

    # Touched only: the walk differs (mtime), the worker hits on its file's
    # fingerprint. Still no inference anywhere.
    f.touch("3.md")
    f.run()
    assert f.model.calls == 0
    assert f.executed() == {"open_source": 1, "open_target": 1, "for_each": 1}

    # Enough deletions to narrow the count: the source card and the collection
    # card both change, so the orchestrator decides its last two turns again.
    (f.docs / "0.md").unlink()
    (f.docs / "1.md").unlink()
    assert "'items': '2–4'" in f.run().answer
    assert f.model.calls_for("orchestrator") == 2
    assert f.model.calls_for("worker") == 0


def _delegating_script(docs: pathlib.Path, role: str) -> ScriptedModel:
    """An orchestrator that opens the docs and calls one child of ``role`` with them."""

    def script(spec: SessionSpec, prefix: Transcript, hints: RunHints) -> ModelOutput:
        n = len(prefix.steps)
        if spec.role.name == "reader":
            src = next(c.handle for c in spec.inputs if c.kind == "localfs")
            if n == 0:
                return ModelOutput(calls=(read(src, "1.md"),))
            return ModelOutput(answer=str(prefix.steps[0].results[0].value))
        if spec.role.name == "quiet":
            return ModelOutput(answer="nothing read")
        if n == 0:
            return ModelOutput(calls=(open_docs(docs),))
        if n == 1:
            return ModelOutput(
                calls=(
                    call(
                        "call_agent",
                        key="child",
                        task="look",
                        role=role,
                        inputs=["src:docs"],
                    ),
                )
            )
        return ModelOutput(answer=str(prefix.steps[1].results[0].value))

    return ScriptedModel(script)


def test_a_child_that_read_flags_its_parent_while_being_skipped_itself(
    coco_env: coco.Environment, tmp_path: pathlib.Path
) -> None:
    f = setup(coco_env, tmp_path, _delegating_script(tmp_path / "docs", "reader"))
    f.run()
    assert f.model.calls_for("reader") == 2

    # The child recorded a read; the parent did not record it and cannot tell,
    # so the parent's body re-runs (its tools execute, its turns hit) while the
    # child is skipped on its own entry: its `call` never runs.
    f.run()
    assert f.model.calls == 0
    assert f.executed() == {"open_source": 1, "call_agent": 1}

    # The child's read changed: the child re-runs; the parent sees the same
    # result card (a handle and a widened size) and still needs no inference.
    f.write("1.md", "doc 1*")
    f.run()
    assert f.model.calls_for("reader") == 1
    assert f.model.calls_for("orchestrator") == 0


def test_a_child_that_read_nothing_lets_its_parent_skip(
    coco_env: coco.Environment, tmp_path: pathlib.Path
) -> None:
    """A session that records nothing is closed in effect, whatever its entry point."""
    f = setup(coco_env, tmp_path, _delegating_script(tmp_path / "docs", "quiet"))
    f.run()
    assert f.model.calls_for("quiet") == 1
    f.run()
    assert f.model.calls == 0 and f.executed() == {}


def test_a_contained_failure_reruns_the_session_until_a_run_absorbs_none(
    coco_env: coco.Environment, tmp_path: pathlib.Path
) -> None:
    """``explode`` records nothing: only the contained failure keeps the session running."""

    def script(spec: SessionSpec, prefix: Transcript, hints: RunHints) -> ModelOutput:
        if not prefix.steps:
            return ModelOutput(calls=(call("explode", key="k"),))
        r = prefix.steps[0].results[0]
        return ModelOutput(answer=f"{r.ok}:{r.value}")

    f = setup(coco_env, tmp_path, ScriptedModel(script))
    f.errors["k"] = RuntimeError("disk on fire")
    assert f.run().answer == "False:RuntimeError: disk on fire"

    # Nothing changed, yet the tool runs again: a failure is never a fact worth
    # keeping. The turns hit, so this costs no inference.
    f.run()
    assert f.executed() == {"explode": 1} and f.model.calls == 0

    # Fixed: the result differs, the model is consulted, and the healed
    # session is closed in effect — the next run skips it.
    f.errors.clear()
    assert f.run().answer == "True:fine"
    assert f.model.calls == 1
    f.run()
    assert f.model.calls == 0 and f.executed() == {}

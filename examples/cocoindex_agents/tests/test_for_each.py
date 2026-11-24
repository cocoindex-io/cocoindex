"""for_each over a directory: one closed worker per file, inference only where files changed.

Also the rules around it: a target passed to workers through `inputs` and found
by the worker through its input cards; a source refused as a closed-session
input; a role with open tools refused for closed workers; a tool description
edit invalidating closed workers; two calls naming one handle in a single turn;
a failed worker reported to the parent and healing on the next run.
"""

from __future__ import annotations

import pathlib
from dataclasses import dataclass, field, replace

import cocoindex as coco

from cocoindex_agents import (
    AGENTS_CONFIG,
    MODEL_CLIENT,
    AgentsConfig,
    RoleSpec,
    SessionResult,
    ToolRegistry,
    run_open_session,
)
from cocoindex_agents.hints import RunHints
from cocoindex_agents.spec import SessionSpec
from cocoindex_agents.testing import ScriptedModel
from cocoindex_agents.tools import agents, sources, targets
from cocoindex_agents.transcript import ModelOutput, ToolCall, Transcript
from helpers import explode_tool

ORCHESTRATOR = RoleSpec(
    "orchestrator",
    "test",
    ("open_source", "open_target", "for_each", "declare", "call_agent"),
)
WORKER = RoleSpec("worker", "test", ("read_input", "declare"))
FRAGILE = RoleSpec("fragile", "test", ("read_input", "explode"))
OPEN_WORKER = RoleSpec("open_worker", "test", ("read_input", "open_source"))
HELPER = RoleSpec("helper", "test", ())


def call(tool: str, **args: object) -> ToolCall:
    return ToolCall(tool, dict(args))


@dataclass
class Scenario:
    docs: pathlib.Path
    out: pathlib.Path
    worker_inputs: list[str]
    worker_role: str = "worker"
    source_as_input: bool = False
    duplicate_names: bool = False
    alias_keys: bool = False
    collect: bool = True

    def script(
        self, spec: SessionSpec, prefix: Transcript, hints: RunHints
    ) -> ModelOutput:
        n = len(prefix.steps)
        if spec.role.name == "helper":
            return ModelOutput(answer="helper")
        if spec.role.name == "orchestrator" and self.alias_keys and n >= 1:
            # for_each under key "agent" mounts a worker per item ("0.md", …);
            # call_agent under key "0.md" must not share an engine path with it.
            if n == 1:
                return ModelOutput(
                    calls=(
                        call(
                            "for_each",
                            source="src:docs",
                            key="agent",
                            task="map",
                            role="worker",
                        ),
                    )
                )
            if n == 2:
                return ModelOutput(
                    calls=(call("call_agent", key="0.md", task="child", role="helper"),)
                )
            return ModelOutput(answer=str([s.results[0].ok for s in prefix.steps[1:3]]))
        if spec.role.name == "orchestrator":
            if n == 0:
                open_docs = call(
                    "open_source",
                    kind="localfs",
                    name="docs",
                    path=str(self.docs),
                    patterns=["*.md"],
                )
                if self.duplicate_names:
                    # Two calls in one turn registering src:docs: both must fail,
                    # whichever ran first.
                    return ModelOutput(calls=(open_docs, open_docs))
                return ModelOutput(
                    calls=(
                        open_docs,
                        call(
                            "open_target",
                            kind="localfs_dir",
                            name="out",
                            path=str(self.out),
                        ),
                    )
                )
            if n == 1 and self.duplicate_names:
                return ModelOutput(answer=str([r.ok for r in prefix.steps[0].results]))
            if n == 1:
                inputs = ["src:docs"] if self.source_as_input else self.worker_inputs
                return ModelOutput(
                    calls=(
                        call(
                            "for_each",
                            source="src:docs",
                            key="notes",
                            task="summarize",
                            role=self.worker_role,
                            inputs=inputs,
                            collect=self.collect,
                        ),
                    )
                )
            return ModelOutput(answer=str(prefix.steps[1].results[0].value))
        if spec.role.name == "fragile":
            # read, then poke the explode tool with the item's key, then answer
            if n == 0:
                return ModelOutput(calls=(call("read_input"),))
            if n == 1:
                return ModelOutput(
                    calls=(
                        call("explode", key=prefix.steps[0].results[0].value["key"]),
                    )
                )
            return ModelOutput(answer="sum:" + prefix.steps[0].results[0].value["text"])
        # worker: read, then (if a target was supplied) declare, then answer
        if n == 0:
            return ModelOutput(calls=(call("read_input"),))
        item = prefix.steps[0].results[0].value
        targets_given = [c.handle for c in spec.inputs if c.kind == "localfs_dir"]
        if n == 1 and targets_given:
            # The worker learns the target's name from its input cards, not from the test.
            return ModelOutput(
                calls=(
                    call(
                        "declare",
                        target=targets_given[0],
                        key=item["key"] + ".txt",
                        value="sum:" + item["text"],
                    ),
                )
            )
        return ModelOutput(answer="sum:" + item["text"])


@dataclass
class Fixture:
    model: ScriptedModel
    app: coco.App[..., SessionResult]
    scenario: Scenario
    config: AgentsConfig
    errors: dict[str, Exception] = field(default_factory=dict)

    def run(self) -> SessionResult:
        self.model.reset()
        result = self.app.update_blocking()
        assert isinstance(result, SessionResult)
        return result


def setup(coco_env: coco.Environment, tmp_path: pathlib.Path, **kw: object) -> Fixture:
    docs = tmp_path / "docs"
    docs.mkdir()
    for i in range(5):
        (docs / f"{i}.md").write_text(f"doc {i}")
    scenario = Scenario(docs, tmp_path / "out", worker_inputs=[], **kw)  # type: ignore[arg-type]
    model = ScriptedModel(scenario.script)
    errors: dict[str, Exception] = {}
    config = AgentsConfig(
        roles={r.name: r for r in (ORCHESTRATOR, WORKER, FRAGILE, OPEN_WORKER, HELPER)},
        tools=ToolRegistry(
            [
                sources.open_source,
                sources.read_input,
                targets.open_target,
                targets.declare,
                agents.for_each,
                agents.call_agent,
                explode_tool(errors),
            ]
        ),
    )
    coco_env.context_provider.provide(MODEL_CLIENT, model)
    coco_env.context_provider.provide(AGENTS_CONFIG, config)
    app = coco.App(
        coco.AppConfig(name="for_each", environment=coco_env),
        run_open_session,
        config.spec("orchestrator", "summarize the docs"),
    )
    return Fixture(model, app, scenario, config, errors)


def test_edit_one_file_reruns_one_workers_content_turn_only(
    coco_env: coco.Environment, tmp_path: pathlib.Path
) -> None:
    f = setup(coco_env, tmp_path)
    docs = f.scenario.docs

    first = f.run()
    assert first.num_steps == 3 and "coll:notes" in first.answer
    assert f.model.calls_for("orchestrator") == 3
    assert f.model.calls_for("worker") == 10  # 5 workers × (read_input, finish)

    f.run()
    assert f.model.calls == 0

    # Edit one file: that worker's component misses on the file's fingerprint and
    # re-runs. Its read_input turn still hits (empty prefix); only the turn that
    # saw the new content needs the model. The orchestrator replays entirely: the
    # for_each card shows a widened count, not the contents.
    (docs / "2.md").write_text("doc 2 (edited)")
    f.run()
    assert f.model.calls_for("worker") == 1
    assert f.model.calls_for("orchestrator") == 0

    # A new file: a new worker, two turns. Still inside the 4–8 bucket.
    (docs / "5.md").write_text("doc 5")
    f.run()
    assert f.model.calls_for("worker") == 2
    assert f.model.calls_for("orchestrator") == 0

    # Deleting a file costs nothing; its component is cleaned up.
    (docs / "0.md").unlink()
    f.run()
    assert f.model.calls == 0


def test_a_tool_description_edit_invalidates_closed_workers(
    coco_env: coco.Environment, tmp_path: pathlib.Path
) -> None:
    f = setup(coco_env, tmp_path)
    f.run()
    f.run()
    assert f.model.calls == 0

    edited = replace(
        sources.read_input, spec=replace(sources.read_input.spec, description="v2")
    )
    # A provided config is immutable: the edit is a new config, provided anew.
    f.config = replace(
        f.config,
        tools=ToolRegistry(
            [
                agents.call_agent,
                sources.open_source,
                edited,
                targets.open_target,
                targets.declare,
                agents.for_each,
                explode_tool(f.errors),
            ]
        ),
    )
    coco_env.context_provider.provide(AGENTS_CONFIG, f.config)
    f.run()
    # The config changed, so every session re-ran its body. The workers' spec
    # changed with it (their tool specs are in it), so every worker turn
    # re-ran; the orchestrator does not have read_input, so its turns hit.
    assert f.model.calls_for("worker") == 10
    assert f.model.calls_for("orchestrator") == 0


def test_workers_write_to_the_parents_target_named_by_their_input_cards(
    coco_env: coco.Environment, tmp_path: pathlib.Path
) -> None:
    f = setup(coco_env, tmp_path)
    f.scenario.worker_inputs = ["tgt:out"]

    result = f.run()
    assert "coll:notes" in result.answer
    assert f.model.calls_for("worker") == 15  # read_input, declare, finish
    written = sorted(p.name for p in f.scenario.out.iterdir())
    assert written == [f"{i}.md.txt" for i in range(5)]
    assert (f.scenario.out / "2.md.txt").read_text() == "sum:doc 2"

    # Unchanged: nothing runs, files stay.
    f.run()
    assert f.model.calls == 0

    # A deleted source file removes the worker and, with it, the file it declared.
    (f.scenario.docs / "4.md").unlink()
    f.run()
    assert not (f.scenario.out / "4.md.txt").exists()


def test_for_each_without_collect_mounts_workers_in_the_background(
    coco_env: coco.Environment, tmp_path: pathlib.Path
) -> None:
    f = setup(coco_env, tmp_path, collect=False)
    f.scenario.worker_inputs = ["tgt:out"]

    result = f.run()
    assert result.answer == "{'mounted': 'notes'}"
    assert f.model.calls_for("worker") == 15  # read_input, declare, finish
    assert sorted(p.name for p in f.scenario.out.iterdir()) == [
        f"{i}.md.txt" for i in range(5)
    ]

    f.run()
    assert f.model.calls == 0

    (f.scenario.docs / "3.md").unlink()
    f.run()
    assert not (f.scenario.out / "3.md.txt").exists()


def test_a_failed_worker_is_reported_to_the_parent_and_heals_next_run(
    coco_env: coco.Environment, tmp_path: pathlib.Path
) -> None:
    """A closed worker that hits an environment error commits nothing; the parent sees it as data."""
    f = setup(coco_env, tmp_path, worker_role="fragile")
    f.errors["2.md"] = RuntimeError("disk on fire")

    first = f.run()
    assert "'failed': 1" in first.answer and "'items': '4–8'" in first.answer
    # Four workers finished (3 turns each); the failing one ran read_input and
    # explode, then died before its answer turn.
    assert f.model.calls_for("fragile") == 4 * 3 + 2
    assert f.model.calls_for("orchestrator") == 3

    # Nothing changed: the failed worker runs again (its two turns hit), fails
    # again, and the orchestrator replays — zero inference.
    f.run()
    assert f.model.calls == 0

    # The cause is fixed: the worker's explode turn hits, the tool succeeds, only
    # its answer turn needs the model; the collection changes, so the orchestrator
    # re-decides its final turn.
    f.errors.clear()
    healed = f.run()
    assert "'failed': 0" in healed.answer
    assert f.model.calls_for("fragile") == 1
    assert f.model.calls_for("orchestrator") == 1


def test_a_source_cannot_be_an_input_of_closed_workers(
    coco_env: coco.Environment, tmp_path: pathlib.Path
) -> None:
    f = setup(coco_env, tmp_path, source_as_input=True)
    result = f.run()
    assert "cannot be an input of a closed session" in result.answer
    assert f.model.calls_for("worker") == 0


def test_a_role_with_open_tools_cannot_run_closed_workers(
    coco_env: coco.Environment, tmp_path: pathlib.Path
) -> None:
    f = setup(coco_env, tmp_path, worker_role="open_worker")
    result = f.run()
    assert "open_source" in result.answer and "closed session" in result.answer
    assert f.model.calls_for("open_worker") == 0


def test_distinct_child_keys_never_alias_one_engine_path(
    coco_env: coco.Environment, tmp_path: pathlib.Path
) -> None:
    f = setup(coco_env, tmp_path, alias_keys=True)
    assert f.run().answer == "[True, True]"
    assert f.model.calls_for("helper") == 1 and f.model.calls_for("worker") == 10


def test_two_calls_registering_one_name_in_a_turn_both_fail(
    coco_env: coco.Environment, tmp_path: pathlib.Path
) -> None:
    f = setup(coco_env, tmp_path, duplicate_names=True)
    assert f.run().answer == "[False, False]"

"""A PDF reaches the model natively, by reference in the transcript (DESIGN.md §3, §4).

Decided by media type, not by flow: a worker's ``read_input`` returns
``{key, text}`` for a text file and a reference ``{key, media_type, bytes,
digest}`` for a PDF, and the rendered request carries the document right
after that result; the transcript and the turn memos hold the reference
only. An unchanged rerun makes no model call; a changed PDF changes the
digest, so exactly the turns that saw it run again, in that file's worker
alone. An open session's ``sample`` shows a document's media type and size
in place of a text head, and ``read`` attaches the document the same way.
"""

from __future__ import annotations

import base64
import os
import pathlib
from dataclasses import dataclass, field

import cocoindex as coco
from cocoindex.connectorkits.fingerprint import fingerprint_bytes

from cocoindex_agents import (
    AGENTS_CONFIG,
    MODEL_CLIENT,
    AgentsConfig,
    RoleSpec,
    SessionResult,
    run_open_session,
)
from cocoindex_agents.hints import RunHints
from cocoindex_agents.litellm_client import Message, render_messages
from cocoindex_agents.spec import SessionSpec
from cocoindex_agents.testing import CountingRegistry, ScriptedModel
from cocoindex_agents.tools import agents, sources, targets
from cocoindex_agents.transcript import (
    ModelOutput,
    ToolCall,
    Transcript,
    canonical_json,
)

ORCHESTRATOR = RoleSpec(
    "orchestrator", "test", ("open_source", "open_target", "for_each", "call_agent")
)
WORKER = RoleSpec("worker", "test", ("read_input", "declare"))
READER = RoleSpec("reader", "test", ("call",))

# The tests cover the plumbing, so the bytes need only look like a PDF; the
# sample flow under flows/orders drives a real model with real ones.
PDF_A = b"%PDF-1.4\n% order A: three items, approved\n%%EOF\n"
PDF_B = b"%PDF-1.4\n% order B: one item, pending\n%%EOF\n"


def call(tool: str, **args: object) -> ToolCall:
    return ToolCall(tool, dict(args))


def reference(key: str, data: bytes) -> dict[str, object]:
    return {
        "bytes": len(data),
        "digest": fingerprint_bytes(data).hex(),
        "key": key,
        "media_type": "application/pdf",
    }


def file_part(key: str, data: bytes) -> dict[str, object]:
    return {
        "type": "file",
        "file": {
            "filename": key,
            "file_data": "data:application/pdf;base64,"
            + base64.b64encode(data).decode("ascii"),
        },
    }


@dataclass
class Scenario:
    """Shape A over a folder of two PDFs and a text file; or one reader over the source."""

    docs: pathlib.Path
    out: pathlib.Path
    reader: bool = False
    # What the model saw: the request rendered for each session's last content
    # turn, and the canonical results of each worker's transcript, by key.
    rendered: dict[str, list[Message]] = field(default_factory=dict)
    transcripts: dict[str, str] = field(default_factory=dict)

    def script(
        self, spec: SessionSpec, prefix: Transcript, hints: RunHints
    ) -> ModelOutput:
        n = len(prefix.steps)
        if spec.role.name == "orchestrator":
            if n == 0:
                return ModelOutput(
                    calls=(
                        call(
                            "open_source",
                            kind="localfs",
                            name="docs",
                            path=str(self.docs),
                        ),
                        call(
                            "open_target",
                            kind="localfs_dir",
                            name="out",
                            path=str(self.out),
                        ),
                    )
                )
            if n == 1 and self.reader:
                return ModelOutput(
                    calls=(
                        call(
                            "call_agent",
                            key="reader",
                            task="look",
                            role="reader",
                            inputs=["src:docs"],
                        ),
                    )
                )
            if n == 1:
                return ModelOutput(
                    calls=(
                        call(
                            "for_each",
                            source="src:docs",
                            key="items",
                            task="extract",
                            inputs=["tgt:out"],
                        ),
                    )
                )
            return ModelOutput(answer=canonical_json(prefix.steps[1].results[0].value))
        if spec.role.name == "reader":
            if n == 0:
                return ModelOutput(
                    calls=(
                        call("call", handle="src:docs", method="sample", args={"k": 5}),
                    )
                )
            if n == 1:
                return ModelOutput(
                    calls=(
                        call(
                            "call",
                            handle="src:docs",
                            method="read",
                            args={"key": "a.pdf"},
                        ),
                    )
                )
            self.rendered["reader"] = render_messages(spec, prefix, hints)
            self.transcripts["reader"] = canonical_json(
                [[r.value for r in s.results] for s in prefix.steps]
            )
            return ModelOutput(answer="looked")
        # worker: read, declare the result it got, answer
        if n == 0:
            return ModelOutput(calls=(call("read_input"),))
        item = prefix.steps[0].results[0].value
        if n == 1:
            self.rendered[item["key"]] = render_messages(spec, prefix, hints)
            return ModelOutput(
                calls=(
                    call(
                        "declare",
                        target="tgt:out",
                        key=item["key"] + ".json",
                        value=canonical_json(item),
                    ),
                )
            )
        self.transcripts[item["key"]] = canonical_json(
            [[r.value for r in s.results] for s in prefix.steps]
        )
        return ModelOutput(answer=item["key"])


@dataclass
class Fixture:
    scenario: Scenario
    model: ScriptedModel
    tools: CountingRegistry
    app: coco.App[..., SessionResult]

    def run(self) -> SessionResult:
        self.model.reset()
        self.tools.reset()
        result = self.app.update_blocking()
        assert isinstance(result, SessionResult)
        return result

    def write(self, name: str, data: bytes) -> None:
        path = self.scenario.docs / name
        path.write_bytes(data)
        st = os.stat(path)
        os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000))

    def output(self, name: str) -> str:
        return (self.scenario.out / name).read_text()


def setup(
    coco_env: coco.Environment, tmp_path: pathlib.Path, *, reader: bool = False
) -> Fixture:
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "a.pdf").write_bytes(PDF_A)
    (docs / "b.pdf").write_bytes(PDF_B)
    (docs / "c.md").write_text("doc c")
    scenario = Scenario(docs, tmp_path / "out", reader=reader)
    model = ScriptedModel(scenario.script)
    tools = CountingRegistry(
        [
            sources.open_source,
            sources.call_method,
            sources.read_input,
            targets.open_target,
            targets.declare,
            agents.for_each,
            agents.call_agent,
        ]
    )
    config = AgentsConfig(
        roles={r.name: r for r in (ORCHESTRATOR, WORKER, READER)}, tools=tools
    )
    coco_env.context_provider.provide(MODEL_CLIENT, model)
    coco_env.context_provider.provide(AGENTS_CONFIG, config)
    app = coco.App(
        coco.AppConfig(name="documents", environment=coco_env),
        run_open_session,
        config.spec("orchestrator", "extract the orders"),
    )
    return Fixture(scenario, model, tools, app)


def test_a_pdf_reaches_the_model_by_reference_and_the_transcript_holds_no_bytes(
    coco_env: coco.Environment, tmp_path: pathlib.Path
) -> None:
    f = setup(coco_env, tmp_path)
    first = f.run()
    assert '"handle":"coll:items"' in first.answer and '"failed":0' in first.answer
    assert f.model.calls_for("worker") == 9  # 3 files × (read_input, declare, answer)

    # read_input decides by media type: a reference for the PDFs, text for the .md.
    ref_a = reference("a.pdf", PDF_A)
    assert f.output("a.pdf.json") == canonical_json(ref_a)
    assert f.output("b.pdf.json") == canonical_json(reference("b.pdf", PDF_B))
    assert f.output("c.md.json") == canonical_json({"key": "c.md", "text": "doc c"})

    # The request the declare turn saw: the tool message carries the reference
    # only, and the document follows it as a user message with a file part.
    messages = f.scenario.rendered["a.pdf"]
    assert [m["role"] for m in messages] == [
        "system",
        "user",
        "assistant",
        "tool",
        "user",
    ]
    assert messages[3]["content"] == canonical_json({"ok": True, "value": ref_a})
    text, part = messages[4]["content"]
    assert text == {
        "type": "text",
        "text": f"The document a.pdf (application/pdf, {len(PDF_A)} bytes) "
        "that the read_input result above refers to:",
    }
    assert part == file_part("a.pdf", PDF_A)
    # A text file renders as before: no attachment.
    assert [m["role"] for m in f.scenario.rendered["c.md"]] == [
        "system",
        "user",
        "assistant",
        "tool",
    ]
    # What is memoized and stored is the transcript: results only, no bytes.
    encoded = base64.b64encode(PDF_A).decode("ascii")
    assert encoded not in f.scenario.transcripts["a.pdf"]
    assert f.scenario.transcripts["a.pdf"] == canonical_json(
        [[ref_a], [{"declared": "a.pdf.json"}]]
    )

    # Unchanged: nothing runs.
    f.run()
    assert f.model.calls == 0 and f.tools.executed == {}

    # One PDF replaced: its reference changes, so that worker's turns after
    # read_input re-infer; the other workers are skipped and the orchestrator
    # replays its body with no inference.
    f.scenario.rendered.clear()
    new_a = PDF_A.replace(b"approved", b"rejected")
    f.write("a.pdf", new_a)
    f.run()
    assert f.model.calls_for("worker") == 2 and f.model.calls_for("orchestrator") == 0
    assert f.tools.executed["read_input"] == 1 and list(f.scenario.rendered) == [
        "a.pdf"
    ]
    assert f.scenario.rendered["a.pdf"][4]["content"][1] == file_part("a.pdf", new_a)
    assert f.output("a.pdf.json") == canonical_json(reference("a.pdf", new_a))
    assert f.output("b.pdf.json") == canonical_json(reference("b.pdf", PDF_B))


def test_an_open_session_samples_documents_by_size_and_reads_them_attached(
    coco_env: coco.Environment, tmp_path: pathlib.Path
) -> None:
    f = setup(coco_env, tmp_path, reader=True)
    first = f.run()
    assert '"result":"res:reader"' in first.answer
    assert f.model.calls_for("reader") == 3
    sample, read = f.scenario.transcripts["reader"], f.scenario.rendered["reader"]
    # A sample shows a document's media type and size, a text file's head.
    assert (
        canonical_json(
            {"bytes": len(PDF_A), "key": "a.pdf", "media_type": "application/pdf"}
        )
        in sample
    )
    assert (
        canonical_json(
            {"bytes": len(PDF_B), "key": "b.pdf", "media_type": "application/pdf"}
        )
        in sample
    )
    assert canonical_json({"head": "doc c", "key": "c.md"}) in sample
    assert canonical_json(reference("a.pdf", PDF_A)) in sample
    # The sample step attaches nothing; the read step attaches the document.
    assert [m["role"] for m in read] == [
        "system",
        "user",
        "assistant",
        "tool",
        "assistant",
        "tool",
        "user",
    ]
    assert read[6]["content"][1] == file_part("a.pdf", PDF_A)

    # Unchanged: the reader's recorded reads validate, so it and its parent are skipped.
    f.run()
    assert f.model.calls == 0 and f.tools.executed == {}

    # The PDF changes without changing size: the sample is the same
    # observation, the read is not. The reader re-runs its body; its sample
    # turn hits and only the turn after the read re-infers. The orchestrator
    # sees the same res: handle and does not infer.
    f.write("a.pdf", PDF_A.replace(b"approved", b"rejected"))
    f.run()
    assert f.model.calls_for("reader") == 1 and f.model.calls_for("orchestrator") == 0
    assert f.tools.executed == {
        "open_source": 1,
        "open_target": 1,
        "call_agent": 1,
        "call": 2,
    }

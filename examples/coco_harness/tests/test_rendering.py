"""What the LiteLLM client renders: task + inputs, the dump, diffs, notes, invalid calls."""

from __future__ import annotations

from coco_harness import HarnessConfig, RoleSpec
from coco_harness.hints import Diff, Dump, Note, RunHints
from coco_harness.litellm_client import (
    has_literal_payload,
    render_dump,
    render_messages,
)
from coco_harness.spec import InputCard
from coco_harness.transcript import (
    INVALID_ARGS_KEY,
    ModelOutput,
    Step,
    ToolCall,
    ToolResult,
    Transcript,
    canonical_output,
)

ROLE = RoleSpec("worker", "be brief", ())


def _spec() -> HarnessConfig:
    return HarnessConfig(roles={ROLE.name: ROLE}, default_model="m")


def test_task_message_lists_input_cards() -> None:
    spec = _spec().spec(
        "worker",
        "do it",
        inputs=(InputCard("tgt:out", "localfs_dir", ("declare_file",)),),
    )
    messages = render_messages(spec, Transcript(), RunHints())
    assert messages[0] == {"role": "system", "content": "be brief"}
    assert messages[1]["role"] == "user"
    assert "do it" in messages[1]["content"]
    assert "tgt:out  (localfs_dir): declare_file" in messages[1]["content"]


def test_dump_marks_literal_payloads_and_renders_canonical_invalid_calls() -> None:
    invalid = canonical_output(ModelOutput(calls=(ToolCall("declare", ["x"]),)))  # type: ignore[arg-type]
    assert invalid.calls[0].args == {INVALID_ARGS_KEY: ["x"]}
    literal = ModelOutput(
        calls=(ToolCall("declare", {"target": "tgt:out", "key": "a", "value": "text"}),)
    )
    by_handle = ModelOutput(
        calls=(
            ToolCall("declare", {"target": "tgt:out", "key": "a", "value": "res:r"}),
        )
    )
    assert has_literal_payload(literal) and not has_literal_payload(by_handle)
    assert has_literal_payload(ModelOutput(answer="done"))

    hints = RunHints(
        dump=Dump(
            at_step=1,
            first_step=4,
            outputs=(invalid, literal, by_handle, ModelOutput(answer="done")),
        )
    )
    text = render_dump(hints)
    assert "[4] declare(" in text and '"$invalid":["x"]' in text
    lines = text.splitlines()
    # Marked: the invalid declare (its arguments are not a handle), the literal
    # declare, and the final answer. Not marked: the declare by handle.
    marked = [line[:5] for line in lines if "literal content" in line]
    assert marked == ["  [4]", "  [5]", "  [7]"]


def test_parse_response_keeps_malformed_arguments_as_the_models_error() -> None:
    from coco_harness.litellm_client import parse_response

    assert parse_response("done", []) == ModelOutput(answer="done")
    assert parse_response(None, []) == ModelOutput(answer="")
    good = parse_response(None, [("probe", '{"name": "a"}'), ("read_input", None)])
    assert good.calls == (ToolCall("probe", {"name": "a"}), ToolCall("read_input", {}))
    # Invalid JSON is not a crash: the raw text survives and becomes a canonical
    # invalid call that the registry reports back to the model.
    bad = canonical_output(parse_response(None, [("probe", "{not json")]))
    assert bad.calls == (ToolCall("probe", {INVALID_ARGS_KEY: "{not json"}),)


def test_dump_diffs_and_notes_render_at_their_positions() -> None:
    spec = _spec().spec("worker", "t")
    probe_a = ModelOutput(calls=(ToolCall("probe", {"name": "a"}),))
    prefix = Transcript((Step(probe_a, (ToolResult("probe", True, "A2"),)),))
    hints = RunHints(
        dump=Dump(at_step=1, first_step=1, outputs=(ModelOutput(answer="done:A"),)),
        diffs=(Diff(0, probe_a.calls[0], "A", "A2"),),
        notes=(
            Note(
                1, "fast-forward reached last run's final answer [1]; confirm or revise"
            ),
        ),
    )
    contents = [m["content"] for m in render_messages(spec, prefix, hints)]
    assert contents[2] == '{"calls":[{"args":{"name":"a"},"tool":"probe"}]}'
    assert contents[3] == '[{"ok":true,"tool":"probe","value":"A2"}]'
    assert (
        contents[4].startswith("[replay] result differs")
        and 'before: "A"' in contents[4]
    )
    assert contents[5].startswith("[replay] Last run continued")
    assert (
        contents[6]
        == "[replay] fast-forward reached last run's final answer [1]; confirm or revise"
    )
    assert len(contents) == 7

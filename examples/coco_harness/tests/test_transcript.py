from __future__ import annotations

from coco_harness.hints import Dump
from coco_harness.replay import find_same_action, resolve_replay
from coco_harness.transcript import (
    ModelOutput,
    Step,
    ToolCall,
    ToolResult,
    Transcript,
    canonical_json,
    normalize_json,
    same_action,
)


def test_canonical_json_is_order_independent_and_compact() -> None:
    assert canonical_json({"b": 1, "a": [1, 2]}) == '{"a":[1,2],"b":1}'
    assert canonical_json({"a": 1, "b": 2}) == canonical_json({"b": 2, "a": 1})


def test_normalize_json_matches_a_storage_round_trip() -> None:
    assert normalize_json({"k": (1, 2)}) == {"k": [1, 2]}
    assert normalize_json("text") == "text"


def test_canonical_output_keeps_the_transcript_dict_typed() -> None:
    from coco_harness.transcript import INVALID_ARGS_KEY, canonical_output

    raw = ModelOutput(
        calls=(ToolCall("probe", ["a"]), ToolCall("probe", {"name": "b"}))
    )  # type: ignore[arg-type]
    out = canonical_output(raw)
    assert out.calls[0].args == {INVALID_ARGS_KEY: ["a"]}
    assert out.calls[1] is raw.calls[1]
    fine = ModelOutput(calls=(ToolCall("probe", {"name": "b"}),))
    assert canonical_output(fine) is fine


def test_validate_args_reports_canonical_invalid_arguments() -> None:
    import pytest

    from coco_harness import ToolError, ToolSpec
    from coco_harness.tools import validate_args
    from coco_harness.transcript import INVALID_ARGS_KEY

    spec = ToolSpec(
        "probe", "", {"type": "object", "properties": {"name": {"type": "string"}}}
    )
    with pytest.raises(ToolError, match=r"JSON object; got \[\"a\"\]"):
        validate_args(spec, {INVALID_ARGS_KEY: ["a"]})
    with pytest.raises(ToolError, match="JSON object"):
        validate_args(spec, ["a"])
    assert validate_args(spec, {"name": "x"}) == {"name": "x"}


def test_same_action_is_per_turn() -> None:
    a = ModelOutput(
        calls=(ToolCall("probe", {"name": "a"}), ToolCall("probe", {"name": "b"}))
    )
    b = ModelOutput(
        calls=(ToolCall("probe", {"name": "a"}), ToolCall("probe", {"name": "c"}))
    )
    assert same_action(a, a)
    assert not same_action(a, b)  # one call differs → a different action, no comparison
    assert not same_action(a, ModelOutput(answer="x"))


def _dump() -> Dump:
    return Dump(
        at_step=1,
        first_step=3,
        outputs=(
            ModelOutput(calls=(ToolCall("probe", {"name": "b"}),)),
            ModelOutput(calls=(ToolCall("probe", {"name": "c"}),)),
            ModelOutput(answer="done"),
        ),
    )


def _replay(**args: object) -> ModelOutput:
    return ModelOutput(calls=(ToolCall("replay", dict(args)),))


def test_resolve_replay_picks_by_previous_step_number_and_keeps_provenance() -> None:
    res = resolve_replay(_replay(from_step=4), _dump())
    assert res.invalid is None
    assert res.output.calls == (ToolCall("probe", {"name": "c"}),)
    assert res.picked_step == 4 and res.steps is None


def test_resolve_replay_bounded_steps_and_final_answer() -> None:
    one = resolve_replay(_replay(from_step=3, steps=1), _dump())
    assert one.output.calls[0].args == {"name": "b"} and one.steps == 1
    fin = resolve_replay(_replay(from_step=5), _dump())
    assert fin.output.is_finish and fin.output.answer == "done" and fin.picked_step == 5


def test_resolve_replay_reports_unresolvable_picks() -> None:
    assert resolve_replay(_replay(from_step=9), _dump()).invalid is not None
    assert resolve_replay(_replay(from_step=3), None).invalid is not None
    assert resolve_replay(_replay(from_step="3"), _dump()).invalid is not None
    assert resolve_replay(_replay(from_step=True), _dump()).invalid is not None
    assert resolve_replay(_replay(from_step=3, steps=0), _dump()).invalid is not None
    mixed = ModelOutput(
        calls=(ToolCall("replay", {"from_step": 3}), ToolCall("probe", {}))
    )
    assert resolve_replay(mixed, _dump()).invalid is not None
    not_an_object = ModelOutput(calls=(ToolCall("replay", [3]),))  # type: ignore[arg-type]
    assert resolve_replay(not_an_object, _dump()).invalid is not None
    plain = ModelOutput(calls=(ToolCall("probe", {"name": "a"}),))
    assert resolve_replay(plain, _dump()).output == plain


def test_find_same_action_aligns_by_identity_not_position() -> None:
    probe_a = ModelOutput(calls=(ToolCall("probe", {"name": "a"}),))
    probe_b = ModelOutput(calls=(ToolCall("probe", {"name": "b"}),))
    prev = Transcript(
        (
            Step(probe_a, (ToolResult("probe", True, "A"),)),
            Step(probe_b, (ToolResult("probe", True, "B"),)),
            Step(ModelOutput(answer="done"), ()),
        )
    )
    assert find_same_action(prev, probe_b, 0) == 1
    assert find_same_action(prev, probe_b, 2) is None
    assert (
        find_same_action(
            prev, ModelOutput(calls=(ToolCall("probe", {"name": "z"}),)), 0
        )
        is None
    )

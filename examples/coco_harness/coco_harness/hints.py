"""Run-time hints shown to the model but excluded from memo keys and never stored.

DESIGN.md §4. ``RunHints`` is the ``hints`` argument of ``llm_step``; the memo
key excludes it (``memo_key={"hints": None}``), so hints can carry anything
about *this* run without changing a turn's identity:

- ``Dump``: the previous transcript's model outputs from a position on,
  numbered by that transcript's step numbers. Shown once and then left in
  place for the rest of the run.
- ``Diff``: a tool result that differs from the previous transcript's for the
  same action.
- ``Note``: a harness message, e.g. "fast-forward reached the final answer".
- ``Replay``: makes ``llm_step`` return this output without inference (used
  by fast-forward so the turn still writes a memo entry).
- ``TurnScratch``: a mutable channel from the turn body back to the session
  loop — did inference happen, was a step picked, which notes were shown. It
  is how the loop knows a hit from a miss without reading engine internals,
  and it never reaches storage because it travels only inside ``hints``.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace

from .spec import Json, ToolSpec
from .transcript import ModelOutput, ToolCall

REPLAY_TOOL = "replay"


@dataclass(frozen=True)
class Dump:
    at_step: int  # position in the current transcript where it is shown
    first_step: int  # step number (in the previous transcript) of outputs[0]
    outputs: tuple[ModelOutput, ...]

    def index_of_step(self, step: int) -> int | None:
        """Position in ``outputs`` of previous-transcript step number ``step``."""
        i = step - self.first_step
        return i if 0 <= i < len(self.outputs) else None

    @property
    def last_step(self) -> int:
        return self.first_step + len(self.outputs) - 1


@dataclass(frozen=True)
class Diff:
    at_step: int
    call: ToolCall
    before: Json
    after: Json


@dataclass(frozen=True)
class Note:
    at_step: int
    text: str


@dataclass(frozen=True)
class Replay:
    output: ModelOutput


@dataclass
class TurnScratch:
    """Filled in by ``llm_step``; read by the session loop after the turn.

    Untouched on a memo hit, which is exactly how the loop recognizes one.
    """

    inferred: bool = False
    replayed: bool = False
    picked_step: int | None = None  # previous-transcript step number the model picked
    pick_steps: int | None = (
        None  # actions the pick covers; None = until a result differs
    )
    notes: list[Note] = field(default_factory=list)  # shown during this turn's retries


@dataclass(frozen=True)
class RunHints:
    dump: Dump | None = None
    diffs: tuple[Diff, ...] = ()
    notes: tuple[Note, ...] = ()
    replay: Replay | None = None
    scratch: TurnScratch = field(default_factory=TurnScratch)

    def add_diff(self, diff: Diff) -> RunHints:
        return replace(self, diffs=(*self.diffs, diff))

    def add_note(self, note: Note) -> RunHints:
        return replace(self, notes=(*self.notes, note))


def replay_tool_spec() -> ToolSpec:
    """The harness meta tool. Resolved inside ``llm_step``; never stored as an action."""
    return ToolSpec(
        name=REPLAY_TOOL,
        description=(
            "Reissue actions from the previous run shown in the [replay] block; "
            "from_step is the number in brackets. Without steps, the harness keeps "
            "reissuing the following actions until a tool result differs from last "
            "run or the final answer is reached; steps=N reissues at most N."
        ),
        parameters={
            "type": "object",
            "properties": {
                "from_step": {"type": "integer"},
                "steps": {"type": "integer", "minimum": 1},
            },
            "required": ["from_step"],
        },
    )

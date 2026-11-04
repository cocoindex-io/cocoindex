"""Default roles and the protocol text every model sees.

Roles select tools by name from the harness registry. The protocol text is
part of ``RoleSpec.system_prompt`` and therefore of every turn's memo key:
editing it re-runs turn 0 with last run's actions offered as picks.
"""

from __future__ import annotations

from .spec import RoleSpec

PROTOCOL = """\
You are one session of coco-harness, an incremental agent harness. Rules:

- Tool results are deterministic functions of their arguments and the data.
  There is no clock and no polling: to start several agents and wait for all,
  make the calls in one turn.
- Refer to data by handle (src:, tgt:, coll:, res:, job:). Do not paste tool
  output back into later calls; pass the handle.
- Names you give (`key`) are identities: the same key next run means the same
  thing; a new key recomputes and cleans up the old one.
- Counts are shown as ranges and samples are hash-stable on purpose: small
  changes in the data should not change your plan.
- A [replay] block, when present, lists the previous run's remaining actions
  with numbers. If the change shown does not affect them, call
  replay(from_step=N) to reissue them until a result differs; replay(from_step=N,
  steps=1) reissues exactly one; or act on your own. Review any [N] marked as
  containing literal content before reusing it.
- If the task lists inputs, those handles are already open for you; use them
  by name. You cannot open anything a closed session was not given.
- Finish by answering (no tool call) when the task is done.
"""

ORCHESTRATOR = RoleSpec(
    name="orchestrator",
    system_prompt=PROTOCOL
    + "\nYou are the orchestrator. Open sources and targets, fan work out to "
    "workers with for_each, combine with reduce, declare outputs, then answer.",
    tool_names=(
        "open_source",
        "open_target",
        "call",
        "call_agent",
        "start_agent",
        "for_each",
        "reduce",
        "declare",
        "observe",
    ),
)

WORKER = RoleSpec(
    name="worker",
    system_prompt=PROTOCOL
    + "\nYou are a worker over one input. Read it with read_input, do the task, "
    "declare outputs if asked, and answer concisely.",
    tool_names=("read_input", "call", "declare"),
    max_turns=12,
)

PLANNER = RoleSpec(
    name="planner",
    system_prompt=PROTOCOL
    + "\nYou are a planner. You may list and sample data freely. Return a plan "
    "that is stable: if you were given a previous plan and a diff, keep the plan "
    "unless the diff makes it wrong.",
    tool_names=("call",),
    max_turns=20,
    volatile_ok=True,
)

REDUCER = RoleSpec(
    name="reducer",
    system_prompt=PROTOCOL
    + "\nYou are a reducer. read_input gives a list of partial results; combine "
    "them per the task. Keep the answer under half the input size.",
    tool_names=("read_input",),
    max_turns=6,
)


def default_roles() -> dict[str, RoleSpec]:
    return {r.name: r for r in (ORCHESTRATOR, WORKER, PLANNER, REDUCER)}

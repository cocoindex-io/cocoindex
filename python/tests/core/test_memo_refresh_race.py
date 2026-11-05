"""A component memo refresh (changed-but-valid states) applies only to the
entry that was validated.

Two updates of one app run at once. Run A validates the stored entry and
suspends inside its async state function. Run B's validation rejects the entry,
so B re-executes and stores a new entry. When A resumes, its refresh must be
discarded: writing A's states into B's entry would attach states that never
belonged to B's run.
"""

import asyncio
from typing import Any

import pytest

import cocoindex as coco

from tests import common
from tests.common.target_states import GlobalDictTarget


class _Script:
    """Drives the state function call by call (see `RacyEntry`)."""

    def __init__(self) -> None:
        self.calls = 0
        self.prevs: list[Any] = []
        self.a_in_validator = asyncio.Event()
        self.gate = asyncio.Event()


_script = _Script()


class RacyEntry:
    """One memo key; the state function decides by the stored state it sees:

    - no stored state (first run, initial collection): state 1
    - stored 1, first time (run A's probe): state 2, valid; then waits for the
      gate
    - stored 1, later (run B's probe and its re-checks under the build
      permit): state 5, invalid, so B re-executes and stores 5
    - stored 5 (run C's probe): keeps it, valid
    """

    def __coco_memo_key__(self) -> object:
        return "racy"

    async def __coco_memo_state__(
        self, prev_state: int | coco.NonExistenceType
    ) -> coco.MemoStateOutcome:
        _script.calls += 1
        _script.prevs.append(prev_state)
        if coco.is_non_existence(prev_state):
            return coco.MemoStateOutcome(state=1)
        if prev_state == 1 and not _script.a_in_validator.is_set():
            _script.a_in_validator.set()
            await _script.gate.wait()
            return coco.MemoStateOutcome(state=2, memo_valid=True)
        if prev_state == 1:
            return coco.MemoStateOutcome(state=5, memo_valid=False)
        return coco.MemoStateOutcome(state=prev_state, memo_valid=True)


@coco.fn(memo=True)
async def _declare_racy(entry: RacyEntry) -> None:
    coco.declare_target_state(
        GlobalDictTarget.target_state("racy", f"run {_script.calls}")
    )


@pytest.mark.asyncio
async def test_refresh_applies_only_to_the_validated_entry() -> None:
    global _script
    GlobalDictTarget.store.clear()
    _script = _Script()
    # Created inside the test so the environment binds to this loop: the state
    # function awaits events that live on it.
    coco_env = common.create_test_env(__file__, suffix="race")

    app = coco.App(
        coco.AppConfig(
            name="test_refresh_applies_only_to_the_validated_entry",
            environment=coco_env,
        ),
        _declare_racy,
        RacyEntry(),
    )

    # Run 0: miss; the entry is stored with state 1.
    await app.update()
    assert len(_script.prevs) == 1 and coco.is_non_existence(_script.prevs[0])

    async def run_b() -> None:
        await _script.a_in_validator.wait()
        await app.update()  # rejects the entry, re-executes, stores state 5
        _script.gate.set()

    # Run A validates (state 2, valid) while B replaces the entry underneath.
    await asyncio.gather(app.update(), run_b())
    probes_before_c = len(_script.prevs)
    assert probes_before_c >= 3 and all(p == 1 for p in _script.prevs[1:])

    # Run C probes the entry B stored. Its state must be B's 5, not A's 2.
    await app.update()
    assert _script.prevs[probes_before_c:] == [5]

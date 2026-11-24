"""A component memo refresh (changed-but-valid states) applies only to the
entry that was validated.

Two updates of one app run at once. Run A validates the stored entry and
suspends inside its async state function. Run B replaces the entry under the
same memo key (its validation rejects the entry, so B re-executes) or under a
different key (B's key does not match the stored entry, so B re-executes). When
A resumes, its refresh must be discarded: writing A's states into B's entry
would attach states that never belonged to B's run.
"""

import asyncio
from typing import Any

import pytest

import cocoindex as coco

from tests import common
from tests.common.target_states import GlobalDictTarget


class _Script:
    """Drives the memo key and the state function call by call (see `RacyEntry`)."""

    def __init__(self, same_key: bool) -> None:
        self.same_key = same_key
        self.key = "k1"
        self.calls = 0
        # (memo key, stored state) seen by each state function call.
        self.prevs: list[tuple[str, Any]] = []
        self.a_in_validator = asyncio.Event()
        self.gate = asyncio.Event()


_script = _Script(same_key=True)


class RacyEntry:
    """Memo key `_script.key`; the state function decides by the stored state
    it sees:

    - no stored state (first run, or B's initial collection after a key
      mismatch): state 1
    - stored 1, first time (run A's probe): state 2, valid; then waits for the
      gate
    - stored 1, later, same-key replacement (run B's probe and its re-checks
      under the build permit): state 5, invalid, so B re-executes and stores 5
    - any other stored state (run C's probe, and the different-key run C's
      probe of B's state 1): keeps it, valid
    """

    def __coco_memo_key__(self) -> object:
        return _script.key

    async def __coco_memo_state__(
        self, prev_state: int | coco.NonExistenceType
    ) -> coco.MemoStateOutcome:
        _script.calls += 1
        _script.prevs.append((_script.key, prev_state))
        if coco.is_non_existence(prev_state):
            return coco.MemoStateOutcome(state=1)
        if prev_state == 1 and not _script.a_in_validator.is_set():
            _script.a_in_validator.set()
            await _script.gate.wait()
            return coco.MemoStateOutcome(state=2, memo_valid=True)
        if prev_state == 1 and _script.same_key:
            return coco.MemoStateOutcome(state=5, memo_valid=False)
        return coco.MemoStateOutcome(state=prev_state, memo_valid=True)


@coco.fn(memo=True)
async def _declare_racy(entry: RacyEntry) -> None:
    coco.declare_target_state(
        GlobalDictTarget.target_state("racy", f"run {_script.calls}")
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("same_key", [True, False], ids=["same_key", "different_key"])
async def test_refresh_applies_only_to_the_validated_entry(same_key: bool) -> None:
    global _script
    GlobalDictTarget.store.clear()
    _script = _Script(same_key=same_key)
    # Created inside the test so the environment binds to this loop: the state
    # function awaits events that live on it.
    coco_env = common.create_test_env(
        __file__, suffix="same_key" if same_key else "different_key"
    )

    app = coco.App(
        coco.AppConfig(
            name="test_refresh_applies_only_to_the_validated_entry",
            environment=coco_env,
        ),
        _declare_racy,
        RacyEntry(),
    )

    # Run 0: miss; the entry is stored under k1 with state 1.
    await app.update()
    assert len(_script.prevs) == 1 and coco.is_non_existence(_script.prevs[0][1])

    async def run_b() -> None:
        await _script.a_in_validator.wait()
        if not same_key:
            _script.key = "k2"
        # Same key: rejects the entry, re-executes, stores state 5 under k1.
        # Different key: the stored k1 entry does not match, re-executes,
        # stores state 1 under k2.
        await app.update()
        _script.gate.set()

    # Run A validates (state 2, valid) while B replaces the entry underneath.
    await asyncio.gather(app.update(), run_b())
    probes_before_c = len(_script.prevs)
    assert probes_before_c >= 3
    assert _script.prevs[1] == ("k1", 1)  # run A's probe
    if same_key:
        assert all(p == ("k1", 1) for p in _script.prevs[2:])
    else:
        b_probes = _script.prevs[2:]
        assert all(k == "k2" and coco.is_non_existence(s) for k, s in b_probes)

    # Run C probes the entry B stored. Its state must be B's (5 under k1, or
    # 1 under k2), not A's 2.
    await app.update()
    expected_c_prev = ("k1", 5) if same_key else ("k2", 1)
    assert _script.prevs[probes_before_c:] == [expected_c_prev]

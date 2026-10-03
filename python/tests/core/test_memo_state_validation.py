"""End-to-end tests for memo state validation.

Tests cover:
- Function-level memoization with state validation (sync and async)
- Component-level memoization with state validation (sync and async)
- State unchanged → memo reused (function not re-executed)
- State changed, not reusable → function re-executed, new states persisted
- State changed, still reusable → function NOT re-executed, new states persisted
- State changed → previously declared target states cleaned up (sync and async)
- State changed, still reusable → target states declared inside the memoized
  body are kept (sync, async, and via a nested callee's memo entry)
"""

from dataclasses import dataclass
from typing import Any

import cocoindex as coco

from tests import common
from tests.common.target_states import (
    DictDataWithPrev,
    GlobalDictTarget,
    Metrics,
)

coco_env = common.create_test_env(__file__)


@dataclass
class Entry:
    """Entry with memo key and memo state for testing."""

    name: str
    version: int  # contributes to memo key
    state_value: int  # contributes to memo state (simulates mtime, etc.)
    content: str  # actual data

    def __coco_memo_key__(self) -> object:
        return (self.name, self.version)

    def __coco_memo_state__(self, prev_state: Any) -> coco.MemoStateOutcome:
        # State changed → not reusable (simple case matching prior behavior)
        memo_valid = (
            not coco.is_non_existence(prev_state) and self.state_value == prev_state
        )
        return coco.MemoStateOutcome(state=self.state_value, memo_valid=memo_valid)


# ============================================================================
# Function-level state validation (sync)
# ============================================================================

_source_data: dict[str, Entry] = {}
_metrics = Metrics()


@coco.fn(memo=True)
def _transform_entry(entry: Entry) -> str:
    _metrics.increment("call.transform_entry")
    return f"processed: {entry.content}"


@coco.fn
def _process_data() -> None:
    for key, value in _source_data.items():
        transformed = _transform_entry(value)
        coco.declare_target_state(GlobalDictTarget.target_state(key, transformed))


def test_state_validation_sync() -> None:
    GlobalDictTarget.store.clear()
    _source_data.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(name="test_state_validation_sync", environment=coco_env),
        _process_data,
    )

    # Run 1: cache miss — both entries execute
    _source_data["A"] = Entry(name="A", version=1, state_value=100, content="contentA1")
    _source_data["B"] = Entry(name="B", version=1, state_value=200, content="contentB1")
    app.update_blocking()
    assert _metrics.collect() == {"call.transform_entry": 2}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="processed: contentA1", prev=[], prev_may_be_missing=True
        ),
        "B": DictDataWithPrev(
            data="processed: contentB1", prev=[], prev_may_be_missing=True
        ),
    }

    # Run 2: same entries, same state → memo valid, 0 calls
    app.update_blocking()
    assert _metrics.collect() == {}

    # Run 3: A state changes (state_value 100→101), B unchanged → A re-executes
    _source_data["A"] = Entry(name="A", version=1, state_value=101, content="contentA2")
    app.update_blocking()
    assert _metrics.collect() == {"call.transform_entry": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="processed: contentA2",
            prev=["processed: contentA1"],
            prev_may_be_missing=False,
        ),
        "B": DictDataWithPrev(
            data="processed: contentB1", prev=[], prev_may_be_missing=True
        ),
    }

    # Run 4: no changes → 0 calls (new states persisted correctly from run 3)
    app.update_blocking()
    assert _metrics.collect() == {}


# ============================================================================
# Function-level state validation (async)
# ============================================================================


@coco.fn.as_async(memo=True)
def _transform_entry_async(entry: Entry) -> str:
    _metrics.increment("call.transform_entry_async")
    return f"processed_async: {entry.content}"


@coco.fn
async def _process_data_async() -> None:
    for key, value in _source_data.items():
        transformed = await _transform_entry_async(value)
        coco.declare_target_state(GlobalDictTarget.target_state(key, transformed))


def test_state_validation_async() -> None:
    GlobalDictTarget.store.clear()
    _source_data.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(name="test_state_validation_async", environment=coco_env),
        _process_data_async,
    )

    # Run 1: cache miss — both entries execute
    _source_data["A"] = Entry(name="A", version=1, state_value=100, content="contentA1")
    _source_data["B"] = Entry(name="B", version=1, state_value=200, content="contentB1")
    app.update_blocking()
    assert _metrics.collect() == {"call.transform_entry_async": 2}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="processed_async: contentA1", prev=[], prev_may_be_missing=True
        ),
        "B": DictDataWithPrev(
            data="processed_async: contentB1", prev=[], prev_may_be_missing=True
        ),
    }

    # Run 2: same entries, same state → memo valid, 0 calls
    app.update_blocking()
    assert _metrics.collect() == {}

    # Run 3: A state changes → A re-executes
    _source_data["A"] = Entry(name="A", version=1, state_value=101, content="contentA2")
    app.update_blocking()
    assert _metrics.collect() == {"call.transform_entry_async": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="processed_async: contentA2",
            prev=["processed_async: contentA1"],
            prev_may_be_missing=False,
        ),
        "B": DictDataWithPrev(
            data="processed_async: contentB1", prev=[], prev_may_be_missing=True
        ),
    }

    # Run 4: no changes → 0 calls
    app.update_blocking()
    assert _metrics.collect() == {}


# ============================================================================
# Component-level state validation (sync)
# ============================================================================


@coco.fn(memo=True)
def _declare_entry(key: str, entry: Entry) -> None:
    _metrics.increment("call.declare_entry")
    coco.declare_target_state(
        GlobalDictTarget.target_state(key, f"comp: {entry.content}")
    )


@coco.fn
def _declare_data() -> None:
    for key, value in _source_data.items():
        _declare_entry(key, value)


def test_state_validation_component_sync() -> None:
    GlobalDictTarget.store.clear()
    _source_data.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_state_validation_component_sync", environment=coco_env
        ),
        _declare_data,
    )

    # Run 1: cache miss
    _source_data["A"] = Entry(name="A", version=1, state_value=100, content="contentA1")
    _source_data["B"] = Entry(name="B", version=1, state_value=200, content="contentB1")
    app.update_blocking()
    assert _metrics.collect() == {"call.declare_entry": 2}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="comp: contentA1", prev=[], prev_may_be_missing=True
        ),
        "B": DictDataWithPrev(
            data="comp: contentB1", prev=[], prev_may_be_missing=True
        ),
    }

    # Run 2: unchanged → no re-execution
    app.update_blocking()
    assert _metrics.collect() == {}

    # Run 3: A state changes → A re-executes, target state updated
    _source_data["A"] = Entry(name="A", version=1, state_value=101, content="contentA2")
    app.update_blocking()
    assert _metrics.collect() == {"call.declare_entry": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="comp: contentA2",
            prev=["comp: contentA1"],
            prev_may_be_missing=False,
        ),
        "B": DictDataWithPrev(
            data="comp: contentB1", prev=[], prev_may_be_missing=True
        ),
    }

    # Run 4: no changes → 0 calls
    app.update_blocking()
    assert _metrics.collect() == {}


# ============================================================================
# Target state cleanup after state change (sync)
# ============================================================================


@dataclass
class MultiEntry:
    """Entry with memo key, memo state, and multiple sub-items for target states."""

    name: str
    version: int  # contributes to memo key
    state_value: int  # contributes to memo state
    items: dict[str, str]  # key → content; each becomes a target state

    def __coco_memo_key__(self) -> object:
        return (self.name, self.version)

    def __coco_memo_state__(self, prev_state: Any) -> coco.MemoStateOutcome:
        memo_valid = (
            not coco.is_non_existence(prev_state) and self.state_value == prev_state
        )
        return coco.MemoStateOutcome(state=self.state_value, memo_valid=memo_valid)


_multi_source_data: dict[str, MultiEntry] = {}


@coco.fn(memo=True)
def _declare_multi(entry: MultiEntry) -> None:
    _metrics.increment("call.declare_multi")
    for key, content in entry.items.items():
        coco.declare_target_state(GlobalDictTarget.target_state(key, content))


@coco.fn
def _declare_multi_data() -> None:
    for value in _multi_source_data.values():
        _declare_multi(value)


def test_state_validation_target_state_cleanup_sync() -> None:
    """State change (memo key unchanged) causes re-execution that declares fewer
    target states → the previously-declared target states should be cleaned up."""
    GlobalDictTarget.store.clear()
    _multi_source_data.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_state_validation_target_state_cleanup_sync",
            environment=coco_env,
        ),
        _declare_multi_data,
    )

    # Run 1: cache miss — declares target states A and B
    _multi_source_data["E1"] = MultiEntry(
        name="E1",
        version=1,
        state_value=100,
        items={"A": "contentA", "B": "contentB"},
    )
    app.update_blocking()
    assert _metrics.collect() == {"call.declare_multi": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA", prev=[], prev_may_be_missing=True),
        "B": DictDataWithPrev(data="contentB", prev=[], prev_may_be_missing=True),
    }

    # Run 2: same state → memo valid, 0 calls
    app.update_blocking()
    assert _metrics.collect() == {}

    # Run 3: state changes (100→101), B removed from items → re-executes,
    # only A declared. B target state should be cleaned up.
    _multi_source_data["E1"] = MultiEntry(
        name="E1",
        version=1,
        state_value=101,
        items={"A": "contentA2"},
    )
    app.update_blocking()
    assert _metrics.collect() == {"call.declare_multi": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="contentA2", prev=["contentA"], prev_may_be_missing=False
        ),
    }

    # Run 4: no changes → 0 calls (new state persisted correctly)
    app.update_blocking()
    assert _metrics.collect() == {}


# ============================================================================
# Target state cleanup after state change (async)
# ============================================================================


@coco.fn.as_async(memo=True)
def _declare_multi_async(entry: MultiEntry) -> None:
    _metrics.increment("call.declare_multi_async")
    for key, content in entry.items.items():
        coco.declare_target_state(GlobalDictTarget.target_state(key, content))


@coco.fn
async def _declare_multi_data_async() -> None:
    for value in _multi_source_data.values():
        await _declare_multi_async(value)


def test_state_validation_target_state_cleanup_async() -> None:
    """Async variant: state change causes re-execution that declares fewer
    target states → previously-declared target states should be cleaned up."""
    GlobalDictTarget.store.clear()
    _multi_source_data.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_state_validation_target_state_cleanup_async",
            environment=coco_env,
        ),
        _declare_multi_data_async,
    )

    # Run 1: cache miss — declares target states A and B
    _multi_source_data["E1"] = MultiEntry(
        name="E1",
        version=1,
        state_value=100,
        items={"A": "contentA", "B": "contentB"},
    )
    app.update_blocking()
    assert _metrics.collect() == {"call.declare_multi_async": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA", prev=[], prev_may_be_missing=True),
        "B": DictDataWithPrev(data="contentB", prev=[], prev_may_be_missing=True),
    }

    # Run 2: same state → memo valid, 0 calls
    app.update_blocking()
    assert _metrics.collect() == {}

    # Run 3: state changes (100→101), B removed from items → re-executes,
    # only A declared. B target state should be cleaned up.
    _multi_source_data["E1"] = MultiEntry(
        name="E1",
        version=1,
        state_value=101,
        items={"A": "contentA2"},
    )
    app.update_blocking()
    assert _metrics.collect() == {"call.declare_multi_async": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="contentA2", prev=["contentA"], prev_may_be_missing=False
        ),
    }

    # Run 4: no changes → 0 calls (new state persisted correctly)
    app.update_blocking()
    assert _metrics.collect() == {}


# ============================================================================
# Component-level state validation (async)
# ============================================================================


@coco.fn.as_async(memo=True)
def _declare_entry_async(key: str, entry: Entry) -> None:
    _metrics.increment("call.declare_entry_async")
    coco.declare_target_state(
        GlobalDictTarget.target_state(key, f"comp_async: {entry.content}")
    )


@coco.fn
async def _declare_data_async() -> None:
    for key, value in _source_data.items():
        await _declare_entry_async(key, value)


def test_state_validation_component_async() -> None:
    GlobalDictTarget.store.clear()
    _source_data.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_state_validation_component_async", environment=coco_env
        ),
        _declare_data_async,
    )

    # Run 1: cache miss
    _source_data["A"] = Entry(name="A", version=1, state_value=100, content="contentA1")
    _source_data["B"] = Entry(name="B", version=1, state_value=200, content="contentB1")
    app.update_blocking()
    assert _metrics.collect() == {"call.declare_entry_async": 2}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="comp_async: contentA1", prev=[], prev_may_be_missing=True
        ),
        "B": DictDataWithPrev(
            data="comp_async: contentB1", prev=[], prev_may_be_missing=True
        ),
    }

    # Run 2: unchanged → no re-execution
    app.update_blocking()
    assert _metrics.collect() == {}

    # Run 3: A state changes → A re-executes
    _source_data["A"] = Entry(name="A", version=1, state_value=101, content="contentA2")
    app.update_blocking()
    assert _metrics.collect() == {"call.declare_entry_async": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="comp_async: contentA2",
            prev=["comp_async: contentA1"],
            prev_may_be_missing=False,
        ),
        "B": DictDataWithPrev(
            data="comp_async: contentB1", prev=[], prev_may_be_missing=True
        ),
    }

    # Run 4: no changes → 0 calls
    app.update_blocking()
    assert _metrics.collect() == {}


# ============================================================================
# State changed but reusable (sync) — multi-level validation
# ============================================================================


@dataclass
class TwoLevelEntry:
    """Simulates a file with mtime + content fingerprint.

    The state is (mtime, fingerprint). On validation:
    - If mtime unchanged → reusable immediately.
    - If mtime changed, compare fingerprints: if same → reusable (state updated
      with new mtime), if different → not reusable.
    """

    name: str
    mtime: int
    fingerprint: str
    content: str

    def __coco_memo_key__(self) -> object:
        return self.name

    def __coco_memo_state__(
        self, prev_state: tuple[int, str] | coco.NonExistenceType
    ) -> coco.MemoStateOutcome:
        new_state = (self.mtime, self.fingerprint)
        if coco.is_non_existence(prev_state):
            return coco.MemoStateOutcome(state=new_state, memo_valid=True)
        prev_mtime, prev_fp = prev_state
        if self.mtime == prev_mtime:
            # mtime unchanged — definitely reusable
            return coco.MemoStateOutcome(state=new_state, memo_valid=True)
        # mtime changed — check content fingerprint
        return coco.MemoStateOutcome(
            state=new_state, memo_valid=self.fingerprint == prev_fp
        )


_two_level_source: dict[str, TwoLevelEntry] = {}


@coco.fn(memo=True)
def _process_two_level(entry: TwoLevelEntry) -> str:
    _metrics.increment("call.process_two_level")
    return f"result: {entry.content}"


@coco.fn
def _run_two_level() -> None:
    for key, value in _two_level_source.items():
        result = _process_two_level(value)
        coco.declare_target_state(GlobalDictTarget.target_state(key, result))


def test_state_changed_but_reusable_sync() -> None:
    """Multi-level validation: mtime changes but content fingerprint is the same.
    Function should NOT re-execute, but new state (with updated mtime) should be persisted."""
    GlobalDictTarget.store.clear()
    _two_level_source.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_state_changed_but_reusable_sync", environment=coco_env
        ),
        _run_two_level,
    )

    # Run 1: cache miss — executes
    _two_level_source["X"] = TwoLevelEntry(
        name="X", mtime=1000, fingerprint="abc123", content="hello"
    )
    app.update_blocking()
    assert _metrics.collect() == {"call.process_two_level": 1}
    assert GlobalDictTarget.store.data == {
        "X": DictDataWithPrev(data="result: hello", prev=[], prev_may_be_missing=True),
    }

    # Run 2: same mtime → reusable, 0 calls
    app.update_blocking()
    assert _metrics.collect() == {}

    # Run 3: mtime changes (1000→2000) but fingerprint unchanged → reusable, 0 calls
    # State changes from (1000, "abc123") to (2000, "abc123") — persisted but no re-execution.
    _two_level_source["X"] = TwoLevelEntry(
        name="X", mtime=2000, fingerprint="abc123", content="hello"
    )
    app.update_blocking()
    assert _metrics.collect() == {}  # NO re-execution!
    assert GlobalDictTarget.store.data == {
        "X": DictDataWithPrev(data="result: hello", prev=[], prev_may_be_missing=True),
    }

    # Run 4: same mtime (2000) — verifies the updated state was persisted
    # (if state wasn't persisted, the old mtime 1000 would be stored, and
    # comparing with 2000 would trigger a fingerprint check again)
    app.update_blocking()
    assert _metrics.collect() == {}

    # Run 5: mtime changes again AND fingerprint changes → not reusable, re-executes
    _two_level_source["X"] = TwoLevelEntry(
        name="X", mtime=3000, fingerprint="def456", content="world"
    )
    app.update_blocking()
    assert _metrics.collect() == {"call.process_two_level": 1}
    assert GlobalDictTarget.store.data == {
        "X": DictDataWithPrev(
            data="result: world",
            prev=["result: hello"],
            prev_may_be_missing=False,
        ),
    }

    # Run 6: no changes → 0 calls
    app.update_blocking()
    assert _metrics.collect() == {}


# ============================================================================
# State changed but reusable (async)
# ============================================================================


@coco.fn.as_async(memo=True)
def _process_two_level_async(entry: TwoLevelEntry) -> str:
    _metrics.increment("call.process_two_level_async")
    return f"result_async: {entry.content}"


@coco.fn
async def _run_two_level_async() -> None:
    for key, value in _two_level_source.items():
        result = await _process_two_level_async(value)
        coco.declare_target_state(GlobalDictTarget.target_state(key, result))


def test_state_changed_but_reusable_async() -> None:
    """Async variant: mtime changes but fingerprint same → no re-execution."""
    GlobalDictTarget.store.clear()
    _two_level_source.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_state_changed_but_reusable_async", environment=coco_env
        ),
        _run_two_level_async,
    )

    # Run 1: cache miss — executes
    _two_level_source["X"] = TwoLevelEntry(
        name="X", mtime=1000, fingerprint="abc123", content="hello"
    )
    app.update_blocking()
    assert _metrics.collect() == {"call.process_two_level_async": 1}

    # Run 2: mtime changes but fingerprint same → reusable, 0 calls
    _two_level_source["X"] = TwoLevelEntry(
        name="X", mtime=2000, fingerprint="abc123", content="hello"
    )
    app.update_blocking()
    assert _metrics.collect() == {}

    # Run 3: verify updated state persisted (same mtime 2000 → 0 calls)
    app.update_blocking()
    assert _metrics.collect() == {}

    # Run 4: fingerprint changes → re-executes
    _two_level_source["X"] = TwoLevelEntry(
        name="X", mtime=3000, fingerprint="def456", content="world"
    )
    app.update_blocking()
    assert _metrics.collect() == {"call.process_two_level_async": 1}


# ============================================================================
# State changed but reusable — target states declared inside the memo body
# ============================================================================


@coco.fn(memo=True)
def _declare_two_level(entry: TwoLevelEntry) -> None:
    _metrics.increment("call.declare_two_level")
    coco.declare_target_state(
        GlobalDictTarget.target_state(entry.name, f"comp: {entry.content}")
    )


@coco.fn
def _run_two_level_comp() -> None:
    for value in _two_level_source.values():
        _declare_two_level(value)


def test_state_changed_but_reusable_keeps_inner_target_states_sync() -> None:
    """A memoized function declares its target state *inside* its body. When its
    memo state changes but stays valid (mtime moved, fingerprint same), the body
    is skipped — and the target state it declared last time must survive.

    Entry Y never changes: it is a plain hit on every run, showing the
    behaviour is per-entry."""
    GlobalDictTarget.store.clear()
    _two_level_source.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_state_changed_but_reusable_keeps_inner_sync",
            environment=coco_env,
        ),
        _run_two_level_comp,
    )

    # Run 1: cache miss — both execute and declare
    _two_level_source["X"] = TwoLevelEntry(
        name="X", mtime=1000, fingerprint="abc123", content="hello"
    )
    _two_level_source["Y"] = TwoLevelEntry(
        name="Y", mtime=1000, fingerprint="yyy", content="stable"
    )
    app.update_blocking()
    assert _metrics.collect() == {"call.declare_two_level": 2}
    expected = {
        "X": DictDataWithPrev(data="comp: hello", prev=[], prev_may_be_missing=True),
        "Y": DictDataWithPrev(data="comp: stable", prev=[], prev_may_be_missing=True),
    }
    assert GlobalDictTarget.store.data == expected

    # Run 2: X's mtime changes but fingerprint same → reusable, 0 calls.
    # The target states must NOT be deleted even though the body did not
    # re-declare them.
    _two_level_source["X"] = TwoLevelEntry(
        name="X", mtime=2000, fingerprint="abc123", content="hello"
    )
    app.update_blocking()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == expected

    # Run 3: no change → still 0 calls, still present
    app.update_blocking()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == expected

    # Run 4: keep X's mtime at 2000 but break its fingerprint. The state fn
    # short-circuits on an mtime match, so this is a hit only if run 2's
    # refreshed state (mtime 2000) was actually persisted. A stale row (mtime
    # 1000) would fall through to the fingerprint check and re-execute.
    _two_level_source["X"] = TwoLevelEntry(
        name="X", mtime=2000, fingerprint="zzz", content="hello"
    )
    app.update_blocking()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == expected

    # Run 5: X's mtime and fingerprint change → re-executes; the previous
    # record is still tracked, so the target sees it as prev (not "may be
    # missing").
    _two_level_source["X"] = TwoLevelEntry(
        name="X", mtime=3000, fingerprint="def456", content="world"
    )
    app.update_blocking()
    assert _metrics.collect() == {"call.declare_two_level": 1}
    expected = {
        "X": DictDataWithPrev(
            data="comp: world", prev=["comp: hello"], prev_may_be_missing=False
        ),
        "Y": DictDataWithPrev(data="comp: stable", prev=[], prev_may_be_missing=True),
    }
    assert GlobalDictTarget.store.data == expected

    # Run 6: no change → 0 calls, unchanged
    app.update_blocking()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == expected

    # Run 7: X is no longer processed → its target state is cleaned up.
    # Protection applies only to entries that hit; an entry nobody calls is
    # still reconciled away.
    del _two_level_source["X"]
    app.update_blocking()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == {
        "Y": DictDataWithPrev(data="comp: stable", prev=[], prev_may_be_missing=True),
    }


# ============================================================================
# State changed but reusable — target states declared inside the memo body
# (async)
# ============================================================================


@coco.fn.as_async(memo=True)
def _declare_two_level_async(entry: TwoLevelEntry) -> None:
    _metrics.increment("call.declare_two_level_async")
    coco.declare_target_state(
        GlobalDictTarget.target_state(entry.name, f"comp_async: {entry.content}")
    )


@coco.fn
async def _run_two_level_comp_async() -> None:
    for value in _two_level_source.values():
        await _declare_two_level_async(value)


def test_state_changed_but_reusable_keeps_inner_target_states_async() -> None:
    """Async twin of the sync test above: the async hit path is separate Python
    code calling the same engine method."""
    GlobalDictTarget.store.clear()
    _two_level_source.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_state_changed_but_reusable_keeps_inner_async",
            environment=coco_env,
        ),
        _run_two_level_comp_async,
    )

    # Run 1: cache miss — executes and declares
    _two_level_source["X"] = TwoLevelEntry(
        name="X", mtime=1000, fingerprint="abc123", content="hello"
    )
    app.update_blocking()
    assert _metrics.collect() == {"call.declare_two_level_async": 1}
    expected = {
        "X": DictDataWithPrev(
            data="comp_async: hello", prev=[], prev_may_be_missing=True
        ),
    }
    assert GlobalDictTarget.store.data == expected

    # Run 2: mtime changes but fingerprint same → reusable, 0 calls, target kept
    _two_level_source["X"] = TwoLevelEntry(
        name="X", mtime=2000, fingerprint="abc123", content="hello"
    )
    app.update_blocking()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == expected

    # Run 3: no change → still present
    app.update_blocking()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == expected

    # Run 4: fingerprint changes → re-executes with the previous record tracked
    _two_level_source["X"] = TwoLevelEntry(
        name="X", mtime=3000, fingerprint="def456", content="world"
    )
    app.update_blocking()
    assert _metrics.collect() == {"call.declare_two_level_async": 1}
    expected = {
        "X": DictDataWithPrev(
            data="comp_async: world",
            prev=["comp_async: hello"],
            prev_may_be_missing=False,
        ),
    }
    assert GlobalDictTarget.store.data == expected

    # Run 5: no change → 0 calls
    app.update_blocking()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == expected


# ============================================================================
# State changed but reusable — target path known only to a dep-walked callee
# ============================================================================


@coco.fn(memo=True)
def _inner_declare(name: str, content: str) -> None:
    _metrics.increment("call.inner_declare")
    coco.declare_target_state(GlobalDictTarget.target_state(name, f"inner: {content}"))


@coco.fn(memo=True)
def _outer_two_level(entry: TwoLevelEntry, version: int) -> None:
    _metrics.increment("call.outer_two_level")
    _inner_declare(entry.name, entry.content)


_outer_version: list[int] = [1]
_call_inner_directly: list[bool] = [True]


@coco.fn
def _run_nested_two_level_inner_first() -> None:
    for value in _two_level_source.values():
        if _call_inner_directly[0]:
            _inner_declare(value.name, value.content)
        _outer_two_level(value, _outer_version[0])


def test_state_changed_but_reusable_keeps_dep_walked_target_states_sync() -> None:
    """The inner memoized function declares the target state; on run 1 it is a
    cache hit inside the outer's execution, so only the inner's own memo entry
    records that target. On run 2 the outer hits with a refreshed state and the
    inner is never probed: its target state, and its memo entry, must survive."""
    GlobalDictTarget.store.clear()
    _two_level_source.clear()
    _metrics.clear()
    _outer_version[0] = 1
    _call_inner_directly[0] = True

    app = coco.App(
        coco.AppConfig(
            name="test_state_changed_but_reusable_keeps_dep_walked_sync",
            environment=coco_env,
        ),
        _run_nested_two_level_inner_first,
    )

    # Run 1: inner misses (direct call), outer misses, inner hits inside outer
    _two_level_source["X"] = TwoLevelEntry(
        name="X", mtime=1000, fingerprint="abc123", content="hello"
    )
    app.update_blocking()
    assert _metrics.collect() == {"call.inner_declare": 1, "call.outer_two_level": 1}
    expected = {
        "X": DictDataWithPrev(data="inner: hello", prev=[], prev_may_be_missing=True),
    }
    assert GlobalDictTarget.store.data == expected

    # Run 2: no direct call; outer hits with refreshed state; inner never
    # probed. Its target survives only via the dep walk.
    _call_inner_directly[0] = False
    _two_level_source["X"] = TwoLevelEntry(
        name="X", mtime=2000, fingerprint="abc123", content="hello"
    )
    app.update_blocking()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == expected

    # Run 3: outer-only miss → inner must be a plain hit (its row survived)
    _outer_version[0] = 2
    app.update_blocking()
    assert _metrics.collect() == {"call.outer_two_level": 1}
    assert GlobalDictTarget.store.data == expected

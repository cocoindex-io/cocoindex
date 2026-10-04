"""End-to-end tests for memo state validation.

Tests cover:
- Function-level memoization with state validation (sync and async)
- Component-level memoization with state validation (sync and async)
- State unchanged → memo reused (function not re-executed)
- State changed, not reusable → function re-executed, new states persisted
- State changed, still reusable → function NOT re-executed, new states persisted
- State changed → previously declared target states cleaned up (sync and async)
"""

from dataclasses import dataclass
from typing import Any

import pytest

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

    def __coco_memo_state__(self, prev_state: Any) -> coco.MemoStateOutcome:
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
# State changed but reusable — component-level (sync)
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


def test_state_changed_but_reusable_component_sync() -> None:
    """Component-level: mtime changes but fingerprint same → no re-execution."""
    GlobalDictTarget.store.clear()
    _two_level_source.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_state_changed_but_reusable_comp_sync", environment=coco_env
        ),
        _run_two_level_comp,
    )

    # Run 1: cache miss
    _two_level_source["X"] = TwoLevelEntry(
        name="X", mtime=1000, fingerprint="abc123", content="hello"
    )
    app.update_blocking()
    assert _metrics.collect() == {"call.declare_two_level": 1}

    # Run 2: mtime changes but fingerprint same → reusable
    _two_level_source["X"] = TwoLevelEntry(
        name="X", mtime=2000, fingerprint="abc123", content="hello"
    )
    app.update_blocking()
    assert _metrics.collect() == {}

    # Run 3: verify state persisted
    app.update_blocking()
    assert _metrics.collect() == {}

    # Run 4: fingerprint changes → re-executes
    _two_level_source["X"] = TwoLevelEntry(
        name="X", mtime=3000, fingerprint="def456", content="world"
    )
    app.update_blocking()
    assert _metrics.collect() == {"call.declare_two_level": 1}


# ============================================================================
# State collected again after a re-run (`recollect_after_run`)
# ============================================================================

# What each key's function reads when it runs, and the version of each thing read.
_reads: dict[str, list[str]] = {}
_versions: dict[str, int] = {}


class _Tracked:
    """A key whose state is the set of things its function read, with versions.

    The set is only known once the function has run, so a re-run triggered by
    the state check must store the state collected after it.
    """

    def __init__(self, key: str) -> None:
        self.key = key
        self.observed: list[tuple[str, int]] = []

    def __coco_memo_key__(self) -> object:
        return self.key

    def __coco_memo_state__(
        self, prev_state: list[tuple[str, int]] | coco.NonExistenceType
    ) -> coco.MemoStateOutcome:
        if coco.is_non_existence(prev_state):
            return coco.MemoStateOutcome(state=list(self.observed))
        valid = all(_versions[name] == version for name, version in prev_state)
        return coco.MemoStateOutcome(
            state=prev_state, memo_valid=valid, recollect_after_run=True
        )


def _read_all(tracked: _Tracked) -> str:
    tracked.observed = [(name, _versions[name]) for name in _reads[tracked.key]]
    return ",".join(f"{name}@{version}" for name, version in tracked.observed)


@coco.fn(memo=True)
def _tracked_sync(tracked: _Tracked) -> str:
    _metrics.increment("call.tracked")
    return _read_all(tracked)


@coco.fn.as_async(memo=True)
def _tracked_async(tracked: _Tracked) -> str:
    _metrics.increment("call.tracked")
    return _read_all(tracked)


@coco.fn
def _process_tracked_sync() -> None:
    for key in _reads:
        coco.declare_target_state(
            GlobalDictTarget.target_state(key, _tracked_sync(_Tracked(key)))
        )


@coco.fn
async def _process_tracked_async() -> None:
    for key in _reads:
        value = await _tracked_async(_Tracked(key))
        coco.declare_target_state(GlobalDictTarget.target_state(key, value))


@pytest.mark.parametrize(
    ("name", "main"),
    [
        ("test_recollect_after_run_sync", _process_tracked_sync),
        ("test_recollect_after_run_async", _process_tracked_async),
    ],
)
def test_recollect_after_run(name: str, main: Any) -> None:
    GlobalDictTarget.store.clear()
    _metrics.clear()
    _reads.clear()
    _versions.clear()
    _reads["A"] = ["x"]
    _versions.update(x=1, y=1)

    app = coco.App(coco.AppConfig(name=name, environment=coco_env), main)

    app.update_blocking()
    assert _metrics.collect() == {"call.tracked": 1}
    assert GlobalDictTarget.store.data["A"].data == "x@1"

    # Nothing it read changed: reused.
    app.update_blocking()
    assert _metrics.collect() == {}

    # `x` changed, and the re-run reads `y` instead.
    _versions["x"] = 2
    _reads["A"] = ["y"]
    app.update_blocking()
    assert _metrics.collect() == {"call.tracked": 1}
    assert GlobalDictTarget.store.data["A"].data == "y@1"

    # The stored state is the one collected after the re-run, so a change to
    # `y` is seen. Without it the state would still name `x` and this run
    # would reuse a stale result.
    _versions["y"] = 2
    app.update_blocking()
    assert _metrics.collect() == {"call.tracked": 1}
    assert GlobalDictTarget.store.data["A"].data == "y@2"

    # A change to `x`, no longer read, does not re-run it.
    _versions["x"] = 3
    app.update_blocking()
    assert _metrics.collect() == {}


@coco.fn(memo=True)
def _tracked_component(tracked: _Tracked) -> None:
    coco.declare_target_state(
        GlobalDictTarget.target_state(tracked.key, _read_all(tracked))
    )


@coco.fn
async def _mount_tracked() -> None:
    for key in _reads:
        # `use_mount` so the child's failure reaches the caller.
        await coco.use_mount(
            coco.component_subpath(key), _tracked_component, _Tracked(key)
        )


def test_recollect_after_run_rejected_for_component() -> None:
    GlobalDictTarget.store.clear()
    _reads.clear()
    _versions.clear()
    _reads["A"] = ["x"]
    _versions["x"] = 1

    app = coco.App(
        coco.AppConfig(
            name="test_recollect_after_run_rejected_for_component",
            environment=coco_env,
        ),
        _mount_tracked,
    )
    app.update_blocking()

    # The state check rejects the memo and asks for a recollect, which a
    # memoized component cannot honor: it must fail, not store a stale state.
    _versions["x"] = 2
    with pytest.raises(Exception, match="recollect_after_run"):
        app.update_blocking()

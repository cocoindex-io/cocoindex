"""End-to-end tests for typed serde with memoization.

Tests cover:
- Memoized function return values round-trip through typed deserialization (dataclass, pydantic, untyped)
- Same-args calls within a single update only execute once (cache hit, no double deserialize)
- __coco_memo_state__ receives typed prev_state on subsequent runs
- State changed but reusable skips re-execution
"""

from dataclasses import dataclass
from typing import Any

import cocoindex as coco
import pytest

from tests import common
from tests.common.target_states import (
    DictDataWithPrev,
    GlobalDictTarget,
    Metrics,
)

coco_env = common.create_test_env(__file__)


# ============================================================================
# Memo roundtrip with dataclass return
# ============================================================================


@dataclass(frozen=True)
class Result:
    count: int
    label: str


@dataclass(frozen=True)
class SourceEntry:
    name: str
    version: int
    content: str

    def __coco_memo_key__(self) -> object:
        return (self.name, self.version)


_dc_source_data: dict[str, SourceEntry] = {}
_dc_metrics = Metrics()
_dc_returned_values: dict[str, Any] = {}


@coco.fn(memo=True)
def _transform_dc(entry: SourceEntry) -> Result:
    _dc_metrics.increment("call.transform_dc")
    return Result(count=len(entry.content), label=entry.content)


@coco.fn
def _process_dc() -> None:
    for key, value in _dc_source_data.items():
        result = _transform_dc(value)
        _dc_returned_values[key] = result
        coco.declare_target_state(
            GlobalDictTarget.target_state(key, f"{result.label}:{result.count}")
        )


def test_memo_roundtrip_dataclass_return() -> None:
    GlobalDictTarget.store.clear()
    _dc_source_data.clear()
    _dc_metrics.clear()
    _dc_returned_values.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_memo_roundtrip_dataclass_return", environment=coco_env
        ),
        _process_dc,
    )

    # Run 1: cache miss -- function executes
    _dc_source_data["A"] = SourceEntry(name="A", version=1, content="hello")
    app.update_blocking()
    assert _dc_metrics.collect() == {"call.transform_dc": 1}
    assert isinstance(_dc_returned_values["A"], Result)
    assert _dc_returned_values["A"] == Result(count=5, label="hello")
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="hello:5", prev=[], prev_may_be_missing=True),
    }

    # Run 2: same input (same memo key) -- cache hit, result deserialized
    _dc_returned_values.clear()
    app.update_blocking()
    assert _dc_metrics.collect() == {}  # function NOT called again
    assert isinstance(_dc_returned_values["A"], Result)
    assert _dc_returned_values["A"] == Result(count=5, label="hello")


# ============================================================================
# Memo roundtrip with pydantic return
# ============================================================================


_pydantic_source_data: dict[str, SourceEntry] = {}
_pydantic_metrics = Metrics()
_pydantic_returned_values: dict[str, Any] = {}


def test_memo_roundtrip_pydantic_return() -> None:
    pydantic = pytest.importorskip("pydantic")

    class PydanticResult(pydantic.BaseModel):  # type: ignore[name-defined,misc]
        score: float
        tag: str

    @coco.fn(memo=True)
    def _transform_pydantic(entry: SourceEntry) -> PydanticResult:
        _pydantic_metrics.increment("call.transform_pydantic")
        return PydanticResult(score=len(entry.content) * 1.5, tag=entry.content)

    @coco.fn
    def _process_pydantic() -> None:
        for key, value in _pydantic_source_data.items():
            result: Any = _transform_pydantic(value)
            _pydantic_returned_values[key] = result
            coco.declare_target_state(
                GlobalDictTarget.target_state(key, f"{result.tag}:{result.score}")  # type: ignore[attr-defined]
            )

    GlobalDictTarget.store.clear()
    _pydantic_source_data.clear()
    _pydantic_metrics.clear()
    _pydantic_returned_values.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_memo_roundtrip_pydantic_return", environment=coco_env
        ),
        _process_pydantic,
    )

    # Run 1: cache miss
    _pydantic_source_data["A"] = SourceEntry(name="A", version=1, content="world")
    app.update_blocking()
    assert _pydantic_metrics.collect() == {"call.transform_pydantic": 1}
    assert isinstance(_pydantic_returned_values["A"], PydanticResult)
    assert _pydantic_returned_values["A"].tag == "world"
    assert _pydantic_returned_values["A"].score == 7.5

    # Run 2: cache hit -- deserialized back to PydanticResult
    _pydantic_returned_values.clear()
    app.update_blocking()
    assert _pydantic_metrics.collect() == {}
    assert isinstance(_pydantic_returned_values["A"], PydanticResult)
    assert _pydantic_returned_values["A"].tag == "world"
    assert _pydantic_returned_values["A"].score == 7.5


# ============================================================================
# Memo roundtrip with no type hint (returns dict)
# ============================================================================


@dataclass(frozen=True)
class NohintSourceEntry:
    name: str
    version: int
    content: str

    def __coco_memo_key__(self) -> object:
        return (self.name, self.version)


_nohint_source_data: dict[str, NohintSourceEntry] = {}
_nohint_metrics = Metrics()
_nohint_returned_values: dict[str, Any] = {}


@coco.fn(memo=True)
def _transform_nohint(entry: NohintSourceEntry):  # type: ignore[no-untyped-def]
    """No return type annotation -- returns a dict."""
    _nohint_metrics.increment("call.transform_nohint")
    return {"text": entry.content, "length": len(entry.content)}


@coco.fn
def _process_nohint() -> None:
    for key, value in _nohint_source_data.items():
        result = _transform_nohint(value)
        _nohint_returned_values[key] = result
        coco.declare_target_state(
            GlobalDictTarget.target_state(key, f"{result['text']}:{result['length']}")
        )


def test_memo_roundtrip_no_type_hint() -> None:
    GlobalDictTarget.store.clear()
    _nohint_source_data.clear()
    _nohint_metrics.clear()
    _nohint_returned_values.clear()

    app = coco.App(
        coco.AppConfig(name="test_memo_roundtrip_no_type_hint", environment=coco_env),
        _process_nohint,
    )

    # Run 1: cache miss
    _nohint_source_data["A"] = NohintSourceEntry(name="A", version=1, content="abc")
    app.update_blocking()
    assert _nohint_metrics.collect() == {"call.transform_nohint": 1}
    assert _nohint_returned_values["A"]["text"] == "abc"
    assert _nohint_returned_values["A"]["length"] == 3

    # Run 2: cache hit -- values equivalent even without type info
    _nohint_returned_values.clear()
    app.update_blocking()
    assert _nohint_metrics.collect() == {}
    assert _nohint_returned_values["A"]["text"] == "abc"
    assert _nohint_returned_values["A"]["length"] == 3


# ============================================================================
# Cache hit -- no double deserialize (same args, same update)
# ============================================================================


@dataclass(frozen=True)
class DedupSourceEntry:
    name: str
    version: int
    content: str

    def __coco_memo_key__(self) -> object:
        return (self.name, self.version)


_dedup_source_data: dict[str, DedupSourceEntry] = {}
_dedup_metrics = Metrics()


@coco.fn(memo=True)
def _transform_dedup(entry: DedupSourceEntry) -> str:
    _dedup_metrics.increment("call.transform_dedup")
    return f"processed:{entry.content}"


@coco.fn
def _process_dedup() -> None:
    for key, value in _dedup_source_data.items():
        # Call the same memo function twice with the same entry in one update
        result1 = _transform_dedup(value)
        result2 = _transform_dedup(value)
        coco.declare_target_state(
            GlobalDictTarget.target_state(key, f"{result1}|{result2}")
        )


def test_memo_cache_hit_no_double_deserialize() -> None:
    GlobalDictTarget.store.clear()
    _dedup_source_data.clear()
    _dedup_metrics.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_memo_cache_hit_no_double_deserialize", environment=coco_env
        ),
        _process_dedup,
    )

    _dedup_source_data["A"] = DedupSourceEntry(name="A", version=1, content="x")
    app.update_blocking()
    # Function body should be called only once despite two calls with same args
    assert _dedup_metrics.collect() == {"call.transform_dedup": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="processed:x|processed:x", prev=[], prev_may_be_missing=True
        ),
    }


# ============================================================================
# Memo state typed deserialization
# ============================================================================


@dataclass(frozen=True)
class MTimeState:
    mtime: int


_state_source_data: dict[str, Any] = {}
_state_metrics = Metrics()
_state_received_prev_types: list[type] = []


@dataclass
class StateEntry:
    name: str
    mtime: int
    content: str

    def __coco_memo_key__(self) -> object:
        return self.name

    def __coco_memo_state__(self, prev_state: MTimeState) -> coco.MemoStateOutcome:
        _state_received_prev_types.append(type(prev_state))
        if coco.is_non_existence(prev_state):
            return coco.MemoStateOutcome(
                state=MTimeState(mtime=self.mtime), memo_valid=True
            )
        memo_valid = self.mtime == prev_state.mtime
        return coco.MemoStateOutcome(
            state=MTimeState(mtime=self.mtime), memo_valid=memo_valid
        )


@coco.fn(memo=True)
def _transform_state(entry: StateEntry) -> str:
    _state_metrics.increment("call.transform_state")
    return f"v:{entry.content}"


@coco.fn
def _process_state() -> None:
    for key, value in _state_source_data.items():
        result = _transform_state(value)
        coco.declare_target_state(GlobalDictTarget.target_state(key, result))


def test_memo_state_typed_deserialization() -> None:
    GlobalDictTarget.store.clear()
    _state_source_data.clear()
    _state_metrics.clear()
    _state_received_prev_types.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_memo_state_typed_deserialization", environment=coco_env
        ),
        _process_state,
    )

    # Run 1: no prev_state -- NonExistence
    _state_source_data["A"] = StateEntry(name="A", mtime=100, content="hello")
    app.update_blocking()
    assert _state_metrics.collect() == {"call.transform_state": 1}

    # Run 2: prev_state should be deserialized as MTimeState
    _state_received_prev_types.clear()
    app.update_blocking()
    assert _state_metrics.collect() == {}  # mtime unchanged => memo valid
    # The prev_state should be typed MTimeState (not a raw dict or tuple)
    assert any(t is MTimeState for t in _state_received_prev_types), (
        f"Expected MTimeState in received types, got {_state_received_prev_types}"
    )

    # Run 3: change mtime => memo invalid, re-execute
    _state_received_prev_types.clear()
    _state_source_data["A"] = StateEntry(name="A", mtime=200, content="world")
    app.update_blocking()
    assert _state_metrics.collect() == {"call.transform_state": 1}
    assert any(t is MTimeState for t in _state_received_prev_types)


# ============================================================================
# State changed but memo_valid=True skips re-execution
# ============================================================================


@dataclass
class ReuseEntry:
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
            return coco.MemoStateOutcome(state=new_state, memo_valid=True)
        # mtime changed -- check fingerprint
        return coco.MemoStateOutcome(
            state=new_state, memo_valid=self.fingerprint == prev_fp
        )


_reuse_source_data: dict[str, ReuseEntry] = {}
_reuse_metrics = Metrics()


@coco.fn(memo=True)
def _transform_reuse(entry: ReuseEntry) -> str:
    _reuse_metrics.increment("call.transform_reuse")
    return f"result:{entry.content}"


@coco.fn
def _process_reuse() -> None:
    for key, value in _reuse_source_data.items():
        result = _transform_reuse(value)
        coco.declare_target_state(GlobalDictTarget.target_state(key, result))


def test_memo_state_reuse_with_changed_states() -> None:
    GlobalDictTarget.store.clear()
    _reuse_source_data.clear()
    _reuse_metrics.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_memo_state_reuse_with_changed_states", environment=coco_env
        ),
        _process_reuse,
    )

    # Run 1: cache miss
    _reuse_source_data["X"] = ReuseEntry(
        name="X", mtime=1000, fingerprint="fp1", content="data"
    )
    app.update_blocking()
    assert _reuse_metrics.collect() == {"call.transform_reuse": 1}
    assert GlobalDictTarget.store.data == {
        "X": DictDataWithPrev(data="result:data", prev=[], prev_may_be_missing=True),
    }

    # Run 2: mtime changes but fingerprint same => memo_valid=True, no re-execute
    _reuse_source_data["X"] = ReuseEntry(
        name="X", mtime=2000, fingerprint="fp1", content="data"
    )
    app.update_blocking()
    assert _reuse_metrics.collect() == {}  # NOT re-executed

    # Run 3: verify updated state persisted (same mtime 2000 => still valid)
    app.update_blocking()
    assert _reuse_metrics.collect() == {}

    # Run 4: fingerprint changes => memo_valid=False, re-execute
    _reuse_source_data["X"] = ReuseEntry(
        name="X", mtime=3000, fingerprint="fp2", content="new_data"
    )
    app.update_blocking()
    assert _reuse_metrics.collect() == {"call.transform_reuse": 1}
    assert GlobalDictTarget.store.data == {
        "X": DictDataWithPrev(
            data="result:new_data",
            prev=["result:data"],
            prev_may_be_missing=False,
        ),
    }


# ============================================================================
# Forward reference return type (defined after @coco.fn decoration)
# ============================================================================


_fwd_source_data: dict[str, SourceEntry] = {}
_fwd_metrics = Metrics()
_fwd_returned_values: dict[str, Any] = {}


# Decorate BEFORE ForwardResult is defined — tests forward reference handling.
@coco.fn(memo=True)
def _transform_fwd(entry: SourceEntry) -> "ForwardResult":
    _fwd_metrics.increment("call.transform_fwd")
    return ForwardResult(value=entry.content.upper())


@dataclass(frozen=True)
class ForwardResult:
    value: str


@coco.fn
def _process_fwd() -> None:
    for key, value in _fwd_source_data.items():
        result = _transform_fwd(value)
        _fwd_returned_values[key] = result
        coco.declare_target_state(GlobalDictTarget.target_state(key, result.value))


def test_memo_roundtrip_forward_reference_return() -> None:
    """Return type is a forward reference (class defined after decoration).

    Run 1 executes the function. Run 2 should hit the cache and deserialize
    the result back to a proper ForwardResult instance (not a dict).
    """
    GlobalDictTarget.store.clear()
    _fwd_source_data.clear()
    _fwd_metrics.clear()
    _fwd_returned_values.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_memo_roundtrip_forward_reference_return", environment=coco_env
        ),
        _process_fwd,
    )

    # Run 1: cache miss
    _fwd_source_data["A"] = SourceEntry(name="A", version=1, content="hello")
    app.update_blocking()
    assert _fwd_metrics.collect() == {"call.transform_fwd": 1}
    assert isinstance(_fwd_returned_values["A"], ForwardResult)
    assert _fwd_returned_values["A"] == ForwardResult(value="HELLO")

    # Run 2: cache hit -- must deserialize to ForwardResult, not dict
    _fwd_returned_values.clear()
    app.update_blocking()
    assert _fwd_metrics.collect() == {}
    assert isinstance(_fwd_returned_values["A"], ForwardResult)
    assert _fwd_returned_values["A"] == ForwardResult(value="HELLO")


# ============================================================================
# Memo state with NonExistenceType in union (real FileLike via localfs)
# ============================================================================

import pathlib
from cocoindex.connectors import localfs

_filelike_metrics = Metrics()
_filelike_source_dir: pathlib.Path | None = None


@coco.fn(memo=True)
async def _transform_filelike(f: localfs.File) -> str:
    _filelike_metrics.increment("call.transform_filelike")
    return f"content:{await f.read_text()}"


@coco.fn
async def _process_filelike() -> None:
    assert _filelike_source_dir is not None
    walker = localfs.walk_dir(_filelike_source_dir)
    async for key, f in walker.items():
        result = await _transform_filelike(f)
        coco.declare_target_state(GlobalDictTarget.target_state(key, result))


def test_memo_state_filelike_non_existence_type(tmp_path: pathlib.Path) -> None:
    """FileLike.__coco_memo_state__ has ``tuple[datetime, bytes] | NonExistenceType``.

    Verifies that the NonExistenceType is stripped from the union before building
    the deserializer, and the stored state round-trips correctly on run 2.
    """
    global _filelike_source_dir
    GlobalDictTarget.store.clear()
    _filelike_metrics.clear()

    _filelike_source_dir = tmp_path
    (tmp_path / "a.txt").write_text("hello")

    app = coco.App(
        coco.AppConfig(
            name="test_memo_state_filelike_non_existence", environment=coco_env
        ),
        _process_filelike,
    )

    # Run 1: first run — function executes
    app.update_blocking()
    assert _filelike_metrics.collect() == {"call.transform_filelike": 1}
    assert GlobalDictTarget.store.data == {
        "a.txt": DictDataWithPrev(
            data="content:hello", prev=[], prev_may_be_missing=True
        ),
    }

    # Run 2: file unchanged — cache hit (state deserialized as tuple, not raw bytes)
    app.update_blocking()
    assert _filelike_metrics.collect() == {}  # no re-execution

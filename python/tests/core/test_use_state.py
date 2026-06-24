"""Integration tests for coco.use_state() — persistent per-component state."""

import dataclasses
from typing import NamedTuple

import msgspec
import pytest
import cocoindex as coco

from tests import common

coco_env = common.create_test_env(__file__)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_source_items: list[str] = []
_captured: dict[str, object] = {}


@coco.serialize_by_pickle
class _SerCounter:
    """Counts its own (de)serializations to assert when encoding happens.

    `__getstate__` runs on every pickle and `__setstate__` on every unpickle,
    so the counters reflect how many times the value was actually encoded to /
    decoded from bytes.
    """

    serialize_count = 0
    deserialize_count = 0

    def __init__(self, n: int) -> None:
        self.n = n

    def __getstate__(self) -> dict[str, int]:
        type(self).serialize_count += 1
        return {"n": self.n}

    def __setstate__(self, state: dict[str, int]) -> None:
        type(self).deserialize_count += 1
        self.n = state["n"]


@coco.fn
async def _root(items: list[str]) -> None:
    for item in items:
        await coco.mount(coco.component_subpath(item), _process_item, item)


@coco.fn
def _process_item(item: str) -> None:
    handle = coco.use_state("counter", 0)
    _captured[item] = handle.value
    handle.value = handle.value + 1


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def _make_app(name: str) -> coco.App:  # type: ignore[type-arg]
    return coco.App(
        coco.AppConfig(name=name, environment=coco_env),
        _root,
        items=_source_items,
    )


def test_use_state_returns_initial_on_first_run() -> None:
    _source_items.clear()
    _captured.clear()

    app = _make_app("use_state_initial")
    _source_items[:] = ["a"]
    app.update_blocking()

    assert _captured["a"] == 0


def test_use_state_persists_across_runs() -> None:
    _source_items.clear()
    _captured.clear()

    app = _make_app("use_state_persist")
    _source_items[:] = ["a"]

    app.update_blocking()
    assert _captured["a"] == 0  # initial

    app.update_blocking()
    assert _captured["a"] == 1  # stored from previous run

    app.update_blocking()
    assert _captured["a"] == 2  # stored from previous run


def test_use_state_independent_per_component() -> None:
    _source_items.clear()
    _captured.clear()

    app = _make_app("use_state_independent")
    _source_items[:] = ["x", "y"]

    app.update_blocking()
    assert _captured["x"] == 0
    assert _captured["y"] == 0

    app.update_blocking()
    assert _captured["x"] == 1
    assert _captured["y"] == 1


def test_use_state_resets_after_component_deleted() -> None:
    _source_items.clear()
    _captured.clear()

    app = _make_app("use_state_delete")
    _source_items[:] = ["a"]

    app.update_blocking()
    assert _captured["a"] == 0

    app.update_blocking()
    assert _captured["a"] == 1

    # Delete the component by removing "a" from source.
    _source_items.clear()
    app.update_blocking()

    # Re-add: state should have been cleaned up, initial_value returned.
    _source_items[:] = ["a"]
    app.update_blocking()
    assert _captured["a"] == 0


def test_use_state_defaults_to_none() -> None:
    _source_items.clear()
    _captured.clear()

    @coco.fn
    def _process_no_initial(item: str) -> None:
        handle = coco.use_state("flag")  # no initial_value
        _captured[item] = handle.value
        handle.value = "set"

    @coco.fn
    async def _root_no_initial(items: list[str]) -> None:
        for item in items:
            await coco.mount(coco.component_subpath(item), _process_no_initial, item)

    app = coco.App(  # type: ignore[type-arg]
        coco.AppConfig(name="use_state_no_initial", environment=coco_env),
        _root_no_initial,
        items=_source_items,
    )
    _source_items[:] = ["a"]
    app.update_blocking()
    assert _captured["a"] is None  # first run: no stored value → None

    app.update_blocking()
    assert _captured["a"] == "set"  # second run: stored value returned


def test_use_state_writes_serialize_once_at_flush() -> None:
    # Assigning state.value repeatedly must NOT serialize on each write.
    # Serialization is deferred to commit, so N writes in one run cost
    # exactly one serialization (of the final value).
    _source_items.clear()

    _seen: dict[str, object] = {}

    @coco.fn
    def _process_writes(item: str) -> None:
        s = coco.use_state("obj")  # initial None — does not touch the counter
        _seen[item] = s.value
        for i in range(5):
            s.value = _SerCounter(i)

    @coco.fn
    async def _root_writes(items: list[str]) -> None:
        for item in items:
            await coco.mount(coco.component_subpath(item), _process_writes, item)

    app = coco.App(  # type: ignore[type-arg]
        coco.AppConfig(name="use_state_write_serialize", environment=coco_env),
        _root_writes,
        items=_source_items,
    )
    _source_items[:] = ["a"]

    _SerCounter.serialize_count = 0
    app.update_blocking()
    assert _seen["a"] is None  # first run: no stored value
    assert _SerCounter.serialize_count == 1  # 5 writes → 1 serialize at commit

    _SerCounter.serialize_count = 0
    app.update_blocking()
    loaded = _seen["a"]
    assert isinstance(loaded, _SerCounter)
    assert loaded.n == 4  # persisted final value from the previous run
    assert _SerCounter.serialize_count == 1  # again, one serialize at commit


def test_use_state_initial_not_serialized_when_stored_value_exists() -> None:
    # Passing an initial_value to use_state must not serialize it when
    # a value is already stored for the key — the initial is discarded.
    _source_items.clear()

    _seen: dict[str, object] = {}

    @coco.fn
    def _process_initial(item: str) -> None:
        # A fresh initial object each run; from run 2 on it is discarded.
        s = coco.use_state("init_obj", _SerCounter(7))
        _seen[item] = s.value

    @coco.fn
    async def _root_initial(items: list[str]) -> None:
        for item in items:
            await coco.mount(coco.component_subpath(item), _process_initial, item)

    app = coco.App(  # type: ignore[type-arg]
        coco.AppConfig(name="use_state_initial_serialize", environment=coco_env),
        _root_initial,
        items=_source_items,
    )
    _source_items[:] = ["a"]

    app.update_blocking()  # first run: initial becomes the stored value

    _SerCounter.serialize_count = 0
    app.update_blocking()  # second run: a stored value already exists
    # The initial passed this run is dropped without being serialized.
    assert _SerCounter.serialize_count == 0
    loaded = _seen["a"]
    assert isinstance(loaded, _SerCounter)
    assert loaded.n == 7  # stored value wins, initial ignored


def test_use_state_first_run_reuses_initial_object_without_roundtrip() -> None:
    # On the first run the handle must return the very object passed as
    # initial_value — no serialize/deserialize round-trip. Serialization is
    # deferred to commit, so nothing is encoded while the component runs.
    _source_items.clear()

    sentinel = _SerCounter(123)
    _info: dict[str, object] = {}

    @coco.fn
    def _proc(item: str) -> None:
        s = coco.use_state("rt", sentinel)
        _info["is_same"] = s.value is sentinel
        _info["ser_during"] = _SerCounter.serialize_count
        _info["deser_during"] = _SerCounter.deserialize_count

    @coco.fn
    async def _root_rt(items: list[str]) -> None:
        for item in items:
            await coco.mount(coco.component_subpath(item), _proc, item)

    app = coco.App(  # type: ignore[type-arg]
        coco.AppConfig(name="use_state_roundtrip", environment=coco_env),
        _root_rt,
        items=_source_items,
    )
    _source_items[:] = ["a"]

    _SerCounter.serialize_count = 0
    _SerCounter.deserialize_count = 0
    app.update_blocking()

    assert _info["is_same"] is True  # same object — no round-trip
    assert _info["ser_during"] == 0  # nothing serialized while running
    assert _info["deser_during"] == 0  # nothing deserialized while running
    assert _SerCounter.serialize_count == 1  # serialized once, at commit


def test_use_state_reload_deserializes_lazily_once() -> None:
    # On a reload, a stored value is deserialized at most once, on first
    # .value access, and the decoded object is cached for repeated reads.
    _source_items.clear()

    _info: dict[str, int] = {}

    @coco.fn
    def _proc(item: str) -> None:
        s = coco.use_state("lazy", _SerCounter(5))
        before = _SerCounter.deserialize_count
        _ = s.value
        _ = s.value
        _ = s.value
        _info[item] = _SerCounter.deserialize_count - before

    @coco.fn
    async def _root_lazy(items: list[str]) -> None:
        for item in items:
            await coco.mount(coco.component_subpath(item), _proc, item)

    app = coco.App(  # type: ignore[type-arg]
        coco.AppConfig(name="use_state_lazy_deser", environment=coco_env),
        _root_lazy,
        items=_source_items,
    )
    _source_items[:] = ["a"]

    app.update_blocking()  # first run: stores the value
    app.update_blocking()  # second run: reload, read .value three times
    assert _info["a"] == 1  # deserialized exactly once despite three reads


def test_use_state_unserializable_value_errors_at_commit_with_key() -> None:
    # Serialization is deferred to commit, so a non-serializable state value
    # fails there (not at assignment). The failure must reach the exception
    # handler (i.e. not be silently dropped) and name the offending key.
    _source_items.clear()

    class _Unserializable:  # not registered for serialization
        pass

    captured: list[BaseException] = []

    @coco.fn
    def _process_bad(item: str) -> None:
        s = coco.use_state("bad_key")
        s.value = _Unserializable()  # no error here — deferred to commit

    @coco.fn
    async def _root_bad(items: list[str]) -> None:
        def handler(exc: BaseException, ctx: coco.ExceptionContext) -> None:
            captured.append(exc)

        async with coco.exception_handler(handler):
            for item in items:
                await coco.mount(coco.component_subpath(item), _process_bad, item)

    app = coco.App(  # type: ignore[type-arg]
        coco.AppConfig(name="use_state_unserializable", environment=coco_env),
        _root_bad,
        items=_source_items,
    )
    _source_items[:] = ["a"]
    app.update_blocking()

    assert len(captured) == 1
    assert "bad_key" in str(captured[0])  # error identifies the failing key


def test_use_state_raises_inside_memoized_function() -> None:
    # use_state is blocked inside an *inline* memoized call (not mounted as a
    # component). If the memo cache hits, the body is skipped — use_state
    # would never run and the key would be GC'd as "not declared". Guard it.
    _source_items.clear()
    _captured.clear()

    _raised: dict[str, bool] = {}

    @coco.fn(memo=True)
    def _memoized_helper(item: str) -> None:
        try:
            coco.use_state("counter", 0)
            _raised[item] = False
        except RuntimeError:
            _raised[item] = True

    @coco.fn
    def _component_fn(item: str) -> None:
        _memoized_helper(
            item
        )  # inline call — memo is function-level, not component-level

    @coco.fn
    async def _root_with_memo(items: list[str]) -> None:
        for item in items:
            await coco.mount(coco.component_subpath(item), _component_fn, item)

    app = coco.App(  # type: ignore[type-arg]
        coco.AppConfig(name="use_state_memo_guard", environment=coco_env),
        _root_with_memo,
        items=_source_items,
    )
    _source_items[:] = ["a"]
    app.update_blocking()
    assert _raised["a"] is True


def test_use_state_raises_inside_async_memoized_function() -> None:
    # Same guard as the sync case, but exercises the AsyncFunction path where
    # in_memo_fn is set via `guard is not None` rather than being hardcoded.
    _source_items.clear()
    _captured.clear()

    _raised: dict[str, bool] = {}

    @coco.fn(memo=True)
    async def _async_memoized_helper(item: str) -> None:
        try:
            coco.use_state("counter", 0)
            _raised[item] = False
        except RuntimeError:
            _raised[item] = True

    @coco.fn
    async def _component_fn(item: str) -> None:
        await _async_memoized_helper(item)  # inline call, not mounted

    @coco.fn
    async def _root_async_memo(items: list[str]) -> None:
        for item in items:
            await coco.mount(coco.component_subpath(item), _component_fn, item)

    app = coco.App(  # type: ignore[type-arg]
        coco.AppConfig(name="use_state_async_memo_guard", environment=coco_env),
        _root_async_memo,
        items=_source_items,
    )
    _source_items[:] = ["a"]
    app.update_blocking()
    assert _raised["a"] is True


def test_use_state_raises_inside_indirect_sync_memoized_function() -> None:
    # use_state is also blocked when called from a non-memoized function that
    # is itself called from within a memoized function body. The in_memo_fn
    # flag propagates transitively so the same GC risk applies.
    # Note: _memoized_outer must be called *inline* (not mounted) so it goes
    # through SyncFunction.__call__ and sets in_memo_fn=True.
    # The propagation logic is the same at every call depth, so one level is
    # sufficient to verify the mechanism.
    _source_items.clear()
    _captured.clear()

    _raised: dict[str, bool] = {}

    @coco.fn
    def _non_memoized_helper(item: str) -> None:
        try:
            coco.use_state("counter", 0)
            _raised[item] = False
        except RuntimeError:
            _raised[item] = True

    @coco.fn(memo=True)
    def _memoized_outer(item: str) -> None:
        _non_memoized_helper(item)  # indirect: use_state called via non-memoized child

    @coco.fn
    def _component_fn(item: str) -> None:
        _memoized_outer(item)  # inline memo call — not mounted

    @coco.fn
    async def _root_indirect_sync(items: list[str]) -> None:
        for item in items:
            await coco.mount(coco.component_subpath(item), _component_fn, item)

    app = coco.App(  # type: ignore[type-arg]
        coco.AppConfig(name="use_state_indirect_sync_memo_guard", environment=coco_env),
        _root_indirect_sync,
        items=_source_items,
    )
    _source_items[:] = ["a"]
    app.update_blocking()
    assert _raised["a"] is True


def test_use_state_raises_inside_indirect_async_memoized_function() -> None:
    # Same as above but exercises the AsyncFunction propagation path.
    # The propagation logic is the same at every call depth.
    _source_items.clear()
    _captured.clear()

    _raised: dict[str, bool] = {}

    @coco.fn
    async def _non_memoized_helper(item: str) -> None:
        try:
            coco.use_state("counter", 0)
            _raised[item] = False
        except RuntimeError:
            _raised[item] = True

    @coco.fn(memo=True)
    async def _memoized_outer(item: str) -> None:
        await _non_memoized_helper(item)

    @coco.fn
    async def _component_fn(item: str) -> None:
        await _memoized_outer(item)  # inline memo call — not mounted

    @coco.fn
    async def _root_indirect_async(items: list[str]) -> None:
        for item in items:
            await coco.mount(coco.component_subpath(item), _component_fn, item)

    app = coco.App(  # type: ignore[type-arg]
        coco.AppConfig(
            name="use_state_indirect_async_memo_guard", environment=coco_env
        ),
        _root_indirect_async,
        items=_source_items,
    )
    _source_items[:] = ["a"]
    app.update_blocking()
    assert _raised["a"] is True


def test_use_state_raises_on_duplicate_key() -> None:
    # UserStateCache rejects a second use_state() call with the same key
    # within the same component run.
    _source_items.clear()
    _captured.clear()

    _raised: dict[str, bool] = {}

    @coco.fn
    def _process_duplicate(item: str) -> None:
        coco.use_state("counter", 0)
        try:
            coco.use_state("counter", 0)  # same key — must raise
            _raised[item] = False
        except RuntimeError:
            _raised[item] = True

    @coco.fn
    async def _root_duplicate(items: list[str]) -> None:
        for item in items:
            await coco.mount(coco.component_subpath(item), _process_duplicate, item)

    app = coco.App(  # type: ignore[type-arg]
        coco.AppConfig(name="use_state_duplicate_key", environment=coco_env),
        _root_duplicate,
        items=_source_items,
    )
    _source_items[:] = ["a"]
    app.update_blocking()
    assert _raised["a"] is True


def test_use_state_accepts_non_string_stable_keys() -> None:
    # use_state accepts any StableKey, not just str. Distinct StableKey values
    # (including tuples, ints, and Symbols) address independent state slots and
    # persist across runs.
    _source_items.clear()
    _captured.clear()

    keys: list[coco.StableKey] = [42, ("ns", 1), coco.Symbol("sym"), b"raw"]

    @coco.fn
    def _process_multikey(item: str) -> None:
        snapshot: dict[coco.StableKey, object] = {}
        for k in keys:
            handle = coco.use_state(k, 0)
            snapshot[k] = handle.value
            handle.value = handle.value + 1
        _captured[item] = snapshot

    @coco.fn
    async def _root_multikey(items: list[str]) -> None:
        for item in items:
            await coco.mount(coco.component_subpath(item), _process_multikey, item)

    app = coco.App(  # type: ignore[type-arg]
        coco.AppConfig(name="use_state_non_string_keys", environment=coco_env),
        _root_multikey,
        items=_source_items,
    )
    _source_items[:] = ["a"]

    app.update_blocking()
    assert _captured["a"] == {k: 0 for k in keys}

    app.update_blocking()
    assert _captured["a"] == {k: 1 for k in keys}


def test_use_state_raises_inside_component_subpath_block() -> None:
    _source_items.clear()
    _captured.clear()

    _raised: dict[str, bool] = {}

    @coco.fn
    async def _root_with_subpath(items: list[str]) -> None:
        for item in items:
            with coco.component_subpath(item):
                try:
                    coco.use_state("counter", 0)
                    _raised[item] = False
                except RuntimeError:
                    _raised[item] = True

    app = coco.App(  # type: ignore[type-arg]
        coco.AppConfig(name="use_state_subpath_guard", environment=coco_env),
        _root_with_subpath,
        items=_source_items,
    )
    _source_items[:] = ["a"]
    app.update_blocking()
    assert _raised["a"] is True


@dataclasses.dataclass
class _Cursor:
    pos: int
    tag: str


def test_use_state_type_hint_deserializes_into_dataclass() -> None:
    _source_items.clear()
    _captured.clear()

    @coco.fn
    def _process(item: str) -> None:
        s = coco.use_state("cur", type_hint=_Cursor, initial_value=_Cursor(0, "init"))
        v = s.value
        assert isinstance(v, _Cursor), f"expected _Cursor, got {type(v)}"
        _captured[item] = v
        s.value = _Cursor(v.pos + 1, "next")

    @coco.fn
    async def _root_typed(items: list[str]) -> None:
        for item in items:
            await coco.mount(coco.component_subpath(item), _process, item)

    app = coco.App(  # type: ignore[type-arg]
        coco.AppConfig(name="use_state_type_hint", environment=coco_env),
        _root_typed,
        items=_source_items,
    )
    _source_items[:] = ["a"]

    app.update_blocking()
    loaded = _captured["a"]
    assert isinstance(loaded, _Cursor)
    assert (loaded.pos, loaded.tag) == (0, "init")

    app.update_blocking()
    loaded = _captured["a"]
    assert isinstance(loaded, _Cursor)
    assert (loaded.pos, loaded.tag) == (1, "next")


def test_use_state_type_hint_without_initial() -> None:
    _source_items.clear()
    _captured.clear()

    @coco.fn
    def _process(item: str) -> None:
        s = coco.use_state("cur", type_hint=_Cursor)
        v = s.value
        _captured[item] = v
        s.value = _Cursor(42, "set")

    @coco.fn
    async def _root_no_initial(items: list[str]) -> None:
        for item in items:
            await coco.mount(coco.component_subpath(item), _process, item)

    app = coco.App(  # type: ignore[type-arg]
        coco.AppConfig(name="use_state_type_hint_no_initial", environment=coco_env),
        _root_no_initial,
        items=_source_items,
    )
    _source_items[:] = ["a"]

    app.update_blocking()
    assert _captured["a"] is None

    app.update_blocking()
    loaded = _captured["a"]
    assert isinstance(loaded, _Cursor)
    assert (loaded.pos, loaded.tag) == (42, "set")


def test_use_state_type_hint_with_positional_none_initial() -> None:
    _source_items.clear()
    _captured.clear()

    @coco.fn
    def _process(item: str) -> None:
        # Exercises the overload: use_state(key, None, *, type_hint=...)
        s = coco.use_state("cur", None, type_hint=_Cursor)
        v = s.value
        _captured[item] = v
        s.value = _Cursor(99, "pos")

    @coco.fn
    async def _root(items: list[str]) -> None:
        for item in items:
            await coco.mount(coco.component_subpath(item), _process, item)

    app = coco.App(  # type: ignore[type-arg]
        coco.AppConfig(
            name="use_state_type_hint_positional_none", environment=coco_env
        ),
        _root,
        items=_source_items,
    )
    _source_items[:] = ["a"]

    app.update_blocking()
    assert _captured["a"] is None

    app.update_blocking()
    loaded = _captured["a"]
    assert isinstance(loaded, _Cursor)
    assert (loaded.pos, loaded.tag) == (99, "pos")


class _Point(NamedTuple):
    x: int
    y: int


def test_use_state_type_hint_deserializes_into_namedtuple() -> None:
    _source_items.clear()
    _captured.clear()

    @coco.fn
    def _process(item: str) -> None:
        s = coco.use_state("pt", type_hint=_Point, initial_value=_Point(1, 2))
        v = s.value
        assert isinstance(v, _Point), f"expected _Point, got {type(v)}"
        _captured[item] = v
        s.value = _Point(v.x + 1, v.y + 1)

    @coco.fn
    async def _root_typed(items: list[str]) -> None:
        for item in items:
            await coco.mount(coco.component_subpath(item), _process, item)

    app = coco.App(  # type: ignore[type-arg]
        coco.AppConfig(name="use_state_type_hint_namedtuple", environment=coco_env),
        _root_typed,
        items=_source_items,
    )
    _source_items[:] = ["a"]

    app.update_blocking()
    loaded = _captured["a"]
    assert isinstance(loaded, _Point)
    assert loaded == _Point(1, 2)

    app.update_blocking()
    loaded = _captured["a"]
    assert isinstance(loaded, _Point)
    assert loaded == _Point(2, 3)


class _MsgSpecPoint(msgspec.Struct):
    x: int
    y: int


def test_use_state_type_hint_deserializes_into_msgspec_struct() -> None:
    _source_items.clear()
    _captured.clear()

    @coco.fn
    def _process(item: str) -> None:
        s = coco.use_state(
            "pt", type_hint=_MsgSpecPoint, initial_value=_MsgSpecPoint(1, 2)
        )
        v = s.value
        assert isinstance(v, _MsgSpecPoint), f"expected _MsgSpecPoint, got {type(v)}"
        _captured[item] = v
        s.value = _MsgSpecPoint(v.x + 1, v.y + 1)

    @coco.fn
    async def _root_typed(items: list[str]) -> None:
        for item in items:
            await coco.mount(coco.component_subpath(item), _process, item)

    app = coco.App(  # type: ignore[type-arg]
        coco.AppConfig(name="use_state_type_hint_msgspec", environment=coco_env),
        _root_typed,
        items=_source_items,
    )
    _source_items[:] = ["a"]

    app.update_blocking()
    loaded = _captured["a"]
    assert isinstance(loaded, _MsgSpecPoint)
    assert (loaded.x, loaded.y) == (1, 2)

    app.update_blocking()
    loaded = _captured["a"]
    assert isinstance(loaded, _MsgSpecPoint)
    assert (loaded.x, loaded.y) == (2, 3)


def test_use_state_type_hint_deserializes_into_pydantic_model() -> None:
    pydantic = pytest.importorskip("pydantic")

    class _PydanticPoint(pydantic.BaseModel):  # type: ignore[name-defined, misc]
        x: int
        y: int

    _source_items.clear()
    _captured.clear()

    @coco.fn
    def _process(item: str) -> None:
        s = coco.use_state(
            "pt", type_hint=_PydanticPoint, initial_value=_PydanticPoint(x=1, y=2)
        )
        v = s.value
        assert isinstance(v, _PydanticPoint), f"expected _PydanticPoint, got {type(v)}"
        _captured[item] = v
        s.value = _PydanticPoint(x=v.x + 1, y=v.y + 1)

    @coco.fn
    async def _root_typed(items: list[str]) -> None:
        for item in items:
            await coco.mount(coco.component_subpath(item), _process, item)

    app = coco.App(  # type: ignore[type-arg]
        coco.AppConfig(name="use_state_type_hint_pydantic", environment=coco_env),
        _root_typed,
        items=_source_items,
    )
    _source_items[:] = ["a"]

    app.update_blocking()
    loaded = _captured["a"]
    assert isinstance(loaded, _PydanticPoint)
    assert (loaded.x, loaded.y) == (1, 2)

    app.update_blocking()
    loaded = _captured["a"]
    assert isinstance(loaded, _PydanticPoint)
    assert (loaded.x, loaded.y) == (2, 3)


def test_use_state_type_hint_mismatch_raises_deserialization_error() -> None:
    _source_items.clear()
    _captured.clear()

    captured: list[BaseException] = []
    _store_mode: list[bool] = [True]  # mutable flag to switch behavior between runs

    @coco.fn
    def _process_store_int(item: str) -> None:
        # Store an int without any type hint.
        s = coco.use_state("cur", 123)
        _captured[item] = s.value

    @coco.fn
    def _process_load_as_cursor(item: str) -> None:
        # On the next run, try to deserialize the stored int as a _Cursor.
        s = coco.use_state("cur", type_hint=_Cursor)
        _captured[item] = s.value  # should raise DeserializationError

    @coco.fn
    async def _root_mismatch(items: list[str]) -> None:
        def handler(exc: BaseException, ctx: coco.ExceptionContext) -> None:
            captured.append(exc)

        async with coco.exception_handler(handler):
            for item in items:
                if _store_mode[0]:
                    await coco.mount(
                        coco.component_subpath(item), _process_store_int, item
                    )
                else:
                    await coco.mount(
                        coco.component_subpath(item), _process_load_as_cursor, item
                    )

    app = coco.App(  # type: ignore[type-arg]
        coco.AppConfig(name="use_state_type_hint_mismatch", environment=coco_env),
        _root_mismatch,
        items=_source_items,
    )
    _source_items[:] = ["a"]

    # First run: store the int.
    app.update_blocking()
    assert _captured["a"] == 123

    # Second run: attempt to load as _Cursor; should fail with DeserializationError.
    _store_mode[0] = False
    _captured.clear()
    captured.clear()
    app.update_blocking()
    assert len(captured) == 1
    exc_text = str(captured[0])
    assert "DeserializationError" in exc_text
    assert "use_state key 'cur'" in exc_text

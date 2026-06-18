"""Integration tests for coco.use_state() — persistent per-component state."""

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
    """Counts its own serializations, to prove writes don't serialize eagerly.

    `__getstate__` runs on every pickle, so `serialize_count` reflects how many
    times the value was actually encoded to bytes.
    """

    serialize_count = 0

    def __init__(self, n: int) -> None:
        self.n = n

    def __getstate__(self) -> dict[str, int]:
        type(self).serialize_count += 1
        return {"n": self.n}

    def __setstate__(self, state: dict[str, int]) -> None:
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

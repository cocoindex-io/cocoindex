"""End-to-end tests for change-detected context value state validation.

Tests the interaction between:
- `context_detect_change_key` (detect_change=True context keys that participate in memoization)
- `memo_validation` (`__coco_memo_state__` state functions that decide memo reuse)

When a change-detected context value (or an object reachable inside its canonical form)
exposes `__coco_memo_state__`, it should behave like an argument-borne state
function — on memo hit, the state function is called with the previously stored
state to decide whether the cached result is still valid.

State functions for context values live in `ContextProvider._context_state_fns`
(Python-side), keyed by the value's fingerprint. Memo entries persist the
captured states in the Rust core as an opaque blob
(`context_memo_states: Vec<(Fingerprint, Vec<MemoizedValue>)>`), which the
Python layer pairs with the env registry to validate.

Each test creates one :class:`coco.Environment` and simulates cross-run state
changes by re-providing the change-detected key on the same provider before calling
``app.update_blocking()`` again. Creating fresh environments back-to-back at
the same ``db_path`` is not supported by LMDB (it refuses to reopen while the
previous env is still live), so the single-env-with-reprovide pattern is the
correct way to exercise multi-run behavior.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import cocoindex as coco

from tests.common.environment import get_env_db_path
from tests.common.target_states import GlobalDictTarget, Metrics


# ============================================================================
# Test fixtures
# ============================================================================


@dataclass
class StatefulEmbedder:
    """Stand-in for an embedder whose validity is change-detected via a state value.

    Fingerprint (memo key) is based on `name` only — two embedders with the
    same name but different `state_value` hash the same.  The state function
    uses `state_value` to decide whether cached results are still valid.
    """

    name: str
    state_value: int

    def __coco_memo_key__(self) -> object:
        return self.name

    def __coco_memo_state__(self, prev_state: Any) -> coco.MemoStateOutcome:
        memo_valid = (
            not coco.is_non_existence(prev_state) and prev_state == self.state_value
        )
        return coco.MemoStateOutcome(state=self.state_value, memo_valid=memo_valid)


# One context key reused by multiple tests. Each test uses its own env to keep
# the registry state isolated.
EMBEDDER_KEY = coco.ContextKey[StatefulEmbedder](
    "_test_ctx_tracked_embedder", detect_change=True
)


def _make_env(db_name: str, embedder: StatefulEmbedder) -> coco.Environment:
    ctx = coco.ContextProvider()
    ctx.provide(EMBEDDER_KEY, embedder)
    settings = coco.Settings.from_env(db_path=get_env_db_path(db_name))
    return coco.Environment(settings, context_provider=ctx)


# ============================================================================
# Test 1: function-level memo — change-detected context value state drives invalidation
# ============================================================================

_metrics_fn = Metrics()
_source_fn: dict[str, str] = {}


@coco.fn(memo=True)
def _embed_text(key: str) -> str:
    _metrics_fn.increment("embed")
    embedder = coco.use_context(EMBEDDER_KEY)
    return f"{embedder.name}({embedder.state_value}):{key}"


@coco.fn
def _run_embed() -> None:
    for key in _source_fn:
        result = _embed_text(key)
        coco.declare_target_state(GlobalDictTarget.target_state(key, result))


def test_detect_change_context_state_validation_function_level() -> None:
    """Memoized function consuming a change-detected context value with state function.

    Scenarios (same env, re-provide between runs):
    1. Cache miss → function executes; initial state collected.
    2. Same state → memo valid, no re-execution.
    3. State changes (memo_valid=False) → function re-executes.
    4. State stable at the new value → memo valid again, 0 calls.
    """
    GlobalDictTarget.store.clear()
    _source_fn.clear()
    _metrics_fn.clear()

    _source_fn["a"] = "hello"
    _source_fn["b"] = "world"
    env = _make_env("test_ctx_tracked_state_fn", StatefulEmbedder("e1", 100))
    app = coco.App(
        coco.AppConfig(name="test_ctx_tracked_state_fn", environment=env), _run_embed
    )

    # Run 1: cache miss, both execute
    app.update_blocking()
    assert _metrics_fn.collect() == {"embed": 2}

    # Run 2: same embedder, same state → 0 executions
    env.context_provider.provide(EMBEDDER_KEY, StatefulEmbedder("e1", 100))
    app.update_blocking()
    assert _metrics_fn.collect() == {}

    # Run 3: same name (same fingerprint), state_value bumped
    #        → __coco_memo_state__ returns memo_valid=False → re-execute.
    env.context_provider.provide(EMBEDDER_KEY, StatefulEmbedder("e1", 101))
    app.update_blocking()
    assert _metrics_fn.collect() == {"embed": 2}

    # Run 4: same new state → memo valid again, 0 executions.
    env.context_provider.provide(EMBEDDER_KEY, StatefulEmbedder("e1", 101))
    app.update_blocking()
    assert _metrics_fn.collect() == {}


# ============================================================================
# Test 2: state unchanged but memo_valid=True with updated state stored
# ============================================================================


@dataclass
class TwoLevelStatefulEmbedder:
    """Embedder whose state is (generation, content_hash).

    The state fn reports memo_valid=True as long as content_hash is unchanged,
    even if generation differs — i.e., generation changes force state refresh
    but not re-execution. This mirrors the mtime/content_hash pattern.
    """

    name: str
    generation: int
    content_hash: int

    def __coco_memo_key__(self) -> object:
        return self.name

    def __coco_memo_state__(self, prev_state: Any) -> coco.MemoStateOutcome:
        new_state = (self.generation, self.content_hash)
        if coco.is_non_existence(prev_state):
            return coco.MemoStateOutcome(state=new_state, memo_valid=False)
        _, prev_hash = prev_state
        memo_valid = prev_hash == self.content_hash
        return coco.MemoStateOutcome(state=new_state, memo_valid=memo_valid)


TWO_LEVEL_KEY = coco.ContextKey[TwoLevelStatefulEmbedder](
    "_test_ctx_tracked_two_level_embedder", detect_change=True
)

_metrics_two_level = Metrics()


@coco.fn(memo=True)
def _embed_two_level(key: str) -> str:
    _metrics_two_level.increment("embed2")
    emb = coco.use_context(TWO_LEVEL_KEY)
    return f"{emb.name}:{key}:{emb.content_hash}"


@coco.fn
def _run_embed_two_level() -> None:
    _embed_two_level("k1")


def test_detect_change_context_state_valid_with_updated_state() -> None:
    """memo_valid=True with state changes → cached result reused, state refreshed."""
    GlobalDictTarget.store.clear()
    _metrics_two_level.clear()

    ctx = coco.ContextProvider()
    ctx.provide(
        TWO_LEVEL_KEY, TwoLevelStatefulEmbedder("x", generation=1, content_hash=10)
    )
    settings = coco.Settings.from_env(
        db_path=get_env_db_path("test_ctx_tracked_two_level")
    )
    env = coco.Environment(settings, context_provider=ctx)
    app = coco.App(
        coco.AppConfig(name="test_ctx_tracked_two_level", environment=env),
        _run_embed_two_level,
    )

    # Run 1: cache miss (no prev state).
    app.update_blocking()
    assert _metrics_two_level.collect() == {"embed2": 1}

    # Run 2: same generation/hash → memo valid, no re-execute.
    env.context_provider.provide(
        TWO_LEVEL_KEY, TwoLevelStatefulEmbedder("x", generation=1, content_hash=10)
    )
    app.update_blocking()
    assert _metrics_two_level.collect() == {}

    # Run 3: generation bump, content_hash unchanged → state fn returns
    #        memo_valid=True with updated state. Function should NOT re-execute,
    #        new state should be persisted.
    env.context_provider.provide(
        TWO_LEVEL_KEY, TwoLevelStatefulEmbedder("x", generation=2, content_hash=10)
    )
    app.update_blocking()
    assert _metrics_two_level.collect() == {}

    # Run 4: another generation bump, hash still the same → still valid, 0 calls.
    env.context_provider.provide(
        TWO_LEVEL_KEY, TwoLevelStatefulEmbedder("x", generation=3, content_hash=10)
    )
    app.update_blocking()
    assert _metrics_two_level.collect() == {}

    # Run 5: content_hash changes → memo_valid=False → re-execute.
    env.context_provider.provide(
        TWO_LEVEL_KEY, TwoLevelStatefulEmbedder("x", generation=4, content_hash=11)
    )
    app.update_blocking()
    assert _metrics_two_level.collect() == {"embed2": 1}


# ============================================================================
# Test 3: change-detected context value replaced with different canonical form
# ============================================================================

REPLACE_KEY = coco.ContextKey[StatefulEmbedder](
    "_test_ctx_tracked_replace_key", detect_change=True
)

_metrics_replace = Metrics()


@coco.fn(memo=True)
def _embed_replace(key: str) -> str:
    _metrics_replace.increment("embed3")
    emb = coco.use_context(REPLACE_KEY)
    return f"{emb.name}:{key}"


@coco.fn
def _run_embed_replace() -> None:
    _embed_replace("k1")


def test_detect_change_context_value_replaced() -> None:
    """Replacing the change-detected value with a different canonical form invalidates."""
    GlobalDictTarget.store.clear()
    _metrics_replace.clear()

    ctx = coco.ContextProvider()
    ctx.provide(REPLACE_KEY, StatefulEmbedder("e_old", 100))
    settings = coco.Settings.from_env(
        db_path=get_env_db_path("test_ctx_tracked_replaced")
    )
    env = coco.Environment(settings, context_provider=ctx)
    app = coco.App(
        coco.AppConfig(name="test_ctx_tracked_replaced", environment=env),
        _run_embed_replace,
    )

    app.update_blocking()
    assert _metrics_replace.collect() == {"embed3": 1}

    # Same everything → no re-execute.
    env.context_provider.provide(REPLACE_KEY, StatefulEmbedder("e_old", 100))
    app.update_blocking()
    assert _metrics_replace.collect() == {}

    # Different name → different fingerprint → invalidation via env.logic_set.
    env.context_provider.provide(REPLACE_KEY, StatefulEmbedder("e_new", 100))
    app.update_blocking()
    assert _metrics_replace.collect() == {"embed3": 1}


# ============================================================================
# Test 4: composite change-detected value (tuple) with nested state-bearing object
# ============================================================================


@dataclass
class _Inner:
    name: str
    state_value: int

    def __coco_memo_key__(self) -> object:
        return self.name

    def __coco_memo_state__(self, prev_state: Any) -> coco.MemoStateOutcome:
        memo_valid = (
            not coco.is_non_existence(prev_state) and prev_state == self.state_value
        )
        return coco.MemoStateOutcome(state=self.state_value, memo_valid=memo_valid)


COMPOSITE_KEY = coco.ContextKey[tuple[str, _Inner]](
    "_test_ctx_tracked_composite", detect_change=True
)

_metrics_composite = Metrics()


@coco.fn(memo=True)
def _embed_composite(key: str) -> str:
    _metrics_composite.increment("embed4")
    label, _ = coco.use_context(COMPOSITE_KEY)
    return f"{label}:{key}"


@coco.fn
def _run_embed_composite() -> None:
    _embed_composite("k1")


def test_detect_change_context_composite_state() -> None:
    """A composite change-detected value (tuple) whose element has state function.

    Verifies state fns are discovered via canonicalization walking the composite,
    not only by looking at the top-level value.
    """
    _metrics_composite.clear()

    ctx = coco.ContextProvider()
    ctx.provide(COMPOSITE_KEY, ("lbl", _Inner(name="i", state_value=1)))
    settings = coco.Settings.from_env(
        db_path=get_env_db_path("test_ctx_tracked_composite")
    )
    env = coco.Environment(settings, context_provider=ctx)
    app = coco.App(
        coco.AppConfig(name="test_ctx_tracked_composite", environment=env),
        _run_embed_composite,
    )

    app.update_blocking()
    assert _metrics_composite.collect() == {"embed4": 1}

    # Same inner state → memo valid.
    env.context_provider.provide(
        COMPOSITE_KEY, ("lbl", _Inner(name="i", state_value=1))
    )
    app.update_blocking()
    assert _metrics_composite.collect() == {}

    # Inner state bumped → state fn returns memo_valid=False → re-execute.
    env.context_provider.provide(
        COMPOSITE_KEY, ("lbl", _Inner(name="i", state_value=2))
    )
    app.update_blocking()
    assert _metrics_composite.collect() == {"embed4": 1}


# ============================================================================
# Test 5: component-level memoization (mounted processor)
# ============================================================================

COMP_KEY = coco.ContextKey[StatefulEmbedder](
    "_test_ctx_tracked_component_key", detect_change=True
)

_metrics_comp = Metrics()
_source_comp: dict[str, str] = {}


@coco.fn(memo=True)
def _embed_component(item: str) -> None:
    _metrics_comp.increment("comp")
    emb = coco.use_context(COMP_KEY)
    coco.declare_target_state(
        GlobalDictTarget.target_state(item, f"{emb.name}({emb.state_value}):{item}")
    )


@coco.fn
async def _mount_embed() -> None:
    for item in _source_comp:
        await coco.mount(coco.component_subpath(item), _embed_component, item)


def test_detect_change_context_state_validation_component_level() -> None:
    """Same invariants, but the memoized unit is mounted as a component."""
    GlobalDictTarget.store.clear()
    _source_comp.clear()
    _metrics_comp.clear()
    _source_comp["a"] = ""
    _source_comp["b"] = ""

    ctx = coco.ContextProvider()
    ctx.provide(COMP_KEY, StatefulEmbedder("ec", 10))
    settings = coco.Settings.from_env(
        db_path=get_env_db_path("test_ctx_tracked_state_comp")
    )
    env = coco.Environment(settings, context_provider=ctx)
    app = coco.App(
        coco.AppConfig(name="test_ctx_tracked_state_comp", environment=env),
        _mount_embed,
    )

    app.update_blocking()
    assert _metrics_comp.collect() == {"comp": 2}

    env.context_provider.provide(COMP_KEY, StatefulEmbedder("ec", 10))
    app.update_blocking()
    assert _metrics_comp.collect() == {}

    env.context_provider.provide(COMP_KEY, StatefulEmbedder("ec", 11))
    app.update_blocking()
    assert _metrics_comp.collect() == {"comp": 2}


# ============================================================================
# Test 6: cache-hit-not-reusable path must refresh context fps from fn_ctx
# ============================================================================
#
# Regression test: when a memoized function's state fn says "invalidate", the
# re-execution may observe a different set of change-detected context fps than was in
# the cached entry. The engine must persist context states derived from the
# fresh fn_ctx, not the stale set from cache-hit validation — otherwise, a new
# fp's state change won't be detected on subsequent runs.
#
# We exercise this with a function whose body branches on a module-level flag
# (invisible to the memo key) and conditionally consumes a second change-detected
# context value.

KEY_A = coco.ContextKey[StatefulEmbedder](
    "_test_ctx_tracked_branching_a", detect_change=True
)
KEY_B = coco.ContextKey[StatefulEmbedder](
    "_test_ctx_tracked_branching_b", detect_change=True
)

_use_both_keys = False
_metrics_branch = Metrics()


@coco.fn(memo=True)
def _branching_fn(k: str) -> str:
    _metrics_branch.increment("branch")
    a = coco.use_context(KEY_A)
    parts = [f"{a.name}({a.state_value})"]
    if _use_both_keys:
        b = coco.use_context(KEY_B)
        parts.append(f"{b.name}({b.state_value})")
    return f"{'|'.join(parts)}:{k}"


@coco.fn
def _run_branching() -> None:
    _branching_fn("x")


def test_cache_hit_not_reusable_refreshes_context_fps() -> None:
    """Re-execution must capture newly-observed change-detected context fps.

    - Run 1: flag off. Cache miss, persist only fp_A.
    - Run 2: flag on, bump A's state so validation invalidates the memo.
             Re-execution observes both fp_A and fp_B. The new entry must
             persist both fps' states.
    - Run 3: same flag, same A's state, but bump B's state. If Run 2 correctly
             stored fp_B, Run 3's validation should invalidate via fp_B → re-
             execute. If Run 2 dropped fp_B (the bug), Run 3 would not detect
             the change and silently reuse.
    """
    global _use_both_keys
    _metrics_branch.clear()

    ctx = coco.ContextProvider()
    ctx.provide(KEY_A, StatefulEmbedder("a", 1))
    ctx.provide(KEY_B, StatefulEmbedder("b", 10))
    settings = coco.Settings.from_env(
        db_path=get_env_db_path("test_ctx_tracked_branching")
    )
    env = coco.Environment(settings, context_provider=ctx)
    app = coco.App(
        coco.AppConfig(name="test_ctx_tracked_branching", environment=env),
        _run_branching,
    )

    try:
        _use_both_keys = False
        app.update_blocking()
        assert _metrics_branch.collect() == {"branch": 1}

        # Flip the flag and bump A's state so cache validation invalidates.
        _use_both_keys = True
        env.context_provider.provide(KEY_A, StatefulEmbedder("a", 2))  # changed
        env.context_provider.provide(KEY_B, StatefulEmbedder("b", 10))
        app.update_blocking()
        assert _metrics_branch.collect() == {"branch": 1}

        # Now bump B's state but keep A steady. If Run 2 stored fp_B, this
        # should invalidate via B and re-execute. If the bug is present
        # (fp_B was dropped), Run 3 would silently reuse Run 2's cached value.
        env.context_provider.provide(KEY_A, StatefulEmbedder("a", 2))
        env.context_provider.provide(KEY_B, StatefulEmbedder("b", 11))  # changed
        app.update_blocking()
        assert _metrics_branch.collect() == {"branch": 1}, (
            "Re-execution should have captured fp_B in Run 2; bumping B's "
            "state in Run 3 must invalidate the memo."
        )
    finally:
        _use_both_keys = False

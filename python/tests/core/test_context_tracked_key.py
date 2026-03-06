"""Tests for tracked context key memo invalidation."""

import gc
import threading
from typing import Any

import pytest

import cocoindex as coco
from tests.common.environment import get_env_db_path
from tests.common.target_states import GlobalDictTarget, Metrics


# Unique context keys for this test module (globally unique strings required).
_TRACKED_KEY = coco.ContextKey[str]("_test_ctx_tracked_d3")
_UNTRACKED_KEY = coco.ContextKey[str]("_test_ctx_untracked_d3", tracked=False)
_TRACKED_TRANSITIVE_KEY = coco.ContextKey[str]("_test_ctx_tracked_transitive_d3")


def _create_env(
    db_name: str, key: coco.ContextKey[str], value: str
) -> coco.Environment:
    """Create an Environment with a single provided context value."""
    ctx = coco.ContextProvider()
    ctx.provide(key, value)
    settings = coco.Settings.from_env(db_path=get_env_db_path(db_name))
    return coco.Environment(settings, context_provider=ctx)


def _run_app(
    db_name: str,
    key: coco.ContextKey[str],
    value: str,
    app_main: Any,
    metrics: Metrics,
) -> list[dict[str, int]]:
    """Create an env+app, run update twice, return metrics from each run.

    The env and app go out of scope on return, allowing LMDB to be reopened.
    """
    env = _create_env(db_name, key, value)
    app = coco.App(coco.AppConfig(name=db_name, environment=env), app_main)
    app.update_blocking()
    m1 = metrics.collect()
    app.update_blocking()
    m2 = metrics.collect()
    return [m1, m2]


# ============================================================================
# Test 1: Tracked key invalidates memo
# ============================================================================


def test_tracked_key_invalidates_memo() -> None:
    """Memo is invalidated when a tracked context key's value changes."""
    GlobalDictTarget.store.clear()
    metrics = Metrics()

    db_name = "test_ctx_tracked_inv"

    @coco.fn(memo=True)
    def process(name: str, content: str) -> None:
        val = coco.use_context(_TRACKED_KEY)
        metrics.increment("calls")
        coco.declare_target_state(
            GlobalDictTarget.target_state(name, f"{val}:{content}")
        )

    @coco.fn
    async def app_main() -> None:
        await coco.mount(coco.component_subpath("A"), process, "A", "data")

    # Phase 1: value="v1" — executes then memo hit
    m = _run_app(db_name, _TRACKED_KEY, "v1", app_main, metrics)
    assert m[0] == {"calls": 1}
    assert m[1] == {}
    assert GlobalDictTarget.store.data["A"].data == "v1:data"
    gc.collect()

    # Phase 2: value="v2" — tracked key changed, memo invalidated, then memo hit
    m = _run_app(db_name, _TRACKED_KEY, "v2", app_main, metrics)
    assert m[0] == {"calls": 1}
    assert m[1] == {}
    assert GlobalDictTarget.store.data["A"].data == "v2:data"


# ============================================================================
# Test 2: Untracked key does NOT invalidate memo
# ============================================================================


def test_untracked_key_no_invalidation() -> None:
    """Memo is NOT invalidated when an untracked context key's value changes."""
    GlobalDictTarget.store.clear()
    metrics = Metrics()

    db_name = "test_ctx_untracked_no_inv"

    @coco.fn(memo=True)
    def process(name: str, content: str) -> None:
        val = coco.use_context(_UNTRACKED_KEY)
        metrics.increment("calls")
        coco.declare_target_state(
            GlobalDictTarget.target_state(name, f"{val}:{content}")
        )

    @coco.fn
    async def app_main() -> None:
        await coco.mount(coco.component_subpath("A"), process, "A", "data")

    # Phase 1: value="v1" — executes then memo hit
    m = _run_app(db_name, _UNTRACKED_KEY, "v1", app_main, metrics)
    assert m[0] == {"calls": 1}
    assert m[1] == {}
    assert GlobalDictTarget.store.data["A"].data == "v1:data"
    gc.collect()

    # Phase 2: value="v2" — untracked key changed, memo NOT invalidated
    m = _run_app(db_name, _UNTRACKED_KEY, "v2", app_main, metrics)
    assert m[0] == {}  # memo hit — untracked key doesn't affect logic_deps
    assert m[1] == {}
    # Target state still has old value since memo was reused
    assert GlobalDictTarget.store.data["A"].data == "v1:data"


# ============================================================================
# Test 3: Transitive tracking through call chain
# ============================================================================


def test_tracked_key_transitive_invalidation() -> None:
    """Tracked key change invalidates memo transitively through call chain.

    foo (memoized) calls bar (non-memoized). bar calls use_context(tracked_key).
    When the tracked key's value changes, foo's memo is invalidated.
    """
    GlobalDictTarget.store.clear()
    metrics = Metrics()

    db_name = "test_ctx_tracked_transitive"

    @coco.fn
    def bar(name: str) -> str:
        val = coco.use_context(_TRACKED_TRANSITIVE_KEY)
        metrics.increment("bar")
        return f"{val}:{name}"

    @coco.fn(memo=True)
    def foo(name: str) -> None:
        result = bar(name)
        metrics.increment("foo")
        coco.declare_target_state(GlobalDictTarget.target_state(name, result))

    @coco.fn
    async def app_main() -> None:
        await coco.mount(coco.component_subpath("A"), foo, "A")

    # Phase 1: value="v1" — both execute, then memo hit
    m = _run_app(db_name, _TRACKED_TRANSITIVE_KEY, "v1", app_main, metrics)
    assert m[0] == {"foo": 1, "bar": 1}
    assert m[1] == {}
    assert GlobalDictTarget.store.data["A"].data == "v1:A"
    gc.collect()

    # Phase 2: value="v2" — tracked key changed, foo's memo invalidated transitively
    m = _run_app(db_name, _TRACKED_TRANSITIVE_KEY, "v2", app_main, metrics)
    assert m[0] == {"foo": 1, "bar": 1}
    assert m[1] == {}
    assert GlobalDictTarget.store.data["A"].data == "v2:A"


# ============================================================================
# Test 4: TypeError on unfingerprintable value for tracked key
# ============================================================================


def test_tracked_key_unfingerprintable_value_raises() -> None:
    """Providing an unfingerprintable value for a tracked key raises TypeError."""
    key = coco.ContextKey[object]("_test_ctx_unfingerprintable_d3")
    ctx = coco.ContextProvider()

    # threading.Lock is not picklable, so it can't be fingerprinted
    with pytest.raises(TypeError):
        ctx.provide(key, threading.Lock())

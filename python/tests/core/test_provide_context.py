import gc
from typing import Any

import pytest

import cocoindex as coco
from tests.common.environment import get_env_db_path
from tests.common.target_states import GlobalDictTarget, Metrics

# Globally-unique context keys for this module.
_CD_KEY = coco.ContextKey[str]("_test_provide_ctx_cd_v1", detect_change=True)
_NCD_KEY = coco.ContextKey[str]("_test_provide_ctx_ncd_v1")
_STATEHOOK_KEY = coco.ContextKey[Any]("_test_provide_ctx_statehook_v1", detect_change=True)

# Mutable holder so the scoped value can vary between runs of the same app.
_scoped: dict[str, str] = {"v": "v1"}
# Records values observed by use_context() during a run.
_observed: list[str] = []


def _create_env(db_name: str, base_value: str) -> coco.Environment:
    """Environment that provides ``base_value`` at the env level for both keys."""
    ctx = coco.ContextProvider()
    ctx.provide(_CD_KEY, base_value)
    ctx.provide(_NCD_KEY, base_value)
    settings = coco.Settings.from_env(db_path=get_env_db_path(db_name))
    return coco.Environment(settings, context_provider=ctx)


# ============================================================================
# Test 1: scoped value with a state function is rejected
# ============================================================================


def test_scoped_state_hook_raises() -> None:
    """provide_context rejects a value carrying __coco_memo_state__."""

    class _Stateful:
        def __coco_memo_key__(self) -> object:
            return "stateful-identity"

        def __coco_memo_state__(self, prev_state: Any) -> coco.MemoStateOutcome:
            return coco.MemoStateOutcome(state="s", memo_valid=False)

    provider = coco.ContextProvider()
    with pytest.raises(RuntimeError, match="state function"):
        provider.acquire_scoped_fingerprint(_STATEHOOK_KEY, _Stateful())
    # Nothing should have been registered.
    assert not provider._fp_refcounts


# ============================================================================
# Test 2: Override visibility
# ============================================================================


def test_nested_override_same_key() -> None:
    """Scoped value overrides env; inner scope shadows outer; all restore on exit."""
    GlobalDictTarget.store.clear()
    _observed.clear()

    @coco.fn
    async def app_main() -> None:
        _observed.append(coco.use_context(_CD_KEY))  # base (env)
        with coco.provide_context(_CD_KEY, "A"):
            _observed.append(coco.use_context(_CD_KEY))  # A
            with coco.provide_context(_CD_KEY, "B"):
                _observed.append(coco.use_context(_CD_KEY))  # B
            _observed.append(coco.use_context(_CD_KEY))  # A
        _observed.append(coco.use_context(_CD_KEY))  # base (env)

    env = _create_env("test_provide_ctx_nested", "base")
    app = coco.App(
        coco.AppConfig(name="test_provide_ctx_nested", environment=env), app_main
    )
    app.update_blocking()

    assert _observed == ["base", "A", "B", "A", "base"]


# ============================================================================
# Test 3: Change detection of the scoped value
# ============================================================================


def _run_consumer_app(
    db_name: str, key: coco.ContextKey[str], metrics: Metrics
) -> list[dict[str, int]]:
    """Mount a worker that opens a scope around an inline memoized consumer.

    The scoped value comes from ``_scoped["v"]`` so the caller can change it
    between the two updates. Returns per-update metrics.
    """

    @coco.fn(memo=True)
    def consumer(name: str, content: str) -> None:
        val = coco.use_context(key)
        metrics.increment("calls")
        coco.declare_target_state(
            GlobalDictTarget.target_state(name, f"{val}:{content}")
        )

    @coco.fn
    async def worker() -> None:
        with coco.provide_context(key, _scoped["v"]):
            consumer("A", "data")

    @coco.fn
    async def app_main() -> None:
        await coco.mount(coco.component_subpath("W"), worker)

    env = _create_env(db_name, "base")
    app = coco.App(coco.AppConfig(name=db_name, environment=env), app_main)
    app.update_blocking()
    m1 = metrics.collect()
    app.update_blocking()
    m2 = metrics.collect()
    return [m1, m2]


def test_scoped_change_invalidates_consumer() -> None:
    """A detect_change scoped value change re-invalidates the inline consumer."""
    GlobalDictTarget.store.clear()
    metrics = Metrics()

    # Phase 1: v1 — executes once, then memo hit on the second update.
    _scoped["v"] = "v1"
    m = _run_consumer_app("test_provide_ctx_cd_inv", _CD_KEY, metrics)
    assert m[0] == {"calls": 1}
    assert m[1] == {}
    assert GlobalDictTarget.store.data["A"].data == "v1:data"
    gc.collect()

    # Phase 2: v2 — scoped value changed → consumer re-executes.
    _scoped["v"] = "v2"
    m = _run_consumer_app("test_provide_ctx_cd_inv", _CD_KEY, metrics)
    assert m[0] == {"calls": 1}
    assert m[1] == {}
    assert GlobalDictTarget.store.data["A"].data == "v2:data"


def test_detect_change_false_no_invalidation() -> None:
    """A non-detect-change scoped value change does NOT invalidate the consumer."""
    GlobalDictTarget.store.clear()
    metrics = Metrics()

    _scoped["v"] = "v1"
    m = _run_consumer_app("test_provide_ctx_ncd_noinv", _NCD_KEY, metrics)
    assert m[0] == {"calls": 1}
    assert m[1] == {}
    assert GlobalDictTarget.store.data["A"].data == "v1:data"
    gc.collect()

    _scoped["v"] = "v2"
    m = _run_consumer_app("test_provide_ctx_ncd_noinv", _NCD_KEY, metrics)
    assert m[0] == {}  # memo hit — non-detect-change value does not affect deps
    assert m[1] == {}
    assert GlobalDictTarget.store.data["A"].data == "v1:data"  # old value reused


# ============================================================================
# Test 4: Absorption: the providing (memoized) function is not invalidated
# ============================================================================


def test_provider_not_invalidated_by_scoped_value() -> None:
    """foo→bar→baz: foo's memo does not depend on the value it scope-provides.

    foo (memoized) provides the value from a plain Python global — i.e. the
    change is invisible to foo's code/args — and bar/baz consume it. The scoped
    fp is absorbed at the provide_context boundary, so foo's memo entry does not
    carry it. Consequently, when only the scoped global changes, foo's inputs are
    unchanged and foo is reused as a whole (and so is everything beneath it).

    The complementary half — that consumers *do* re-evaluate under a changed
    scoped value when they actually get re-run — is covered by
    ``test_scoped_change_invalidates_consumer`` (non-memoized wrapper).
    """
    GlobalDictTarget.store.clear()
    metrics = Metrics()

    @coco.fn(memo=True)
    def baz(name: str) -> str:
        val = coco.use_context(_CD_KEY)
        metrics.increment("baz")
        coco.declare_target_state(GlobalDictTarget.target_state(name, val))
        return val

    @coco.fn(memo=True)
    def bar(name: str) -> str:
        metrics.increment("bar")
        return baz(name)

    @coco.fn(memo=True)
    def foo(name: str) -> None:
        metrics.increment("foo")
        with coco.provide_context(_CD_KEY, _scoped["v"]):
            bar(name)

    @coco.fn
    async def app_main() -> None:
        await coco.mount(coco.component_subpath("F"), foo, "A")

    def run(db: str) -> list[dict[str, int]]:
        env = _create_env(db, "base")
        app = coco.App(coco.AppConfig(name=db, environment=env), app_main)
        app.update_blocking()
        a = metrics.collect()
        app.update_blocking()
        b = metrics.collect()
        return [a, b]

    db = "test_provide_ctx_absorb"
    _scoped["v"] = "v1"
    m = run(db)
    assert m[0].get("foo") == 1 and m[0].get("bar") == 1 and m[0].get("baz") == 1
    assert m[1] == {}  # all memo hits on the second update
    assert GlobalDictTarget.store.data["A"].data == "v1"
    gc.collect()

    # Change only the scoped global. foo's code/args are unchanged and foo's memo
    # entry does not carry the scoped fp (it was absorbed), so foo is reused as a
    # whole — nothing re-executes and the prior output is kept.
    _scoped["v"] = "v2"
    m = run(db)
    assert m[0] == {}, "foo (and everything under it) should be reused unchanged"
    assert GlobalDictTarget.store.data["A"].data == "v1"


# ============================================================================
# Test 5: Boundary: scope does not cross mount
# ============================================================================


def test_scope_not_crossing_mount() -> None:
    """A mounted child sees the env-level value, not the parent's scoped override."""
    GlobalDictTarget.store.clear()
    _observed.clear()

    @coco.fn
    async def child() -> None:
        _observed.append(coco.use_context(_CD_KEY))

    @coco.fn
    async def app_main() -> None:
        with coco.provide_context(_CD_KEY, "scoped"):
            await coco.mount(coco.component_subpath("C"), child)

    env = _create_env("test_provide_ctx_mount_boundary", "base")
    app = coco.App(
        coco.AppConfig(name="test_provide_ctx_mount_boundary", environment=env),
        app_main,
    )
    app.update_blocking()

    assert _observed == ["base"]  # not "scoped" — override does not cross mount

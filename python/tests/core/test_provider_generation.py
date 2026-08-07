from typing import Any, Literal

import cocoindex as coco

from tests import common
from tests.common.target_states import DictsTarget, DictDataWithPrev, AtMost

_inner_exec_count: int = 0


@coco.fn(memo=True)
async def _insert_rows_memo(provider: Any, data: dict[str, Any]) -> None:
    global _inner_exec_count
    _inner_exec_count += 1
    for key, value in data.items():
        coco.declare_target_state(provider.target_state(key, value))


async def _declare_dicts_with_memo() -> None:
    with coco.component_subpath("dict"):
        for name, data in _source_data.items():
            with coco.component_subpath(name):
                single_dict_provider = await coco.use_mount(
                    coco.component_subpath("setup"),
                    DictsTarget.declare_dict_target,
                    name,
                )
                await coco.use_mount(  # type: ignore[call-overload]
                    coco.component_subpath("rows"),
                    _insert_rows_memo,
                    single_dict_provider,
                    data,
                )


coco_env = common.create_test_env(__file__)

_source_data: dict[str, dict[str, Any]] = {}


async def _declare_dicts_data() -> None:
    with coco.component_subpath("dict"):
        for name, data in _source_data.items():
            single_dict_provider = await coco.use_mount(
                coco.component_subpath(name),
                DictsTarget.declare_dict_target,
                name,
            )
            for key, value in data.items():
                coco.declare_target_state(single_dict_provider.target_state(key, value))


def _new_app(name: str) -> coco.App[[], None]:
    DictsTarget.store.clear()
    _source_data.clear()
    return coco.App(
        coco.AppConfig(name=name, environment=coco_env),
        _declare_dicts_data,
    )


def test_destructive_change_ignores_stale_children() -> None:
    app = _new_app("test_destructive_change_ignores_stale_children")

    # Run 1: Normal insert
    _source_data["D1"] = {"a": 1, "b": 2}
    app.update_blocking()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(1), "insert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(1), "upsert": 2}

    # Run 2: Destructive change with same data — children re-inserted (stale tracking ignored)
    DictsTarget.store.child_invalidation = "destructive"
    try:
        app.update_blocking()
    finally:
        DictsTarget.store.child_invalidation = None

    # Children should be treated as entirely new (prev=[], prev_may_be_missing=True)
    assert DictsTarget.store.data["D1"] == {
        "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
        "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    child_metrics = DictsTarget.store.collect_child_metrics()
    assert child_metrics.get("upsert", 0) == 2


def test_lossy_change_forces_child_upsert() -> None:
    app = _new_app("test_lossy_change_forces_child_upsert")

    # Run 1: Normal insert
    _source_data["D1"] = {"a": 1, "b": 2}
    app.update_blocking()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
    }
    DictsTarget.store.metrics.collect()
    DictsTarget.store.collect_child_metrics()

    # Run 2: Lossy change with same data — children get prev_may_be_missing=True
    DictsTarget.store.child_invalidation = "lossy"
    try:
        app.update_blocking()
    finally:
        DictsTarget.store.child_invalidation = None

    # Children should keep prev (same provider_id) but have prev_may_be_missing=True
    assert DictsTarget.store.data["D1"] == {
        "a": DictDataWithPrev(data=1, prev=[1], prev_may_be_missing=True),
        "b": DictDataWithPrev(data=2, prev=[2], prev_may_be_missing=True),
    }
    child_metrics = DictsTarget.store.collect_child_metrics()
    assert child_metrics.get("upsert", 0) == 2


def test_no_invalidation_skips_unchanged_children() -> None:
    app = _new_app("test_no_invalidation_skips_unchanged_children")

    # Run 1: Normal insert
    _source_data["D1"] = {"a": 1, "b": 2}
    app.update_blocking()
    DictsTarget.store.metrics.collect()
    DictsTarget.store.collect_child_metrics()

    # Run 2: Same data, no invalidation — no child sink calls
    app.update_blocking()
    assert DictsTarget.store.collect_child_metrics() == {}


def test_destructive_then_normal_restores_optimization() -> None:
    app = _new_app("test_destructive_then_normal_restores_optimization")

    # Run 1: Normal insert
    _source_data["D1"] = {"a": 1, "b": 2}
    app.update_blocking()
    DictsTarget.store.metrics.collect()
    DictsTarget.store.collect_child_metrics()

    # Run 2: Destructive change — children re-inserted
    DictsTarget.store.child_invalidation = "destructive"
    try:
        app.update_blocking()
    finally:
        DictsTarget.store.child_invalidation = None
    DictsTarget.store.metrics.collect()
    DictsTarget.store.collect_child_metrics()

    # Run 3: Same data, no invalidation — optimization restored, no child calls
    app.update_blocking()
    assert DictsTarget.store.collect_child_metrics() == {}


def test_lossy_then_normal_restores_optimization() -> None:
    app = _new_app("test_lossy_then_normal_restores_optimization")

    # Run 1: Normal insert
    _source_data["D1"] = {"a": 1, "b": 2}
    app.update_blocking()
    DictsTarget.store.metrics.collect()
    DictsTarget.store.collect_child_metrics()

    # Run 2: Lossy change — children upserted
    DictsTarget.store.child_invalidation = "lossy"
    try:
        app.update_blocking()
    finally:
        DictsTarget.store.child_invalidation = None
    DictsTarget.store.metrics.collect()
    DictsTarget.store.collect_child_metrics()

    # Run 3: Same data, no invalidation — optimization restored, no child calls
    app.update_blocking()
    assert DictsTarget.store.collect_child_metrics() == {}


def test_destructive_change_with_data_change() -> None:
    app = _new_app("test_destructive_change_with_data_change")

    # Run 1: Normal insert
    _source_data["D1"] = {"a": 1, "b": 2}
    app.update_blocking()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
    }
    DictsTarget.store.metrics.collect()
    DictsTarget.store.collect_child_metrics()

    # Run 2: Destructive + data change — stale "b" cleaned up, "a" re-inserted, "c" new
    _source_data["D1"] = {"a": 1, "c": 3}
    DictsTarget.store.child_invalidation = "destructive"
    try:
        app.update_blocking()
    finally:
        DictsTarget.store.child_invalidation = None

    assert DictsTarget.store.data["D1"] == {
        "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
        "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
    }
    child_metrics = DictsTarget.store.collect_child_metrics()
    # Stale children are not explicitly deleted — the parent's destructive upsert
    # already cleaned up the external state (recreated the container).
    assert child_metrics.get("upsert", 0) == 2
    assert child_metrics.get("delete", 0) == 0


def _new_memo_app(name: str) -> coco.App[[], None]:
    global _inner_exec_count
    DictsTarget.store.clear()
    _source_data.clear()
    DictsTarget.store.child_invalidation = None
    _inner_exec_count = 0
    return coco.App(
        coco.AppConfig(name=name, environment=coco_env),
        _declare_dicts_with_memo,
    )


def test_destructive_change_invalidates_memo() -> None:
    global _inner_exec_count
    app = _new_memo_app("test_destructive_change_invalidates_memo")
    _source_data["D1"] = {"a": 1}

    # Run 1: Initial insert — inner function executes
    app.update_blocking()
    assert _inner_exec_count == 1
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(1), "upsert": 1}

    # Run 2: Same data, no invalidation — inner function skipped (memo hit)
    _inner_exec_count = 0
    app.update_blocking()
    assert _inner_exec_count == 0
    assert DictsTarget.store.collect_child_metrics() == {}

    # Run 3: Destructive change — provider_id changes, memo key changes, inner re-executes
    DictsTarget.store.child_invalidation = "destructive"
    _inner_exec_count = 0
    try:
        app.update_blocking()
    finally:
        DictsTarget.store.child_invalidation = None
    assert _inner_exec_count == 1
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(1), "upsert": 1}

    # Run 4: Same data, no invalidation — memo hit again (new provider_id is stable)
    _inner_exec_count = 0
    app.update_blocking()
    assert _inner_exec_count == 0
    assert DictsTarget.store.collect_child_metrics() == {}


def test_lossy_change_invalidates_memo() -> None:
    global _inner_exec_count
    app = _new_memo_app("test_lossy_change_invalidates_memo")
    _source_data["D1"] = {"a": 1}

    # Run 1: Initial insert — inner function executes
    app.update_blocking()
    assert _inner_exec_count == 1
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(1), "upsert": 1}

    # Run 2: Same data, no invalidation — inner function skipped (memo hit)
    _inner_exec_count = 0
    app.update_blocking()
    assert _inner_exec_count == 0
    assert DictsTarget.store.collect_child_metrics() == {}

    # Run 3: Lossy change — schema_version changes, memo key changes, inner re-executes
    DictsTarget.store.child_invalidation = "lossy"
    _inner_exec_count = 0
    try:
        app.update_blocking()
    finally:
        DictsTarget.store.child_invalidation = None
    assert _inner_exec_count == 1
    # Lossy forces upsert (prev_may_be_missing=True) for child rows
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(1), "upsert": 1}

    # Run 4: Same data, no invalidation — memo hit again (schema_version is stable)
    _inner_exec_count = 0
    app.update_blocking()
    assert _inner_exec_count == 0
    assert DictsTarget.store.collect_child_metrics() == {}


# The tests above pass the provider as an argument, so an invalidation reaches
# the memoized code through the memo key. The ones below capture the provider
# from an enclosing scope instead — it is *not* part of the memo key, so the
# invalidation has to travel through the provider generations recorded with the
# memo entry. Without that channel a memo hit silently skips re-declaring the
# rows, leaving cocoindex convinced they are written while the target has lost
# them, and no later run ever repairs it.

_captured_provider: Any = None


@coco.fn(memo=True)
async def _insert_rows_memo_captured(data: dict[str, Any]) -> None:
    global _inner_exec_count
    _inner_exec_count += 1
    for key, value in data.items():
        coco.declare_target_state(_captured_provider.target_state(key, value))


async def _declare_dicts_with_captured_provider() -> None:
    global _captured_provider
    with coco.component_subpath("dict"):
        for name, data in _source_data.items():
            with coco.component_subpath(name):
                _captured_provider = await coco.use_mount(
                    coco.component_subpath("setup"),
                    DictsTarget.declare_dict_target,
                    name,
                )
                await coco.use_mount(  # type: ignore[call-overload]
                    coco.component_subpath("rows"),
                    _insert_rows_memo_captured,
                    data,
                )


@coco.fn(memo=True)
async def _insert_rows_fn_memo_captured(data: dict[str, Any]) -> None:
    global _inner_exec_count
    _inner_exec_count += 1
    for key, value in data.items():
        coco.declare_target_state(_captured_provider.target_state(key, value))


async def _declare_dicts_with_captured_provider_fn_memo() -> None:
    """Same, but the memoized function is *called*, not mounted — so it goes
    through the function-call memo cache rather than the component memo."""
    global _captured_provider
    with coco.component_subpath("dict"):
        for name, data in _source_data.items():
            with coco.component_subpath(name):
                _captured_provider = await coco.use_mount(
                    coco.component_subpath("setup"),
                    DictsTarget.declare_dict_target,
                    name,
                )
                await _insert_rows_fn_memo_captured(data)


def _new_captured_provider_app(
    name: str, main: Any = _declare_dicts_with_captured_provider
) -> coco.App[[], None]:
    global _inner_exec_count, _captured_provider
    DictsTarget.store.clear()
    _source_data.clear()
    DictsTarget.store.child_invalidation = None
    _inner_exec_count = 0
    _captured_provider = None
    return coco.App(coco.AppConfig(name=name, environment=coco_env), main)


def _assert_invalidates_memo_without_provider_arg(
    app: coco.App[[], None], invalidation: Literal["destructive", "lossy"]
) -> None:
    global _inner_exec_count
    _source_data["D1"] = {"a": 1}

    # Run 1: initial insert — the memoized function executes.
    app.update_blocking()
    assert _inner_exec_count == 1
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(1), "upsert": 1}

    # Run 2: same data, no invalidation — memo hit.
    _inner_exec_count = 0
    app.update_blocking()
    assert _inner_exec_count == 0
    assert DictsTarget.store.collect_child_metrics() == {}

    # Run 3: the provider generation moves. The provider is not in the memo key,
    # so only the recorded generation dep can force the re-execution.
    DictsTarget.store.child_invalidation = invalidation
    _inner_exec_count = 0
    try:
        app.update_blocking()
    finally:
        DictsTarget.store.child_invalidation = None
    assert _inner_exec_count == 1
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(1), "upsert": 1}

    # Run 4: the generation is stable again — back to a memo hit. An invalidation
    # must not leave the entry permanently unmemoizable.
    _inner_exec_count = 0
    app.update_blocking()
    assert _inner_exec_count == 0
    assert DictsTarget.store.collect_child_metrics() == {}


def test_destructive_change_invalidates_memo_without_provider_arg() -> None:
    _assert_invalidates_memo_without_provider_arg(
        _new_captured_provider_app(
            "test_destructive_change_invalidates_memo_without_provider_arg"
        ),
        "destructive",
    )


def test_lossy_change_invalidates_memo_without_provider_arg() -> None:
    _assert_invalidates_memo_without_provider_arg(
        _new_captured_provider_app(
            "test_lossy_change_invalidates_memo_without_provider_arg"
        ),
        "lossy",
    )


def test_lossy_change_invalidates_fn_memo_without_provider_arg() -> None:
    _assert_invalidates_memo_without_provider_arg(
        _new_captured_provider_app(
            "test_lossy_change_invalidates_fn_memo_without_provider_arg",
            _declare_dicts_with_captured_provider_fn_memo,
        ),
        "lossy",
    )


# --- Composed scenario: a memoized intermediate component re-executes while an
# inner memoized fn HITS. The hit skips the fn's declarations, so its provider
# deps must be merged from the stored entry into the intermediate's rebuilt
# memo (via FnCallMemoGuard.join_cached_target_provider_deps) — otherwise the
# intermediate's new entry loses the dep and a later lossy/destructive change
# is silently ignored.

_leaf_runs: list[str] = []


@coco.fn(memo=True)
async def _composed_leaf() -> None:
    _leaf_runs.append("leaf")
    coco.declare_target_state(_captured_provider.target_state("a", 1))


@coco.fn(memo=True)
async def _composed_mid(n: int) -> None:
    _leaf_runs.append(f"mid{n}")
    await _composed_leaf()


_composed_n: dict[str, int] = {"n": 1}


async def _declare_composed() -> None:
    global _captured_provider
    with coco.component_subpath("dict"):
        _captured_provider = await coco.use_mount(
            coco.component_subpath("setup"), DictsTarget.declare_dict_target, "D1"
        )
        await coco.use_mount(
            coco.component_subpath("rows"), _composed_mid, _composed_n["n"]
        )


def test_lossy_change_after_intermediate_rebuild_with_fn_hit() -> None:
    global _captured_provider
    DictsTarget.store.clear()
    DictsTarget.store.child_invalidation = None
    _leaf_runs.clear()
    _captured_provider = None
    _composed_n["n"] = 1
    app: coco.App[[], None] = coco.App(
        coco.AppConfig(
            name="test_lossy_composed_fn_hit",
            environment=coco_env,
        ),
        _declare_composed,
    )

    # Run 1: everything executes; deps recorded everywhere.
    app.update_blocking()
    assert _leaf_runs == ["mid1", "leaf"]
    DictsTarget.store.collect_child_metrics()

    # Run 2: mid's memo key changes -> mid re-executes, leaf HITS. Mid's
    # rebuilt entry must retain leaf's provider dep via the stored-entry merge.
    _leaf_runs.clear()
    _composed_n["n"] = 2
    app.update_blocking()
    assert _leaf_runs == ["mid2"]
    DictsTarget.store.collect_child_metrics()

    # Run 3: lossy provider change. Both mid and leaf must re-execute so the
    # row is re-upserted.
    _leaf_runs.clear()
    DictsTarget.store.child_invalidation = "lossy"
    try:
        app.update_blocking()
    finally:
        DictsTarget.store.child_invalidation = None
    assert _leaf_runs == ["mid2", "leaf"]
    metrics = DictsTarget.store.collect_child_metrics()
    assert metrics.get("upsert", 0) == 1

    # Run 4: stable again — memo hits all the way down.
    _leaf_runs.clear()
    app.update_blocking()
    assert _leaf_runs == []
    assert DictsTarget.store.collect_child_metrics() == {}

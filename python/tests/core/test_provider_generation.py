from typing import Any

import cocoindex as coco

from tests import common
from tests.common.target_states import DictsTarget, DictDataWithPrev

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
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "insert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}

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
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 1}

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
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 1}

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
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 1}

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
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 1}

    # Run 4: Same data, no invalidation — memo hit again (schema_version is stable)
    _inner_exec_count = 0
    app.update_blocking()
    assert _inner_exec_count == 0
    assert DictsTarget.store.collect_child_metrics() == {}

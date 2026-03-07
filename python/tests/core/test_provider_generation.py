from typing import Any

import cocoindex as coco

from tests import common
from tests.common.target_states import DictsTarget, DictDataWithPrev

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

from typing import Any

import pytest

import cocoindex as coco

from tests import common
from tests.common.target_states import (
    GlobalDictTarget,
    AsyncGlobalDictTarget,
    DictDataWithPrev,
)

coco_env = common.create_test_env(__file__)

_source_data: dict[str, Any] = {}


def declare_global_dict_entries() -> None:
    for key, value in _source_data.items():
        coco.declare_target_state(GlobalDictTarget.target_state(key, value))


def test_global_dict_target_state_insert() -> None:
    GlobalDictTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_global_dict_target_state_insert", environment=coco_env
        ),
        declare_global_dict_entries,
    )

    _source_data["a"] = 1
    app.update_blocking()
    assert GlobalDictTarget.store.data == {
        "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
    }
    assert GlobalDictTarget.store.metrics.collect() == {"sink": 1, "upsert": 1}

    _source_data["b"] = 2
    app.update_blocking()
    assert GlobalDictTarget.store.data == {
        "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
        "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert GlobalDictTarget.store.metrics.collect() == {"sink": 1, "upsert": 1}


def test_global_dict_target_state_upsert() -> None:
    GlobalDictTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_global_dict_target_state_upsert", environment=coco_env
        ),
        declare_global_dict_entries,
    )

    _source_data["a"] = 1
    _source_data["b"] = 2
    app.update_blocking()
    assert GlobalDictTarget.store.data == {
        "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
        "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert GlobalDictTarget.store.metrics.collect() == {"sink": 1, "upsert": 2}

    _source_data["a"] = 3
    app.update_blocking()
    assert GlobalDictTarget.store.data == {
        "a": DictDataWithPrev(data=3, prev=[1], prev_may_be_missing=False),
        "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert GlobalDictTarget.store.metrics.collect() == {"sink": 1, "upsert": 1}


def test_global_dict_target_state_delete() -> None:
    GlobalDictTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_global_dict_target_state_delete", environment=coco_env
        ),
        declare_global_dict_entries,
    )

    _source_data["a"] = 1
    _source_data["b"] = 2
    app.update_blocking()
    assert GlobalDictTarget.store.metrics.collect() == {"sink": 1, "upsert": 2}

    del _source_data["a"]
    app.update_blocking()
    assert GlobalDictTarget.store.data == {
        "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert GlobalDictTarget.store.metrics.collect() == {"sink": 1, "delete": 1}


def test_global_dict_target_state_no_change() -> None:
    GlobalDictTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_global_dict_target_state_no_change", environment=coco_env
        ),
        declare_global_dict_entries,
    )

    _source_data["a"] = 1
    _source_data["b"] = 2

    app.update_blocking()
    assert GlobalDictTarget.store.data == {
        "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
        "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert GlobalDictTarget.store.metrics.collect() == {"sink": 1, "upsert": 2}

    app.update_blocking()
    assert GlobalDictTarget.store.data == {
        "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
        "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert GlobalDictTarget.store.metrics.collect() == {}

    _source_data["a"] = 3

    app.update_blocking()
    assert GlobalDictTarget.store.data == {
        "a": DictDataWithPrev(data=3, prev=[1], prev_may_be_missing=False),
        "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert GlobalDictTarget.store.metrics.collect() == {"sink": 1, "upsert": 1}

    app.update_blocking()
    assert GlobalDictTarget.store.data == {
        "a": DictDataWithPrev(data=3, prev=[1], prev_may_be_missing=False),
        "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert GlobalDictTarget.store.metrics.collect() == {}


def declare_async_global_dict_entries() -> None:
    for key, value in _source_data.items():
        coco.declare_target_state(AsyncGlobalDictTarget.target_state(key, value))


def test_async_global_dict_target_state_insert() -> None:
    AsyncGlobalDictTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_async_global_dict_target_state_insert", environment=coco_env
        ),
        declare_async_global_dict_entries,
    )

    _source_data["a"] = 1
    app.update_blocking()
    assert AsyncGlobalDictTarget.store.data == {
        "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
    }
    assert AsyncGlobalDictTarget.store.metrics.collect() == {"sink": 1, "upsert": 1}

    _source_data["b"] = 2
    app.update_blocking()
    assert AsyncGlobalDictTarget.store.data == {
        "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
        "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert AsyncGlobalDictTarget.store.metrics.collect() == {"sink": 1, "upsert": 1}


def test_global_dict_target_state_proceed_with_exception() -> None:
    GlobalDictTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_global_dict_target_state_proceed_with_exception",
            environment=coco_env,
        ),
        declare_global_dict_entries,
    )

    _source_data["a"] = 1
    try:
        GlobalDictTarget.store.sink_exception = True
        with pytest.raises(Exception):
            app.update_blocking()
    finally:
        GlobalDictTarget.store.sink_exception = False
    assert GlobalDictTarget.store.data == {}

    _source_data["a"] = 2
    app.update_blocking()
    assert GlobalDictTarget.store.data == {
        "a": DictDataWithPrev(data=2, prev=[1], prev_may_be_missing=True),
    }
    assert GlobalDictTarget.store.metrics.collect() == {"sink": 1, "upsert": 1}

    _source_data["a"] = 3
    app.update_blocking()
    assert GlobalDictTarget.store.data == {
        "a": DictDataWithPrev(data=3, prev=[2], prev_may_be_missing=False),
    }
    assert GlobalDictTarget.store.metrics.collect() == {"sink": 1, "upsert": 1}

    del _source_data["a"]
    try:
        GlobalDictTarget.store.sink_exception = True
        with pytest.raises(Exception):
            app.update_blocking()
    finally:
        GlobalDictTarget.store.sink_exception = False
    app.update_blocking()
    assert GlobalDictTarget.store.data == {}
    assert GlobalDictTarget.store.metrics.collect() == {"sink": 1, "delete": 1}

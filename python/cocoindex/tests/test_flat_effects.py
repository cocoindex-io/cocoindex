from typing import Any

import pytest

import cocoindex as coco

from . import common
from .common.effects import GlobalDictTarget, AsyncGlobalDictTarget, DictDataWithPrev

coco_env = common.create_test_env(__file__)

_source_data: dict[str, Any] = {}


@coco.function
def declare_global_dict_entries(scope: coco.Scope) -> None:
    for key, value in _source_data.items():
        coco.declare_effect(scope, GlobalDictTarget.effect(key, value))


def test_global_dict_effect_insert() -> None:
    GlobalDictTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        "test_global_dict_effect_insert",
        declare_global_dict_entries,
        environment=coco_env,
    )

    _source_data["a"] = 1
    app.run()
    assert GlobalDictTarget.store.data == {
        "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
    }
    assert GlobalDictTarget.store.metrics.collect() == {"sink": 1, "upsert": 1}

    _source_data["b"] = 2
    app.run()
    assert GlobalDictTarget.store.data == {
        "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
        "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert GlobalDictTarget.store.metrics.collect() == {"sink": 1, "upsert": 1}


def test_global_dict_effect_upsert() -> None:
    GlobalDictTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        "test_global_dict_effect_upsert",
        declare_global_dict_entries,
        environment=coco_env,
    )

    _source_data["a"] = 1
    _source_data["b"] = 2
    app.run()
    assert GlobalDictTarget.store.data == {
        "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
        "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert GlobalDictTarget.store.metrics.collect() == {"sink": 1, "upsert": 2}

    _source_data["a"] = 3
    app.run()
    assert GlobalDictTarget.store.data == {
        "a": DictDataWithPrev(data=3, prev=[1], prev_may_be_missing=False),
        "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert GlobalDictTarget.store.metrics.collect() == {"sink": 1, "upsert": 1}


def test_global_dict_effect_delete() -> None:
    GlobalDictTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        "test_global_dict_effect_delete",
        declare_global_dict_entries,
        environment=coco_env,
    )

    _source_data["a"] = 1
    _source_data["b"] = 2
    app.run()
    assert GlobalDictTarget.store.metrics.collect() == {"sink": 1, "upsert": 2}

    del _source_data["a"]
    app.run()
    assert GlobalDictTarget.store.data == {
        "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert GlobalDictTarget.store.metrics.collect() == {"sink": 1, "delete": 1}


def test_global_dict_effect_no_change() -> None:
    GlobalDictTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        "test_global_dict_effect_no_change",
        declare_global_dict_entries,
        environment=coco_env,
    )

    _source_data["a"] = 1
    _source_data["b"] = 2

    app.run()
    assert GlobalDictTarget.store.data == {
        "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
        "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert GlobalDictTarget.store.metrics.collect() == {"sink": 1, "upsert": 2}

    app.run()
    assert GlobalDictTarget.store.data == {
        "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
        "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert GlobalDictTarget.store.metrics.collect() == {}

    _source_data["a"] = 3

    app.run()
    assert GlobalDictTarget.store.data == {
        "a": DictDataWithPrev(data=3, prev=[1], prev_may_be_missing=False),
        "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert GlobalDictTarget.store.metrics.collect() == {"sink": 1, "upsert": 1}

    app.run()
    assert GlobalDictTarget.store.data == {
        "a": DictDataWithPrev(data=3, prev=[1], prev_may_be_missing=False),
        "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert GlobalDictTarget.store.metrics.collect() == {}


@coco.function
def declare_async_global_dict_entries(scope: coco.Scope) -> None:
    for key, value in _source_data.items():
        coco.declare_effect(scope, AsyncGlobalDictTarget.effect(key, value))


def test_async_global_dict_effect_insert() -> None:
    AsyncGlobalDictTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        "test_async_global_dict_effect_insert",
        declare_async_global_dict_entries,
        environment=coco_env,
    )

    _source_data["a"] = 1
    app.run()
    assert AsyncGlobalDictTarget.store.data == {
        "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
    }
    assert AsyncGlobalDictTarget.store.metrics.collect() == {"sink": 1, "upsert": 1}

    _source_data["b"] = 2
    app.run()
    assert AsyncGlobalDictTarget.store.data == {
        "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
        "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert AsyncGlobalDictTarget.store.metrics.collect() == {"sink": 1, "upsert": 1}


def test_global_dict_effect_proceed_with_exception() -> None:
    GlobalDictTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        "test_global_dict_effect_proceed_with_exception",
        declare_global_dict_entries,
        environment=coco_env,
    )

    _source_data["a"] = 1
    try:
        GlobalDictTarget.store.sink_exception = True
        with pytest.raises(Exception):
            app.run()
    finally:
        GlobalDictTarget.store.sink_exception = False
    assert GlobalDictTarget.store.data == {}

    _source_data["a"] = 2
    app.run()
    assert GlobalDictTarget.store.data == {
        "a": DictDataWithPrev(data=2, prev=[1], prev_may_be_missing=True),
    }
    assert GlobalDictTarget.store.metrics.collect() == {"sink": 1, "upsert": 1}

    _source_data["a"] = 3
    app.run()
    assert GlobalDictTarget.store.data == {
        "a": DictDataWithPrev(data=3, prev=[2], prev_may_be_missing=False),
    }
    assert GlobalDictTarget.store.metrics.collect() == {"sink": 1, "upsert": 1}

    del _source_data["a"]
    try:
        GlobalDictTarget.store.sink_exception = True
        with pytest.raises(Exception):
            app.run()
    finally:
        GlobalDictTarget.store.sink_exception = False
    app.run()
    assert GlobalDictTarget.store.data == {}
    assert GlobalDictTarget.store.metrics.collect() == {"sink": 1, "delete": 1}

import cocoindex as coco

from typing import Any

from . import common
from .common.effects import GlobalDictTarget, DictDataWithPrev

coco_env = common.create_test_env(__file__)

_source_data: dict[str, Any] = {}


@coco.function
def declare_global_dict_entries(csp: coco.StatePath) -> None:
    for key, value in _source_data.items():
        coco.declare_effect(GlobalDictTarget.effect(key, value))


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

import cocoindex as coco

from typing import Any, Collection

from . import common
from .common.effects import DictsTarget, DictDataWithPrev

coco_env = common.create_test_env(__file__)

_source_data: dict[str, dict[str, Any]] = {}


@coco.function
def _declare_dict_container(csp: coco.StatePath, name: str) -> coco.EffectProvider[str]:
    provider = coco.declare_effect_with_child(DictsTarget.effect(name, None))
    return provider


##################################################################################


@coco.function
def _declare_dicts_data_together(csp: coco.StatePath) -> None:
    for name, data in _source_data.items():
        single_dict_provider = coco.mount_run(
            _declare_dict_container,
            csp / "dict" / name,
            name,
        ).result()
        for key, value in data.items():
            coco.declare_effect(single_dict_provider.effect(key, value))


def test_dicts_data_together_insert() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        _declare_dicts_data_together,
        coco.AppConfig(
            name="test_dicts_data_data_together_insert", environment=coco_env
        ),
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    _source_data["D2"] = {}
    app.update()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {},
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 2, "upsert": 2}

    _source_data["D2"]["c"] = 3
    _source_data["D3"] = {"a": 4}
    app.update()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
        "D3": {
            "a": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 3, "upsert": 1}


##################################################################################


@coco.function
def _declare_one_dict(csp: coco.StatePath, name: str) -> None:
    dict_provider = coco.mount_run(_declare_dict_container, csp / name, name).result()
    for key, value in _source_data[name].items():
        coco.declare_effect(dict_provider.effect(key, value))


@coco.function
def _declare_dicts_in_sub_components(csp: coco.StatePath) -> None:
    for name in _source_data.keys():
        coco.mount(_declare_one_dict, csp / name, name)


def test_dicts_in_sub_components_insert() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        _declare_dicts_in_sub_components,
        coco.AppConfig(
            name="test_dicts_in_sub_components_insert", environment=coco_env
        ),
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    _source_data["D2"] = {}
    app.update()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {},
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 2, "upsert": 2}

    _source_data["D2"]["c"] = 3
    _source_data["D3"] = {"a": 4}
    app.update()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
        "D3": {
            "a": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 3, "upsert": 1}


##################################################################################


@coco.function
def _declare_dict_containers(
    csp: coco.StatePath, names: Collection[str]
) -> dict[str, coco.EffectProvider[str]]:
    providers = {
        name: coco.declare_effect_with_child(DictsTarget.effect(name, None))
        for name in names
    }
    return providers


@coco.function
def _declare_one_dict_data(
    csp: coco.StatePath, name: str, provider: coco.EffectProvider[str]
) -> None:
    for key, value in _source_data[name].items():
        coco.declare_effect(provider.effect(key, value))


@coco.function
def _declare_dict_containers_together(csp: coco.StatePath) -> None:
    providers = coco.mount_run(
        _declare_dict_containers, csp / "setup", _source_data.keys()
    ).result()
    for name, provider in providers.items():
        coco.mount(_declare_one_dict_data, csp / name, name, provider)


def test_dicts_containers_together_insert() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        _declare_dict_containers_together,
        coco.AppConfig(
            name="test_dicts_containers_together_insert", environment=coco_env
        ),
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    _source_data["D2"] = {}
    app.update()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {},
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "upsert": 2}

    _source_data["D2"]["c"] = 3
    _source_data["D3"] = {"a": 4}
    app.update()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
        "D3": {
            "a": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "upsert": 1}

import cocoindex as coco
import cocoindex.aio as coco_aio
import cocoindex.inspect as coco_inspect
import pytest

from typing import Any, Collection

from . import common
from .common.effects import DictsTarget, DictDataWithPrev

coco_env = common.create_test_env(__file__)

_source_data: dict[str, dict[str, Any]] = {}


@coco.function
def _declare_dict_container(scope: coco.Scope, name: str) -> coco.EffectProvider[str]:
    provider = coco.declare_effect_with_child(scope, DictsTarget.effect(name, None))
    return provider


##################################################################################


@coco.function
def _declare_dicts_data_together(scope: coco.Scope) -> None:
    for name, data in _source_data.items():
        single_dict_provider = coco.mount_run(
            scope / "dict" / name,
            _declare_dict_container,
            name,
        ).result()
        for key, value in data.items():
            coco.declare_effect(scope, single_dict_provider.effect(key, value))


def test_dicts_data_together_insert() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        "test_dicts_data_together_insert",
        _declare_dicts_data_together,
        environment=coco_env,
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    _source_data["D2"] = {}
    app.run()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {},
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 2, "upsert": 2}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}

    _source_data["D2"]["c"] = 3
    _source_data["D3"] = {"a": 4}
    app.run()
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
    assert DictsTarget.store.collect_child_metrics() == {"sink": 2, "upsert": 2}
    assert coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "dict",
        coco.ROOT_PATH / "dict" / "D1",
        coco.ROOT_PATH / "dict" / "D2",
        coco.ROOT_PATH / "dict" / "D3",
    ]


def test_dicts_data_together_delete_dict() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        "test_dicts_data_together_delete_dict",
        _declare_dicts_data_together,
        environment=coco_env,
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    _source_data["D2"] = {}
    app.run()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {},
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 2, "upsert": 2}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}
    assert coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "dict",
        coco.ROOT_PATH / "dict" / "D1",
        coco.ROOT_PATH / "dict" / "D2",
    ]

    del _source_data["D1"]
    _source_data["D2"]["c"] = 3
    _source_data["D3"] = {"a": 4}
    app.run()
    assert DictsTarget.store.data == {
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
        "D3": {
            "a": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 3, "upsert": 1, "delete": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 2, "upsert": 2}
    assert coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "dict",
        coco.ROOT_PATH / "dict" / "D2",
        coco.ROOT_PATH / "dict" / "D3",
    ]

    # Re-insert after deletion
    _source_data["D1"] = {"a": 3, "c": 4}
    app.run()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
            "c": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
        "D3": {
            "a": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 3, "upsert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}
    assert coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "dict",
        coco.ROOT_PATH / "dict" / "D1",
        coco.ROOT_PATH / "dict" / "D2",
        coco.ROOT_PATH / "dict" / "D3",
    ]


def test_dicts_data_together_delete_entry() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        "test_dicts_data_together_delete_entry",
        _declare_dicts_data_together,
        environment=coco_env,
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    app.run()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "upsert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}

    del _source_data["D1"]["a"]
    app.run()
    assert DictsTarget.store.data == {
        "D1": {
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "delete": 1}

    # Re-insert after deletion
    _source_data["D1"]["a"] = 3
    _source_data["D1"]["c"] = 4
    app.run()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
            "c": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}
    assert coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "dict",
        coco.ROOT_PATH / "dict" / "D1",
    ]


##################################################################################


@coco.function
def _declare_one_dict(scope: coco.Scope, name: str) -> None:
    dict_provider = coco.mount_run(
        scope / "setup", _declare_dict_container, name
    ).result()
    for key, value in _source_data[name].items():
        coco.declare_effect(scope, dict_provider.effect(key, value))


@coco.function
def _declare_dicts_in_sub_components(scope: coco.Scope) -> None:
    for name in _source_data.keys():
        coco.mount(scope / name, _declare_one_dict, name)


def test_dicts_in_sub_components_insert() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        "test_dicts_in_sub_components_insert",
        _declare_dicts_in_sub_components,
        environment=coco_env,
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    _source_data["D2"] = {}
    app.run()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {},
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 2, "upsert": 2}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}

    _source_data["D2"]["c"] = 3
    _source_data["D3"] = {"a": 4}
    app.run()
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
    assert DictsTarget.store.collect_child_metrics() == {"sink": 2, "upsert": 2}
    assert coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "D1",
        coco.ROOT_PATH / "D1" / "setup",
        coco.ROOT_PATH / "D2",
        coco.ROOT_PATH / "D2" / "setup",
        coco.ROOT_PATH / "D3",
        coco.ROOT_PATH / "D3" / "setup",
    ]


def test_dicts_in_sub_components_delete_dict() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        "test_dicts_in_sub_components_delete_dict",
        _declare_dicts_in_sub_components,
        environment=coco_env,
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    _source_data["D2"] = {}
    app.run()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {},
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 2, "upsert": 2}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}
    assert coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "D1",
        coco.ROOT_PATH / "D1" / "setup",
        coco.ROOT_PATH / "D2",
        coco.ROOT_PATH / "D2" / "setup",
    ]

    del _source_data["D1"]
    _source_data["D2"]["c"] = 3
    _source_data["D3"] = {"a": 4}
    app.run()
    assert DictsTarget.store.data == {
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
        "D3": {
            "a": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 3, "upsert": 1, "delete": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 2, "upsert": 2}
    assert coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "D2",
        coco.ROOT_PATH / "D2" / "setup",
        coco.ROOT_PATH / "D3",
        coco.ROOT_PATH / "D3" / "setup",
    ]

    # Re-insert after deletion
    _source_data["D1"] = {"a": 3, "c": 4}
    app.run()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
            "c": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
        "D3": {
            "a": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 3, "upsert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}
    assert coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "D1",
        coco.ROOT_PATH / "D1" / "setup",
        coco.ROOT_PATH / "D2",
        coco.ROOT_PATH / "D2" / "setup",
        coco.ROOT_PATH / "D3",
        coco.ROOT_PATH / "D3" / "setup",
    ]


def test_dicts_in_sub_components_delete_entry() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        "test_dicts_in_sub_components_delete_entry",
        _declare_dicts_in_sub_components,
        environment=coco_env,
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    app.run()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "upsert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}

    del _source_data["D1"]["a"]
    app.run()
    assert DictsTarget.store.data == {
        "D1": {
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "delete": 1}

    # Re-insert after deletion
    _source_data["D1"]["a"] = 3
    _source_data["D1"]["c"] = 4
    app.run()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
            "c": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}
    assert coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "D1",
        coco.ROOT_PATH / "D1" / "setup",
    ]


##################################################################################


@coco.function
def _declare_dict_containers(
    scope: coco.Scope, names: Collection[str]
) -> dict[str, coco.EffectProvider[str]]:
    providers = {
        name: coco.declare_effect_with_child(scope, DictsTarget.effect(name, None))
        for name in names
    }
    return providers


@coco.function
def _declare_one_dict_data(
    scope: coco.Scope, name: str, provider: coco.EffectProvider[str]
) -> None:
    for key, value in _source_data[name].items():
        coco.declare_effect(scope, provider.effect(key, value))


@coco.function
def _declare_dict_containers_together(scope: coco.Scope) -> None:
    providers = coco.mount_run(
        scope / "setup", _declare_dict_containers, _source_data.keys()
    ).result()
    for name, provider in providers.items():
        coco.mount(scope / name, _declare_one_dict_data, name, provider)


def test_dicts_containers_together_insert() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        "test_dicts_containers_together_insert",
        _declare_dict_containers_together,
        environment=coco_env,
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    _source_data["D2"] = {}
    app.run()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {},
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "upsert": 2}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}

    _source_data["D2"]["c"] = 3
    _source_data["D3"] = {"a": 4}
    app.run()
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
    assert DictsTarget.store.collect_child_metrics() == {"sink": 2, "upsert": 2}
    assert coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "D1",
        coco.ROOT_PATH / "D2",
        coco.ROOT_PATH / "D3",
        coco.ROOT_PATH / "setup",
    ]


def test_dicts_containers_together_delete_dict() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        "test_dicts_containers_together_delete_dict",
        _declare_dict_containers_together,
        environment=coco_env,
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    _source_data["D2"] = {}
    app.run()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {},
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "upsert": 2}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}
    assert coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "D1",
        coco.ROOT_PATH / "D2",
        coco.ROOT_PATH / "setup",
    ]

    del _source_data["D1"]
    _source_data["D2"]["c"] = 3
    _source_data["D3"] = {"a": 4}
    app.run()
    assert DictsTarget.store.data == {
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
        "D3": {
            "a": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "upsert": 1, "delete": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 2, "upsert": 2}
    assert coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "D2",
        coco.ROOT_PATH / "D3",
        coco.ROOT_PATH / "setup",
    ]

    # Re-insert after deletion
    _source_data["D1"] = {"a": 3, "c": 4}
    app.run()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
            "c": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
        "D3": {
            "a": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "upsert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}
    assert coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "D1",
        coco.ROOT_PATH / "D2",
        coco.ROOT_PATH / "D3",
        coco.ROOT_PATH / "setup",
    ]


def test_dicts_containers_together_delete_entry() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        "test_dicts_containers_together_delete_entry",
        _declare_dict_containers_together,
        environment=coco_env,
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    app.run()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "upsert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}

    del _source_data["D1"]["a"]
    app.run()
    assert DictsTarget.store.data == {
        "D1": {
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "delete": 1}

    # Re-insert after deletion
    _source_data["D1"]["a"] = 3
    _source_data["D1"]["c"] = 4
    app.run()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
            "c": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}
    assert coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "D1",
        coco.ROOT_PATH / "setup",
    ]


@coco.function
async def _declare_dict_containers_together_async(scope: coco.Scope) -> None:
    providers = await coco_aio.mount_run(
        scope / "setup", _declare_dict_containers, _source_data.keys()
    ).result()
    for name, provider in providers.items():
        coco_aio.mount(scope / name, _declare_one_dict_data, name, provider)


@pytest.mark.asyncio
async def test_dicts_containers_together_insert_async() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco_aio.App(
        "test_dicts_containers_together_insert_async",
        _declare_dict_containers_together_async,
        environment=coco_env,
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    _source_data["D2"] = {}
    await app.run()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {},
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "upsert": 2}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}

    _source_data["D2"]["c"] = 3
    _source_data["D3"] = {"a": 4}
    await app.run()
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
    assert DictsTarget.store.collect_child_metrics() == {"sink": 2, "upsert": 2}
    assert coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "D1",
        coco.ROOT_PATH / "D2",
        coco.ROOT_PATH / "D3",
        coco.ROOT_PATH / "setup",
    ]


@pytest.mark.asyncio
async def test_dicts_containers_together_delete_dict_async() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco_aio.App(
        "test_dicts_containers_together_delete_dict_async",
        _declare_dict_containers_together_async,
        environment=coco_env,
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    _source_data["D2"] = {}
    await app.run()
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "upsert": 2}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}
    assert coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "D1",
        coco.ROOT_PATH / "D2",
        coco.ROOT_PATH / "setup",
    ]

    del _source_data["D1"]
    _source_data["D2"]["c"] = 3
    _source_data["D3"] = {"a": 4}
    await app.run()
    assert DictsTarget.store.data == {
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
        "D3": {
            "a": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "upsert": 1, "delete": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 2, "upsert": 2}
    assert coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "D2",
        coco.ROOT_PATH / "D3",
        coco.ROOT_PATH / "setup",
    ]

    # Re-insert after deletion
    _source_data["D1"] = {"a": 3, "c": 4}
    await app.run()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
            "c": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
        "D3": {
            "a": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "upsert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}
    assert coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "D1",
        coco.ROOT_PATH / "D2",
        coco.ROOT_PATH / "D3",
        coco.ROOT_PATH / "setup",
    ]


@pytest.mark.asyncio
async def test_dicts_containers_together_delete_entry_async() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco_aio.App(
        "test_dicts_containers_together_delete_entry_async",
        _declare_dict_containers_together_async,
        environment=coco_env,
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    await app.run()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "upsert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}

    del _source_data["D1"]["a"]
    await app.run()
    assert DictsTarget.store.data == {
        "D1": {
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "delete": 1}

    # Re-insert after deletion
    _source_data["D1"]["a"] = 3
    _source_data["D1"]["c"] = 4
    await app.run()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
            "c": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}
    assert coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "D1",
        coco.ROOT_PATH / "setup",
    ]


##################################################################################
# Test for cleanup of partially-built components


@coco.function
def _declare_one_dict_w_exception(scope: coco.Scope, name: str) -> None:
    dict_provider = coco.mount_run(
        scope / "setup", _declare_dict_container, name
    ).result()
    for key, value in _source_data[name].items():
        coco.declare_effect(scope, dict_provider.effect(key, value))
    raise ValueError("test exception")


@coco.function
def _declare_dicts_in_sub_components_w_exception(scope: coco.Scope) -> None:
    for name in _source_data.keys():
        coco.mount(scope / name, _declare_one_dict_w_exception, name)


def test_cleanup_partially_built_components() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        "test_cleanup_partially_built_components",
        _declare_dicts_in_sub_components_w_exception,
        environment=coco_env,
    )

    _source_data["D1"] = {"a": 1}
    app.run()
    assert DictsTarget.store.data == {"D1": {}}
    assert coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "D1",
        coco.ROOT_PATH / "D1" / "setup",
    ]

    del _source_data["D1"]
    app.run()
    assert DictsTarget.store.data == {}
    assert coco_inspect.list_stable_paths(app) == [coco.ROOT_PATH]


##################################################################################
# Test for restoring from GC-failed components


def test_retry_from_gc_failed_components() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        "test_retry_from_gc_failed_components",
        _declare_dicts_data_together,
        environment=coco_env,
    )

    _source_data["D1"] = {}
    app.run()
    assert DictsTarget.store.data == {"D1": {}}
    assert coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "dict",
        coco.ROOT_PATH / "dict" / "D1",
    ]

    # Inject an exception for GC
    del _source_data["D1"]
    try:
        DictsTarget.store.sink_exception = True
        with pytest.raises(Exception):
            app.run()
    finally:
        DictsTarget.store.sink_exception = False
    assert DictsTarget.store.data == {"D1": {}}
    assert coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "dict" / "D1",
    ]

    # After retry, it should proceed with GC
    app.run()
    assert DictsTarget.store.data == {}
    assert coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
    ]


def test_restore_from_gc_failed_components() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        "test_restore_from_gc_failed_components",
        _declare_dicts_data_together,
        environment=coco_env,
    )

    _source_data["D1"] = {}
    app.run()
    assert DictsTarget.store.data == {"D1": {}}
    assert coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "dict",
        coco.ROOT_PATH / "dict" / "D1",
    ]

    # Inject an exception for GC
    del _source_data["D1"]
    DictsTarget.store.sink_exception = True
    try:
        with pytest.raises(Exception):
            app.run()
    finally:
        DictsTarget.store.sink_exception = False
    assert DictsTarget.store.data == {"D1": {}}
    assert coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "dict" / "D1",
    ]

    # The entry reappears, and the previous failed GC shouldn't affect it
    _source_data["D1"] = {"a": 1}
    app.run()
    assert DictsTarget.store.data == {
        "D1": {"a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True)}
    }
    assert coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "dict",
        coco.ROOT_PATH / "dict" / "D1",
    ]

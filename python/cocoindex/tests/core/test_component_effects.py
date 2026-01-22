import cocoindex as coco
import cocoindex.asyncio as coco_aio
import cocoindex.inspect as coco_inspect
import pytest

from typing import Any, Collection

from .. import common
from ..common.effects import DictsTarget, DictDataWithPrev, AsyncDictsTarget

coco_env = common.create_test_env(__file__)

_source_data: dict[str, dict[str, Any]] = {}


##################################################################################


def _declare_dicts_data_together(scope: coco.Scope) -> None:
    for name, data in _source_data.items():
        single_dict_provider = coco.mount_run(
            DictsTarget.declare_dict_target,
            scope / "dict" / name,
            name,
        ).result()
        for key, value in data.items():
            coco.declare_target_state(scope, single_dict_provider.effect(key, value))


def test_dicts_data_together_insert() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        _declare_dicts_data_together,
        coco.AppConfig(name="test_dicts_data_together_insert", environment=coco_env),
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
    assert DictsTarget.store.metrics.collect() == {"sink": 2, "insert": 2}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}

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
    assert DictsTarget.store.metrics.collect() == {"sink": 3, "insert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 2, "upsert": 2}
    assert coco_inspect.list_stable_paths_sync(app) == [
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
        _declare_dicts_data_together,
        coco.AppConfig(
            name="test_dicts_data_together_delete_dict", environment=coco_env
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
    assert DictsTarget.store.metrics.collect() == {"sink": 2, "insert": 2}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}
    assert coco_inspect.list_stable_paths_sync(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "dict",
        coco.ROOT_PATH / "dict" / "D1",
        coco.ROOT_PATH / "dict" / "D2",
    ]

    del _source_data["D1"]
    _source_data["D2"]["c"] = 3
    _source_data["D3"] = {"a": 4}
    app.update()
    assert DictsTarget.store.data == {
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
        "D3": {
            "a": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 3, "insert": 1, "delete": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 2, "upsert": 2}
    assert coco_inspect.list_stable_paths_sync(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "dict",
        coco.ROOT_PATH / "dict" / "D2",
        coco.ROOT_PATH / "dict" / "D3",
    ]

    # Re-insert after deletion
    _source_data["D1"] = {"a": 3, "c": 4}
    app.update()
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
    assert DictsTarget.store.metrics.collect() == {"sink": 3, "insert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}
    assert coco_inspect.list_stable_paths_sync(app) == [
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
        _declare_dicts_data_together,
        coco.AppConfig(
            name="test_dicts_data_together_delete_entry", environment=coco_env
        ),
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    app.update()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "insert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}

    del _source_data["D1"]["a"]
    app.update()
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
    app.update()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
            "c": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}
    assert coco_inspect.list_stable_paths_sync(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "dict",
        coco.ROOT_PATH / "dict" / "D1",
    ]


##################################################################################


def _declare_one_dict(scope: coco.Scope, name: str) -> None:
    dict_provider = coco.mount_run(
        DictsTarget.declare_dict_target, scope / "setup", name
    ).result()
    for key, value in _source_data[name].items():
        coco.declare_target_state(scope, dict_provider.effect(key, value))


def _declare_dicts_in_sub_components(scope: coco.Scope) -> None:
    for name in _source_data.keys():
        coco.mount(_declare_one_dict, scope / name, name)


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
    assert DictsTarget.store.metrics.collect() == {"sink": 2, "insert": 2}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}

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
    assert DictsTarget.store.metrics.collect() == {"sink": 3, "insert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 2, "upsert": 2}
    assert coco_inspect.list_stable_paths_sync(app) == [
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
        _declare_dicts_in_sub_components,
        coco.AppConfig(
            name="test_dicts_in_sub_components_delete_dict", environment=coco_env
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
    assert DictsTarget.store.metrics.collect() == {"sink": 2, "insert": 2}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}
    assert coco_inspect.list_stable_paths_sync(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "D1",
        coco.ROOT_PATH / "D1" / "setup",
        coco.ROOT_PATH / "D2",
        coco.ROOT_PATH / "D2" / "setup",
    ]

    del _source_data["D1"]
    _source_data["D2"]["c"] = 3
    _source_data["D3"] = {"a": 4}
    app.update()
    assert DictsTarget.store.data == {
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
        "D3": {
            "a": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 3, "insert": 1, "delete": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 2, "upsert": 2}
    assert coco_inspect.list_stable_paths_sync(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "D2",
        coco.ROOT_PATH / "D2" / "setup",
        coco.ROOT_PATH / "D3",
        coco.ROOT_PATH / "D3" / "setup",
    ]

    # Re-insert after deletion
    _source_data["D1"] = {"a": 3, "c": 4}
    app.update()
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
    assert DictsTarget.store.metrics.collect() == {"sink": 3, "insert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}
    assert coco_inspect.list_stable_paths_sync(app) == [
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
        _declare_dicts_in_sub_components,
        coco.AppConfig(
            name="test_dicts_in_sub_components_delete_entry", environment=coco_env
        ),
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    app.update()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "insert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}

    del _source_data["D1"]["a"]
    app.update()
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
    app.update()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
            "c": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}
    assert coco_inspect.list_stable_paths_sync(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "D1",
        coco.ROOT_PATH / "D1" / "setup",
    ]


##################################################################################


def _declare_dict_containers(
    scope: coco.Scope, names: Collection[str]
) -> dict[str, coco.PendingTargetStateProvider[str]]:
    providers = {name: DictsTarget.declare_dict_target(scope, name) for name in names}
    return providers


def _declare_one_dict_data(
    scope: coco.Scope, name: str, provider: coco.TargetStateProvider[str]
) -> None:
    for key, value in _source_data[name].items():
        coco.declare_target_state(scope, provider.effect(key, value))


def _declare_dict_containers_together(scope: coco.Scope) -> None:
    providers = coco.mount_run(
        _declare_dict_containers, scope / "setup", _source_data.keys()
    ).result()
    for name, provider in providers.items():
        coco.mount(_declare_one_dict_data, scope / name, name, provider)


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
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "insert": 2}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}

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
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "insert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 2, "upsert": 2}
    assert coco_inspect.list_stable_paths_sync(app) == [
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
        _declare_dict_containers_together,
        coco.AppConfig(
            name="test_dicts_containers_together_delete_dict", environment=coco_env
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
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "insert": 2}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}
    assert coco_inspect.list_stable_paths_sync(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "D1",
        coco.ROOT_PATH / "D2",
        coco.ROOT_PATH / "setup",
    ]

    del _source_data["D1"]
    _source_data["D2"]["c"] = 3
    _source_data["D3"] = {"a": 4}
    app.update()
    assert DictsTarget.store.data == {
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
        "D3": {
            "a": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "insert": 1, "delete": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 2, "upsert": 2}
    assert coco_inspect.list_stable_paths_sync(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "D2",
        coco.ROOT_PATH / "D3",
        coco.ROOT_PATH / "setup",
    ]

    # Re-insert after deletion
    _source_data["D1"] = {"a": 3, "c": 4}
    app.update()
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
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "insert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}
    assert coco_inspect.list_stable_paths_sync(app) == [
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
        _declare_dict_containers_together,
        coco.AppConfig(
            name="test_dicts_containers_together_delete_entry", environment=coco_env
        ),
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    app.update()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "insert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}

    del _source_data["D1"]["a"]
    app.update()
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
    app.update()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
            "c": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}
    assert coco_inspect.list_stable_paths_sync(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "D1",
        coco.ROOT_PATH / "setup",
    ]


async def _declare_dict_containers_together_async(scope: coco.Scope) -> None:
    providers = await coco_aio.mount_run(
        _declare_dict_containers, scope / "setup", _source_data.keys()
    ).result()
    for name, provider in providers.items():
        coco_aio.mount(_declare_one_dict_data, scope / name, name, provider)


@pytest.mark.asyncio
async def test_dicts_containers_together_insert_async() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco_aio.App(
        _declare_dict_containers_together_async,
        coco.AppConfig(
            name="test_dicts_containers_together_insert_async", environment=coco_env
        ),
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    _source_data["D2"] = {}
    await app.update()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {},
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "insert": 2}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}

    _source_data["D2"]["c"] = 3
    _source_data["D3"] = {"a": 4}
    await app.update()
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
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "insert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 2, "upsert": 2}
    assert await coco_inspect.list_stable_paths(app) == [
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
        _declare_dict_containers_together_async,
        coco.AppConfig(
            name="test_dicts_containers_together_delete_dict_async",
            environment=coco_env,
        ),
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    _source_data["D2"] = {}
    await app.update()
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "insert": 2}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}
    assert await coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "D1",
        coco.ROOT_PATH / "D2",
        coco.ROOT_PATH / "setup",
    ]

    del _source_data["D1"]
    _source_data["D2"]["c"] = 3
    _source_data["D3"] = {"a": 4}
    await app.update()
    assert DictsTarget.store.data == {
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
        "D3": {
            "a": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "insert": 1, "delete": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 2, "upsert": 2}
    assert await coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "D2",
        coco.ROOT_PATH / "D3",
        coco.ROOT_PATH / "setup",
    ]

    # Re-insert after deletion
    _source_data["D1"] = {"a": 3, "c": 4}
    await app.update()
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
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "insert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}
    assert await coco_inspect.list_stable_paths(app) == [
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
        _declare_dict_containers_together_async,
        coco.AppConfig(
            name="test_dicts_containers_together_delete_entry_async",
            environment=coco_env,
        ),
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    await app.update()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "insert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}

    del _source_data["D1"]["a"]
    await app.update()
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
    await app.update()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
            "c": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}
    assert await coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "D1",
        coco.ROOT_PATH / "setup",
    ]


##################################################################################
# Test for proceeding with failed creation


def test_proceed_with_failed_creation() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        _declare_dicts_data_together,
        coco.AppConfig(name="test_proceed_with_failed_creation", environment=coco_env),
    )

    _source_data["D1"] = {"a": 1}
    try:
        DictsTarget.store.sink_exception = True
        with pytest.raises(Exception):
            app.update()
    finally:
        DictsTarget.store.sink_exception = False
    assert DictsTarget.store.data == {}
    app.update()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": 1, "upsert": 1}
    assert coco_inspect.list_stable_paths_sync(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "dict",
        coco.ROOT_PATH / "dict" / "D1",
    ]


##################################################################################
# Test for cleanup of partially-built components


def _declare_one_dict_w_exception(scope: coco.Scope, name: str) -> None:
    dict_provider = coco.mount_run(
        DictsTarget.declare_dict_target, scope / "setup", name
    ).result()
    for key, value in _source_data[name].items():
        coco.declare_target_state(scope, dict_provider.effect(key, value))
    raise ValueError("injected test exception (which is expected)")


def _declare_dicts_in_sub_components_w_exception(scope: coco.Scope) -> None:
    for name in _source_data.keys():
        coco.mount(_declare_one_dict_w_exception, scope / name, name)


def test_cleanup_partially_built_components() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        _declare_dicts_in_sub_components_w_exception,
        coco.AppConfig(
            name="test_cleanup_partially_built_components", environment=coco_env
        ),
    )

    _source_data["D1"] = {"a": 1}
    app.update()
    assert DictsTarget.store.data == {"D1": {}}
    assert coco_inspect.list_stable_paths_sync(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "D1",
        coco.ROOT_PATH / "D1" / "setup",
    ]

    del _source_data["D1"]
    app.update()
    assert DictsTarget.store.data == {}
    assert coco_inspect.list_stable_paths_sync(app) == [coco.ROOT_PATH]


##################################################################################
# Test for restoring from GC-failed components


def test_retry_from_gc_failed_components() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        _declare_dicts_data_together,
        coco.AppConfig(
            name="test_retry_from_gc_failed_components", environment=coco_env
        ),
    )

    _source_data["D1"] = {}
    app.update()
    assert DictsTarget.store.data == {"D1": {}}
    assert coco_inspect.list_stable_paths_sync(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "dict",
        coco.ROOT_PATH / "dict" / "D1",
    ]

    # Inject an exception for GC
    del _source_data["D1"]
    try:
        DictsTarget.store.sink_exception = True
        app.update()
    finally:
        DictsTarget.store.sink_exception = False
    assert DictsTarget.store.data == {"D1": {}}
    assert coco_inspect.list_stable_paths_sync(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "dict" / "D1",
    ]

    # After retry, it should proceed with GC
    app.update()
    assert DictsTarget.store.data == {}
    assert coco_inspect.list_stable_paths_sync(app) == [
        coco.ROOT_PATH,
    ]


def test_restore_from_gc_failed_components() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        _declare_dicts_data_together,
        coco.AppConfig(
            name="test_restore_from_gc_failed_components", environment=coco_env
        ),
    )

    _source_data["D1"] = {}
    app.update()
    assert DictsTarget.store.data == {"D1": {}}
    assert coco_inspect.list_stable_paths_sync(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "dict",
        coco.ROOT_PATH / "dict" / "D1",
    ]

    # Inject an exception for GC
    del _source_data["D1"]
    DictsTarget.store.sink_exception = True
    try:
        app.update()
    finally:
        DictsTarget.store.sink_exception = False
    assert DictsTarget.store.data == {"D1": {}}
    assert coco_inspect.list_stable_paths_sync(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "dict" / "D1",
    ]

    # The entry reappears, and the previous failed GC shouldn't affect it
    _source_data["D1"] = {"a": 1}
    app.update()
    assert DictsTarget.store.data == {
        "D1": {"a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True)}
    }
    assert coco_inspect.list_stable_paths_sync(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "dict",
        coco.ROOT_PATH / "dict" / "D1",
    ]


##################################################################################
# Test for async effects


async def _declare_async_dicts_data_together(scope: coco.Scope) -> None:
    for name, data in _source_data.items():
        single_dict_provider = await coco_aio.mount_run(
            AsyncDictsTarget.declare_dict_target,
            scope / "dict" / name,
            name,
        ).result()
        for key, value in data.items():
            coco.declare_target_state(scope, single_dict_provider.effect(key, value))


@pytest.mark.asyncio
async def test_async_dicts() -> None:
    AsyncDictsTarget.store.clear()
    _source_data.clear()

    app = coco_aio.App(
        _declare_async_dicts_data_together,
        coco.AppConfig(name="test_async_dicts", environment=coco_env),
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    _source_data["D2"] = {}
    await app.update()
    assert AsyncDictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {},
    }
    assert AsyncDictsTarget.store.metrics.collect() == {"sink": 2, "insert": 2}
    assert AsyncDictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}

    _source_data["D2"]["c"] = 3
    _source_data["D3"] = {"a": 4}
    await app.update()
    assert AsyncDictsTarget.store.data == {
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
    assert AsyncDictsTarget.store.metrics.collect() == {"sink": 3, "insert": 1}
    assert AsyncDictsTarget.store.collect_child_metrics() == {"sink": 2, "upsert": 2}
    assert await coco_inspect.list_stable_paths(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "dict",
        coco.ROOT_PATH / "dict" / "D1",
        coco.ROOT_PATH / "dict" / "D2",
        coco.ROOT_PATH / "dict" / "D3",
    ]


def test_async_dicts_sync_app() -> None:
    AsyncDictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        _declare_async_dicts_data_together,
        coco.AppConfig(name="test_async_dicts_sync_app", environment=coco_env),
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    _source_data["D2"] = {}
    app.update()
    assert AsyncDictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {},
    }
    assert AsyncDictsTarget.store.metrics.collect() == {"sink": 2, "insert": 2}
    assert AsyncDictsTarget.store.collect_child_metrics() == {"sink": 1, "upsert": 2}

    _source_data["D2"]["c"] = 3
    _source_data["D3"] = {"a": 4}
    app.update()
    assert AsyncDictsTarget.store.data == {
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
    assert AsyncDictsTarget.store.metrics.collect() == {"sink": 3, "insert": 1}
    assert AsyncDictsTarget.store.collect_child_metrics() == {"sink": 2, "upsert": 2}
    assert coco_inspect.list_stable_paths_sync(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "dict",
        coco.ROOT_PATH / "dict" / "D1",
        coco.ROOT_PATH / "dict" / "D2",
        coco.ROOT_PATH / "dict" / "D3",
    ]

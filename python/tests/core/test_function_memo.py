import asyncio

import cocoindex as coco
import pytest
from dataclasses import dataclass
from typing import Any

from cocoindex._internal.runner import Runner

from tests import common
from tests.common.target_states import (
    DictDataWithPrev,
    GlobalDictTarget,
    Metrics,
)

coco_env = common.create_test_env(__file__)


@dataclass(frozen=True)
class SourceDataEntry:
    name: str
    version: int
    content: str

    def __coco_memo_key__(self) -> object:
        return (self.name, self.version)


@dataclass
class DictSourceDataEntry:
    name: str
    version: int
    content: dict[str, SourceDataEntry]

    def __coco_memo_key__(self) -> object:
        return (self.name, self.version)


_plain_source_data: dict[str, SourceDataEntry] = {}
_dict_source_data: dict[str, DictSourceDataEntry] = {}
_metrics = Metrics()


@coco.function(memo=True)
def _transform_entry(entry: SourceDataEntry) -> str:
    _metrics.increment("call.transform_entry")
    return f"processed: {entry.content}"


@coco.function
def _process_plain_source_data() -> None:
    for key, value in _plain_source_data.items():
        transformed_value = _transform_entry(value)
        coco.declare_target_state(GlobalDictTarget.target_state(key, transformed_value))


def test_memo_pure_function() -> None:
    GlobalDictTarget.store.clear()
    _plain_source_data.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(name="test_memo_pure_function", environment=coco_env),
        _process_plain_source_data,
    )

    _plain_source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA1")
    _plain_source_data["B"] = SourceDataEntry(name="B", version=1, content="contentB1")
    app.update()
    assert _metrics.collect() == {"call.transform_entry": 2}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="processed: contentA1", prev=[], prev_may_be_missing=True
        ),
        "B": DictDataWithPrev(
            data="processed: contentB1", prev=[], prev_may_be_missing=True
        ),
    }

    _plain_source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA2")
    _plain_source_data["B"] = SourceDataEntry(name="B", version=2, content="contentB2")
    app.update()
    assert _metrics.collect() == {"call.transform_entry": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="processed: contentA1", prev=[], prev_may_be_missing=True
        ),
        "B": DictDataWithPrev(
            data="processed: contentB2",
            prev=["processed: contentB1"],
            prev_may_be_missing=False,
        ),
    }

    app.update()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="processed: contentA1", prev=[], prev_may_be_missing=True
        ),
        "B": DictDataWithPrev(
            data="processed: contentB2",
            prev=["processed: contentB1"],
            prev_may_be_missing=False,
        ),
    }

    _plain_source_data.clear()
    app.update()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == {}

    # The same version reappears after deletion. Verify cached values are not used.
    _plain_source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA1")
    app.update()
    assert _metrics.collect() == {"call.transform_entry": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="processed: contentA1", prev=[], prev_may_be_missing=True
        ),
    }


@coco.function(memo=True)
async def _transform_entry_async(entry: SourceDataEntry) -> str:
    _metrics.increment("call.transform_entry_async")
    return f"processed: {entry.content}"


@coco.function
async def _process_plain_source_data_async() -> None:
    for key, value in _plain_source_data.items():
        transformed_value = await _transform_entry_async(value)
        coco.declare_target_state(GlobalDictTarget.target_state(key, transformed_value))


def test_memo_pure_function_async() -> None:
    GlobalDictTarget.store.clear()
    _plain_source_data.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(name="test_memo_pure_function_async", environment=coco_env),
        _process_plain_source_data_async,
    )

    _plain_source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA1")
    _plain_source_data["B"] = SourceDataEntry(name="B", version=1, content="contentB1")
    app.update()
    assert _metrics.collect() == {"call.transform_entry_async": 2}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="processed: contentA1", prev=[], prev_may_be_missing=True
        ),
        "B": DictDataWithPrev(
            data="processed: contentB1", prev=[], prev_may_be_missing=True
        ),
    }

    _plain_source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA2")
    _plain_source_data["B"] = SourceDataEntry(name="B", version=2, content="contentB2")
    app.update()
    assert _metrics.collect() == {"call.transform_entry_async": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="processed: contentA1", prev=[], prev_may_be_missing=True
        ),
        "B": DictDataWithPrev(
            data="processed: contentB2",
            prev=["processed: contentB1"],
            prev_may_be_missing=False,
        ),
    }

    app.update()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="processed: contentA1", prev=[], prev_may_be_missing=True
        ),
        "B": DictDataWithPrev(
            data="processed: contentB2",
            prev=["processed: contentB1"],
            prev_may_be_missing=False,
        ),
    }

    _plain_source_data.clear()
    app.update()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == {}

    # The same version reappears after deletion. Verify cached values are not used.
    _plain_source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA1")
    app.update()
    assert _metrics.collect() == {"call.transform_entry_async": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="processed: contentA1", prev=[], prev_may_be_missing=True
        ),
    }


@coco.function(memo=True)
def _declare_data_entry(key: str, entry: SourceDataEntry) -> None:
    _metrics.increment("call.declare_data_entry")
    coco.declare_target_state(GlobalDictTarget.target_state(key, entry.content))


@coco.function
def _declare_plain_data() -> None:
    for key, value in _plain_source_data.items():
        _declare_data_entry(key, value)


def test_memo_function_with_target_states() -> None:
    GlobalDictTarget.store.clear()
    _plain_source_data.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_memo_function_with_target_states", environment=coco_env
        ),
        _declare_plain_data,
    )

    _plain_source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA1")
    _plain_source_data["B"] = SourceDataEntry(name="B", version=1, content="contentB1")
    app.update()
    assert _metrics.collect() == {"call.declare_data_entry": 2}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA1", prev=[], prev_may_be_missing=True),
        "B": DictDataWithPrev(data="contentB1", prev=[], prev_may_be_missing=True),
    }

    _plain_source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA2")
    _plain_source_data["B"] = SourceDataEntry(name="B", version=2, content="contentB2")
    app.update()
    assert _metrics.collect() == {"call.declare_data_entry": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA1", prev=[], prev_may_be_missing=True),
        "B": DictDataWithPrev(
            data="contentB2", prev=["contentB1"], prev_may_be_missing=False
        ),
    }

    app.update()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA1", prev=[], prev_may_be_missing=True),
        "B": DictDataWithPrev(
            data="contentB2", prev=["contentB1"], prev_may_be_missing=False
        ),
    }

    _plain_source_data.clear()
    app.update()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == {}

    # The same version reappears after deletion. Verify cached values are not used.
    _plain_source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA1")
    app.update()
    assert _metrics.collect() == {"call.declare_data_entry": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA1", prev=[], prev_may_be_missing=True),
    }


@coco.function(memo=True)
async def _declare_data_entry_async(key: str, entry: SourceDataEntry) -> None:
    _metrics.increment("call.declare_data_entry_async")
    coco.declare_target_state(GlobalDictTarget.target_state(key, entry.content))


@coco.function
async def _declare_plain_data_async() -> None:
    for key, value in _plain_source_data.items():
        await _declare_data_entry_async(key, value)


def test_memo_function_with_target_states_async() -> None:
    GlobalDictTarget.store.clear()
    _plain_source_data.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_memo_function_with_target_states_async", environment=coco_env
        ),
        _declare_plain_data_async,
    )

    _plain_source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA1")
    _plain_source_data["B"] = SourceDataEntry(name="B", version=1, content="contentB1")
    app.update()
    assert _metrics.collect() == {"call.declare_data_entry_async": 2}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA1", prev=[], prev_may_be_missing=True),
        "B": DictDataWithPrev(data="contentB1", prev=[], prev_may_be_missing=True),
    }

    _plain_source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA2")
    _plain_source_data["B"] = SourceDataEntry(name="B", version=2, content="contentB2")
    app.update()
    assert _metrics.collect() == {"call.declare_data_entry_async": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA1", prev=[], prev_may_be_missing=True),
        "B": DictDataWithPrev(
            data="contentB2", prev=["contentB1"], prev_may_be_missing=False
        ),
    }

    app.update()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA1", prev=[], prev_may_be_missing=True),
        "B": DictDataWithPrev(
            data="contentB2", prev=["contentB1"], prev_may_be_missing=False
        ),
    }

    _plain_source_data.clear()
    app.update()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == {}

    # The same version reappears after deletion. Verify cached values are not used.
    _plain_source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA1")
    app.update()
    assert _metrics.collect() == {"call.declare_data_entry_async": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA1", prev=[], prev_may_be_missing=True),
    }


def test_memo_function_with_target_states_with_exception() -> None:
    GlobalDictTarget.store.clear()
    _plain_source_data.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_memo_function_with_target_states_with_exception",
            environment=coco_env,
        ),
        _declare_plain_data,
    )

    _plain_source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA1")

    try:
        GlobalDictTarget.store.sink_exception = True
        with pytest.raises(Exception):
            app.update()
    finally:
        GlobalDictTarget.store.sink_exception = False
    assert _metrics.collect() == {"call.declare_data_entry": 1}
    assert GlobalDictTarget.store.data == {}

    app.update()
    assert _metrics.collect() == {"call.declare_data_entry": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="contentA1", prev=["contentA1"], prev_may_be_missing=True
        ),
    }


@coco.function(memo=True)
def _declare_dict_data_entry(entry: DictSourceDataEntry) -> None:
    _metrics.increment("call.declare_dict_data_entry")
    for key, value in entry.content.items():
        _declare_data_entry(key, value)


@coco.function
def _declare_dict_data() -> None:
    for entry in _dict_source_data.values():
        _declare_dict_data_entry(entry)


def test_memo_nested_functions_with_target_states() -> None:
    GlobalDictTarget.store.clear()
    _dict_source_data.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_memo_nested_functions_with_target_states", environment=coco_env
        ),
        _declare_dict_data,
    )

    _dict_source_data["D1"] = DictSourceDataEntry(
        name="D1",
        version=1,
        content={
            "1A": SourceDataEntry(name="1A", version=1, content="content1A"),
            "1B": SourceDataEntry(name="1B", version=1, content="content1B"),
        },
    )
    _dict_source_data["D2"] = DictSourceDataEntry(
        name="D2",
        version=1,
        content={"2A": SourceDataEntry(name="2A", version=1, content="content2A")},
    )
    app.update()
    assert _metrics.collect() == {
        "call.declare_dict_data_entry": 2,
        "call.declare_data_entry": 3,
    }
    assert GlobalDictTarget.store.data == {
        "1A": DictDataWithPrev(data="content1A", prev=[], prev_may_be_missing=True),
        "1B": DictDataWithPrev(data="content1B", prev=[], prev_may_be_missing=True),
        "2A": DictDataWithPrev(data="content2A", prev=[], prev_may_be_missing=True),
    }

    app.update()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == {
        "1A": DictDataWithPrev(data="content1A", prev=[], prev_may_be_missing=True),
        "1B": DictDataWithPrev(data="content1B", prev=[], prev_may_be_missing=True),
        "2A": DictDataWithPrev(data="content2A", prev=[], prev_may_be_missing=True),
    }

    _dict_source_data["D1"].version = 2
    _dict_source_data["D1"].content["1A"] = SourceDataEntry(
        name="1A", version=2, content="content1A2"
    )
    del _dict_source_data["D1"].content["1B"]
    app.update()
    assert _metrics.collect() == {
        "call.declare_dict_data_entry": 1,
        "call.declare_data_entry": 1,
    }
    assert GlobalDictTarget.store.data == {
        "1A": DictDataWithPrev(
            data="content1A2", prev=["content1A"], prev_may_be_missing=False
        ),
        "2A": DictDataWithPrev(data="content2A", prev=[], prev_may_be_missing=True),
    }

    del _dict_source_data["D1"]
    app.update()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == {
        "2A": DictDataWithPrev(data="content2A", prev=[], prev_may_be_missing=True),
    }

    # The same version reappears after deletion. Verify cached values are not used.
    _dict_source_data["D1"] = DictSourceDataEntry(
        name="D1",
        version=2,
        content={
            "1A": SourceDataEntry(name="1A", version=2, content="content1A2.2"),
        },
    )
    app.update()
    assert _metrics.collect() == {
        "call.declare_dict_data_entry": 1,
        "call.declare_data_entry": 1,
    }
    assert GlobalDictTarget.store.data == {
        "1A": DictDataWithPrev(data="content1A2.2", prev=[], prev_may_be_missing=True),
        "2A": DictDataWithPrev(data="content2A", prev=[], prev_may_be_missing=True),
    }


@coco.function(memo=True)
def _declare_dict_data_entry_w_components(entry: DictSourceDataEntry) -> None:
    _metrics.increment("call.declare_dict_data_entry_w_components")
    for key, value in entry.content.items():
        coco.mount(coco.component_subpath(key), _declare_data_entry, key, value)


@coco.function
def _declare_dict_data_w_components() -> None:
    for entry in _dict_source_data.values():
        _declare_dict_data_entry_w_components(entry)


def test_memo_nested_functions_with_components() -> None:
    GlobalDictTarget.store.clear()
    _dict_source_data.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_memo_nested_functions_with_components", environment=coco_env
        ),
        _declare_dict_data_w_components,
    )

    _dict_source_data["D1"] = DictSourceDataEntry(
        name="D1",
        version=1,
        content={
            "1A": SourceDataEntry(name="1A", version=1, content="content1A"),
            "1B": SourceDataEntry(name="1B", version=1, content="content1B"),
        },
    )
    _dict_source_data["D2"] = DictSourceDataEntry(
        name="D2",
        version=1,
        content={"2A": SourceDataEntry(name="2A", version=1, content="content2A")},
    )
    app.update()
    assert _metrics.collect() == {
        "call.declare_dict_data_entry_w_components": 2,
        "call.declare_data_entry": 3,
    }
    assert GlobalDictTarget.store.data == {
        "1A": DictDataWithPrev(data="content1A", prev=[], prev_may_be_missing=True),
        "1B": DictDataWithPrev(data="content1B", prev=[], prev_may_be_missing=True),
        "2A": DictDataWithPrev(data="content2A", prev=[], prev_may_be_missing=True),
    }

    app.update()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == {
        "1A": DictDataWithPrev(data="content1A", prev=[], prev_may_be_missing=True),
        "1B": DictDataWithPrev(data="content1B", prev=[], prev_may_be_missing=True),
        "2A": DictDataWithPrev(data="content2A", prev=[], prev_may_be_missing=True),
    }

    _dict_source_data["D1"].version = 2
    _dict_source_data["D1"].content["1A"] = SourceDataEntry(
        name="1A", version=2, content="content1A2"
    )
    del _dict_source_data["D1"].content["1B"]
    app.update()
    assert _metrics.collect() == {
        "call.declare_dict_data_entry_w_components": 1,
        "call.declare_data_entry": 1,
    }
    assert GlobalDictTarget.store.data == {
        "1A": DictDataWithPrev(
            data="content1A2", prev=["content1A"], prev_may_be_missing=False
        ),
        "2A": DictDataWithPrev(data="content2A", prev=[], prev_may_be_missing=True),
    }

    del _dict_source_data["D1"]
    app.update()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == {
        "2A": DictDataWithPrev(data="content2A", prev=[], prev_may_be_missing=True),
    }

    # The same version reappears after deletion. Verify cached values are not used.
    _dict_source_data["D1"] = DictSourceDataEntry(
        name="D1",
        version=2,
        content={
            "1A": SourceDataEntry(name="1A", version=2, content="content1A2.2"),
        },
    )
    app.update()
    assert _metrics.collect() == {
        "call.declare_dict_data_entry_w_components": 1,
        "call.declare_data_entry": 1,
    }
    assert GlobalDictTarget.store.data == {
        "1A": DictDataWithPrev(data="content1A2.2", prev=[], prev_may_be_missing=True),
        "2A": DictDataWithPrev(data="content2A", prev=[], prev_may_be_missing=True),
    }


def test_memo_nested_functions_with_components_with_exception() -> None:
    GlobalDictTarget.store.clear()
    _dict_source_data.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_memo_nested_functions_with_components_with_exception",
            environment=coco_env,
        ),
        _declare_dict_data_w_components,
    )

    _dict_source_data["D1"] = DictSourceDataEntry(
        name="D1",
        version=1,
        content={
            "1A": SourceDataEntry(name="1A", version=1, content="content1A"),
        },
    )
    try:
        GlobalDictTarget.store.sink_exception = True
        app.update()
    finally:
        GlobalDictTarget.store.sink_exception = False
    assert _metrics.collect() == {
        "call.declare_dict_data_entry_w_components": 1,
        "call.declare_data_entry": 1,
    }
    assert GlobalDictTarget.store.data == {}

    app.update()
    assert _metrics.collect() == {
        "call.declare_dict_data_entry_w_components": 1,
        "call.declare_data_entry": 1,
    }
    assert GlobalDictTarget.store.data == {
        "1A": DictDataWithPrev(
            data="content1A", prev=["content1A"], prev_may_be_missing=True
        ),
    }


@coco.function(memo=True)
async def _declare_dict_data_entry_w_components_async(
    entry: DictSourceDataEntry,
) -> None:
    _metrics.increment("call.declare_dict_data_entry_w_components_async")
    for key, value in entry.content.items():
        coco.mount(coco.component_subpath(key), _declare_data_entry, key, value)


@coco.function
async def _declare_dict_data_w_components_async() -> None:
    for entry in _dict_source_data.values():
        await _declare_dict_data_entry_w_components_async(entry)


def test_memo_nested_functions_with_components_async() -> None:
    GlobalDictTarget.store.clear()
    _dict_source_data.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_memo_nested_functions_with_components_async",
            environment=coco_env,
        ),
        _declare_dict_data_w_components_async,
    )

    _dict_source_data["D1"] = DictSourceDataEntry(
        name="D1",
        version=1,
        content={
            "1A": SourceDataEntry(name="1A", version=1, content="content1A"),
            "1B": SourceDataEntry(name="1B", version=1, content="content1B"),
        },
    )
    _dict_source_data["D2"] = DictSourceDataEntry(
        name="D2",
        version=1,
        content={"2A": SourceDataEntry(name="2A", version=1, content="content2A")},
    )
    app.update()
    assert _metrics.collect() == {
        "call.declare_dict_data_entry_w_components_async": 2,
        "call.declare_data_entry": 3,
    }
    assert GlobalDictTarget.store.data == {
        "1A": DictDataWithPrev(data="content1A", prev=[], prev_may_be_missing=True),
        "1B": DictDataWithPrev(data="content1B", prev=[], prev_may_be_missing=True),
        "2A": DictDataWithPrev(data="content2A", prev=[], prev_may_be_missing=True),
    }

    app.update()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == {
        "1A": DictDataWithPrev(data="content1A", prev=[], prev_may_be_missing=True),
        "1B": DictDataWithPrev(data="content1B", prev=[], prev_may_be_missing=True),
        "2A": DictDataWithPrev(data="content2A", prev=[], prev_may_be_missing=True),
    }

    _dict_source_data["D1"].version = 2
    _dict_source_data["D1"].content["1A"] = SourceDataEntry(
        name="1A", version=2, content="content1A2"
    )
    del _dict_source_data["D1"].content["1B"]
    app.update()
    assert _metrics.collect() == {
        "call.declare_dict_data_entry_w_components_async": 1,
        "call.declare_data_entry": 1,
    }
    assert GlobalDictTarget.store.data == {
        "1A": DictDataWithPrev(
            data="content1A2", prev=["content1A"], prev_may_be_missing=False
        ),
        "2A": DictDataWithPrev(data="content2A", prev=[], prev_may_be_missing=True),
    }

    del _dict_source_data["D1"]
    app.update()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == {
        "2A": DictDataWithPrev(data="content2A", prev=[], prev_may_be_missing=True),
    }

    # The same version reappears after deletion. Verify cached values are not used.
    _dict_source_data["D1"] = DictSourceDataEntry(
        name="D1",
        version=2,
        content={
            "1A": SourceDataEntry(name="1A", version=2, content="content1A2.2"),
        },
    )
    app.update()
    assert _metrics.collect() == {
        "call.declare_dict_data_entry_w_components_async": 1,
        "call.declare_data_entry": 1,
    }
    assert GlobalDictTarget.store.data == {
        "1A": DictDataWithPrev(data="content1A2.2", prev=[], prev_may_be_missing=True),
        "2A": DictDataWithPrev(data="content2A", prev=[], prev_may_be_missing=True),
    }


def test_memo_nested_functions_with_components_with_exception_async() -> None:
    GlobalDictTarget.store.clear()
    _dict_source_data.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_memo_nested_functions_with_components_with_exception_async",
            environment=coco_env,
        ),
        _declare_dict_data_w_components_async,
    )

    _dict_source_data["D1"] = DictSourceDataEntry(
        name="D1",
        version=1,
        content={
            "1A": SourceDataEntry(name="1A", version=1, content="content1A"),
        },
    )
    try:
        GlobalDictTarget.store.sink_exception = True
        app.update()
    finally:
        GlobalDictTarget.store.sink_exception = False
    assert _metrics.collect() == {
        "call.declare_dict_data_entry_w_components_async": 1,
        "call.declare_data_entry": 1,
    }
    assert GlobalDictTarget.store.data == {}

    app.update()
    assert _metrics.collect() == {
        "call.declare_dict_data_entry_w_components_async": 1,
        "call.declare_data_entry": 1,
    }
    assert GlobalDictTarget.store.data == {
        "1A": DictDataWithPrev(
            data="content1A", prev=["content1A"], prev_may_be_missing=True
        ),
    }


# ============================================================================
# Memo with batching tests
# ============================================================================


@coco.function(memo=True, batching=True)
def _batched_transform(inputs: list[SourceDataEntry]) -> list[str]:
    for inp in inputs:
        _metrics.increment("call.batched_transform")
    return [f"batched: {entry.content}" for entry in inputs]


@coco.function
async def _process_with_batched_transform() -> None:
    for key, value in _plain_source_data.items():
        transformed_value = await _batched_transform(value)
        coco.declare_target_state(GlobalDictTarget.target_state(key, transformed_value))


def test_memo_with_batching() -> None:
    """Test that memo=True works correctly with batching=True."""
    GlobalDictTarget.store.clear()
    _plain_source_data.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(name="test_memo_with_batching", environment=coco_env),
        _process_with_batched_transform,
    )

    _plain_source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA1")
    _plain_source_data["B"] = SourceDataEntry(name="B", version=1, content="contentB1")
    app.update()
    assert _metrics.collect() == {"call.batched_transform": 2}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="batched: contentA1", prev=[], prev_may_be_missing=True
        ),
        "B": DictDataWithPrev(
            data="batched: contentB1", prev=[], prev_may_be_missing=True
        ),
    }

    # Same version for A (should be memoized), new version for B (should re-execute)
    _plain_source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA2")
    _plain_source_data["B"] = SourceDataEntry(name="B", version=2, content="contentB2")
    app.update()
    assert _metrics.collect() == {"call.batched_transform": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="batched: contentA1", prev=[], prev_may_be_missing=True
        ),
        "B": DictDataWithPrev(
            data="batched: contentB2",
            prev=["batched: contentB1"],
            prev_may_be_missing=False,
        ),
    }

    # No changes - everything should be memoized
    app.update()
    assert _metrics.collect() == {}


@coco.function(memo=True, batching=True)
async def _batched_transform_async(inputs: list[SourceDataEntry]) -> list[str]:
    for inp in inputs:
        _metrics.increment("call.batched_transform_async")
    return [f"batched_async: {entry.content}" for entry in inputs]


@coco.function
async def _process_with_batched_transform_async() -> None:
    for key, value in _plain_source_data.items():
        transformed_value = await _batched_transform_async(value)
        coco.declare_target_state(GlobalDictTarget.target_state(key, transformed_value))


def test_memo_with_batching_async() -> None:
    """Test that memo=True works correctly with batching=True for async functions."""
    GlobalDictTarget.store.clear()
    _plain_source_data.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(name="test_memo_with_batching_async", environment=coco_env),
        _process_with_batched_transform_async,
    )

    _plain_source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA1")
    _plain_source_data["B"] = SourceDataEntry(name="B", version=1, content="contentB1")
    app.update()
    assert _metrics.collect() == {"call.batched_transform_async": 2}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="batched_async: contentA1", prev=[], prev_may_be_missing=True
        ),
        "B": DictDataWithPrev(
            data="batched_async: contentB1", prev=[], prev_may_be_missing=True
        ),
    }

    # Same version for A (should be memoized), new version for B (should re-execute)
    _plain_source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA2")
    _plain_source_data["B"] = SourceDataEntry(name="B", version=2, content="contentB2")
    app.update()
    assert _metrics.collect() == {"call.batched_transform_async": 1}

    # No changes - everything should be memoized
    app.update()
    assert _metrics.collect() == {}


# ============================================================================
# Memo with runner tests
# ============================================================================


class MockRunner(Runner):
    """Mock runner for testing memo with runner."""

    def __init__(self) -> None:
        super().__init__()
        self.call_count = 0

    async def run(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        self.call_count += 1
        return await fn(*args, **kwargs)

    async def run_sync_fn(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        self.call_count += 1
        return await asyncio.to_thread(fn, *args, **kwargs)


_test_runner = MockRunner()


@coco.function(memo=True, runner=_test_runner)
def _runner_transform(entry: SourceDataEntry) -> str:
    _metrics.increment("call.runner_transform")
    return f"runner: {entry.content}"


@coco.function
async def _process_with_runner_transform() -> None:
    for key, value in _plain_source_data.items():
        transformed_value = await _runner_transform(value)
        coco.declare_target_state(GlobalDictTarget.target_state(key, transformed_value))


def test_memo_with_runner() -> None:
    """Test that memo=True works correctly with runner."""
    GlobalDictTarget.store.clear()
    _plain_source_data.clear()
    _metrics.clear()
    _test_runner.call_count = 0

    app = coco.App(
        coco.AppConfig(name="test_memo_with_runner", environment=coco_env),
        _process_with_runner_transform,
    )

    _plain_source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA1")
    _plain_source_data["B"] = SourceDataEntry(name="B", version=1, content="contentB1")
    app.update()
    assert _metrics.collect() == {"call.runner_transform": 2}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="runner: contentA1", prev=[], prev_may_be_missing=True
        ),
        "B": DictDataWithPrev(
            data="runner: contentB1", prev=[], prev_may_be_missing=True
        ),
    }

    # Same version for A (should be memoized), new version for B (should re-execute)
    _plain_source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA2")
    _plain_source_data["B"] = SourceDataEntry(name="B", version=2, content="contentB2")
    app.update()
    assert _metrics.collect() == {"call.runner_transform": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="runner: contentA1", prev=[], prev_may_be_missing=True
        ),
        "B": DictDataWithPrev(
            data="runner: contentB2",
            prev=["runner: contentB1"],
            prev_may_be_missing=False,
        ),
    }

    # No changes - everything should be memoized
    app.update()
    assert _metrics.collect() == {}


_test_runner_async = MockRunner()


@coco.function(memo=True, runner=_test_runner_async)
async def _runner_transform_async(entry: SourceDataEntry) -> str:
    _metrics.increment("call.runner_transform_async")
    return f"runner_async: {entry.content}"


@coco.function
async def _process_with_runner_transform_async() -> None:
    for key, value in _plain_source_data.items():
        transformed_value = await _runner_transform_async(value)
        coco.declare_target_state(GlobalDictTarget.target_state(key, transformed_value))


def test_memo_with_runner_async() -> None:
    """Test that memo=True works correctly with runner for async functions."""
    GlobalDictTarget.store.clear()
    _plain_source_data.clear()
    _metrics.clear()
    _test_runner_async.call_count = 0

    app = coco.App(
        coco.AppConfig(name="test_memo_with_runner_async", environment=coco_env),
        _process_with_runner_transform_async,
    )

    _plain_source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA1")
    _plain_source_data["B"] = SourceDataEntry(name="B", version=1, content="contentB1")
    app.update()
    assert _metrics.collect() == {"call.runner_transform_async": 2}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="runner_async: contentA1", prev=[], prev_may_be_missing=True
        ),
        "B": DictDataWithPrev(
            data="runner_async: contentB1", prev=[], prev_may_be_missing=True
        ),
    }

    # Same version for A (should be memoized), new version for B (should re-execute)
    _plain_source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA2")
    _plain_source_data["B"] = SourceDataEntry(name="B", version=2, content="contentB2")
    app.update()
    assert _metrics.collect() == {"call.runner_transform_async": 1}

    # No changes - everything should be memoized
    app.update()
    assert _metrics.collect() == {}

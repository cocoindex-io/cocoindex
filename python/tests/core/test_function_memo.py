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


@coco.fn(memo=True)
def _transform_entry(entry: SourceDataEntry) -> str:
    _metrics.increment("call.transform_entry")
    return f"processed: {entry.content}"


@coco.fn
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
    app.update_blocking()
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
    app.update_blocking()
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

    app.update_blocking()
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
    app.update_blocking()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == {}

    # The same version reappears after deletion. Verify cached values are not used.
    _plain_source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA1")
    app.update_blocking()
    assert _metrics.collect() == {"call.transform_entry": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="processed: contentA1", prev=[], prev_may_be_missing=True
        ),
    }


@coco.fn.as_async(memo=True)
def _transform_entry_async(entry: SourceDataEntry) -> str:
    _metrics.increment("call.transform_entry_async")
    return f"processed: {entry.content}"


@coco.fn
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
    app.update_blocking()
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
    app.update_blocking()
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

    app.update_blocking()
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
    app.update_blocking()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == {}

    # The same version reappears after deletion. Verify cached values are not used.
    _plain_source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA1")
    app.update_blocking()
    assert _metrics.collect() == {"call.transform_entry_async": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="processed: contentA1", prev=[], prev_may_be_missing=True
        ),
    }


@coco.fn(memo=True)
def _declare_data_entry(key: str, entry: SourceDataEntry) -> None:
    _metrics.increment("call.declare_data_entry")
    coco.declare_target_state(GlobalDictTarget.target_state(key, entry.content))


@coco.fn
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
    app.update_blocking()
    assert _metrics.collect() == {"call.declare_data_entry": 2}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA1", prev=[], prev_may_be_missing=True),
        "B": DictDataWithPrev(data="contentB1", prev=[], prev_may_be_missing=True),
    }

    _plain_source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA2")
    _plain_source_data["B"] = SourceDataEntry(name="B", version=2, content="contentB2")
    app.update_blocking()
    assert _metrics.collect() == {"call.declare_data_entry": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA1", prev=[], prev_may_be_missing=True),
        "B": DictDataWithPrev(
            data="contentB2", prev=["contentB1"], prev_may_be_missing=False
        ),
    }

    app.update_blocking()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA1", prev=[], prev_may_be_missing=True),
        "B": DictDataWithPrev(
            data="contentB2", prev=["contentB1"], prev_may_be_missing=False
        ),
    }

    _plain_source_data.clear()
    app.update_blocking()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == {}

    # The same version reappears after deletion. Verify cached values are not used.
    _plain_source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA1")
    app.update_blocking()
    assert _metrics.collect() == {"call.declare_data_entry": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA1", prev=[], prev_may_be_missing=True),
    }


@coco.fn(memo=True)
async def _declare_data_entry_async(key: str, entry: SourceDataEntry) -> None:
    _metrics.increment("call.declare_data_entry_async")
    coco.declare_target_state(GlobalDictTarget.target_state(key, entry.content))


@coco.fn
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
    app.update_blocking()
    assert _metrics.collect() == {"call.declare_data_entry_async": 2}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA1", prev=[], prev_may_be_missing=True),
        "B": DictDataWithPrev(data="contentB1", prev=[], prev_may_be_missing=True),
    }

    _plain_source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA2")
    _plain_source_data["B"] = SourceDataEntry(name="B", version=2, content="contentB2")
    app.update_blocking()
    assert _metrics.collect() == {"call.declare_data_entry_async": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA1", prev=[], prev_may_be_missing=True),
        "B": DictDataWithPrev(
            data="contentB2", prev=["contentB1"], prev_may_be_missing=False
        ),
    }

    app.update_blocking()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA1", prev=[], prev_may_be_missing=True),
        "B": DictDataWithPrev(
            data="contentB2", prev=["contentB1"], prev_may_be_missing=False
        ),
    }

    _plain_source_data.clear()
    app.update_blocking()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == {}

    # The same version reappears after deletion. Verify cached values are not used.
    _plain_source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA1")
    app.update_blocking()
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
            app.update_blocking()
    finally:
        GlobalDictTarget.store.sink_exception = False
    assert _metrics.collect() == {"call.declare_data_entry": 1}
    assert GlobalDictTarget.store.data == {}

    app.update_blocking()
    assert _metrics.collect() == {"call.declare_data_entry": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="contentA1", prev=["contentA1"], prev_may_be_missing=True
        ),
    }


@coco.fn(memo=True)
def _declare_dict_data_entry(entry: DictSourceDataEntry) -> None:
    _metrics.increment("call.declare_dict_data_entry")
    for key, value in entry.content.items():
        _declare_data_entry(key, value)


@coco.fn
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
    app.update_blocking()
    assert _metrics.collect() == {
        "call.declare_dict_data_entry": 2,
        "call.declare_data_entry": 3,
    }
    assert GlobalDictTarget.store.data == {
        "1A": DictDataWithPrev(data="content1A", prev=[], prev_may_be_missing=True),
        "1B": DictDataWithPrev(data="content1B", prev=[], prev_may_be_missing=True),
        "2A": DictDataWithPrev(data="content2A", prev=[], prev_may_be_missing=True),
    }

    app.update_blocking()
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
    app.update_blocking()
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
    app.update_blocking()
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
    app.update_blocking()
    assert _metrics.collect() == {
        "call.declare_dict_data_entry": 1,
        "call.declare_data_entry": 1,
    }
    assert GlobalDictTarget.store.data == {
        "1A": DictDataWithPrev(data="content1A2.2", prev=[], prev_may_be_missing=True),
        "2A": DictDataWithPrev(data="content2A", prev=[], prev_may_be_missing=True),
    }


@coco.fn(memo=True)
async def _declare_dict_data_entry_w_components(entry: DictSourceDataEntry) -> None:
    _metrics.increment("call.declare_dict_data_entry_w_components")
    for key, value in entry.content.items():
        await coco.mount(coco.component_subpath(key), _declare_data_entry, key, value)


@coco.fn
async def _declare_dict_data_w_components() -> None:
    for entry in _dict_source_data.values():
        await _declare_dict_data_entry_w_components(entry)


def test_memo_nested_functions_with_components() -> None:
    """A memo=True function that mounts child components should raise an error."""
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
        },
    )
    with pytest.raises(Exception, match="memo=True mounted child components"):
        app.update_blocking()


@coco.fn(memo=True)
async def _declare_dict_data_entry_w_components_async(
    entry: DictSourceDataEntry,
) -> None:
    _metrics.increment("call.declare_dict_data_entry_w_components_async")
    for key, value in entry.content.items():
        await coco.mount(coco.component_subpath(key), _declare_data_entry, key, value)


@coco.fn
async def _declare_dict_data_w_components_async() -> None:
    for entry in _dict_source_data.values():
        await _declare_dict_data_entry_w_components_async(entry)


def test_memo_nested_functions_with_components_async() -> None:
    """A memo=True async function that mounts child components should raise an error."""
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
        },
    )
    with pytest.raises(Exception, match="memo=True mounted child components"):
        app.update_blocking()


# ============================================================================
# Memo with batching tests
# ============================================================================


@coco.fn.as_async(memo=True, batching=True)
def _batched_transform(inputs: list[SourceDataEntry]) -> list[str]:
    for inp in inputs:
        _metrics.increment("call.batched_transform")
    return [f"batched: {entry.content}" for entry in inputs]


@coco.fn
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
    app.update_blocking()
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
    app.update_blocking()
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
    app.update_blocking()
    assert _metrics.collect() == {}


@coco.fn.as_async(memo=True, batching=True)
async def _batched_transform_async(inputs: list[SourceDataEntry]) -> list[str]:
    for inp in inputs:
        _metrics.increment("call.batched_transform_async")
    return [f"batched_async: {entry.content}" for entry in inputs]


@coco.fn
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
    app.update_blocking()
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
    app.update_blocking()
    assert _metrics.collect() == {"call.batched_transform_async": 1}

    # No changes - everything should be memoized
    app.update_blocking()
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


@coco.fn.as_async(memo=True, runner=_test_runner)
def _runner_transform(entry: SourceDataEntry) -> str:
    _metrics.increment("call.runner_transform")
    return f"runner: {entry.content}"


@coco.fn
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
    app.update_blocking()
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
    app.update_blocking()
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
    app.update_blocking()
    assert _metrics.collect() == {}


_test_runner_async = MockRunner()


@coco.fn.as_async(memo=True, runner=_test_runner_async)
def _runner_transform_async(entry: SourceDataEntry) -> str:
    _metrics.increment("call.runner_transform_async")
    return f"runner_async: {entry.content}"


@coco.fn
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
    app.update_blocking()
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
    app.update_blocking()
    assert _metrics.collect() == {"call.runner_transform_async": 1}

    # No changes - everything should be memoized
    app.update_blocking()
    assert _metrics.collect() == {}


# ============================================================================
# Bound method memo tests — verifies that @coco.fn(memo=True) on a class method
# is respected when the bound method is called directly (function-level memo).
# ============================================================================


class _MemoMethodTransformer:
    @coco.fn(memo=True)
    def transform(self, entry: SourceDataEntry) -> str:
        _metrics.increment("call.bound_transform")
        return f"bound: {entry.content}"


_memo_transformer = _MemoMethodTransformer()


@coco.fn
def _process_with_bound_method() -> None:
    for key, value in _plain_source_data.items():
        transformed_value = _memo_transformer.transform(value)
        coco.declare_target_state(GlobalDictTarget.target_state(key, transformed_value))


def test_memo_bound_method() -> None:
    """@coco.fn(memo=True) on a bound method should memoize when called directly."""
    GlobalDictTarget.store.clear()
    _plain_source_data.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(name="test_memo_bound_method", environment=coco_env),
        _process_with_bound_method,
    )

    _plain_source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA1")
    _plain_source_data["B"] = SourceDataEntry(name="B", version=1, content="contentB1")
    app.update_blocking()
    assert _metrics.collect() == {"call.bound_transform": 2}

    # A unchanged (version=1), B changed (version=2) — A should be memoized.
    _plain_source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA2")
    _plain_source_data["B"] = SourceDataEntry(name="B", version=2, content="contentB2")
    app.update_blocking()
    assert _metrics.collect() == {"call.bound_transform": 1}  # Only B re-executes

    # No changes - everything should be memoized.
    app.update_blocking()
    assert _metrics.collect() == {}


# ============================================================================
# memo_key tests — per-argument memoization key control
# ============================================================================

_memo_key_source_data: dict[str, SourceDataEntry] = {}


# 'content' is excluded from the memo key; only 'name' and 'version' matter.
@coco.fn(memo=True, memo_key={"entry": lambda e: (e.name, e.version)})
def _transform_entry_with_memo_key(entry: SourceDataEntry) -> str:
    _metrics.increment("call.transform_entry_with_memo_key")
    return f"processed: {entry.content}"


@coco.fn
def _process_with_memo_key() -> None:
    for key, value in _memo_key_source_data.items():
        transformed_value = _transform_entry_with_memo_key(value)
        coco.declare_target_state(GlobalDictTarget.target_state(key, transformed_value))


def test_memo_key_callable_controls_invalidation() -> None:
    """memo_key with callable: only the key fn's output triggers re-execution."""
    GlobalDictTarget.store.clear()
    _memo_key_source_data.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_memo_key_callable_controls_invalidation", environment=coco_env
        ),
        _process_with_memo_key,
    )

    _memo_key_source_data["A"] = SourceDataEntry(
        name="A", version=1, content="contentA1"
    )
    app.update_blocking()
    assert _metrics.collect() == {"call.transform_entry_with_memo_key": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="processed: contentA1", prev=[], prev_may_be_missing=True
        ),
    }

    # Change only 'content' (not covered by memo key) — should NOT re-execute.
    _memo_key_source_data["A"] = SourceDataEntry(
        name="A", version=1, content="contentA_NEW"
    )
    app.update_blocking()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="processed: contentA1", prev=[], prev_may_be_missing=True
        ),
    }

    # Change 'version' (covered by memo key) — should re-execute.
    _memo_key_source_data["A"] = SourceDataEntry(
        name="A", version=2, content="contentA_NEW"
    )
    app.update_blocking()
    assert _metrics.collect() == {"call.transform_entry_with_memo_key": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="processed: contentA_NEW",
            prev=["processed: contentA1"],
            prev_may_be_missing=False,
        ),
    }


_memo_key_none_source_data: dict[str, SourceDataEntry] = {}
_memo_key_none_extra_data: dict[str, str] = {}


# 'extra' is excluded; only 'entry' matters for memoization.
@coco.fn(memo=True, memo_key={"extra": None})
def _transform_with_excluded_arg(entry: SourceDataEntry, extra: str) -> str:
    _metrics.increment("call.transform_with_excluded_arg")
    return f"processed: {entry.content} [{extra}]"


@coco.fn
def _process_with_excluded_arg() -> None:
    for key, value in _memo_key_none_source_data.items():
        extra = _memo_key_none_extra_data.get(key, "default")
        transformed_value = _transform_with_excluded_arg(value, extra)
        coco.declare_target_state(GlobalDictTarget.target_state(key, transformed_value))


def test_memo_key_none_excludes_arg_from_invalidation() -> None:
    """memo_key with None: excluded arg never triggers re-execution."""
    GlobalDictTarget.store.clear()
    _memo_key_none_source_data.clear()
    _memo_key_none_extra_data.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_memo_key_none_excludes_arg_from_invalidation",
            environment=coco_env,
        ),
        _process_with_excluded_arg,
    )

    _memo_key_none_source_data["A"] = SourceDataEntry(name="A", version=1, content="c1")
    _memo_key_none_extra_data["A"] = "tag1"
    app.update_blocking()
    assert _metrics.collect() == {"call.transform_with_excluded_arg": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="processed: c1 [tag1]", prev=[], prev_may_be_missing=True
        ),
    }

    # Change only the excluded 'extra' arg — should NOT re-execute.
    _memo_key_none_extra_data["A"] = "tag2"
    app.update_blocking()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="processed: c1 [tag1]", prev=[], prev_may_be_missing=True
        ),
    }

    # Change 'entry' (not excluded) — should re-execute.
    _memo_key_none_source_data["A"] = SourceDataEntry(name="A", version=2, content="c2")
    app.update_blocking()
    assert _metrics.collect() == {"call.transform_with_excluded_arg": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="processed: c2 [tag2]",
            prev=["processed: c1 [tag1]"],
            prev_may_be_missing=False,
        ),
    }


@coco.fn.as_async(memo=True, memo_key={"entry": lambda e: (e.name, e.version)})
def _transform_entry_with_memo_key_async(entry: SourceDataEntry) -> str:
    _metrics.increment("call.transform_entry_with_memo_key_async")
    return f"processed_async: {entry.content}"


@coco.fn
async def _process_with_memo_key_async() -> None:
    for key, value in _memo_key_source_data.items():
        transformed_value = await _transform_entry_with_memo_key_async(value)
        coco.declare_target_state(GlobalDictTarget.target_state(key, transformed_value))


def test_memo_key_callable_controls_invalidation_async() -> None:
    """memo_key with callable should work for @coco.fn.as_async as well."""
    GlobalDictTarget.store.clear()
    _memo_key_source_data.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_memo_key_callable_controls_invalidation_async",
            environment=coco_env,
        ),
        _process_with_memo_key_async,
    )

    _memo_key_source_data["A"] = SourceDataEntry(
        name="A", version=1, content="contentA1"
    )
    app.update_blocking()
    assert _metrics.collect() == {"call.transform_entry_with_memo_key_async": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="processed_async: contentA1", prev=[], prev_may_be_missing=True
        ),
    }

    # Change only content -> should stay memoized.
    _memo_key_source_data["A"] = SourceDataEntry(
        name="A", version=1, content="contentA_NEW"
    )
    app.update_blocking()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="processed_async: contentA1", prev=[], prev_may_be_missing=True
        ),
    }

    # Change version -> should re-execute.
    _memo_key_source_data["A"] = SourceDataEntry(
        name="A", version=2, content="contentA_NEW"
    )
    app.update_blocking()
    assert _metrics.collect() == {"call.transform_entry_with_memo_key_async": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="processed_async: contentA_NEW",
            prev=["processed_async: contentA1"],
            prev_may_be_missing=False,
        ),
    }


def test_memo_key_validation_rejects_unknown_parameter() -> None:
    with pytest.raises(ValueError, match="Unknown memo_key parameter"):

        @coco.fn(memo=True, memo_key={"missing": None})
        def _invalid_unknown(entry: SourceDataEntry) -> str:
            return entry.content


def test_memo_key_validation_rejects_non_callable_value() -> None:
    from typing import cast

    with pytest.raises(TypeError, match="must be a callable or None"):

        @coco.fn(memo=True, memo_key=cast(Any, {"entry": 123}))
        def _invalid_non_callable(entry: SourceDataEntry) -> str:
            return entry.content

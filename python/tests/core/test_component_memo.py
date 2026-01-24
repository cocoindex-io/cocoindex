import cocoindex as coco
import cocoindex.inspect as coco_inspect

from dataclasses import dataclass
from typing import NamedTuple
import sys
import pathlib

import pytest

from tests import common
from tests.common.target_states import GlobalDictTarget, DictDataWithPrev, Metrics
from tests.common.module_utils import load_module_as


coco_env = common.create_test_env(__file__)


class SourceDataEntry(NamedTuple):
    name: str
    version: int
    content: str
    err: bool = False

    def __coco_memo_key__(self) -> object:
        return (self.name, self.version)


_source_data: dict[str, SourceDataEntry] = {}
_metrics = Metrics()


@dataclass(frozen=True)
class SourceDataResult:
    name: str
    content: str


@coco.function(memo=True)
def _declare_dict_entry(scope: coco.Scope, entry: SourceDataEntry) -> None:
    # Track the actual number of component executions for this function.
    if entry.err:
        raise Exception("injected test exception (which is expected)")
    _metrics.increment("calls")
    coco.declare_target_state(
        scope, GlobalDictTarget.target_state(entry.name, entry.content)
    )


@coco.function
def _declare_dict_data(scope: coco.Scope) -> None:
    for entry in _source_data.values():
        coco.mount(_declare_dict_entry, scope / entry.name, entry)


@coco.function(memo=True)
def _declare_transform_dict_entry(
    scope: coco.Scope, entry: SourceDataEntry
) -> SourceDataResult:
    if entry.err:
        raise Exception("injected test exception (which is expected)")
    _metrics.increment("calls")
    coco.declare_target_state(
        scope, GlobalDictTarget.target_state(entry.name, entry.content)
    )
    return SourceDataResult(name=entry.name, content=entry.content)


@coco.function
def _declare_transform_dict_data(scope: coco.Scope) -> list[SourceDataResult]:
    # Deterministic ordering for stable assertions.
    results: list[SourceDataResult] = []
    for name in sorted(_source_data):
        entry = _source_data[name]
        handle = coco.mount_run(
            _declare_transform_dict_entry, scope / entry.name, entry
        )
        results.append(handle.result())
    return results


def test_source_data_memo() -> None:
    GlobalDictTarget.store.clear()
    _source_data.clear()
    _metrics.clear()

    app = coco.App(
        _declare_dict_data,
        coco.AppConfig(name="test_source_data_memo", environment=coco_env),
    )

    _source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA1")
    _source_data["B"] = SourceDataEntry(name="B", version=1, content="contentB1")

    app.update()
    # 2 children, each updates 1 key => 2 calls into _declare_source_data_entry.
    assert _metrics.collect() == {"calls": 2}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA1", prev=[], prev_may_be_missing=True),
        "B": DictDataWithPrev(data="contentB1", prev=[], prev_may_be_missing=True),
    }

    # memo key no change, reprocessing should be skipped
    _source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA2")
    _source_data["B"] = SourceDataEntry(name="B", version=2, content="contentB2")
    app.update()
    # A is skipped (memo hit), B runs (memo miss) => 1 call into _declare_source_data_entry.
    assert _metrics.collect() == {"calls": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA1", prev=[], prev_may_be_missing=True),
        "B": DictDataWithPrev(
            data="contentB2", prev=["contentB1"], prev_may_be_missing=False
        ),
    }

    # Test deletion and re-insertion.
    del _source_data["A"]
    app.update()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == {
        "B": DictDataWithPrev(
            data="contentB2", prev=["contentB1"], prev_may_be_missing=False
        ),
    }

    _source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA2")
    app.update()
    assert _metrics.collect() == {"calls": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA2", prev=[], prev_may_be_missing=True),
        "B": DictDataWithPrev(
            data="contentB2", prev=["contentB1"], prev_may_be_missing=False
        ),
    }

    # When the component starts to run on a new version, memoization is expected to be invalidated,
    # even if it doesn't finish (e.g. an exception is raised).
    # Because once it starts, there can be target states created by child components.
    _source_data["A"] = SourceDataEntry(
        name="A", version=2, content="contentA2", err=True
    )
    app.update()
    _source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA3")
    app.update()
    assert _metrics.collect() == {"calls": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="contentA3", prev=["contentA2"], prev_may_be_missing=False
        ),
        "B": DictDataWithPrev(
            data="contentB2", prev=["contentB1"], prev_may_be_missing=False
        ),
    }


def test_source_data_memo_cleanup() -> None:
    GlobalDictTarget.store.clear()
    _source_data.clear()
    _metrics.clear()

    app = coco.App(
        _declare_dict_data,
        coco.AppConfig(name="test_source_data_memo_cleanup", environment=coco_env),
    )

    _source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA1")
    app.update()
    assert _metrics.collect() == {"calls": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA1", prev=[], prev_may_be_missing=True),
    }
    assert coco_inspect.list_stable_paths_sync(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "A",
    ]

    del _source_data["A"]
    app.update()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == {}
    assert coco_inspect.list_stable_paths_sync(app) == [coco.ROOT_PATH]


def test_source_data_memo_mount_run() -> None:
    GlobalDictTarget.store.clear()
    _source_data.clear()
    _metrics.clear()

    app = coco.App(
        _declare_transform_dict_data,
        coco.AppConfig(name="test_source_data_memo_mount_run", environment=coco_env),
    )

    _source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA1")
    _source_data["B"] = SourceDataEntry(name="B", version=1, content="contentB1")
    ret1 = app.update()
    assert _metrics.collect() == {"calls": 2}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA1", prev=[], prev_may_be_missing=True),
        "B": DictDataWithPrev(data="contentB1", prev=[], prev_may_be_missing=True),
    }
    assert ret1 == [
        SourceDataResult(name="A", content="contentA1"),
        SourceDataResult(name="B", content="contentB1"),
    ]

    # A memo key unchanged => cached return is used; B changes => recomputed.
    _source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA2")
    _source_data["B"] = SourceDataEntry(name="B", version=2, content="contentB2")
    ret2 = app.update()
    assert _metrics.collect() == {"calls": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA1", prev=[], prev_may_be_missing=True),
        "B": DictDataWithPrev(
            data="contentB2", prev=["contentB1"], prev_may_be_missing=False
        ),
    }
    assert ret2 == [
        SourceDataResult(name="A", content="contentA1"),
        SourceDataResult(name="B", content="contentB2"),
    ]

    _source_data["A"] = SourceDataEntry(name="A", version=2, content="contentA2")
    ret3 = app.update()
    assert _metrics.collect() == {"calls": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="contentA2", prev=["contentA1"], prev_may_be_missing=False
        ),
        "B": DictDataWithPrev(
            data="contentB2", prev=["contentB1"], prev_may_be_missing=False
        ),
    }
    assert ret3 == [
        SourceDataResult(name="A", content="contentA2"),
        SourceDataResult(name="B", content="contentB2"),
    ]

    # Test deletion and re-insertion.
    del _source_data["A"]
    ret4 = app.update()
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == {
        "B": DictDataWithPrev(
            data="contentB2", prev=["contentB1"], prev_may_be_missing=False
        ),
    }
    assert ret4 == [
        SourceDataResult(name="B", content="contentB2"),
    ]

    _source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA2")
    ret5 = app.update()
    assert _metrics.collect() == {"calls": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA2", prev=[], prev_may_be_missing=True),
        "B": DictDataWithPrev(
            data="contentB2", prev=["contentB1"], prev_may_be_missing=False
        ),
    }
    assert ret5 == [
        SourceDataResult(name="A", content="contentA2"),
        SourceDataResult(name="B", content="contentB2"),
    ]

    # When the component starts to run on a new version, memoization is expected to be invalidated,
    # even if it doesn't finish (e.g. an exception is raised).
    # Because once it starts, there can be target states created by child components.
    _source_data["A"] = SourceDataEntry(
        name="A", version=2, content="contentA2", err=True
    )
    with pytest.raises(Exception):
        app.update()
    _source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA3")
    ret6 = app.update()
    assert _metrics.collect() == {"calls": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="contentA3", prev=["contentA2"], prev_may_be_missing=False
        ),
        "B": DictDataWithPrev(
            data="contentB2", prev=["contentB1"], prev_may_be_missing=False
        ),
    }
    assert ret6 == [
        SourceDataResult(name="A", content="contentA3"),
        SourceDataResult(name="B", content="contentB2"),
    ]


def test_source_data_memo_mount_run_cleanup() -> None:
    GlobalDictTarget.store.clear()
    _source_data.clear()
    _metrics.clear()

    app = coco.App(
        _declare_transform_dict_data,
        coco.AppConfig(
            name="test_source_data_memo_mount_run_cleanup", environment=coco_env
        ),
    )

    _source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA1")
    ret1 = app.update()
    assert ret1 == [
        SourceDataResult(name="A", content="contentA1"),
    ]
    assert _metrics.collect() == {"calls": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA1", prev=[], prev_may_be_missing=True),
    }
    assert coco_inspect.list_stable_paths_sync(app) == [
        coco.ROOT_PATH,
        coco.ROOT_PATH / "A",
    ]

    del _source_data["A"]
    ret2 = app.update()
    assert ret2 == []
    assert _metrics.collect() == {}
    assert GlobalDictTarget.store.data == {}
    assert coco_inspect.list_stable_paths_sync(app) == [coco.ROOT_PATH]


def test_memo_invalidation_on_decorator_change() -> None:
    """
    Test that memoization is invalidated when memo=True is removed from a function.

    Simulates in-place code changes:
    1. Function with memo=True - runs and caches
    2. Function without memo=True - runs (no memo)
    3. Function with memo=True again - should run again (cache invalidated in step 2)

    The key insight is that when we remove memo=True from a function, the memoization
    state should be cleared. When we add memo=True back, the function should run again
    and not use the old cached value from step 1.
    """
    GlobalDictTarget.store.clear()
    metrics = Metrics()

    # The fake module name to simulate in-place code changes.
    fake_module_name = "tests.core._dynamic_memo_test_module"

    # Get paths to the two module versions (in the same directory as this test).
    test_dir = pathlib.Path(__file__).parent
    with_memo_path = str(test_dir / "mod_process_entry_w_memo.py")
    without_memo_path = str(test_dir / "mod_process_entry_wo_memo.py")

    # Use a mutable container to hold the current module's function.
    current_module: list[object] = []

    @coco.function
    def app_main(scope: coco.Scope) -> None:
        mod = current_module[0]
        coco.mount(mod.process_entry, scope / "A", "A", "value1")  # type: ignore[attr-defined]

    app = coco.App(
        app_main,
        coco.AppConfig(
            name="test_memo_invalidation_on_decorator_change", environment=coco_env
        ),
    )

    # Step 1: Load with memo=True and run.
    mod_with = load_module_as(with_memo_path, fake_module_name)
    mod_with.set_metrics(metrics)
    current_module.clear()
    current_module.append(mod_with)

    app.update()
    assert metrics.collect() == {"calls": 1}
    app.update()
    assert metrics.collect() == {}

    # Step 2: Load without memo=True and run.
    mod_without = load_module_as(without_memo_path, fake_module_name)
    mod_without.set_metrics(metrics)
    current_module.clear()
    current_module.append(mod_without)

    app.update()
    assert metrics.collect() == {"calls": 1}
    app.update()
    assert metrics.collect() == {"calls": 1}

    # Step 3: Load with memo=True again and run.
    mod_with_again = load_module_as(with_memo_path, fake_module_name)
    mod_with_again.set_metrics(metrics)
    current_module.clear()
    current_module.append(mod_with_again)

    app.update()
    assert metrics.collect() == {"calls": 1}
    app.update()
    assert metrics.collect() == {}

    # Cleanup fake module from sys.modules.
    if fake_module_name in sys.modules:
        del sys.modules[fake_module_name]

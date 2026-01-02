from dataclasses import dataclass
from typing import NamedTuple
import sys
import pathlib

import pytest


import cocoindex as coco
from .. import common
from ..common.effects import GlobalDictTarget, DictDataWithPrev, Metrics
from ..common.module_utils import load_module_as


coco_env = common.create_test_env(__file__)


class SourceDataEntry(NamedTuple):
    name: str
    version: int
    content: str
    err: bool = False

    def __coco_memo_key__(self) -> object:
        return (self.name, self.version)


_source_data: dict[str, SourceDataEntry] = {}
_declare_source_data_entry_metrics = Metrics()


@dataclass(frozen=True)
class SourceDataResult:
    name: str
    content: str


_source_data_run: dict[str, SourceDataEntry] = {}
_run_source_data_entry_metrics = Metrics()


@coco.function(memo=True)
def _declare_source_data_entry(scope: coco.Scope, entry: SourceDataEntry) -> None:
    # Track the actual number of component executions for this function.
    if entry.err:
        raise Exception("injected test exception (which is expected)")
    _declare_source_data_entry_metrics.increment("calls")
    coco.declare_effect(scope, GlobalDictTarget.effect(entry.name, entry.content))


@coco.function
def _declare_source_data(scope: coco.Scope) -> None:
    for entry in _source_data.values():
        coco.mount(_declare_source_data_entry, scope / entry.name, entry)


@coco.function(memo=True)
def _run_source_data_entry(
    _scope: coco.Scope, entry: SourceDataEntry
) -> SourceDataResult:
    if entry.err:
        raise Exception("injected test exception (which is expected)")
    _run_source_data_entry_metrics.increment("calls")
    return SourceDataResult(name=entry.name, content=entry.content)


@coco.function
def _run_source_data(scope: coco.Scope) -> list[SourceDataResult]:
    # Deterministic ordering for stable assertions.
    results: list[SourceDataResult] = []
    for name in sorted(_source_data_run):
        entry = _source_data_run[name]
        handle = coco.mount_run(_run_source_data_entry, scope / entry.name, entry)
        results.append(handle.result())
    return results


def test_source_data_memo() -> None:
    GlobalDictTarget.store.clear()
    _source_data.clear()
    _declare_source_data_entry_metrics.clear()

    app = coco.App(
        _declare_source_data,
        coco.AppConfig(name="test_source_data_memo", environment=coco_env),
    )

    _source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA1")
    _source_data["B"] = SourceDataEntry(name="B", version=1, content="contentB1")

    app.run()
    # 2 children, each updates 1 key => 2 calls into _declare_source_data_entry.
    assert _declare_source_data_entry_metrics.collect() == {"calls": 2}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA1", prev=[], prev_may_be_missing=True),
        "B": DictDataWithPrev(data="contentB1", prev=[], prev_may_be_missing=True),
    }

    # memo key no change, reprocessing should be skipped
    _source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA2")
    _source_data["B"] = SourceDataEntry(name="B", version=2, content="contentB2")
    app.run()
    # A is skipped (memo hit), B runs (memo miss) => 1 call into _declare_source_data_entry.
    assert _declare_source_data_entry_metrics.collect() == {"calls": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA1", prev=[], prev_may_be_missing=True),
        "B": DictDataWithPrev(
            data="contentB2", prev=["contentB1"], prev_may_be_missing=False
        ),
    }

    # Test deletion and re-insertion.
    del _source_data["A"]
    app.run()
    assert _declare_source_data_entry_metrics.collect() == {}
    assert GlobalDictTarget.store.data == {
        "B": DictDataWithPrev(
            data="contentB2", prev=["contentB1"], prev_may_be_missing=False
        ),
    }

    _source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA2")
    app.run()
    assert _declare_source_data_entry_metrics.collect() == {"calls": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA2", prev=[], prev_may_be_missing=True),
        "B": DictDataWithPrev(
            data="contentB2", prev=["contentB1"], prev_may_be_missing=False
        ),
    }

    # When the component starts to run on a new version, memoization is expected to be invalidated,
    # even if it doesn't finish (e.g. an exception is raised).
    # Because once it starts, there can be effects created by child components.
    _source_data["A"] = SourceDataEntry(
        name="A", version=2, content="contentA2", err=True
    )
    app.run()
    _source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA3")
    app.run()
    assert _declare_source_data_entry_metrics.collect() == {"calls": 1}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="contentA3", prev=["contentA2"], prev_may_be_missing=False
        ),
        "B": DictDataWithPrev(
            data="contentB2", prev=["contentB1"], prev_may_be_missing=False
        ),
    }


def test_source_data_memo_mount_run() -> None:
    _source_data_run.clear()
    _run_source_data_entry_metrics.clear()

    app = coco.App(
        _run_source_data,
        coco.AppConfig(name="test_source_data_memo_mount_run", environment=coco_env),
    )

    _source_data_run["A"] = SourceDataEntry(name="A", version=1, content="contentA1")
    _source_data_run["B"] = SourceDataEntry(name="B", version=1, content="contentB1")
    ret1 = app.run()
    assert _run_source_data_entry_metrics.collect() == {"calls": 2}
    assert ret1 == [
        SourceDataResult(name="A", content="contentA1"),
        SourceDataResult(name="B", content="contentB1"),
    ]

    # A memo key unchanged => cached return is used; B changes => recomputed.
    _source_data_run["A"] = SourceDataEntry(name="A", version=1, content="contentA2")
    _source_data_run["B"] = SourceDataEntry(name="B", version=2, content="contentB2")
    ret2 = app.run()
    assert _run_source_data_entry_metrics.collect() == {"calls": 1}
    assert ret2 == [
        SourceDataResult(name="A", content="contentA1"),
        SourceDataResult(name="B", content="contentB2"),
    ]

    _source_data_run["A"] = SourceDataEntry(name="A", version=2, content="contentA2")
    ret3 = app.run()
    assert _run_source_data_entry_metrics.collect() == {"calls": 1}
    assert ret3 == [
        SourceDataResult(name="A", content="contentA2"),
        SourceDataResult(name="B", content="contentB2"),
    ]

    # Test deletion and re-insertion.
    del _source_data_run["A"]
    ret4 = app.run()
    assert _run_source_data_entry_metrics.collect() == {}
    assert ret4 == [
        SourceDataResult(name="B", content="contentB2"),
    ]

    _source_data_run["A"] = SourceDataEntry(name="A", version=1, content="contentA2")
    ret5 = app.run()
    assert _run_source_data_entry_metrics.collect() == {"calls": 1}
    assert ret5 == [
        SourceDataResult(name="A", content="contentA2"),
        SourceDataResult(name="B", content="contentB2"),
    ]

    # When the component starts to run on a new version, memoization is expected to be invalidated,
    # even if it doesn't finish (e.g. an exception is raised).
    # Because once it starts, there can be effects created by child components.
    _source_data_run["A"] = SourceDataEntry(
        name="A", version=2, content="contentA2", err=True
    )
    with pytest.raises(Exception):
        app.run()
    _source_data_run["A"] = SourceDataEntry(name="A", version=1, content="contentA3")
    ret6 = app.run()
    assert _run_source_data_entry_metrics.collect() == {"calls": 1}
    assert ret6 == [
        SourceDataResult(name="A", content="contentA3"),
        SourceDataResult(name="B", content="contentB2"),
    ]


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
    fake_module_name = "cocoindex.tests.core._dynamic_memo_test_module"

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
    mod_with.set_metrics(metrics)  # type: ignore[attr-defined]
    current_module.clear()
    current_module.append(mod_with)

    app.run()
    assert metrics.collect() == {"calls": 1}
    app.run()
    assert metrics.collect() == {}

    # Step 2: Load without memo=True and run.
    mod_without = load_module_as(without_memo_path, fake_module_name)
    mod_without.set_metrics(metrics)  # type: ignore[attr-defined]
    current_module.clear()
    current_module.append(mod_without)

    app.run()
    assert metrics.collect() == {"calls": 1}
    app.run()
    assert metrics.collect() == {"calls": 1}

    # Step 3: Load with memo=True again and run.
    mod_with_again = load_module_as(with_memo_path, fake_module_name)
    mod_with_again.set_metrics(metrics)  # type: ignore[attr-defined]
    current_module.clear()
    current_module.append(mod_with_again)

    app.run()
    assert metrics.collect() == {"calls": 1}
    app.run()
    assert metrics.collect() == {}

    # Cleanup fake module from sys.modules.
    if fake_module_name in sys.modules:
        del sys.modules[fake_module_name]

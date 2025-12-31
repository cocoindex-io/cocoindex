from dataclasses import dataclass
from typing import NamedTuple

import cocoindex as coco
from . import common
from .common.effects import GlobalDictTarget, DictDataWithPrev, Metrics


coco_env = common.create_test_env(__file__)


class SourceDataEntry(NamedTuple):
    name: str
    version: int
    content: str

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

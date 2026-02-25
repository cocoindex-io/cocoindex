"""Tests for full_reprocess behavior."""

import cocoindex as coco

from typing import NamedTuple

from tests import common
from tests.common.target_states import GlobalDictTarget, DictDataWithPrev, Metrics


coco_env = common.create_test_env(__file__)


class SourceDataEntry(NamedTuple):
    name: str
    version: int
    content: str

    def __coco_memo_key__(self) -> object:
        return (self.name, self.version)


_source_data: dict[str, SourceDataEntry] = {}
_metrics = Metrics()


@coco.function(memo=True)
def _declare_dict_entry(entry: SourceDataEntry) -> None:
    """Memoized function that declares a target state."""
    _metrics.increment("calls")
    coco.declare_target_state(GlobalDictTarget.target_state(entry.name, entry.content))


@coco.function(memo=True)
async def _declare_dict_data_memoized(data_version: int) -> None:
    """Main function that mounts child components. Memoized to test component memoization.

    Takes data_version as argument to enable component memoization.
    """
    _metrics.increment("root_component")
    for entry in _source_data.values():
        await coco.mount(coco.component_subpath(entry.name), _declare_dict_entry, entry)


@coco.function
async def _declare_dict_data() -> None:
    """Main function that mounts child components. Not memoized."""
    _metrics.increment("root_component")
    for entry in _source_data.values():
        await coco.mount(coco.component_subpath(entry.name), _declare_dict_entry, entry)


def test_full_reprocess_force_execution_of_memoized_functions() -> None:
    """Test that full_reprocess forces execution of memoized functions even when unchanged."""
    GlobalDictTarget.store.clear()
    _source_data.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_full_reprocess_force_execution", environment=coco_env
        ),
        _declare_dict_data,
    )

    # First run: create targets
    _source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA1")
    _source_data["B"] = SourceDataEntry(name="B", version=1, content="contentB1")
    app.update_blocking()
    # Root component + 2 children, each updates 1 key => 1 root + 2 calls
    assert _metrics.collect() == {"root_component": 1, "calls": 2}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA1", prev=[], prev_may_be_missing=True),
        "B": DictDataWithPrev(data="contentB1", prev=[], prev_may_be_missing=True),
    }

    # Second run: unchanged, root component runs (not memoized) but memoized functions should skip
    _metrics.clear()
    app.update_blocking()
    # Root component always runs (it's not memoized), but memoized child functions should be skipped
    assert _metrics.collect() == {"root_component": 1}, (
        "Second run: root runs (not memoized) but memoized functions should skip execution"
    )

    # Third run with full_reprocess: should force execution even though unchanged
    _metrics.clear()
    app.update_blocking(full_reprocess=True)
    assert _metrics.collect() == {"root_component": 1, "calls": 2}, (
        "full_reprocess should force execution of root component and memoized functions even when unchanged"
    )


def test_full_reprocess_force_rewrite_unchanged_targets() -> None:
    """Test that full_reprocess forces rewrite of unchanged target states."""
    GlobalDictTarget.store.clear()
    _source_data.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(name="test_full_reprocess_force_rewrite", environment=coco_env),
        _declare_dict_data,
    )

    # First run: create targets
    _source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA1")
    app.update_blocking()
    # Clear metrics after first run to ensure clean state
    _metrics.collect()
    # Clear store metrics after first run
    GlobalDictTarget.store.metrics.collect()
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA1", prev=[], prev_may_be_missing=True),
    }

    # Second run: unchanged, should skip write (prev_may_be_missing stays True)
    _metrics.clear()
    app.update_blocking()
    # Verify no writes happened in second run
    assert GlobalDictTarget.store.metrics.collect() == {}, (
        "Second run should skip writes when unchanged"
    )

    # Third run with full_reprocess: should force rewrite
    # Under full_reprocess, prev_may_be_missing is set to True, so the target should be rewritten
    _metrics.clear()
    app.update_blocking(full_reprocess=True)
    # Verify target state was applied again by checking store metrics
    # For one target state: 1 upsert + 1 sink call
    store_metrics = GlobalDictTarget.store.metrics.collect()
    assert store_metrics.get("sink", 0) == 1, (
        "full_reprocess should force rewrite (sink called)"
    )
    assert store_metrics.get("upsert", 0) == 1, (
        "full_reprocess should force rewrite (upsert called)"
    )
    assert "A" in GlobalDictTarget.store.data


def test_full_reprocess_deleted_targets_not_resurrected() -> None:
    """Test that full_reprocess doesn't keep deleted targets alive via memo reuse."""
    GlobalDictTarget.store.clear()
    _source_data.clear()
    _metrics.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_full_reprocess_deleted_targets", environment=coco_env
        ),
        _declare_dict_data,
    )

    # First run: create both targets A and B
    _source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA1")
    _source_data["B"] = SourceDataEntry(name="B", version=1, content="contentB1")
    app.update_blocking()
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(data="contentA1", prev=[], prev_may_be_missing=True),
        "B": DictDataWithPrev(data="contentB1", prev=[], prev_may_be_missing=True),
    }

    # Second run: remove B, only A should remain
    _source_data.pop("B")
    app.update_blocking()
    assert "A" in GlobalDictTarget.store.data
    assert "B" not in GlobalDictTarget.store.data, "B should be deleted"

    # Third run with full_reprocess: B should still be deleted, not resurrected by old memos
    app.update_blocking(full_reprocess=True)
    assert "A" in GlobalDictTarget.store.data
    assert "B" not in GlobalDictTarget.store.data, (
        "B should remain deleted, not kept alive by old memos"
    )


def test_full_reprocess_component_memoization() -> None:
    """Test that full_reprocess invalidates component memoization."""
    GlobalDictTarget.store.clear()
    _source_data.clear()
    _metrics.clear()

    # Use a stable data_version to enable component memoization
    data_version = 1
    app = coco.App(
        coco.AppConfig(name="test_full_reprocess_component_memo", environment=coco_env),
        _declare_dict_data_memoized,
        data_version=data_version,
    )

    # First run
    _source_data["A"] = SourceDataEntry(name="A", version=1, content="contentA1")
    app.update_blocking()
    # Clear metrics after first run
    first_run_metrics = _metrics.collect()
    assert first_run_metrics == {"root_component": 1, "calls": 1}

    # Second run: unchanged, should use component memoization (root component skipped)
    app.update_blocking()
    second_run_metrics = _metrics.collect()
    assert second_run_metrics == {}, (
        "Should use component memoization and skip execution"
    )

    # Third run with full_reprocess: should invalidate component memoization
    app.update_blocking(full_reprocess=True)
    third_run_metrics = _metrics.collect()
    assert third_run_metrics == {"root_component": 1, "calls": 1}, (
        "full_reprocess should invalidate component memoization and force re-execution of root component"
    )

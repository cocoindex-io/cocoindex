"""Tests for target state ownership transfer between components."""

from __future__ import annotations

import pytest

import cocoindex as coco

from tests import common
from tests.common.target_states import GlobalDictTarget, DictDataWithPrev, AtMost

coco_env = common.create_test_env(__file__)

# Controls which component paths declare which target state keys+values.
# Outer key = component name (becomes component subpath), inner key = target state key.
_source_data: dict[str, dict[str, object]] = {}


@coco.fn
async def _process_component(name: str) -> None:
    for key, value in _source_data.get(name, {}).items():
        coco.declare_target_state(GlobalDictTarget.target_state(key, value))


@coco.fn
async def _app_main() -> None:
    for name in sorted(_source_data):
        await coco.mount(coco.component_subpath(name), _process_component, name)


def test_ownership_transfer_basic() -> None:
    """Target state moves from C1 to C2 — final data is correct."""
    GlobalDictTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        coco.AppConfig(name="test_ownership_transfer_basic", environment=coco_env),
        _app_main,
    )

    # Run 1: C1 owns "x"
    _source_data["C1"] = {"x": 1}
    app.update_blocking()
    assert GlobalDictTarget.store.data["x"].data == 1
    assert common.list_target_state_owners_sync(app) == {
        '/@test_target_state/global_dict/"x"': coco.ROOT_PATH / "C1",
    }
    GlobalDictTarget.store.metrics.collect()

    # Run 2: Ownership transfers from C1 to C2
    _source_data.clear()
    _source_data["C2"] = {"x": 2}
    app.update_blocking()
    assert GlobalDictTarget.store.data["x"].data == 2
    assert common.list_target_state_owners_sync(app) == {
        '/@test_target_state/global_dict/"x"': coco.ROOT_PATH / "C2",
    }


def test_ownership_transfer_same_value() -> None:
    """Transfer with same value — final data is correct."""
    GlobalDictTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        coco.AppConfig(name="test_ownership_transfer_same_value", environment=coco_env),
        _app_main,
    )

    # Run 1: C1 owns "x" with value 1
    _source_data["C1"] = {"x": 1}
    app.update_blocking()
    assert GlobalDictTarget.store.data["x"].data == 1
    GlobalDictTarget.store.metrics.collect()

    # Run 2: C2 takes over with same value
    _source_data.clear()
    _source_data["C2"] = {"x": 1}
    app.update_blocking()
    # Final state must still be 1, regardless of whether preempt or delete+insert happened.
    assert GlobalDictTarget.store.data["x"].data == 1
    assert common.list_target_state_owners_sync(app) == {
        '/@test_target_state/global_dict/"x"': coco.ROOT_PATH / "C2",
    }


def test_ownership_transfer_then_delete() -> None:
    """After transfer, new owner can delete the target state."""
    GlobalDictTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_ownership_transfer_then_delete", environment=coco_env
        ),
        _app_main,
    )

    # Run 1: C1 owns "x"
    _source_data["C1"] = {"x": 1}
    app.update_blocking()
    GlobalDictTarget.store.metrics.collect()

    # Run 2: C2 takes over
    _source_data.clear()
    _source_data["C2"] = {"x": 2}
    app.update_blocking()
    GlobalDictTarget.store.metrics.collect()

    # Run 3: C2 stops declaring "x"
    _source_data.clear()
    _source_data["C2"] = {}
    app.update_blocking()
    assert GlobalDictTarget.store.data == {}
    assert GlobalDictTarget.store.metrics.collect() == {"sink": AtMost(1), "delete": 1}
    assert common.list_target_state_owners_sync(app) == {}


def test_ownership_transfer_ordering_independence() -> None:
    """Regardless of submission order, the target state survives transfer."""
    GlobalDictTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        coco.AppConfig(name="test_ownership_transfer_ordering", environment=coco_env),
        _app_main,
    )

    # Run 1: C1 owns "x"
    _source_data["C1"] = {"x": 1}
    app.update_blocking()
    GlobalDictTarget.store.metrics.collect()

    # Run 2: C1 drops "x", C2 picks it up
    _source_data.clear()
    _source_data["C1"] = {}
    _source_data["C2"] = {"x": 2}
    app.update_blocking()
    # The target state must exist (this is the bug fix)
    assert "x" in GlobalDictTarget.store.data
    assert GlobalDictTarget.store.data["x"].data == 2
    assert common.list_target_state_owners_sync(app) == {
        '/@test_target_state/global_dict/"x"': coco.ROOT_PATH / "C2",
    }


def test_ownership_transfer_multiple_keys() -> None:
    """Only transferred keys move; others stay with original owner."""
    GlobalDictTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_ownership_transfer_multiple_keys", environment=coco_env
        ),
        _app_main,
    )

    # Run 1: C1 owns "a" and "b"
    _source_data["C1"] = {"a": 1, "b": 2}
    app.update_blocking()
    GlobalDictTarget.store.metrics.collect()

    # Run 2: C2 takes "a", C1 keeps "b"
    _source_data.clear()
    _source_data["C1"] = {"b": 2}
    _source_data["C2"] = {"a": 3}
    app.update_blocking()
    # Only check final target state values — reconciliation details (prev,
    # prev_may_be_missing) are nondeterministic due to concurrent processing order.
    assert GlobalDictTarget.store.data["a"].data == 3
    assert GlobalDictTarget.store.data["b"].data == 2
    assert common.list_target_state_owners_sync(app) == {
        '/@test_target_state/global_dict/"a"': coco.ROOT_PATH / "C2",
        '/@test_target_state/global_dict/"b"': coco.ROOT_PATH / "C1",
    }


def test_ownership_transfer_chain() -> None:
    """Target state can be transferred multiple times: C1→C2→C3."""
    GlobalDictTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        coco.AppConfig(name="test_ownership_transfer_chain", environment=coco_env),
        _app_main,
    )

    # Run 1: C1 owns "x"
    _source_data["C1"] = {"x": 1}
    app.update_blocking()
    assert GlobalDictTarget.store.data["x"].data == 1
    assert common.list_target_state_owners_sync(app) == {
        '/@test_target_state/global_dict/"x"': coco.ROOT_PATH / "C1",
    }

    # Run 2: C1 gone, C2 takes over
    _source_data.clear()
    _source_data["C2"] = {"x": 2}
    app.update_blocking()
    assert GlobalDictTarget.store.data["x"].data == 2
    assert common.list_target_state_owners_sync(app) == {
        '/@test_target_state/global_dict/"x"': coco.ROOT_PATH / "C2",
    }

    # Run 3: C2 gone, C3 takes over
    _source_data.clear()
    _source_data["C3"] = {"x": 3}
    app.update_blocking()
    assert GlobalDictTarget.store.data["x"].data == 3
    assert common.list_target_state_owners_sync(app) == {
        '/@test_target_state/global_dict/"x"': coco.ROOT_PATH / "C3",
    }


@coco.fn
async def _app_main_await_ready() -> None:
    """Like _app_main but awaits ready() on all children, guaranteeing
    children submit before the root (so preempt always happens before GC delete)."""
    handles = []
    for name in sorted(_source_data):
        h = await coco.mount(coco.component_subpath(name), _process_component, name)
        handles.append(h)
    for h in handles:
        await h.ready()


def test_ownership_transfer_preempt_strict() -> None:
    """When children are awaited before root returns, preempt is guaranteed.
    This validates update semantics: single upsert, correct prev, no delete+insert."""
    GlobalDictTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_ownership_transfer_preempt_strict", environment=coco_env
        ),
        _app_main_await_ready,
    )

    # Run 1: C1 owns "x"
    _source_data["C1"] = {"x": 1}
    app.update_blocking()
    assert GlobalDictTarget.store.data == {
        "x": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
    }
    assert GlobalDictTarget.store.metrics.collect() == {"sink": AtMost(1), "upsert": 1}

    # Run 2: Ownership transfers from C1 to C2
    _source_data.clear()
    _source_data["C2"] = {"x": 2}
    app.update_blocking()
    assert GlobalDictTarget.store.data == {
        "x": DictDataWithPrev(data=2, prev=[1], prev_may_be_missing=False),
    }
    # Should be 1 upsert (update), NOT a delete + insert
    assert GlobalDictTarget.store.metrics.collect() == {"sink": AtMost(1), "upsert": 1}
    assert common.list_target_state_owners_sync(app) == {
        '/@test_target_state/global_dict/"x"': coco.ROOT_PATH / "C2",
    }

    # Run 3: Same value transfer — no action needed
    _source_data.clear()
    _source_data["C3"] = {"x": 2}
    app.update_blocking()
    assert GlobalDictTarget.store.data == {
        "x": DictDataWithPrev(data=2, prev=[1], prev_may_be_missing=False),
    }
    assert GlobalDictTarget.store.metrics.collect() == {}
    assert common.list_target_state_owners_sync(app) == {
        '/@test_target_state/global_dict/"x"': coco.ROOT_PATH / "C3",
    }


def test_component_delete_cleans_inverted_tracking() -> None:
    """After component deletion, re-declaration is a fresh insert."""
    GlobalDictTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_component_delete_cleans_inverted", environment=coco_env
        ),
        _app_main,
    )

    # Run 1: C1 owns "x"
    _source_data["C1"] = {"x": 1}
    app.update_blocking()
    assert common.list_target_state_owners_sync(app) == {
        '/@test_target_state/global_dict/"x"': coco.ROOT_PATH / "C1",
    }
    GlobalDictTarget.store.metrics.collect()

    # Run 2: C1 is gone entirely — its owner row must go with it, not linger
    # as a dangling entry pointing at a component with no tracking info.
    _source_data.clear()
    app.update_blocking()
    assert GlobalDictTarget.store.data == {}
    assert common.list_target_state_owners_sync(app) == {}
    GlobalDictTarget.store.metrics.collect()

    # Run 3: C2 declares "x" fresh (no previous owner)
    _source_data["C2"] = {"x": 2}
    app.update_blocking()
    assert GlobalDictTarget.store.data == {
        "x": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert common.list_target_state_owners_sync(app) == {
        '/@test_target_state/global_dict/"x"': coco.ROOT_PATH / "C2",
    }


@coco.fn
async def _app_main_use_mount() -> None:
    """Like _app_main but with use_mount, so a child's submit failure (e.g. a
    sink exception) propagates to the root and surfaces from App.update —
    plain mount() routes child failures to the exception-handler chain, which
    logs and swallows by default."""
    for name in sorted(_source_data):
        await coco.use_mount(coco.component_subpath(name), _process_component, name)


def test_ownership_transfer_sink_failure_then_retry() -> None:
    """Sink failure mid-transfer, then retry: the next run re-reconciles from
    the multi-state tracking record and converges.

    The failed attempt's precommit already moved the item from C1's tracking
    into C2's (multi-state) and claimed `__target` for C2 — the claim is
    visible at precommit by design, so concurrent claimants can detect the
    in-flight lifecycle. Recovery is "fix state on next run", not revert.
    """
    GlobalDictTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_ownership_transfer_sink_failure_then_retry",
            environment=coco_env,
        ),
        _app_main_use_mount,
    )

    # Run 1: C1 owns "x"
    _source_data["C1"] = {"x": 1}
    app.update_blocking()
    assert GlobalDictTarget.store.metrics.collect() == {"sink": AtMost(1), "upsert": 1}

    # Run 2: C2 takes over, but the sink fails after precommit.
    _source_data.clear()
    _source_data["C2"] = {"x": 2}
    try:
        GlobalDictTarget.store.sink_exception = True
        with pytest.raises(Exception):
            app.update_blocking()
    finally:
        GlobalDictTarget.store.sink_exception = False
    # Nothing reached the sink; the external state still holds C1's value.
    assert GlobalDictTarget.store.data == {
        "x": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
    }
    assert GlobalDictTarget.store.metrics.collect() == {}
    # The `__target` claim landed at precommit, ahead of the failed sink apply.
    assert common.list_target_state_owners_sync(app) == {
        '/@test_target_state/global_dict/"x"': coco.ROOT_PATH / "C2",
    }

    # Run 3: retry with a healthy sink. C2 finds "x" in its own tracking with
    # both candidate previous states (C1's applied value and C2's unapplied
    # attempt) and re-issues the upsert. No delete is ever emitted — C1's
    # GC'd tracking no longer contains "x".
    app.update_blocking()
    assert GlobalDictTarget.store.data == {
        "x": DictDataWithPrev(data=2, prev=[1, 2], prev_may_be_missing=False),
    }
    assert GlobalDictTarget.store.metrics.collect() == {"sink": AtMost(1), "upsert": 1}
    assert common.list_target_state_owners_sync(app) == {
        '/@test_target_state/global_dict/"x"': coco.ROOT_PATH / "C2",
    }


def test_ownership_transfer_sink_failure_then_owner_deleted() -> None:
    """Sink failure mid-transfer, then the claimant is unmounted: GC cleans up.

    The failed claimant's precommit durably recorded the item in its own
    tracking, so Delete-mode reconciliation still knows about the target
    state and removes it from the sink — nothing is orphaned.
    """
    GlobalDictTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_ownership_transfer_sink_failure_then_owner_deleted",
            environment=coco_env,
        ),
        _app_main_use_mount,
    )

    # Run 1: C1 owns "x"
    _source_data["C1"] = {"x": 1}
    app.update_blocking()
    GlobalDictTarget.store.metrics.collect()

    # Run 2: C2 takes over, but the sink fails after precommit.
    _source_data.clear()
    _source_data["C2"] = {"x": 2}
    try:
        GlobalDictTarget.store.sink_exception = True
        with pytest.raises(Exception):
            app.update_blocking()
    finally:
        GlobalDictTarget.store.sink_exception = False
    GlobalDictTarget.store.metrics.collect()

    # Run 3: both components are gone. C2's GC delete must remove "x" from
    # the sink (using the multi-state previous states from the failed
    # attempt) and drop the owner row.
    _source_data.clear()
    app.update_blocking()
    assert GlobalDictTarget.store.data == {}
    assert GlobalDictTarget.store.metrics.collect() == {"sink": AtMost(1), "delete": 1}
    assert common.list_target_state_owners_sync(app) == {}

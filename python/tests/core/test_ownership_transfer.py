"""Tests for target state ownership transfer between components."""

from __future__ import annotations

import cocoindex as coco

from tests import common
from tests.common.target_states import GlobalDictTarget, DictDataWithPrev

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
    """Target state moves from C1 to C2 with update semantics."""
    GlobalDictTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        coco.AppConfig(name="test_ownership_transfer_basic", environment=coco_env),
        _app_main,
    )

    # Run 1: C1 owns "x"
    _source_data["C1"] = {"x": 1}
    app.update_blocking()
    assert GlobalDictTarget.store.data == {
        "x": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
    }
    assert GlobalDictTarget.store.metrics.collect() == {"sink": 1, "upsert": 1}

    # Run 2: Ownership transfers from C1 to C2
    _source_data.clear()
    _source_data["C2"] = {"x": 2}
    app.update_blocking()
    assert GlobalDictTarget.store.data == {
        "x": DictDataWithPrev(data=2, prev=[1], prev_may_be_missing=False),
    }
    # Should be 1 upsert (update), NOT a delete + insert
    assert GlobalDictTarget.store.metrics.collect() == {"sink": 1, "upsert": 1}


def test_ownership_transfer_same_value() -> None:
    """Transfer with same value triggers no-change detection."""
    GlobalDictTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        coco.AppConfig(name="test_ownership_transfer_same_value", environment=coco_env),
        _app_main,
    )

    # Run 1: C1 owns "x" with value 1
    _source_data["C1"] = {"x": 1}
    app.update_blocking()
    assert GlobalDictTarget.store.data == {
        "x": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
    }
    GlobalDictTarget.store.metrics.collect()

    # Run 2: C2 takes over with same value
    _source_data.clear()
    _source_data["C2"] = {"x": 1}
    app.update_blocking()
    # No-change: prev == desired and prev_may_be_missing=False → reconcile returns None
    assert GlobalDictTarget.store.data == {
        "x": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
    }
    assert GlobalDictTarget.store.metrics.collect() == {}


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
    assert GlobalDictTarget.store.metrics.collect() == {"sink": 1, "delete": 1}


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

    # Run 2: C1 gone, C2 takes over
    _source_data.clear()
    _source_data["C2"] = {"x": 2}
    app.update_blocking()
    assert GlobalDictTarget.store.data["x"].data == 2

    # Run 3: C2 gone, C3 takes over
    _source_data.clear()
    _source_data["C3"] = {"x": 3}
    app.update_blocking()
    assert GlobalDictTarget.store.data["x"].data == 3


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
    GlobalDictTarget.store.metrics.collect()

    # Run 2: C1 is gone entirely
    _source_data.clear()
    app.update_blocking()
    assert GlobalDictTarget.store.data == {}
    GlobalDictTarget.store.metrics.collect()

    # Run 3: C2 declares "x" fresh (no previous owner)
    _source_data["C2"] = {"x": 2}
    app.update_blocking()
    assert GlobalDictTarget.store.data == {
        "x": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }

"""Tests for target state ownership transfer between components."""

from __future__ import annotations

import asyncio
from collections.abc import Collection

import cocoindex as coco
import pytest

from tests import common
from tests.common.target_states import (
    BYTES_SEGMENT,
    TUPLE_SEGMENT,
    AtMost,
    DictDataWithPrev,
    DictTargetStateStore,
    GlobalDictTarget,
)

coco_env = common.create_test_env(__file__)

# Controls which component paths declare which target state keys+values.
# Outer key = component name (becomes component subpath), inner key = target state key.
_source_data: dict[str, dict[str, object]] = {}


class _BlockingSinkStore(DictTargetStateStore):
    def __init__(self) -> None:
        self._block_next_apply = False
        self._apply_started: asyncio.Event | None = None
        self._release_apply: asyncio.Event | None = None
        super().__init__(use_async=True)

    def block_next_apply(self) -> None:
        self._block_next_apply = True
        self._apply_started = asyncio.Event()
        self._release_apply = asyncio.Event()

    async def wait_for_blocked_apply(self) -> None:
        assert self._apply_started is not None
        await self._apply_started.wait()

    def release_blocked_apply(self) -> None:
        assert self._release_apply is not None
        self._release_apply.set()

    async def _async_sink(
        self,
        context_provider: coco.ContextProvider,
        actions: Collection[tuple[str, DictDataWithPrev | coco.NonExistenceType]],
        /,
    ) -> None:
        if self._block_next_apply:
            self._block_next_apply = False
            assert self._apply_started is not None
            assert self._release_apply is not None
            self._apply_started.set()
            await self._release_apply.wait()
        self._sink(context_provider, actions)


_concurrent_claim_store = _BlockingSinkStore()
_concurrent_claim_provider = coco.register_root_target_states_provider(
    "test_target_state/concurrent_claim", _concurrent_claim_store
)


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


@pytest.mark.parametrize(
    "owner, reclaimer, late",
    [
        pytest.param("C1", "C2", "C3", id="str-segments"),
        # These tuple/bytes segments collided under the old decoder.
        pytest.param((1, 2), b"\x01\x02", b"abc", id="tuple-and-bytes-segments"),
    ],
)
@pytest.mark.parametrize(
    "exhaust_pending", [False, True], ids=["same-update", "retry-exhaustion"]
)
@pytest.mark.asyncio
async def test_concurrent_claimant_sees_owner_during_sink_apply(
    owner: coco.StableKey,
    reclaimer: coco.StableKey,
    late: coco.StableKey,
    exhaust_pending: bool,
    request: pytest.FixtureRequest,
) -> None:
    """A concurrent claim can resume in this update or retry after exhaustion."""
    source: dict[coco.StableKey, int] = {owner: 1}
    allow_late_mount = asyncio.Event()
    late_declared = asyncio.Event()
    error_received = asyncio.Event()
    errors: list[BaseException] = []

    def record_error(exc: BaseException, ctx: coco.ExceptionContext) -> None:
        errors.append(exc)
        error_received.set()

    @coco.fn
    async def process_claimant(value: int, is_late: bool) -> None:
        coco.declare_target_state(_concurrent_claim_provider.target_state("x", value))
        if is_late:
            late_declared.set()

    @coco.fn
    async def app_main() -> None:
        handles: list[coco.ComponentMountHandle] = []
        async with coco.exception_handler(record_error):
            for segment, value in source.items():
                is_late = segment == late
                if is_late:
                    await allow_late_mount.wait()
                handles.append(
                    await coco.mount(
                        coco.component_subpath(segment),
                        process_claimant,
                        value,
                        is_late,
                    )
                )
        # Finish claims before root cleanup can delete the previous owner.
        for handle in handles:
            await handle.ready()

    _concurrent_claim_store.clear()
    test_env = common.create_test_env(__file__, suffix=request.node.name)
    app = coco.App(
        coco.AppConfig(name="concurrent_claimant", environment=test_env), app_main
    )
    target_path = '/@test_target_state/concurrent_claim/"x"'

    # Establish the first component as the committed owner.
    await app.update()
    assert errors == []
    _concurrent_claim_store.metrics.collect()
    assert await common.list_target_state_owners(app) == {
        target_path: coco.ROOT_PATH / owner,
    }

    # The reclaimer claims x in precommit, then pauses inside sink.apply. The
    # late claimant is mounted only after we have observed that in-flight claim.
    source.clear()
    source.update({reclaimer: 2, late: 3})
    _concurrent_claim_store.block_next_apply()
    update_task = asyncio.ensure_future(app.update())
    try:
        await asyncio.wait_for(
            _concurrent_claim_store.wait_for_blocked_apply(), timeout=5.0
        )
        assert await common.list_target_state_owners(app) == {
            target_path: coco.ROOT_PATH / reclaimer,
        }
        allow_late_mount.set()
        if exhaust_pending:
            # This error proves precommit observed the in-flight owner and
            # refused the takeover for every pending retry while the sink
            # stayed blocked. Collect every error so extra failures cannot hide.
            await asyncio.wait_for(error_received.wait(), timeout=5.0)
            assert len(errors) == 1
            assert "retries waiting for concurrent ownership transfer" in str(errors[0])
            assert _concurrent_claim_store.data["x"].data == 1
            assert _concurrent_claim_store.metrics.collect() == {}
            assert await common.list_target_state_owners(app) == {
                target_path: coco.ROOT_PATH / reclaimer,
            }
        else:
            # Preserve the success case: release after the late declaration
            # and require its transfer to finish within this same update. The
            # exhaustion case above separately proves pending detection.
            await asyncio.wait_for(late_declared.wait(), timeout=5.0)
    finally:
        allow_late_mount.set()
        _concurrent_claim_store.release_blocked_apply()
        await update_task

    if exhaust_pending:
        assert len(errors) == 1
        assert _concurrent_claim_store.data["x"] == DictDataWithPrev(
            data=2, prev=[1], prev_may_be_missing=False
        )
        assert await common.list_target_state_owners(app) == {
            target_path: coco.ROOT_PATH / reclaimer,
        }

        # A subsequent update retries the failed claim using the committed
        # previous state. No additional component errors are allowed.
        errors.clear()
        source.pop(reclaimer)
        await app.update()

    assert errors == []
    assert _concurrent_claim_store.data["x"] == DictDataWithPrev(
        data=3, prev=[2], prev_may_be_missing=False
    )
    assert await common.list_target_state_owners(app) == {
        target_path: coco.ROOT_PATH / late,
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


# Segment -> target states that component declares.
_typed_path_source: dict[coco.StableKey, dict[str, object]] = {}


@coco.fn
async def _process_typed_path_component(entries: dict[str, object]) -> None:
    for key, value in entries.items():
        coco.declare_target_state(GlobalDictTarget.target_state(key, value))


@coco.fn
async def _app_main_typed_paths_await_ready() -> None:
    """Awaits every child's ready() so transfer is a preempt, not delete+insert."""
    handles = []
    with coco.component_subpath("process"):
        for segment in sorted(_typed_path_source, key=repr):
            handles.append(
                await coco.mount(
                    coco.component_subpath(segment),
                    _process_typed_path_component,
                    _typed_path_source[segment],
                )
            )
    for handle in handles:
        await handle.ready()


def _typed_owner(segment: coco.StableKey) -> coco.StablePath:
    return coco.ROOT_PATH / "process" / segment


def test_ownership_transfer_between_typed_component_paths() -> None:
    """Transfer between a tuple-segment and a bytes-segment component path.

    Checks the previous state reaches the new owner (a single upsert carrying
    `prev`, not delete+insert), that the new owner's row is the one kept, and
    that unmounting clears every row.
    """
    GlobalDictTarget.store.clear()
    _typed_path_source.clear()

    app = coco.App(
        coco.AppConfig(name="test_typed_path_ownership_transfer", environment=coco_env),
        _app_main_typed_paths_await_ready,
    )

    # Run 1: the tuple-segment component owns "x".
    _typed_path_source[TUPLE_SEGMENT] = {"x": 1}
    app.update_blocking()
    assert GlobalDictTarget.store.data == {
        "x": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
    }
    assert GlobalDictTarget.store.metrics.collect() == {"sink": AtMost(1), "upsert": 1}
    assert common.list_target_state_owners_sync(app) == {
        '/@test_target_state/global_dict/"x"': _typed_owner(TUPLE_SEGMENT),
    }

    # Run 2: the bytes-segment component takes over. Resolving the previous
    # owner is what the decoder broke, so `prev` is the load-bearing assertion.
    _typed_path_source.clear()
    _typed_path_source[BYTES_SEGMENT] = {"x": 2}
    app.update_blocking()
    assert GlobalDictTarget.store.data == {
        "x": DictDataWithPrev(data=2, prev=[1], prev_may_be_missing=False),
    }
    assert GlobalDictTarget.store.metrics.collect() == {"sink": AtMost(1), "upsert": 1}
    assert common.list_target_state_owners_sync(app) == {
        '/@test_target_state/global_dict/"x"': _typed_owner(BYTES_SEGMENT),
    }

    # Run 3: transfer back from the bytes-segment owner to the tuple segment.
    _typed_path_source.clear()
    _typed_path_source[TUPLE_SEGMENT] = {"x": 3}
    app.update_blocking()
    assert GlobalDictTarget.store.data == {
        "x": DictDataWithPrev(data=3, prev=[2], prev_may_be_missing=False),
    }
    assert common.list_target_state_owners_sync(app) == {
        '/@test_target_state/global_dict/"x"': _typed_owner(TUPLE_SEGMENT),
    }

    # Run 4: unmount. No owner row may survive its component.
    _typed_path_source.clear()
    app.update_blocking()
    assert GlobalDictTarget.store.data == {}
    assert common.list_target_state_owners_sync(app) == {}

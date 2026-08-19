"""Optimistic target-state writes through the Python API.

Exercises the PyO3 bridge against an in-memory target, so the lifecycle has
evidence that does not depend on Docker:

* `coco.declare_target_state_optimistic` combines immediate visibility,
  submit confirmation, failure cleanup, and CAS winner election.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any, Collection

import cocoindex as coco

from tests import common

coco_env = common.create_test_env(__file__)


class _RowStore:
    """Minimal dict-backed target whose delete path honours
    `prev_may_be_missing`, which is what optimistic cleanup relies on: it
    must delete a row it may have written even though nothing is tracked
    yet. (`tests.common.target_states.GlobalDictTarget` deliberately skips
    that case, so it can't observe cleanup.)"""

    def __init__(self) -> None:
        self.data: dict[str, Any] = {}
        self.upserts = 0
        self.deletes = 0
        self.fail_next_upsert = False
        self._lock = threading.Lock()
        self._sink = coco.TargetActionSink.from_fn(self._apply)

    def clear(self) -> None:
        with self._lock:
            self.data.clear()
            self.upserts = 0
            self.deletes = 0
            self.fail_next_upsert = False

    def _apply(
        self,
        context_provider: coco.ContextProvider,
        actions: Collection[tuple[str, Any]],
        /,
    ) -> None:
        with self._lock:
            for key, value in actions:
                if coco.is_non_existence(value):
                    self.data.pop(key, None)
                    self.deletes += 1
                else:
                    if self.fail_next_upsert:
                        self.fail_next_upsert = False
                        raise ValueError("injected sink failure")
                    self.data[key] = value
                    self.upserts += 1

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: Any | coco.NonExistenceType,
        prev_possible_records: Collection[Any],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[tuple[str, Any], Any] | None:
        assert isinstance(key, str)
        if coco.is_non_existence(desired_state):
            if not prev_possible_records and not prev_may_be_missing:
                return None
            return coco.TargetReconcileOutput(
                action=(key, coco.NON_EXISTENCE),
                sink=self._sink,
                tracking_record=coco.NON_EXISTENCE,
            )
        if not prev_may_be_missing and all(
            prev == desired_state for prev in prev_possible_records
        ):
            return None
        return coco.TargetReconcileOutput(
            action=(key, desired_state),
            sink=self._sink,
            tracking_record=desired_state,
        )


_store = _RowStore()
_target = coco.register_root_target_states_provider(
    "test_target_state/optimistic_rows", _store
).target_state


class _Scenario:
    """Per-test wiring for the module-level component functions."""

    proposals: dict[str, str] = {}
    claim_results: dict[str, bool] = {}
    observed: dict[str, Any] = {}
    fail_after_write: set[str] = set()
    barrier: asyncio.Barrier | None = None
    caught_eager_error: dict[str, str] = {}
    executions: int = 0
    dependency_barriers: tuple[asyncio.Barrier, asyncio.Barrier] | None = None

    @classmethod
    def reset(cls) -> None:
        _store.clear()
        cls.proposals = {}
        cls.claim_results = {}
        cls.observed = {}
        cls.fail_after_write = set()
        cls.barrier = None
        cls.caught_eager_error = {}
        cls.executions = 0
        cls.dependency_barriers = None


def _stored(key: str) -> Any:
    return _store.data.get(key)


@coco.fn
async def _optimistic_writer(comp: str, key: str) -> None:
    if _Scenario.barrier is not None:
        await _Scenario.barrier.wait()
    won = await coco.declare_target_state_optimistic(
        _target(key, _Scenario.proposals[comp])
    )
    _Scenario.claim_results[comp] = won
    # Still inside the processor: the write is already applied.
    _Scenario.observed[comp] = _stored(key)
    if comp in _Scenario.fail_after_write:
        raise RuntimeError(f"component {comp} failed after its optimistic write")


@coco.fn(memo=True)
async def _memoized_optimistic_writer(comp: str, key: str) -> None:
    _Scenario.executions += 1
    won = await coco.declare_target_state_optimistic(
        _target(key, _Scenario.proposals[comp])
    )
    if not won:
        raise RuntimeError("first memoized execution unexpectedly lost its claim")


@coco.fn
async def _fail_before_optimistic_write(comp: str, key: str) -> None:
    raise RuntimeError(f"component {comp} failed before writing {key}")


@coco.fn
async def _failing_eager_writer(comp: str, key: str) -> None:
    _store.fail_next_upsert = True
    try:
        await coco.declare_target_state_optimistic(
            _target(key, _Scenario.proposals[comp])
        )
    except Exception as e:  # noqa: BLE001 - the point of the test
        _Scenario.caught_eager_error[comp] = str(e)
    finally:
        _store.fail_next_upsert = False


@coco.fn
async def _conditional_writer(comp: str, key: str) -> None:
    """Get-or-create: look first, claim only when nothing is there."""
    existing = _stored(key)
    if _Scenario.barrier is not None:
        await _Scenario.barrier.wait()

    if existing is not None:
        _Scenario.claim_results[comp] = False
        coco.declare_target_state(_target(key, existing))
        _Scenario.observed[comp] = existing
        return

    won = await coco.declare_target_state_optimistic(
        _target(key, _Scenario.proposals[comp])
    )
    _Scenario.claim_results[comp] = won
    _Scenario.observed[comp] = _stored(key)


@coco.fn
async def _failing_entity_writer(entity_key: str) -> None:
    barriers = _Scenario.dependency_barriers
    assert barriers is not None
    won = await coco.declare_target_state_optimistic(_target(entity_key, "entity-id"))
    assert won
    await barriers[0].wait()
    await barriers[1].wait()
    raise RuntimeError("entity writer failed after its sibling read the row")


@coco.fn
async def _reference_reader(entity_key: str, reference_key: str) -> None:
    barriers = _Scenario.dependency_barriers
    assert barriers is not None
    await barriers[0].wait()
    entity_id = _stored(entity_key)
    assert entity_id == "entity-id"
    _Scenario.observed["reference"] = entity_id
    coco.declare_target_state(_target(reference_key, entity_id))
    await barriers[1].wait()


def _make_app(
    name: str, writer: Any, components: list[str], key: str
) -> "coco.App[..., None]":
    async def app_main() -> None:
        with coco.component_subpath("writers"):
            for comp in components:
                await coco.mount(coco.component_subpath(comp), writer, comp, key)

    return coco.App(coco.AppConfig(name=name, environment=coco_env), app_main)


# ---------------------------------------------------------------------------
# Unified optimistic-write API
# ---------------------------------------------------------------------------


def test_optimistic_write_is_visible_during_processing() -> None:
    _Scenario.reset()
    _Scenario.proposals = {"c1": "v1"}

    _make_app(
        "test_optimistic_visible", _optimistic_writer, ["c1"], "einstein"
    ).update_blocking()

    assert _Scenario.observed["c1"] == "v1", (
        "eager write must be visible mid-processing"
    )
    assert _stored("einstein") == "v1"
    # Eager write plus the authoritative re-apply from normal submit.
    assert _store.upserts == 2


def test_caught_eager_failure_heals_at_submit() -> None:
    _Scenario.reset()
    _Scenario.proposals = {"c1": "v1"}

    _make_app(
        "test_optimistic_heal", _failing_eager_writer, ["c1"], "einstein"
    ).update_blocking()

    assert "c1" in _Scenario.caught_eager_error, "eager write should have failed"
    assert _stored("einstein") == "v1", "submit must heal the caught eager failure"


def test_component_failure_removes_the_eager_write() -> None:
    _Scenario.reset()
    _Scenario.proposals = {"c1": "v1"}
    _Scenario.fail_after_write = {"c1"}

    # A background-mounted child's failure is logged and swallowed by the
    # default error handler, so the update itself still completes.
    _make_app(
        "test_optimistic_cleanup", _optimistic_writer, ["c1"], "einstein"
    ).update_blocking()

    assert _Scenario.observed["c1"] == "v1", "the row existed during processing"
    assert _stored("einstein") is None, "engine cleanup must delete the eager write"


def test_sibling_reference_can_dangle_after_optimistic_writer_fails() -> None:
    """Scenario 6: pin the documented dependency-revalidation limitation.

    The reader observes the eager entity and ordinarily declares its reference
    before allowing the entity writer to fail. Cleanup removes the entity, but
    the independently owned reference remains.
    """
    _Scenario.reset()
    _Scenario.dependency_barriers = (asyncio.Barrier(2), asyncio.Barrier(2))

    async def app_main() -> None:
        await coco.mount(
            coco.component_subpath("entity-writer"),
            _failing_entity_writer,
            "entity",
        )
        await coco.mount(
            coco.component_subpath("reference-reader"),
            _reference_reader,
            "entity",
            "reference",
        )

    coco.App(
        coco.AppConfig(name="test_optimistic_dangling_reference", environment=coco_env),
        app_main,
    ).update_blocking()

    assert _Scenario.observed["reference"] == "entity-id"
    assert _stored("entity") is None
    assert _stored("reference") == "entity-id"


def test_failure_before_optimistic_call_has_nothing_to_clean() -> None:
    """Scenario 4: failing before the call creates no claim, write, or delete."""
    _Scenario.reset()

    _make_app(
        "test_optimistic_fail_before",
        _fail_before_optimistic_write,
        ["c1"],
        "einstein",
    ).update_blocking()

    assert _store.upserts == 0
    assert _store.deletes == 0
    assert _stored("einstein") is None


def test_unchanged_rerun_is_a_memo_hit() -> None:
    _Scenario.reset()
    _Scenario.proposals = {"c1": "v1"}
    app = _make_app(
        "test_optimistic_rerun", _memoized_optimistic_writer, ["c1"], "einstein"
    )

    app.update_blocking()
    assert _store.upserts == 2
    _store.upserts = 0

    app.update_blocking()
    assert _store.upserts == 0
    assert _Scenario.executions == 1
    assert _stored("einstein") == "v1"


def test_independent_optimistic_keys_do_not_contend() -> None:
    _Scenario.reset()
    _Scenario.proposals = {"c1": "v1", "c2": "v2"}

    async def app_main() -> None:
        with coco.component_subpath("writers"):
            for comp, key in (("c1", "k1"), ("c2", "k2")):
                await coco.mount(
                    coco.component_subpath(comp), _optimistic_writer, comp, key
                )

    coco.App(
        coco.AppConfig(name="test_optimistic_pair", environment=coco_env), app_main
    ).update_blocking()

    assert _stored("k1") == "v1"
    assert _stored("k2") == "v2"


def test_optimistic_write_elects_one_winner() -> None:
    """Two components propose different values for one key at the same
    moment: exactly one wins and the loser writes nothing."""
    _Scenario.reset()
    _Scenario.proposals = {"c1": "from-c1", "c2": "from-c2"}
    _Scenario.barrier = asyncio.Barrier(2)

    _make_app(
        "test_optimistic_cas_race", _conditional_writer, ["c1", "c2"], "einstein"
    ).update_blocking()

    assert sorted(_Scenario.claim_results) == ["c1", "c2"]
    winners = [c for c, won in _Scenario.claim_results.items() if won]
    assert len(winners) == 1, f"expected one winner, got {_Scenario.claim_results}"
    assert _stored("einstein") == _Scenario.proposals[winners[0]]


def test_changed_rerun_reuses_confirmed_owner() -> None:
    _Scenario.reset()
    _Scenario.proposals = {"c1": "first"}
    app = _make_app(
        "test_optimistic_cas_confirmed", _conditional_writer, ["c1"], "einstein"
    )

    app.update_blocking()
    assert _Scenario.claim_results == {"c1": True}

    _Scenario.claim_results = {}
    _Scenario.proposals = {"c1": "second"}
    app.update_blocking()
    assert _Scenario.claim_results == {"c1": False}, (
        "an existing value must be reused, not re-claimed"
    )
    assert _stored("einstein") == "first"


def test_optimistic_writes_on_independent_keys_both_win() -> None:
    _Scenario.reset()
    _Scenario.proposals = {"c1": "v1", "c2": "v2"}

    async def app_main() -> None:
        with coco.component_subpath("writers"):
            for comp, key in (("c1", "k1"), ("c2", "k2")):
                await coco.mount(
                    coco.component_subpath(comp), _conditional_writer, comp, key
                )

    coco.App(
        coco.AppConfig(name="test_optimistic_cas_indep", environment=coco_env), app_main
    ).update_blocking()

    assert _Scenario.claim_results == {"c1": True, "c2": True}
    assert _stored("k1") == "v1"
    assert _stored("k2") == "v2"

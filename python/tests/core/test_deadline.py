from __future__ import annotations

import asyncio
from collections.abc import Collection
from datetime import timedelta
from typing import Any, Iterator

import pytest

import cocoindex as coco
from cocoindex._internal import core
from cocoindex._internal import deadline as _deadline
from cocoindex._internal.component_ctx import next_id as _next_id
from tests import common
from tests.common.target_states import DictDataWithPrev, GlobalDictTarget


class _FakeClock:
    def __init__(
        self,
        now: float = 0.0,
        real_sleep: Any = asyncio.sleep,
    ) -> None:
        self._now = 0.0
        self.sleeps: list[float] = []
        self._real_sleep = real_sleep
        core.testing_reset_deadline_clock()
        self.now = now

    @property
    def now(self) -> float:
        return self._now

    @now.setter
    def now(self, value: float) -> None:
        if value < self._now:
            core.testing_reset_deadline_clock()
            self._now = 0.0
        delta = value - self._now
        if delta:
            core.testing_advance_deadline_clock(round(delta * 1000))
        self._now = value

    async def sleep(self, delay: float) -> None:
        self.sleeps.append(delay)
        self.now += delay
        await self._real_sleep(0)


@pytest.fixture
def fake_clock(monkeypatch: pytest.MonkeyPatch) -> Iterator[_FakeClock]:
    real_sleep = asyncio.sleep
    clock = _FakeClock(real_sleep=real_sleep)
    monkeypatch.setattr(asyncio, "sleep", clock.sleep)
    yield clock
    core.testing_disable_deadline_clock()


def _env(suffix: str) -> coco.Environment:
    return common.create_test_env(__file__, suffix=suffix)


class _RecordingTargetStore:
    # Used by submit-boundary tests:
    #
    # processor deadline       submit/sink body             caller result
    # ------------------       ----------------             -------------
    # checked before submit -> deadline is cleared here -> checked again
    #
    # If the fake clock advances inside _apply(), target writes must still land
    # consistently, and only the caller's post-submit checkpoint should raise.
    def __init__(
        self,
        *,
        fake_clock: _FakeClock | None = None,
        advance_clock_to: float | None = None,
    ) -> None:
        self.seen_deadlines: list[float | None] = []
        self.applied: list[Any] = []
        self._fake_clock = fake_clock
        self._advance_clock_to = advance_clock_to
        self._sink = coco.TargetActionSink.from_fn(self._apply)

    def _apply(
        self,
        context_provider: coco.ContextProvider,
        actions: Collection[tuple[coco.StableKey, Any]],
        /,
    ) -> None:
        self.seen_deadlines.append(_deadline.remaining_seconds())
        self.applied.extend(value for _, value in actions)
        if self._fake_clock is not None and self._advance_clock_to is not None:
            self._fake_clock.now = self._advance_clock_to

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: Any | coco.NonExistenceType,
        prev_possible_records: Collection[Any],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[tuple[coco.StableKey, Any], Any] | None:
        if coco.is_non_existence(desired_state):
            return None
        if not prev_may_be_missing and desired_state in prev_possible_records:
            return None
        return coco.TargetReconcileOutput(
            action=(key, desired_state),
            sink=self._sink,
            tracking_record=desired_state,
        )


def test_timeout_nested_uses_min_and_restores_exactly(
    fake_clock: _FakeClock,
) -> None:
    assert _deadline.remaining_seconds() is None

    with coco.timeout(timedelta(seconds=10)):
        assert _deadline.remaining_seconds() == 10

        with coco.timeout(timedelta(seconds=20)):
            assert _deadline.remaining_seconds() == 10
        assert _deadline.remaining_seconds() == 10

        fake_clock.now = 5
        with coco.timeout(timedelta(seconds=1)):
            assert _deadline.remaining_seconds() == 1
        assert _deadline.remaining_seconds() == 5

    assert _deadline.remaining_seconds() is None


def test_check_deadline_raises_only_after_deadline(fake_clock: _FakeClock) -> None:
    with coco.timeout(timedelta(seconds=10)):
        fake_clock.now = 10
        coco.check_deadline()

        fake_clock.now = 10.001
        with pytest.raises(coco.DeadlineExceededError):
            coco.check_deadline()


def test_use_mount_child_processor_inherits_parent_deadline(
    fake_clock: _FakeClock,
) -> None:
    seen: list[float | None] = []

    @coco.fn
    async def child() -> None:
        seen.append(_deadline.remaining_seconds())

    @coco.fn
    async def main() -> None:
        await coco.use_mount(coco.component_subpath("child"), child)

    app = coco.App(coco.AppConfig(name="deadline_d3", environment=_env("d3")), main)
    with coco.timeout(timedelta(seconds=10)):
        app.update_blocking()

    assert seen == [10]


def test_root_processor_inherits_update_deadline(fake_clock: _FakeClock) -> None:
    seen: list[float | None] = []

    @coco.fn
    async def main() -> None:
        seen.append(_deadline.remaining_seconds())

    app = coco.App(coco.AppConfig(name="deadline_d3b", environment=_env("d3b")), main)
    with coco.timeout(timedelta(seconds=5)):
        app.update_blocking()

    assert seen == [5]


def test_processor_return_checks_deadline_before_submit(fake_clock: _FakeClock) -> None:
    GlobalDictTarget.store.clear()

    @coco.fn
    async def main() -> None:
        coco.declare_target_state(GlobalDictTarget.target_state("post_body", "v"))
        fake_clock.now = 11

    app = coco.App(
        coco.AppConfig(name="deadline_post_body", environment=_env("post_body")),
        main,
    )
    with coco.timeout(timedelta(seconds=10)):
        with pytest.raises(coco.DeadlineExceededError):
            app.update_blocking()

    assert GlobalDictTarget.store.data == {}


@pytest.mark.asyncio
async def test_lazy_update_handle_uses_captured_deadline_snapshot(
    fake_clock: _FakeClock,
) -> None:
    seen: list[tuple[str, float | None]] = []

    @coco.fn
    async def main(label: str) -> None:
        seen.append((label, _deadline.remaining_seconds()))

    outside_app = coco.App(
        coco.AppConfig(
            name="deadline_lazy_handle_outside", environment=_env("lazy_outside")
        ),
        main,
        "outside",
    )
    outside_handle = outside_app.update()
    with coco.timeout(timedelta(seconds=10)):
        fake_clock.now = 11
        await outside_handle.result()

    assert seen == [("outside", None)]

    fake_clock.now = 0
    inside_app = coco.App(
        coco.AppConfig(
            name="deadline_lazy_handle_inside", environment=_env("lazy_inside")
        ),
        main,
        "inside",
    )
    with coco.timeout(timedelta(seconds=10)):
        inside_handle = inside_app.update()

    fake_clock.now = 11
    with pytest.raises(coco.DeadlineExceededError):
        await inside_handle.result()

    assert seen == [("outside", None)]


def test_use_mount_checks_deadline_when_child_returns_after_deadline(
    fake_clock: _FakeClock,
) -> None:
    # use_mount() keeps the child and parent consistent:
    #
    # parent awaits use_mount(child)
    #          |
    #          v
    # child finishes after the parent's deadline
    #          |
    #          v
    # parent checks its own deadline before using the child result
    #
    # The parent must fail here, before it can declare target states that depend
    # on a child result received after its timeout.
    GlobalDictTarget.store.clear()
    continued = False

    @coco.fn
    async def child() -> str:
        fake_clock.now = 11
        return "done"

    @coco.fn
    async def main() -> None:
        nonlocal continued
        await coco.use_mount(coco.component_subpath("child"), child)
        continued = True
        coco.declare_target_state(GlobalDictTarget.target_state("use_mount", "v"))

    app = coco.App(
        coco.AppConfig(
            name="deadline_use_mount_return", environment=_env("use_mount_return")
        ),
        main,
    )
    with coco.timeout(timedelta(seconds=10)):
        with pytest.raises(coco.DeadlineExceededError):
            app.update_blocking()

    assert not continued
    assert GlobalDictTarget.store.data == {}


def test_mount_and_mount_each_children_are_deadline_isolated(
    fake_clock: _FakeClock,
) -> None:
    seen: dict[str, float | None] = {}

    @coco.fn
    async def mounted(label: str) -> None:
        seen[label] = _deadline.remaining_seconds()

    @coco.fn
    async def main() -> None:
        one = await coco.mount(coco.component_subpath("mount"), mounted, "mount")
        many = await coco.mount_each(
            coco.component_subpath("each"), mounted, [("item", "mount_each")]
        )
        await one.ready()
        await many.ready()

    app = coco.App(coco.AppConfig(name="deadline_d4", environment=_env("d4")), main)
    with coco.timeout(timedelta(seconds=10)):
        app.update_blocking()

    assert seen == {"mount": None, "mount_each": None}


def test_mount_ready_checks_deadline_after_isolated_child_returns(
    fake_clock: _FakeClock,
) -> None:
    GlobalDictTarget.store.clear()
    continued = False
    saved_handle: coco.ComponentMountHandle | None = None

    @coco.fn
    async def mounted() -> None:
        fake_clock.now = 11

    @coco.fn
    async def main() -> None:
        nonlocal continued, saved_handle
        handle = await coco.mount(coco.component_subpath("mounted"), mounted)
        saved_handle = handle
        await handle.ready()
        continued = True
        coco.declare_target_state(GlobalDictTarget.target_state("mount_ready", "v"))

    app = coco.App(
        coco.AppConfig(
            name="deadline_mount_ready_return", environment=_env("mount_ready_return")
        ),
        main,
    )
    with coco.timeout(timedelta(seconds=10)):
        with pytest.raises(coco.DeadlineExceededError):
            app.update_blocking()

    assert not continued
    assert GlobalDictTarget.store.data == {}
    assert saved_handle is not None

    fake_clock.now = 0
    with coco.timeout(timedelta(seconds=20)):
        asyncio.run(saved_handle.ready())


def test_live_component_process_live_is_deadline_isolated(
    fake_clock: _FakeClock,
) -> None:
    seen: dict[str, float | None] = {}

    class Live:
        async def process(self) -> None:
            seen["process"] = _deadline.remaining_seconds()

        async def process_live(self, operator: coco.LiveComponentOperator) -> None:
            seen["process_live"] = _deadline.remaining_seconds()
            await operator.update_full()
            await operator.mark_ready()

    @coco.fn
    async def main() -> None:
        await coco.mount(coco.component_subpath("live"), Live)

    app = coco.App(
        coco.AppConfig(name="deadline_live_isolated", environment=_env("live_iso")),
        main,
    )
    with coco.timeout(timedelta(seconds=10)):
        app.update_blocking()

    assert seen == {"process_live": None, "process": None}


def test_map_task_checks_deadline_after_return(fake_clock: _FakeClock) -> None:
    continued = False

    async def mapped(_: int) -> int:
        fake_clock.now = 11
        return 1

    @coco.fn
    async def main() -> None:
        nonlocal continued
        await coco.map(mapped, [1])
        continued = True

    app = coco.App(
        coco.AppConfig(name="deadline_map_return", environment=_env("map_return")),
        main,
    )
    with coco.timeout(timedelta(seconds=10)):
        with pytest.raises(coco.DeadlineExceededError):
            app.update_blocking()

    assert not continued


@pytest.mark.asyncio
async def test_map_deadline_drains_started_siblings_without_cancelling(
    fake_clock: _FakeClock,
) -> None:
    # map() proof for cooperative deadlines:
    #
    # slow task starts and waits
    # deadline task observes DeadlineExceededError
    # map() drains slow task instead of cancelling it
    # caller receives DeadlineExceededError after all started tasks settle
    started = asyncio.Event()
    unblock_sibling = asyncio.Event()
    sibling_cancelled = False
    sibling_finished = False

    async def mapped(label: str) -> str:
        nonlocal sibling_cancelled, sibling_finished
        if label == "slow":
            started.set()
            try:
                await unblock_sibling.wait()
            except asyncio.CancelledError:
                sibling_cancelled = True
                raise
            sibling_finished = True
            return label

        await started.wait()

        async def release_sibling() -> None:
            await asyncio.sleep(0)
            unblock_sibling.set()

        asyncio.create_task(release_sibling())
        fake_clock.now = 11
        coco.check_deadline()
        return label

    with coco.timeout(timedelta(seconds=10)):
        with pytest.raises(coco.DeadlineExceededError):
            await coco.map(mapped, ["slow", "deadline"])

    assert sibling_finished
    assert not sibling_cancelled


@pytest.mark.asyncio
async def test_map_mixed_failures_are_reported_by_input_order(
    fake_clock: _FakeClock,
) -> None:
    # Determinism proof:
    #
    # input order decides the reported failure, not task scheduling order.
    # ["runtime", "deadline"] -> RuntimeError
    # ["deadline", "runtime"] -> DeadlineExceededError
    async def mapped(label: str) -> str:
        if label == "runtime":
            raise RuntimeError("mapped boom")
        fake_clock.now = 11
        coco.check_deadline()
        return label

    with coco.timeout(timedelta(seconds=10)):
        with pytest.raises(RuntimeError, match="mapped boom"):
            await coco.map(mapped, ["runtime", "deadline"])

    fake_clock.now = 0
    with coco.timeout(timedelta(seconds=10)):
        with pytest.raises(coco.DeadlineExceededError):
            await coco.map(mapped, ["deadline", "runtime"])


@pytest.mark.asyncio
async def test_map_post_return_deadline_is_item_failure_in_input_order(
    fake_clock: _FakeClock,
) -> None:
    runtime_started = asyncio.Event()
    release_runtime = asyncio.Event()

    async def mapped(label: str) -> str:
        if label == "runtime":
            runtime_started.set()
            await release_runtime.wait()
            raise RuntimeError("mapped boom")

        await runtime_started.wait()
        fake_clock.now = 11
        release_runtime.set()
        return label

    with coco.timeout(timedelta(seconds=10)):
        with pytest.raises(coco.DeadlineExceededError):
            await coco.map(mapped, ["deadline", "runtime"])


@pytest.mark.asyncio
async def test_map_can_return_exception_objects() -> None:
    async def mapped(label: str) -> Exception:
        return RuntimeError(label)

    results = await coco.map(mapped, ["value"])

    assert len(results) == 1
    assert isinstance(results[0], RuntimeError)
    assert str(results[0]) == "value"


def test_plain_coco_fn_checks_deadline_after_return(fake_clock: _FakeClock) -> None:
    continued = False

    @coco.fn
    async def child() -> str:
        fake_clock.now = 11
        return "done"

    @coco.fn
    async def main() -> None:
        nonlocal continued
        await child()
        continued = True

    app = coco.App(
        coco.AppConfig(name="deadline_fn_return", environment=_env("fn_return")),
        main,
    )
    with coco.timeout(timedelta(seconds=10)):
        with pytest.raises(coco.DeadlineExceededError):
            app.update_blocking()

    assert not continued


def test_sink_body_is_deadline_isolated(fake_clock: _FakeClock) -> None:
    with coco.timeout(timedelta(seconds=10)):
        store = _RecordingTargetStore()
        provider = coco.register_root_target_states_provider(
            "test_deadline/sink_isolated", store
        )

        @coco.fn
        async def main() -> None:
            coco.declare_target_state(provider.target_state("k", "v"))

        app = coco.App(coco.AppConfig(name="deadline_d5", environment=_env("d5")), main)
        app.update_blocking()

    assert store.seen_deadlines == [None]


def test_update_blocking_checks_captured_deadline_after_submit(
    fake_clock: _FakeClock,
) -> None:
    # Submit is isolated, but the caller still owns the wait:
    #
    # processor succeeds -> sink applies "v" with no deadline -> clock expires
    #                  -> update_blocking() raises before returning to caller
    store = _RecordingTargetStore(fake_clock=fake_clock, advance_clock_to=11)
    provider = coco.register_root_target_states_provider(
        "test_deadline/update_blocking_post_submit", store
    )

    @coco.fn
    async def main() -> None:
        coco.declare_target_state(provider.target_state("k", "v"))

    app = coco.App(
        coco.AppConfig(
            name="deadline_update_blocking_post_submit",
            environment=_env("update_blocking_post_submit"),
        ),
        main,
    )
    with coco.timeout(timedelta(seconds=10)):
        with pytest.raises(coco.DeadlineExceededError):
            app.update_blocking()

    assert store.seen_deadlines == [None]
    assert store.applied == ["v"]

    fake_clock.now = 0
    with coco.timeout(timedelta(seconds=20)):
        app.update_blocking()

    assert store.applied == ["v"]


@pytest.mark.asyncio
async def test_update_handle_checks_captured_deadline_after_submit(
    fake_clock: _FakeClock,
) -> None:
    # Same post-submit proof for the async handle path:
    #
    # handle created under timeout -> submit runs isolated -> result() checks
    # the captured caller deadline before handing the result back.
    store = _RecordingTargetStore(fake_clock=fake_clock, advance_clock_to=11)
    provider = coco.register_root_target_states_provider(
        "test_deadline/update_handle_post_submit", store
    )

    @coco.fn
    async def main() -> None:
        coco.declare_target_state(provider.target_state("k", "v"))

    app = coco.App(
        coco.AppConfig(
            name="deadline_update_handle_post_submit",
            environment=_env("update_handle_post_submit"),
        ),
        main,
    )
    with coco.timeout(timedelta(seconds=10)):
        handle = app.update()

    with pytest.raises(coco.DeadlineExceededError):
        await handle.result()

    assert store.seen_deadlines == [None]
    assert store.applied == ["v"]


def test_batched_runner_body_is_deadline_isolated(fake_clock: _FakeClock) -> None:
    seen: list[float | None] = []

    @coco.fn.as_async(batching=True)
    def batched(items: list[int]) -> list[int]:
        seen.append(_deadline.remaining_seconds())
        return items

    @coco.fn
    async def main() -> None:
        assert await batched(1) == 1

    app = coco.App(coco.AppConfig(name="deadline_d6", environment=_env("d6")), main)
    with coco.timeout(timedelta(seconds=10)):
        app.update_blocking()

    assert seen == [None]


def test_batched_runner_caller_checks_deadline_after_return(
    fake_clock: _FakeClock,
) -> None:
    @coco.fn.as_async(batching=True)
    def batched(items: list[int]) -> list[int]:
        fake_clock.now = 11
        return items

    @coco.fn
    async def main() -> None:
        await batched(1)

    app = coco.App(
        coco.AppConfig(name="deadline_d6_after_return", environment=_env("d6_after")),
        main,
    )
    with coco.timeout(timedelta(seconds=10)):
        with pytest.raises(coco.DeadlineExceededError):
            app.update_blocking()


def test_next_id_checks_deadline_before_allocating(fake_clock: _FakeClock) -> None:
    @coco.fn
    async def main() -> None:
        fake_clock.now = 11
        await _next_id()

    app = coco.App(coco.AppConfig(name="deadline_d7", environment=_env("d7")), main)
    with coco.timeout(timedelta(seconds=10)):
        with pytest.raises(coco.DeadlineExceededError):
            app.update_blocking()


@pytest.mark.asyncio
async def test_retry_until_deadline_bounds_attempts_and_sleeps(
    fake_clock: _FakeClock,
) -> None:
    attempts: list[float] = []

    async def attempt() -> str | None:
        attempts.append(fake_clock.now)
        return None

    with coco.timeout(timedelta(seconds=10)):
        with pytest.raises(coco.DeadlineExceededError):
            await _deadline.retry_until_deadline(attempt, backoff_seconds=3)

    assert attempts == [0, 3, 6, 9]
    assert fake_clock.sleeps == [3, 3, 3, 1]


def test_deadline_after_declaring_target_states_applies_no_sink_actions(
    fake_clock: _FakeClock,
) -> None:
    # Two-phase proof:
    #
    # declare target state in memory -> deadline raises during processor
    #                             -> submit is never entered -> zero sink writes
    # next run without timeout    -> same declaration retries and lands
    GlobalDictTarget.store.clear()
    should_timeout = True

    @coco.fn
    async def main() -> None:
        nonlocal should_timeout
        coco.declare_target_state(GlobalDictTarget.target_state("k", "v"))
        if should_timeout:
            fake_clock.now = 11
            coco.check_deadline()

    app = coco.App(coco.AppConfig(name="deadline_d9", environment=_env("d9")), main)

    with coco.timeout(timedelta(seconds=10)):
        with pytest.raises(coco.DeadlineExceededError):
            app.update_blocking()
    assert GlobalDictTarget.store.data == {}
    assert GlobalDictTarget.store.metrics.collect() == {}

    should_timeout = False
    fake_clock.now = 0
    with coco.timeout(timedelta(seconds=10)):
        app.update_blocking()

    assert GlobalDictTarget.store.data == {
        "k": DictDataWithPrev(data="v", prev=[], prev_may_be_missing=True)
    }


def test_deadline_exceptions_are_not_memoized(fake_clock: _FakeClock) -> None:
    # Memo proof:
    #
    # run 1: body raises DeadlineExceededError -> no memo value stored
    # run 2: wider deadline executes body again and stores "ok"
    # run 3: expired before memo lookup -> core pre-memo checkpoint raises
    # run 4: wider deadline returns cached "ok" without re-running body
    calls = 0
    should_timeout = True
    expire_before_call = False
    memo_value_returned_to_main = False

    @coco.fn(memo=True)
    def memoized() -> str:
        nonlocal calls, should_timeout
        calls += 1
        if should_timeout:
            fake_clock.now = 11
            coco.check_deadline()
        return "ok"

    @coco.fn
    async def main() -> str:
        nonlocal memo_value_returned_to_main
        if expire_before_call:
            fake_clock.now = 11
        result = memoized()
        memo_value_returned_to_main = True
        return result

    app = coco.App(coco.AppConfig(name="deadline_d10", environment=_env("d10")), main)

    with coco.timeout(timedelta(seconds=10)):
        with pytest.raises(coco.DeadlineExceededError):
            app.update_blocking()
    assert calls == 1

    should_timeout = False
    fake_clock.now = 0
    with coco.timeout(timedelta(seconds=20)):
        assert app.update_blocking() == "ok"
    assert calls == 2

    expire_before_call = True
    memo_value_returned_to_main = False
    fake_clock.now = 0
    with coco.timeout(timedelta(seconds=10)):
        with pytest.raises(coco.DeadlineExceededError):
            app.update_blocking()
    assert calls == 2
    assert not memo_value_returned_to_main

    with coco.timeout(timedelta(seconds=20)):
        assert app.update_blocking() == "ok"
    assert calls == 2


def test_expired_deadline_boundary_matrix(fake_clock: _FakeClock) -> None:
    # Boundary matrix proof for an already-expired caller deadline:
    #
    # inherited entry points:  check_deadline, coco.fn, map, use_mount,
    #                          mount entry, mount_each entry, mount_target,
    #                          next_id
    # isolated work bodies:    mounted children, mount_each children,
    #                          batched runner body, sink body
    #
    # One wrong propagation decision flips exactly one value in this vector.
    vector: dict[str, str] = {}

    class Store:
        def __init__(self) -> None:
            self._sink = coco.TargetActionSink.from_fn(self._apply)

        def _apply(
            self,
            context_provider: coco.ContextProvider,
            actions: Collection[tuple[coco.StableKey, Any]],
            /,
        ) -> None:
            vector["sink_body"] = (
                "raise" if _raises_deadline(coco.check_deadline) else "no_raise"
            )

        def reconcile(
            self,
            key: coco.StableKey,
            desired_state: Any | coco.NonExistenceType,
            prev_possible_records: Collection[Any],
            prev_may_be_missing: bool,
            /,
        ) -> coco.TargetReconcileOutput[tuple[coco.StableKey, Any], Any] | None:
            if coco.is_non_existence(desired_state):
                return None
            return coco.TargetReconcileOutput(
                action=(key, desired_state),
                sink=self._sink,
                tracking_record=desired_state,
            )

    provider = coco.register_root_target_states_provider(
        "test_deadline/boundary_matrix", Store()
    )

    @coco.fn
    async def plain() -> None:
        vector["plain_coco_fn_call"] = "no_raise"

    async def mapped(_: int) -> int:
        vector["map_task"] = "no_raise"
        return 1

    @coco.fn
    async def mounted(label: str) -> None:
        vector[label] = "raise" if _raises_deadline(coco.check_deadline) else "no_raise"

    @coco.fn.as_async(batching=True)
    def batched(items: list[int]) -> list[int]:
        vector["batched_body"] = (
            "raise" if _raises_deadline(coco.check_deadline) else "no_raise"
        )
        return items

    @coco.fn
    async def main() -> None:
        mount_handle = await coco.mount(
            coco.component_subpath("mount_before_expiry"), mounted, "mount_child"
        )
        mount_each_handle = await coco.mount_each(
            coco.component_subpath("each_before_expiry"),
            mounted,
            [("item", "mount_each_child")],
        )
        await mount_handle.ready()
        await mount_each_handle.ready()
        await batched(1)

        fake_clock.now = 11

        vector["check_deadline"] = (
            "raise" if _raises_deadline(coco.check_deadline) else "no_raise"
        )
        vector["plain_coco_fn_call"] = (
            "raise" if await _raises_deadline_async(plain) else "no_raise"
        )
        vector["map_entry"] = (
            "raise"
            if await _raises_deadline_async(coco.map, mapped, [1])
            else "no_raise"
        )
        vector["use_mount_entry"] = (
            "raise"
            if await _raises_deadline_async(
                coco.use_mount, coco.component_subpath("use_after_expiry"), mounted, "x"
            )
            else "no_raise"
        )
        vector["mount_entry"] = (
            "raise"
            if await _raises_deadline_async(
                coco.mount, coco.component_subpath("mount_after_expiry"), mounted, "x"
            )
            else "no_raise"
        )
        vector["mount_each_entry"] = (
            "raise"
            if await _raises_deadline_async(
                coco.mount_each,
                coco.component_subpath("each_after_expiry"),
                mounted,
                [("item", "x")],
            )
            else "no_raise"
        )
        vector["mount_target_entry"] = (
            "raise"
            if await _raises_deadline_async(
                coco.mount_target, provider.target_state("container", "v")
            )
            else "no_raise"
        )
        vector["next_id"] = (
            "raise" if await _raises_deadline_async(_next_id) else "no_raise"
        )
        coco.declare_target_state(provider.target_state("k", "v"))

    app = coco.App(
        coco.AppConfig(name="deadline_boundary_matrix", environment=_env("matrix")),
        main,
    )
    with coco.timeout(timedelta(seconds=10)):
        with pytest.raises(coco.DeadlineExceededError):
            app.update_blocking()

    assert vector == {
        "mount_child": "no_raise",
        "mount_each_child": "no_raise",
        "batched_body": "no_raise",
        "check_deadline": "raise",
        "plain_coco_fn_call": "raise",
        "map_entry": "raise",
        "use_mount_entry": "raise",
        "mount_entry": "raise",
        "mount_each_entry": "raise",
        "mount_target_entry": "raise",
        "next_id": "raise",
    }


def _raises_deadline(fn: Any, *args: Any, **kwargs: Any) -> bool:
    try:
        fn(*args, **kwargs)
    except coco.DeadlineExceededError:
        return True
    return False


async def _raises_deadline_async(fn: Any, *args: Any, **kwargs: Any) -> bool:
    try:
        await fn(*args, **kwargs)
    except coco.DeadlineExceededError:
        return True
    return False

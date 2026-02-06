"""Tests for function batching and runner support."""

import asyncio
from typing import Any

import cocoindex as coco
from cocoindex._internal.runner import Runner
import pytest


# ============================================================================
# Test utilities for event-based synchronization
# ============================================================================


async def wait_for_condition(
    condition: Any, timeout: float = 2.0, interval: float = 0.01
) -> None:
    """Wait until condition() returns True, with timeout."""
    elapsed = 0.0
    while elapsed < timeout:
        if condition():
            return
        await asyncio.sleep(interval)
        elapsed += interval
    raise TimeoutError(f"Condition not met within {timeout}s")


# ============================================================================
# Basic batching tests
# ============================================================================


@coco.function(batching=True)
def _double_sync(inputs: list[int]) -> list[int]:
    """Sync batched function that doubles inputs.

    Note: With batching=True, this becomes an async function externally,
    even though the underlying implementation is sync.
    """
    return [x * 2 for x in inputs]


@pytest.mark.asyncio
async def test_batching_basic_sync() -> None:
    """Test basic sync batching - single call (now async externally)."""
    result = await _double_sync(5)
    assert result == 10


@coco.function(batching=True)
async def _double_async(inputs: list[int]) -> list[int]:
    """Async batched function that doubles inputs."""
    await asyncio.sleep(0.01)  # Simulate async work
    return [x * 2 for x in inputs]


@pytest.mark.asyncio
async def test_batching_basic_async() -> None:
    """Test basic async batching - single call."""
    result = await _double_async(5)
    assert result == 10


# ============================================================================
# Concurrent calls get batched together
# ============================================================================


class TrackedBatcher:
    """Helper class for tracking batch calls with event-based synchronization.

    Uses pre-created events keyed by input value, similar to the Rust test pattern
    where each input has its own oneshot receiver.
    """

    def __init__(self) -> None:
        self.batch_call_count = 0
        self.batch_inputs: list[list[int]] = []
        # Pre-created events keyed by input value
        self.input_events: dict[int, asyncio.Event] = {}

    def create_event(self, value: int) -> asyncio.Event:
        """Create an event for a specific input value."""
        event = asyncio.Event()
        self.input_events[value] = event
        return event

    def create_function(self) -> Any:
        """Create a tracked batched function."""
        tracker = self

        @coco.function(batching=True)
        async def tracked_double(inputs: list[int]) -> list[int]:
            """Async batched function that tracks calls and waits for signals."""
            tracker.batch_call_count += 1
            tracker.batch_inputs.append(sorted(inputs))
            # Wait for all input events before returning
            for v in inputs:
                await tracker.input_events[v].wait()
            return [x * 2 for x in inputs]

        return tracked_double


@pytest.mark.asyncio
async def test_batching_concurrent_calls() -> None:
    """Test that concurrent calls get batched together."""
    tracker = TrackedBatcher()
    tracked_double = tracker.create_function()

    # Pre-create events for each input
    for v in [1, 2, 3, 4, 5]:
        tracker.create_event(v)

    # Submit first call - it should execute inline
    task1 = asyncio.create_task(tracked_double(1))

    # Wait for first batch (inline call) to be recorded
    await wait_for_condition(lambda: len(tracker.batch_inputs) >= 1)

    # Now submit remaining calls - they should batch together
    # since the first call is still ongoing
    task2 = asyncio.create_task(tracked_double(2))
    task3 = asyncio.create_task(tracked_double(3))
    task4 = asyncio.create_task(tracked_double(4))
    task5 = asyncio.create_task(tracked_double(5))

    # Verify first batch is recorded, others are waiting
    assert tracker.batch_inputs == [[1]]

    # Unblock first call - this should trigger batch for 2-5
    tracker.input_events[1].set()

    # Wait for second batch to be recorded
    await wait_for_condition(lambda: len(tracker.batch_inputs) >= 2)

    # First call should be done
    result1 = await task1
    assert result1 == 2

    # Unblock remaining calls
    for v in [2, 3, 4, 5]:
        tracker.input_events[v].set()

    results = await asyncio.gather(task2, task3, task4, task5)

    # Results should be correct
    assert list(results) == [4, 6, 8, 10]


# ============================================================================
# max_batch_size is respected
# ============================================================================


class MaxBatchTracker:
    """Helper for testing max_batch_size with event-based synchronization."""

    def __init__(self, max_batch_size: int) -> None:
        self.max_batch_size = max_batch_size
        self.batch_sizes: list[int] = []

    def create_function(self) -> Any:
        """Create a batched function with max_batch_size."""
        tracker = self

        @coco.function(batching=True, max_batch_size=tracker.max_batch_size)
        async def limited_double(inputs: list[int]) -> list[int]:
            """Batched function that tracks sizes and waits for signal."""
            tracker.batch_sizes.append(len(inputs))
            return [x * 2 for x in inputs]

        return limited_double


@pytest.mark.asyncio
async def test_batching_max_batch_size() -> None:
    """Test that max_batch_size is respected."""
    tracker = MaxBatchTracker(max_batch_size=2)
    limited_double = tracker.create_function()

    # Submit 5 items concurrently
    task1 = asyncio.create_task(limited_double(1))
    task2 = asyncio.create_task(limited_double(2))
    task3 = asyncio.create_task(limited_double(3))
    task4 = asyncio.create_task(limited_double(4))
    task5 = asyncio.create_task(limited_double(5))

    results = await asyncio.gather(task1, task2, task3, task4, task5)

    # Results should be correct
    assert sorted(results) == [2, 4, 6, 8, 10]

    # All batch sizes should be <= 2
    for size in tracker.batch_sizes:
        assert size <= 2, f"Batch size {size} exceeds max_batch_size=2"


# ============================================================================
# Method batching (with self)
# ============================================================================


class BatchedProcessor:
    """Class with batched method using event-based synchronization.

    Uses pre-created events keyed by input value.
    """

    def __init__(self, multiplier: int):
        self.multiplier = multiplier
        self.call_count = 0
        self.batch_inputs: list[list[int]] = []
        self.input_events: dict[int, asyncio.Event] = {}

    def create_event(self, value: int) -> asyncio.Event:
        """Create an event for a specific input value."""
        event = asyncio.Event()
        self.input_events[value] = event
        return event

    @coco.function(batching=True)
    async def multiply(self, inputs: list[int]) -> list[int]:
        """Batched method that multiplies inputs, waits for signals."""
        self.call_count += 1
        self.batch_inputs.append(sorted(inputs))
        # Wait for all input events
        for v in inputs:
            await self.input_events[v].wait()
        return [x * self.multiplier for x in inputs]


@pytest.mark.asyncio
async def test_batching_method() -> None:
    """Test batching with methods."""
    proc = BatchedProcessor(3)
    proc.create_event(5)

    # Create task and wait for batch to be recorded
    task = asyncio.create_task(proc.multiply(5))
    await wait_for_condition(lambda: len(proc.batch_inputs) >= 1)

    # Signal completion
    proc.input_events[5].set()

    result = await task
    assert result == 15


@pytest.mark.asyncio
async def test_batching_method_concurrent() -> None:
    """Test concurrent calls to batched method."""
    proc = BatchedProcessor(3)

    # Pre-create events
    for v in [1, 2, 3]:
        proc.create_event(v)

    # Submit first call - it should execute inline
    task1 = asyncio.create_task(proc.multiply(1))  # type: ignore[arg-type]

    # Wait for first batch to be recorded
    await wait_for_condition(lambda: len(proc.batch_inputs) >= 1)

    # Submit remaining calls - they should batch together
    task2 = asyncio.create_task(proc.multiply(2))  # type: ignore[arg-type]
    task3 = asyncio.create_task(proc.multiply(3))  # type: ignore[arg-type]

    # Unblock first call - triggers batch for 2,3
    proc.input_events[1].set()

    # Wait for second batch
    await wait_for_condition(lambda: len(proc.batch_inputs) >= 2)

    result1 = await task1
    assert result1 == 3

    # Unblock remaining calls
    proc.input_events[2].set()
    proc.input_events[3].set()

    results = await asyncio.gather(task2, task3)

    assert sorted(results) == [6, 9]


# ============================================================================
# Out of component context
# ============================================================================


@pytest.mark.asyncio
async def test_batching_out_of_component() -> None:
    """Test that batched functions work outside of CocoIndex app."""
    # This should work without any component context

    @coco.function(batching=True)
    def standalone_double(inputs: list[int]) -> list[int]:
        return [x * 2 for x in inputs]

    result = await standalone_double(42)  # type: ignore[misc]
    assert result == 84


# ============================================================================
# Async batching tests
# ============================================================================


_async_batch_count = 0


@coco.function(batching=True)
async def _async_tracked_double(inputs: list[int]) -> list[int]:
    """Async batched function that tracks calls."""
    global _async_batch_count
    _async_batch_count += 1
    await asyncio.sleep(0.05)
    return [x * 2 for x in inputs]


@pytest.mark.asyncio
async def test_batching_async_concurrent() -> None:
    """Test concurrent async calls get batched."""
    global _async_batch_count
    _async_batch_count = 0

    # Submit multiple async calls concurrently
    results = await asyncio.gather(
        _async_tracked_double(1),
        _async_tracked_double(2),
        _async_tracked_double(3),
    )

    assert sorted(results) == [2, 4, 6]


# ============================================================================
# Runner tests (without subprocess to avoid test complexity)
# ============================================================================


class MockRunner(Runner):
    """Mock runner for testing.

    Extends the Runner base class with tracking for calls.
    """

    def __init__(self) -> None:
        super().__init__()
        self.call_count = 0
        self.last_args: tuple[Any, ...] = ()

    async def run(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        """Execute an async function."""
        self.call_count += 1
        self.last_args = args
        return await fn(*args, **kwargs)

    async def run_sync_fn(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        """Execute a sync function (async wrapper)."""
        self.call_count += 1
        self.last_args = args
        # Wrap sync function execution in to_thread to simulate async behavior
        return await asyncio.to_thread(fn, *args, **kwargs)


@pytest.mark.asyncio
async def test_runner_basic() -> None:
    """Test basic runner functionality."""
    runner = MockRunner()

    @coco.function(runner=runner)
    def add_one(x: int) -> int:
        return x + 1

    result = await add_one(5)  # type: ignore[misc]
    assert result == 6
    assert runner.call_count == 1


@pytest.mark.asyncio
async def test_runner_with_batching() -> None:
    """Test runner combined with batching."""
    runner = MockRunner()

    @coco.function(batching=True, runner=runner)
    def double_batch(inputs: list[int]) -> list[int]:
        return [x * 2 for x in inputs]

    result = await double_batch(5)  # type: ignore[misc]
    assert result == 10
    assert runner.call_count >= 1


# ============================================================================
# Queue sharing tests
# ============================================================================


@pytest.mark.asyncio
async def test_runner_queue_sharing() -> None:
    """Test that functions with the same runner share a queue."""
    runner = MockRunner()
    execution_order: list[str] = []
    fn_events: list[asyncio.Event] = []

    @coco.function(runner=runner)
    async def fn_a(x: int) -> int:
        execution_order.append("a")
        event = asyncio.Event()
        fn_events.append(event)
        await event.wait()
        return x + 1

    @coco.function(runner=runner)
    async def fn_b(x: int) -> int:
        execution_order.append("b")
        event = asyncio.Event()
        fn_events.append(event)
        await event.wait()
        return x + 2

    # Run both concurrently
    task1 = asyncio.create_task(fn_a(1))
    task2 = asyncio.create_task(fn_b(2))

    # Wait for events to be registered
    await wait_for_condition(lambda: len(fn_events) >= 2)

    # Signal completion
    for event in fn_events:
        event.set()

    r1, r2 = await asyncio.gather(task1, task2)

    assert r1 == 2
    assert r2 == 4

    # Both should have gone through the runner
    assert runner.call_count == 2


# ============================================================================
# Runner with multiple arguments tests
# ============================================================================


@pytest.mark.asyncio
async def test_runner_multiple_args() -> None:
    """Test runner with multiple positional arguments."""
    runner = MockRunner()

    @coco.function(runner=runner)
    def add(a: int, b: int, c: int) -> int:
        return a + b + c

    result = await add(1, 2, 3)  # type: ignore[misc]
    assert result == 6
    assert runner.call_count == 1


@pytest.mark.asyncio
async def test_runner_with_kwargs() -> None:
    """Test runner with keyword arguments."""
    runner = MockRunner()

    @coco.function(runner=runner)
    def greet(name: str, greeting: str = "Hello") -> str:
        return f"{greeting}, {name}!"

    result1 = await greet("Alice")  # type: ignore[misc]
    assert result1 == "Hello, Alice!"

    result2 = await greet("Bob", greeting="Hi")  # type: ignore[misc]
    assert result2 == "Hi, Bob!"

    assert runner.call_count == 2


@pytest.mark.asyncio
async def test_runner_mixed_args_kwargs() -> None:
    """Test runner with both positional and keyword arguments."""
    runner = MockRunner()

    @coco.function(runner=runner)
    def format_message(
        template: str, *values: int, prefix: str = "", suffix: str = ""
    ) -> str:
        formatted = template.format(*values)
        return f"{prefix}{formatted}{suffix}"

    result = await format_message("{} + {} = {}", 1, 2, 3, prefix="[", suffix="]")  # type: ignore[misc]
    assert result == "[1 + 2 = 3]"
    assert runner.call_count == 1


@pytest.mark.asyncio
async def test_runner_multiple_args_async() -> None:
    """Test async runner with multiple arguments."""
    runner = MockRunner()

    @coco.function(runner=runner)
    async def async_add(a: int, b: int, c: int) -> int:
        return a + b + c

    result = await async_add(10, 20, 30)
    assert result == 60
    assert runner.call_count == 1


@pytest.mark.asyncio
async def test_runner_with_kwargs_async() -> None:
    """Test async runner with keyword arguments."""
    runner = MockRunner()

    @coco.function(runner=runner)
    async def async_greet(name: str, greeting: str = "Hello") -> str:
        return f"{greeting}, {name}!"

    result = await async_greet("World", greeting="Hi")
    assert result == "Hi, World!"
    assert runner.call_count == 1


# ============================================================================
# Runner with methods (no batching) tests
# ============================================================================


class RunnerProcessor:
    """Class with methods that use runner (no batching)."""

    def __init__(self, multiplier: int):
        self.multiplier = multiplier

    @coco.function(runner=coco.GPU)
    def multiply_sync(self, x: int) -> int:
        """Sync method with runner."""
        return x * self.multiplier

    @coco.function(runner=coco.GPU)
    async def multiply_async(self, x: int) -> int:
        """Async method with runner."""
        return x * self.multiplier


@pytest.mark.asyncio
async def test_runner_method_sync() -> None:
    """Test runner with sync method (no batching)."""
    proc = RunnerProcessor(3)

    result = await proc.multiply_sync(5)  # type: ignore[misc]
    assert result == 15


@pytest.mark.asyncio
async def test_runner_method_async() -> None:
    """Test runner with async method (no batching)."""
    proc = RunnerProcessor(3)

    result = await proc.multiply_async(5)
    assert result == 15


@pytest.mark.asyncio
async def test_runner_method_concurrent() -> None:
    """Test concurrent calls to runner method (no batching)."""
    proc = RunnerProcessor(3)

    results = await asyncio.gather(
        proc.multiply_sync(1),  # type: ignore[arg-type]
        proc.multiply_sync(2),  # type: ignore[arg-type]
        proc.multiply_sync(3),  # type: ignore[arg-type]
    )

    assert sorted(results) == [3, 6, 9]


# ============================================================================
# Memo with batching/runner tests
# ============================================================================


@pytest.mark.asyncio
async def test_memo_with_batching() -> None:
    """Test that memo=True works with batching (no warning, memo is supported)."""

    # This should not raise any warnings - memo is now supported with batching
    @coco.function(batching=True, memo=True)
    def batched_with_memo(inputs: list[int]) -> list[int]:
        return [x * 2 for x in inputs]

    # Works outside of component context (memo just skipped)
    result = await batched_with_memo(5)  # type: ignore[misc]
    assert result == 10


@pytest.mark.asyncio
async def test_memo_with_runner() -> None:
    """Test that memo=True works with runner (no warning, memo is supported)."""
    runner = MockRunner()

    # This should not raise any warnings - memo is now supported with runner
    @coco.function(runner=runner, memo=True)
    def runner_with_memo(x: int) -> int:
        return x + 1

    # Works outside of component context (memo just skipped)
    result = await runner_with_memo(5)  # type: ignore[misc]
    assert result == 6
    assert runner.call_count == 1


# ============================================================================
# GPU Runner tests (subprocess with pickling)
#
# The @coco.function decorator with runner=coco.GPU works with normal syntax.
# Functions and methods are pickled using __reduce__ which stores (module, qualname)
# and reconstructs via __wrapped__ on unpickle.
# ============================================================================


@coco.function(runner=coco.GPU)
def _gpu_add_one(x: int) -> int:
    """GPU runner test function."""
    return x + 1


@pytest.mark.asyncio
async def test_gpu_runner_basic() -> None:
    """Test basic GPU runner functionality with subprocess."""
    result = await _gpu_add_one(5)  # type: ignore[misc]
    assert result == 6


@coco.function(batching=True, runner=coco.GPU)
def _gpu_double_batch(inputs: list[int]) -> list[int]:
    """GPU runner + batching test function."""
    return [x * 2 for x in inputs]


@pytest.mark.asyncio
async def test_gpu_runner_with_batching() -> None:
    """Test GPU runner combined with batching - subprocess must pickle the batch function."""
    result = await _gpu_double_batch(5)  # type: ignore[misc]
    assert result == 10


@coco.function(batching=True, max_batch_size=10, runner=coco.GPU)
def _gpu_double_batch_concurrent(inputs: list[int]) -> list[int]:
    """GPU runner + batching concurrent test function."""
    return [x * 2 for x in inputs]


@pytest.mark.asyncio
async def test_gpu_runner_with_batching_concurrent() -> None:
    """Test GPU runner + batching with concurrent calls."""
    results = await asyncio.gather(
        _gpu_double_batch_concurrent(1),  # type: ignore[arg-type]
        _gpu_double_batch_concurrent(2),  # type: ignore[arg-type]
        _gpu_double_batch_concurrent(3),  # type: ignore[arg-type]
    )

    assert sorted(results) == [2, 4, 6]


class GPUBatchedProcessor:
    """Class with batched method that runs on GPU.

    Both the class and its instances must be picklable for subprocess execution.
    Normal @decorator syntax works - pickling uses __reduce__ with (module, qualname).
    """

    def __init__(self, multiplier: int):
        self.multiplier = multiplier

    @coco.function(batching=True, runner=coco.GPU)
    def multiply(self, inputs: list[int]) -> list[int]:
        """Batched method that multiplies inputs, runs in subprocess."""
        return [x * self.multiplier for x in inputs]


@pytest.mark.asyncio
async def test_gpu_runner_with_batching_method() -> None:
    """Test GPU runner + batching with a method (self parameter)."""
    proc = GPUBatchedProcessor(3)

    result = await proc.multiply(5)  # type: ignore[misc]
    assert result == 15


@pytest.mark.asyncio
async def test_gpu_runner_with_batching_method_concurrent() -> None:
    """Test GPU runner + batching with method and concurrent calls."""
    proc = GPUBatchedProcessor(3)

    results = await asyncio.gather(
        proc.multiply(1),  # type: ignore[arg-type]
        proc.multiply(2),  # type: ignore[arg-type]
        proc.multiply(3),  # type: ignore[arg-type]
    )

    assert sorted(results) == [3, 6, 9]


# Note: With always-async design, functions with batching/runner are always async.
# The underlying implementation can be sync - it gets wrapped appropriately.
# Subprocess execution still works for sync underlying functions.

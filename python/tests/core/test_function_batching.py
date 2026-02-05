"""Tests for function batching and runner support."""

import asyncio
import time
from typing import Any

import cocoindex as coco
from cocoindex._internal.runner import Runner
import pytest


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


_batch_call_count = 0
_batch_sizes: list[int] = []


@coco.function(batching=True)
def _tracked_double(inputs: list[int]) -> list[int]:
    """Sync batched function that tracks call count and batch sizes."""
    global _batch_call_count
    _batch_call_count += 1
    _batch_sizes.append(len(inputs))
    # Small delay to allow batching
    time.sleep(0.05)
    return [x * 2 for x in inputs]


@pytest.mark.asyncio
async def test_batching_concurrent_calls() -> None:
    """Test that concurrent calls get batched together."""
    global _batch_call_count, _batch_sizes
    _batch_call_count = 0
    _batch_sizes = []

    # Submit multiple calls concurrently using asyncio.gather
    results = await asyncio.gather(
        _tracked_double(1),  # type: ignore[arg-type]
        _tracked_double(2),  # type: ignore[arg-type]
        _tracked_double(3),  # type: ignore[arg-type]
        _tracked_double(4),  # type: ignore[arg-type]
        _tracked_double(5),  # type: ignore[arg-type]
    )

    # Results should be correct (order preserved with gather)
    assert list(results) == [2, 4, 6, 8, 10]

    # Should have fewer than 5 calls due to batching
    # (exact number depends on timing, but should be batched)
    assert _batch_call_count <= 3, f"Expected batching, got {_batch_call_count} calls"


# ============================================================================
# max_batch_size is respected
# ============================================================================


_max_batch_sizes: list[int] = []


@coco.function(batching=True, max_batch_size=2)
def _limited_double(inputs: list[int]) -> list[int]:
    """Batched function with max_batch_size=2."""
    _max_batch_sizes.append(len(inputs))
    time.sleep(0.02)
    return [x * 2 for x in inputs]


@pytest.mark.asyncio
async def test_batching_max_batch_size() -> None:
    """Test that max_batch_size is respected."""
    global _max_batch_sizes
    _max_batch_sizes = []

    # Submit 5 items concurrently using asyncio.gather
    results = await asyncio.gather(
        _limited_double(1),  # type: ignore[arg-type]
        _limited_double(2),  # type: ignore[arg-type]
        _limited_double(3),  # type: ignore[arg-type]
        _limited_double(4),  # type: ignore[arg-type]
        _limited_double(5),  # type: ignore[arg-type]
    )

    # Results should be correct
    assert sorted(results) == [2, 4, 6, 8, 10]

    # All batch sizes should be <= 2
    for size in _max_batch_sizes:
        assert size <= 2, f"Batch size {size} exceeds max_batch_size=2"


# ============================================================================
# Method batching (with self)
# ============================================================================


class BatchedProcessor:
    """Class with batched method."""

    def __init__(self, multiplier: int):
        self.multiplier = multiplier
        self.call_count = 0

    @coco.function(batching=True)
    def multiply(self, inputs: list[int]) -> list[int]:
        """Batched method that multiplies inputs."""
        self.call_count += 1
        # Small delay to allow concurrent calls to batch together
        time.sleep(0.02)
        return [x * self.multiplier for x in inputs]


@pytest.mark.asyncio
async def test_batching_method() -> None:
    """Test batching with methods."""
    proc = BatchedProcessor(3)

    result = await proc.multiply(5)  # type: ignore[misc]
    assert result == 15


@pytest.mark.asyncio
async def test_batching_method_concurrent() -> None:
    """Test concurrent calls to batched method."""
    proc = BatchedProcessor(3)

    results = await asyncio.gather(
        proc.multiply(1),  # type: ignore[arg-type]
        proc.multiply(2),  # type: ignore[arg-type]
        proc.multiply(3),  # type: ignore[arg-type]
    )

    assert sorted(results) == [3, 6, 9]
    # Should have fewer calls due to batching
    assert proc.call_count <= 2


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
    # Should have fewer calls due to batching
    assert _async_batch_count <= 2, f"Expected batching, got {_async_batch_count} calls"


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

    @coco.function(runner=runner)
    def fn_a(x: int) -> int:
        execution_order.append("a")
        time.sleep(0.02)
        return x + 1

    @coco.function(runner=runner)
    def fn_b(x: int) -> int:
        execution_order.append("b")
        time.sleep(0.02)
        return x + 2

    # Run both concurrently using asyncio.gather
    r1, r2 = await asyncio.gather(
        fn_a(1),  # type: ignore[arg-type]
        fn_b(2),  # type: ignore[arg-type]
    )

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

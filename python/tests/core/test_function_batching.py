"""Tests for function batching and runner support."""

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import cocoindex as coco
from cocoindex._internal.runner import Runner
import pytest


# ============================================================================
# Basic batching tests
# ============================================================================


@coco.function(batching=True)
def _double_sync(inputs: list[int]) -> list[int]:
    """Sync batched function that doubles inputs."""
    return [x * 2 for x in inputs]


def test_batching_basic_sync() -> None:
    """Test basic sync batching - single call."""
    result = _double_sync(5)
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


def test_batching_concurrent_calls() -> None:
    """Test that concurrent calls get batched together."""
    global _batch_call_count, _batch_sizes
    _batch_call_count = 0
    _batch_sizes = []

    # Submit multiple calls concurrently
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = [pool.submit(_tracked_double, i) for i in range(1, 6)]
        results = [f.result() for f in futures]

    # Results should be correct (order may vary)
    assert sorted(results) == [2, 4, 6, 8, 10]

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


def test_batching_max_batch_size() -> None:
    """Test that max_batch_size is respected."""
    global _max_batch_sizes
    _max_batch_sizes = []

    # Submit 5 items concurrently
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = [pool.submit(_limited_double, i) for i in range(1, 6)]
        results = [f.result() for f in futures]

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
        return [x * self.multiplier for x in inputs]


def test_batching_method() -> None:
    """Test batching with methods."""
    proc = BatchedProcessor(3)

    result = proc.multiply(5)
    assert result == 15


def test_batching_method_concurrent() -> None:
    """Test concurrent calls to batched method."""
    proc = BatchedProcessor(3)

    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = [pool.submit(proc.multiply, i) for i in [1, 2, 3]]
        results = [f.result() for f in futures]

    assert sorted(results) == [3, 6, 9]
    # Should have fewer calls due to batching
    assert proc.call_count <= 2


# ============================================================================
# Out of component context
# ============================================================================


def test_batching_out_of_component() -> None:
    """Test that batched functions work outside of CocoIndex app."""
    # This should work without any component context

    @coco.function(batching=True)
    def standalone_double(inputs: list[int]) -> list[int]:
        return [x * 2 for x in inputs]

    result = standalone_double(42)
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

    def run_sync_fn(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        """Execute a sync function."""
        self.call_count += 1
        self.last_args = args
        return fn(*args, **kwargs)


def test_runner_basic() -> None:
    """Test basic runner functionality."""
    runner = MockRunner()

    @coco.function(runner=runner)
    def add_one(x: int) -> int:
        return x + 1

    result = add_one(5)
    assert result == 6
    assert runner.call_count == 1


def test_runner_with_batching() -> None:
    """Test runner combined with batching."""
    runner = MockRunner()

    @coco.function(batching=True, runner=runner)
    def double_batch(inputs: list[int]) -> list[int]:
        return [x * 2 for x in inputs]

    result = double_batch(5)
    assert result == 10
    assert runner.call_count >= 1


# ============================================================================
# Queue sharing tests
# ============================================================================


def test_runner_queue_sharing() -> None:
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

    # Run both concurrently - they should share a queue
    with ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(fn_a, 1)
        f2 = pool.submit(fn_b, 2)

        r1 = f1.result()
        r2 = f2.result()

    assert r1 == 2
    assert r2 == 4

    # Both should have gone through the runner
    assert runner.call_count == 2


# ============================================================================
# Runner with multiple arguments tests
# ============================================================================


def test_runner_multiple_args() -> None:
    """Test runner with multiple positional arguments."""
    runner = MockRunner()

    @coco.function(runner=runner)
    def add(a: int, b: int, c: int) -> int:
        return a + b + c

    result = add(1, 2, 3)
    assert result == 6
    assert runner.call_count == 1


def test_runner_with_kwargs() -> None:
    """Test runner with keyword arguments."""
    runner = MockRunner()

    @coco.function(runner=runner)
    def greet(name: str, greeting: str = "Hello") -> str:
        return f"{greeting}, {name}!"

    result1 = greet("Alice")
    assert result1 == "Hello, Alice!"

    result2 = greet("Bob", greeting="Hi")
    assert result2 == "Hi, Bob!"

    assert runner.call_count == 2


def test_runner_mixed_args_kwargs() -> None:
    """Test runner with both positional and keyword arguments."""
    runner = MockRunner()

    @coco.function(runner=runner)
    def format_message(
        template: str, *values: int, prefix: str = "", suffix: str = ""
    ) -> str:
        formatted = template.format(*values)
        return f"{prefix}{formatted}{suffix}"

    result = format_message("{} + {} = {}", 1, 2, 3, prefix="[", suffix="]")
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
# Memo with batching/runner tests
# ============================================================================


def test_memo_with_batching() -> None:
    """Test that memo=True works with batching (no warning, memo is supported)."""

    # This should not raise any warnings - memo is now supported with batching
    @coco.function(batching=True, memo=True)
    def batched_with_memo(inputs: list[int]) -> list[int]:
        return [x * 2 for x in inputs]

    # Works outside of component context (memo just skipped)
    result = batched_with_memo(5)
    assert result == 10


def test_memo_with_runner() -> None:
    """Test that memo=True works with runner (no warning, memo is supported)."""
    runner = MockRunner()

    # This should not raise any warnings - memo is now supported with runner
    @coco.function(runner=runner, memo=True)
    def runner_with_memo(x: int) -> int:
        return x + 1

    # Works outside of component context (memo just skipped)
    result = runner_with_memo(5)
    assert result == 6
    assert runner.call_count == 1

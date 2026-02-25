"""
Test that LazyEnvironment's lazy lock creation works correctly with pytest-asyncio.

This test verifies that CocoIndex's environment setup works correctly when
pytest-asyncio creates fresh event loops for each test. This ensures the
asyncio.Lock used internally is created lazily (when first needed from within
an async context) rather than at initialization time.

The tests use only public APIs. If these tests pass without "Event loop is
closed" errors, it confirms the lazy lock pattern is working correctly.

See BUG_REPORT/COCOINDEX_FIX_README.md for detailed background on the bug fix.
"""

import pathlib
import sys
import tempfile

import pytest

import cocoindex as coco


@pytest.mark.asyncio
async def test_async_app_with_fresh_event_loop_first() -> None:
    """
    First test using async App with pytest-asyncio.

    This test and the following ones verify that the internal LazyEnvironment
    works correctly when pytest-asyncio creates fresh event loops for each test.
    The bug this prevents: "Event loop is closed" errors caused by creating
    asyncio.Lock() at module initialization time.
    """
    # On Windows, LMDB keeps files open, so we need to ignore cleanup errors
    ignore_cleanup = sys.platform == "win32"

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=ignore_cleanup) as tmpdir:
        db_path = pathlib.Path(tmpdir) / "test.db"

        env = coco.Environment(coco.Settings(db_path=db_path), name="test_env_1")

        @coco.function
        async def main() -> str:
            return "test_success_1"

        app = coco.App(
            coco.AppConfig(name="test1", environment=env),
            main,
        )

        result = await app.update()
        assert result == "test_success_1"


@pytest.mark.asyncio
async def test_async_app_with_fresh_event_loop_second() -> None:
    """
    Second test with a new event loop.

    With the lazy lock fix, this should succeed even though pytest-asyncio
    may create a new event loop. Without the fix, this could fail with
    "Event loop is closed" if the lock was bound to a previous loop.
    """
    ignore_cleanup = sys.platform == "win32"

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=ignore_cleanup) as tmpdir:
        db_path = pathlib.Path(tmpdir) / "test.db"

        env = coco.Environment(coco.Settings(db_path=db_path), name="test_env_2")

        @coco.function
        async def main() -> str:
            return "test_success_2"

        app = coco.App(
            coco.AppConfig(name="test2", environment=env),
            main,
        )

        result = await app.update()
        assert result == "test_success_2"


@pytest.mark.asyncio
async def test_async_app_with_fresh_event_loop_third() -> None:
    """
    Third test to further verify stability across multiple event loops.
    """
    ignore_cleanup = sys.platform == "win32"

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=ignore_cleanup) as tmpdir:
        db_path = pathlib.Path(tmpdir) / "test.db"

        env = coco.Environment(coco.Settings(db_path=db_path), name="test_env_3")

        @coco.function
        async def main() -> str:
            return "test_success_3"

        app = coco.App(
            coco.AppConfig(name="test3", environment=env),
            main,
        )

        result = await app.update()
        assert result == "test_success_3"


@pytest.mark.asyncio
async def test_multiple_sequential_app_updates() -> None:
    """
    Test multiple app updates in sequence within same async context.

    This verifies that the lazy lock can be reused correctly within the
    same event loop for multiple operations.
    """
    ignore_cleanup = sys.platform == "win32"

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=ignore_cleanup) as tmpdir:
        db_path = pathlib.Path(tmpdir) / "test.db"

        env = coco.Environment(coco.Settings(db_path=db_path), name="seq_env")

        results = []

        for i in range(3):

            @coco.function
            async def main(iteration: int = i) -> str:
                return f"iteration_{iteration}"

            app = coco.App(
                coco.AppConfig(name=f"seq_test_{i}", environment=env),
                main,
            )
            result = await app.update()
            results.append(result)

        assert results == ["iteration_0", "iteration_1", "iteration_2"]

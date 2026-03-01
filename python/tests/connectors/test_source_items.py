"""Tests for source connector items() methods."""

from __future__ import annotations

from pathlib import Path

import pytest

from cocoindex.connectors import localfs


@pytest.mark.asyncio
class TestDirWalkerItems:
    """Tests for DirWalker.items() keyed iteration."""

    async def test_items_flat_directory(self, tmp_path: Path) -> None:
        """items() yields (relative_path, File) pairs."""
        (tmp_path / "a.txt").write_text("hello")
        (tmp_path / "b.txt").write_text("world")

        base = localfs.register_base_dir("test_flat", tmp_path)
        try:
            walker = localfs.walk_dir(base)
            items: list[tuple[str, localfs.File]] = []
            async for item in walker.items():
                items.append(item)
            items.sort(key=lambda x: x[0])

            assert len(items) == 2
            assert items[0][0] == "a.txt"
            assert items[1][0] == "b.txt"
            assert isinstance(items[0][1], localfs.File)
            assert isinstance(items[1][1], localfs.File)
            assert await items[0][1].read_text() == "hello"
            assert await items[1][1].read_text() == "world"
        finally:
            localfs.unregister_base_dir("test_flat")

    async def test_items_recursive(self, tmp_path: Path) -> None:
        """items() with recursive walk includes subdirectory paths as keys."""
        sub = tmp_path / "sub"
        sub.mkdir()
        (tmp_path / "root.txt").write_text("root")
        (sub / "nested.txt").write_text("nested")

        base = localfs.register_base_dir("test_recursive", tmp_path)
        try:
            walker = localfs.walk_dir(base, recursive=True)
            items: list[tuple[str, localfs.File]] = []
            async for item in walker.items():
                items.append(item)
            items.sort(key=lambda x: x[0])

            assert len(items) == 2
            assert items[0][0] == "root.txt"
            assert items[1][0] == "sub/nested.txt"
        finally:
            localfs.unregister_base_dir("test_recursive")

    async def test_items_empty_directory(self, tmp_path: Path) -> None:
        """items() on empty directory yields nothing."""
        walker = localfs.walk_dir(tmp_path)
        items: list[tuple[str, localfs.File]] = []
        async for item in walker.items():
            items.append(item)
        assert items == []

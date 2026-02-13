"""Tests for source connector items() methods."""

from __future__ import annotations

from pathlib import Path

from cocoindex.connectors import localfs


class TestDirWalkerItems:
    """Tests for DirWalker.items() keyed iteration."""

    def test_items_flat_directory(self, tmp_path: Path) -> None:
        """items() yields (relative_path, File) pairs."""
        (tmp_path / "a.txt").write_text("hello")
        (tmp_path / "b.txt").write_text("world")

        base = localfs.register_base_dir("test_flat", tmp_path)
        try:
            walker = localfs.walk_dir(base)
            items = sorted(walker.items(), key=lambda x: str(x[0]))

            assert len(items) == 2
            assert items[0][0] == "a.txt"
            assert items[1][0] == "b.txt"
            assert isinstance(items[0][1], localfs.File)
            assert isinstance(items[1][1], localfs.File)
            assert items[0][1].read_text() == "hello"
            assert items[1][1].read_text() == "world"
        finally:
            localfs.unregister_base_dir("test_flat")

    def test_items_recursive(self, tmp_path: Path) -> None:
        """items() with recursive walk includes subdirectory paths as keys."""
        sub = tmp_path / "sub"
        sub.mkdir()
        (tmp_path / "root.txt").write_text("root")
        (sub / "nested.txt").write_text("nested")

        base = localfs.register_base_dir("test_recursive", tmp_path)
        try:
            walker = localfs.walk_dir(base, recursive=True)
            items = sorted(walker.items(), key=lambda x: str(x[0]))

            assert len(items) == 2
            assert items[0][0] == "root.txt"
            assert items[1][0] == "sub/nested.txt"
        finally:
            localfs.unregister_base_dir("test_recursive")

    def test_items_empty_directory(self, tmp_path: Path) -> None:
        """items() on empty directory yields nothing."""
        walker = localfs.walk_dir(tmp_path)
        items = list(walker.items())
        assert items == []

    def test_items_key_matches_stable_key(self, tmp_path: Path) -> None:
        """The key from items() matches the file's stable_key property."""
        (tmp_path / "test.txt").write_text("data")

        base = localfs.register_base_dir("test_stable_key", tmp_path)
        try:
            walker = localfs.walk_dir(base)
            items = list(walker.items())

            assert len(items) == 1
            key, file = items[0]
            assert key == file.stable_key
            assert key == "test.txt"
        finally:
            localfs.unregister_base_dir("test_stable_key")

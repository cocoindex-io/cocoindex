"""Unit tests for localfs source helper logic.

Tests cover:
- _stat_to_metadata: conversion of os.stat_result to FileMetadata
- PatternFilePathMatcher / MatchAllFilePathMatcher: file path matching
- to_file_path: path conversion helper
- DirWalker._walk_sync: directory walking with filtering
"""

from __future__ import annotations

import os
import pathlib
from pathlib import Path, PurePath
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from cocoindex.resources.file import (
    FileMetadata,
    MatchAllFilePathMatcher,
    PatternFilePathMatcher,
)
from cocoindex.connectors.localfs._common import FilePath, to_file_path
from cocoindex.connectors.localfs._source import _stat_to_metadata, DirWalker
from cocoindex._internal.context_keys import ContextKey


# =============================================================================
# _stat_to_metadata
# =============================================================================


class TestStatToMetadata:
    def _make_stat(self, size: int, mtime_ns: int) -> os.stat_result:
        stat = MagicMock(spec=os.stat_result)
        stat.st_size = size
        stat.st_mtime_ns = mtime_ns
        return stat

    def test_size_is_preserved(self) -> None:
        stat = self._make_stat(size=1234, mtime_ns=1_000_000_000_000_000)
        meta = _stat_to_metadata(stat)
        assert meta.size == 1234

    def test_returns_file_metadata(self) -> None:
        stat = self._make_stat(size=0, mtime_ns=1_000_000_000_000_000)
        meta = _stat_to_metadata(stat)
        assert isinstance(meta, FileMetadata)

    def test_mtime_microsecond_precision(self) -> None:
        # _stat_to_metadata divides mtime_ns by 1_000 to get microseconds, then
        # splits into seconds and remainder microseconds.
        # 1_000_000_500_000 us = 1_000_000 seconds + 500_000 microseconds
        stat = self._make_stat(size=0, mtime_ns=1_000_000_500_000_000)
        meta = _stat_to_metadata(stat)
        assert isinstance(meta.modified_time, datetime)
        assert meta.modified_time.microsecond == 500_000

    def test_mtime_zero(self) -> None:
        stat = self._make_stat(size=0, mtime_ns=0)
        meta = _stat_to_metadata(stat)
        assert meta.size == 0
        assert isinstance(meta.modified_time, datetime)

    def test_real_stat(self, tmp_path: Path) -> None:
        f = tmp_path / "sample.txt"
        f.write_bytes(b"hello world")
        stat = f.stat()
        meta = _stat_to_metadata(stat)
        assert meta.size == 11
        assert isinstance(meta.modified_time, datetime)


# =============================================================================
# MatchAllFilePathMatcher
# =============================================================================


class TestMatchAllFilePathMatcher:
    def setup_method(self) -> None:
        self.matcher = MatchAllFilePathMatcher()

    def test_file_always_included(self) -> None:
        assert self.matcher.is_file_included(PurePath("anything.txt"))
        assert self.matcher.is_file_included(PurePath("deep/nested/file.py"))

    def test_dir_always_included(self) -> None:
        assert self.matcher.is_dir_included(PurePath("somedir"))
        assert self.matcher.is_dir_included(PurePath("a/b/c"))


# =============================================================================
# PatternFilePathMatcher
# =============================================================================


class TestPatternFilePathMatcher:
    def test_include_pattern_matches(self) -> None:
        matcher = PatternFilePathMatcher(included_patterns=["**/*.py"])
        assert matcher.is_file_included(PurePath("foo/bar.py"))
        assert matcher.is_file_included(PurePath("bar.py"))

    def test_include_pattern_excludes_non_matching(self) -> None:
        matcher = PatternFilePathMatcher(included_patterns=["**/*.py"])
        assert not matcher.is_file_included(PurePath("foo/bar.txt"))

    def test_exclude_pattern_excludes_files(self) -> None:
        matcher = PatternFilePathMatcher(excluded_patterns=["**/*.log"])
        assert not matcher.is_file_included(PurePath("app.log"))
        assert not matcher.is_file_included(PurePath("logs/app.log"))

    def test_exclude_pattern_does_not_affect_other_files(self) -> None:
        matcher = PatternFilePathMatcher(excluded_patterns=["**/*.log"])
        assert matcher.is_file_included(PurePath("main.py"))

    def test_include_and_exclude_combined(self) -> None:
        matcher = PatternFilePathMatcher(
            included_patterns=["**/*.py"],
            excluded_patterns=["**/test_*.py"],
        )
        assert matcher.is_file_included(PurePath("main.py"))
        assert not matcher.is_file_included(PurePath("test_main.py"))
        assert not matcher.is_file_included(PurePath("tests/test_foo.py"))

    def test_dir_included_when_not_excluded(self) -> None:
        matcher = PatternFilePathMatcher(excluded_patterns=["**/node_modules/**"])
        assert matcher.is_dir_included(PurePath("src"))
        assert matcher.is_dir_included(PurePath("lib"))

    def test_dir_excluded_by_pattern(self) -> None:
        matcher = PatternFilePathMatcher(excluded_patterns=["**/.*"])
        assert not matcher.is_dir_included(PurePath(".git"))

    def test_no_patterns_includes_all(self) -> None:
        matcher = PatternFilePathMatcher()
        assert matcher.is_file_included(PurePath("any/file.xyz"))
        assert matcher.is_dir_included(PurePath("any/dir"))

    def test_root_only_pattern(self) -> None:
        matcher = PatternFilePathMatcher(included_patterns=["*.txt"])
        assert matcher.is_file_included(PurePath("readme.txt"))
        # A non-.txt file at root should not be included
        assert not matcher.is_file_included(PurePath("main.py"))

    def test_multiple_included_patterns(self) -> None:
        matcher = PatternFilePathMatcher(included_patterns=["**/*.py", "**/*.txt"])
        assert matcher.is_file_included(PurePath("main.py"))
        assert matcher.is_file_included(PurePath("docs/readme.txt"))
        assert not matcher.is_file_included(PurePath("data.csv"))

    def test_negation_in_excluded_patterns(self) -> None:
        # Without negation: both .log files are excluded.
        matcher_no_negation = PatternFilePathMatcher(excluded_patterns=["**/*.log"])
        assert not matcher_no_negation.is_file_included(PurePath("app.log"))
        assert not matcher_no_negation.is_file_included(PurePath("keep.log"))

        # Standard exclusion of a non-matching file is unaffected.
        assert matcher_no_negation.is_file_included(PurePath("main.py"))


# =============================================================================
# to_file_path
# =============================================================================


class TestToFilePath:
    def test_from_pathlib_path(self, tmp_path: Path) -> None:
        result = to_file_path(tmp_path)
        assert isinstance(result, FilePath)
        assert result.base_dir is None

    def test_from_file_path_returns_same(self) -> None:
        fp = FilePath("some/path.txt")
        result = to_file_path(fp)
        assert result is fp

    def test_from_context_key_sets_base_dir(self) -> None:
        key: ContextKey[pathlib.Path] = ContextKey("test_to_file_path_key_unique_99")
        result = to_file_path(key)
        assert isinstance(result, FilePath)
        assert result.base_dir is key
        assert result.path == PurePath(".")

    def test_from_string_path(self) -> None:
        result = to_file_path(pathlib.Path("some/path"))
        assert isinstance(result, FilePath)
        assert result.base_dir is None


# =============================================================================
# DirWalker._walk_sync
# =============================================================================


class TestDirWalkerWalkSync:
    def test_walks_flat_directory(self, tmp_path: Path) -> None:
        (tmp_path / "a.txt").write_bytes(b"A")
        (tmp_path / "b.txt").write_bytes(b"B")

        walker = DirWalker(tmp_path)
        files = list(walker._walk_sync())
        names = {f.file_path.name for f in files}
        assert names == {"a.txt", "b.txt"}

    def test_non_recursive_does_not_enter_subdirs(self, tmp_path: Path) -> None:
        (tmp_path / "top.txt").write_bytes(b"top")
        subdir = tmp_path / "sub"
        subdir.mkdir()
        (subdir / "nested.txt").write_bytes(b"nested")

        walker = DirWalker(tmp_path, recursive=False)
        files = list(walker._walk_sync())
        names = {f.file_path.name for f in files}
        assert "top.txt" in names
        assert "nested.txt" not in names

    def test_recursive_enters_subdirs(self, tmp_path: Path) -> None:
        (tmp_path / "top.txt").write_bytes(b"top")
        subdir = tmp_path / "sub"
        subdir.mkdir()
        (subdir / "nested.txt").write_bytes(b"nested")

        walker = DirWalker(tmp_path, recursive=True)
        files = list(walker._walk_sync())
        names = {f.file_path.name for f in files}
        assert "top.txt" in names
        assert "nested.txt" in names

    def test_empty_directory_yields_nothing(self, tmp_path: Path) -> None:
        walker = DirWalker(tmp_path)
        files = list(walker._walk_sync())
        assert files == []

    def test_raises_on_non_directory(self, tmp_path: Path) -> None:
        f = tmp_path / "notadir.txt"
        f.write_bytes(b"x")
        walker = DirWalker(f)
        with pytest.raises(ValueError, match="not a directory"):
            list(walker._walk_sync())

    def test_path_matcher_filters_files(self, tmp_path: Path) -> None:
        (tmp_path / "keep.py").write_bytes(b"x")
        (tmp_path / "skip.txt").write_bytes(b"y")

        matcher = PatternFilePathMatcher(included_patterns=["**/*.py"])
        walker = DirWalker(tmp_path, path_matcher=matcher)
        files = list(walker._walk_sync())
        names = {f.file_path.name for f in files}
        assert "keep.py" in names
        assert "skip.txt" not in names

    def test_path_matcher_filters_dirs_recursively(self, tmp_path: Path) -> None:
        (tmp_path / "root.py").write_bytes(b"r")
        included = tmp_path / "src"
        included.mkdir()
        (included / "code.py").write_bytes(b"c")
        excluded = tmp_path / "node_modules"
        excluded.mkdir()
        (excluded / "lib.js").write_bytes(b"j")

        matcher = PatternFilePathMatcher(excluded_patterns=["**/node_modules/**"])
        walker = DirWalker(tmp_path, recursive=True, path_matcher=matcher)
        files = list(walker._walk_sync())
        names = {f.file_path.name for f in files}
        assert "root.py" in names
        assert "code.py" in names
        assert "lib.js" not in names

    def test_file_objects_have_metadata(self, tmp_path: Path) -> None:
        f = tmp_path / "sized.bin"
        f.write_bytes(b"1234567890")

        walker = DirWalker(tmp_path)
        files = list(walker._walk_sync())
        assert len(files) == 1
        assert files[0]._metadata is not None
        assert files[0]._metadata.size == 10

    def test_default_uses_match_all(self, tmp_path: Path) -> None:
        (tmp_path / "a.xyz").write_bytes(b"a")
        walker = DirWalker(tmp_path)
        assert isinstance(walker._path_matcher, MatchAllFilePathMatcher)

    def test_subdir_not_yielded_as_file(self, tmp_path: Path) -> None:
        (tmp_path / "subdir").mkdir()
        walker = DirWalker(tmp_path)
        files = list(walker._walk_sync())
        assert files == []

"""Tests for PatternFilePathMatcher with globset-backed pattern matching."""

from pathlib import PurePath

import pytest

from cocoindex.resources.file import PatternFilePathMatcher


class TestPatternFilePathMatcherNoPatterns:
    def test_all_files_included(self) -> None:
        matcher = PatternFilePathMatcher()
        assert matcher.is_file_included(PurePath("anything.txt"))
        assert matcher.is_file_included(PurePath("nested/path/file.py"))

    def test_all_dirs_included(self) -> None:
        matcher = PatternFilePathMatcher()
        assert matcher.is_dir_included(PurePath("anydir"))
        assert matcher.is_dir_included(PurePath(".hidden"))


class TestPatternFilePathMatcherInclude:
    def test_basename_pattern(self) -> None:
        """*.py (no path separator) matches basename at any depth in globset."""
        matcher = PatternFilePathMatcher(included_patterns=["*.py"])
        assert matcher.is_file_included(PurePath("main.py"))
        assert matcher.is_file_included(PurePath("src/main.py"))
        assert matcher.is_file_included(PurePath("a/b/main.py"))
        assert not matcher.is_file_included(PurePath("main.rs"))

    def test_path_scoped_pattern(self) -> None:
        """Patterns with / match against the full path, not just basename."""
        matcher = PatternFilePathMatcher(included_patterns=["src/*.py"])
        assert matcher.is_file_included(PurePath("src/main.py"))
        assert not matcher.is_file_included(PurePath("main.py"))
        assert not matcher.is_file_included(PurePath("lib/main.py"))

    def test_recursive_pattern(self) -> None:
        """**/*.py matches at any depth."""
        matcher = PatternFilePathMatcher(included_patterns=["**/*.py"])
        assert matcher.is_file_included(PurePath("main.py"))
        assert matcher.is_file_included(PurePath("src/main.py"))
        assert matcher.is_file_included(PurePath("a/b/c/main.py"))
        assert not matcher.is_file_included(PurePath("main.rs"))

    def test_multiple_patterns(self) -> None:
        matcher = PatternFilePathMatcher(included_patterns=["**/*.py", "**/*.md"])
        assert matcher.is_file_included(PurePath("main.py"))
        assert matcher.is_file_included(PurePath("docs/readme.md"))
        assert not matcher.is_file_included(PurePath("image.png"))

    def test_include_does_not_affect_dirs(self) -> None:
        """Include patterns only affect files, not directory traversal."""
        matcher = PatternFilePathMatcher(included_patterns=["**/*.py"])
        assert matcher.is_dir_included(PurePath("src"))
        assert matcher.is_dir_included(PurePath("docs"))


class TestPatternFilePathMatcherExclude:
    def test_exclude_files(self) -> None:
        matcher = PatternFilePathMatcher(excluded_patterns=["**/*.tmp"])
        assert matcher.is_file_included(PurePath("main.py"))
        assert not matcher.is_file_included(PurePath("temp.tmp"))
        assert not matcher.is_file_included(PurePath("sub/temp.tmp"))

    def test_exclude_dirs(self) -> None:
        matcher = PatternFilePathMatcher(excluded_patterns=["**/.*"])
        assert not matcher.is_dir_included(PurePath(".git"))
        assert not matcher.is_dir_included(PurePath("src/.hidden"))
        assert matcher.is_dir_included(PurePath("src"))
        assert matcher.is_dir_included(PurePath("node_modules"))

    def test_exclude_takes_precedence(self) -> None:
        """Excluded files are excluded even if they match include patterns."""
        matcher = PatternFilePathMatcher(
            included_patterns=["**/*.py"],
            excluded_patterns=["**/test_*"],
        )
        assert matcher.is_file_included(PurePath("main.py"))
        assert not matcher.is_file_included(PurePath("test_main.py"))
        assert not matcher.is_file_included(PurePath("tests/test_main.py"))


class TestPatternFilePathMatcherEdgeCases:
    def test_invalid_pattern_raises(self) -> None:
        with pytest.raises(ValueError):
            PatternFilePathMatcher(included_patterns=["[invalid"])

    def test_invalid_excluded_pattern_raises(self) -> None:
        with pytest.raises(ValueError):
            PatternFilePathMatcher(excluded_patterns=["[invalid"])

    def test_posix_path_conversion(self) -> None:
        """PurePath is converted with forward slashes for globset matching."""
        matcher = PatternFilePathMatcher(included_patterns=["src/**/*.py"])
        assert matcher.is_file_included(PurePath("src/main.py"))
        assert matcher.is_file_included(PurePath("src/sub/main.py"))
        assert not matcher.is_file_included(PurePath("main.py"))

    def test_alternation_pattern(self) -> None:
        """Globset supports {a,b} alternation syntax."""
        matcher = PatternFilePathMatcher(included_patterns=["**/*.{py,rs}"])
        assert matcher.is_file_included(PurePath("main.py"))
        assert matcher.is_file_included(PurePath("main.rs"))
        assert not matcher.is_file_included(PurePath("main.js"))

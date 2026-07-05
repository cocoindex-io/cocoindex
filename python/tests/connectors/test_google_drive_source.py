"""Tests for Google Drive source connector.

Helper-level tests run without a live Google Drive service.

Live tests are gated on the ``GOOGLE_DRIVE_CREDENTIALS`` env var; they are
skipped when it isn't set.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import PurePath

import pytest

# ---------------------------------------------------------------------------
# Optional dependency guard — mirrors the pattern in test_turbopuffer_target.py
# ---------------------------------------------------------------------------

try:
    from cocoindex.connectors.google_drive._source import (
        DriveFilePath,
        _parse_modified_time,
    )

    HAS_GOOGLE_DRIVE = True
except ImportError:
    HAS_GOOGLE_DRIVE = False

requires_google_drive = pytest.mark.skipif(
    not HAS_GOOGLE_DRIVE,
    reason="google-auth / google-api-python-client are not installed",
)

_HAS_LIVE = bool(os.environ.get("GOOGLE_DRIVE_CREDENTIALS"))
requires_live = pytest.mark.skipif(
    not (_HAS_LIVE and HAS_GOOGLE_DRIVE),
    reason="GOOGLE_DRIVE_CREDENTIALS not set; skipping live tests",
)


# =============================================================================
# Unit tests — _parse_modified_time
# =============================================================================


@requires_google_drive
class TestParseModifiedTime:
    def test_valid_utc_string(self) -> None:
        result = _parse_modified_time("2024-01-15T10:30:00Z")
        assert result == datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)

    def test_none_returns_epoch(self) -> None:
        result = _parse_modified_time(None)
        assert result == datetime.fromtimestamp(0)

    def test_empty_string_returns_epoch(self) -> None:
        result = _parse_modified_time("")
        assert result == datetime.fromtimestamp(0)

    def test_with_offset(self) -> None:
        result = _parse_modified_time("2024-06-01T12:00:00+05:30")
        assert result.tzinfo is not None
        assert result.hour == 12
        assert result.minute == 0

    def test_with_milliseconds(self) -> None:
        result = _parse_modified_time("2024-03-20T08:15:30.123Z")
        assert result.year == 2024
        assert result.month == 3
        assert result.day == 20


# =============================================================================
# Unit tests — DriveFilePath
# =============================================================================


@requires_google_drive
class TestDriveFilePath:
    def test_resolve_returns_file_id(self) -> None:
        fp = DriveFilePath("some/file.txt", file_id="drive_id_abc")
        assert fp.resolve() == "drive_id_abc"

    def test_path_preserved(self) -> None:
        fp = DriveFilePath("folder/subfolder/doc.md", file_id="xyz")
        assert fp.path == PurePath("folder/subfolder/doc.md")

    def test_with_path_preserves_file_id(self) -> None:
        fp = DriveFilePath("original.txt", file_id="id123")
        new_fp = fp._with_path(PurePath("renamed.txt"))
        assert new_fp.resolve() == "id123"
        assert new_fp.path == PurePath("renamed.txt")

    def test_with_path_returns_same_type(self) -> None:
        fp = DriveFilePath("a.txt", file_id="id1")
        new_fp = fp._with_path(PurePath("b.txt"))
        assert type(new_fp) is DriveFilePath

    def test_equality_same_values(self) -> None:
        fp1 = DriveFilePath("file.txt", file_id="abc")
        fp2 = DriveFilePath("file.txt", file_id="abc")
        assert fp1 == fp2

    def test_inequality_different_path(self) -> None:
        fp1 = DriveFilePath("a.txt", file_id="abc")
        fp2 = DriveFilePath("b.txt", file_id="abc")
        assert fp1 != fp2

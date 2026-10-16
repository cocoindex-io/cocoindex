"""Tests for Google Drive source connector.

Helper-level tests run without a live Google Drive service.

Live tests are gated on the ``GOOGLE_DRIVE_CREDENTIALS`` env var; they are
skipped when it isn't set.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import PurePath
from typing import AsyncIterator

import pytest

# ---------------------------------------------------------------------------
# Optional dependency guard — mirrors the pattern in test_turbopuffer_target.py
# ---------------------------------------------------------------------------

try:
    from cocoindex.connectors.google_drive._source import (
        DriveFile,
        DriveFileInfo,
        DriveFilePath,
        GoogleDriveSource,
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

    def test_memo_key_distinguishes_ids_and_paths(self) -> None:
        first = DriveFilePath("report.txt", file_id="id-1")
        second = DriveFilePath("report.txt", file_id="id-2")
        renamed = DriveFilePath("renamed.txt", file_id="id-1")
        assert first.__coco_memo_key__() != second.__coco_memo_key__()
        assert first.__coco_memo_key__() != renamed.__coco_memo_key__()


@requires_google_drive
@pytest.mark.asyncio
async def test_items_key_same_named_files_by_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = GoogleDriveSource(
        service_account_credential_path="unused.json", root_folder_ids=["root"]
    )

    async def files() -> AsyncIterator[DriveFile]:
        for file_id in ("id-1", "id-2"):
            yield DriveFile(
                None,
                DriveFileInfo(
                    file_id=file_id,
                    name="report.txt",
                    mime_type="text/plain",
                    size=0,
                    modified_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
                ),
            )

    monkeypatch.setattr(source, "files", files)
    items = [item async for item in source.items()]
    assert [key for key, _ in items] == ["id-1", "id-2"]
    assert [file.file_path.path.as_posix() for _, file in items] == [
        "report.txt",
        "report.txt",
    ]

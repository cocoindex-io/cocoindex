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
import cocoindex as coco
from tests.common import create_test_env

# ---------------------------------------------------------------------------
# Optional dependency guard — mirrors the pattern in test_turbopuffer_target.py
# ---------------------------------------------------------------------------

try:
    from cocoindex.connectors.google_drive._source import (
        DriveFile,
        DriveFileInfo,
        DriveFilePath,
        GoogleDriveSource,
        GoogleDriveSourceSpec,
        list_files,
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

    def test_sort_by_path_then_id(self) -> None:
        first = DriveFilePath("a.txt", file_id="id-2")
        second = DriveFilePath("b.txt", file_id="id-1")
        third = DriveFilePath("b.txt", file_id="id-3")
        assert sorted([third, second, first]) == [first, second, third]

    def test_memo_key_distinguishes_ids_and_paths(self) -> None:
        first = DriveFilePath("report.txt", file_id="id-1")
        second = DriveFilePath("report.txt", file_id="id-2")
        renamed = DriveFilePath("renamed.txt", file_id="id-1")
        assert first.__coco_memo_key__() != second.__coco_memo_key__()
        assert first.__coco_memo_key__() != renamed.__coco_memo_key__()
        assert first != second
        assert len({first, second}) == 2
        assert {first: "first", second: "second"}[second] == "second"


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


@requires_google_drive
def test_listing_preserves_paths_and_deduplicates_roots(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from cocoindex.connectors.google_drive import _source

    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    folder = DriveFileInfo("child", "nested", _source._FOLDER_MIME, 0, now)
    report = DriveFileInfo("report", "report.txt", "text/plain", 1, now)
    only = DriveFileInfo("only", "only.txt", "text/plain", 1, now)
    children = {"parent": [folder, report], "child": [report, only]}
    monkeypatch.setattr(_source, "_build_service", lambda _: None)
    monkeypatch.setattr(
        _source, "_list_children", lambda _, folder_id: children[folder_id]
    )
    spec = GoogleDriveSourceSpec("unused.json", ["parent", "child", "parent"])
    files = list(list_files(spec))
    assert [file.file_path.resolve() for file in files] == ["report", "only"]
    assert [file.file_path.as_posix() for file in files] == ["report.txt", "only.txt"]

    children["parent"] = [folder]
    files = list(list_files(GoogleDriveSourceSpec("unused.json", ["parent"])))
    assert [file.file_path.as_posix() for file in files] == [
        "nested/only.txt",
        "nested/report.txt",
    ]


@requires_google_drive
@pytest.mark.asyncio
async def test_memo_distinguishes_drive_ids_and_folder_moves(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    files = [
        DriveFile(
            None, DriveFileInfo(file_id, "report.txt", "text/plain", 1, now, path)
        )
        for file_id, path in [
            ("id-1", "a/report.txt"),
            ("id-2", "a/report.txt"),
            ("id-2", "b/report.txt"),
        ]
    ]
    calls: list[str] = []
    index = 0

    async def read(file: DriveFile, size: int = -1) -> bytes:
        return file.file_path.resolve().encode()

    monkeypatch.setattr(DriveFile, "_read_impl", read)

    @coco.fn(memo=True)
    async def process(file: DriveFile) -> str:
        calls.append(file.file_path.resolve())
        return f"{await file.read_text()}:{file.file_path.as_posix()}"

    @coco.fn
    async def main() -> str:
        return await process(files[index])

    env = create_test_env(__file__, suffix="drive_memo_identity")
    app = coco.App(coco.AppConfig(name="drive_memo_identity", environment=env), main)
    assert await app.update() == "id-1:a/report.txt"
    index = 1
    assert await app.update() == "id-2:a/report.txt"
    index = 2
    assert await app.update() == "id-2:b/report.txt"
    assert await app.update() == "id-2:b/report.txt"
    assert calls == ["id-1", "id-2", "id-2"]


class _FakeDriveFiles:
    """The ``service.files()`` surface that ``list_files`` uses."""

    def __init__(self, children: dict[str, list[dict[str, str]]]) -> None:
        self.children = children

    def list(self, *, q: str, fields: str, pageToken: str | None) -> _FakeRequest:
        folder_id = q.split("'")[1]
        return _FakeRequest({"files": self.children[folder_id]})


class _FakeRequest:
    def __init__(self, response: dict[str, object]) -> None:
        self._response = response

    def execute(self) -> dict[str, object]:
        return self._response


class _FakeDriveService:
    def __init__(self, children: dict[str, list[dict[str, str]]]) -> None:
        self._files = _FakeDriveFiles(children)

    def files(self) -> _FakeDriveFiles:
        return self._files


def _drive_entry(
    file_id: str, name: str, mime_type: str = "text/plain", **extra: str
) -> dict[str, str]:
    return {
        "id": file_id,
        "name": name,
        "mimeType": mime_type,
        "modifiedTime": "2024-01-01T00:00:00Z",
        **extra,
    }


@requires_google_drive
@pytest.mark.asyncio
async def test_listing_uses_md5_as_content_fingerprint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from cocoindex.connectors.google_drive import _source

    service = _FakeDriveService(
        {
            "root": [
                _drive_entry("bin", "data.bin", md5Checksum="abc123", size="3"),
                _drive_entry("doc", "notes", _source._DOCS_MIME),
            ]
        }
    )
    monkeypatch.setattr(_source, "_build_service", lambda _: service)
    files = {
        f.file_path.resolve(): f
        for f in list_files(GoogleDriveSourceSpec("unused.json", ["root"]))
    }
    assert await files["bin"].content_fingerprint() == b"abc123"
    # Google Docs have no MD5: the fingerprint hashes the exported content.
    assert (await files["doc"]._ensure_metadata()).content_fingerprint is None


@requires_google_drive
def test_file_with_several_parents_keeps_one_path_whatever_the_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from cocoindex.connectors.google_drive import _source

    folders = [
        _drive_entry("f-b", "b", _source._FOLDER_MIME),
        _drive_entry("f-a", "a", _source._FOLDER_MIME),
    ]
    shared = _drive_entry("shared", "report.txt")
    children = {"root": folders, "f-a": [shared], "f-b": [shared]}
    monkeypatch.setattr(
        _source, "_build_service", lambda _: _FakeDriveService(children)
    )
    spec = GoogleDriveSourceSpec("unused.json", ["root"])

    first = [f.file_path.as_posix() for f in list_files(spec)]
    children["root"] = list(reversed(folders))
    second = [f.file_path.as_posix() for f in list_files(spec)]
    assert first == second == ["a/report.txt"]


@requires_google_drive
@pytest.mark.asyncio
async def test_memo_follows_md5_not_modified_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A new mtime with the same MD5 reuses the memo; a new MD5 recomputes."""
    reads: list[str] = []

    async def read(file: DriveFile, size: int = -1) -> bytes:
        reads.append(file.file_path.resolve())
        return b"content"

    monkeypatch.setattr(DriveFile, "_read_impl", read)

    calls: list[str] = []

    @coco.fn(memo=True)
    async def process(file: DriveFile) -> int:
        calls.append(file.file_path.resolve())
        return len(await file.read())

    current: DriveFile

    @coco.fn
    async def main() -> int:
        return await process(current)

    env = create_test_env(__file__, suffix="drive_md5_memo")
    app = coco.App(coco.AppConfig(name="drive_md5_memo", environment=env), main)
    t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    t2 = datetime(2024, 1, 2, tzinfo=timezone.utc)
    for mtime, md5 in [(t1, "m1"), (t2, "m1"), (t2, "m2")]:
        current = DriveFile(
            None,
            DriveFileInfo(
                "id-1", "a.bin", "application/octet-stream", 7, mtime, md5_checksum=md5
            ),
        )
        assert await app.update() == 7
    assert calls == ["id-1", "id-1"]
    assert reads == ["id-1", "id-1"]


@requires_google_drive
@pytest.mark.asyncio
async def test_memo_without_md5_skips_read_while_mtime_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Google Doc has no MD5: an unchanged mtime reuses the memo without a download."""
    from cocoindex.connectors.google_drive import _source

    reads: list[str] = []

    async def read(file: DriveFile, size: int = -1) -> bytes:
        reads.append(file.file_path.resolve())
        return b"content"

    monkeypatch.setattr(DriveFile, "_read_impl", read)

    calls: list[str] = []

    @coco.fn(memo=True)
    async def process(file: DriveFile) -> int:
        calls.append(file.file_path.resolve())
        return len(await file.read())

    current: DriveFile

    @coco.fn
    async def main() -> int:
        return await process(current)

    env = create_test_env(__file__, suffix="drive_no_md5_memo")
    app = coco.App(coco.AppConfig(name="drive_no_md5_memo", environment=env), main)
    mtime = datetime(2024, 1, 1, tzinfo=timezone.utc)
    for _ in range(2):
        current = DriveFile(
            None, DriveFileInfo("doc-1", "notes", _source._DOCS_MIME, 0, mtime)
        )
        assert await app.update() == 7
    assert calls == ["doc-1"]
    assert reads == ["doc-1"]

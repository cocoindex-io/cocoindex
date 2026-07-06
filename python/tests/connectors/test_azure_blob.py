"""Tests for the Azure Blob Storage source connector."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import PurePath
from typing import AsyncIterator

import pytest

from cocoindex.connectors import azure_blob
from cocoindex.resources.file import PatternFilePathMatcher


_NOW = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)


@dataclass(frozen=True)
class _FakeBlobProperties:
    name: str
    size: int
    last_modified: datetime
    etag: str | None


class _FakeDownloader:
    def __init__(self, data: bytes, offset: int | None, length: int | None) -> None:
        self._data = data
        self._offset = offset or 0
        self._length = length

    async def readall(self) -> bytes:
        if self._length is None:
            return self._data[self._offset :]
        return self._data[self._offset : self._offset + self._length]


class _FakeBlobClient:
    def __init__(self, container: _FakeContainerClient, blob_name: str) -> None:
        self._container = container
        self._blob_name = blob_name

    async def get_blob_properties(self) -> _FakeBlobProperties:
        return self._container.properties_for(self._blob_name)

    async def download_blob(
        self, *, offset: int | None = None, length: int | None = None
    ) -> _FakeDownloader:
        return _FakeDownloader(
            self._container.data_for(self._blob_name), offset, length
        )


class _FakeContainerClient:
    account_name: str | None = "testaccount"
    container_name: str | None = "docs"
    url = "https://testaccount.blob.core.windows.net/docs"

    def __init__(self) -> None:
        self._objects = {
            "file1.txt": b"hello",
            "file2.md": b"# Title",
            "data/nested.json": b'{"key": "value"}',
            "data/deep/file.txt": b"deep content",
            "data/empty/": b"",
            "large.bin": b"x" * 10000,
        }

    def _summary(self, name: str) -> _FakeBlobProperties:
        return _FakeBlobProperties(
            name=name,
            size=len(self._objects[name]),
            last_modified=_NOW,
            etag=f'"etag-{name}"',
        )

    async def list_blobs(
        self, *, name_starts_with: str | None = None
    ) -> AsyncIterator[_FakeBlobProperties]:
        for name in sorted(self._objects):
            if name_starts_with and not name.startswith(name_starts_with):
                continue
            yield self._summary(name)

    def get_blob_client(
        self,
        blob: str,
        snapshot: str | None = None,
        *,
        version_id: str | None = None,
    ) -> _FakeBlobClient:
        return _FakeBlobClient(self, blob)

    def properties_for(self, blob_name: str) -> _FakeBlobProperties:
        if blob_name not in self._objects:
            raise FileNotFoundError(blob_name)
        return self._summary(blob_name)

    def data_for(self, blob_name: str) -> bytes:
        if blob_name not in self._objects:
            raise FileNotFoundError(blob_name)
        return self._objects[blob_name]


@pytest.fixture
def container_client() -> _FakeContainerClient:
    return _FakeContainerClient()


@pytest.mark.asyncio
class TestAsyncListBlobs:
    async def test_basic_listing(self, container_client: _FakeContainerClient) -> None:
        walker = azure_blob.list_blobs(container_client)
        files: list[azure_blob.AzureBlobFile] = []
        async for f in walker:
            files.append(f)

        paths = sorted(f.file_path.as_posix() for f in files)
        assert paths == [
            "data/deep/file.txt",
            "data/nested.json",
            "file1.txt",
            "file2.md",
            "large.bin",
        ]

    async def test_with_prefix(self, container_client: _FakeContainerClient) -> None:
        walker = azure_blob.list_blobs(container_client, prefix="data/")
        paths: list[str] = []
        async for f in walker:
            paths.append(f.file_path.as_posix())

        assert sorted(paths) == ["deep/file.txt", "nested.json"]

    async def test_with_pattern_matcher(
        self, container_client: _FakeContainerClient
    ) -> None:
        matcher = PatternFilePathMatcher(included_patterns=["**/*.txt"])
        walker = azure_blob.list_blobs(container_client, path_matcher=matcher)
        paths: list[str] = []
        async for f in walker:
            paths.append(f.file_path.as_posix())

        assert "file1.txt" in paths
        assert "data/deep/file.txt" in paths
        assert "file2.md" not in paths

    async def test_with_excluded_patterns(
        self, container_client: _FakeContainerClient
    ) -> None:
        matcher = PatternFilePathMatcher(excluded_patterns=["**/*.bin"])
        walker = azure_blob.list_blobs(container_client, path_matcher=matcher)
        paths: list[str] = []
        async for f in walker:
            paths.append(f.file_path.as_posix())

        assert "large.bin" not in paths
        assert len(paths) == 4

    async def test_max_file_size(self, container_client: _FakeContainerClient) -> None:
        walker = azure_blob.list_blobs(container_client, max_file_size=100)
        paths: list[str] = []
        async for f in walker:
            paths.append(f.file_path.as_posix())

        assert "large.bin" not in paths
        assert len(paths) == 4

    async def test_directory_markers_skipped(
        self, container_client: _FakeContainerClient
    ) -> None:
        walker = azure_blob.list_blobs(container_client)
        paths: list[str] = []
        async for f in walker:
            paths.append(f.file_path.as_posix())

        assert "data/empty/" not in paths
        assert "data/empty" not in paths

    async def test_items(self, container_client: _FakeContainerClient) -> None:
        walker = azure_blob.list_blobs(container_client, prefix="data/")
        items: list[tuple[str, azure_blob.AzureBlobFile]] = []
        async for item in walker.items():
            items.append(item)

        items.sort(key=lambda x: x[0])
        assert items[0][0] == "deep/file.txt"
        assert items[1][0] == "nested.json"
        assert isinstance(items[0][1], azure_blob.AzureBlobFile)


@pytest.mark.asyncio
class TestAzureBlobFile:
    async def test_read(self, container_client: _FakeContainerClient) -> None:
        f = await azure_blob.get_blob(container_client, "data/nested.json")
        assert await f.read() == b'{"key": "value"}'

    async def test_read_text(self, container_client: _FakeContainerClient) -> None:
        f = await azure_blob.get_blob(container_client, "file1.txt")
        assert await f.read_text() == "hello"

    async def test_read_size_limit(
        self, container_client: _FakeContainerClient
    ) -> None:
        f = await azure_blob.get_blob(container_client, "data/deep/file.txt")
        assert await f.read(4) == b"deep"

    async def test_size(self, container_client: _FakeContainerClient) -> None:
        f = await azure_blob.get_blob(container_client, "file1.txt")
        assert await f.size() == 5

    async def test_file_path_properties(
        self, container_client: _FakeContainerClient
    ) -> None:
        f = await azure_blob.get_blob(container_client, "data/nested.json")

        assert f.file_path.name == "nested.json"
        assert f.file_path.suffix == ".json"
        assert f.file_path.stem == "nested"

    async def test_resolve_returns_full_blob_name(
        self, container_client: _FakeContainerClient
    ) -> None:
        walker = azure_blob.list_blobs(container_client, prefix="data/")
        files: dict[str, azure_blob.AzureBlobFile] = {}
        async for f in walker:
            files[f.file_path.as_posix()] = f

        assert files["nested.json"].file_path.resolve() == "data/nested.json"


@pytest.mark.asyncio
class TestGetBlob:
    async def test_get_blob_returns_file(
        self, container_client: _FakeContainerClient
    ) -> None:
        f = await azure_blob.get_blob(container_client, "file1.txt")
        assert isinstance(f, azure_blob.AzureBlobFile)

    async def test_get_blob_nonexistent_raises(
        self, container_client: _FakeContainerClient
    ) -> None:
        with pytest.raises(FileNotFoundError):
            await azure_blob.get_blob(container_client, "missing.txt")


@pytest.mark.asyncio
class TestRead:
    async def test_read_shortcut(self, container_client: _FakeContainerClient) -> None:
        data = await azure_blob.read(container_client, "file1.txt")
        assert data == b"hello"

    async def test_read_shortcut_size_limit(
        self, container_client: _FakeContainerClient
    ) -> None:
        data = await azure_blob.read(container_client, "file1.txt", size=2)
        assert data == b"he"

    async def test_read_zero_bytes(
        self, container_client: _FakeContainerClient
    ) -> None:
        data = await azure_blob.read(container_client, "file1.txt", size=0)
        assert data == b""


@pytest.mark.asyncio
class TestMemoization:
    async def test_memo_key_is_path_only(
        self, container_client: _FakeContainerClient
    ) -> None:
        import cocoindex

        f1 = await azure_blob.get_blob(container_client, "file1.txt")
        outcome1 = await f1.__coco_memo_state__(cocoindex.NON_EXISTENCE)

        f2 = azure_blob.AzureBlobFile(
            container_client=container_client,
            file_path=f1.file_path,
        )
        outcome2 = await f2.__coco_memo_state__(outcome1.state)

        assert f1.__coco_memo_key__() == f2.__coco_memo_key__()
        assert outcome2.memo_valid is True

    async def test_memo_state_serde_roundtrip(
        self, container_client: _FakeContainerClient
    ) -> None:
        import cocoindex
        from cocoindex._internal import serde

        f = await azure_blob.get_blob(container_client, "file1.txt")
        fp = await f.content_fingerprint()
        assert isinstance(fp, bytes)

        outcome = await f.__coco_memo_state__(cocoindex.NON_EXISTENCE)
        hint = serde.strip_non_existence_type(
            serde.get_param_annotation(f.__coco_memo_state__, 0)
        )
        payload = serde.serialize(outcome.state)
        restored = serde.make_deserialize_fn(hint)(payload)

        assert restored == outcome.state


class TestAzureBlobFilePathUnit:
    def test_properties(self) -> None:
        fp = azure_blob.AzureBlobFilePath(
            account_name="acct",
            container_name="docs",
            path="folder/file.txt",
            blob_name="folder/file.txt",
        )

        assert fp.account_name == "acct"
        assert fp.container_name == "docs"
        assert fp.blob_name == "folder/file.txt"
        assert fp.resolve() == "folder/file.txt"

    def test_memo_key_includes_account_container_and_path(self) -> None:
        fp = azure_blob.AzureBlobFilePath(
            account_name="acct",
            container_name="docs",
            path="folder/file.txt",
            blob_name="folder/file.txt",
        )

        assert fp.__coco_memo_key__() == (
            "acct",
            "docs",
            PurePath("folder/file.txt"),
        )

    def test_memo_key_differs_across_containers(self) -> None:
        fp1 = azure_blob.AzureBlobFilePath(
            account_name="acct",
            container_name="docs-a",
            path="file.txt",
            blob_name="file.txt",
        )
        fp2 = azure_blob.AzureBlobFilePath(
            account_name="acct",
            container_name="docs-b",
            path="file.txt",
            blob_name="file.txt",
        )

        assert fp1.__coco_memo_key__() != fp2.__coco_memo_key__()

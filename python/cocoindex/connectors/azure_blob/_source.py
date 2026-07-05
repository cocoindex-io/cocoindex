"""Azure Blob Storage source utilities."""

from __future__ import annotations

__all__ = [
    "AzureBlobFile",
    "AzureBlobFilePath",
    "AzureBlobWalker",
    "get_blob",
    "list_blobs",
    "read",
]

from collections.abc import AsyncIterable, AsyncIterator
from datetime import datetime
from pathlib import PurePath
from typing import Protocol, cast
from urllib.parse import urlparse

from cocoindex.resources import file


class _BlobProperties(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def size(self) -> int: ...

    @property
    def last_modified(self) -> datetime: ...

    @property
    def etag(self) -> str | None: ...


class _BlobDownloader(Protocol):
    async def readall(self) -> bytes: ...


class _BlobClient(Protocol):
    async def get_blob_properties(self) -> _BlobProperties: ...

    async def download_blob(
        self, *, offset: int | None = None, length: int | None = None
    ) -> _BlobDownloader: ...


class _ContainerClient(Protocol):
    @property
    def account_name(self) -> str | None: ...

    @property
    def container_name(self) -> str | None: ...

    @property
    def url(self) -> str: ...

    def list_blobs(
        self, *, name_starts_with: str | None = None
    ) -> AsyncIterable[_BlobProperties]: ...

    def get_blob_client(
        self,
        blob: str,
        snapshot: str | None = None,
        *,
        version_id: str | None = None,
    ) -> object: ...


def _etag_to_fingerprint(etag: object) -> bytes | None:
    return etag.encode("utf-8") if isinstance(etag, str) else None


def _text_attr(obj: object, name: str) -> str | None:
    value = getattr(obj, name, None)
    return value if isinstance(value, str) and value else None


def _identity_from_url(client: _ContainerClient) -> tuple[str | None, str | None]:
    url = _text_attr(client, "url")
    if url is None:
        return None, None

    parsed = urlparse(url)
    account_name = parsed.netloc.split(".", 1)[0] if parsed.netloc else None
    path_parts = [part for part in parsed.path.split("/") if part]
    container_name = path_parts[0] if path_parts else None
    return account_name, container_name


def _container_identity(client: _ContainerClient) -> tuple[str, str]:
    account_name = _text_attr(client, "account_name")
    container_name = _text_attr(client, "container_name")
    url_account_name, url_container_name = _identity_from_url(client)
    account_name = account_name or url_account_name
    container_name = container_name or url_container_name
    if account_name is None or container_name is None:
        raise ValueError(
            "Azure Blob container client must expose account_name and "
            "container_name, or a URL containing both."
        )
    return account_name, container_name


def _metadata_from_properties(props: _BlobProperties) -> file.FileMetadata:
    return file.FileMetadata(
        size=int(props.size),
        modified_time=props.last_modified,
        content_fingerprint=_etag_to_fingerprint(props.etag),
    )


def _get_blob_client(
    container_client: _ContainerClient,
    blob_name: str,
) -> _BlobClient:
    return cast(_BlobClient, container_client.get_blob_client(blob_name))


class AzureBlobFilePath(file.FilePath[str]):
    """File path for Azure Blob Storage objects."""

    __slots__ = ("_account_name", "_container_name", "_blob_name")

    _account_name: str
    _container_name: str
    _blob_name: str

    def __init__(
        self,
        account_name: str,
        container_name: str,
        path: str | PurePath,
        *,
        blob_name: str,
    ) -> None:
        super().__init__(None, PurePath(path))
        self._account_name = account_name
        self._container_name = container_name
        self._blob_name = blob_name

    @property
    def account_name(self) -> str:
        return self._account_name

    @property
    def container_name(self) -> str:
        return self._container_name

    @property
    def blob_name(self) -> str:
        return self._blob_name

    def resolve(self) -> str:
        """Return the full blob name."""
        return self._blob_name

    def _with_path(self, path: PurePath) -> AzureBlobFilePath:
        return type(self)(
            self._account_name,
            self._container_name,
            path,
            blob_name=self._blob_name,
        )

    def __coco_memo_key__(self) -> object:
        return (self._account_name, self._container_name, self._path)


class AzureBlobFile(file.FileLike[str]):
    """An Azure Blob Storage object exposed as a FileLike."""

    _file_path: AzureBlobFilePath
    _container_client: _ContainerClient

    def __init__(
        self,
        container_client: _ContainerClient,
        file_path: AzureBlobFilePath,
        *,
        _metadata: file.FileMetadata | None = None,
    ) -> None:
        super().__init__(file_path, _metadata=_metadata)
        self._container_client = container_client

    @property
    def file_path(self) -> AzureBlobFilePath:
        return self._file_path

    async def _fetch_metadata(self) -> file.FileMetadata:
        blob_client = _get_blob_client(
            self._container_client, self._file_path.blob_name
        )
        props = await blob_client.get_blob_properties()
        return _metadata_from_properties(props)

    async def _read_impl(self, size: int = -1) -> bytes:
        if size == 0:
            return b""

        blob_client = _get_blob_client(
            self._container_client, self._file_path.blob_name
        )
        length = None if size < 0 else size
        downloader = await blob_client.download_blob(offset=0, length=length)
        return bytes(await downloader.readall())


async def get_blob(container_client: _ContainerClient, blob_name: str) -> AzureBlobFile:
    """Get a single blob by name."""
    account_name, container_name = _container_identity(container_client)
    blob_client = _get_blob_client(container_client, blob_name)
    props = await blob_client.get_blob_properties()
    file_path = AzureBlobFilePath(
        account_name,
        container_name,
        blob_name,
        blob_name=blob_name,
    )
    return AzureBlobFile(
        container_client=container_client,
        file_path=file_path,
        _metadata=_metadata_from_properties(props),
    )


async def read(
    container_client: _ContainerClient, blob_name: str, size: int = -1
) -> bytes:
    """Read blob content directly by name."""
    if size == 0:
        return b""

    blob_client = _get_blob_client(container_client, blob_name)
    length = None if size < 0 else size
    downloader = await blob_client.download_blob(offset=0, length=length)
    return bytes(await downloader.readall())


class AzureBlobWalker:
    """A walker that lists Azure blobs via async iteration."""

    _container_client: _ContainerClient
    _account_name: str
    _container_name: str
    _prefix: str
    _path_matcher: file.FilePathMatcher
    _max_file_size: int | None

    def __init__(
        self,
        container_client: _ContainerClient,
        *,
        prefix: str = "",
        path_matcher: file.FilePathMatcher | None = None,
        max_file_size: int | None = None,
    ) -> None:
        self._container_client = container_client
        self._account_name, self._container_name = _container_identity(container_client)
        self._prefix = prefix
        self._path_matcher = path_matcher or file.MatchAllFilePathMatcher()
        self._max_file_size = max_file_size

    async def _aiter_blob_files(self) -> AsyncIterator[AzureBlobFile]:
        name_starts_with = self._prefix or None
        async for blob in self._container_client.list_blobs(
            name_starts_with=name_starts_with
        ):
            blob_name = blob.name
            if blob_name.endswith("/"):
                continue

            if self._prefix:
                relative_name = blob_name[len(self._prefix) :].lstrip("/")
            else:
                relative_name = blob_name

            if not relative_name:
                continue

            relative_path = PurePath(relative_name)
            if not self._path_matcher.is_file_included(relative_path):
                continue

            blob_size = int(blob.size)
            if self._max_file_size is not None and blob_size > self._max_file_size:
                continue

            file_path = AzureBlobFilePath(
                self._account_name,
                self._container_name,
                relative_name,
                blob_name=blob_name,
            )
            yield AzureBlobFile(
                container_client=self._container_client,
                file_path=file_path,
                _metadata=_metadata_from_properties(blob),
            )

    async def __aiter__(self) -> AsyncIterator[AzureBlobFile]:
        async for f in self._aiter_blob_files():
            yield f

    async def items(self) -> AsyncIterator[tuple[str, AzureBlobFile]]:
        async for f in self._aiter_blob_files():
            yield f.file_path.path.as_posix(), f


def list_blobs(
    container_client: _ContainerClient,
    *,
    prefix: str = "",
    path_matcher: file.FilePathMatcher | None = None,
    max_file_size: int | None = None,
) -> AzureBlobWalker:
    """List blobs in a container and yield file entries."""
    return AzureBlobWalker(
        container_client,
        prefix=prefix,
        path_matcher=path_matcher,
        max_file_size=max_file_size,
    )

"""Amazon S3 source utilities.

This module provides a read-only API for listing and reading objects from
Amazon S3 buckets (and S3-compatible services like MinIO).

The API is async-only: ``S3File`` implements ``AsyncFileLike`` and is the
primary file type.
"""

from __future__ import annotations

__all__ = [
    "S3File",
    "S3FilePath",
    "S3Walker",
    "get_object",
    "list_objects",
    "read",
]

from datetime import datetime
from pathlib import PurePath
from typing import AsyncIterator, overload

try:
    from aiobotocore.client import AioBaseClient  # type: ignore[import-untyped]
except ImportError as e:
    raise ImportError(
        "aiobotocore is required to use the Amazon S3 source connector. "
        "Please install cocoindex[amazon_s3]."
    ) from e

from cocoindex.connectorkits import connection
from cocoindex.resources import file


_REGISTRY_NAME = "cocoindex/amazon_s3"


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    """Parse an ``s3://bucket/key`` URI into *(bucket_name, key)*."""
    if not uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI {uri!r}: expected 's3://bucket/key'.")
    without_scheme = uri[len("s3://") :]
    slash_idx = without_scheme.find("/")
    if slash_idx == -1:
        raise ValueError(f"Invalid S3 URI {uri!r}: expected 's3://bucket/key'.")
    return without_scheme[:slash_idx], without_scheme[slash_idx + 1 :]


class S3FilePath(file.FilePath[str]):
    """
    File path for Amazon S3 objects.

    The resolved path is the full S3 object key (string).
    The relative path is the object key relative to the walker prefix (or the full key
    if no prefix was used).
    """

    __slots__ = ("_object_key",)

    _object_key: str

    def __init__(
        self,
        base_dir: connection.KeyedConnection[str],
        path: str | PurePath,
        *,
        object_key: str,
    ) -> None:
        super().__init__(base_dir, PurePath(path))
        self._object_key = object_key

    def resolve(self) -> str:
        """Return the full S3 object key."""
        return self._object_key

    def _with_path(self, path: PurePath) -> S3FilePath:
        """Create a new S3FilePath with the given path."""
        return type(self)(self._base_dir, path, object_key=self._object_key)


class S3File(file.AsyncFileLike[str]):
    """Represents a file entry from an S3 bucket (async).

    Implements the ``AsyncFileLike`` protocol using native aiobotocore async I/O.
    """

    _client: AioBaseClient
    _file_path: S3FilePath
    _size: int
    _modified_time: datetime

    def __init__(
        self,
        client: AioBaseClient,
        file_path: S3FilePath,
        *,
        size: int,
        modified_time: datetime,
    ) -> None:
        self._client = client
        self._file_path = file_path
        self._size = size
        self._modified_time = modified_time

    @property
    def file_path(self) -> S3FilePath:
        """Return the S3FilePath of this file."""
        return self._file_path

    @property
    def stable_key(self) -> str:
        """Return the stable key for this file."""
        return self.file_path.path.as_posix()

    @property
    def size(self) -> int:
        """Return the file size in bytes."""
        return self._size

    @property
    def modified_time(self) -> datetime:
        """Return the last modified time."""
        return self._modified_time

    async def read(self, size: int = -1) -> bytes:
        """Asynchronously read and return file content as bytes.

        Args:
            size: Number of bytes to read. If -1 (default), read the entire file.
        """
        bucket_name: str = self._file_path.base_dir.value
        object_key: str = self._file_path.resolve()
        if size >= 0:
            response = await self._client.get_object(
                Bucket=bucket_name,
                Key=object_key,
                Range=f"bytes=0-{size - 1}",
            )
        else:
            response = await self._client.get_object(
                Bucket=bucket_name,
                Key=object_key,
            )
        async with response["Body"] as stream:
            return bytes(await stream.read())


async def _s3file_from_head(
    client: AioBaseClient,
    bucket_name: str,
    object_key: str,
    base_dir: connection.KeyedConnection[str],
) -> S3File:
    """Create an S3File by fetching object metadata via head_object."""
    head = await client.head_object(Bucket=bucket_name, Key=object_key)
    file_path = S3FilePath(
        base_dir,
        object_key,
        object_key=object_key,
    )
    return S3File(
        client=client,
        file_path=file_path,
        size=int(head["ContentLength"]),
        modified_time=head["LastModified"],
    )


@overload
async def get_object(client: AioBaseClient, uri: str, /) -> S3File: ...
@overload
async def get_object(
    client: AioBaseClient, bucket_name: str, key: str, /
) -> S3File: ...


async def get_object(
    client: AioBaseClient,
    bucket_name_or_uri: str,
    key: str | None = None,
) -> S3File:
    """
    Get a single object from an S3 bucket by its key.

    Accepts either an S3 URI or a bucket name with a separate key:

    * ``get_object(client, "s3://my-bucket/data/config.json")``
    * ``get_object(client, "my-bucket", "data/config.json")``

    Args:
        client: An aiobotocore S3 client.
        bucket_name_or_uri: Either a full S3 URI (``s3://bucket/key``) or the
            bucket name when *key* is supplied separately.
        key: The full S3 object key.  Required when *bucket_name_or_uri* is a
            bucket name; must be omitted (or ``None``) when a URI is given.

    Returns:
        An S3File (async) for the specified object.

    Example::

        import aiobotocore.session
        from cocoindex.connectors import amazon_s3

        session = aiobotocore.session.get_session()
        async with session.create_client("s3") as client:
            # Via S3 URI:
            f = await amazon_s3.get_object(client, "s3://my-bucket/data/config.json")
            data = await f.read()

            # Via bucket name + key:
            f = await amazon_s3.get_object(client, "my-bucket", "data/config.json")
            data = await f.read()
    """
    if bucket_name_or_uri.startswith("s3://"):
        if key is not None:
            raise ValueError(
                "Cannot specify both an S3 URI and a separate key. "
                "Pass either get_object(client, 's3://bucket/key') "
                "or get_object(client, 'bucket', 'key')."
            )
        bucket_name, key = _parse_s3_uri(bucket_name_or_uri)
    else:
        bucket_name = bucket_name_or_uri
        if key is None:
            raise ValueError(
                "key must be provided when bucket_name_or_uri is not an S3 URI."
            )
    base_dir = connection.keyed_value(_REGISTRY_NAME, bucket_name)
    return await _s3file_from_head(client, bucket_name, key, base_dir)


async def read(client: AioBaseClient, uri: str, size: int = -1) -> bytes:
    """
    Read object content directly from an S3 URI.

    This is a convenience shortcut that skips the metadata fetch
    (``head_object``) performed by :func:`get_object`.

    Args:
        client: An aiobotocore S3 client.
        uri: An S3 URI (``s3://bucket/key``).
        size: Number of bytes to read. If -1 (default), read the entire object.

    Returns:
        The object content as bytes.
    """
    bucket_name, key = _parse_s3_uri(uri)
    if size >= 0:
        response = await client.get_object(
            Bucket=bucket_name, Key=key, Range=f"bytes=0-{size - 1}"
        )
    else:
        response = await client.get_object(Bucket=bucket_name, Key=key)
    async with response["Body"] as stream:
        return bytes(await stream.read())


class S3Walker:
    """A walker that lists objects in an S3 bucket via async iteration.

    Async iteration yields ``S3File`` objects::

        async for file in list_objects(client, "my-bucket"):
            content = await file.read()
    """

    _client: AioBaseClient
    _bucket_name: str
    _prefix: str
    _path_matcher: file.FilePathMatcher
    _max_file_size: int | None
    _base_dir: connection.KeyedConnection[str]

    def __init__(
        self,
        client: AioBaseClient,
        bucket_name: str,
        *,
        prefix: str = "",
        path_matcher: file.FilePathMatcher | None = None,
        max_file_size: int | None = None,
    ) -> None:
        self._client = client
        self._bucket_name = bucket_name
        self._prefix = prefix
        self._path_matcher = path_matcher or file.MatchAllFilePathMatcher()
        self._max_file_size = max_file_size
        self._base_dir = connection.keyed_value(_REGISTRY_NAME, bucket_name)

    async def _aiter_s3files(self) -> AsyncIterator[S3File]:
        """Primary async iteration over S3 objects, yielding S3File objects."""
        page_kwargs: dict[str, str] = {"Bucket": self._bucket_name}
        if self._prefix:
            page_kwargs["Prefix"] = self._prefix

        paginator = self._client.get_paginator("list_objects_v2")
        async for page in paginator.paginate(**page_kwargs):
            for obj in page.get("Contents", []):
                key: str = obj["Key"]

                # Skip directory markers (keys ending with /)
                if key.endswith("/"):
                    continue

                # Compute relative path (strip prefix)
                if self._prefix:
                    relative_key = key[len(self._prefix) :]
                    relative_key = relative_key.lstrip("/")
                else:
                    relative_key = key

                if not relative_key:
                    continue

                relative_path = PurePath(relative_key)

                # Apply path matcher
                if not self._path_matcher.is_file_included(relative_path):
                    continue

                size: int = obj["Size"]

                # Apply max_file_size filter
                if self._max_file_size is not None and size > self._max_file_size:
                    continue

                modified_time: datetime = obj["LastModified"]

                file_path = S3FilePath(
                    self._base_dir,
                    relative_key,
                    object_key=key,
                )

                yield S3File(
                    client=self._client,
                    file_path=file_path,
                    size=size,
                    modified_time=modified_time,
                )

    async def __aiter__(self) -> AsyncIterator[S3File]:
        """Asynchronously iterate over S3 objects, yielding S3File objects."""
        async for f in self._aiter_s3files():
            yield f

    async def items(self) -> AsyncIterator[tuple[str, S3File]]:
        """Async iterate over (key, file) pairs.

        The key is the file's relative path within the bucket (after prefix stripping).

        Example::

            async for key, file in walker.items():
                content = await file.read()
        """
        async for f in self._aiter_s3files():
            yield (f.file_path.path.as_posix(), f)


def list_objects(
    client: AioBaseClient,
    bucket_name: str,
    *,
    prefix: str = "",
    path_matcher: file.FilePathMatcher | None = None,
    max_file_size: int | None = None,
) -> S3Walker:
    """
    List objects in an S3 bucket and yield file entries.

    Returns an S3Walker that supports async iteration, yielding ``S3File`` objects.

    Args:
        client: An aiobotocore S3 client.
        bucket_name: The S3 bucket name.
        prefix: Only list objects whose key starts with this prefix.
        path_matcher: Optional file path matcher to filter files by glob patterns.
            Patterns are matched against the relative path (after prefix stripping).
        max_file_size: Skip objects larger than this size in bytes.

    Returns:
        An S3Walker that can be used with ``async for`` loops.

    Example::

        import aiobotocore.session
        from cocoindex.connectors import amazon_s3

        session = aiobotocore.session.get_session()
        async with session.create_client("s3") as client:
            async for file in amazon_s3.list_objects(client, "my-bucket", prefix="data/"):
                content = await file.read()

        With pattern matching::

            from cocoindex.resources.file import PatternFilePathMatcher
            matcher = PatternFilePathMatcher(included_patterns=["**/*.json"])
            async for file in amazon_s3.list_objects(client, "my-bucket", path_matcher=matcher):
                data = await file.read()
    """
    return S3Walker(
        client,
        bucket_name,
        prefix=prefix,
        path_matcher=path_matcher,
        max_file_size=max_file_size,
    )

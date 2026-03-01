"""Tests for the Amazon S3 source connector."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, AsyncIterator

import aiobotocore.session
import boto3
import pytest
import pytest_asyncio
from aiomoto import mock_aws

from cocoindex.connectors import amazon_s3
from cocoindex.resources.file import PatternFilePathMatcher


@pytest_asyncio.fixture
async def s3_client() -> AsyncIterator[tuple[Any, str]]:
    """Create a mocked S3 bucket with test files and yield an aiobotocore client."""
    async with mock_aws():
        bucket_name = "test-bucket"

        # Setup with sync boto3 (works inside aiomoto's mock_aws)
        sync_client = boto3.client("s3", region_name="us-east-1")
        sync_client.create_bucket(Bucket=bucket_name)
        sync_client.put_object(Bucket=bucket_name, Key="file1.txt", Body=b"hello")
        sync_client.put_object(Bucket=bucket_name, Key="file2.md", Body=b"# Title")
        sync_client.put_object(
            Bucket=bucket_name, Key="data/nested.json", Body=b'{"key": "value"}'
        )
        sync_client.put_object(
            Bucket=bucket_name, Key="data/deep/file.txt", Body=b"deep content"
        )
        # Directory marker
        sync_client.put_object(Bucket=bucket_name, Key="data/empty/", Body=b"")
        # Large file
        sync_client.put_object(Bucket=bucket_name, Key="large.bin", Body=b"x" * 10000)

        # Create aiobotocore async client
        session = aiobotocore.session.get_session()
        async with session.create_client("s3", region_name="us-east-1") as client:
            yield client, bucket_name


# ---------------------------------------------------------------------------
# Async iteration tests (primary path)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestAsyncListObjects:
    """Tests for async iteration (primary path)."""

    async def test_basic_listing(self, s3_client: tuple[Any, str]) -> None:
        """Async iteration yields all non-directory S3File objects."""
        client, bucket_name = s3_client
        walker = amazon_s3.list_objects(client, bucket_name)
        files: list[amazon_s3.S3File] = []
        async for f in walker:
            files.append(f)

        files.sort(key=lambda f: f.file_path.as_posix())
        assert len(files) == 5
        keys = [f.file_path.as_posix() for f in files]
        assert "file1.txt" in keys
        assert "file2.md" in keys
        assert "data/nested.json" in keys
        assert "data/deep/file.txt" in keys
        assert "large.bin" in keys
        # Directory marker should not appear
        assert "data/empty/" not in keys
        assert "data/empty" not in keys

    async def test_with_prefix(self, s3_client: tuple[Any, str]) -> None:
        """Prefix filters and strips correctly."""
        client, bucket_name = s3_client
        walker = amazon_s3.list_objects(client, bucket_name, prefix="data/")
        files: list[amazon_s3.S3File] = []
        async for f in walker:
            files.append(f)

        paths = sorted(f.file_path.as_posix() for f in files)
        assert paths == ["deep/file.txt", "nested.json"]

    async def test_with_pattern_matcher(self, s3_client: tuple[Any, str]) -> None:
        """PatternFilePathMatcher filters by glob patterns."""
        client, bucket_name = s3_client
        matcher = PatternFilePathMatcher(included_patterns=["**/*.txt"])
        walker = amazon_s3.list_objects(client, bucket_name, path_matcher=matcher)
        paths: list[str] = []
        async for f in walker:
            paths.append(f.file_path.as_posix())

        assert "file1.txt" in paths
        assert "data/deep/file.txt" in paths
        assert "file2.md" not in paths

    async def test_with_excluded_patterns(self, s3_client: tuple[Any, str]) -> None:
        """Excluded patterns filter out matching files."""
        client, bucket_name = s3_client
        matcher = PatternFilePathMatcher(excluded_patterns=["**/*.bin"])
        walker = amazon_s3.list_objects(client, bucket_name, path_matcher=matcher)
        paths: list[str] = []
        async for f in walker:
            paths.append(f.file_path.as_posix())

        assert "large.bin" not in paths
        assert len(paths) == 4

    async def test_max_file_size(self, s3_client: tuple[Any, str]) -> None:
        """max_file_size skips objects exceeding the limit."""
        client, bucket_name = s3_client
        walker = amazon_s3.list_objects(client, bucket_name, max_file_size=100)
        paths: list[str] = []
        async for f in walker:
            paths.append(f.file_path.as_posix())

        assert "large.bin" not in paths
        assert len(paths) == 4

    async def test_empty_bucket(self) -> None:
        """Listing an empty bucket yields nothing."""
        async with mock_aws():
            boto3.client("s3", region_name="us-east-1").create_bucket(
                Bucket="empty-bucket"
            )

            session = aiobotocore.session.get_session()
            async with session.create_client("s3", region_name="us-east-1") as client:
                walker = amazon_s3.list_objects(client, "empty-bucket")
                files: list[amazon_s3.S3File] = []
                async for f in walker:
                    files.append(f)
                assert files == []

    async def test_directory_markers_skipped(self) -> None:
        """Keys ending with / are treated as directory markers and skipped."""
        async with mock_aws():
            sync_client = boto3.client("s3", region_name="us-east-1")
            sync_client.create_bucket(Bucket="dir-test")
            sync_client.put_object(Bucket="dir-test", Key="folder/", Body=b"")
            sync_client.put_object(
                Bucket="dir-test", Key="folder/file.txt", Body=b"content"
            )

            session = aiobotocore.session.get_session()
            async with session.create_client("s3", region_name="us-east-1") as client:
                walker = amazon_s3.list_objects(client, "dir-test")
                files: list[amazon_s3.S3File] = []
                async for f in walker:
                    files.append(f)
                assert len(files) == 1
                assert files[0].file_path.as_posix() == "folder/file.txt"

    async def test_async_items(self, s3_client: tuple[Any, str]) -> None:
        """items() async iteration yields (stable_key, S3File) pairs."""
        client, bucket_name = s3_client
        walker = amazon_s3.list_objects(client, bucket_name, prefix="data/")
        items: list[tuple[str, amazon_s3.S3File]] = []
        async for item in walker.items():
            items.append(item)

        items.sort(key=lambda x: str(x[0]))
        assert len(items) == 2
        assert items[0][0] == "deep/file.txt"
        assert items[1][0] == "nested.json"
        assert isinstance(items[0][1], amazon_s3.S3File)


# ---------------------------------------------------------------------------
# S3File (async) tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestS3File:
    """Tests for S3File (async primary type)."""

    async def test_read(self, s3_client: tuple[Any, str]) -> None:
        """await read() returns file content as bytes."""
        client, bucket_name = s3_client
        f = await amazon_s3.get_object(client, bucket_name, "data/nested.json")
        assert await f.read() == b'{"key": "value"}'

    async def test_read_text(self, s3_client: tuple[Any, str]) -> None:
        """await read_text() returns file content as text."""
        client, bucket_name = s3_client
        f = await amazon_s3.get_object(client, bucket_name, "file1.txt")
        assert await f.read_text() == "hello"

    async def test_size(self, s3_client: tuple[Any, str]) -> None:
        """size() returns the correct file size."""
        client, bucket_name = s3_client
        f = await amazon_s3.get_object(client, bucket_name, "file1.txt")
        assert await f.size() == 5

    async def test_file_path_properties(self, s3_client: tuple[Any, str]) -> None:
        """S3FilePath has correct name, suffix, etc."""
        client, bucket_name = s3_client
        f = await amazon_s3.get_object(client, bucket_name, "data/nested.json")

        assert f.file_path.name == "nested.json"
        assert f.file_path.suffix == ".json"
        assert f.file_path.stem == "nested"

    async def test_resolve_returns_full_key(self, s3_client: tuple[Any, str]) -> None:
        """resolve() returns the full S3 object key."""
        client, bucket_name = s3_client
        walker = amazon_s3.list_objects(client, bucket_name, prefix="data/")
        files: dict[str, amazon_s3.S3File] = {}
        async for f in walker:
            files[f.file_path.as_posix()] = f

        # Relative path is "nested.json" but resolve() gives full key
        assert files["nested.json"].file_path.resolve() == "data/nested.json"


# ---------------------------------------------------------------------------
# get_object() tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestGetObject:
    """Tests for get_object()."""

    async def test_get_object_returns_s3file(self, s3_client: tuple[Any, str]) -> None:
        """get_object() returns an S3File."""
        client, bucket_name = s3_client
        f = await amazon_s3.get_object(client, bucket_name, "file1.txt")
        assert isinstance(f, amazon_s3.S3File)

    async def test_get_object_stable_key(self, s3_client: tuple[Any, str]) -> None:
        """get_object() file has a stable key equal to its key path."""
        client, bucket_name = s3_client
        f = await amazon_s3.get_object(client, bucket_name, "data/nested.json")
        assert f.file_path.as_posix() == "data/nested.json"

    async def test_get_object_nonexistent_raises(
        self, s3_client: tuple[Any, str]
    ) -> None:
        """get_object() raises an error for nonexistent keys."""
        client, bucket_name = s3_client
        with pytest.raises(Exception):
            await amazon_s3.get_object(client, bucket_name, "nonexistent.txt")


# ---------------------------------------------------------------------------
# Memoization tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestMemoization:
    """Tests for memoization key and state behavior."""

    async def test_memo_key_is_path_only(self) -> None:
        """Memo key is based only on file path identity, not metadata."""
        async with mock_aws():
            sync_client = boto3.client("s3", region_name="us-east-1")
            sync_client.create_bucket(Bucket="memo-test")
            sync_client.put_object(Bucket="memo-test", Key="f.txt", Body=b"v1")

            session = aiobotocore.session.get_session()
            async with session.create_client("s3", region_name="us-east-1") as client:
                f1 = await amazon_s3.get_object(client, "memo-test", "f.txt")
                key1 = f1.__coco_memo_key__()

                # Same file path with different metadata → same memo key
                from cocoindex.resources.file import FileMetadata

                assert isinstance(f1.file_path, amazon_s3.S3FilePath)
                f2 = amazon_s3.S3File(
                    client=client,
                    file_path=f1.file_path,
                    _metadata=FileMetadata(
                        size=99,
                        modified_time=datetime(2099, 1, 1, tzinfo=timezone.utc),
                    ),
                )
                assert f1.__coco_memo_key__() == f2.__coco_memo_key__()

                # Memo key equals the file_path's memo key
                assert key1 == f1.file_path.__coco_memo_key__()

    async def test_memo_key_deterministic(self) -> None:
        """Memo key is deterministic for the same file."""
        async with mock_aws():
            sync_client = boto3.client("s3", region_name="us-east-1")
            sync_client.create_bucket(Bucket="memo-test")
            sync_client.put_object(Bucket="memo-test", Key="f.txt", Body=b"data")

            session = aiobotocore.session.get_session()
            async with session.create_client("s3", region_name="us-east-1") as client:
                f = await amazon_s3.get_object(client, "memo-test", "f.txt")
                assert f.__coco_memo_key__() == f.__coco_memo_key__()

    async def test_memo_state_first_run(self) -> None:
        """__coco_memo_state__ computes initial state on first run."""
        import cocoindex

        async with mock_aws():
            sync_client = boto3.client("s3", region_name="us-east-1")
            sync_client.create_bucket(Bucket="memo-state-test")
            sync_client.put_object(Bucket="memo-state-test", Key="f.txt", Body=b"hello")

            session = aiobotocore.session.get_session()
            async with session.create_client("s3", region_name="us-east-1") as client:
                f = await amazon_s3.get_object(client, "memo-state-test", "f.txt")
                outcome = await f.__coco_memo_state__(cocoindex.NON_EXISTENCE)

                assert isinstance(outcome, cocoindex.MemoStateOutcome)
                assert outcome.memo_valid is True
                assert isinstance(outcome.state, tuple)
                assert len(outcome.state) == 2

    async def test_memo_state_unchanged(self) -> None:
        """__coco_memo_state__ returns valid when mtime matches."""
        import cocoindex

        async with mock_aws():
            sync_client = boto3.client("s3", region_name="us-east-1")
            sync_client.create_bucket(Bucket="memo-state-test2")
            sync_client.put_object(
                Bucket="memo-state-test2", Key="f.txt", Body=b"hello"
            )

            session = aiobotocore.session.get_session()
            async with session.create_client("s3", region_name="us-east-1") as client:
                f = await amazon_s3.get_object(client, "memo-state-test2", "f.txt")

                # Get initial state
                outcome1 = await f.__coco_memo_state__(cocoindex.NON_EXISTENCE)

                # Same file, same state → valid
                f2 = await amazon_s3.get_object(client, "memo-state-test2", "f.txt")
                outcome2 = await f2.__coco_memo_state__(outcome1.state)
                assert outcome2.memo_valid is True

    async def test_file_path_memo_key(self, s3_client: tuple[Any, str]) -> None:
        """S3FilePath.__coco_memo_key__() incorporates bucket and path."""
        client, bucket_name = s3_client
        walker = amazon_s3.list_objects(client, bucket_name)
        files: list[amazon_s3.S3File] = []
        async for f in walker:
            files.append(f)

        # All file paths should have distinct memo keys
        memo_keys = {f.file_path.__coco_memo_key__() for f in files}
        assert len(memo_keys) == len(files)

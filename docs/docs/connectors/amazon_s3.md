---
title: Amazon S3
toc_max_heading_level: 4
description: CocoIndex connector for reading objects from Amazon S3 buckets and S3-compatible services.
---

# Amazon S3

The `amazon_s3` connector provides utilities for reading objects from Amazon S3 buckets and S3-compatible services (e.g. MinIO).

```python
from cocoindex.connectors import amazon_s3
```

:::note Installation
This connector requires the `aiobotocore` library. Install with:

```bash
pip install cocoindex[amazon_s3]
```
:::

## As source

The connector provides two ways to read from S3:

- `list_objects()` — List and iterate over objects in a bucket (with optional prefix and filtering)
- `get_object()` — Fetch a single object by its key

Both require an aiobotocore S3 client, which you create and manage yourself:

```python
import aiobotocore.session

session = aiobotocore.session.get_session()
async with session.create_client("s3") as client:
    # Use client with list_objects() or get_object()
    ...

# For S3-compatible services:
async with session.create_client("s3", endpoint_url="http://localhost:9000") as client:
    ...
```

### list_objects

List objects in an S3 bucket. Returns an `S3Walker` that supports async iteration.

```python
def list_objects(
    client: AioBaseClient,
    bucket_name: str,
    *,
    prefix: str = "",
    path_matcher: FilePathMatcher | None = None,
    max_file_size: int | None = None,
) -> S3Walker
```

**Parameters:**

- `client` — An aiobotocore S3 client.
- `bucket_name` — The S3 bucket name.
- `prefix` — Only list objects whose key starts with this prefix. The prefix is stripped from relative paths in the returned files.
- `path_matcher` — Optional filter for files. Patterns are matched against the relative path (after prefix stripping). See [PatternFilePathMatcher](../resource_types.md#patternfilepathmatcher).
- `max_file_size` — Skip objects larger than this size in bytes.

**Returns:** An `S3Walker` that can be used with `async for` loops.

### Iterating files

`list_objects()` returns an `S3Walker` that yields `S3File` objects (implementing the `AsyncFileLike` protocol):

```python
import aiobotocore.session
from cocoindex.connectors import amazon_s3

session = aiobotocore.session.get_session()
async with session.create_client("s3") as client:
    async for file in amazon_s3.list_objects(client, "my-bucket", prefix="data/"):
        text = await file.read_text()
        ...
```

See [`AsyncFileLike`](../resource_types.md#filelike--asyncfilelike) for details on the file objects.

### Keyed iteration with `items()`

`S3Walker.items()` yields `(str, S3File)` pairs, useful for associating each file with a stable string key (its relative path):

```python
async for key, file in amazon_s3.list_objects(client, "my-bucket").items():
    content = await file.read()
```

### Filtering files

Use `PatternFilePathMatcher` to filter which objects are included. Patterns are matched against the relative path (after prefix stripping):

```python
from cocoindex.connectors import amazon_s3
from cocoindex.resources.file import PatternFilePathMatcher

matcher = PatternFilePathMatcher(included_patterns=["**/*.json"])

async for file in amazon_s3.list_objects(client, "my-bucket", prefix="data/", path_matcher=matcher):
    process(file)
```

### Limiting file size

Use `max_file_size` to skip objects that exceed a size threshold:

```python
# Skip objects larger than 10 MB
async for file in amazon_s3.list_objects(client, "my-bucket", max_file_size=10 * 1024 * 1024):
    process(file)
```

### get_object

Fetch a single object from an S3 bucket by its key.

```python
async def get_object(
    client: AioBaseClient,
    bucket_name_or_uri: str,
    key: str | None = None,
) -> S3File
```

**Parameters:**

- `client` — An aiobotocore S3 client.
- `bucket_name_or_uri` — Either a full S3 URI (`s3://bucket/key`) or the bucket name when `key` is supplied separately.
- `key` — The full S3 object key. Required when `bucket_name_or_uri` is a bucket name; must be omitted when a URI is given.

**Returns:** An `S3File` (AsyncFileLike) for the specified object.

**Example:**

```python
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
```

### S3FilePath

Each file returned by the connector has an `S3FilePath` — a [`FilePath`](../resource_types.md#filepath) specialized for S3:

- **Relative path** (`file.file_path.path`) — The object key relative to the walker prefix (or the full key if no prefix was used).
- **Resolved path** (`file.file_path.resolve()`) — The full S3 object key.

For example, with `prefix="data/"` and an object key `"data/docs/readme.md"`:
- `file.file_path.path` → `PurePath("docs/readme.md")`
- `file.file_path.resolve()` → `"data/docs/readme.md"`

### Example

```python
import aiobotocore.session
import cocoindex as coco
from cocoindex.connectors import amazon_s3
from cocoindex.resources.file import AsyncFileLike, PatternFilePathMatcher

@coco.function
async def app_main(bucket: str) -> None:
    session = aiobotocore.session.get_session()
    async with session.create_client("s3") as client:
        matcher = PatternFilePathMatcher(included_patterns=["**/*.md"])

        walker = amazon_s3.list_objects(
            client, bucket, prefix="docs/", path_matcher=matcher,
        )

        with coco.component_subpath("file"):
            async for key, file in walker.items():
                coco.mount(
                    coco.component_subpath(key),
                    process_file,
                    file,
                )

@coco.function(memo=True)
async def process_file(file: AsyncFileLike[str]) -> None:
    text = await file.read_text()
    # ... process the file content ...
```

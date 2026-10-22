# MinIO notifications into a live file index

Run an authenticated webhook receiver alongside a CocoIndex v1 app. The app
scans an S3-compatible bucket, then reconciles MinIO create and remove
notifications into local JSON records. Every record contains the original
bucket/key, content length, and SHA-256 digest. Deleting an object removes its
derived record. No model, vector database, or cloud account is required.

The source uses `amazon_s3.list_objects(...).live_items(stream)`. Ordinary
`walker.items()` remains a one-shot async iterator. This example requires a
CocoIndex checkout containing that new method; install the current checkout
until the feature is released.

## Run

Use an existing local MinIO instance and a disposable bucket. Choose a random,
nonempty `WEBHOOK_TOKEN` without whitespace and export it in the MinIO process
environment. Use the same value in the receiver's `.env` file. Configure the
webhook target **before starting MinIO**:

```sh
export MINIO_NOTIFY_WEBHOOK_ENABLE_COCO=on
export MINIO_NOTIFY_WEBHOOK_ENDPOINT_COCO=http://127.0.0.1:8000/events
export MINIO_NOTIFY_WEBHOOK_AUTH_TOKEN_COCO="$WEBHOOK_TOKEN"
export MINIO_NOTIFY_WEBHOOK_QUEUE_DIR_COCO=/absolute/path/to/minio-notification-queue
export MINIO_NOTIFY_WEBHOOK_QUEUE_LIMIT_COCO=10000
```

The queue directory belongs to MinIO and must be writable and persistent. With Docker, use an address reachable **from the MinIO
container**, such as `http://host.docker.internal:8000/events` on Docker Desktop,
and bind the example to an appropriate interface. `127.0.0.1` inside a container
refers to that container. Use HTTPS when crossing a trusted local boundary;
the receiver accepts a bearer token, so plain HTTP does not protect it in transit.

From a repository checkout:

```sh
cd examples/minio_webhook
cp .env.example .env
# Set AWS credentials, S3_BUCKET, and WEBHOOK_TOKEN in .env.
# Install CocoIndex from ../../ using the repository's development setup,
# then install this example's dependencies into that same environment:
pip install -e .
python main.py
```

Use one Uvicorn process, without `--reload` or multiple workers. Its ASGI
lifespan owns one CocoIndex environment, S3 client, and live watcher. The server
starts receiving while the initial scan runs; accepted object notifications
wait for that scan and their own target completion before receiving HTTP 204.

Configure notifications for the bucket using an existing `mc` alias:

```sh
mc event add local/cocoindex-events arn:minio:sqs::COCO:webhook --event put,delete
printf 'first version\n' > sample.txt
mc cp sample.txt local/cocoindex-events/sample.txt
cat output/*.json
printf 'second version\n' > sample.txt
mc cp sample.txt local/cocoindex-events/sample.txt
cat output/*.json
mc rm local/cocoindex-events/sample.txt
ls output
```

The endpoint and token follow MinIO's
[webhook target configuration](https://docs.min.io/aistor/administration/bucket-notifications/publish-events-to-webhook/).
The notification ARN's target ID must match `COCO` above. Configure both create
and remove events. S3 access needs list, HEAD/read object permissions for the
selected bucket/prefix. The example never modifies source objects.

Output names hash the **original bucket and key**, so keys containing Unicode,
spaces, `+`, or path separators are preserved in JSON and cannot choose a local
output path. `MAX_FILE_SIZE` defaults to 16 MiB; oversized or newly excluded
objects lose their derived row. The source's existing relative-path identity
and path matching rules also apply to live updates.

## Delivery and failure behavior

- Notifications are hints to read the **current object**, not a historical
  version. Duplicate and out-of-order events converge through HEAD/read. A
  delete notification for an object that has been recreated keeps the current
  object. Different object keys can proceed independently; updates for one key
  hold their ordering gate through target completion.
- An object HEAD returning 404 deletes its derived state. Permission failures,
  throttling, network failures, and other server errors fail the delivery; they
  do not masquerade as deletion. The example installs a propagating CocoIndex
  exception handler, so a failed transform cannot be acknowledged as success.
- Authentication is checked before reading the body. Each of at most eight
  admitted requests owns its capacity slot through body reading and target
  completion, even if the HTTP waiter disconnects. Bodies are limited to 1 MiB,
  batches to 64 records, and body reads to ten seconds. Saturation returns 429;
  oversize requests return 413. The whole envelope is validated before any
  record is processed, but a multi-record delivery is not a target transaction.
  The initial scan also limits concurrent component work to eight.
- The receiver keeps no durable inbox. Configure MinIO's persistent notification
  queue and retry behavior. HTTP 204 means the submitted reconciliation completed
  with the example's error policy, not that an event was durably stored by the
  webhook. Failed or disconnected deliveries must be retried by the producer.
  An already committed earlier record may be processed again after a later
  record in the same batch fails.
- Every startup scans the bucket again, covering changes made while the receiver
  was offline. Recovery is eventual; lost notifications are not repaired during
  an uninterrupted run. Stop and restart the example to force reconciliation.
  There is no exactly-once or historical-event processing guarantee.

For AWS S3, supply a `LiveStream[bytes]` from an appropriate notification
transport. AWS bucket notifications do not directly POST arbitrary webhooks.
The connector accepts the AWS S3 `Records` envelope and MinIO's equivalent;
this runnable HTTP example is specifically for MinIO.

The same transport can feed a transform that chunks, embeds, or writes to a
database by replacing `index_file`. Its acknowledgement depends on the target's
completion semantics and the configured exception policy.

## Run the integration test

After completing the repository development setup, run from its root:

```sh
uv run pytest python/tests/connectors/test_minio_webhook_integration.py
```

The test starts a disposable MinIO server and a real HTTP receiver, checks the
output at each HTTP acknowledgement, and exercises create, overwrite, duplicate
delivery, delete, and restart recovery. It downloads and caches a checksum-pinned
official MinIO release executable on supported platforms. Set
`COCOINDEX_TEST_MINIO_BINARY` to use an existing trusted executable instead.

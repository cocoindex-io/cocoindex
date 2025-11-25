---
title: AmazonS3
toc_max_heading_level: 4
description: CocoIndex AmazonS3 Built-in Sources
---

### Setup for Amazon S3

#### Setup AWS accounts

You need to setup AWS accounts to own and access Amazon S3. In particular,

*   Setup an AWS account from [AWS homepage](https://aws.amazon.com/) or login with an existing account.
*   AWS recommends all programming access to AWS should be done using [IAM users](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users.html) instead of root account. You can create an IAM user at [AWS IAM Console](https://console.aws.amazon.com/iam/home).
*   Make sure your IAM user at least have the following permissions in the IAM console:
    *   Attach permission policy `AmazonS3ReadOnlyAccess` for read-only access to Amazon S3.
    *   (optional) Attach permission policy `AmazonSQSFullAccess` to receive notifications from Amazon SQS, if you want to enable change event notifications.
        Note that `AmazonSQSReadOnlyAccess` is not enough, as we need to be able to delete messages from the queue after they're processed.


#### Setup Credentials for AWS SDK

AWS SDK needs to access credentials to access Amazon S3.
The easiest way to setup credentials is to run:

```sh
aws configure
```

It will create a credentials file at `~/.aws/credentials` and config at `~/.aws/config`.

See the following documents if you need more control:

*   [`aws configure`](https://docs.aws.amazon.com/cli/v1/userguide/cli-configure-files.html)
*   [Globally configuring AWS SDKs and tools](https://docs.aws.amazon.com/sdkref/latest/guide/creds-config-files.html)


#### Create Amazon S3 buckets

You can create a Amazon S3 bucket in the [Amazon S3 Console](https://s3.console.aws.amazon.com/s3/home), and upload your files to it.

It's also doable by using the AWS CLI `aws s3 mb` (to create buckets) and `aws s3 cp` (to upload files).
When doing so, make sure your current user also has permission policy `AmazonS3FullAccess`.

#### (Optional) Setup SQS queue for event notifications

You can setup an Amazon Simple Queue Service (Amazon SQS) queue to receive change event notifications from Amazon S3.
It provides a change capture mechanism for your AmazonS3 data source, to trigger reprocessing of your AWS S3 files on any creation, update or deletion.  Please use a dedicated SQS queue for each of your S3 data source.

This is how to setup:

*   Create a SQS queue with proper access policy.
    *   In the [Amazon SQS Console](https://console.aws.amazon.com/sqs/home), create a queue.
    *   Add access policy statements, to make sure Amazon S3 can send messages to the queue.
        ```json
        {
          ...
          "Statement": [
            ...
            {
              "Sid": "__publish_statement",
              "Effect": "Allow",
              "Principal": {
                "Service": "s3.amazonaws.com"
              },
              "Resource": "${SQS_QUEUE_ARN}",
              "Action": "SQS:SendMessage",
              "Condition": {
                "ArnLike": {
                  "aws:SourceArn": "${S3_BUCKET_ARN}"
                }
              }
            }
          ]
        }
        ```

        Here, you need to replace `${SQS_QUEUE_ARN}` and `${S3_BUCKET_ARN}` with the actual ARN of your SQS queue and S3 bucket.
        You can find the ARN of your SQS queue in the existing policy statement (it starts with `arn:aws:sqs:`), and the ARN of your S3 bucket in the S3 console (it starts with `arn:aws:s3:`).

*   In the [Amazon S3 Console](https://s3.console.aws.amazon.com/s3/home), open your S3 bucket. Under *Properties* tab, click *Create event notification*.
    *   Fill in an arbitrary event name, e.g. `S3ChangeNotifications`.
    *   If you want your AmazonS3 data source to expose a subset of files sharing a prefix, set the same prefix here. Otherwise, leave it empty.
    *   Select the following event types: *All object create events*, *All object removal events*.
    *   Select *SQS queue* as the destination, and specify the SQS queue you created above.

AWS's [Guide of Configuring a Bucket for Notifications](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ways-to-add-notification-config-to-bucket.html#step1-create-sqs-queue-for-notification) provides more details.

#### (Alternative) Setup Redis for event notifications (MinIO)

For MinIO setups that don't use AWS SQS, you can configure MinIO to publish event notifications to Redis:

*   Configure MinIO to publish events to Redis by setting environment variables:
    ```sh
    export MINIO_NOTIFY_REDIS_ENABLE="on"
    export MINIO_NOTIFY_REDIS_ADDRESS="redis-endpoint.example.net:6379"
    export MINIO_NOTIFY_REDIS_KEY="bucketevents"
    export MINIO_NOTIFY_REDIS_FORMAT="namespace"
    ```
    Replace the values with your Redis server details.

*   Alternatively, use the `mc` command-line tool:
    ```sh
    mc alias set myminio http://minio.example.com:9000 ACCESSKEY SECRETKEY
    mc admin config set myminio/ notify_redis \
      address="redis-endpoint.example.net:6379" \
      key="bucketevents" \
      format="namespace"
    mc admin service restart myminio
    ```

*   Ensure your Redis server is accessible and configured to accept connections from MinIO.

MinIO's [Redis Notification Settings](https://min.io/docs/minio/linux/reference/minio-server/settings/notifications/redis.html) documentation provides more details on configuration options.

### Spec

The spec takes the following fields:
*   `bucket_name` (`str`): Amazon S3 bucket name.
*   `prefix` (`str`, optional): if provided, only files with path starting with this prefix will be imported.
*   `binary` (`bool`, optional): whether reading files as binary (instead of text).
*   `included_patterns` (`list[str]`, optional): a list of glob patterns to include files, e.g. `["*.txt", "docs/**/*.md"]`.
    If not specified, all files will be included.
*   `excluded_patterns` (`list[str]`, optional): a list of glob patterns to exclude files, e.g. `["*.tmp", "**/*.log"]`.
    Any file or directory matching these patterns will be excluded even if they match `included_patterns`.
    If not specified, no files will be excluded.

    :::info

    `included_patterns` and `excluded_patterns` are using Unix-style glob syntax. See [globset syntax](https://docs.rs/globset/latest/globset/index.html#syntax) for the details.

    :::

*   `max_file_size` (`int`, optional): if provided, files exceeding this size in bytes will be treated as non-existent and skipped during processing.
    This is useful to avoid processing large files that are not relevant to your use case, such as videos or backups.
    If not specified, no size limit is applied.
*   `sqs_queue_url` (`str`, optional): if provided, the source will receive change event notifications from Amazon S3 via this SQS queue.

    :::info

    We will delete messages from the queue after they're processed.
    If there are unrelated messages in the queue (e.g. test messages that SQS will send automatically on queue creation, messages for a different bucket, for non-included files, etc.), we will delete the message upon receiving it, to avoid repeatedly receiving irrelevant messages after they're redelivered.

    :::

*   `redis_url` (`str`, optional): if provided, the source will receive change event notifications via Redis pub/sub. This is particularly useful for MinIO setups that publish events to Redis instead of SQS.

*   `redis_channel` (`str`, optional): the Redis channel to subscribe to for event notifications. Required when `redis_url` is provided.

    :::info

    Redis pub/sub is preferred over SQS when both are configured. This allows MinIO users to receive S3-compatible event notifications without requiring AWS SQS.
    The Redis implementation expects S3 event notifications in the same JSON format as SQS messages.

    :::

### Schema

The output is a [*KTable*](/docs/core/data_types#ktable) with the following sub fields:

*   `filename` (*Str*, key): the filename of the file, including the path, relative to the root directory, e.g. `"dir1/file1.md"`.
*   `content` (*Str* if `binary` is `False`, otherwise *Bytes*): the content of the file.

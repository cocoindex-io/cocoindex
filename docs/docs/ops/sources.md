---
title: Sources
toc_max_heading_level: 4
description: CocoIndex Built-in Sources
---

# CocoIndex Built-in Sources

## LocalFile

The `LocalFile` source imports files from a local file system.

### Spec

The spec takes the following fields:
*   `path` (type: `str`, required): full path of the root directory to import files from
*   `binary` (type: `bool`, optional): whether reading files as binary (instead of text)
*   `included_patterns` (type: `list[str]`, optional): a list of glob patterns to include files, e.g. `["*.txt", "docs/**/*.md"]`.
    If not specified, all files will be included.
*   `excluded_patterns` (type: `list[str]`, optional): a list of glob patterns to exclude files, e.g. `["tmp", "**/node_modules"]`.
    Any file or directory matching these patterns will be excluded even if they match `included_patterns`.
    If not specified, no files will be excluded.

    :::info

    `included_patterns` and `excluded_patterns` are using Unix-style glob syntax. See [globset syntax](https://docs.rs/globset/latest/globset/index.html#syntax) for the details.

    :::

### Schema

The output is a [KTable](/docs/core/data_types#ktable) with the following sub fields:
*   `filename` (key, type: `str`): the filename of the file, including the path, relative to the root directory, e.g. `"dir1/file1.md"`
*   `content` (type: `str` if `binary` is `False`, otherwise `bytes`): the content of the file

## AmazonS3

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
    *   If you want your AmazonS3 data source expose a subset of files sharing a prefix, set the same prefix here. Otherwise, leave it empty.
    *   Select the following event types: *All object create events*, *All object removal events*.
    *   Select *SQS queue* as the destination, and specify the SQS queue you created above.
and enable *Change Event Notifications* for your bucket, and specify the SQS queue as the destination.

AWS's [Guide of Configuring a Bucket for Notifications](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ways-to-add-notification-config-to-bucket.html#step1-create-sqs-queue-for-notification) provides more details.

### Spec

The spec takes the following fields:
*   `bucket_name` (type: `str`, required): Amazon S3 bucket name.
*   `prefix` (type: `str`, optional): if provided, only files with path starting with this prefix will be imported.
*   `binary` (type: `bool`, optional): whether reading files as binary (instead of text).
*   `included_patterns` (type: `list[str]`, optional): a list of glob patterns to include files, e.g. `["*.txt", "docs/**/*.md"]`.
    If not specified, all files will be included.
*   `excluded_patterns` (type: `list[str]`, optional): a list of glob patterns to exclude files, e.g. `["*.tmp", "**/*.log"]`.
    Any file or directory matching these patterns will be excluded even if they match `included_patterns`.
    If not specified, no files will be excluded.

    :::info

    `included_patterns` and `excluded_patterns` are using Unix-style glob syntax. See [globset syntax](https://docs.rs/globset/latest/globset/index.html#syntax) for the details.

    :::

*   `sqs_queue_url` (type: `str`, optional): if provided, the source will receive change event notifications from Amazon S3 via this SQS queue.

    :::info

    We will delete messages from the queue after they're processed.
    If there're unrelated messages in the queue (e.g. test messages that SQS will send automatically on queue creation, messages for a different bucket, for non-included files, etc.), we will delete the message upon receiving it, to avoid keeping receiving irrelevant messages again and again after they're redelivered.

    :::

### Schema

The output is a [KTable](/docs/core/data_types#ktable) with the following sub fields:
*   `filename` (key, type: `str`): the filename of the file, including the path, relative to the root directory, e.g. `"dir1/file1.md"`.
*   `content` (type: `str` if `binary` is `False`, otherwise `bytes`): the content of the file.


## GoogleDrive

The `GoogleDrive` source imports files from Google Drive.

### Setup for Google Drive

To access files in Google Drive, the `GoogleDrive` source will need to authenticate by service accounts.

1.  Register / login in **Google Cloud**.
2.  In [**Google Cloud Console**](https://console.cloud.google.com/), search for *Service Accounts*, to enter the *IAM & Admin / Service Accounts* page.
    -   **Create a new service account**: Click *+ Create Service Account*. Follow the instructions to finish service account creation.
    -   **Add a key and download the credential**: Under "Actions" for this new service account, click *Manage keys* → *Add key* → *Create new key* → *JSON*.
      Download the key file to a safe place.
3.  In **Google Cloud Console**, search for *Google Drive API*. Enable this API.
4.  In **Google Drive**, share the folders containing files that need to be imported through your source with the service account's email address.
    **Viewer permission** is sufficient.
    -   The email address can be found under the *IAM & Admin / Service Accounts* page (in Step 2), in the format of `{service-account-id}@{gcp-project-id}.iam.gserviceaccount.com`.
    -   Copy the folder ID. Folder ID can be found from the last part of the folder's URL, e.g. `https://drive.google.com/drive/u/0/folders/{folder-id}` or `https://drive.google.com/drive/folders/{folder-id}?usp=drive_link`.


### Spec

The spec takes the following fields:

*   `service_account_credential_path` (type: `str`, required): full path to the service account credential file in JSON format.
*   `root_folder_ids` (type: `list[str]`, required): a list of Google Drive folder IDs to import files from.
*   `binary` (type: `bool`, optional): whether reading files as binary (instead of text).
*   `recent_changes_poll_interval` (type: `datetime.timedelta`, optional): when set, this source provides a change capture mechanism by polling Google Drive for recent modified files periodically.

    :::info

    Since it only retrieves metadata for recent modified files (up to the previous poll) during polling,
    it's typically cheaper than a full refresh by setting the [refresh interval](../core/flow_def#refresh-interval) especially when the folder contains a large number of files.
    So you can usually set it with a smaller value compared to the `refresh_interval`.

    On the other hand, this only detects changes for files still exists.
    If the file is deleted (or the current account no longer has access to), this change will not be detected by this change stream.

    So when a `GoogleDrive` source enabled `recent_changes_poll_interval`, it's still recommended to set a `refresh_interval`, with a larger value.
    So that most changes can be covered by polling recent changes (with low latency, like 10 seconds), and remaining changes (files no longer exist or accessible) will still be covered (with a higher latency, like 5 minutes, and should be larger if you have a huge number of files like 1M).
    In reality, configure them based on your requirement: how freshness do you need to target index to be?

    :::

### Schema

The output is a [KTable](/docs/core/data_types#ktable) with the following sub fields:

*   `file_id` (key, type: `str`): the ID of the file in Google Drive.
*   `filename` (type: `str`): the filename of the file, without the path, e.g. `"file1.md"`
*   `mime_type` (type: `str`): the MIME type of the file.
*   `content` (type: `str` if `binary` is `False`, otherwise `bytes`): the content of the file.

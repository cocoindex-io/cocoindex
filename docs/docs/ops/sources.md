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
*   `path` (`str`): full path of the root directory to import files from
*   `binary` (`bool`, optional): whether reading files as binary (instead of text)
*   `included_patterns` (`list[str]`, optional): a list of glob patterns to include files, e.g. `["*.txt", "docs/**/*.md"]`.
    If not specified, all files will be included.
*   `excluded_patterns` (`list[str]`, optional): a list of glob patterns to exclude files, e.g. `["tmp", "**/node_modules"]`.
    Any file or directory matching these patterns will be excluded even if they match `included_patterns`.
    If not specified, no files will be excluded.

    :::info

    `included_patterns` and `excluded_patterns` are using Unix-style glob syntax. See [globset syntax](https://docs.rs/globset/latest/globset/index.html#syntax) for the details.

    :::

### Schema

The output is a [*KTable*](/docs/core/data_types#ktable) with the following sub fields:
*   `filename` (*Str*, key): the filename of the file, including the path, relative to the root directory, e.g. `"dir1/file1.md"`
*   `content` (*Str* if `binary` is `False`, *Bytes* otherwise): the content of the file

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
    *   If you want your AmazonS3 data source to expose a subset of files sharing a prefix, set the same prefix here. Otherwise, leave it empty.
    *   Select the following event types: *All object create events*, *All object removal events*.
    *   Select *SQS queue* as the destination, and specify the SQS queue you created above.

AWS's [Guide of Configuring a Bucket for Notifications](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ways-to-add-notification-config-to-bucket.html#step1-create-sqs-queue-for-notification) provides more details.

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

*   `sqs_queue_url` (`str`, optional): if provided, the source will receive change event notifications from Amazon S3 via this SQS queue.

    :::info

    We will delete messages from the queue after they're processed.
    If there are unrelated messages in the queue (e.g. test messages that SQS will send automatically on queue creation, messages for a different bucket, for non-included files, etc.), we will delete the message upon receiving it, to avoid repeatedly receiving irrelevant messages after they're redelivered.

    :::

### Schema

The output is a [*KTable*](/docs/core/data_types#ktable) with the following sub fields:

*   `filename` (*Str*, key): the filename of the file, including the path, relative to the root directory, e.g. `"dir1/file1.md"`.
*   `content` (*Str* if `binary` is `False`, otherwise *Bytes*): the content of the file.


## AzureBlob

The `AzureBlob` source imports files from Azure Blob Storage.

### Setup for Azure Blob Storage

#### Get Started

If you didn't have experience with Azure Blob Storage, you can refer to the [quickstart](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal).
These are actions you need to take:

*   Create a storage account in the [Azure Portal](https://portal.azure.com/).
*   Create a container in the storage account.
*   Upload your files to the container.
*   Grant the user / identity / service principal (depends on your authentication method, see below) access to the storage account. At minimum, a **Storage Blob Data Reader** role is needed. See [this doc](https://learn.microsoft.com/en-us/azure/storage/blobs/authorize-data-operations-portal) for reference.

#### Authentication

We support the following authentication methods:

*   Shared access signature (SAS) tokens.
    You can generate it from the Azure Portal in the settings for a specific container.
    You need to provide at least *List* and *Read* permissions when generating the SAS token.
    It's a query string in the form of
    `sp=rl&st=2025-07-20T09:33:00Z&se=2025-07-19T09:48:53Z&sv=2024-11-04&sr=c&sig=i3FDjsadfklj3%23adsfkk`.

*   Storage account access key. You can find it in the Azure Portal in the settings for a specific storage account.

*   Default credential. When none of the above is provided, it will use the default credential.

    This allows you to connect to Azure services without putting any secrets in the code or flow spec.
    It automatically chooses the best authentication method based on your environment:

    *   On your local machine: uses your Azure CLI login (`az login`) or environment variables.

        ```sh
        az login
        # Optional: Set a default subscription if you have more than one
        az account set --subscription "<YOUR_SUBSCRIPTION_NAME_OR_ID>"
        ```
    *   In Azure (VM, App Service, AKS, etc.): uses the resource’s Managed Identity.
    *   In automated environments: supports Service Principals via environment variables
        *   `AZURE_CLIENT_ID`
        *   `AZURE_TENANT_ID`
        *   `AZURE_CLIENT_SECRET`

You can refer to [this doc](https://learn.microsoft.com/en-us/azure/developer/python/sdk/authentication/overview) for more details.

### Spec

The spec takes the following fields:

*   `account_name` (`str`): the name of the storage account.
*   `container_name` (`str`): the name of the container.
*   `prefix` (`str`, optional): if provided, only files with path starting with this prefix will be imported.
*   `binary` (`bool`, optional): whether reading files as binary (instead of text).
*   `included_patterns` (`list[str]`, optional): a list of glob patterns to include files, e.g. `["*.txt", "docs/**/*.md"]`.
    If not specified, all files will be included.
*   `excluded_patterns` (`list[str]`, optional): a list of glob patterns to exclude files, e.g. `["*.tmp", "**/*.log"]`.
    Any file or directory matching these patterns will be excluded even if they match `included_patterns`.
    If not specified, no files will be excluded.
*   `sas_token` (`cocoindex.TransientAuthEntryReference[str]`, optional): a SAS token for authentication.
*   `account_access_key` (`cocoindex.TransientAuthEntryReference[str]`, optional): an account access key for authentication.

    :::info

    `included_patterns` and `excluded_patterns` are using Unix-style glob syntax. See [globset syntax](https://docs.rs/globset/latest/globset/index.html#syntax) for the details.

    :::

### Schema

The output is a [*KTable*](/docs/core/data_types#ktable) with the following sub fields:

*   `filename` (*Str*, key): the filename of the file, including the path, relative to the root directory, e.g. `"dir1/file1.md"`.
*   `content` (*Str* if `binary` is `False`, otherwise *Bytes*): the content of the file.


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

*   `service_account_credential_path` (`str`): full path to the service account credential file in JSON format.
*   `root_folder_ids` (`list[str]`): a list of Google Drive folder IDs to import files from.
*   `binary` (`bool`, optional): whether reading files as binary (instead of text).
*   `recent_changes_poll_interval` (`datetime.timedelta`, optional): when set, this source provides a change capture mechanism by polling Google Drive for recent modified files periodically.

    :::info

    Since it only retrieves metadata for recent modified files (up to the previous poll) during polling,
    it's typically cheaper than a full refresh by setting the [refresh interval](../core/flow_def#refresh-interval) especially when the folder contains a large number of files.
    So you can usually set it with a smaller value compared to the `refresh_interval`.

    On the other hand, this only detects changes for files that still exist.
    If the file is deleted (or the current account no longer has access to it), this change will not be detected by this change stream.

    So when a `GoogleDrive` source has `recent_changes_poll_interval` enabled, it's still recommended to set a `refresh_interval`, with a larger value.
    So that most changes can be covered by polling recent changes (with low latency, like 10 seconds), and remaining changes (files no longer exist or accessible) will still be covered (with a higher latency, like 5 minutes, and should be larger if you have a huge number of files like 1M).
    In reality, configure them based on your requirement: how fresh do you need the target index to be?

    :::

### Schema

The output is a [*KTable*](/docs/core/data_types#ktable) with the following sub fields:

*   `file_id` (*Str*, key): the ID of the file in Google Drive.
*   `filename` (*Str*): the filename of the file, without the path, e.g. `"file1.md"`
*   `mime_type` (*Str*): the MIME type of the file.
*   `content` (*Str* if `binary` is `False`, otherwise *Bytes*): the content of the file.


## Postgres

The `Postgres` source imports rows from a PostgreSQL table.

### Setup for PostgreSQL

*   Ensure the table exists and has a primary key. Tables without a primary key are not supported.
*   Grant the connecting user read permissions on the target table (e.g. `SELECT`).
*   Provide a database connection. You can:
    *   Use CocoIndex's default database connection, or
    *   Provide an explicit connection via a transient auth entry referencing a `DatabaseConnectionSpec` with a `url`, for example:

        ```python
        cocoindex.add_transient_auth_entry(
            cocoindex.sources.DatabaseConnectionSpec(
                url="postgres://user:password@host:5432/dbname?sslmode=require",
            )
        )
        ```

### Spec

The spec takes the following fields:

*   `table_name` (`str`): the PostgreSQL table to read from.
*   `database` (`cocoindex.TransientAuthEntryReference[DatabaseConnectionSpec]`, optional): database connection reference. If not provided, the default CocoIndex database is used.
*   `included_columns` (`list[str]`, optional): non-primary-key columns to include. If not specified, all non-PK columns are included.
*   `ordinal_column` (`str`, optional): to specify a non-primary-key column used for change tracking and ordering, e.g. can be a modified timestamp or a monotonic version number. Supported types are integer-like (`bigint`/`integer`) and timestamps (`timestamp`, `timestamptz`).
    `ordinal_column` must not be a primary key column.
*   `notification` (`cocoindex.sources.PostgresNotification`, optional): when present, enable change capture based on Postgres LISTEN/NOTIFY. It has the following fields:
    *   `channel_name` (`str`, optional): the Postgres notification channel to listen on. CocoIndex will automatically create the channel with the given name. If omitted, CocoIndex uses `{flow_name}__{source_name}__cocoindex`.

    :::info

    If `notification` is provided, CocoIndex listens for row changes using Postgres LISTEN/NOTIFY and creates the required database objects on demand when the flow starts listening:

    - Function to create notification message: `{channel_name}_n`.
    - Trigger to react to table changes: `{channel_name}_t` on the specified `table_name`.

    Creation is automatic when listening begins.

    Currently CocoIndex doesn't automatically clean up these objects when the flow is dropped (unlike targets)
    It's usually OK to leave them as they are, but if you want to clean them up, you can run the following SQL statements to manually drop them:

    ```sql
    DROP TRIGGER IF EXISTS {channel_name}_t ON "{table_name}";
    DROP FUNCTION IF EXISTS {channel_name}_n();
    ```

    :::

### Schema

The output is a [*KTable*](/docs/core/data_types#ktable) with straightforward 1 to 1 mapping from Postgres table columns to CocoIndex table fields:

*   Key fields: All primary key columns in the Postgres table will be included automatically as key fields.
*   Value fields: All non-primary-key columns in the Postgres table (included by `included_columns` or all when not specified) appear as value fields.

### Example

You can find end-to-end example using Postgres source at:
*   [examples/postgres_source](https://github.com/cocoindex-io/cocoindex/tree/main/examples/postgres_source)

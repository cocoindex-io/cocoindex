---
title: AzureBlob
toc_max_heading_level: 4
description: CocoIndex AzureBlob Built-in Sources
---

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

    :::info

    `included_patterns` and `excluded_patterns` are using Unix-style glob syntax. See [globset syntax](https://docs.rs/globset/latest/globset/index.html#syntax) for the details.

    :::

*   `max_file_size` (`int`, optional): if provided, files exceeding this size in bytes will be treated as non-existent and skipped during processing.
    This is useful to avoid processing large files that are not relevant to your use case, such as videos or backups.
    If not specified, no size limit is applied.
*   `sas_token` (`cocoindex.TransientAuthEntryReference[str]`, optional): a SAS token for authentication.
*   `account_access_key` (`cocoindex.TransientAuthEntryReference[str]`, optional): an account access key for authentication.

### Schema

The output is a [*KTable*](/docs/core/data_types#ktable) with the following sub fields:

*   `filename` (*Str*, key): the filename of the file, including the path, relative to the root directory, e.g. `"dir1/file1.md"`.
*   `content` (*Str* if `binary` is `False`, otherwise *Bytes*): the content of the file.

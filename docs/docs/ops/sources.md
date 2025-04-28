---
title: Sources
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
*   `recent_changes_poll_interval` (type: `datetime.timedelta`, optional): when set, this source provides a *change capture mechanism* by polling Google Drive for recent modified files periodically.

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

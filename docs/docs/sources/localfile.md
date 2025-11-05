---
title: LocalFile
toc_max_heading_level: 4
description: CocoIndex LocalFile Built-in Sources
---

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

*   `max_file_size` (`int`, optional): if provided, files exceeding this size in bytes will be treated as non-existent and skipped during processing.
    This is useful to avoid processing large files that are not relevant to your use case, such as videos or backups.
    If not specified, no size limit is applied.

### Schema

The output is a [*KTable*](/docs/core/data_types#ktable) with the following sub fields:
*   `filename` (*Str*, key): the filename of the file, including the path, relative to the root directory, e.g. `"dir1/file1.md"`
*   `content` (*Str* if `binary` is `False`, *Bytes* otherwise): the content of the file

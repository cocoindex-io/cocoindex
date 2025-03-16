---
title: Sources
description: CocoIndex Built-in Sources
---

# CocoIndex Built-in Sources

## LocalFile

The `LocalFile` source imports files from a local file system.

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


The output is a table with the following sub fields:
*   `filename` (key, type: `str`): the filename of the file, including the path, relative to the root directory, e.g. `"dir1/file1.md"`
*   `content` (type: `str` if `binary` is `False`, otherwise `bytes`): the content of the file

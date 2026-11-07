"""Tests for the readable component paths of failure logs."""

from __future__ import annotations

import pytest

from cocoindex._internal.update_stats import readable_path


@pytest.mark.parametrize(
    ("path", "readable"),
    [
        ('/"item"/"3"', "item/3"),
        ('/"item"/3', "item/3"),
        ("/", "(root)"),
        # `mount_each` paths start with a symbol, and keys can hold a `/`.
        ('/@process_doc/"guide/page-7.md"', "process_doc/guide/page-7.md"),
        # Keys that are not plain keep the exact form.
        ('/"a\\"b"/"3"', '/"a\\"b"/"3"'),
        ('/"x"/true', '/"x"/true'),
    ],
)
def test_readable_path(path: str, readable: str) -> None:
    assert readable_path(path) == readable

"""Cases for how the object store sources (S3, Azure Blob, OCI) treat excluded directories.

An object store has flat keys, yet a directory that ``excluded_patterns`` excludes
must exclude every object under it, as it does in a directory walk.
"""

from __future__ import annotations

import pytest

# Object keys to store; the cases below list some of them.
EXCLUSION_TEST_KEYS = (
    "visible.txt",
    "ignored.txt",
    "ignored/deep/hidden.txt",
    "outer/kept.txt",
    "outer/ignored/hidden.txt",
    ".github/workflows/ci.yml",
    ".cache/x.txt",
)

# (prefix, excluded_patterns, item keys listed)
EXCLUDED_DIRECTORY_CASES = [
    # The key `ignored/deep/hidden.txt` matches no pattern: its folder excludes it.
    pytest.param(
        "",
        ["ignored"],
        [
            ".cache/x.txt",
            ".github/workflows/ci.yml",
            "ignored.txt",
            "outer/ignored/hidden.txt",
            "outer/kept.txt",
            "visible.txt",
        ],
        id="top-level",
    ),
    pytest.param(
        "",
        ["outer/ignored"],
        [
            ".cache/x.txt",
            ".github/workflows/ci.yml",
            "ignored.txt",
            "ignored/deep/hidden.txt",
            "outer/kept.txt",
            "visible.txt",
        ],
        id="nested",
    ),
    # Patterns match the key below the prefix folder.
    pytest.param("outer", ["ignored"], ["kept.txt"], id="below-prefix"),
    # A negation keeps the folder it carves out of an exclusion.
    pytest.param(
        "",
        ["**/.*", "!**/.github/**"],
        [
            ".github/workflows/ci.yml",
            "ignored.txt",
            "ignored/deep/hidden.txt",
            "outer/ignored/hidden.txt",
            "outer/kept.txt",
            "visible.txt",
        ],
        id="negation",
    ),
]

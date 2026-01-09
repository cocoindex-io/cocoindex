"""
Schema-related helper types.

Currently this module contains helpers for connector schemas that need extra
out-of-band information beyond Python type annotations.
"""

from __future__ import annotations

import typing as _typing


class VectorSpec(_typing.NamedTuple):
    """Additional information for a vector column."""

    dim: int


class FtsSpec(_typing.NamedTuple):
    """Additional information for a full-text search column."""

    tokenizer: str = "simple"  # "simple", "en_stem", "raw"


__all__ = ["VectorSpec", "FtsSpec"]

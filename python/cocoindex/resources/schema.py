"""
Schema-related helper types.

Currently this module contains helpers for connector schemas that need extra
out-of-band information beyond Python type annotations.
"""

from __future__ import annotations

import typing as _typing
import dataclasses as _dataclasses

if _typing.TYPE_CHECKING:
    import numpy as _np


@_typing.runtime_checkable
class VectorSchemaProvider(_typing.Protocol):
    """Additional information for a vector column."""

    def __coco_vector_schema__(self) -> VectorSchema: ...


@_dataclasses.dataclass(slots=True, frozen=True)
class VectorSchema:
    """Additional information for a vector column."""

    dtype: _np.dtype
    size: int

    def __coco_vector_schema__(self) -> VectorSchema:
        return self


class FtsSpec(_typing.NamedTuple):
    """Additional information for a full-text search column."""

    tokenizer: str = "simple"  # "simple", "en_stem", "raw"


__all__ = ["VectorSchema", "FtsSpec"]

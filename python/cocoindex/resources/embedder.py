"""Embedder protocol for single-text async embedding."""

from __future__ import annotations

import typing as _typing

if _typing.TYPE_CHECKING:
    import numpy as _np
    from numpy.typing import NDArray as _NDArray

__all__ = ["Embedder"]


@_typing.runtime_checkable
class Embedder(_typing.Protocol):
    """Single-text async embedder.

    Consumers (e.g. ``cocoindex.ops.entity_resolution.resolve_entities``) call
    ``await embedder.embed(text)`` and get a single float32 vector back.
    Implementations may use batching under the hood (see
    ``LiteLLMEmbedder`` / ``SentenceTransformerEmbedder`` for the pattern
    using ``@coco.fn.as_async(batching=True)`` on a private method).
    """

    async def embed(self, text: str) -> _NDArray[_np.float32]: ...

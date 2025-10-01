"""Functions module for cocoindex.

This module provides various function specifications and executors for data processing,
including embedding functions, text processing, and multimodal operations.
"""

# Import all engine builtin function specs
from ._engine_builtin_specs import (
    ParseJson,
    SplitRecursively,
    SplitBySeparators,
    EmbedText,
    ExtractByLlm,
)

# Import SentenceTransformer embedding functionality
from .sbert import (
    SentenceTransformerEmbed,
    SentenceTransformerEmbedExecutor,
)

# Import ColPali multimodal embedding functionality
from .colpali import (
    ColPaliEmbedImage,
    ColPaliEmbedImageExecutor,
    ColPaliEmbedQuery,
    ColPaliEmbedQueryExecutor,
)

__all__ = [
    # Engine builtin specs
    "ParseJson",
    "SplitRecursively",
    "SplitBySeparators",
    "EmbedText",
    "ExtractByLlm",
    # SentenceTransformer
    "SentenceTransformerEmbed",
    "SentenceTransformerEmbedExecutor",
    # ColPali
    "ColPaliEmbedImage",
    "ColPaliEmbedImageExecutor",
    "ColPaliEmbedQuery",
    "ColPaliEmbedQueryExecutor",
]

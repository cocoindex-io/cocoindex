from enum import Enum
from dataclasses import dataclass
from typing import Sequence, Union, Any


class VectorSimilarityMetric(Enum):
    COSINE_SIMILARITY = "CosineSimilarity"
    L2_DISTANCE = "L2Distance"
    INNER_PRODUCT = "InnerProduct"


@dataclass
class HnswVectorIndexMethod:
    """HNSW vector index parameters."""

    kind: str = "Hnsw"
    m: int | None = None
    ef_construction: int | None = None


@dataclass
class IvfFlatVectorIndexMethod:
    """IVFFlat vector index parameters."""

    kind: str = "IvfFlat"
    lists: int | None = None


VectorIndexMethod = Union[HnswVectorIndexMethod, IvfFlatVectorIndexMethod]


@dataclass
class VectorIndexDef:
    """
    Define a vector index on a field.
    """

    field_name: str
    metric: VectorSimilarityMetric
    method: VectorIndexMethod | None = None


@dataclass
class FtsIndexDef:
    """
    Define a full-text search index on a field.

    The parameters field can contain any keyword arguments supported by the target's
    FTS index creation API (e.g., tokenizer_name for LanceDB).
    """

    field_name: str
    parameters: dict[str, Any] | None = None


@dataclass
class IndexOptions:
    """
    Options for an index.
    """

    primary_key_fields: Sequence[str]
    vector_indexes: Sequence[VectorIndexDef] = ()
    fts_indexes: Sequence[FtsIndexDef] = ()

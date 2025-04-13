from enum import Enum
from dataclasses import dataclass

class VectorSimilarityMetric(Enum):
    COSINE_SIMILARITY = "CosineSimilarity"
    L2_DISTANCE = "L2Distance"
    INNER_PRODUCT = "InnerProduct"

@dataclass
class VectorIndexDef:
    """
    Define a vector index on a field.
    """
    field_name: str
    metric: VectorSimilarityMetric

@dataclass
class IndexOptions:
    """
    Options for an index.
    """
    primary_key_fields: list[str] | None = None
    vector_index_defs: list[VectorIndexDef] | None = None

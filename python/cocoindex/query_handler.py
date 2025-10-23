import dataclasses
import numpy as np
from numpy import typing as npt
from typing import Generic, Any
from .index import VectorSimilarityMetric
import sys

if sys.version_info >= (3, 13):
    from typing import TypeVar
else:
    from typing_extensions import TypeVar  # PEP 696 backport


@dataclasses.dataclass
class QueryHandlerResultFields:
    """
    Specify field names in query results returned by the query handler.
    This provides metadata for tools like CocoInsight to recognize structure of the query results.
    """

    embedding: list[str] = dataclasses.field(default_factory=list)
    score: str | None = None


@dataclasses.dataclass
class QueryHandlerInfo:
    """
    Info to configure a query handler.
    """

    result_fields: QueryHandlerResultFields | None = None


@dataclasses.dataclass
class QueryInfo:
    """
    Info about the query.
    """

    embedding: list[float] | npt.NDArray[np.float32] | None = None
    similarity_metric: VectorSimilarityMetric | None = None


R = TypeVar("R", default=Any)


@dataclasses.dataclass
class QueryOutput(Generic[R]):
    """
    Output of a query handler.

    results: list of results. Each result can be a dict or a dataclass.
    query_info: Info about the query.
    """

    results: list[R]
    query_info: QueryInfo = dataclasses.field(default_factory=QueryInfo)

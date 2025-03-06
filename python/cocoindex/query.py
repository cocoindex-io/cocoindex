from typing import Callable, Any
from dataclasses import dataclass
from threading import Lock

from . import flow as fl
from . import vector
from . import _engine

_handlers_lock = Lock()
_handlers: dict[str, _engine.SimpleSemanticsQueryHandler] = {}

@dataclass
class SimpleSemanticsQueryInfo:
    """
    Additional information about the query.
    """
    similarity_metric: vector.VectorSimilarityMetric
    query_vector: list[float]
    vector_field_name: str

@dataclass
class QueryResult:
    """
    A single result from the query.
    """
    data: dict[str, Any]
    score: float


class SimpleSemanticsQueryHandler:
    """
    A query handler that uses simple semantics to query the index.
    """
    _lazy_query_handler: Callable[[], _engine.SimpleSemanticsQueryHandler]

    def __init__(
        self,
        name: str,
        flow: fl.Flow,
        target_name: str,
        query_transform_flow: Callable[..., fl.DataSlice],
        default_similarity_metric: vector.VectorSimilarityMetric = vector.VectorSimilarityMetric.COSINE_SIMILARITY) -> None:

        engine_handler = None
        lock = Lock()
        def _lazy_handler() -> _engine.SimpleSemanticsQueryHandler:
            nonlocal engine_handler, lock
            if engine_handler is None:
                with lock:
                    if engine_handler is None:
                        engine_handler = _engine.SimpleSemanticsQueryHandler(
                            flow.internal_flow(), target_name,
                            fl.TransientFlow(query_transform_flow, [str]).internal_flow(),
                            default_similarity_metric.value)
                        engine_handler.register_query_handler(name)
            return engine_handler
        self._lazy_query_handler = _lazy_handler

        with _handlers_lock:
            _handlers[name] = self

    def internal_handler(self) -> _engine.SimpleSemanticsQueryHandler:
        """
        Get the internal query handler.
        """
        return self._lazy_query_handler()

    def search(self, query: str, limit: int, vector_field_name: str | None = None,
               similarity_matric: vector.VectorSimilarityMetric | None = None) -> tuple[list[QueryResult], SimpleSemanticsQueryInfo]:
        """
        Search the index with the given query, limit, vector field name, and similarity metric.
        """
        internal_results, internal_info = self.internal_handler().search(
            query, limit, vector_field_name,
            similarity_matric.value if similarity_matric is not None else None)
        fields = [field['name'] for field in internal_results['fields']]
        results = [QueryResult(data=dict(zip(fields, result['data'])),  score=result['score']) for result in internal_results['results']]
        info = SimpleSemanticsQueryInfo(
            similarity_metric=vector.VectorSimilarityMetric(internal_info['similarity_metric']),
            query_vector=internal_info['query_vector'],
            vector_field_name=internal_info['vector_field_name']
        )
        return results, info

def ensure_all_handlers_built() -> None:
    """
    Ensure all handlers are built.
    """
    with _handlers_lock:
        for handler in _handlers.values():
            handler.internal_handler()

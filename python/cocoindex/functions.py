"""All builtin functions."""
from typing import Annotated

import json
import sentence_transformers
from .typing import Float32, Vector, TypeAttr
from . import op

class SplitRecursively(op.FunctionSpec):
    """Split a document (in string) recursively."""
    chunk_size: int
    chunk_overlap: int
    language: str | None = None

class SentenceTransformerEmbed(op.FunctionSpec):
    """Run the sentence transformer"""
    model: str

@op.executor_class(gpu=True, cache=True, behavior_version=1)
class SentenceTransformerEmbedExecutor:
    """Executor for SentenceTransformerEmbed."""

    spec: SentenceTransformerEmbed
    _model: sentence_transformers.SentenceTransformer

    def analyze(self, text = None):
        self._model = sentence_transformers.SentenceTransformer(self.spec.model, 3)
        dim = self._model.get_sentence_embedding_dimension()
        return Annotated[list[Float32], Vector(dim=dim), TypeAttr("cocoindex.io/vector_origin_text", json.loads(text.analyzed_value))]

    def __call__(self, text: str) -> list[Float32]:
        return self._model.encode(text).tolist()

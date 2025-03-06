"""All builtin functions."""
from typing import Annotated, Any

import sentence_transformers
from .typing import Float32, Vector, TypeAttr
from . import op

class SplitRecursively(op.FunctionSpec):
    """Split a document (in string) recursively."""
    chunk_size: int
    chunk_overlap: int
    language: str | None = None

class SentenceTransformerEmbed(op.FunctionSpec):
    """
    `SentenceTransformerEmbed` embeds a text into a vector space using the [SentenceTransformer](https://huggingface.co/sentence-transformers) library.

    Args:

        model: The name of the SentenceTransformer model to use.
        args: Additional arguments to pass to the SentenceTransformer constructor. e.g. {"trust_remote_code": True}
    """
    model: str
    args: dict[str, Any] | None = None

@op.executor_class(gpu=True, cache=True, behavior_version=1)
class SentenceTransformerEmbedExecutor:
    """Executor for SentenceTransformerEmbed."""

    spec: SentenceTransformerEmbed
    _model: sentence_transformers.SentenceTransformer

    def analyze(self, text):
        args = self.spec.args or {}
        self._model = sentence_transformers.SentenceTransformer(self.spec.model, **args)
        dim = self._model.get_sentence_embedding_dimension()
        return Annotated[list[Float32], Vector(dim=dim), TypeAttr("cocoindex.io/vector_origin_text", text.analyzed_value)]

    def __call__(self, text: str) -> list[Float32]:
        return self._model.encode(text).tolist()

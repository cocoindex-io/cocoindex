"""All builtin functions."""

import dataclasses
from typing import Annotated, Any, Literal

import numpy as np
from numpy.typing import NDArray

from . import llm, op
from .typing import TypeAttr, Vector

# Check if sentence_transformers is available
try:
    import sentence_transformers  # type: ignore

    _SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    _SENTENCE_TRANSFORMERS_AVAILABLE = False


class ParseJson(op.FunctionSpec):
    """Parse a text into a JSON object."""


@dataclasses.dataclass
class CustomLanguageSpec:
    """Custom language specification."""

    language_name: str
    separators_regex: list[str]
    aliases: list[str] = dataclasses.field(default_factory=list)


class SplitRecursively(op.FunctionSpec):
    """Split a document (in string) recursively."""

    custom_languages: list[CustomLanguageSpec] = dataclasses.field(default_factory=list)


class EmbedText(op.FunctionSpec):
    """Embed a text into a vector space."""

    api_type: llm.LlmApiType
    model: str
    address: str | None = None
    output_dimension: int | None = None
    task_type: str | None = None
    api_config: llm.VertexAiConfig | None = None


class ExtractByLlm(op.FunctionSpec):
    """Extract information from a text using a LLM."""

    llm_spec: llm.LlmSpec
    output_type: type
    instruction: str | None = None


class SentenceTransformerEmbed(op.FunctionSpec):
    """
    `SentenceTransformerEmbed` embeds a text into a vector space using the [SentenceTransformer](https://huggingface.co/sentence-transformers) library.

    Args:

        model: The name of the SentenceTransformer model to use.
        args: Additional arguments to pass to the SentenceTransformer constructor. e.g. {"trust_remote_code": True}

    Note:
        This function requires the optional sentence-transformers dependency.
        Install it with: pip install 'cocoindex[embeddings]'
    """

    model: str
    args: dict[str, Any] | None = None


@op.executor_class(gpu=True, cache=True, behavior_version=1)
class SentenceTransformerEmbedExecutor:
    """Executor for SentenceTransformerEmbed."""

    spec: SentenceTransformerEmbed
    _model: "sentence_transformers.SentenceTransformer"

    def analyze(self, text: Any) -> type:
        if not _SENTENCE_TRANSFORMERS_AVAILABLE:
            raise ImportError(
                "sentence_transformers is required for SentenceTransformerEmbed function. "
                "Install it with one of these commands:\n"
                "  pip install 'cocoindex[embeddings]'\n"
                "  pip install sentence-transformers"
            )

        import sentence_transformers  # pylint: disable=import-outside-toplevel

        args = self.spec.args or {}
        self._model = sentence_transformers.SentenceTransformer(self.spec.model, **args)
        dim = self._model.get_sentence_embedding_dimension()
        result: type = Annotated[
            Vector[np.float32, Literal[dim]],  # type: ignore
            TypeAttr("cocoindex.io/vector_origin_text", text.analyzed_value),
        ]
        return result

    def __call__(self, text: str) -> NDArray[np.float32]:
        result: NDArray[np.float32] = self._model.encode(text, convert_to_numpy=True)
        return result

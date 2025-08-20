"""All builtin functions."""

import dataclasses
import functools
from typing import Annotated, Any, Literal

import numpy as np
from numpy.typing import NDArray

from . import llm, op
from .typing import TypeAttr, Vector


class ParseJson(op.FunctionSpec):
    """Parse a text into a JSON object."""


@dataclasses.dataclass
class CustomLanguageSpec:
    """Custom language specification."""

    language_name: str
    separators_regex: list[str]
    aliases: list[str] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class ColPaliModelInfo:
    """Data structure for ColPali model and processor."""

    model: Any
    processor: Any
    dimension: int
    device: Any


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


@op.executor_class(
    gpu=True,
    cache=True,
    behavior_version=1,
    arg_relationship=(op.ArgRelationship.EMBEDDING_ORIGIN_TEXT, "text"),
)
class SentenceTransformerEmbedExecutor:
    """Executor for SentenceTransformerEmbed."""

    spec: SentenceTransformerEmbed
    _model: Any | None = None

    def analyze(self) -> type:
        try:
            # Only import sentence_transformers locally when it's needed, as its import is very slow.
            import sentence_transformers  # pylint: disable=import-outside-toplevel
        except ImportError as e:
            raise ImportError(
                "sentence_transformers is required for SentenceTransformerEmbed function. "
                "Install it with one of these commands:\n"
                "  pip install 'cocoindex[embeddings]'\n"
                "  pip install sentence-transformers"
            ) from e

        args = self.spec.args or {}
        self._model = sentence_transformers.SentenceTransformer(self.spec.model, **args)
        dim = self._model.get_sentence_embedding_dimension()
        return Vector[np.float32, Literal[dim]]  # type: ignore

    def __call__(self, text: str) -> NDArray[np.float32]:
        assert self._model is not None
        result: NDArray[np.float32] = self._model.encode(text, convert_to_numpy=True)
        return result


@functools.cache
def _get_colpali_model_and_processor(model_name: str) -> ColPaliModelInfo:
    """Get or load ColPali model and processor, with caching."""
    try:
        from colpali_engine.models import (  # type: ignore[import-untyped]
            ColPali,
            ColPaliProcessor,
            ColQwen2,
            ColQwen2Processor,
            ColQwen2_5,
            ColQwen2_5_Processor,
            ColIdefics3,
            ColIdefics3Processor,
        )
        from colpali_engine.utils.torch_utils import get_torch_device  # type: ignore[import-untyped]
        import torch
    except ImportError as e:
        raise ImportError(
            "ColVision models are not available. Make sure cocoindex is installed with ColPali support."
        ) from e

    device = get_torch_device("auto")

    # Manual model detection based on model name
    model_name_lower = model_name.lower()

    try:
        if "qwen2.5" in model_name_lower:
            model = ColQwen2_5.from_pretrained(
                model_name,
                torch_dtype=torch.bfloat16,
                device_map=device,
            ).eval()
            processor = ColQwen2_5_Processor.from_pretrained(model_name)
        elif "qwen2" in model_name_lower:
            model = ColQwen2.from_pretrained(
                model_name,
                torch_dtype=torch.bfloat16,
                device_map=device,
            ).eval()
            processor = ColQwen2Processor.from_pretrained(model_name)
        elif "colsmol" in model_name_lower or "smol" in model_name_lower:
            # ColSmol models use Idefics3 architecture
            model = ColIdefics3.from_pretrained(
                model_name,
                torch_dtype=torch.bfloat16,
                device_map=device,
            ).eval()
            processor = ColIdefics3Processor.from_pretrained(model_name)
        else:
            # Default to ColPali
            model = ColPali.from_pretrained(
                model_name,
                torch_dtype=torch.bfloat16,
                device_map=device,
            ).eval()
            processor = ColPaliProcessor.from_pretrained(model_name)

    except Exception as e:
        raise RuntimeError(f"Failed to load model {model_name}: {e}")

    # Get dimension from the actual model
    dimension = _detect_colpali_dimension(model, processor, device)

    return ColPaliModelInfo(
        model=model,
        processor=processor,
        dimension=dimension,
        device=device,
    )


def _detect_colpali_dimension(model: Any, processor: Any, device: Any) -> int:
    """Detect ColPali embedding dimension from the actual model config."""
    # Try to access embedding dimension
    if hasattr(model.config, "embedding_dim"):
        dim = model.config.embedding_dim
    else:
        # Fallback: infer from output shape with dummy data
        from PIL import Image
        import numpy as np
        import torch

        dummy_img = Image.fromarray(np.zeros((224, 224, 3), np.uint8))
        # Use the processor to process the dummy image
        processed = processor.process_images([dummy_img]).to(device)
        with torch.no_grad():
            output = model(**processed)
        dim = int(output.shape[-1])
    if isinstance(dim, int):
        return dim
    else:
        raise ValueError(f"Expected integer dimension, got {type(dim)}: {dim}")
    return dim


class ColPaliEmbedImage(op.FunctionSpec):
    """
    `ColPaliEmbedImage` embeds images using ColVision multimodal models.

    Supports ALL models available in the colpali-engine library, including:
    - ColPali models (colpali-*): PaliGemma-based, best for general document retrieval
    - ColQwen2 models (colqwen-*): Qwen2-VL-based, excellent for multilingual text (29+ languages) and general vision
    - ColSmol models (colsmol-*): Lightweight, good for resource-constrained environments
    - Any future ColVision models supported by colpali-engine

    These models use late interaction between image patch embeddings and text token
    embeddings for retrieval.

    Args:
        model: Any ColVision model name supported by colpali-engine
               (e.g., "vidore/colpali-v1.2", "vidore/colqwen2.5-v0.2", "vidore/colsmol-v1.0")
               See https://github.com/illuin-tech/colpali for the complete list of supported models.

    Note:
        This function requires the optional colpali-engine dependency.
        Install it with: pip install 'cocoindex[colpali]'
    """

    model: str


@op.executor_class(
    gpu=True,
    cache=True,
    behavior_version=1,
)
class ColPaliEmbedImageExecutor:
    """Executor for ColVision image embedding (ColPali, ColQwen2, ColSmol, etc.)."""

    spec: ColPaliEmbedImage
    _model_info: ColPaliModelInfo

    def analyze(self) -> type:
        # Get shared model and dimension
        self._model_info = _get_colpali_model_and_processor(self.spec.model)

        # Return multi-vector type: Variable patches x Fixed hidden dimension
        dimension = self._model_info.dimension
        return Vector[Vector[np.float32, Literal[dimension]]]  # type: ignore

    def __call__(self, img_bytes: bytes) -> Any:
        try:
            from PIL import Image
            import torch
            import io
        except ImportError as e:
            raise ImportError(
                "Required dependencies (PIL, torch) are missing for ColVision image embedding."
            ) from e

        model = self._model_info.model
        processor = self._model_info.processor
        device = self._model_info.device

        pil_image = Image.open(io.BytesIO(img_bytes)).convert("RGB")
        inputs = processor.process_images([pil_image]).to(device)
        with torch.no_grad():
            embeddings = model(**inputs)

        # Return multi-vector format: [patches, hidden_dim]
        if len(embeddings.shape) != 3:
            raise ValueError(
                f"Expected 3D tensor [batch, patches, hidden_dim], got shape {embeddings.shape}"
            )

        # Keep patch-level embeddings: [batch, patches, hidden_dim] -> [patches, hidden_dim]
        patch_embeddings = embeddings[0]  # Remove batch dimension

        return patch_embeddings.cpu().to(torch.float32).numpy()


class ColPaliEmbedQuery(op.FunctionSpec):
    """
    `ColPaliEmbedQuery` embeds text queries using ColVision multimodal models.

    Supports ALL models available in the colpali-engine library, including:
    - ColPali models (colpali-*): PaliGemma-based, best for general document retrieval
    - ColQwen2 models (colqwen-*): Qwen2-VL-based, excellent for multilingual text (29+ languages) and general vision
    - ColSmol models (colsmol-*): Lightweight, good for resource-constrained environments
    - Any future ColVision models supported by colpali-engine

    This produces query embeddings compatible with ColVision image embeddings
    for late interaction scoring (MaxSim).

    Args:
        model: Any ColVision model name supported by colpali-engine
               (e.g., "vidore/colpali-v1.2", "vidore/colqwen2.5-v0.2", "vidore/colsmol-v1.0")
               See https://github.com/illuin-tech/colpali for the complete list of supported models.

    Note:
        This function requires the optional colpali-engine dependency.
        Install it with: pip install 'cocoindex[colpali]'
    """

    model: str


@op.executor_class(
    gpu=True,
    cache=True,
    behavior_version=1,
)
class ColPaliEmbedQueryExecutor:
    """Executor for ColVision query embedding (ColPali, ColQwen2, ColSmol, etc.)."""

    spec: ColPaliEmbedQuery
    _model_info: ColPaliModelInfo

    def analyze(self) -> type:
        # Get shared model and dimension
        self._model_info = _get_colpali_model_and_processor(self.spec.model)

        # Return multi-vector type: Variable tokens x Fixed hidden dimension
        dimension = self._model_info.dimension
        return Vector[Vector[np.float32, Literal[dimension]]]  # type: ignore

    def __call__(self, query: str) -> Any:
        try:
            import torch
        except ImportError as e:
            raise ImportError(
                "Required dependencies (torch) are missing for ColVision query embedding."
            ) from e

        model = self._model_info.model
        processor = self._model_info.processor
        device = self._model_info.device

        inputs = processor.process_queries([query]).to(device)
        with torch.no_grad():
            embeddings = model(**inputs)

        # Return multi-vector format: [tokens, hidden_dim]
        if len(embeddings.shape) != 3:
            raise ValueError(
                f"Expected 3D tensor [batch, tokens, hidden_dim], got shape {embeddings.shape}"
            )

        # Keep token-level embeddings: [batch, tokens, hidden_dim] -> [tokens, hidden_dim]
        token_embeddings = embeddings[0]  # Remove batch dimension

        return token_embeddings.cpu().to(torch.float32).numpy()

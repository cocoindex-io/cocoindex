"""ColPali image and query embedding functions for multimodal document retrieval."""

import functools
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING, Literal
import numpy as np

from .. import op
from ..typing import Vector

if TYPE_CHECKING:
    import torch


@dataclass
class ColPaliModelInfo:
    """Shared model information for ColPali embedding functions."""

    model: Any
    processor: Any
    device: Any
    dimension: int


@functools.cache
def _get_colpali_model_and_processor(model_name: str) -> ColPaliModelInfo:
    """Load and cache ColPali model and processor with shared device setup."""
    try:
        import colpali_engine as ce
        import torch
    except ImportError as e:
        raise ImportError(
            "ColPali support requires the optional 'colpali' dependency. "
            "Install it with: pip install 'cocoindex[colpali]'"
        ) from e

    device = "cuda" if torch.cuda.is_available() else "cpu"
    lower_model_name = model_name.lower()

    # Determine model type from name
    if lower_model_name.startswith("colpali"):
        model = ce.ColPali.from_pretrained(
            model_name, torch_dtype=torch.bfloat16, device_map=device
        )
        processor = ce.ColPaliProcessor.from_pretrained(model_name)
    elif lower_model_name.startswith("colqwen2.5"):
        model = ce.ColQwen2_5.from_pretrained(
            model_name, torch_dtype=torch.bfloat16, device_map=device
        )
        processor = ce.ColQwen2_5_Processor.from_pretrained(model_name)
    elif lower_model_name.startswith("colqwen"):
        model = ce.ColQwen2.from_pretrained(
            model_name, torch_dtype=torch.bfloat16, device_map=device
        )
        processor = ce.ColQwen2Processor.from_pretrained(model_name)
    else:
        # Fallback to ColPali for backwards compatibility
        model = ce.ColPali.from_pretrained(
            model_name, torch_dtype=torch.bfloat16, device_map=device
        )
        processor = ce.ColPaliProcessor.from_pretrained(model_name)

    # Detect dimension
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
    batching=True,
    max_batch_size=32,
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

    def __call__(self, img_bytes_list: list[bytes]) -> Any:
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

        pil_images = [
            Image.open(io.BytesIO(img_bytes)).convert("RGB")
            for img_bytes in img_bytes_list
        ]
        inputs = processor.process_images(pil_images).to(device)
        with torch.no_grad():
            embeddings = model(**inputs)

        # Return multi-vector format: [patches, hidden_dim]
        if len(embeddings.shape) != 3:
            raise ValueError(
                f"Expected 3D tensor [batch, patches, hidden_dim], got shape {embeddings.shape}"
            )

        # [patches, hidden_dim]
        return embeddings.cpu().to(torch.float32).numpy()


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
    batching=True,
    max_batch_size=32,
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

    def __call__(self, queries: list[str]) -> Any:
        try:
            import torch
        except ImportError as e:
            raise ImportError(
                "Required dependencies (torch) are missing for ColVision query embedding."
            ) from e

        model = self._model_info.model
        processor = self._model_info.processor
        device = self._model_info.device

        inputs = processor.process_queries(queries).to(device)
        with torch.no_grad():
            embeddings = model(**inputs)

        # Return multi-vector format: [tokens, hidden_dim]
        if len(embeddings.shape) != 3:
            raise ValueError(
                f"Expected 3D tensor [batch, tokens, hidden_dim], got shape {embeddings.shape}"
            )

        # [tokens, hidden_dim]
        return embeddings.cpu().to(torch.float32).numpy()

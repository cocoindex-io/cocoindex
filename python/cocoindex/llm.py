from dataclasses import dataclass
from enum import Enum


class LlmApiType(Enum):
    """The type of LLM API to use."""

    OPENAI = "OpenAi"
    OLLAMA = "Ollama"
    GEMINI = "Gemini"
    VERTEX_AI = "VertexAi"
    ANTHROPIC = "Anthropic"
    LITE_LLM = "LiteLlm"
    OPEN_ROUTER = "OpenRouter"
    VOYAGE = "Voyage"
    VLLM = "Vllm"


@dataclass
class VertexAiConfig:
    """A specification for a Vertex AI LLM."""

    kind = "VertexAi"

    project: str
    region: str | None = None


@dataclass
class LlmSpec:
    """A specification for a LLM."""

    api_type: LlmApiType
    model: str
    address: str | None = None
    api_config: VertexAiConfig | None = None

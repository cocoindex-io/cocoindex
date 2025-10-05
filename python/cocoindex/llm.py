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
class OpenAiConfig:
    """A specification for a OpenAI LLM."""

    kind = "OpenAi"

    org_id: str | None = None
    project_id: str | None = None


@dataclass
class AnthropicConfig:
    """A specification for an Anthropic LLM."""

    kind = "Anthropic"


@dataclass
class GeminiConfig:
    """A specification for a Gemini LLM."""

    kind = "Gemini"


@dataclass
class VoyageConfig:
    """A specification for a Voyage LLM."""

    kind = "Voyage"


@dataclass
class LiteLlmConfig:
    """A specification for a LiteLLM LLM."""

    kind = "LiteLlm"


@dataclass
class OpenRouterConfig:
    """A specification for an OpenRouter LLM."""

    kind = "OpenRouter"


@dataclass
class VllmConfig:
    """A specification for a VLLM LLM."""

    kind = "Vllm"


@dataclass
class LlmSpec:
    """A specification for a LLM."""

    api_type: LlmApiType
    model: str
    address: str | None = None
    api_key: str | None = None
    api_config: VertexAiConfig | OpenAiConfig | AnthropicConfig | GeminiConfig | VoyageConfig | LiteLlmConfig | OpenRouterConfig | VllmConfig | None = None

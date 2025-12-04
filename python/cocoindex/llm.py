from dataclasses import dataclass
from enum import Enum

from .auth_registry import TransientAuthEntryReference


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
    BEDROCK = "Bedrock"
    AZURE_OPENAI = "AzureOpenAi"


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
class AzureOpenAiConfig:
    """A specification for an Azure OpenAI LLM."""

    kind = "AzureOpenAi"

    deployment_id: str
    api_version: str | None = None


@dataclass
class LlmSpec:
    """A specification for a LLM."""

    api_type: LlmApiType
    model: str
    address: str | None = None
    api_key: TransientAuthEntryReference[str] | None = None
    api_config: VertexAiConfig | OpenAiConfig | AzureOpenAiConfig | None = None

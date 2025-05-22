from dataclasses import dataclass
from enum import Enum


class LlmApiType(Enum):
    """The type of LLM API to use."""

    OPENAI = "OpenAi"
    OLLAMA = "Ollama"
    GEMINI = "Gemini"
    ANTHROPIC = "Anthropic"


@dataclass
class LlmSpec:
    """A specification for a LLM."""

    api_type: LlmApiType
    model: str
    address: str | None = None

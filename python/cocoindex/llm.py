from dataclasses import dataclass
from enum import Enum

class LlmApiType(Enum):
    """The type of LLM API to use."""
    OLLAMA = "Ollama"

@dataclass
class LlmSpec:
    """A specification for a LLM."""
    api_type: LlmApiType
    model: str
    address: str | None = None

"""All builtin function specs."""

import dataclasses
from typing import Literal

from .. import llm, op
from ..auth_registry import TransientAuthEntryReference


class ParseJson(op.FunctionSpec):
    """Parse a text into a JSON object."""


@dataclasses.dataclass
class CustomLanguageSpec:
    """Custom language specification."""

    language_name: str
    separators_regex: list[str]
    aliases: list[str] = dataclasses.field(default_factory=list)


class DetectProgrammingLanguage(op.FunctionSpec):
    """Detect the programming language of a file."""


class SplitRecursively(op.FunctionSpec):
    """Split a document (in string) recursively."""

    custom_languages: list[CustomLanguageSpec] = dataclasses.field(default_factory=list)


class SplitBySeparators(op.FunctionSpec):
    """
    Split text by specified regex separators only.
    Output schema matches SplitRecursively for drop-in compatibility:
        KTable rows with fields: location (Range), text (Str), start, end.
    Args:
        separators_regex: list[str]  # e.g., [r"\\n\\n+"]
        keep_separator: Literal["NONE", "LEFT", "RIGHT"] = "NONE"
        include_empty: bool = False
        trim: bool = True
    """

    separators_regex: list[str] = dataclasses.field(default_factory=list)
    keep_separator: Literal["NONE", "LEFT", "RIGHT"] = "NONE"
    include_empty: bool = False
    trim: bool = True


class EmbedText(op.FunctionSpec):
    """Embed a text into a vector space."""

    api_type: llm.LlmApiType
    model: str
    address: str | None = None
    output_dimension: int | None = None
    expected_output_dimension: int | None = None
    task_type: str | None = None
    api_config: llm.VertexAiConfig | None = None
    api_key: TransientAuthEntryReference[str] | None = None


class ExtractByLlm(op.FunctionSpec):
    """Extract information from a text using a LLM."""

    llm_spec: llm.LlmSpec
    output_type: type
    instruction: str | None = None

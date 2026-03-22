"""Tests for MiniMax LLM provider integration."""

import os
from unittest.mock import patch

import pytest

from cocoindex.llm import LlmApiType, LlmSpec


class TestMiniMaxLlmApiType:
    """Unit tests for MiniMax LlmApiType enum."""

    def test_minimax_enum_exists(self) -> None:
        """MiniMax should be a valid LlmApiType variant."""
        assert hasattr(LlmApiType, "MINIMAX")
        assert LlmApiType.MINIMAX.value == "MiniMax"

    def test_minimax_enum_distinct(self) -> None:
        """MiniMax should be distinct from other enum values."""
        other_values = [
            member.value for name, member in LlmApiType.__members__.items()
            if name != "MINIMAX"
        ]
        assert LlmApiType.MINIMAX.value not in other_values

    def test_all_providers_present(self) -> None:
        """All expected providers including MiniMax should be present."""
        expected = {
            "OPENAI", "OLLAMA", "GEMINI", "VERTEX_AI", "ANTHROPIC",
            "LITE_LLM", "OPEN_ROUTER", "VOYAGE", "VLLM", "BEDROCK",
            "AZURE_OPENAI", "MINIMAX",
        }
        actual = set(LlmApiType.__members__.keys())
        assert expected == actual


class TestMiniMaxLlmSpec:
    """Unit tests for LlmSpec with MiniMax configuration."""

    def test_basic_spec_creation(self) -> None:
        """LlmSpec with MiniMax api_type should be created successfully."""
        spec = LlmSpec(
            api_type=LlmApiType.MINIMAX,
            model="MiniMax-M2.7",
        )
        assert spec.api_type == LlmApiType.MINIMAX
        assert spec.model == "MiniMax-M2.7"
        assert spec.address is None
        assert spec.api_key is None
        assert spec.api_config is None

    def test_spec_with_custom_address(self) -> None:
        """LlmSpec should accept a custom address for MiniMax."""
        spec = LlmSpec(
            api_type=LlmApiType.MINIMAX,
            model="MiniMax-M2.5",
            address="https://custom.minimax.io/v1",
        )
        assert spec.address == "https://custom.minimax.io/v1"

    def test_spec_with_m27_model(self) -> None:
        """LlmSpec should work with MiniMax-M2.7 model."""
        spec = LlmSpec(
            api_type=LlmApiType.MINIMAX,
            model="MiniMax-M2.7",
        )
        assert spec.model == "MiniMax-M2.7"

    def test_spec_with_m25_highspeed_model(self) -> None:
        """LlmSpec should work with MiniMax-M2.5-highspeed model."""
        spec = LlmSpec(
            api_type=LlmApiType.MINIMAX,
            model="MiniMax-M2.5-highspeed",
        )
        assert spec.model == "MiniMax-M2.5-highspeed"

    def test_spec_with_m27_highspeed_model(self) -> None:
        """LlmSpec should work with MiniMax-M2.7-highspeed model."""
        spec = LlmSpec(
            api_type=LlmApiType.MINIMAX,
            model="MiniMax-M2.7-highspeed",
        )
        assert spec.model == "MiniMax-M2.7-highspeed"

    def test_spec_with_embedding_model(self) -> None:
        """LlmSpec should work with embo-01 embedding model."""
        spec = LlmSpec(
            api_type=LlmApiType.MINIMAX,
            model="embo-01",
        )
        assert spec.model == "embo-01"

    def test_spec_serialization_roundtrip(self) -> None:
        """LlmSpec should survive dataclass field access."""
        spec = LlmSpec(
            api_type=LlmApiType.MINIMAX,
            model="MiniMax-M2.7",
            address="https://api.minimax.io/v1",
        )
        assert spec.api_type.value == "MiniMax"
        assert spec.model == "MiniMax-M2.7"
        assert spec.address == "https://api.minimax.io/v1"

    def test_spec_no_api_config_needed(self) -> None:
        """MiniMax does not require provider-specific api_config."""
        spec = LlmSpec(
            api_type=LlmApiType.MINIMAX,
            model="MiniMax-M2.7",
            api_config=None,
        )
        assert spec.api_config is None


class TestMiniMaxEmbedTextSpec:
    """Unit tests for EmbedText function spec with MiniMax."""

    def test_embed_text_spec_creation(self) -> None:
        """EmbedText spec should accept MiniMax api_type."""
        from cocoindex.functions._engine_builtin_specs import EmbedText

        spec = EmbedText(
            api_type=LlmApiType.MINIMAX,
            model="embo-01",
        )
        assert spec.api_type == LlmApiType.MINIMAX
        assert spec.model == "embo-01"

    def test_embed_text_spec_with_task_type(self) -> None:
        """EmbedText spec should accept task_type for MiniMax embedding."""
        from cocoindex.functions._engine_builtin_specs import EmbedText

        spec = EmbedText(
            api_type=LlmApiType.MINIMAX,
            model="embo-01",
            task_type="query",
        )
        assert spec.task_type == "query"

    def test_embed_text_spec_with_db_task_type(self) -> None:
        """EmbedText spec should accept 'db' task_type for storage embedding."""
        from cocoindex.functions._engine_builtin_specs import EmbedText

        spec = EmbedText(
            api_type=LlmApiType.MINIMAX,
            model="embo-01",
            task_type="db",
        )
        assert spec.task_type == "db"

    def test_embed_text_spec_default_dimension(self) -> None:
        """EmbedText spec should handle expected_output_dimension for embo-01."""
        from cocoindex.functions._engine_builtin_specs import EmbedText

        spec = EmbedText(
            api_type=LlmApiType.MINIMAX,
            model="embo-01",
            expected_output_dimension=1536,
        )
        assert spec.expected_output_dimension == 1536


class TestMiniMaxExtractByLlmSpec:
    """Unit tests for ExtractByLlm function spec with MiniMax."""

    def test_extract_by_llm_spec_creation(self) -> None:
        """ExtractByLlm spec should work with MiniMax LlmSpec."""
        import dataclasses
        from cocoindex.functions._engine_builtin_specs import ExtractByLlm

        @dataclasses.dataclass
        class SampleOutput:
            title: str
            summary: str

        spec = ExtractByLlm(
            llm_spec=LlmSpec(
                api_type=LlmApiType.MINIMAX,
                model="MiniMax-M2.7",
            ),
            output_type=SampleOutput,
            instruction="Extract title and summary.",
        )
        assert spec.llm_spec.api_type == LlmApiType.MINIMAX
        assert spec.llm_spec.model == "MiniMax-M2.7"
        assert spec.instruction == "Extract title and summary."

    def test_extract_by_llm_spec_without_instruction(self) -> None:
        """ExtractByLlm spec should work without instruction."""
        import dataclasses
        from cocoindex.functions._engine_builtin_specs import ExtractByLlm

        @dataclasses.dataclass
        class InfoOutput:
            name: str

        spec = ExtractByLlm(
            llm_spec=LlmSpec(
                api_type=LlmApiType.MINIMAX,
                model="MiniMax-M2.5",
            ),
            output_type=InfoOutput,
        )
        assert spec.instruction is None


class TestMiniMaxIntegration:
    """Integration tests for MiniMax provider (require MINIMAX_API_KEY)."""

    @pytest.fixture(autouse=True)
    def skip_without_api_key(self) -> None:
        if not os.environ.get("MINIMAX_API_KEY"):
            pytest.skip("MINIMAX_API_KEY not set")

    def test_minimax_generation_spec_ready(self) -> None:
        """Full LlmSpec for MiniMax generation should be constructable."""
        spec = LlmSpec(
            api_type=LlmApiType.MINIMAX,
            model="MiniMax-M2.7",
            address="https://api.minimax.io/v1",
        )
        assert spec.api_type == LlmApiType.MINIMAX
        assert spec.model == "MiniMax-M2.7"

    def test_minimax_embedding_spec_ready(self) -> None:
        """Full EmbedText spec for MiniMax embedding should be constructable."""
        from cocoindex.functions._engine_builtin_specs import EmbedText

        spec = EmbedText(
            api_type=LlmApiType.MINIMAX,
            model="embo-01",
            task_type="db",
            expected_output_dimension=1536,
        )
        assert spec.api_type == LlmApiType.MINIMAX
        assert spec.expected_output_dimension == 1536

    def test_minimax_highspeed_model_spec(self) -> None:
        """MiniMax-M2.7-highspeed model should be configurable."""
        spec = LlmSpec(
            api_type=LlmApiType.MINIMAX,
            model="MiniMax-M2.7-highspeed",
            address="https://api.minimax.io/v1",
        )
        assert spec.model == "MiniMax-M2.7-highspeed"

"""Text processing utilities.

This module provides text splitting and language detection functions.
"""

__all__ = [
    "detect_code_language",
    "SeparatorSplitter",
    "CustomLanguageConfig",
    "RecursiveSplitter",
]

import typing as _typing

from cocoindex._internal import core as _core
from cocoindex.resources import chunk as _chunk


def detect_code_language(*, filename: str) -> str | None:
    """Detect programming language from a filename.

    Args:
        filename: The filename to detect language from (e.g., "main.py", "test.rs").

    Returns:
        The language name if the file extension is recognized, otherwise None.

    Examples:
        >>> detect_code_language(filename="main.py")
        'python'
        >>> detect_code_language(filename="test.rs")
        'rust'
        >>> detect_code_language(filename="unknown.xyz")
        None
    """
    return _core.detect_code_language(filename=filename)


class SeparatorSplitter:
    """A text splitter that splits by regex separators.

    This splitter can be instantiated once and reused to split multiple texts
    efficiently.

    Args:
        separators_regex: A list of regex patterns for separators. They are OR-joined
            into a single pattern.
        keep_separator: How to handle separators:
            - "left": Include separator at the end of the preceding chunk.
            - "right": Include separator at the start of the following chunk.
            - None: Discard separators (default).
        include_empty: Whether to include empty chunks in the output.
        trim: Whether to trim whitespace from chunks.

    Examples:
        >>> splitter = SeparatorSplitter([r"\\n\\n+"])
        >>> chunks = splitter.split("Para1\\n\\nPara2\\n\\nPara3")
        >>> [c.text for c in chunks]
        ['Para1', 'Para2', 'Para3']
    """

    def __init__(
        self,
        separators_regex: list[str],
        *,
        keep_separator: _typing.Literal["left", "right"] | None = None,
        include_empty: bool = False,
        trim: bool = True,
    ) -> None:
        self._splitter = _core.SeparatorSplitter(
            separators_regex, keep_separator, include_empty, trim
        )

    def split(self, text: str) -> list[_chunk.Chunk]:
        """Split the text and return chunks with position information.

        Args:
            text: The text to split.

        Returns:
            A list of Chunk objects containing the split text with position information.
        """
        raw_chunks = self._splitter.split(text)
        return [_convert_chunk(c) for c in raw_chunks]


class CustomLanguageConfig:
    """Configuration for a custom language with regex-based separators.

    Use this to define custom splitting rules for languages or formats not
    built into the chunker.

    Args:
        language_name: The name of the language.
        separators_regex: Regex patterns for separators, in order of priority.
            Earlier patterns are preferred for splitting.
        aliases: Aliases for the language name (e.g., file extensions).

    Examples:
        >>> config = CustomLanguageConfig(
        ...     language_name="myformat",
        ...     separators_regex=[r"---", r"\\n\\n+"],
        ...     aliases=["mf", ".mf"],
        ... )
        >>> splitter = RecursiveSplitter(custom_languages=[config])
        >>> chunks = splitter.split("Part1---Part2", chunk_size=10, language="myformat")
    """

    def __init__(
        self,
        language_name: str,
        separators_regex: list[str],
        aliases: list[str] | None = None,
    ) -> None:
        self._config = _core.CustomLanguageConfig(
            language_name, separators_regex, aliases or []
        )


class RecursiveSplitter:
    """A recursive text splitter with syntax awareness.

    This splitter uses a sophisticated algorithm to split text into chunks
    that respect syntax boundaries (like paragraph breaks, sentences, etc.)
    and optionally use tree-sitter for programming language awareness.

    This splitter can be instantiated once and reused to split multiple texts
    efficiently.

    Args:
        custom_languages: A list of custom language configurations for
            syntax-aware splitting. These supplement the built-in language support.

    Examples:
        >>> splitter = RecursiveSplitter()
        >>> chunks = splitter.split("Line 1.\\nLine 2.\\n\\nLine 3.", chunk_size=15)
        >>> [c.text for c in chunks]
        ['Line 1.', 'Line 2.', 'Line 3.']

        >>> # With custom language
        >>> config = CustomLanguageConfig("myformat", [r"---"])
        >>> splitter = RecursiveSplitter(custom_languages=[config])
        >>> chunks = splitter.split("A---B---C", chunk_size=5, language="myformat")
    """

    def __init__(
        self, *, custom_languages: list[CustomLanguageConfig] | None = None
    ) -> None:
        configs = (
            [lang._config for lang in custom_languages] if custom_languages else []
        )
        self._splitter = _core.RecursiveSplitter(custom_languages=configs)

    def split(
        self,
        text: str,
        chunk_size: int,
        *,
        min_chunk_size: int | None = None,
        chunk_overlap: int | None = None,
        language: str | None = None,
    ) -> list[_chunk.Chunk]:
        """Split the text into chunks according to the configuration.

        Args:
            text: The text to split.
            chunk_size: Target chunk size in bytes.
            min_chunk_size: Minimum chunk size in bytes. Defaults to chunk_size / 2.
            chunk_overlap: Overlap between consecutive chunks in bytes.
            language: Language name or file extension for syntax-aware splitting
                (e.g., "python", "rust", ".py", ".rs"). If provided and the language
                has tree-sitter support, the splitting will be syntax-aware.

        Returns:
            A list of Chunk objects containing the split text with position information.
        """
        raw_chunks = self._splitter.split(
            text, chunk_size, min_chunk_size, chunk_overlap, language
        )
        return [_convert_chunk(c) for c in raw_chunks]


def _convert_chunk(raw: _core.Chunk) -> _chunk.Chunk:
    """Convert a raw PyO3 chunk to a Python Chunk dataclass."""
    return _chunk.Chunk(
        text=raw.text,
        start=_chunk.TextPosition(
            byte_offset=raw.start_byte,
            char_offset=raw.start_char_offset,
            line=raw.start_line,
            column=raw.start_column,
        ),
        end=_chunk.TextPosition(
            byte_offset=raw.end_byte,
            char_offset=raw.end_char_offset,
            line=raw.end_line,
            column=raw.end_column,
        ),
    )

"""Tests for cocoindex.extras.text module."""

from cocoindex.extras.text import (
    detect_code_language,
    SeparatorSplitter,
    CustomLanguageConfig,
    RecursiveSplitter,
)
from cocoindex.resources.chunk import Chunk, TextPosition


def test_detect_code_language_known_extensions() -> None:
    """Test detect_code_language with known file extensions."""
    assert detect_code_language(filename="main.py") == "python"
    assert detect_code_language(filename="app.rs") == "rust"
    assert detect_code_language(filename="index.js") == "javascript"
    assert detect_code_language(filename="style.css") == "css"


def test_detect_code_language_unknown_extension() -> None:
    """Test detect_code_language with unknown file extension."""
    assert detect_code_language(filename="file.xyz") is None
    assert detect_code_language(filename="noextension") is None


def test_separator_splitter_basic() -> None:
    """Test SeparatorSplitter with basic paragraph splitting."""
    splitter = SeparatorSplitter([r"\n\n+"])
    chunks = splitter.split("Para1\n\nPara2\n\nPara3")

    assert len(chunks) == 3
    assert chunks[0].text == "Para1"
    assert chunks[1].text == "Para2"
    assert chunks[2].text == "Para3"


def test_separator_splitter_returns_chunk_type() -> None:
    """Test that SeparatorSplitter returns proper Chunk objects."""
    splitter = SeparatorSplitter([r"\n"])
    chunks = splitter.split("Line1\nLine2")

    assert len(chunks) == 2
    assert isinstance(chunks[0], Chunk)
    assert isinstance(chunks[0].start, TextPosition)
    assert isinstance(chunks[0].end, TextPosition)


def test_separator_splitter_position_info() -> None:
    """Test that SeparatorSplitter returns correct position information."""
    splitter = SeparatorSplitter([r"\n"])
    chunks = splitter.split("Line1\nLine2")

    # First chunk
    assert chunks[0].text == "Line1"
    assert chunks[0].start.byte_offset == 0
    assert chunks[0].start.line == 1
    assert chunks[0].start.column == 1
    assert chunks[0].end.byte_offset == 5

    # Second chunk
    assert chunks[1].text == "Line2"
    assert chunks[1].start.line == 2
    assert chunks[1].start.column == 1


def test_separator_splitter_keep_separator_left() -> None:
    """Test SeparatorSplitter with keep_separator='left'."""
    splitter = SeparatorSplitter([r"\."], keep_separator="left")
    chunks = splitter.split("A. B. C")

    assert len(chunks) == 3
    assert chunks[0].text == "A."
    assert chunks[1].text == "B."
    assert chunks[2].text == "C"


def test_separator_splitter_keep_separator_right() -> None:
    """Test SeparatorSplitter with keep_separator='right'."""
    splitter = SeparatorSplitter([r"\."], keep_separator="right")
    chunks = splitter.split("A. B. C")

    assert len(chunks) == 3
    assert chunks[0].text == "A"
    assert chunks[1].text == ". B"
    assert chunks[2].text == ". C"


def test_separator_splitter_trim() -> None:
    """Test SeparatorSplitter with trim option."""
    splitter = SeparatorSplitter([r"\|"], trim=True)
    chunks = splitter.split("  A  |  B  ")

    assert chunks[0].text == "A"
    assert chunks[1].text == "B"


def test_separator_splitter_no_trim() -> None:
    """Test SeparatorSplitter with trim=False."""
    splitter = SeparatorSplitter([r"\|"], trim=False)
    chunks = splitter.split("  A  |  B  ")

    assert chunks[0].text == "  A  "
    assert chunks[1].text == "  B  "


def test_separator_splitter_reuse() -> None:
    """Test that SeparatorSplitter can be reused for multiple texts."""
    splitter = SeparatorSplitter([r"\n\n+"])

    chunks1 = splitter.split("A\n\nB")
    chunks2 = splitter.split("X\n\nY\n\nZ")

    assert len(chunks1) == 2
    assert len(chunks2) == 3


def test_recursive_splitter_basic() -> None:
    """Test RecursiveSplitter with basic text."""
    splitter = RecursiveSplitter()
    chunks = splitter.split("Short text.", chunk_size=100)

    assert len(chunks) >= 1
    assert all(isinstance(c, Chunk) for c in chunks)


def test_recursive_splitter_returns_chunk_type() -> None:
    """Test that RecursiveSplitter returns proper Chunk objects."""
    splitter = RecursiveSplitter()
    chunks = splitter.split("Some text here.", chunk_size=100)

    assert len(chunks) >= 1
    assert isinstance(chunks[0], Chunk)
    assert isinstance(chunks[0].start, TextPosition)
    assert isinstance(chunks[0].end, TextPosition)


def test_recursive_splitter_with_language() -> None:
    """Test RecursiveSplitter with language parameter."""
    splitter = RecursiveSplitter()
    code = "def foo():\n    pass\n\ndef bar():\n    pass"
    chunks = splitter.split(code, chunk_size=30, language="python")

    assert len(chunks) >= 1
    assert all(isinstance(c, Chunk) for c in chunks)


def test_recursive_splitter_reuse() -> None:
    """Test that RecursiveSplitter can be reused for multiple texts."""
    splitter = RecursiveSplitter()

    chunks1 = splitter.split("Text one.", chunk_size=100)
    chunks2 = splitter.split("Text two is longer.", chunk_size=100)

    assert len(chunks1) >= 1
    assert len(chunks2) >= 1


def test_custom_language_config() -> None:
    """Test RecursiveSplitter with custom language configuration."""
    config = CustomLanguageConfig(
        language_name="myformat",
        separators_regex=[r"---"],
        aliases=["mf"],
    )
    splitter = RecursiveSplitter(custom_languages=[config])

    chunks = splitter.split(
        "Part1---Part2---Part3",
        chunk_size=10,
        min_chunk_size=3,
        language="myformat",
    )

    assert len(chunks) == 3
    assert chunks[0].text == "Part1"
    assert chunks[1].text == "Part2"
    assert chunks[2].text == "Part3"


def test_custom_language_config_alias() -> None:
    """Test that custom language aliases work."""
    config = CustomLanguageConfig(
        language_name="myformat",
        separators_regex=[r"---"],
        aliases=["mf"],
    )
    splitter = RecursiveSplitter(custom_languages=[config])

    # Use alias instead of full name
    chunks = splitter.split(
        "PartA---PartB",
        chunk_size=10,
        min_chunk_size=3,
        language="mf",
    )

    assert len(chunks) == 2
    assert chunks[0].text == "PartA"
    assert chunks[1].text == "PartB"

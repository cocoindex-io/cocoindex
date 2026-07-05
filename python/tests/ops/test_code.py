"""Tests for cocoindex.ops.code module."""

import pytest

from pathlib import Path

from cocoindex.ops.code import (
    CodeMatch,
    CodePattern,
    CodeSource,
    index_terms,
    match_code,
)
from cocoindex.resources.chunk import Chunk

_PY_SRC = "def foo(a, b):\n    return a + b\n\ndef bar(x):\n    return x\n"
_DEF_PATTERN = r"def \NAME(\(ARGS*\)):"


def _cap(m: CodeMatch, name: str) -> str:
    """Text of a single-chunk capture (asserts exactly one chunk)."""
    (chunk,) = m.captures[name]
    return chunk.text


def test_code_source_properties() -> None:
    src = CodeSource(_PY_SRC, language="python")
    assert src.text == _PY_SRC
    assert src.language == "python"
    assert CodeSource("plain text").language is None


def test_code_source_unknown_language_never_raises() -> None:
    # Construction is tolerant: consumers degrade (or raise at call time when
    # they genuinely require an AST).
    src = CodeSource("hello world", language="not-a-language")
    assert src.language == "not-a-language"


def test_matches_returns_dataclasses_with_captures() -> None:
    matches = match_code(_DEF_PATTERN, CodeSource(_PY_SRC, language="python"))
    assert all(isinstance(m, CodeMatch) for m in matches)
    by_name = {_cap(m, "NAME"): m for m in matches}
    assert set(by_name) == {"foo", "bar"}
    assert _cap(by_name["foo"], "ARGS") == "a, b"
    assert by_name["foo"].kind == "function_definition"
    # exactly one chunk today (the whole matched node), carrying text + positions
    foo = by_name["foo"]
    assert len(foo.chunks) == 1
    assert isinstance(foo.chunks[0], Chunk)
    assert foo.chunks[0].text.startswith("def foo(a, b):")
    assert foo.chunks[0].start.line == 1
    assert by_name["bar"].chunks[0].start.line == 4  # third line is blank
    # captures carry positions too
    assert foo.captures["NAME"][0].start.line == 1


def test_one_parse_many_patterns() -> None:
    """A single CodeSource can be matched against multiple patterns (reused parse)."""
    src = CodeSource(_PY_SRC, language="python")
    assert {_cap(m, "NAME") for m in match_code(_DEF_PATTERN, src)} == {"foo", "bar"}
    assert {_cap(m, "X") for m in match_code(r"return \X", src)} >= {"a + b", "x"}


def test_match_code_one_shot_str() -> None:
    matches = match_code(_DEF_PATTERN, _PY_SRC, "python")
    assert {_cap(m, "NAME") for m in matches} == {"foo", "bar"}
    # a str source requires a language
    with pytest.raises(ValueError, match="language"):
        match_code(_DEF_PATTERN, _PY_SRC)
    # ... and a CodeSource rejects one (it carries its own)
    with pytest.raises(ValueError, match="language"):
        match_code(_DEF_PATTERN, CodeSource(_PY_SRC, language="python"), "python")


def test_language_alias() -> None:
    # "c++" alias resolves for both parsing and matching.
    src = CodeSource("int main() { return 0; }", language="c++")
    assert _cap(match_code(r"return \V;", src)[0], "V") == "0"


def test_matching_unknown_language_raises() -> None:
    with pytest.raises(ValueError):
        match_code(r"\X", "x = 1", "nonsense-lang")
    with pytest.raises(ValueError):
        match_code(r"\X", CodeSource("x = 1", language="nonsense-lang"))


def test_matching_unsupported_language_raises() -> None:
    # Markdown can be parsed/split but has no structural matcher.
    with pytest.raises(ValueError):
        match_code(r"\X", CodeSource("# hello", language="markdown"))


def test_malformed_pattern_raises() -> None:
    with pytest.raises(ValueError):
        # invalid regex matcher (unterminated char class)
        CodePattern(r"\(/[/\)", language="python")
    with pytest.raises(ValueError):
        match_code(r"\(/[/\)", _PY_SRC, "python")


def test_code_pattern_reused_across_sources() -> None:
    # Compile once, match against many sources — same results as one-shots.
    cp = CodePattern(_DEF_PATTERN, language="python")
    assert cp.language == "python"
    src = CodeSource(_PY_SRC, language="python")
    via_obj = {_cap(m, "NAME") for m in cp.match_source(src)}
    via_str = {_cap(m, "NAME") for m in match_code(_DEF_PATTERN, src)}
    assert via_obj == via_str == {"foo", "bar"}


def test_code_pattern_match_source_and_might_match() -> None:
    cp = CodePattern(r"return process(\X)", language="python")
    # prefilter: a source without `process` is rejected without parsing
    assert cp.might_match("def f():\n    return process(x)\n")
    assert not cp.might_match("def f():\n    return other(x)\n")
    # match_source agrees with the prefilter and binds captures
    hit = cp.match_source("def f():\n    return process(item)\n")
    assert [_cap(m, "X") for m in hit] == ["item"]
    assert cp.match_source("def f():\n    return other(item)\n") == []


def test_code_source_match_agrees_with_str_match() -> None:
    cp = CodePattern(r"def \NAME(\(A*\)):", language="python")
    via_source = cp.match_source(CodeSource(_PY_SRC, language="python"))
    via_str = cp.match_source(_PY_SRC)
    assert via_source == via_str
    # A CodeSource without (or with a different) language still matches: the
    # pattern's own grammar is used, same as the str path.
    assert cp.match_source(CodeSource(_PY_SRC)) == via_str


def test_code_source_shared_by_matcher_and_splitter() -> None:
    """One CodeSource handle serves pattern matching and splitting (one parse)."""
    from cocoindex.ops.text import RecursiveSplitter

    src = CodeSource(_PY_SRC, language="python")
    cp = CodePattern(r"def \NAME(\(A*\)):", language="python")
    assert {_cap(m, "NAME") for m in cp.match_source(src)} == {"foo", "bar"}
    chunks = RecursiveSplitter().split(src, chunk_size=1000)
    assert chunks and chunks[0].text.startswith("def foo")


def test_code_pattern_match_file(tmp_path: Path) -> None:
    cp = CodePattern(r"def \NAME(\(A*\)):", language="python")

    hit = tmp_path / "hit.py"
    # newline="" so the bytes round-trip verbatim (no `\n`→`\r\n` on Windows).
    hit.write_text(_PY_SRC, newline="")
    fm = cp.match_file(str(hit))
    assert fm is not None
    assert fm.path == str(hit)
    assert fm.source.text == _PY_SRC  # the parsed source is bundled
    assert fm.source.language == "python"
    assert {_cap(m, "NAME") for m in fm.matches} == {"foo", "bar"}
    # the bundled source is reusable without re-parsing
    from cocoindex.ops.text import RecursiveSplitter

    assert RecursiveSplitter().split(fm.source, chunk_size=1000)

    # a file with no match → None (and binary/missing handled gracefully)
    miss = tmp_path / "miss.py"
    miss.write_text("x = 1\n", newline="")
    assert cp.match_file(str(miss)) is None

    binary = tmp_path / "blob.bin"
    binary.write_bytes(b"\xff\xfe\x00\x01def foo():")
    assert cp.match_file(str(binary)) is None  # non-utf8 skipped


def test_comments_in_source_are_ignored() -> None:
    # Code written inside a comment must not match; a comment is transparent to
    # the matcher.
    src = "# bar(commented)\ndef g():\n    return bar(real)  # bar(also_commented)\n"
    args = [
        _cap(m, "X") for m in match_code(r"bar(\X)", CodeSource(src, language="python"))
    ]
    assert args == ["real"]  # the two `bar(...)` in comments do not match


def test_index_terms_str_and_code_source() -> None:
    src_text = 'def handler(req):\n    return process(req, "payload")\n'
    want = {"handler", "req", "process", "payload"}
    assert want <= set(index_terms(src_text, language="python"))
    # the CodeSource path reuses the parse and agrees
    src = CodeSource(src_text, language="python")
    assert sorted(index_terms(src)) == sorted(index_terms(src_text, language="python"))
    # keywords excluded
    assert "def" not in index_terms(src)
    assert "return" not in index_terms(src)
    # a str source requires a language; unknown languages raise (silently
    # indexing nothing would poison a prefilter index with false negatives)
    with pytest.raises(ValueError, match="language"):
        index_terms(src_text)
    with pytest.raises(ValueError):
        index_terms(src_text, language="nonsense-lang")
    with pytest.raises(ValueError):
        index_terms(CodeSource(src_text, language="nonsense-lang"))
    with pytest.raises(ValueError, match="language"):
        index_terms(src, language="python")  # CodeSource carries its own

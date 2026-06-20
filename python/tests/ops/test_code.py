"""Tests for cocoindex.ops.code module."""

import pytest

from cocoindex.ops.code import CodeAst, CodeMatch, CodePattern, match_code
from cocoindex.resources.chunk import Chunk

_PY_SRC = "def foo(a, b):\n    return a + b\n\ndef bar(x):\n    return x\n"


def _cap(m: CodeMatch, name: str) -> str:
    """Text of a single-chunk capture (asserts exactly one chunk)."""
    (chunk,) = m.captures[name]
    return chunk.text


def test_codeast_properties() -> None:
    ast = CodeAst(_PY_SRC, language="python")
    assert ast.language == "python"
    assert ast.source == _PY_SRC


def test_matches_returns_dataclasses_with_captures() -> None:
    ast = CodeAst(_PY_SRC, language="python")
    matches = ast.matches(r"def \NAME(\(ARGS*)):")
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
    """A single CodeAst can be matched against multiple patterns (reused parse)."""
    ast = CodeAst(_PY_SRC, language="python")
    assert {_cap(m, "NAME") for m in ast.matches(r"def \NAME(\(ARGS*)):")} == {
        "foo",
        "bar",
    }
    assert {_cap(m, "X") for m in ast.matches(r"return \X")} >= {"a + b", "x"}


def test_split_returns_chunks() -> None:
    ast = CodeAst(_PY_SRC, language="python")
    chunks = ast.split(chunk_size=30)
    assert chunks  # non-empty
    assert all(isinstance(c, Chunk) for c in chunks)
    # chunk text is sliced from the source and positions are populated
    assert all(c.text for c in chunks)
    assert chunks[0].start.line == 1


def test_match_code_one_shot() -> None:
    matches = match_code(r"def \NAME(\(ARGS*)):", _PY_SRC, "python")
    assert {_cap(m, "NAME") for m in matches} == {"foo", "bar"}


def test_language_alias() -> None:
    # "c++" alias resolves for both parsing and matching.
    src = "int main() { return 0; }"
    ast = CodeAst(src, language="c++")
    assert _cap(ast.matches(r"return \V;")[0], "V") == "0"


def test_unknown_language_raises() -> None:
    with pytest.raises(ValueError):
        CodeAst("x = 1", language="nonsense-lang")


def test_matching_unsupported_language_raises() -> None:
    # Markdown can be parsed/split but has no structural matcher.
    ast = CodeAst("# hello", language="markdown")
    with pytest.raises(ValueError):
        ast.matches(r"\X")


def test_malformed_pattern_raises() -> None:
    ast = CodeAst(_PY_SRC, language="python")
    with pytest.raises(ValueError):
        ast.matches(r"\(:/[/)")  # unterminated regex matcher


def test_code_pattern_reused_across_asts() -> None:
    # Compile once, match against many ASTs — same results as passing the string.
    cp = CodePattern(r"def \NAME(\(ARGS*)):", language="python")
    assert cp.language == "python"
    ast = CodeAst(_PY_SRC, language="python")
    via_obj = {_cap(m, "NAME") for m in ast.matches(cp)}
    via_str = {_cap(m, "NAME") for m in ast.matches(r"def \NAME(\(ARGS*)):")}
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


def test_code_pattern_language_mismatch_raises() -> None:
    cp = CodePattern(r"return \X;", language="c++")
    ast = CodeAst(_PY_SRC, language="python")
    with pytest.raises(ValueError):
        ast.matches(cp)

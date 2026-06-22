//! Integration tests for `ops::code` — structural code matching.
//!
//! Each test exercises the public SDK API: `CodeAst`, `CodePattern`,
//! `match_code`, `index_terms`, `FileMatch`.  We use short Python and Rust
//! snippets because both grammars are always present in the code_match crate.

use cocoindex::ops::code::{CodeAst, CodePattern, FileMatch, match_code, index_terms};

// ─── match_code (one-shot free function) ────────────────────────────────────

#[test]
fn match_code_finds_python_function_defs() {
    let src = "def foo(x): return x\ndef bar(a, b): pass";
    let ms = match_code(r"def \NAME(\(A*\)):", src, "python").unwrap();
    assert_eq!(ms.len(), 2, "expected two matches, got {}", ms.len());
    let names: Vec<&str> = ms
        .iter()
        .map(|m| m.captures["NAME"][0].text(src))
        .collect();
    assert!(names.contains(&"foo"), "missing 'foo' in {names:?}");
    assert!(names.contains(&"bar"), "missing 'bar' in {names:?}");
}

#[test]
fn match_code_rust_fn_capture() {
    let src = "fn greet(name: &str) -> String { name.to_string() }";
    let ms = match_code(r"fn \NAME(\(P*\))", src, "rust").unwrap();
    assert!(!ms.is_empty());
    assert_eq!(ms[0].captures["NAME"][0].text(src), "greet");
}

#[test]
fn match_code_no_match_returns_empty() {
    let src = "x = 1 + 2";
    let ms = match_code(r"def \NAME(\(A*\)):", src, "python").unwrap();
    assert!(ms.is_empty());
}

#[test]
fn match_code_returns_kind() {
    let src = "def f(x): pass";
    let ms = match_code(r"def \NAME(\(A*\)):", src, "python").unwrap();
    assert!(!ms.is_empty(), "expected a match for a Python function definition");
    // tree-sitter node kind for a Python function definition
    assert_eq!(ms[0].kind, "function_definition");
}

#[test]
fn match_code_chunk_positions_sane() {
    let src = "x = 1\ndef foo(z): pass\ny = 2";
    let ms = match_code(r"def \NAME(\(A*\)):", src, "python").unwrap();
    assert_eq!(ms.len(), 1);
    let chunk = &ms[0].chunks[0];
    // "def foo(z): pass" starts at byte offset 6 (after "x = 1\n")
    assert_eq!(chunk.start.byte_offset, 6, "function should start at byte 6");
    assert_eq!(chunk.start.line, 2, "function starts on line 2");
    assert_eq!(chunk.start.column, 1, "column should be 1-based");
}

#[test]
fn match_code_unknown_language_errors() {
    let result = match_code(r"x", "src", "brainfuck");
    assert!(result.is_err(), "expected error for unknown language");
    let msg = result.err().expect("expected error").to_string();
    assert!(msg.contains("brainfuck"), "error should mention the bad language: {msg}");
}

// ─── CodePattern ─────────────────────────────────────────────────────────────

#[test]
fn code_pattern_might_match_prefilter() {
    // Use a pattern with a long, non-keyword concrete identifier so the prefilter
    // has a required term to filter on. "evaluate_model" (14 chars) must appear in
    // source for this pattern to possibly match.
    let pat = CodePattern::compile(r"evaluate_model(\(A*\))", "python").unwrap();
    assert!(pat.might_match("evaluate_model(x, y)"));
    assert!(!pat.might_match("x = 1 + foo()"));
}

#[test]
fn code_pattern_def_might_match_is_conservative() {
    // The `def \NAME(...)` pattern has no non-keyword required terms so the
    // prefilter always passes (it's conservative: never a false negative).
    let pat = CodePattern::compile(r"def \NAME(\(A*\)):", "python").unwrap();
    assert!(pat.might_match("def foo(): pass"));
    // The prefilter may pass even for non-matching sources — that's correct
    // behaviour; the full parse will then confirm there's no match.
    let ms = pat.match_source("x = 1");
    assert!(ms.is_empty(), "no match should be found by full parse");
}

#[test]
fn code_pattern_match_source_finds_matches() {
    let pat = CodePattern::compile(r"def \NAME(\(A*\)):", "python").unwrap();
    let src = "def alpha():\n    pass\n\ndef beta(x, y):\n    return x + y";
    let ms = pat.match_source(src);
    assert_eq!(ms.len(), 2);
    let mut names: Vec<&str> = ms
        .iter()
        .map(|m| m.captures["NAME"][0].text(src))
        .collect();
    names.sort_unstable();
    assert_eq!(names, ["alpha", "beta"]);
}

#[test]
fn code_pattern_match_source_respects_prefilter() {
    // "ZZZUNLIKELY" won't appear in the prefilter index for "x = 1"
    let pat = CodePattern::compile(r"ZZZUNLIKELY", "python").unwrap();
    // prefilter should reject before parsing
    assert!(!pat.might_match("x = 1"));
    let ms = pat.match_source("x = 1");
    assert!(ms.is_empty());
}

#[test]
fn code_pattern_language_accessor() {
    let pat = CodePattern::compile(r"def \F(\(A*\)):", "python").unwrap();
    assert_eq!(pat.language(), "python");
}

#[test]
fn code_pattern_match_file_finds_match() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sample.py");
    std::fs::write(&path, "def compute(x):\n    return x * 2\n").unwrap();

    let pat = CodePattern::compile(r"def \NAME(\(A*\)):", "python").unwrap();
    let result = pat.match_file(path.to_str().unwrap()).unwrap();
    let fm: FileMatch = result.expect("expected a FileMatch");
    assert_eq!(fm.matches.len(), 1);
    assert_eq!(fm.matches[0].captures["NAME"][0].text(fm.ast.source()), "compute");
    assert_eq!(fm.path, path.to_str().unwrap());
}

#[test]
fn code_pattern_match_file_no_match_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("no_match.py");
    std::fs::write(&path, "x = 42\n").unwrap();

    let pat = CodePattern::compile(r"def \NAME(\(A*\)):", "python").unwrap();
    let result = pat.match_file(path.to_str().unwrap()).unwrap();
    assert!(result.is_none());
}

#[test]
fn code_pattern_match_file_binary_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("binary.bin");
    // Write bytes that are invalid UTF-8
    std::fs::write(&path, b"\xff\xfe\x00\x01binary data").unwrap();

    let pat = CodePattern::compile(r"def \NAME(\(A*\)):", "python").unwrap();
    let result = pat.match_file(path.to_str().unwrap()).unwrap();
    assert!(result.is_none(), "binary file should return None");
}

#[test]
fn code_pattern_bad_pattern_errors() {
    let result = CodePattern::compile(r"def \(UNCLOSED:", "python");
    assert!(result.is_err(), "expected compilation error for malformed pattern");
}

#[test]
fn code_pattern_unsupported_language_errors() {
    let result = CodePattern::compile(r"x", "made_up_language");
    assert!(result.is_err(), "expected error for unsupported language");
    let msg = result.err().expect("expected error").to_string();
    assert!(msg.contains("made_up_language"), "error should mention the bad language: {msg}");
}

#[test]
fn code_pattern_min_len_tunes_prefilter() {
    // With min_len=1 the prefilter is more aggressive (more required terms)
    let pat_strict = CodePattern::new(r"def \N(\(A*\)):", "python", 1).unwrap();
    // With min_len=10 short terms are dropped — fewer required terms
    let pat_relaxed = CodePattern::new(r"def \N(\(A*\)):", "python", 10).unwrap();

    // Both should still find the same matches; the only difference is prefilter speed
    let src = "def f(): pass";
    assert_eq!(pat_strict.match_source(src).len(), pat_relaxed.match_source(src).len());
}

// ─── CodeAst ─────────────────────────────────────────────────────────────────

#[test]
fn code_ast_language_and_source_accessors() {
    let ast = CodeAst::new("def f(): pass", "python").unwrap();
    assert_eq!(ast.language(), "python");
    assert_eq!(ast.source(), "def f(): pass");
}

#[test]
fn code_ast_matches_str_pattern() {
    let src = "def alpha(): pass\ndef beta(x): return x";
    let ast = CodeAst::new(src, "python").unwrap();
    let ms = ast.matches(r"def \NAME(\(A*\)):").unwrap();
    assert_eq!(ms.len(), 2);
    let names: Vec<&str> = ms.iter().map(|m| m.captures["NAME"][0].text(src)).collect();
    assert!(names.contains(&"alpha"));
    assert!(names.contains(&"beta"));
}

#[test]
fn code_ast_matches_with_compiled_pattern() {
    let src = "fn add(a: i32, b: i32) -> i32 { a + b }";
    let ast = CodeAst::new(src, "rust").unwrap();
    let pat = CodePattern::compile(r"fn \NAME(\(P*\))", "rust").unwrap();
    let ms = ast.matches_with(&pat).unwrap();
    assert_eq!(ms.len(), 1);
    assert_eq!(ms[0].captures["NAME"][0].text(src), "add");
}

#[test]
fn code_ast_matches_with_language_mismatch_errors() {
    let ast = CodeAst::new("def f(): pass", "python").unwrap();
    let pat = CodePattern::compile(r"fn \N(\(P*\))", "rust").unwrap();
    let result = ast.matches_with(&pat);
    assert!(result.is_err(), "expected error for language mismatch");
    let msg = result.err().expect("expected error").to_string();
    assert!(
        msg.contains("python") || msg.contains("rust"),
        "error should mention the conflicting languages: {msg}"
    );
}

#[test]
fn code_ast_matches_same_grammar_alias() {
    // "cpp" and "c++" should be treated as the same grammar
    let src = "void foo(int x) {}";
    let ast = CodeAst::new(src, "cpp").unwrap();
    let pat = CodePattern::compile(r"void \NAME(\(P*\))", "c++").unwrap();
    // language aliases map to the same tree-sitter grammar — should succeed
    let ms = ast.matches_with(&pat).unwrap();
    assert_eq!(ms.len(), 1);
}

#[test]
fn code_ast_reuses_parse_across_patterns() {
    let src = "def f(x): return x\nclass C: pass";
    let ast = CodeAst::new(src, "python").unwrap();

    // Match functions
    let fns = ast.matches(r"def \NAME(\(A*\)):").unwrap();
    assert_eq!(fns.len(), 1);
    assert_eq!(fns[0].captures["NAME"][0].text(src), "f");

    // Match classes — reuses the same parse
    let classes = ast.matches(r"class \NAME:").unwrap();
    assert_eq!(classes.len(), 1);
    assert_eq!(classes[0].captures["NAME"][0].text(src), "C");
}

#[test]
fn code_ast_split_produces_nonempty_chunks() {
    let src = "def alpha():\n    return 1\n\ndef beta():\n    return 2\n";
    let ast = CodeAst::new(src, "python").unwrap();
    let chunks = ast.split(50, None, None).unwrap();
    assert!(!chunks.is_empty(), "split should return at least one chunk");
    // Chunks should cover the whole source
    let covered: usize = chunks.iter().map(|c| c.range().len()).sum();
    assert!(covered > 0);
}

#[test]
fn code_ast_split_chunk_text_is_source_slice() {
    let src = "x = 1\ny = 2\nz = 3\n";
    let ast = CodeAst::new(src, "python").unwrap();
    let chunks = ast.split(10, None, None).unwrap();
    for chunk in &chunks {
        let text = chunk.text(src);
        // The text should be a non-empty substring of the source
        assert!(!text.is_empty());
        assert!(src.contains(text), "chunk text {text:?} not in source");
    }
}

#[test]
fn code_ast_split_chunk_positions_are_accurate() {
    // Three-line Python source; verify start/end positions of first chunk.
    let src = "a = 1\nb = 2\nc = 3\n";
    let ast = CodeAst::new(src, "python").unwrap();
    let chunks = ast.split(200, None, None).unwrap(); // one big chunk
    assert!(!chunks.is_empty());
    let first = &chunks[0];
    assert_eq!(first.start.byte_offset, first.range().start);
    assert_eq!(first.end.byte_offset, first.range().end);
    assert!(first.start.line >= 1);
    assert!(first.end.line >= first.start.line);
}

#[test]
fn code_ast_index_terms_extracts_identifiers() {
    let src = "def compute_total(price, tax):\n    return price + tax";
    let ast = CodeAst::new(src, "python").unwrap();
    let terms = ast.index_terms(3);
    assert!(terms.contains(&"compute_total".to_string()), "terms: {terms:?}");
    assert!(terms.contains(&"price".to_string()), "terms: {terms:?}");
    assert!(terms.contains(&"return".to_string()) || terms.contains(&"tax".to_string()), "terms: {terms:?}");
    // "def" is a keyword — it may or may not appear depending on language config
}

#[test]
fn code_ast_index_terms_min_len_filters() {
    let src = "x = foo_long_name";
    let ast = CodeAst::new(src, "python").unwrap();
    let terms_long = ast.index_terms(5);
    let terms_short = ast.index_terms(1);
    // "foo_long_name" should appear at both min_lens
    assert!(terms_long.contains(&"foo_long_name".to_string()));
    // With min_len=1, "x" might appear; with min_len=5 it shouldn't
    assert!(!terms_long.iter().any(|t| t == "x"), "short id 'x' should be filtered at min_len=5");
    assert!(terms_short.len() >= terms_long.len(), "fewer terms with longer min_len");
}

#[test]
fn code_ast_unknown_language_errors() {
    let result = CodeAst::new("x", "notareallangjkjk");
    assert!(result.is_err(), "expected error for unknown language");
    let msg = result.err().expect("expected error").to_string();
    assert!(msg.contains("notareallangjkjk"), "error should mention the bad language: {msg}");
}

// ─── index_terms (free function) ─────────────────────────────────────────────

#[test]
fn index_terms_one_shot() {
    let src = "func main() { println!(\"{}\", x_val) }";
    let terms = index_terms(src, "rust", 3).unwrap();
    assert!(terms.contains(&"main".to_string()), "terms: {terms:?}");
    assert!(terms.contains(&"x_val".to_string()), "terms: {terms:?}");
}

#[test]
fn index_terms_deduplicates() {
    let src = "foo + foo + foo";
    let terms = index_terms(src, "python", 1).unwrap();
    let foo_count = terms.iter().filter(|t| t.as_str() == "foo").count();
    assert_eq!(foo_count, 1, "index_terms should deduplicate");
}

#[test]
fn index_terms_unknown_language_errors() {
    let result = index_terms("x", "cobol_2025", 3);
    assert!(result.is_err(), "expected error for unknown language");
    let msg = result.err().expect("expected error").to_string();
    assert!(msg.contains("cobol_2025"), "error should mention the bad language: {msg}");
}

// ─── Language aliases ─────────────────────────────────────────────────────────

#[test]
fn language_aliases_accepted() {
    // Python: "py" alias
    let ms = match_code(r"def \N(\(A*\)):", "def f(): pass", "py").unwrap();
    assert_eq!(ms.len(), 1);

    // C++: "c++" and "cpp" aliases
    let ms = match_code(r"void \N(\(P*\))", "void run() {}", "c++").unwrap();
    assert_eq!(ms.len(), 1);
    let ms2 = match_code(r"void \N(\(P*\))", "void run() {}", "cpp").unwrap();
    assert_eq!(ms2.len(), 1);

    // Go
    let ms = match_code(r"func \N(\(P*\))", "func main() {}", "go").unwrap();
    assert_eq!(ms.len(), 1);
}

// ─── Multiple captures ────────────────────────────────────────────────────────

#[test]
fn multiple_captures_extracted() {
    // Two single-node named captures in one pattern.
    // In a Python assignment `x = 42`, the assignment node has the identifier
    // and the integer as siblings separated by `=`, so both are captured.
    let src = "score = 100";
    let ast = CodeAst::new(src, "python").unwrap();
    let ms = ast.matches(r"\VAR = \VALUE").unwrap();
    assert_eq!(ms.len(), 1, "expected one match for assignment; got {}", ms.len());
    let m = &ms[0];
    assert_eq!(m.captures["VAR"][0].text(src), "score");
    assert_eq!(m.captures["VALUE"][0].text(src), "100");
}

// ─── FileMatch struct ─────────────────────────────────────────────────────────

#[test]
fn file_match_ast_can_split_and_index() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("logic.py");
    std::fs::write(
        &path,
        "def add(a, b):\n    return a + b\n\ndef mul(a, b):\n    return a * b\n",
    )
    .unwrap();

    let pat = CodePattern::compile(r"def \NAME(\(A*\)):", "python").unwrap();
    let fm = pat
        .match_file(path.to_str().unwrap())
        .unwrap()
        .expect("expected matches");

    // Two function definitions
    assert_eq!(fm.matches.len(), 2);

    // The embedded AST can split without re-parsing
    let chunks = fm.ast.split(40, None, None).unwrap();
    assert!(!chunks.is_empty());

    // And index terms without re-parsing
    let terms = fm.ast.index_terms(3);
    assert!(terms.contains(&"add".to_string()), "terms: {terms:?}");
    assert!(terms.contains(&"mul".to_string()), "terms: {terms:?}");
}


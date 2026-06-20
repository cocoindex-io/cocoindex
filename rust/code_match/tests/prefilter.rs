//! Prefilter tests: required-content extraction + the index-free `might_match`
//! scan. The load-bearing property is **no false negatives** — anything that
//! actually matches must pass the prefilter.

use std::collections::HashSet;

use cocoindex_code_match::{Boundary, Pattern, Prefilter, index_terms, lang};

fn prefilter(pat: &str, min_len: usize) -> Prefilter {
    Pattern::compile(pat, &lang::python())
        .expect("valid pattern")
        .prefilter(min_len)
}

fn terms(src: &str, min_len: usize) -> HashSet<String> {
    index_terms(src, &lang::python(), min_len)
        .into_iter()
        .collect()
}

#[test]
fn identifier_terms_are_word_bounded() {
    let pf = prefilter(r"foobar(\X)", 3);
    assert!(pf.might_match("y = foobar(z)"));
    assert!(!pf.might_match("y = other(z)")); // foobar absent
    assert!(!pf.might_match("y = foobarbaz(z)")); // word boundary: foobar not a whole token
}

#[test]
fn keywords_and_punct_are_not_required() {
    // `if`/`return` are keywords, `(`/`)`/`:` punctuation → not extracted. Only the
    // identifiers `cond` and `val` are required, so a structurally different file
    // with both still passes.
    let pf = prefilter(r"if cond: return val", 3);
    assert!(pf.might_match("while cond:\n    yield val"));
    assert!(!pf.might_match("if cond: pass")); // `val` missing
}

#[test]
fn string_content_runs_required() {
    let pf = prefilter(r#""hello world""#, 3);
    assert!(pf.might_match(r#"msg = "say hello to the world""#)); // both runs present
    assert!(!pf.might_match(r#"msg = "hello there""#)); // `world` missing
}

#[test]
fn regex_substring_literal() {
    // `.*foobar.*` → required substring `foobar`, NOT word-bounded.
    let pf = prefilter(r"\(:/.*foobar.*/)", 3);
    assert!(pf.might_match("name = xfoobary")); // mid-token substring is fine
    assert!(!pf.might_match("name = foo_bar")); // no contiguous `foobar`
}

#[test]
fn regex_alternation_is_a_disjunction() {
    let pf = prefilter(r"\(:/get|set/)", 3);
    assert!(pf.might_match("o.getValue()")); // get
    assert!(pf.might_match("o.setValue()")); // set
    assert!(!pf.might_match("o.value()")); // neither
}

#[test]
fn min_len_drops_short_terms() {
    // `io` (2 chars) is dropped at min_len 3 → not required; `read` (4) stays.
    let pf = prefilter(r"io.read(\X)", 3);
    assert!(pf.might_match("xyz.read(q)")); // io not required
    assert!(!pf.might_match("io.write(q)")); // read missing
}

#[test]
fn empty_prefilter_passes_everything() {
    // Pure metavars + punctuation → no extractable terms → always "maybe".
    let pf = prefilter(r"\A = \B", 3);
    assert!(pf.clauses().is_empty());
    assert!(pf.might_match("literally anything"));
}

#[test]
fn no_false_negative_on_a_real_match() {
    // The core soundness property: a source the precise matcher accepts must pass
    // the prefilter.
    let src = "def handler(req):\n    return process(req)\n";
    let pat = Pattern::compile(r"return process(\X)", &lang::python()).unwrap();
    assert!(
        !pat.matches(src).is_empty(),
        "sanity: pattern really matches"
    );
    assert!(
        pat.prefilter(3).might_match(src),
        "prefilter must never reject a real match",
    );
}

#[test]
fn index_terms_extracts_identifiers_and_string_content() {
    let t = terms(
        "def handler(req):\n    return process(req, \"hello world\")\n",
        3,
    );
    for want in ["handler", "req", "process", "hello", "world"] {
        assert!(t.contains(want), "expected {want:?} in {t:?}");
    }
    assert!(!t.contains("def"), "keyword must be excluded");
    assert!(!t.contains("return"), "keyword must be excluded");
}

#[test]
fn index_terms_skips_comments_and_short_terms() {
    let t = terms("x = 1  # secret note\nvalue = compute()\n", 3);
    assert!(t.contains("value"));
    assert!(t.contains("compute"));
    assert!(!t.contains("secret"), "comment text must be skipped");
    assert!(!t.contains("note"));
    assert!(!t.contains("x"), "below min_len");
}

#[test]
fn index_terms_supply_every_required_term_of_a_match() {
    // Index-path soundness: every clause a matching pattern requires is satisfiable
    // from the source's index terms (Word => token present; Substring => substring
    // of some indexed token, as an n-gram index would resolve).
    let src = "def f():\n    return process(item, \"payload data\")\n";
    let pat = Pattern::compile(r#"process(\X, "payload data")"#, &lang::python()).unwrap();
    assert!(!pat.matches(src).is_empty(), "sanity: pattern matches");

    let idx = terms(src, 3);
    for clause in pat.prefilter(3).clauses() {
        let ok = clause.any_of.iter().any(|t| match t.boundary {
            Boundary::Word => idx.contains(&t.text),
            Boundary::Substring => idx.iter().any(|term| term.contains(&t.text)),
        });
        assert!(
            ok,
            "index terms miss a required clause {clause:?} (have {idx:?})"
        );
    }
}

#[test]
fn extracted_terms_match_literally_not_as_regex() {
    // A regex matcher's extracted literal contains a `.` (escaped in the regex).
    // `might_match` uses aho-corasick *literal* search, so the `.` matches only a
    // literal dot — no re-interpretation of the term as a regex.
    let pf = prefilter(r"\(:/foo\.bar/)", 3);
    assert!(pf.might_match("x = foo.bar")); // literal `foo.bar`
    assert!(!pf.might_match("x = fooXbar")); // `.` is literal, not "any char"
}

#[test]
fn matches_prefiltered_skips_on_reject_and_agrees_on_accept() {
    let pat = Pattern::compile(r"return process(\X)", &lang::python()).unwrap();
    let pf = pat.prefilter(3);

    // Accept: identical to a plain match.
    let hit = "def f():\n    return process(x)\n";
    assert_eq!(
        pat.matches_prefiltered(hit, &pf).len(),
        pat.matches(hit).len(),
    );
    assert_eq!(pat.matches_prefiltered(hit, &pf).len(), 1);

    // Reject: `process` absent → prefilter short-circuits → empty (no parse needed).
    let miss = "def f():\n    return other(x)\n";
    assert!(!pf.might_match(miss));
    assert!(pat.matches_prefiltered(miss, &pf).is_empty());
}

#[test]
fn index_terms_in_tree_matches_the_parsing_variant() {
    use tree_sitter::Parser;
    let src = "def f():\n    return process(item, \"payload\")\n";
    let cfg = lang::python();
    let mut parser = Parser::new();
    parser.set_language(&cfg.language).unwrap();
    let tree = parser.parse(src, None).unwrap();

    let mut a = cocoindex_code_match::index_terms_in_tree(&tree, src, 3);
    let mut b = cocoindex_code_match::index_terms(src, &cfg, 3);
    a.sort();
    b.sort();
    assert_eq!(a, b);
    assert!(a.contains(&"process".to_string()));
}

#[test]
fn clause_structure_is_exposed() {
    let pf = prefilter(r"foobar", 3);
    let clauses = pf.clauses();
    assert_eq!(clauses.len(), 1);
    assert_eq!(clauses[0].any_of.len(), 1);
    assert_eq!(clauses[0].any_of[0].text, "foobar");
    assert_eq!(clauses[0].any_of[0].boundary, Boundary::Word);
}

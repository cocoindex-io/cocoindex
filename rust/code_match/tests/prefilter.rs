//! Prefilter tests: required-content extraction + the index-free `might_match`
//! scan. The load-bearing property is **no false negatives** — anything that
//! actually matches must pass the prefilter.

use cocoindex_code_match::{Boundary, Pattern, Prefilter, lang};

fn prefilter(pat: &str, min_len: usize) -> Prefilter {
    Pattern::compile(pat, &lang::python())
        .expect("valid pattern")
        .prefilter(min_len)
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
fn clause_structure_is_exposed() {
    let pf = prefilter(r"foobar", 3);
    let clauses = pf.clauses();
    assert_eq!(clauses.len(), 1);
    assert_eq!(clauses[0].any_of.len(), 1);
    assert_eq!(clauses[0].any_of[0].text, "foobar");
    assert_eq!(clauses[0].any_of[0].boundary, Boundary::Word);
}

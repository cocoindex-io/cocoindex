//! Shared helpers for the integration tests.
#![allow(dead_code)] // each test crate uses a subset

use cocoindex_code_match::{LangConfig, Match, Pattern};

/// Compile `pat` for `cfg` and match it against `src`. Also cross-checks the
/// **prefilter** on every call: it must never reject a source that actually matches
/// (soundness — no false negatives), and `matches_prefiltered` must agree with the
/// plain run. So every feature test doubles as a prefilter soundness test for free.
/// `min_len = 1` keeps even short terms, exercising the most prefilter logic.
pub fn matches<'s>(cfg: LangConfig, pat: &str, src: &'s str) -> Vec<Match<'s>> {
    let compiled = Pattern::compile(pat, &cfg).expect("valid test pattern");
    let out = compiled.matches(src);
    let pf = compiled.prefilter(1);
    assert!(
        out.is_empty() || pf.might_match(src),
        "prefilter wrongly rejected a matching source\n  pattern: {pat:?}\n  source:  {src:?}",
    );
    assert_eq!(
        compiled.matches_prefiltered(src, &pf).len(),
        out.len(),
        "matches_prefiltered disagrees with matches\n  pattern: {pat:?}\n  source:  {src:?}",
    );
    out
}

/// First captured value for `name` across the matches, as an owned String.
pub fn cap(ms: &[Match], name: &str) -> Option<String> {
    ms.iter()
        .find_map(|m| m.capture_text(name).map(str::to_string))
}

/// Whether any match has node kind `kind`.
pub fn has_kind(ms: &[Match], kind: &str) -> bool {
    ms.iter().any(|m| m.kind == kind)
}

/// The first match whose matched text equals `text`.
pub fn by_text<'a, 's>(ms: &'a [Match<'s>], text: &str) -> Option<&'a Match<'s>> {
    ms.iter().find(|m| m.text == text)
}

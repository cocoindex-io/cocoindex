//! Shared helpers for the integration tests.
#![allow(dead_code)] // each test crate uses a subset

use cocoindex_code_match::{LangConfig, Match, Pattern};

/// Compile `pat` for `cfg` and match it against `src`.
pub fn matches<'s>(cfg: LangConfig, pat: &str, src: &'s str) -> Vec<Match<'s>> {
    Pattern::compile(pat, &cfg)
        .expect("valid test pattern")
        .matches(src)
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

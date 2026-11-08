//! Randomized and adversarial inputs for the text splitters, checking the
//! invariants every split must keep: chunk ranges lie on char boundaries of
//! the text and in order, positions agree with the ranges, chunks cover all
//! non-whitespace text, and chunks respect `chunk_size` wherever the text
//! can be cut.
//!
//! Seeded, so every run checks the same inputs. Run with:
//! `cargo test -p cocoindex --features text --test ops_text_fuzz`.
#![cfg(feature = "text")]

use cocoindex::ops::text::{
    KeepSeparator, RecursiveChunkConfig, RecursiveSplitter, SeparatorSplitConfig, SeparatorSplitter,
};
use cocoindex::resources::chunk::Chunk;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// The position of byte offset `offset` of `text`: char offset, 1-based line
/// and column, the way the splitters count them.
fn position_of(text: &str, offset: usize) -> (usize, u32, u32) {
    let before = &text[..offset];
    let char_offset = before.chars().count();
    let line = before.matches('\n').count() as u32 + 1;
    let line_start = before.rfind('\n').map_or(0, |index| index + 1);
    let column = before[line_start..].chars().count() as u32 + 1;
    (char_offset, line, column)
}

/// Check the invariants every split keeps, whatever the configuration.
fn check_chunks(text: &str, chunks: &[Chunk], context: &str) {
    let mut previous_start = 0;
    for chunk in chunks {
        let range = chunk.range();
        assert!(range.start < range.end, "{context}: empty chunk {range:?}");
        assert!(
            text.get(range.clone()).is_some(),
            "{context}: chunk {range:?} is not on char boundaries"
        );
        assert!(
            range.start >= previous_start,
            "{context}: chunks out of order at {range:?}"
        );
        previous_start = range.start;
        assert_eq!(chunk.start.byte_offset, range.start, "{context}");
        assert_eq!(chunk.end.byte_offset, range.end, "{context}");
        let (char_offset, line, column) = position_of(text, range.start);
        assert_eq!(
            (
                chunk.start.char_offset,
                chunk.start.line,
                chunk.start.column
            ),
            (char_offset, line, column),
            "{context}: start position of {range:?}"
        );
        let (char_offset, line, column) = position_of(text, range.end);
        assert_eq!(
            (chunk.end.char_offset, chunk.end.line, chunk.end.column),
            (char_offset, line, column),
            "{context}: end position of {range:?}"
        );
    }
}

/// Punctuation the default separators consume (`[\.\?!]\s+`, `[;:\-—]\s+`,
/// `,\s+` and their CJK forms): a split at such a separator leaves the
/// punctuation in neither chunk (`"aaaa. bbbb"` at size 6 gives `"aaaa"`,
/// `"bbbb"`). Shared with the Python splitter, which uses the same core.
const SEPARATOR_PUNCTUATION: &[char] = &[
    '.', '?', '!', ';', ':', '-', '—', ',', '。', '？', '！', '；', '：', '，',
];

/// Every char of `text` lies in some chunk, except whitespace and the
/// punctuation of a separator a split consumed.
fn check_coverage(text: &str, chunks: &[Chunk], context: &str) {
    let mut covered = vec![false; text.len()];
    for chunk in chunks {
        covered[chunk.range()].iter_mut().for_each(|c| *c = true);
    }
    for (offset, ch) in text.char_indices() {
        assert!(
            // A grammar may skip a byte order mark like whitespace.
            ch.is_whitespace()
                || ch == '\u{feff}'
                || SEPARATOR_PUNCTUATION.contains(&ch)
                || covered[offset],
            "{context}: char {ch:?} at byte {offset} is in no chunk"
        );
    }
}

/// Random text from a pool of fragments chosen to hit the splitters'
/// boundaries: separators of every level, multi-byte chars, long words.
fn random_text(rng: &mut StdRng, max_fragments: usize) -> String {
    const FRAGMENTS: &[&str] = &[
        "word",
        "Lorem",
        "ipsum",
        " ",
        "  ",
        "\t",
        "\n",
        "\n\n",
        "\n\n\n",
        "\r\n",
        ". ",
        "! ",
        "? ",
        ", ",
        "; ",
        "é",
        "日本語",
        "🦀",
        "👩‍👩‍👧",
        "\u{0301}",
        "\u{200b}",
        "{",
        "}",
        "(",
        ")",
        "fn main() {",
        "def f():",
        "    return 1",
        "<div>",
        "</div>",
        "# Title",
        "- item",
        "```",
        "|a|b|",
        "\0",
        "\u{feff}",
    ];
    let count = rng.gen_range(0..=max_fragments);
    let mut text = String::new();
    for _ in 0..count {
        if rng.gen_ratio(1, 20) {
            // A long run without any separator.
            let ch = ['x', 'é', '日', '🦀'][rng.gen_range(0..4)];
            text.extend(std::iter::repeat_n(ch, rng.gen_range(1..300)));
        } else {
            text.push_str(FRAGMENTS[rng.gen_range(0..FRAGMENTS.len())]);
        }
    }
    text
}

fn random_config(rng: &mut StdRng, language: Option<String>) -> RecursiveChunkConfig {
    let chunk_size = rng.gen_range(1..400);
    RecursiveChunkConfig {
        chunk_size,
        // Includes a minimum above the chunk size and an overlap at and
        // above it: configurations the splitter must survive.
        min_chunk_size: rng
            .gen_bool(0.5)
            .then(|| rng.gen_range(0..chunk_size * 2 + 2)),
        chunk_overlap: rng
            .gen_bool(0.5)
            .then(|| rng.gen_range(0..chunk_size * 2 + 2)),
        language,
    }
}

#[test]
fn recursive_splitter_keeps_its_invariants_on_random_text() {
    let splitter = RecursiveSplitter::new().unwrap();
    let mut rng = StdRng::seed_from_u64(0x5eed_0001);
    for case in 0..400 {
        let text = random_text(&mut rng, 80);
        let config = random_config(&mut rng, None);
        let context = format!("case {case}, config {config:?}, text {text:?}");
        let chunks = splitter.split_with(&text, config);
        check_chunks(&text, &chunks, &context);
        check_coverage(&text, &chunks, &context);
    }
}

#[test]
fn recursive_splitter_respects_chunk_size_without_overlap() {
    // Without overlap, a chunk exceeds `chunk_size` only when no separator
    // lets it shrink: the last separator level is whitespace, so such a
    // chunk has none (a run without separators is kept whole).
    let splitter = RecursiveSplitter::new().unwrap();
    let mut rng = StdRng::seed_from_u64(0x5eed_0002);
    for case in 0..300 {
        let text = random_text(&mut rng, 60);
        let chunk_size = rng.gen_range(4..200);
        let config = RecursiveChunkConfig {
            chunk_size,
            min_chunk_size: None,
            chunk_overlap: None,
            language: None,
        };
        let chunks = splitter.split_with(&text, config);
        for chunk in &chunks {
            let chunk_text = chunk.text(&text);
            assert!(
                // A chunk keeps the indentation of its first line.
                chunk_text.len() <= chunk_size || !chunk_text.trim().contains(char::is_whitespace),
                "case {case}: chunk of {} bytes over chunk_size {chunk_size}: {chunk_text:?}",
                chunk_text.len()
            );
        }
    }
}

#[test]
fn recursive_splitter_handles_every_registered_language() {
    let mut languages: Vec<&str> = cocoindex_code_ast::prog_langs::language_aliases()
        .into_iter()
        .map(|alias| alias.language)
        .collect();
    languages.sort_unstable();
    languages.dedup();
    assert!(languages.len() > 20, "{languages:?}");
    let splitter = RecursiveSplitter::new().unwrap();
    let mut rng = StdRng::seed_from_u64(0x5eed_0003);
    for language in languages {
        for case in 0..6 {
            // Random fragments are malformed source in every language.
            let text = random_text(&mut rng, 60);
            let config = random_config(&mut rng, Some(language.to_string()));
            let context = format!("{language} case {case}, config {config:?}, text {text:?}");
            let chunks = splitter.split_with(&text, config);
            check_chunks(&text, &chunks, &context);
            check_coverage(&text, &chunks, &context);
        }
    }
}

#[test]
fn recursive_splitter_handles_adversarial_inputs() {
    let splitter = RecursiveSplitter::new().unwrap();
    let huge_line = "a".repeat(200_000);
    // Kept small: tree-sitter's error recovery is quadratic on a line of
    // chars a grammar cannot tokenize (8000 of `a🦀` take a second to parse
    // as Rust in a release build).
    let huge_multibyte_line = "日🦀é".repeat(1_000);
    let deep_nesting = format!("{}{}", "(".repeat(5_000), ")".repeat(5_000));
    let only_newlines = "\n".repeat(10_000);
    let inputs: Vec<(&str, &str)> = vec![
        ("empty", ""),
        ("one space", " "),
        ("whitespace only", " \t\n\r\n  "),
        ("one char", "x"),
        ("one multibyte char", "🦀"),
        ("combining marks", "e\u{0301}\u{0301}\u{0301}\u{0301}"),
        ("zwj sequence", "👩‍👩‍👧👩‍👩‍👧👩‍👩‍👧"),
        ("nul bytes", "\0\0\0a\0"),
        ("huge single line", &huge_line),
        ("huge multibyte line", &huge_multibyte_line),
        ("only newlines", &only_newlines),
    ];
    let rust_nesting = deep_nesting.clone();
    for (chunk_size, overlap) in [(1, None), (3, Some(1)), (64, Some(64)), (1000, Some(2000))] {
        for (name, text) in &inputs {
            for language in [None, Some("rust"), Some("markdown")] {
                let config = RecursiveChunkConfig {
                    chunk_size,
                    min_chunk_size: None,
                    chunk_overlap: overlap,
                    language: language.map(str::to_string),
                };
                let context = format!("{name}, {config:?}");
                let chunks = splitter.split_with(text, config);
                check_chunks(text, &chunks, &context);
                check_coverage(text, &chunks, &context);
            }
        }
        let config = RecursiveChunkConfig {
            chunk_size,
            min_chunk_size: None,
            chunk_overlap: overlap,
            language: Some("rust".to_string()),
        };
        let chunks = splitter.split_with(&rust_nesting, config);
        check_chunks(&rust_nesting, &chunks, "deep nesting");
        check_coverage(&rust_nesting, &chunks, "deep nesting");
    }
}

#[test]
fn recursive_splitter_is_deterministic() {
    let splitter = RecursiveSplitter::new().unwrap();
    let other = RecursiveSplitter::new().unwrap();
    let mut rng = StdRng::seed_from_u64(0x5eed_0004);
    for _ in 0..50 {
        let text = random_text(&mut rng, 60);
        let config = random_config(&mut rng, Some("python".to_string()));
        assert_eq!(
            splitter.split_with(&text, config.clone()),
            other.split_with(&text, config)
        );
    }
}

#[test]
fn separator_splitter_keeps_its_invariants_on_random_text() {
    let mut rng = StdRng::seed_from_u64(0x5eed_0005);
    let separator_sets: &[&[&str]] = &[
        &[r"\n\n+"],
        &[r"\n"],
        &[r"\.\s*", r"!\s*"],
        &[r"\s+"],
        // Zero-width matches: the splitter must not loop or emit bad ranges.
        &[r"\b"],
        &[r"(?m)^"],
        &[""],
        &["日"],
    ];
    for separators in separator_sets {
        for keep_separator in [None, Some(KeepSeparator::Left), Some(KeepSeparator::Right)] {
            for include_empty in [false, true] {
                for trim in [false, true] {
                    let splitter = SeparatorSplitter::with_config(SeparatorSplitConfig {
                        separators_regex: separators.iter().map(|s| s.to_string()).collect(),
                        keep_separator,
                        include_empty,
                        trim,
                    })
                    .unwrap();
                    for case in 0..10 {
                        let text = random_text(&mut rng, 40);
                        let context = format!(
                            "{separators:?} keep={keep_separator:?} empty={include_empty} \
                             trim={trim} case {case}, text {text:?}"
                        );
                        let chunks = splitter.split(&text);
                        check_separator_chunks(&text, &chunks, include_empty, &context);
                        if keep_separator.is_some() && !trim {
                            // Keeping separators and whitespace, the chunks
                            // tile the text.
                            let joined: String =
                                chunks.iter().map(|chunk| chunk.text(&text)).collect();
                            if !include_empty || !chunks.is_empty() {
                                assert_eq!(joined, text, "{context}");
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Like [`check_chunks`], allowing the empty chunks `include_empty` asks for.
fn check_separator_chunks(text: &str, chunks: &[Chunk], include_empty: bool, context: &str) {
    let mut previous_start = 0;
    for chunk in chunks {
        let range = chunk.range();
        assert!(
            range.start < range.end || (include_empty && range.start == range.end),
            "{context}: empty chunk {range:?}"
        );
        assert!(
            text.get(range.clone()).is_some(),
            "{context}: chunk {range:?} is not on char boundaries"
        );
        assert!(range.start >= previous_start, "{context}: out of order");
        previous_start = range.start;
        let (char_offset, line, column) = position_of(text, range.start);
        assert_eq!(
            (
                chunk.start.char_offset,
                chunk.start.line,
                chunk.start.column
            ),
            (char_offset, line, column),
            "{context}: start position of {range:?}"
        );
        let (char_offset, line, column) = position_of(text, range.end);
        assert_eq!(
            (chunk.end.char_offset, chunk.end.line, chunk.end.column),
            (char_offset, line, column),
            "{context}: end position of {range:?}"
        );
    }
}

#[test]
fn separator_splitter_rejects_an_invalid_regex() {
    assert!(SeparatorSplitter::new(["("]).is_err());
    assert!(SeparatorSplitter::new(["[z-a]"]).is_err());
}

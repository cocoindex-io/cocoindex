//! Structural matching and rendering over malformed source: truncated files,
//! random bytes of syntax, unbalanced brackets. Matching must not panic, every
//! match and capture must lie on char boundaries of the source with positions
//! that agree with its byte range, and `render_match` must partition its text.
//!
//! Seeded, so every run checks the same inputs. Run with:
//! `cargo test -p cocoindex --features code_match --test code_fuzz`.
#![cfg(feature = "code_match")]

use cocoindex::ops::code::{CodeAst, render_match};
use cocoindex::resources::chunk::Chunk;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// A language, a well-formed sample to truncate and mutate, and patterns.
const CASES: &[(&str, &str, &[&str])] = &[
    (
        "python",
        "class Greeter:\n    def greet(self, name):\n        prefix = \"hi 日本\"\n        return prefix + name\n\ndef top(a, b=1, *args):\n    return [x for x in a]\n",
        &[r"def \NAME(\(A*\)):", r"return \X", r"\VAR = \VALUE"],
    ),
    (
        "rust",
        "pub struct S { a: u32 }\nimpl S {\n    fn greet(&self, name: &str) -> String { format!(\"{name} 🦀\") }\n}\nfn main() { let x = S { a: 1 }; println!(\"{}\", x.greet(\"é\")); }\n",
        &[r"fn \NAME(\(P*\))", r"let \X = \Y;", r"impl \T"],
    ),
    (
        "go",
        "package main\n\nfunc main() {\n\tx := 1\n\tif x > 0 { println(\"ok\") }\n}\n\nfunc add(a, b int) int { return a + b }\n",
        &[r"func \N(\(P*\))", r"return \X"],
    ),
    (
        "cpp",
        "#include <cstdio>\nvoid run() { int x = 1; }\nclass A { public: int f(int y) { return y * 2; } };\n",
        &[r"void \N(\(P*\))", r"return \X;"],
    ),
    (
        "javascript",
        "function greet(name) { return `hi ${name}`; }\nconst f = (a, b) => a + b;\nclass C { m() { return 1; } }\n",
        &[r"function \N(\(P*\))", r"return \X;"],
    ),
];

/// Check that `chunk` is a valid region of `source`.
fn check_chunk(source: &str, chunk: &Chunk, context: &str) {
    let range = chunk.range();
    assert!(range.start <= range.end, "{context}: inverted {range:?}");
    assert!(
        source.get(range.clone()).is_some(),
        "{context}: {range:?} is not on char boundaries"
    );
    let before = &source[..range.start];
    assert_eq!(
        chunk.start.char_offset,
        before.chars().count(),
        "{context}: char offset of {range:?}"
    );
    assert_eq!(
        chunk.start.line as usize,
        before.matches('\n').count() + 1,
        "{context}: line of {range:?}"
    );
    assert_eq!(chunk.end.byte_offset, range.end, "{context}");
}

/// Malformed variants of `sample`: truncations at every few chars, and
/// random deletions, duplications and insertions of syntax.
fn malformed_variants(sample: &str, rng: &mut StdRng) -> Vec<String> {
    let boundaries: Vec<usize> = sample
        .char_indices()
        .map(|(index, _)| index)
        .chain([sample.len()])
        .collect();
    let mut out: Vec<String> = boundaries
        .iter()
        .step_by(7)
        .map(|end| sample[..*end].to_string())
        .collect();
    const NOISE: &[&str] = &[
        "(", ")", "{", "}", "[", "\"", "'", "`", ":", ";", "\n", "\t", "\\", "/*", "*/", "#", "é",
        "🦀", "\0",
    ];
    for _ in 0..40 {
        let mut text = sample.to_string();
        for _ in 0..rng.gen_range(1..6) {
            let positions: Vec<usize> = text
                .char_indices()
                .map(|(index, _)| index)
                .chain([text.len()])
                .collect();
            let at = positions[rng.gen_range(0..positions.len())];
            match rng.gen_range(0..3) {
                0 => text.insert_str(at, NOISE[rng.gen_range(0..NOISE.len())]),
                1 => {
                    let end = positions.iter().copied().find(|p| *p > at).unwrap_or(at);
                    text.replace_range(at..end, "");
                }
                _ => {
                    let end = positions
                        .iter()
                        .copied()
                        .filter(|p| *p >= at)
                        .nth(rng.gen_range(0..10))
                        .unwrap_or(text.len());
                    let piece = text[at..end].to_string();
                    text.insert_str(at, &piece);
                }
            }
        }
        out.push(text);
    }
    out.push(String::new());
    out.push("(".repeat(3000));
    out
}

#[test]
fn matching_and_rendering_survive_malformed_source() {
    let mut rng = StdRng::seed_from_u64(0xc0de_0001);
    let mut total_matches = 0;
    for (language, sample, patterns) in CASES {
        for (variant_index, source) in malformed_variants(sample, &mut rng).iter().enumerate() {
            let ast = CodeAst::new(source.clone(), *language).unwrap();
            for pattern in *patterns {
                let context = format!("{language} variant {variant_index} {pattern}: {source:?}");
                let matches = ast.matches(pattern).unwrap();
                total_matches += matches.len();
                for m in &matches {
                    for chunk in &m.chunks {
                        check_chunk(source, chunk, &context);
                    }
                    for (name, captures) in &m.captures {
                        for chunk in captures {
                            check_chunk(source, chunk, &format!("{context} capture {name}"));
                        }
                    }
                    let view = render_match(&ast, m);
                    let mut cursor = 0;
                    for seg in &view.segments {
                        assert_eq!(seg.rendered_start, cursor, "{context}");
                        assert!(seg.rendered_end >= seg.rendered_start, "{context}");
                        cursor = seg.rendered_end;
                    }
                    assert_eq!(cursor, view.text.chars().count(), "{context}");
                }
            }
            // Splitting and term extraction of the same malformed parse.
            for chunk in ast.split(40, None, None).unwrap() {
                check_chunk(source, &chunk, &format!("{language} split"));
            }
            let _ = ast.index_terms(2);
        }
    }
    assert!(total_matches > 100, "the variants must exercise matches");
}

//! Stress-test the matcher + prefilter over a real codebase.
//!
//!   cargo run --release -p cocoindex_code_match --example stress -- <dir> [max_files_per_lang]
//!
//! Samples files by extension, auto-generates patterns from real lines (literal,
//! metavar-injected, containment-wrapped), single-layer templates, and multi-layer
//! nested containment (`gen_layered_patterns`), then checks the load-bearing
//! invariants on every (pattern, file): no panic, prefilter soundness
//! (matches ⟹ might_match), and bounded match time (a pathologically slow match —
//! e.g. super-linear nested containment — is reported like a panic). The tree is
//! parsed once per file and reused via `matches_in_tree`, so timing reflects
//! matching, not parsing. Reports any violation with the pattern + file so it's
//! reproducible.

use cocoindex_code_match::{LangConfig, Pattern, lang};
use std::collections::HashSet;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::time::Instant;
use tree_sitter::Parser;

fn lang_for_ext(ext: &str) -> Option<(&'static str, LangConfig)> {
    let name = match ext {
        "rs" => "rust",
        "py" | "pyi" => "python",
        "ts" => "typescript",
        "tsx" => "tsx",
        "js" | "jsx" | "mjs" | "cjs" => "javascript",
        "c" | "h" => "c",
        "cpp" | "cc" | "cxx" | "hpp" | "hh" => "cpp",
        "go" => "go",
        "java" => "java",
        "rb" => "ruby",
        "json" => "json",
        "toml" => "toml",
        "yaml" | "yml" => "yaml",
        "css" => "css",
        "html" => "html",
        "sql" => "sql",
        "swift" => "swift",
        "kt" | "kts" => "kotlin",
        "scala" => "scala",
        "sh" | "bash" => "bash",
        "cs" => "csharp",
        "php" => "php",
        _ => return None,
    };
    lang::by_name(name).map(|c| (name, c))
}

/// Replace the first `[a-z_][A-Za-z0-9_]*` identifier run with `\X` (a crude metavar
/// injection — invalid results are fine, they exercise the lexer/compiler).
fn inject_metavar(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c == b'_' || c.is_ascii_lowercase() {
            let start = i;
            while i < b.len() && (b[i] == b'_' || b[i].is_ascii_alphanumeric()) {
                i += 1;
            }
            // skip if it's right after a `.`/`\` (avoid mangling member access oddly)
            if start > 0 && (b[start - 1] == b'\\') {
                continue;
            }
            return Some(format!("{}\\X{}", &s[..start], &s[i..]));
        }
        i += 1;
    }
    None
}

/// The word-shaped tokens (`[A-Za-z_][A-Za-z0-9_]*`) present in the source — used to
/// gate layered patterns on keywords that actually appear, so they exercise real
/// nesting instead of trivially failing to compile/match.
fn word_set(src: &str) -> HashSet<&str> {
    let mut set = HashSet::new();
    let b = src.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i].is_ascii_alphabetic() || b[i] == b'_' {
            let s = i;
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                i += 1;
            }
            set.insert(&src[s..i]);
        } else {
            i += 1;
        }
    }
    set
}

/// Multi-layer nested-containment patterns like `catch \* \{{ if \* \{{ throw \}} \}}`
/// (a conditional re-throw). Real code has structure several layers deep; these probe
/// the containment + leading/trailing-tolerance + prefilter interaction that single-
/// layer patterns never reach. Gated on block/action keywords that appear in `words`
/// so each pattern has a realistic chance of matching, and bounded so the keyword
/// cross-product can't explode the run.
fn gen_layered_patterns(words: &HashSet<&str>) -> Vec<String> {
    const BLOCKS: &[&str] = &[
        "if", "for", "while", "catch", "switch", "match", "loop", "function", "fn", "def", "class",
        "impl",
    ];
    const ACTIONS: &[&str] = &["throw", "return", "break", "continue", "raise", "yield"];
    let blocks: Vec<&str> = BLOCKS
        .iter()
        .copied()
        .filter(|k| words.contains(*k))
        .collect();
    let actions: Vec<&str> = ACTIONS
        .iter()
        .copied()
        .filter(|k| words.contains(*k))
        .collect();
    let (open, close) = (r"\{{", r"\}}");
    let mut out = Vec::new();
    for &outer in &blocks {
        for &act in &actions {
            // 2-layer: a block whose body contains an action.
            out.push(format!(r"{outer} \* {open} {act} {close}"));
        }
        for &inner in &blocks {
            for &act in &actions {
                // 3-layer: outer block → inner block → action (e.g. the re-throw above).
                out.push(format!(
                    r"{outer} \* {open} {inner} \* {open} {act} {close} {close}"
                ));
                if out.len() >= 80 {
                    return out;
                }
            }
        }
    }
    out
}

fn gen_patterns(src: &str) -> Vec<String> {
    let mut pats = Vec::new();
    for line in src.lines() {
        let t = line.trim();
        if t.len() < 4 || t.len() > 90 {
            continue;
        }
        pats.push(t.to_string()); // the literal line
        pats.push(format!("\\{{{{ {t} \\}}}}")); // containment over the line
        if let Some(inj) = inject_metavar(t) {
            pats.push(inj); // metavar-injected
        }
    }
    // a few language-agnostic templates (panic/soundness coverage, not self-match)
    for tpl in [
        r"\X = \Y",
        r"return \X",
        r"\X(\*)",
        r"\X.\Y(\*)",
        r"\X + \Y",
        r"\{{ return \X \}}",
        r"\{{ \X(\*) \}}",
        r"\(N:/.*/)",
        r"\(N:/[a-z]+/*\)",
        r"if \*",
        // bare compound operators — exercise `match_token_run` (split pattern char-run
        // vs. one source leaf) across every language. Most won't exist in a given
        // language; we only care that they never panic and stay prefilter-sound.
        "=>",
        "==",
        "===",
        "!=",
        "&&",
        "||",
        "->",
        "::",
        ">>",
        "<<",
        "+=",
        "<=",
        r"\X => \Y",
        r"\X == \Y",
        r"\X && \Y",
        r"\X::\Y",
        // deep metavar-only containment (no keyword needed) — double/triple nesting.
        r"\{{ \{{ \X = \Y \}} \}}",
        r"\{{ \X(\*) \{{ return \X \}} \}}",
        r"\{{ if \* \{{ \X = \Y \}} \}}",
    ] {
        pats.push(tpl.to_string());
    }
    pats.extend(gen_layered_patterns(&word_set(src)));
    pats
}

fn collect_files(root: &Path, max_per_lang: usize) -> Vec<(&'static str, LangConfig, PathBuf)> {
    use std::collections::HashMap;
    let mut counts: HashMap<&'static str, usize> = HashMap::new();
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in rd.flatten() {
            let p = entry.path();
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with('.')
                || matches!(
                    name.as_ref(),
                    "target" | "node_modules" | "build" | "dist" | "vendor" | "third_party"
                )
            {
                continue;
            }
            let Ok(ft) = entry.file_type() else { continue };
            if ft.is_dir() {
                stack.push(p);
            } else if ft.is_file() {
                if let Some(ext) = p.extension().and_then(|e| e.to_str()) {
                    if let Some((langname, cfg)) = lang_for_ext(ext) {
                        let c = counts.entry(langname).or_default();
                        if *c < max_per_lang {
                            // skip very large files (keep the run bounded)
                            if std::fs::metadata(&p)
                                .map(|m| m.len() < 200_000)
                                .unwrap_or(false)
                            {
                                *c += 1;
                                out.push((langname, cfg, p));
                            }
                        }
                    }
                }
            }
        }
    }
    out
}

fn main() {
    let root = std::env::args()
        .nth(1)
        .expect("usage: stress <dir> [max_files]");
    let max_per_lang: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);
    std::panic::set_hook(Box::new(|_| {})); // suppress the default print; we report ourselves

    let files = collect_files(Path::new(&root), max_per_lang);
    eprintln!("sampled {} files", files.len());

    // Flag a pattern only when its match time exceeds the file's *index baseline* by
    // this margin. `matches_in_tree` rebuilds the index every call (cost ∝ file size,
    // ~0.6µs/node, pattern-independent), so an absolute threshold just flags big
    // files. Subtracting the baseline isolates a real matcher pathology — super-linear
    // nested containment, catastrophic backtracking — which is a genuine bug.
    const SLOW_EXCESS_US: u128 = 20_000;
    let (mut total, mut compiled, mut panics, mut unsound, mut slow) =
        (0u64, 0u64, 0u64, 0u64, 0u64);
    let mut worst = (0u128, String::new(), String::new()); // (excess_us, file, pattern)
    for (langname, cfg, path) in &files {
        let Ok(src) = std::fs::read_to_string(path) else {
            continue;
        };
        if src.is_empty() {
            continue;
        }
        // Parse ONCE per file and reuse the tree across every pattern via
        // `matches_in_tree`. `Pattern::matches` re-parses on each call, which at ~250
        // patterns/file dominated the run; tree-sitter parsing is the same regardless
        // of pattern, so caching it is pure win and lets timing reflect *matching*.
        let mut parser = Parser::new();
        if parser.set_language(&cfg.language).is_err() {
            continue;
        }
        let Some(tree) = parser.parse(&src, None) else {
            continue;
        };
        // Baseline ≈ the per-call index cost: a trivial identifier pattern that still
        // indexes the whole tree but does almost no DP work. Min of a few runs to
        // shed scheduler noise. Per-pattern excess over this is the matching cost.
        let base_us = {
            let mut b = u128::MAX;
            if let Ok(p) = Pattern::compile("zz_baseline_zz", cfg) {
                for _ in 0..3 {
                    let t = Instant::now();
                    let _ = p.matches_in_tree(&tree, &src);
                    b = b.min(t.elapsed().as_micros());
                }
            }
            if b == u128::MAX { 0 } else { b }
        };
        for pat in gen_patterns(&src) {
            total += 1;
            let res = catch_unwind(AssertUnwindSafe(|| {
                let p = match Pattern::compile(&pat, cfg) {
                    Ok(p) => p,
                    Err(_) => return None, // a malformed pattern is fine
                };
                // `matches_prefiltered` is `if might_match { matches } else { empty }`,
                // so checking it against `matches` is tautological. The real invariant
                // is soundness: a true match implies the prefilter accepts.
                let might = p.prefilter(3).might_match(&src);
                let t0 = Instant::now();
                let ms = p.matches_in_tree(&tree, &src);
                Some((ms.len(), might, t0.elapsed().as_micros()))
            }));
            match res {
                Err(payload) => {
                    panics += 1;
                    let msg = payload
                        .downcast_ref::<String>()
                        .map(|s| s.as_str())
                        .or_else(|| payload.downcast_ref::<&str>().copied())
                        .unwrap_or("?");
                    eprintln!("PANIC [{msg}]\n  lang={langname} file={path:?}\n  pat={pat:?}");
                }
                Ok(Some((nm, might, us))) => {
                    compiled += 1;
                    if nm > 0 && !might {
                        unsound += 1;
                        eprintln!(
                            "UNSOUND prefilter: {nm} matches but might_match=false\n  lang={langname} file={path:?}\n  pat={pat:?}"
                        );
                    }
                    let excess = us.saturating_sub(base_us);
                    if excess >= SLOW_EXCESS_US {
                        slow += 1;
                        eprintln!(
                            "SLOW +{}ms over {}ms index ({nm} matches)\n  lang={langname} file={path:?}\n  pat={pat:?}",
                            excess / 1000,
                            base_us / 1000,
                        );
                    }
                    if excess > worst.0 {
                        worst = (excess, format!("{path:?}"), pat.clone());
                    }
                }
                Ok(None) => {}
            }
        }
    }
    println!(
        "== {total} patterns run ({compiled} compiled); {panics} panics, {unsound} unsound, {slow} slow (excess >={}ms) ==",
        SLOW_EXCESS_US / 1000
    );
    if worst.0 > 0 {
        println!(
            "slowest: +{}ms  pat={:?}  file={}",
            worst.0 / 1000,
            worst.2,
            worst.1
        );
    }
}

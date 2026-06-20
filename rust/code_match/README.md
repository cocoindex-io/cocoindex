# cocoindex_code_match

Match **by-example code patterns** against **tree-sitter ASTs**, with metavariables.

The bet: parse the *source* with a full tree-sitter grammar, but keep the *pattern* a flat token + metavar skeleton, and match it against the AST with metavariables snapping to node boundaries. This gives Comby-style pattern ergonomics (fragments like `else { \(*) }` need not parse as a node) together with tree-sitter's correctness — balanced/context-sensitive nesting (`>>`, `<>`), AST-resolved precedence, and matches that are integral subtrees.

## Example

```rust
use cocoindex_code_match::{lang, Pattern};

let cfg = lang::typescript();
let pat = Pattern::compile(r"console.log(\(ARGS*))", &cfg).unwrap();

let src = r#"console.log("hi", x);"#;
for m in pat.matches(src) {
    println!("{} -> ARGS = {:?}", m.text, m.capture_text("ARGS"));
    // console.log("hi", x) -> ARGS = Some("\"hi\", x")
}
```

### Metavariables

The sigil is `\` by default (shell-safe; configurable to `$` via `LangConfig::with_meta_char('$')`). Names are `[A-Za-z0-9_]+` (uppercase by convention; lowercase/digits allowed).

- `\NAME` — one node (named); `\_` — one node, anonymous.
- `\(NAME*)` — a run of **same-level** sibling nodes (many); `\*` — anonymous. A cross-level skip is written as multiple `\*`, one per grammar level.
- `\(NAME?)` — optional (zero or one node); `\?` — anonymous.
- `\(NAME:/regex/)` — the captured source text must match the regex (unanchored; `^…$` for whole-node).
- A repeated name must capture equal text: `\X == \X` matches `a == a`, not `a == b`.

Use raw strings (`r"..."`) when writing patterns in Rust so the backslashes stay literal.

## Supported languages

TypeScript/TSX, JavaScript, C, C++, Python, Rust, Go, Java, C#, Ruby, PHP, Scala, Bash, SQL — each a one-line `LangConfig` constructor; the matcher and lexer are language-agnostic.

## Status

Matching + captures (same-level runs, leading/trailing tolerance, regex matchers); no rewriting yet. Planned: descendant containment (`\{{ … \}}`), node-kind matchers, rewriting, a rule DSL, prefiltering/indexing.

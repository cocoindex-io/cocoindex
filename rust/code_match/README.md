# cocoindex_code_match

Match **by-example code patterns** against **tree-sitter ASTs**, with metavariables.

The bet: parse the *source* with a full tree-sitter grammar, but keep the *pattern* a flat token + metavar skeleton, and match it against the AST with metavariables snapping to node boundaries. This gives Comby-style pattern ergonomics (fragments like `else { \(*\) }` need not parse as a node) together with tree-sitter's correctness — balanced/context-sensitive nesting (`>>`, `<>`), AST-resolved precedence, and matches that are integral subtrees.

## Example

```rust
use cocoindex_code_match::{lang, Pattern};

let cfg = lang::typescript();
let pat = Pattern::compile(r"console.log(\(ARGS*\))", &cfg).unwrap();

let src = r#"console.log("hi", x);"#;
for m in pat.matches(src) {
    println!("{} -> ARGS = {:?}", m.text, m.capture_text("ARGS"));
    // console.log("hi", x) -> ARGS = Some("\"hi\", x")
}
```

### Metavariables

The sigil is `\` by default (shell-safe; configurable to `$` via `LangConfig::with_meta_char('$')`). Names are `[A-Za-z0-9_]+` (uppercase by convention; lowercase/digits allowed).

Metavar delimiters are **symmetric** — a metavar is `\( … \)` (the `\` marks both ends).

- `\NAME` — one node (named); `\_` — one node, anonymous.
- `\(NAME*\)` — a run of **same-level** sibling nodes (many); `\*` — anonymous. A cross-level skip is written as multiple `\*`, one per grammar level.
- `\(NAME?\)` — optional (zero or one node); `\?` — anonymous.
- `\(NAME:/regex/\)` — the captured source text must match the regex, **anchored to the whole node** (`/set_/` is exactly `set_`; add `.*` for prefix/suffix/substring). Anonymous: `\(/regex/\)` or the short form `\/regex/`.
- A repeated name must capture equal text: `\X == \X` matches `a == a`, not `a == b`.

Use raw strings (`r"..."`) when writing patterns in Rust so the backslashes stay literal.

## Supported languages

Parity with the cocoindex chunk splitter — each a one-line `LangConfig` constructor; the matcher and lexer are language-agnostic:

- **Programming:** TypeScript/TSX, JavaScript, C, C++, Python, Rust, Go, Java, C#, Ruby, PHP, Scala, Bash, SQL, Kotlin, Swift, Julia, R, Fortran, Pascal, Elm, Solidity, HCL, CMake.
- **Data:** JSON, TOML, YAML.
- **Markup:** HTML, CSS, XML.

For data/markup, match over content with a metavar (`<p>\X</p>`, `{"key": \V}`) — literal free text in a pattern isn't supported (the flat lexer would split it).

## Status

Matching + captures (same-level runs, leading/trailing tolerance, regex matchers, descendant containment `\{{ … \}}`, prefiltering/indexing); no rewriting yet. Planned: sub-patterns with alternation/quantifiers (`\{ … \}`, `|`, `*`/`+`/`?` on terms), node-kind matchers, rewriting, a rule DSL.

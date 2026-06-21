//! Engine-feature tests (language-agnostic behavior, illustrated in whatever
//! language is convenient): balancing, precedence, the wildcard DP, cardinality,
//! metavar equality, the `>>` split, number-lexing limits, and the sigil config.

mod common;
use common::*;

use cocoindex_code_match::{Pattern, lang};

// ---------------- structural matching ----------------

#[test]
fn string_is_atomic() {
    // The `)` inside the string literal must be invisible — strings are one node.
    let src = r#"foo("a)b");"#;
    let ms = matches(lang::typescript(), r"foo(\(ARGS*))", src);
    assert_eq!(cap(&ms, "ARGS").as_deref(), Some(r#""a)b""#));
}

#[test]
fn precedence_from_ast() {
    // `a = b = c` is right-associative; the outer node binds \B = (b = c).
    let src = "a = b = c;";
    let ms = matches(lang::typescript(), r"\A = \B", src);
    let outer = by_text(&ms, "a = b = c").expect("outer assignment");
    assert_eq!(outer.capture_text("A"), Some("a"));
    assert_eq!(outer.capture_text("B"), Some("b = c"));
    let inner = by_text(&ms, "b = c").expect("inner assignment");
    assert_eq!(inner.capture_text("A"), Some("b"));
    assert_eq!(inner.capture_text("B"), Some("c"));
}

#[test]
fn leading_trailing_tolerance() {
    // A pattern may cover a contiguous run of a node's direct children that
    // spans ≥2 children; leading/trailing siblings are free context. Here the
    // arrow function's params + body are covered, ignoring the leading `async`.
    let src = "const f = async (x) => x + 1;";
    let ms = matches(lang::typescript(), r"(\P) => \B", src);
    assert!(
        ms.iter().any(|m| m.kind == "arrow_function"),
        "should match the arrow function ignoring leading `async`, got {ms:?}",
    );
}

#[test]
fn function_signature_ignores_body() {
    // Signature-style search should match the full function declaration even
    // though the pattern stops before the trailing body child.
    let src = "function f() { return 1; }";
    let ms = matches(lang::typescript(), r"function f()", src);
    assert!(
        ms.iter().any(|m| m.kind == "function_declaration"),
        "should match the function declaration from its signature, got {ms:?}",
    );
}

#[test]
fn star_is_same_level() {
    // `\*` is same-level: `~D() \* }` requires the destructor to be the LAST
    // member — a same-level run can't leak past the class body into siblings.
    let same = r"class \NAME \* { \* ~\NAME() \* }";
    assert!(!matches(lang::cpp(), same, "class Foo { ~Foo() {} };").is_empty());
    assert!(
        matches(lang::cpp(), same, "class Bar { ~Bar(); int x; };").is_empty(),
        "same-level `*` must not skip past following members",
    );
}

#[test]
fn star_after_cpp_destructor_does_not_absorb_next_member() {
    // This is the concrete leak regression: after `~A()` the next `\*` is still
    // inside the destructor declaration, so it cannot absorb `void f();`.
    let src = "class A { ~A(); void f(); };";
    let ms = matches(lang::cpp(), r"class \C { \* ~\C() \* }", src);
    assert!(
        !ms.iter().any(|m| m.kind == "class_specifier"),
        "a single trailing `*` must not absorb following class members, got {ms:?}",
    );
}

#[test]
fn multiple_stars_cross_levels() {
    // A cross-level skip is written as multiple `\*`, one per grammar level: the
    // first absorbs the destructor's own tail, the second the following members
    // (which sit at a different depth). No single hole crosses, so nothing leaks.
    let pat = r"class \NAME \* { \* ~\NAME() \* \* }";
    // destructor anywhere in the class
    assert!(!matches(lang::cpp(), pat, "class Bar { int x; ~Bar(); void f(); };").is_empty());
    // and it can't leak out of one class into the next — only the two real
    // classes match, no `translation_unit` span across the `;`.
    let ms = matches(
        lang::cpp(),
        pat,
        "class Foo { ~Foo() {} }; class Bar { ~Bar() {} };",
    );
    assert!(
        ms.iter().all(|m| !m.text.contains(';')),
        "no match may span the `;` between classes, got {ms:?}",
    );
    let names: Vec<&str> = ms.iter().filter_map(|m| m.capture_text("NAME")).collect();
    assert!(names.contains(&"Foo") && names.contains(&"Bar"));
}

#[test]
fn partial_match_dedup() {
    // The ≥2-children rule: a single-child run is left to the child, so the
    // assignment is the match — not the `expr;` statement that wraps it.
    let ms = matches(lang::typescript(), r"\A = \B", "a = b;");
    assert!(ms.iter().any(|m| m.kind == "assignment_expression"));
    assert!(
        !ms.iter().any(|m| m.kind == "expression_statement"),
        "the enclosing statement must not also match (dedup), got {ms:?}",
    );
}

#[test]
fn multiple_gaps_dp() {
    // Two `\(*)` gaps around an anchor — exercises the wildcard DP / backtracking.
    let src = "function f() { a(); foo(); b(); }";
    let ms = matches(lang::typescript(), r"{ \(A*) foo(); \(B*) }", src);
    let m = ms
        .iter()
        .find(|m| m.kind == "statement_block")
        .expect("statement_block match");
    assert_eq!(m.capture_text("A"), Some("a();"));
    assert_eq!(m.capture_text("B"), Some("b();"));
}

#[test]
fn optional_metavar() {
    // `\(ARG?)` matches calls with zero or one argument.
    let ms = matches(lang::typescript(), r"f(\(ARG?))", "f(x);");
    assert_eq!(cap(&ms, "ARG").as_deref(), Some("x"));
    let ms = matches(lang::typescript(), r"f(\(ARG?))", "f();");
    assert!(has_kind(&ms, "call_expression"), "no-arg call should match");
}

#[test]
fn anonymous_metavars() {
    // `\_` single + `\*` anonymous many: matches but captures nothing.
    let src = "obj.method(1, 2, 3);";
    let ms = matches(lang::typescript(), r"\_.method(\*)", src);
    assert!(has_kind(&ms, "call_expression"));
}

#[test]
fn multiple_match_sites() {
    let src = "log(1); log(2); other(3);";
    let ms = matches(lang::typescript(), r"log(\A)", src);
    let caps: Vec<Option<&str>> = ms
        .iter()
        .filter(|m| m.kind == "call_expression")
        .map(|m| m.capture_text("A"))
        .collect();
    assert!(caps.contains(&Some("1")) && caps.contains(&Some("2")));
    assert_eq!(caps.len(), 2);
}

#[test]
fn no_false_match() {
    let ms = matches(lang::typescript(), r"console.log(\*)", "foo(1);");
    assert!(ms.is_empty(), "should not match anything, got {ms:?}");
}

// ---------------- metavar equality ----------------

#[test]
fn repeated_metavar_must_be_equal() {
    let ms = matches(lang::typescript(), r"\X == \X", "if (a == a) {}");
    assert_eq!(cap(&ms, "X").as_deref(), Some("a"));

    let src = "if (a == b) {}";
    let ms = matches(lang::typescript(), r"\X == \X", src);
    assert!(ms.is_empty(), "a == b must not match \\X == \\X");
}

#[test]
fn distinct_metavars_need_not_be_equal() {
    let src = "x = y;";
    let ms = matches(lang::typescript(), r"\A = \B", src);
    assert!(
        ms.iter()
            .any(|m| m.capture_text("A") == Some("x") && m.capture_text("B") == Some("y"))
    );
}

// ---------------- regex metavar matcher ----------------

fn caps_all<'a>(ms: &'a [cocoindex_code_match::Match], name: &str) -> Vec<&'a str> {
    let mut v: Vec<&str> = ms.iter().filter_map(|m| m.capture_text(name)).collect();
    v.sort();
    v
}

#[test]
fn regex_constrains_identifier() {
    // Matchers are whole-node anchored, so `get.*` filters callees *starting* with
    // `get` (the trailing `.*` is the explicit "prefix").
    let src = "getUser(1); setName(2); getId(3);";
    let ms = matches(lang::typescript(), r"\(F:/get.*/)(\*)", src);
    assert_eq!(caps_all(&ms, "F"), vec!["getId", "getUser"]);
}

#[test]
fn anonymous_regex_matcher() {
    // `\(:/re/)` — a regex constraint with no name: filter a node without
    // capturing it. Here the callee must start with `get` (`get.*`), but isn't bound.
    let src = "getUser(1); setName(2); getId(3);";
    let ms = matches(lang::typescript(), r"\(:/get.*/)(\*)", src);
    let callees: Vec<&str> = ms
        .iter()
        .filter(|m| m.kind == "call_expression")
        .map(|m| m.text)
        .collect();
    assert!(callees.contains(&"getUser(1)") && callees.contains(&"getId(3)"));
    assert!(
        !callees.iter().any(|t| t.contains("setName")),
        "the `^get` constraint must exclude setName, got {callees:?}",
    );
}

#[test]
fn regex_pins_nesting_level() {
    // Larger spans (`foo.bar`, `foo.bar(x)`) all start at `foo`; `^foo$` filters
    // the candidates down to the leaf. Exercises "every sub-layer is a candidate".
    let src = "foo.bar(x);";
    let ms = matches(lang::typescript(), r"\(OBJ:/^foo$/).bar(\*)", src);
    assert_eq!(cap(&ms, "OBJ").as_deref(), Some("foo"));
}

#[test]
fn regex_on_subtree() {
    // The metavar binds a whole expression; the regex tests its source text.
    // Anchored, so `.*\+.*` is the explicit "contains `+`".
    let yes = matches(lang::typescript(), r"f(\(A:/.*\+.*/))", "f(a + b);");
    assert_eq!(cap(&yes, "A").as_deref(), Some("a + b"));
    let no = matches(lang::typescript(), r"f(\(A:/.*\+.*/))", "f(ab);");
    assert!(no.iter().all(|m| m.kind != "call_expression"));
}

#[test]
fn regex_alternation_parens_free() {
    // Balanced `()` inside a delimited regex need no escaping (paren-depth close).
    let src = "foo; bar; baz;";
    let ms = matches(lang::typescript(), r"\(N:/^(foo|bar)$/)", src);
    assert_eq!(caps_all(&ms, "N"), vec!["bar", "foo"]);
}

#[test]
fn regex_shorthand_no_closing_slash() {
    // Shorthand (no closing `/`): the `)` closes the matcher. Anchored, so `get.*`.
    let src = "getUser(1); setName(2);";
    let ms = matches(lang::typescript(), r"\(F:/get.*)(\*)", src);
    assert_eq!(cap(&ms, "F").as_deref(), Some("getUser"));
}

#[test]
fn regex_escaped_slash() {
    // `\/` is a literal slash (the `regex` crate accepts `\<punct>`).
    let ms = matches(lang::typescript(), r"f(\(P:/a\/b/))", "f(a/b); f(ab);");
    assert_eq!(cap(&ms, "P").as_deref(), Some("a/b"));
}

#[test]
fn regex_dotstar_equals_bare() {
    // `/.*/ ` is a pure pass-through filter, so it must behave exactly like `\X`.
    let src = "a = b = c;";
    let bare = matches(lang::typescript(), r"\X = \Y", src);
    let dotstar = matches(lang::typescript(), r"\(X:/.*/) = \Y", src);
    assert_eq!(bare.len(), dotstar.len());
    assert_eq!(caps_all(&bare, "X"), caps_all(&dotstar, "X"));
}

#[test]
fn regex_optional_constrains_when_present() {
    // Present must match the regex; absence is still allowed (the `?` is kept).
    let ts = lang::typescript();
    assert_eq!(
        cap(&matches(ts.clone(), r"f(\(A?:/^x/))", "f(x);"), "A").as_deref(),
        Some("x")
    );
    assert!(has_kind(
        &matches(ts.clone(), r"f(\(A?:/^x/))", "f();"),
        "call_expression"
    ));
    assert!(
        matches(ts, r"f(\(A?:/^x/))", "f(y);")
            .iter()
            .all(|m| m.kind != "call_expression")
    );
}

#[test]
fn regex_invalid_errors() {
    // An unparseable regex surfaces as a `client` compile error (not a panic,
    // not a silently-dropped constraint).
    let res = Pattern::compile(r"\(Z:/[/) = \Y", &lang::typescript());
    assert!(
        matches!(res, Err(cocoindex_code_match::Error::Client { .. })),
        "expected a client error"
    );
}

// ---------------- metavar names (digit / lowercase) ----------------

#[test]
fn metavar_digit_name_equality() {
    // sed-like `\1`; repeated name still enforces equality.
    assert!(!matches(lang::typescript(), r"\1 == \1", "if (a == a) {}").is_empty());
    assert!(matches(lang::typescript(), r"\1 == \1", "if (a == b) {}").is_empty());
}

#[test]
fn metavar_lowercase_name() {
    let ms = matches(lang::typescript(), r"\x = \y", "m = n;");
    assert!(
        ms.iter()
            .any(|m| m.capture_text("x") == Some("m") && m.capture_text("y") == Some("n"))
    );
}

// ---------------- configurable sigil ----------------

#[test]
fn configurable_dollar_sigil() {
    let cfg = lang::typescript().with_meta_char('$');
    let src = "foo(a, b);";
    let ms = Pattern::compile("foo($(ARGS*))", &cfg)
        .unwrap()
        .matches(src);
    assert_eq!(cap(&ms, "ARGS").as_deref(), Some("a, b"));
    let ms = Pattern::compile("foo($A, $B)", &cfg).unwrap().matches(src);
    assert!(
        ms.iter()
            .any(|m| m.capture_text("A") == Some("a") && m.capture_text("B") == Some("b"))
    );
}

#[test]
fn escaped_sigil_is_literal() {
    // A doubled sigil is one *literal* sigil. `\\X` is a literal `\` + `X`, not a
    // metavar, so it does not match `a = 1` the way `\X` does.
    let ts = lang::typescript();
    assert!(!matches(ts.clone(), r"\X = 1", "a = 1;").is_empty());
    assert!(
        matches(ts, r"\\X = 1", "a = 1;").is_empty(),
        "`\\\\X` must be a literal backslash + X, not a metavar",
    );
    // And the escape is sigil-agnostic: with `$` as the sigil, `$$` is a literal
    // `$` — here matching a jQuery-style `$(…)` call.
    let dollar = lang::typescript().with_meta_char('$');
    assert!(has_kind(
        &matches(dollar, "$$(a)", "$(a);"),
        "call_expression"
    ));
}

// ---------------- lexer robustness ----------------

#[test]
fn malformed_string_pattern_does_not_panic() {
    let cfg = lang::typescript();
    // A bare/unterminated sigil is lenient (compiles, lexed as a literal).
    let _ = Pattern::compile("foo(\"\\", &cfg)
        .unwrap()
        .matches("foo();");
    let _ = Pattern::compile("\\", &cfg).unwrap().matches("x;");
}

// ---------------- numbers ----------------

#[test]
fn float_literal_is_one_token() {
    let src = "void g(){ foo(3.14); }";
    let ms = matches(lang::c(), "foo(3.14)", src);
    assert!(has_kind(&ms, "call_expression"));
}

#[test]
fn float_literal_capture() {
    let src = "let x = f(2.5e3);";
    let ms = matches(lang::typescript(), r"f(\N)", src);
    assert_eq!(cap(&ms, "N").as_deref(), Some("2.5e3"));
}

#[test]
fn cpp_separated_number_metavar_ok() {
    // Metavar over a `'`-separated C++ number works (source node matched whole).
    let src = "int x = 1'000 + 5;";
    let ms = matches(lang::cpp(), r"\N + 5", src);
    assert_eq!(cap(&ms, "N").as_deref(), Some("1'000"));
}

#[test]
fn cpp_apostrophe_separator_literal() {
    // The C++ number rule includes `'`, so a literal `1'000` in a pattern lexes
    // as one token and aligns with the source number (M2.5).
    let src = "int x = 1'000;";
    assert!(!matches(lang::cpp(), "1'000", src).is_empty());
}

// ---------------- per-language literal lexing (M2.5) ----------------

#[test]
fn rust_raw_string_literal() {
    // `r#"a"b"#` is one node despite the inner `"`; a literal raw string in the
    // pattern must lex as a single Str and match.
    let src = r##"fn m() { log(r#"a"b"#); }"##;
    let ms = matches(lang::rust(), r##"log(r#"a"b"#)"##, src);
    assert!(
        has_kind(&ms, "call_expression"),
        "rust raw string should match"
    );
}

#[test]
fn rust_raw_string_metavar() {
    let src = r##"fn m() { log(r#"a"b"#); }"##;
    let ms = matches(lang::rust(), r"log(\S)", src);
    assert_eq!(cap(&ms, "S").as_deref(), Some(r##"r#"a"b"#"##));
}

#[test]
fn python_triple_string_literal() {
    let src = "x = \"\"\"a\nb\"\"\"\n";
    let ms = matches(lang::python(), "x = \"\"\"a\nb\"\"\"", src);
    assert!(!ms.is_empty(), "python triple-quoted string should match");
}

#[test]
fn number_suffix_and_separator() {
    // Rust suffixed/separated integer lexes as one token.
    let src = "fn m() { let n = 1_000u64; }";
    let ms = matches(lang::rust(), "1_000u64", src);
    assert!(
        !ms.is_empty(),
        "rust suffixed/separated number should match"
    );
}

// ---------------- non-ASCII / UTF-8 ----------------
// The lexer scans bytes for ASCII structural decisions but hands content to
// UTF-8-aware tokenizers, so non-ASCII works without char-by-char scanning.

#[test]
fn cjk_in_string_literal() {
    let src = r#"print("你好")"#;
    assert!(has_kind(
        &matches(lang::python(), r#"print("你好")"#, src),
        "call",
    ));
    // and captured by a metavar
    let ms = matches(lang::python(), r"print(\S)", src);
    assert_eq!(cap(&ms, "S").as_deref(), Some(r#""你好""#));
}

#[test]
fn cjk_identifier() {
    // Unicode-aware identifier tokenizer: `变量` lexes as one identifier.
    let src = "变量 = 1";
    let ms = matches(lang::python(), r"变量 = \V", src);
    assert_eq!(cap(&ms, "V").as_deref(), Some("1"));
}

#[test]
fn emoji_in_string_and_as_arg() {
    let src = r#"f("😀", 你好)"#;
    let ms = matches(lang::typescript(), r"f(\(ARGS*))", src);
    assert_eq!(cap(&ms, "ARGS").as_deref(), Some(r#""😀", 你好"#));
}

#[test]
fn non_ascii_never_panics() {
    // The panic-safety is structural (all slices land on char boundaries), so
    // arbitrary non-ASCII placements must never crash — only lex as best effort.
    let cfg = lang::typescript();
    for pat in [
        "😀",
        r"\😀", // sigil then emoji (not a valid name → literal sigil + char)
        "a😀b",
        r#""你好\"#, // unterminated string with CJK + trailing backslash
        "λ + 你好 * \\X",
        "变量.😀()",
    ] {
        // just must not panic (compile may error; matching must never crash)
        let _ = cocoindex_code_match::Pattern::compile(pat, &cfg).map(|p| p.matches("x;"));
    }
}

#[test]
fn non_ascii_sigil() {
    // The sigil compares as a char, so a non-ASCII sigil works (and its bytes
    // are sliced correctly).
    let cfg = lang::typescript().with_meta_char('§');
    let src = "a = b;";
    let ms = cocoindex_code_match::Pattern::compile("§A = §B", &cfg)
        .unwrap()
        .matches(src);
    assert!(
        ms.iter()
            .any(|m| m.capture_text("A") == Some("a") && m.capture_text("B") == Some("b"))
    );
}

// ---------------- containment `\{{ ... \}}` ----------------

#[test]
fn contains_basic() {
    // `\{{ return \X \}}` asserts the function body *contains* `return \X` (any
    // depth). The whole function_definition is reported; X binds inside the group
    // and is exposed on the match.
    let src = "def foo():\n    x = 1\n    return a + b\n";
    let ms = matches(lang::python(), r"def foo(): \{{ return \X \}}", src);
    let m = ms
        .iter()
        .find(|m| m.kind == "function_definition")
        .unwrap_or_else(|| panic!("should match the function containing `return \\X`, got {ms:?}"));
    assert_eq!(m.capture_text("X"), Some("a + b"));
}

#[test]
fn contains_searches_any_depth() {
    // The `return` is nested inside an `if` — the descendant search must descend.
    let src = "def foo():\n    if c:\n        return a + b\n";
    let ms = matches(lang::python(), r"def foo(): \{{ return \X \}}", src);
    assert_eq!(cap(&ms, "X").as_deref(), Some("a + b"));
}

#[test]
fn contains_negative_when_absent() {
    // No `return` in the body → the containment predicate fails → no match.
    let src = "def foo():\n    x = 1\n";
    let ms = matches(lang::python(), r"def foo(): \{{ return \X \}}", src);
    assert!(
        !ms.iter().any(|m| m.kind == "function_definition"),
        "must not match a function whose body has no `return`, got {ms:?}",
    );
}

#[test]
fn contains_binding_threads_across_the_group() {
    // A name bound *before* the group constrains a use *inside* it (forward
    // threading): `\P` is the parameter and must equal the returned name.
    let pat = r"def foo(\P): \{{ return \P \}}";
    let yes = matches(lang::python(), pat, "def foo(a):\n    return a\n");
    assert!(
        yes.iter().any(|m| m.kind == "function_definition"),
        "param `a` and returned `a` are equal → match, got {yes:?}",
    );
    let no = matches(lang::python(), pat, "def foo(b):\n    return a\n");
    assert!(
        !no.iter().any(|m| m.kind == "function_definition"),
        "param `b` ≠ returned `a` → no match, got {no:?}",
    );
}

#[test]
fn contains_nested() {
    // Nested groups: foo's body contains an `if` whose body contains `return \X`.
    // Exercises the back-patched close indices and recursive `match_contains`.
    let src = "def foo():\n    if c:\n        return a + b\n";
    let ms = matches(
        lang::python(),
        r"def foo(): \{{ if \C: \{{ return \X \}} \}}",
        src,
    );
    let m = ms
        .iter()
        .find(|m| m.kind == "function_definition")
        .unwrap_or_else(|| panic!("nested containment should match, got {ms:?}"));
    assert_eq!(m.capture_text("C"), Some("c"));
    assert_eq!(m.capture_text("X"), Some("a + b"));
}

#[test]
fn contains_unbalanced_markers_error() {
    assert!(Pattern::compile(r"def foo(): \{{ return \X", &lang::python()).is_err());
    assert!(Pattern::compile(r"return \X \}}", &lang::python()).is_err());
}

// ---------------- single bare keyword / fragment range / anchored regex ----------------

#[test]
fn bare_keyword_matches_its_enclosing_node() {
    // A single anonymous-leaf token (a keyword) matches the node it's a direct
    // child of — `if` → `if_statement` — reported as the keyword fragment.
    let ms = matches(lang::python(), r"if", "if x:\n    pass\n");
    let m = by_text(&ms, "if").expect("`if` should match");
    assert_eq!(m.kind, "if_statement");

    let src = "if x:\n    pass\nelse:\n    pass\n";
    assert!(has_kind(
        &matches(lang::python(), r"else", src),
        "else_clause"
    ));
    // a single *identifier* already matched (it's a named leaf node) — unchanged
    assert!(has_kind(
        &matches(lang::python(), r"foo", "y = foo + 1"),
        "identifier"
    ));
}

#[test]
fn fragment_match_reports_the_fragment_span_not_the_whole_node() {
    // The pattern covers the signature but not the body: the reported range is the
    // signature *fragment*, with `kind` still the enclosing function node.
    let src = "def foo(a, b):\n    return a + b\n";
    let ms = matches(lang::python(), r"def \NAME(\(A*)):", src);
    let m = by_text(&ms, "def foo(a, b):").expect("fragment span, not whole node");
    assert_eq!(m.kind, "function_definition");
    assert_eq!(m.capture_text("NAME"), Some("foo"));
}

#[test]
fn regex_matcher_is_whole_node_anchored() {
    // Matchers anchor to the whole node text, so a bare `set_.*` is a *prefix*
    // (no false positive on `test_unset_is`, where `set_` is only a substring).
    let src = "def set_value(): pass\ndef test_unset_is(): pass\n";
    let prefix = matches(lang::python(), r"def \(N:/set_.*/)(\*):", src);
    assert_eq!(caps_all(&prefix, "N"), vec!["set_value"]);

    // exact (no `.*`): the text must equal the regex
    let exact = matches(lang::python(), r"def \(N:/set_value/)(\*):", src);
    assert_eq!(caps_all(&exact, "N"), vec!["set_value"]);

    // substring needs explicit `.*…*.`
    let substring = matches(lang::python(), r"def \(N:/.*set_.*/)(\*):", src);
    assert_eq!(
        caps_all(&substring, "N"),
        vec!["set_value", "test_unset_is"]
    );
}

#[test]
fn leading_wildcard_trailing_token_still_matches() {
    // `\*: Path` — leading wildcard, trailing literal token. The trailing-token
    // prune (which makes this fast on huge nodes) must not drop the real match.
    let ms = matches(lang::python(), r"\*: Path", "def f(x: Path):\n    pass\n");
    assert!(
        ms.iter().any(|m| m.text == "x: Path"),
        "should match the annotated `x: Path`, got {ms:?}",
    );
}

// ---------------- comments are transparent ----------------

#[test]
fn comment_between_tokens_is_ignored() {
    // A block comment sitting between call args must not break the match, and must
    // not leak into the capture.
    let ms = matches(lang::rust(), r"foo(\X)", "fn m() { foo(/* hi */ bar); }");
    assert_eq!(cap(&ms, "X").as_deref(), Some("bar"));
}

#[test]
fn code_inside_a_comment_is_not_matched() {
    // `foo(bar)` written inside a line comment must not match.
    let ms = matches(lang::python(), r"foo(\X)", "# foo(bar)\ny = 1\n");
    assert!(ms.is_empty(), "must not match inside a comment, got {ms:?}");
}

#[test]
fn comment_between_statements_is_ignored() {
    let src = "def f():\n    # leading comment\n    return a + b\n";
    let ms = matches(lang::python(), r"return \X", src);
    assert_eq!(cap(&ms, "X").as_deref(), Some("a + b"));
}

#[test]
fn star_run_does_not_absorb_a_comment_into_its_capture() {
    // A `\*` sibling run skips comment nodes — the captured text is the args only.
    let ms = matches(
        lang::rust(),
        r"foo(\(ARGS*))",
        "fn m() { foo(a, /* c */ b); }",
    );
    assert_eq!(cap(&ms, "ARGS").as_deref(), Some("a, /* c */ b"));
    // the matched call is reported, args captured across the comment
    assert!(has_kind(&ms, "call_expression"));
}

#[test]
fn prefilter_passes_comment_text_but_the_matcher_skips_it() {
    // The index-free prefilter is a raw-text scan, so it *can* pass on a term that
    // only appears in a comment (a sound false positive); the matcher then skips
    // the comment, so the end-to-end result is correctly no match.
    let pat = Pattern::compile(r"process(\X)", &lang::python()).unwrap();
    let pf = pat.prefilter(3);
    let src = "# process(stuff)\nx = 1\n";
    assert!(pf.might_match(src), "raw-text prefilter sees `process`");
    assert!(
        pat.matches_prefiltered(src, &pf).is_empty(),
        "matcher skips the comment → no match",
    );
}

// ---------------- operator/generics alignment ----------------

#[test]
fn alignment_operators_and_generics() {
    let ok = |cfg, frag, src| !matches(cfg, frag, src).is_empty();
    assert!(ok(lang::cpp(), "a >> b", "int n = a >> b;"));
    assert!(ok(
        lang::cpp(),
        "vector<vector<int>>",
        "vector<vector<int>> v;"
    ));
    assert!(ok(lang::c(), "p->field", "void g(){ p->field; }"));
    assert!(ok(
        lang::rust(),
        "std::mem::swap",
        "fn m(){ std::mem::swap(); }"
    ));
    assert!(ok(lang::typescript(), "a === b", "if (a === b) {}"));
}

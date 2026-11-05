use cocoindex_code_ast::CodeSource;
use cocoindex_code_ast::graph::{GraphSyntax, extract_code_graph};

fn graph(text: &str, language: &str) -> GraphSyntax {
    extract_code_graph(&CodeSource::with_language(text, language)).expect("supported source")
}

#[test]
fn cpp_classes_fields_overloads_and_unresolved_relations() {
    let source = r#"
namespace engine {
class Base {};
struct Worker : public Base, private api::Runner<int> {
    int value, count;
    int (*callback)(int);
    virtual int run(int n) const { return helper(n) + this->callback(n); }
    int run(double n) const;
};
int Worker::run(double n) const { return unknown::invoke(n); }
}
"#;
    let graph = graph(source, "cpp");
    assert!(!graph.has_errors, "{graph:#?}");
    let declarations: Vec<_> = graph
        .declarations
        .iter()
        .map(|declaration| (declaration.kind.as_str(), declaration.name.as_str()))
        .collect();
    assert_eq!(
        declarations,
        vec![
            ("class", "engine::Base"),
            ("struct", "engine::Worker"),
            ("field", "engine::Worker::value"),
            ("field", "engine::Worker::count"),
            ("field", "engine::Worker::callback"),
            ("method", "engine::Worker::run"),
            ("method", "engine::Worker::run"),
            ("function", "engine::Worker::run"),
        ]
    );
    let worker = &graph.declarations[1];
    assert_eq!(
        worker.signature,
        "struct Worker : public Base, private api::Runner<int>"
    );
    assert_eq!(graph.declarations[2].signature, "int value");
    assert_eq!(graph.declarations[3].signature, "int count");
    for declaration in &graph.declarations[2..7] {
        assert_eq!(declaration.parent_start_byte, Some(worker.start_byte));
    }
    let references: Vec<_> = graph
        .references
        .iter()
        .map(|reference| (reference.kind.as_str(), reference.spelling.as_str()))
        .collect();
    assert_eq!(
        references,
        vec![
            ("inherits", "Base"),
            ("inherits", "api::Runner<int>"),
            ("call", "helper"),
            ("call", "this->callback"),
            ("call", "unknown::invoke"),
        ]
    );
    assert_eq!(
        graph.references[2].source_start_byte,
        Some(graph.declarations[5].start_byte)
    );
    assert_eq!(
        graph.references[4].source_start_byte,
        Some(graph.declarations[7].start_byte)
    );
    for reference in &graph.references {
        assert_eq!(
            &source[reference.start_byte..reference.end_byte],
            reference.spelling
        );
    }
}

#[test]
fn c_prototypes_definitions_and_function_pointer_fields() {
    let graph = graph(
        r#"
struct Packet { int bytes[4], size; int (*decode)(int); };
int api(int), *allocate(int);
int (*factory(int n))(int) { return choose(n); }
int (*callback)(int);
int main(void) { return api(factory(1)(2)); }
"#,
        "c",
    );
    assert!(!graph.has_errors, "{graph:#?}");
    let declarations: Vec<_> = graph
        .declarations
        .iter()
        .map(|declaration| (declaration.kind.as_str(), declaration.name.as_str()))
        .collect();
    assert_eq!(
        declarations,
        vec![
            ("struct", "Packet"),
            ("field", "Packet::bytes"),
            ("field", "Packet::size"),
            ("field", "Packet::decode"),
            ("function", "api"),
            ("function", "allocate"),
            ("function", "factory"),
            ("function", "main"),
        ]
    );
    assert_eq!(graph.declarations[5].signature, "int *allocate(int)");
    assert_eq!(
        graph
            .references
            .iter()
            .map(|reference| reference.spelling.as_str())
            .collect::<Vec<_>>(),
        vec!["choose", "api", "factory(1)", "factory"]
    );
}

#[test]
fn comments_strings_and_preprocessor_bodies_do_not_create_facts() {
    let graph = graph(
        r#"
// class Fake { void invented() { bogus(); } };
#define DEFINE_FN() int made_up() { phantom(); }
#if ENABLED
int conditional() { conditional_call(); }
#endif
const char *text = "class StringFake { void fake(); }; call()";
int real() { return actual(); }
"#,
        "cpp",
    );
    assert!(!graph.has_errors, "{graph:#?}");
    assert_eq!(graph.declarations.len(), 1, "{graph:#?}");
    assert_eq!(graph.declarations[0].name, "real");
    assert_eq!(graph.references.len(), 1);
    assert_eq!(graph.references[0].spelling, "actual");
}

#[test]
fn malformed_input_is_flagged_and_error_subtrees_are_opaque() {
    let graph = graph(
        "int valid() { return ok(); }\nclass Broken { int @bad; };",
        "cpp",
    );
    assert!(graph.has_errors);
    assert!(
        graph
            .declarations
            .iter()
            .any(|declaration| declaration.name == "valid")
    );
    assert!(
        !graph
            .declarations
            .iter()
            .any(|declaration| declaration.name.ends_with("bad"))
    );
    assert_eq!(graph.references.len(), 1);
    assert_eq!(graph.references[0].spelling, "ok");
}

#[test]
fn qualified_templates_and_header_signatures_preserve_written_syntax() {
    let graph = graph(
        r#"
namespace one::two {
template<typename T> struct Box {
    T get() const & noexcept;
    T get() &&;
    ~Box() { finish<T>(); }
};
template<typename T> T make(T value) { return build<T>(value); }
}
"#,
        "c++",
    );
    assert!(!graph.has_errors, "{graph:#?}");
    assert_eq!(graph.declarations[0].name, "one::two::Box");
    assert_eq!(
        graph.declarations[0].signature,
        "template<typename T> struct Box"
    );
    assert_eq!(graph.declarations[1].signature, "T get() const & noexcept");
    assert_eq!(graph.declarations[2].signature, "T get() &&");
    assert_eq!(graph.declarations[3].name, "one::two::Box::~Box");
    assert_eq!(
        graph.declarations[4].signature,
        "template<typename T> T make(T value)"
    );
    assert_eq!(
        graph
            .references
            .iter()
            .map(|reference| reference.spelling.as_str())
            .collect::<Vec<_>>(),
        vec!["finish<T>", "build<T>"]
    );
}

#[test]
fn unsupported_language_is_an_error() {
    for source in [CodeSource::new(""), CodeSource::with_language("", "python")] {
        assert_eq!(
            extract_code_graph(&source).unwrap_err(),
            "code graph extraction supports only C and C++"
        );
    }
}

#[test]
fn elaborated_type_uses_do_not_redeclare_classes() {
    let graph = graph(
        "struct Item; struct Item { int value; }; struct Item *global; int read(struct Item *item) { return item->value; }",
        "c",
    );
    assert!(!graph.has_errors, "{graph:#?}");
    assert_eq!(
        graph
            .declarations
            .iter()
            .map(|declaration| declaration.name.as_str())
            .collect::<Vec<_>>(),
        vec!["Item", "Item", "Item::value", "read"],
    );
}

#[test]
fn prototype_default_calls_belong_to_the_prototype() {
    let graph = graph(
        "class Item { Item(); int read(int n = default_value()); friend void external(); };",
        "cpp",
    );
    assert!(!graph.has_errors, "{graph:#?}");
    assert_eq!(
        graph
            .declarations
            .iter()
            .map(|declaration| (declaration.kind.as_str(), declaration.name.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("class", "Item"),
            ("method", "Item::Item"),
            ("method", "Item::read"),
        ]
    );
    assert_eq!(graph.references.len(), 1);
    assert_eq!(
        graph.references[0].source_start_byte,
        Some(graph.declarations[2].start_byte)
    );
}

#[test]
fn unsupported_conversion_declarators_do_not_misattribute_calls() {
    let graph = graph(
        "class Item { operator bool() const { return ready(); } };",
        "cpp",
    );
    assert!(!graph.has_errors, "{graph:#?}");
    assert_eq!(graph.declarations.len(), 1);
    assert_eq!(graph.references.len(), 1);
    assert_eq!(graph.references[0].spelling, "ready");
    assert_eq!(graph.references[0].source_start_byte, None);
}

#[test]
fn deeply_nested_bodies_are_walked_without_rust_recursion() {
    let source = format!(
        "int outer() {{ {}return leaf();{} }}",
        "{".repeat(1000),
        "}".repeat(1000)
    );
    let graph = graph(&source, "c");
    assert!(!graph.has_errors, "{graph:#?}");
    assert_eq!(graph.declarations.len(), 1);
    assert_eq!(graph.references.len(), 1);
    assert_eq!(graph.references[0].spelling, "leaf");
    assert_eq!(
        graph.references[0].source_start_byte,
        Some(graph.declarations[0].start_byte)
    );
}

#[test]
fn nested_inline_type_and_lambda_bodies_do_not_enter_header_identity() {
    let source = "struct Outer { struct Inner { int value; int f() { return old_call(); } } member; int read(int n = []() { return 1; }()); };";
    let before = graph(source, "cpp");
    let after = graph(
        &source
            .replace("old_call()", "changed(123)")
            .replace("return 1;", "return 222;"),
        "cpp",
    );
    assert!(
        !before.has_errors && !after.has_errors,
        "{before:#?}\n{after:#?}"
    );
    assert_eq!(
        before
            .declarations
            .iter()
            .map(|declaration| (&declaration.name, &declaration.signature))
            .collect::<Vec<_>>(),
        after
            .declarations
            .iter()
            .map(|declaration| (&declaration.name, &declaration.signature))
            .collect::<Vec<_>>(),
    );
    assert_eq!(
        before
            .declarations
            .iter()
            .find(|declaration| declaration.name == "Outer::member")
            .expect("inline typed field")
            .signature,
        "struct Inner {} member"
    );
    assert_eq!(
        before
            .declarations
            .iter()
            .find(|declaration| declaration.name == "Outer::read")
            .expect("lambda default argument")
            .signature,
        "int read(int n = []() {}())"
    );
}

# Incremental C/C++ syntax graph

This v1 example indexes native Tree-sitter facts into Neo4j for source retrieval:
classes, structs, unions, functions, method declarations/definitions, fields,
lexical containment, and explicit call/base-type spellings. It uses the existing
Neo4j targets; no core engine or target connector API is added.

This is a syntactic starting point for #847, not a compiler-resolved call graph
or a complete graph RAG application. Every call and inheritance reference has
`resolution = 'unresolved'`. A `CPP_CALLS_SPELLING` edge points to a reference
site, not to an inferred callee. For example, `obj.run()` preserves `obj.run`;
`class C : Base<int>` preserves `Base<int>`. Even a same-file spelling match is
not proof of a C++ symbol binding. Includes, macros, overload selection, aliases,
receiver types, virtual dispatch, and cross-file linking need a compiler frontend.
Only C and C++ are supported. Preprocessor subtrees, including conditional bodies,
are opaque. Friend declarations, anonymous aggregates, and unsupported
declarators such as conversion operators may be omitted. Out-of-line definitions
retain their qualified spelling but are classified as functions because a
qualifier alone cannot distinguish a class from a namespace. A syntactically
valid tree does not imply complete extraction or a compilable program.

## Run from this checkout

The new operation requires a native extension built from this checkout. A
published release that predates this operation will not work. From the repo root,
use the contributor setup, then build with `uv run maturin develop`. Install the
existing `neo4j` extra in your own development environment. Start a dedicated
Neo4j instance and configure its connection in this example's `.env` using
`.env.example` as a template. Use a database dedicated to this app: the labels
`CppEntity`/`CppReference` and the three `CPP_*` relation types are app-owned.

```sh
cd examples/code_graph_neo4j
cocoindex update main.py
```

Set `CODE_SOURCE_DIR` to a C/C++ source directory, relative to the current
directory or absolute. `.h` files are parsed as C++; `.c` files as C. Files are
mounted by relative path. Run the command again after an edit or deletion to
apply incremental changes. The sample needs no embeddings, LLM, or compiler.

`pipeline.parse_file` memoizes extraction and tracks the source file through
`FileLike.read_text()`. Unchanged files reuse the graph. Changed declarations
replace their previous target states; missing declarations and deleted files
remove their owned nodes, reference sites, and relations. Parse recovery raises
before graph declarations, leaving the file's previous successful graph in place.
Fix the syntax and update again. Omitted preprocessor content does not establish
an empty compiler symbol table.

The explicit `@coco.fn(..., version=1)` in `pipeline.py` is the invalidation
boundary for native extractor/grammar changes. Bump it when those semantics
change; Python logic tracking cannot inspect Rust. Python pipeline edits outside
that versioned function are tracked normally.

IDs are SHA-256 digests of file path, declaration kind, qualified spelling,
header, lexical parent, and duplicate occurrence. They do not use byte offsets
or function bodies. A body edit or preceding comment preserves declaration IDs;
header edits, renames, moves, and inserting identical duplicate declarations can
change IDs. Prototypes and definitions are separate syntactic declarations.
Reference IDs use owner, kind, spelling, and occurrence; repeated identical call
sites can shift identity when another identical call is inserted. Byte spans are
half-open UTF-8 offsets in the original file.

## Retrieve context for a code question

Use a driver parameter (`$name`, for example `demo::Worker::run`) to retrieve the
declaration's source and its explicitly written calls:

```cypher
MATCH (e:CppEntity)
WHERE e.name = $name
OPTIONAL MATCH (e)-[:CPP_CALLS_SPELLING]->(r:CppReference)
RETURN e.file_path, e.start_byte, e.end_byte, e.text,
       e.signature,
       collect({spelling: r.spelling, resolution: r.resolution}) AS calls
```

Fetch a type's members and base-type spellings to expand a retrieval result:

```cypher
MATCH (c:CppEntity {name: $name})
OPTIONAL MATCH (c)-[:CPP_CONTAINS]->(m:CppEntity)
OPTIONAL MATCH (c)-[:CPP_INHERITS_SPELLING]->(b:CppReference)
RETURN c.file_path, c.text, collect(DISTINCT m.name) AS members,
       collect(DISTINCT b.spelling) AS unresolved_bases
```

Pass the source text and unresolved annotations as context to your chosen
retrieval/answering layer. A spelling search can find candidates, but must not
be presented as compiler-level resolution.

Prototype and field spans cover their individual declarators; `signature`
includes the shared type prefix. Nested implementation bodies in headers are
elided as `{}` for identity, while `text` preserves the original source span.

## Verification

```sh
cargo test -p cocoindex_code_ast --test graph
uv run pytest python/tests/ops/test_code_graph.py
uv run pytest python/tests/ops/test_code_graph_neo4j.py
```

The first two cover actual native extraction and per-file engine memo/update/
delete behavior. The Neo4j integration test needs `testcontainers[neo4j]`, the
`neo4j` extra, Docker, and the marked container image. It validates graph writes
and cleanup through this example against a real server. A skipped test does not
establish Neo4j integration success.

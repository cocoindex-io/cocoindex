# Product Recommendation Graph — Neo4j (Rust)

Rust port of the Python
[`product_recommendation`](../../product_recommendation) example.

An LLM tags what each product *is* and what *pairs* with it; the shared
taxonomy edges become a "people who bought this also need…" engine in Neo4j.
Point it at a folder of product JSON, and it re-extracts only what changes as
you edit the catalog.

## How it works

Two node types, two relationship types:

- **`Product`** nodes — one per listing (title, price), keyed by the file stem.
- **`Taxonomy`** nodes — one per distinct label (`gel pen`, `notebook`,
  `ink refill`), keyed by value and **shared** across products.
- **`PRODUCT_TAXONOMY`** edges — `Product → Taxonomy`: what the product is.
- **`PRODUCT_COMPLEMENTARY_TAXONOMY`** edges — `Product → Taxonomy`: what a
  buyer might also need.

Products whose *complementary* taxonomy matches another product's *is-a*
taxonomy are the things to recommend together.

Because taxonomy labels are shared across products, the pipeline runs in two
phases:

1. `process_file`, one child component per product JSON: declares the
   `Product` node, renders the listing to Markdown and asks the LLM for its
   taxonomies (`extract_taxonomy`, memoized by the rendered detail), and
   carries the labels forward.
2. `build_graph`, a single component that owns the deduplicated `Taxonomy`
   nodes and both edge types across all products.

## Parallel to the Python example

| Concern          | Python                                                 | Rust (this example)                                      |
| ---------------- | ------------------------------------------------------ | -------------------------------------------------------- |
| LLM extraction   | `instructor` + LiteLLM, Pydantic models                | `ApiChatClient::chat_json` + `serde` structs             |
| Prompt rendering | Jinja2 `PRODUCT_TEMPLATE`                              | `RawProduct::render` (`format!`)                         |
| Per-product phase| `coco.use_mount(component_subpath("file", key), ...)`  | `ctx.mount_each(files, |(key, _)| "file/{key}", ...)`    |
| Graph phase      | `coco.mount(component_subpath("build_graph"), ...)`    | `ctx.mount(&"build_graph", ...)`                         |
| Node targets     | `neo4j.mount_table_target`                             | `neo4j::mount_table_target`                              |
| Edge targets     | `neo4j.mount_relation_target` / `declare_relation`     | `neo4j::mount_relation_target` / `declare_relation`      |
| Row schema       | Dataclass annotations                                  | `#[derive(SchemaFields)]` + `TableSchema::from_row`      |
| Cache busting    | `ContextKey("llm_model", detect_change=True)`          | `context_key!(static LLM_MODEL: String, detect_change)`  |

The LLM goes through an OpenAI-compatible endpoint: `OPENAI_API_KEY`, and
`LLM_BASE_URL` for another provider (e.g. Ollama). The model id is a plain
model name (`gpt-4.1`), not a LiteLLM provider string.

## Run

**1. Start Neo4j:**

```sh
docker run -d -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/cocoindex --name cocoindex-neo4j neo4j:5.26-community
```

**2. Configure:**

```sh
export OPENAI_API_KEY=...
# optional: LLM_MODEL (default gpt-4.1), LLM_BASE_URL,
# NEO4J_URI (default bolt://localhost:7687), NEO4J_USER / NEO4J_PASSWORD (default neo4j / cocoindex), NEO4J_DATABASE
```

**3. Build the graph** — the example ships a `products/` folder of sample
listings (pens, notebooks, monitors, …):

```sh
cargo run                           # graph ./products
cargo run -- /path/to/products      # graph another folder (*.json)
```

On the 9 sample products that's 9 `Product` nodes, ~40 `Taxonomy` nodes, and
the two edge types wired up.

**4. Explore the recommendations** — open [Neo4j Browser](http://localhost:7474)
(`neo4j` / `cocoindex`) and ask the graph:

```cypher
-- Recommend products to pair with anything that is a "gel pen":
-- find products whose is-a taxonomy matches a pen's complementary taxonomy
MATCH (:Taxonomy {value: "gel pen"})<-[:PRODUCT_TAXONOMY]-(:Product)
      -[:PRODUCT_COMPLEMENTARY_TAXONOMY]->(need:Taxonomy)
MATCH (rec:Product)-[:PRODUCT_TAXONOMY]->(need)
RETURN DISTINCT rec.title
```

Edit one product and only that product re-extracts, then the graph diffs —
adding new nodes/edges and removing ones no longer supported anywhere.

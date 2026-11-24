# SEC EDGAR Analytics — Apache Doris (Rust)

Rust port of the Python [`sec_edgar_analytics`](../../sec_edgar_analytics)
example.

Scrub, chunk, embed, and tag 10-K text *and* XBRL JSON into one
[Apache Doris](https://doris.apache.org/) table — with a vector index *and* a
full-text index, fused with RRF at query time.

## How it works

Two source formats fan into one chunk table:

1. **Sources** — `*.txt` 10-K filings and `*.json` XBRL company facts (the
   JSON is rendered to searchable text first).
2. **Scrub & chunk** — strip SSNs / phones / emails **before** indexing, then
   split into overlapping chunks (`RecursiveSplitter`, Markdown-aware).
3. **Embed & tag** — a sentence-transformer embeds each chunk; a keyword pass
   tags `RISK:*` / `TOPIC:*` labels.
4. **Load into Doris** — one row per chunk, into a table with a vector (ANN)
   index and a full-text (inverted) index.

The same `mount_table_target_with_options` call declares both indexes:

```rust
let table = doris::mount_table_target_with_options(
    &ctx, &DORIS_DB, TABLE,
    TableSchema::from_row::<FilingChunk>(["chunk_id"])?
        .with_vector_schema("embedding", ctx.get_key(&EMBEDDER)?).await?,
    DorisTableOptions {
        vector_indexes: vec![VectorIndexDef::new("embedding").metric_type("l2_distance")],
        inverted_indexes: vec![InvertedIndexDef::new("text").parser("unicode")],
        ..DorisTableOptions::default()
    },
).await?;
```

Both sources `declare_row` into the **same** table — `chunk_id` is a stable
`uuid5` of the file and chunk offsets, so re-running reconciles cleanly instead
of duplicating. Each file is its own child component.

## Parallel to the Python example

| Concern          | Python                                           | Rust (this example)                                              |
| ---------------- | ------------------------------------------------ | ---------------------------------------------------------------- |
| Per-file compute | `coco.mount_each(process_filing, ...)`           | `ctx.mount_each` + `#[cocoindex::function]`                      |
| Chunking         | `RecursiveSplitter` (Markdown)                   | `ops::text::RecursiveSplitter` (Markdown)                        |
| Embeddings       | `SentenceTransformerEmbedder.embed`              | `SentenceTransformerEmbedder::embed(&ctx, text)` (memoized, batched) |
| Target           | `doris.mount_table_target(vector_indexes=, inverted_indexes=)` | `doris::mount_table_target_with_options` + `DorisTableOptions` |
| Row schema       | Dataclass + `Annotated[NDArray, EMBEDDER]`       | `#[derive(SchemaFields)]` + `#[coco(vector)]` + `with_vector_schema` |
| PII scrubbing    | `re.sub`                                         | `regex`                                                          |
| Hybrid search    | `search.py` (pymysql)                            | `cargo run -- search` (sqlx over the MySQL protocol)             |

## Run

> Needs **Apache Doris 4.0+** for vector-index support — the Python example's
> [`docker-compose.yml`](../../sec_edgar_analytics/docker-compose.yml) starts
> one.

**1. Start Doris (FE + BE):**

```sh
docker compose -f ../../sec_edgar_analytics/docker-compose.yml up -d fe be
```

**2. Configure** — the sample data (synthetic 10-K filings + XBRL company
facts, the output of the Python example's `download.py`) ships under
[`data/`](data/):

```sh
# optional: DORIS_FE_HOST (default localhost), DORIS_HTTP_PORT (8030), DORIS_QUERY_PORT (9030),
# DORIS_USERNAME (root), DORIS_PASSWORD, DORIS_DATABASE (sec_analytics),
# DORIS_BE_LOAD_HOST (rewrite the Stream Load BE redirect to a reachable host)
```

**3. Build the index:**

```sh
cargo run                        # index ./data
cargo run -- index /path/to/data # another data dir (filings/*.txt, company_facts/*.json)
```

On the sample data this loads 4 chunks (2 filings + 2 company-facts) into
Doris, creating both the ANN index on `embedding` and the inverted index on
`text`.

**4. Hybrid search** — vector + keyword, fused with Reciprocal Rank Fusion:

```sh
cargo run -- search "cloud computing and AI risk"
cargo run -- search "cloud revenue" --source facts
```

On the sample data that ranks Microsoft's cloud-and-AI filing first (it carries
both `TOPIC:CLOUD` and `TOPIC:AI`), Apple's second, and the company-facts rows
below.

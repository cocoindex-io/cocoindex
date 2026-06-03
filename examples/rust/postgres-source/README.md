# Postgres Source (Rust)

Rust port of the Python [`postgres_source`](../../postgres_source) example.

It reads product rows from a Postgres **source** table, computes a derived field
(`total_value = price × amount`) and a text embedding per row, and writes the
result into another Postgres table (with a pgvector index) using CocoIndex's
declarative `TableTarget`. Then it serves pgvector similarity search over the
output.

## Parallel to the Python example

| Concern            | Python                                            | Rust (this example)                                    |
| ------------------ | ------------------------------------------------- | ------------------------------------------------------ |
| Source rows        | `postgres.PgTableSource(...).fetch_rows()`        | `postgres::read_table::<SourceProduct>(db, "source_products")` |
| Per-row compute    | `@coco.fn(memo=True) process_product`             | `#[cocoindex::function(memo)] process_product`         |
| Output store       | `postgres.TableTarget` + vector index             | `postgres::mount_table_target` + `declare_vector_index` |
| Embeddings         | `sentence-transformers/all-MiniLM-L6-v2`          | `fastembed` `AllMiniLML6V2` (same model, 384-dim)      |

### Incrementality

- Unchanged source rows are **memo-skipped** — no re-embedding on re-runs.
- Changed source rows are **reprocessed**, and their output rows updated.
- Source rows that are **deleted** have their derived output rows reconciled
  away automatically (the managed `TableTarget` deletes the orphaned row).

## Run

Start a Postgres with pgvector and point `POSTGRES_URL` at it:

```bash
export POSTGRES_URL="postgres://cocoindex:cocoindex@localhost/cocoindex"
export SOURCE_DATABASE_URL="$POSTGRES_URL"  # optional; defaults to POSTGRES_URL

# 1. Create + seed the source table
psql "$SOURCE_DATABASE_URL" -f prepare_source_data.sql

# 2. Read source -> embed -> write output table (incremental on re-run)
cargo run -- index

# 3. Semantic search over the output
cargo run -- query "wireless headphones"
```

Re-run `cargo run -- index` after `UPDATE`/`DELETE`/`INSERT` on
`source_products` to see incremental reprocessing.

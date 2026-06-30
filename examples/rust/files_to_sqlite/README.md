# Files → SQLite (Rust)

A self-contained example of the Rust SDK's embedded **SQLite table target**
(`cocoindex::sqlite`). No external server is needed — SQLite is embedded.

It walks `**/*.md` / `**/*.txt` files under a source directory, computes a small
per-file summary (word count + first line), and writes one row per file into a
SQLite table through the declarative target. Reconciliation upserts changed
files, skips unchanged ones (the `summarize` function is memoized), and removes
rows for files that disappear.

| Concept | API |
| --- | --- |
| Connection | `sqlite::Database::connect(path)` |
| Schema | `sqlite::TableSchema` + `sqlite::ColumnDef` |
| Target | `sqlite::mount_table_target` + `TableTarget::declare_row` |
| Source | `cocoindex::fs::walk` + `Ctx::mount_each` |

## Run

```bash
# Walk the bundled data/ dir and write rows into ./files.db
cargo run -- index

# Or point at your own directory + db path
cargo run -- index /path/to/dir ./my.db

# List the indexed files by word count
cargo run -- query
```

Re-running `index` after editing/adding/deleting files reconciles the table:
changed files are re-summarized and upserted, unchanged files are skipped, and
rows for deleted files are removed.

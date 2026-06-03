//! Files → SQLite — a self-contained example of the `sqlite::TableTarget`.
//!
//! Walks text/markdown files in a source directory, computes a small per-file
//! summary (word count + first line), and writes one row per file into an
//! embedded SQLite table via the declarative table target. Reconciliation
//! upserts changed files, skips unchanged ones (memoized), and deletes rows for
//! files that disappeared — the same shape as the Postgres example, but with no
//! server (SQLite is embedded).
//!
//!   cargo run -- index [SOURCE_DIR] [DB_PATH]   # walk -> write rows
//!   cargo run -- query [DB_PATH]                 # list files by word count
//!
//! Defaults: SOURCE_DIR = this example's `data/`, DB_PATH = `./files.db`.

use std::path::PathBuf;
use std::sync::LazyLock;

use cocoindex::prelude::*;
use cocoindex::sqlite;
use serde::{Deserialize, Serialize};
use sqlx::Row as _;

static DB: LazyLock<ContextKey<sqlite::Database>> = LazyLock::new(|| {
    ContextKey::new_with_state("files_sqlite_db", |db: &sqlite::Database| {
        db.state_id().to_string()
    })
});

const TABLE: &str = "files";

/// One output row: the file path is the primary key.
#[derive(Clone, Serialize, Deserialize)]
struct FileRow {
    path: String,
    word_count: i64,
    first_line: String,
}

fn files_schema() -> Result<sqlite::TableSchema> {
    sqlite::TableSchema::new(
        [
            ("path", sqlite::ColumnDef::new("TEXT")),
            ("word_count", sqlite::ColumnDef::new("INTEGER")),
            ("first_line", sqlite::ColumnDef::new("TEXT")),
        ],
        ["path"],
    )
}

/// Summarize one file. Memoized: unchanged files are skipped on re-runs.
#[cocoindex::function(memo)]
async fn summarize(_ctx: &Ctx, file: &FileEntry) -> Result<FileRow> {
    let text = file.content_str()?;
    let word_count = text.split_whitespace().count() as i64;
    let first_line = text.lines().next().unwrap_or_default().to_string();
    Ok(FileRow {
        path: file.relative_path().to_string_lossy().into_owned(),
        word_count,
        first_line,
    })
}

async fn index(source_dir: PathBuf, db_path: String) -> Result<()> {
    let db = sqlite::Database::connect(&db_path).await?;
    let app = cocoindex::App::builder("FilesToSqlite")
        .db_path(".cocoindex_db")
        .provide_key(&DB, db)
        .build()
        .await?;

    let stats = app
        .run(move |ctx| {
            let source_dir = source_dir.clone();
            async move {
                let db = ctx.get_key(&DB)?;
                let table = sqlite::mount_table_target(&ctx, db, TABLE, files_schema()?).await?;

                let files = cocoindex::fs::walk(&source_dir, &["**/*.md", "**/*.txt"])?;
                ctx.mount_each(files, |file| file.key(), {
                    let table = table.clone();
                    move |file_ctx, file| {
                        let table = table.clone();
                        async move {
                            let row = summarize(&file_ctx, &file).await?;
                            table.declare_row(&file_ctx, &row)?;
                            Ok(())
                        }
                    }
                })
                .await?;
                Ok(())
            }
        })
        .await?;

    println!("{stats}");
    Ok(())
}

async fn query(db_path: String) -> Result<()> {
    let db = sqlite::Database::connect(&db_path).await?;
    let rows = sqlx::query(&format!(
        "SELECT path, word_count, first_line FROM \"{TABLE}\" ORDER BY word_count DESC"
    ))
    .fetch_all(db.pool())
    .await
    .map_err(|e| Error::engine(format!("sqlite query: {e}")))?;

    println!("{:>6}  {:<40}  {}", "words", "path", "first line");
    println!("{}", "-".repeat(80));
    for r in &rows {
        let path: String = r.get("path");
        let words: i64 = r.get("word_count");
        let first: String = r.get("first_line");
        let first: String = first.chars().take(40).collect();
        println!("{words:>6}  {path:<40}  {first}");
    }
    Ok(())
}

fn data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data")
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("query") => {
            let db_path = args.get(1).cloned().unwrap_or_else(|| "./files.db".to_string());
            query(db_path).await
        }
        _ => {
            let source_dir = args
                .get(1)
                .map(PathBuf::from)
                .unwrap_or_else(data_dir);
            let db_path = args.get(2).cloned().unwrap_or_else(|| "./files.db".to_string());
            index(source_dir, db_path).await
        }
    }
}

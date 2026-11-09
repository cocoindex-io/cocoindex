//! End-to-end test for the embedded `sqlite::TableTarget`.
//!
//! SQLite is embedded, so this runs everywhere with no external service — each
//! test uses a fresh temp `.db` file. Mirrors the Python `test_sqlite_target.py`
//! regular-table family: create/insert, update, delete, no-change, drop,
//! user-managed, multiple tables, and the declare (pending) variant.
#![cfg(feature = "sqlite")]

use cocoindex::connectors::sqlite;
use cocoindex::{App, Ctx, Environment, PreviewAction, Result};
use serde::Serialize;
use serde_json::json;
use sqlx::Row as _;

cocoindex::context_key!(
    static DB: sqlite::Database, key = "sqlite_target_test_db"
);

mod common;

#[derive(Clone, Serialize)]
struct Item {
    id: i64,
    label: String,
}

fn item(id: i64, label: &str) -> Item {
    Item {
        id,
        label: label.to_string(),
    }
}

fn item_schema() -> sqlite::TableSchema {
    sqlite::TableSchema::new(
        [
            ("id", sqlite::ColumnDef::new("INTEGER")),
            ("label", sqlite::ColumnDef::new("TEXT")),
        ],
        ["id"],
    )
    .unwrap()
}

async fn temp_db() -> (sqlite::Database, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("target.db");
    let db = sqlite::Database::connect(path.to_str().unwrap())
        .await
        .unwrap();
    (db, dir)
}

/// Build an app over a fresh state directory. Installs the SQL statement log
/// first: it has to be in place before the engine initializes its own tracing.
async fn build_app(db: &sqlite::Database) -> (App, tempfile::TempDir) {
    common::record_sql_statements();
    let dir = tempfile::tempdir().unwrap();
    let app = Environment::builder()
        .db_path(dir.path().join(".cocoindex_db"))
        .provide_key(&DB, db.clone())
        .build()
        .await
        .unwrap()
        .app("SqliteTargetTest")
        .await
        .unwrap();
    (app, dir)
}

async fn fetch(db: &sqlite::Database, table: &str) -> Vec<(i64, String)> {
    let rows = sqlx::query(&format!("SELECT id, label FROM \"{table}\" ORDER BY id"))
        .fetch_all(db.pool())
        .await
        .unwrap();
    rows.into_iter()
        .map(|r| (r.get::<i64, _>("id"), r.get::<String, _>("label")))
        .collect()
}

async fn declare_items(ctx: Ctx, items: Vec<Item>) -> Result<()> {
    let table = sqlite::mount_table_target(&ctx, &DB, "items", item_schema()).await?;
    for it in &items {
        table.declare_row(&ctx, it)?;
    }
    Ok(())
}

#[tokio::test]
async fn sqlite_table_target_insert_update_delete_nochange() {
    let (db, _dbdir) = temp_db().await;
    let (app, _appdir) = build_app(&db).await;

    // Insert two rows.
    app.run(|ctx| declare_items(ctx, vec![item(1, "one"), item(2, "two")]))
        .await
        .unwrap();
    assert_eq!(
        fetch(&db, "items").await,
        vec![(1, "one".to_string()), (2, "two".to_string())]
    );

    // Update one, add one, drop one.
    app.run(|ctx| declare_items(ctx, vec![item(2, "two-updated"), item(3, "three")]))
        .await
        .unwrap();
    assert_eq!(
        fetch(&db, "items").await,
        vec![(2, "two-updated".to_string()), (3, "three".to_string())]
    );

    // No-change re-run leaves the table identical.
    app.run(|ctx| declare_items(ctx, vec![item(2, "two-updated"), item(3, "three")]))
        .await
        .unwrap();
    assert_eq!(
        fetch(&db, "items").await,
        vec![(2, "two-updated".to_string()), (3, "three".to_string())]
    );
}

async fn table_exists(db: &sqlite::Database, table: &str) -> bool {
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?")
            .bind(table)
            .fetch_one(db.pool())
            .await
            .unwrap();
    count == 1
}

/// Planned actions as sorted strings. The connector's actions encode as
/// positional arrays: a table action is `[db_key, table_name, spec,
/// main_action, column_actions]`, a row action `[pk, state]`.
fn describe(actions: &[PreviewAction]) -> Vec<String> {
    let mut described: Vec<String> = actions
        .iter()
        .map(|action| {
            let (verb, value) = match action {
                PreviewAction::Create(v) => ("create", v),
                PreviewAction::Update(v) => ("update", v),
                PreviewAction::Delete(v) => ("delete", v),
            };
            let value: serde_json::Value = value.decode().unwrap();
            match value.as_array().map(Vec::as_slice) {
                Some([_db_key, table, _spec, main_action, column_actions]) => {
                    format!("{verb} table {table} main={main_action} columns={column_actions}")
                }
                Some([pk, state]) => format!("{verb} row {pk} {}", state[0]),
                _ => panic!("unexpected action payload {value}"),
            }
        })
        .collect();
    described.sort();
    described
}

#[tokio::test]
async fn sqlite_table_target_previews_rows() {
    let (db, _dbdir) = temp_db().await;
    let (app, _appdir) = build_app(&db).await;
    let table_count = || async {
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='items'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap()
    };

    // The table would be created and both rows written (the connector plans
    // every row write as an upsert); nothing is.
    let actions = app
        .preview(|ctx| declare_items(ctx, vec![item(1, "one"), item(2, "two")]))
        .await
        .unwrap();
    assert_eq!(
        describe(&actions),
        vec![
            r#"update row [1] {"id":1,"label":"one"}"#,
            r#"update row [2] {"id":2,"label":"two"}"#,
            r#"update table "items" main="insert" columns={}"#,
        ]
    );
    assert_eq!(table_count().await, 0);

    app.run(|ctx| declare_items(ctx, vec![item(1, "one"), item(2, "two")]))
        .await
        .unwrap();

    // Against the applied state: rows 2 and 3 written; row 1 and the table
    // unchanged; the container action is still listed. Nothing is applied.
    let actions = app
        .preview(|ctx| {
            declare_items(
                ctx,
                vec![item(1, "one"), item(2, "two-updated"), item(3, "three")],
            )
        })
        .await
        .unwrap();
    assert_eq!(
        describe(&actions),
        vec![
            r#"update row [2] {"id":2,"label":"two-updated"}"#,
            r#"update row [3] {"id":3,"label":"three"}"#,
            r#"update table "items" main=null columns={}"#,
        ]
    );
    assert_eq!(
        fetch(&db, "items").await,
        vec![(1, "one".to_string()), (2, "two".to_string())]
    );
}

#[tokio::test]
async fn sqlite_undeclared_and_dropped_tables_are_removed() {
    let (db, _dbdir) = temp_db().await;
    let (app, _appdir) = build_app(&db).await;

    app.run(|ctx| declare_items(ctx, vec![item(1, "one")]))
        .await
        .unwrap();
    assert!(table_exists(&db, "items").await);

    // A run that no longer declares the table drops it.
    app.run(|_ctx| async { Ok(()) }).await.unwrap();
    assert!(
        !table_exists(&db, "items").await,
        "a system-managed table no longer declared is dropped"
    );

    app.run(|ctx| declare_items(ctx, vec![item(1, "one")]))
        .await
        .unwrap();
    assert!(table_exists(&db, "items").await);

    app.drop_state().await.unwrap();
    assert!(
        !table_exists(&db, "items").await,
        "drop_state drops the table"
    );
}

#[tokio::test]
async fn sqlite_user_managed_table_keeps_table_manages_rows() {
    let (db, _dbdir) = temp_db().await;
    // Pre-create the table; CocoIndex must NOT drop it, only manage rows.
    sqlx::query("CREATE TABLE \"items\" (id INTEGER PRIMARY KEY, label TEXT)")
        .execute(db.pool())
        .await
        .unwrap();
    let (app, _appdir) = build_app(&db).await;

    async fn declare_user(ctx: Ctx, items: Vec<Item>) -> Result<()> {
        let table = sqlite::mount_table_target_with_options(
            &ctx,
            &DB,
            "items",
            item_schema(),
            sqlite::SqliteTableOptions {
                managed_by: cocoindex::ManagedBy::User,
                ..Default::default()
            },
        )
        .await?;
        for it in &items {
            table.declare_row(&ctx, it)?;
        }
        Ok(())
    }

    app.run(|ctx| declare_user(ctx, vec![item(1, "one")]))
        .await
        .unwrap();
    assert_eq!(fetch(&db, "items").await, vec![(1, "one".to_string())]);

    // Remove all rows; the user-managed table must still exist (empty).
    app.run(|ctx| declare_user(ctx, vec![])).await.unwrap();
    assert!(
        table_exists(&db, "items").await,
        "user-managed table must persist when empty"
    );
    assert_eq!(fetch(&db, "items").await, Vec::<(i64, String)>::new());
}

#[tokio::test]
async fn sqlite_multiple_tables_in_one_run() {
    let (db, _dbdir) = temp_db().await;
    let (app, _appdir) = build_app(&db).await;

    app.run(|ctx| async move {
        let users = sqlite::mount_table_target(
            &ctx,
            &DB,
            "users",
            sqlite::TableSchema::new(
                [
                    ("id", sqlite::ColumnDef::new("INTEGER")),
                    ("label", sqlite::ColumnDef::new("TEXT")),
                ],
                ["id"],
            )?,
        )
        .await?;
        let products = sqlite::mount_table_target(
            &ctx,
            &DB,
            "products",
            sqlite::TableSchema::new(
                [
                    ("id", sqlite::ColumnDef::new("INTEGER")),
                    ("label", sqlite::ColumnDef::new("TEXT")),
                ],
                ["id"],
            )?,
        )
        .await?;
        users.declare_row(&ctx, &item(1, "alice"))?;
        products.declare_row(&ctx, &item(10, "widget"))?;
        Ok(())
    })
    .await
    .unwrap();

    assert_eq!(fetch(&db, "users").await, vec![(1, "alice".to_string())]);
    assert_eq!(
        fetch(&db, "products").await,
        vec![(10, "widget".to_string())]
    );
}

// ---------------------------------------------------------------------------
// Schema evolution — incremental ALTER TABLE (mirrors Python's composite-layer
// behavior). Before the composite rewrite these would silently no-op
// (`CREATE TABLE IF NOT EXISTS` on an existing table never alters it).
// ---------------------------------------------------------------------------

/// Column `(name, type)` pairs as SQLite reports them (PRAGMA table_info).
async fn table_columns(db: &sqlite::Database, table: &str) -> Vec<(String, String)> {
    let rows = sqlx::query(&format!("PRAGMA table_info(\"{table}\")"))
        .fetch_all(db.pool())
        .await
        .unwrap();
    rows.into_iter()
        .map(|r| (r.get::<String, _>("name"), r.get::<String, _>("type")))
        .collect()
}

fn col(ty: &str) -> sqlite::ColumnDef {
    sqlite::ColumnDef::new(ty)
}

#[tokio::test]
async fn sqlite_add_column_preserves_rows() {
    let (db, _dbdir) = temp_db().await;
    let (app, _appdir) = build_app(&db).await;

    // v1: (id, label)
    app.run(|ctx| async move {
        let s = sqlite::TableSchema::new([("id", col("INTEGER")), ("label", col("TEXT"))], ["id"])?;
        let t = sqlite::mount_table_target(&ctx, &DB, "items", s).await?;
        t.declare_row(&ctx, &json!({"id": 1, "label": "one"}))?;
        t.declare_row(&ctx, &json!({"id": 2, "label": "two"}))?;
        Ok(())
    })
    .await
    .unwrap();

    // v2: add nullable `extra` column; same PK → incremental ALTER TABLE ADD COLUMN.
    app.run(|ctx| async move {
        let s = sqlite::TableSchema::new(
            [
                ("id", col("INTEGER")),
                ("label", col("TEXT")),
                ("extra", col("TEXT")),
            ],
            ["id"],
        )?;
        let t = sqlite::mount_table_target(&ctx, &DB, "items", s).await?;
        t.declare_row(&ctx, &json!({"id": 1, "label": "one", "extra": "x1"}))?;
        t.declare_row(&ctx, &json!({"id": 2, "label": "two"}))?; // extra → NULL
        Ok(())
    })
    .await
    .unwrap();

    // The column was added (table NOT dropped+recreated), and rows survived.
    let cols = table_columns(&db, "items").await;
    assert!(
        cols.iter().any(|(n, _)| n == "extra"),
        "expected `extra` column to be added via ALTER TABLE, got {cols:?}"
    );
    let rows = sqlx::query("SELECT id, label, extra FROM \"items\" ORDER BY id")
        .fetch_all(db.pool())
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<i64, _>("id"), 1);
    assert_eq!(rows[0].get::<String, _>("label"), "one");
    assert_eq!(rows[0].get::<Option<String>, _>("extra"), Some("x1".into()));
    assert_eq!(rows[1].get::<Option<String>, _>("extra"), None);
}

#[tokio::test]
async fn sqlite_drop_column_preserves_rows() {
    let (db, _dbdir) = temp_db().await;
    let (app, _appdir) = build_app(&db).await;

    // v1: (id, label, extra)
    app.run(|ctx| async move {
        let s = sqlite::TableSchema::new(
            [
                ("id", col("INTEGER")),
                ("label", col("TEXT")),
                ("extra", col("TEXT")),
            ],
            ["id"],
        )?;
        let t = sqlite::mount_table_target(&ctx, &DB, "items", s).await?;
        t.declare_row(&ctx, &json!({"id": 1, "label": "one", "extra": "x1"}))?;
        Ok(())
    })
    .await
    .unwrap();

    // v2: drop `extra`; same PK → incremental ALTER TABLE DROP COLUMN.
    app.run(|ctx| async move {
        let s = sqlite::TableSchema::new([("id", col("INTEGER")), ("label", col("TEXT"))], ["id"])?;
        let t = sqlite::mount_table_target(&ctx, &DB, "items", s).await?;
        t.declare_row(&ctx, &json!({"id": 1, "label": "one"}))?;
        Ok(())
    })
    .await
    .unwrap();

    let cols = table_columns(&db, "items").await;
    assert!(
        !cols.iter().any(|(n, _)| n == "extra"),
        "expected `extra` column to be dropped, got {cols:?}"
    );
    assert_eq!(fetch(&db, "items").await, vec![(1, "one".to_string())]);
}

#[tokio::test]
async fn sqlite_change_column_type_recreates_column() {
    let (db, _dbdir) = temp_db().await;
    let (app, _appdir) = build_app(&db).await;

    // v1: score TEXT
    app.run(|ctx| async move {
        let s = sqlite::TableSchema::new([("id", col("INTEGER")), ("score", col("TEXT"))], ["id"])?;
        let t = sqlite::mount_table_target(&ctx, &DB, "scores", s).await?;
        t.declare_row(&ctx, &json!({"id": 1, "score": "10"}))?;
        Ok(())
    })
    .await
    .unwrap();

    // v2: score INTEGER; same PK → column type change (DROP + ADD COLUMN).
    app.run(|ctx| async move {
        let s =
            sqlite::TableSchema::new([("id", col("INTEGER")), ("score", col("INTEGER"))], ["id"])?;
        let t = sqlite::mount_table_target(&ctx, &DB, "scores", s).await?;
        t.declare_row(&ctx, &json!({"id": 1, "score": 42}))?;
        Ok(())
    })
    .await
    .unwrap();

    let cols = table_columns(&db, "scores").await;
    let score_type = cols
        .iter()
        .find(|(n, _)| n == "score")
        .map(|(_, t)| t.to_uppercase());
    assert_eq!(
        score_type.as_deref(),
        Some("INTEGER"),
        "score column type should be rewritten to INTEGER, got {cols:?}"
    );
    // PK row identity preserved across the column rewrite; new value applied.
    let row = sqlx::query("SELECT id, score FROM \"scores\"")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(row.get::<i64, _>("id"), 1);
    assert_eq!(row.get::<i64, _>("score"), 42);
}

#[tokio::test]
async fn sqlite_column_type_change_that_cannot_drop_fails_instead_of_keeping_old_type() {
    let (db, _dbdir) = temp_db().await;
    let (app, _appdir) = build_app(&db).await;

    app.run(|ctx| async move {
        let s = sqlite::TableSchema::new([("id", col("INTEGER")), ("score", col("TEXT"))], ["id"])?;
        let t = sqlite::mount_table_target(&ctx, &DB, "scores", s).await?;
        t.declare_row(&ctx, &json!({"id": 1, "score": "10"}))?;
        Ok(())
    })
    .await
    .unwrap();

    // SQLite cannot drop a column that an index references.
    sqlx::query("CREATE INDEX scores_score ON \"scores\" (\"score\")")
        .execute(db.pool())
        .await
        .unwrap();

    let change_type = || {
        app.run(|ctx| async move {
            let s = sqlite::TableSchema::new(
                [("id", col("INTEGER")), ("score", col("INTEGER"))],
                ["id"],
            )?;
            let t = sqlite::mount_table_target(&ctx, &DB, "scores", s).await?;
            t.declare_row(&ctx, &json!({"id": 1, "score": 42}))?;
            Ok(())
        })
    };
    let err = change_type().await.unwrap_err();
    assert!(err.to_string().contains("score"), "{err}");

    // Once the blocking index is gone, the next run retries and applies the
    // change, because the failed run recorded nothing.
    sqlx::query("DROP INDEX scores_score")
        .execute(db.pool())
        .await
        .unwrap();
    change_type().await.unwrap();
    let cols = table_columns(&db, "scores").await;
    assert!(
        cols.iter()
            .any(|(n, t)| n == "score" && t.eq_ignore_ascii_case("INTEGER")),
        "{cols:?}"
    );
}

#[tokio::test]
async fn sqlite_mount_each_per_item_rows() {
    let (db, _dbdir) = temp_db().await;
    let (app, _appdir) = build_app(&db).await;

    // The canonical source→target shape: mount the table foreground, then declare
    // one row per item from `mount_each` sub-components (mirrors Python's
    // `await coco.mount_each(process, items, target)`).
    app.run(|ctx| async move {
        let table = sqlite::mount_table_target(&ctx, &DB, "items", item_schema()).await?;
        ctx.mount_each(
            vec![item(1, "one"), item(2, "two"), item(3, "three")],
            |it| it.id.to_string(),
            move |child, it| {
                let table = table.clone();
                async move { table.declare_row(&child, &it) }
            },
        )
        .await?;
        Ok(())
    })
    .await
    .unwrap();

    assert_eq!(
        fetch(&db, "items").await,
        vec![
            (1, "one".to_string()),
            (2, "two".to_string()),
            (3, "three".to_string())
        ]
    );
}

// ---------------------------------------------------------------------------
// Derived-schema overrides and `sqlite-vec` loading.
// ---------------------------------------------------------------------------

cocoindex::context_key!(
    static EMBEDDER: cocoindex::VectorSchema, key = "sqlite_target_test_embedder"
);

#[derive(Clone, Serialize, cocoindex::SchemaFields)]
struct DocRow {
    id: i64,
    title: String,
    #[coco(vector)]
    embedding: Vec<f32>,
}

async fn build_app_with_embedder(db: &sqlite::Database, name: &str) -> (App, tempfile::TempDir) {
    common::record_sql_statements();
    let dir = tempfile::tempdir().unwrap();
    let app = Environment::builder()
        .db_path(dir.path().join(".cocoindex_db"))
        .provide_key(&DB, db.clone())
        .provide_key(&EMBEDDER, cocoindex::VectorSchema::f32(3))
        .build()
        .await
        .unwrap()
        .app(name)
        .await
        .unwrap();
    (app, dir)
}

/// The embedder in the context supplies the vector dimension, and a per-call
/// type override replaces one derived column type.
#[tokio::test]
async fn sqlite_from_row_resolves_vector_from_context_provider() {
    let (db, _db_dir) = temp_db().await;
    let (app, _dir) = build_app_with_embedder(&db, "SqliteProviderSchemaTest").await;

    app.run(|ctx| async move {
        let schema = sqlite::TableSchema::from_row::<DocRow>(["id"])?
            .with_vector_schema("embedding", ctx.get_key(&EMBEDDER)?)
            .await?
            .with_column_type("title", "VARCHAR(40)")?;
        let table = sqlite::mount_table_target(&ctx, &DB, "docs", schema).await?;
        table.declare_row(
            &ctx,
            &DocRow {
                id: 1,
                title: "first".to_string(),
                embedding: vec![0.0, 1.0, 0.0],
            },
        )?;
        Ok(())
    })
    .await
    .unwrap();

    assert_eq!(
        table_columns(&db, "docs").await,
        vec![
            ("embedding".to_string(), "float[3]".to_string()),
            ("id".to_string(), "INTEGER".to_string()),
            ("title".to_string(), "VARCHAR(40)".to_string()),
        ]
    );
    let titles: Vec<String> = sqlx::query_scalar("SELECT title FROM docs")
        .fetch_all(db.pool())
        .await
        .unwrap();
    assert_eq!(titles, vec!["first".to_string()]);
}

#[tokio::test]
async fn sqlite_connect_with_vec_fails_when_the_extension_is_missing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("target.db");
    let missing = dir.path().join("no_such_vec0");
    let result =
        sqlite::Database::connect_with_vec(path.to_str().unwrap(), missing.to_str().unwrap()).await;
    let message = result
        .expect_err("a missing sqlite-vec library must fail the connection")
        .to_string();
    assert!(message.contains("sqlite"), "{message}");
}

/// With `sqlite-vec` loaded, a `vec0` table is created, filled and searched.
/// Skips unless `SQLITE_VEC_EXTENSION` names the loadable library (e.g. the
/// `vec0.so` inside the Python `sqlite-vec` package).
#[tokio::test]
async fn sqlite_connect_with_vec_creates_and_searches_a_vec0_table() {
    let Ok(extension) = std::env::var("SQLITE_VEC_EXTENSION") else {
        eprintln!("skipping sqlite-vec test; SQLITE_VEC_EXTENSION is not set");
        return;
    };
    let db_dir = tempfile::tempdir().unwrap();
    let path = db_dir.path().join("target.db");
    let db = sqlite::Database::connect_with_vec(path.to_str().unwrap(), &extension)
        .await
        .unwrap();
    let (app, _dir) = build_app_with_embedder(&db, "SqliteVecTest").await;

    app.run(|ctx| async move {
        let schema = sqlite::TableSchema::from_row::<DocRow>(["id"])?
            .with_vector_schema("embedding", ctx.get_key(&EMBEDDER)?)
            .await?;
        let table = sqlite::mount_table_target_with_options(
            &ctx,
            &DB,
            "vec_docs",
            schema,
            sqlite::SqliteTableOptions {
                virtual_table_def: Some(sqlite::Vec0TableDef {
                    auxiliary_columns: vec!["title".to_string()],
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await?;
        for (id, title, embedding) in [(1, "x", vec![1.0, 0.0, 0.0]), (2, "y", vec![0.0, 1.0, 0.0])]
        {
            table.declare_row(
                &ctx,
                &DocRow {
                    id,
                    title: title.to_string(),
                    embedding,
                },
            )?;
        }
        Ok(())
    })
    .await
    .unwrap();

    let nearest: Vec<i64> = sqlx::query_scalar(
        "SELECT id FROM vec_docs WHERE embedding MATCH '[0.0, 0.9, 0.1]' AND k = 1",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(nearest, vec![2]);
}

async fn declare_batch(ctx: Ctx, items: Vec<Item>) -> Result<()> {
    let table = sqlite::mount_table_target(&ctx, &DB, "batch_items", item_schema()).await?;
    for it in &items {
        table.declare_row(&ctx, it)?;
    }
    Ok(())
}

#[tokio::test]
async fn sqlite_applies_a_batch_in_chunked_statements() {
    let (db, _dbdir) = temp_db().await;
    let (app, _appdir) = build_app(&db).await;

    // 20,000 rows of 2 columns: two upsert statements (16,383 rows fit the
    // 32,766-parameter bind limit), not one statement per row.
    let items: Vec<Item> = (1..=20_000).map(|id| item(id, &format!("l{id}"))).collect();
    app.run(|ctx| declare_batch(ctx, items)).await.unwrap();
    let inserts = common::sql_statements_containing("INSERT INTO \"batch_items\"");
    assert_eq!(inserts.len(), 2, "{}", inserts.len());
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM batch_items")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(count, 20_000);

    // Keep 10 rows: 19,990 deletes in one `IN` list, 10 upserts in one statement.
    let kept: Vec<Item> = (1..=10).map(|id| item(id, &format!("k{id}"))).collect();
    app.run(|ctx| declare_batch(ctx, kept)).await.unwrap();
    let deletes = common::sql_statements_containing("DELETE FROM \"batch_items\"");
    assert_eq!(deletes.len(), 1, "{}", deletes.len());
    let inserts = common::sql_statements_containing("INSERT INTO \"batch_items\"");
    assert_eq!(inserts.len(), 3, "{}", inserts.len());
    assert_eq!(
        fetch(&db, "batch_items").await,
        (1..=10)
            .map(|id| (id, format!("k{id}")))
            .collect::<Vec<_>>()
    );
}

#[derive(Clone, Serialize)]
struct Pair {
    left: String,
    right: i64,
    label: String,
}

async fn declare_pairs(ctx: Ctx, pairs: Vec<Pair>) -> Result<()> {
    let schema = sqlite::TableSchema::new(
        [
            ("left", sqlite::ColumnDef::new("TEXT")),
            ("right", sqlite::ColumnDef::new("INTEGER")),
            ("label", sqlite::ColumnDef::new("TEXT")),
        ],
        ["left", "right"],
    )?;
    let table = sqlite::mount_table_target(&ctx, &DB, "pairs", schema).await?;
    for pair in &pairs {
        table.declare_row(&ctx, pair)?;
    }
    Ok(())
}

#[tokio::test]
async fn sqlite_composite_key_rows_are_deleted_by_row_value() {
    let (db, _dbdir) = temp_db().await;
    let (app, _appdir) = build_app(&db).await;
    fn pair(left: &str, right: i64) -> Pair {
        Pair {
            left: left.to_string(),
            right,
            label: format!("{left}{right}"),
        }
    }
    app.run(|ctx| declare_pairs(ctx, vec![pair("a", 1), pair("a", 2), pair("b", 1)]))
        .await
        .unwrap();
    // Dropping (a, 2) and (b, 1) must leave (a, 1): the keys are matched as
    // rows, not column by column.
    app.run(|ctx| declare_pairs(ctx, vec![pair("a", 1)]))
        .await
        .unwrap();
    let rows: Vec<(String, i64)> =
        sqlx::query_as("SELECT left, right FROM pairs ORDER BY left, right")
            .fetch_all(db.pool())
            .await
            .unwrap();
    assert_eq!(rows, vec![("a".to_string(), 1)]);
}

#[tokio::test]
async fn sqlite_connect_with_options_sets_the_busy_timeout() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("options.db");
    let db = sqlite::Database::connect_with_options(
        path.to_str().unwrap(),
        sqlite::ConnectOptions {
            busy_timeout: std::time::Duration::from_millis(1234),
            vec_extension: None,
        },
    )
    .await
    .unwrap();
    let timeout_ms: i64 = sqlx::query_scalar("PRAGMA busy_timeout")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(timeout_ms, 1234);
}

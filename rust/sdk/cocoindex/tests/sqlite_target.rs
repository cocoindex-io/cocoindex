//! End-to-end test for the embedded `sqlite::TableTarget`.
//!
//! SQLite is embedded, so this runs everywhere with no external service — each
//! test uses a fresh temp `.db` file. Mirrors the Python `test_sqlite_target.py`
//! regular-table family: create/insert, update, delete, no-change, drop,
//! user-managed, multiple tables, and the declare (pending) variant.
#![cfg(feature = "sqlite")]

use std::sync::LazyLock;

use cocoindex::{App, ContextKey, Ctx, Result, sqlite};
use serde::Serialize;
use serde_json::json;
use sqlx::Row as _;

static DB: LazyLock<ContextKey<sqlite::Database>> = LazyLock::new(|| {
    ContextKey::new_with_state("sqlite_target_test_db", |db: &sqlite::Database| {
        db.state_id().to_string()
    })
});

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

async fn build_app(db: &sqlite::Database) -> (App, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let app = App::builder("SqliteTargetTest")
        .db_path(dir.path().join(".cocoindex_db"))
        .provide_key(&DB, db.clone())
        .build()
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
    let db = ctx.get_key(&DB)?;
    let table = sqlite::mount_table_target(&ctx, db, "items", item_schema()).await?;
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

#[tokio::test]
async fn sqlite_table_dropped_when_no_longer_declared() {
    let (db, _dbdir) = temp_db().await;
    let (app, _appdir) = build_app(&db).await;

    app.run(|ctx| declare_items(ctx, vec![item(1, "one")]))
        .await
        .unwrap();
    let exists: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='items'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(exists, 1);

    // Re-register the table provider but declare nothing → the orphaned table
    // is dropped (handler present, table state absent).
    app.run(|ctx| async move {
        let db = ctx.get_key(&DB)?;
        let _ = sqlite::table_target(&ctx, db, "items", item_schema())?;
        Ok(())
    })
    .await
    .unwrap();
    let exists: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='items'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(exists, 0, "orphaned system-managed table should be dropped");
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
        let db = ctx.get_key(&DB)?;
        let table = sqlite::mount_table_target_with_options(
            &ctx,
            db,
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
    let exists: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='items'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(exists, 1, "user-managed table must persist when empty");
    assert_eq!(fetch(&db, "items").await, Vec::<(i64, String)>::new());
}

#[tokio::test]
async fn sqlite_multiple_tables_in_one_run() {
    let (db, _dbdir) = temp_db().await;
    let (app, _appdir) = build_app(&db).await;

    app.run(|ctx| async move {
        let db = ctx.get_key(&DB)?;
        let users = sqlite::mount_table_target(
            &ctx,
            db,
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
            db,
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
        let db = ctx.get_key(&DB)?;
        let s = sqlite::TableSchema::new([("id", col("INTEGER")), ("label", col("TEXT"))], ["id"])?;
        let t = sqlite::mount_table_target(&ctx, db, "items", s).await?;
        t.declare_row(&ctx, &json!({"id": 1, "label": "one"}))?;
        t.declare_row(&ctx, &json!({"id": 2, "label": "two"}))?;
        Ok(())
    })
    .await
    .unwrap();

    // v2: add nullable `extra` column; same PK → incremental ALTER TABLE ADD COLUMN.
    app.run(|ctx| async move {
        let db = ctx.get_key(&DB)?;
        let s = sqlite::TableSchema::new(
            [
                ("id", col("INTEGER")),
                ("label", col("TEXT")),
                ("extra", col("TEXT")),
            ],
            ["id"],
        )?;
        let t = sqlite::mount_table_target(&ctx, db, "items", s).await?;
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
        let db = ctx.get_key(&DB)?;
        let s = sqlite::TableSchema::new(
            [
                ("id", col("INTEGER")),
                ("label", col("TEXT")),
                ("extra", col("TEXT")),
            ],
            ["id"],
        )?;
        let t = sqlite::mount_table_target(&ctx, db, "items", s).await?;
        t.declare_row(&ctx, &json!({"id": 1, "label": "one", "extra": "x1"}))?;
        Ok(())
    })
    .await
    .unwrap();

    // v2: drop `extra`; same PK → incremental ALTER TABLE DROP COLUMN.
    app.run(|ctx| async move {
        let db = ctx.get_key(&DB)?;
        let s = sqlite::TableSchema::new([("id", col("INTEGER")), ("label", col("TEXT"))], ["id"])?;
        let t = sqlite::mount_table_target(&ctx, db, "items", s).await?;
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
        let db = ctx.get_key(&DB)?;
        let s = sqlite::TableSchema::new([("id", col("INTEGER")), ("score", col("TEXT"))], ["id"])?;
        let t = sqlite::mount_table_target(&ctx, db, "scores", s).await?;
        t.declare_row(&ctx, &json!({"id": 1, "score": "10"}))?;
        Ok(())
    })
    .await
    .unwrap();

    // v2: score INTEGER; same PK → column type change (DROP + ADD COLUMN).
    app.run(|ctx| async move {
        let db = ctx.get_key(&DB)?;
        let s =
            sqlite::TableSchema::new([("id", col("INTEGER")), ("score", col("INTEGER"))], ["id"])?;
        let t = sqlite::mount_table_target(&ctx, db, "scores", s).await?;
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
async fn sqlite_mount_each_per_item_rows() {
    let (db, _dbdir) = temp_db().await;
    let (app, _appdir) = build_app(&db).await;

    // The canonical source→target shape: mount the table foreground, then declare
    // one row per item from `mount_each` sub-components (mirrors Python's
    // `await coco.mount_each(process, items, target)`).
    app.run(|ctx| async move {
        let db = ctx.get_key(&DB)?;
        let table = sqlite::mount_table_target(&ctx, db, "items", item_schema()).await?;
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

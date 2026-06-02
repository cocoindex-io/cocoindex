//! Integration test for the native LanceDB target connector.
//!
//! Fully hermetic: it runs a real CocoIndex pipeline into a LanceDB database in a
//! temp directory (no external services), then reads it back to assert the
//! managed-target reconcile semantics (create, upsert, skip-unchanged, search,
//! delete-orphan).
//!
//!   cargo test -p cocoindex --features lancedb --test lancedb_target
#![cfg(feature = "lancedb")]

use cocoindex::lancedb::{self, ColumnDef, ColumnType, LanceDatabase, TableSchema};
use cocoindex::{App, ContextKey, ManagedTargetOptions, Result};
use serde::Serialize;
use std::sync::LazyLock;

static DB: LazyLock<ContextKey<LanceDatabase>> = LazyLock::new(|| {
    ContextKey::new_with_state("lancedb_test", |db: &LanceDatabase| {
        db.state_id().to_string()
    })
});

const TABLE: &str = "docs";

#[derive(Clone, Serialize)]
struct Row {
    id: i64,
    text: String,
    embedding: Vec<f32>,
}

#[derive(Clone, Serialize)]
struct RowV2 {
    id: i64,
    text: String,
    summary: String,
    embedding: Vec<f32>,
}

fn schema() -> TableSchema {
    TableSchema::new(
        [
            ("id", ColumnDef::new(ColumnType::Int64)),
            ("text", ColumnDef::new(ColumnType::Text)),
            ("embedding", ColumnDef::new(ColumnType::Vector(3))),
        ],
        ["id"],
    )
    .unwrap()
}

fn schema_v2() -> TableSchema {
    TableSchema::new(
        [
            ("id", ColumnDef::new(ColumnType::Int64)),
            ("text", ColumnDef::new(ColumnType::Text)),
            ("summary", ColumnDef::new(ColumnType::Text).nullable()),
            ("embedding", ColumnDef::new(ColumnType::Vector(3))),
        ],
        ["id"],
    )
    .unwrap()
}

async fn row_count(db: &LanceDatabase) -> usize {
    db.connection()
        .open_table(TABLE)
        .execute()
        .await
        .unwrap()
        .count_rows(None)
        .await
        .unwrap()
}

async fn table_exists(db: &LanceDatabase, table_name: &str) -> bool {
    db.connection()
        .table_names()
        .execute()
        .await
        .unwrap()
        .iter()
        .any(|name| name == table_name)
}

#[tokio::test]
async fn lancedb_target_creates_upserts_searches_and_reconciles() -> Result<()> {
    let tempdir = tempfile::tempdir().unwrap();
    let uri = tempdir.path().join("lancedb_data");
    let db = LanceDatabase::connect(uri.to_str().unwrap()).await?;
    let coco_db_path = tempdir.path().join(".cocoindex_db");

    // Build + run a pipeline declaring the given rows. coco_db_path persists
    // across runs so reconciliation sees prior tracking records.
    let run = |rows: Vec<Row>| {
        let db = db.clone();
        let coco_db_path = coco_db_path.clone();
        async move {
            let app = App::builder("LanceTargetTest")
                .db_path(&coco_db_path)
                .provide_key(&DB, db)
                .build()
                .await
                .unwrap();
            app.run(move |ctx| {
                let rows = rows.clone();
                async move {
                    let db = ctx.get_key(&DB)?;
                    let table = lancedb::mount_table_target(&ctx, db, TABLE, schema())?;
                    for row in &rows {
                        table.declare_row(&ctx, row)?;
                    }
                    Ok(())
                }
            })
            .await
            .unwrap();
        }
    };

    let mk = |id: i64, text: &str, emb: [f32; 3]| Row {
        id,
        text: text.to_string(),
        embedding: emb.to_vec(),
    };
    let mk_v2 = |id: i64, text: &str, summary: &str, emb: [f32; 3]| RowV2 {
        id,
        text: text.to_string(),
        summary: summary.to_string(),
        embedding: emb.to_vec(),
    };

    // --- Run 1: create table + insert 3 rows ---
    run(vec![
        mk(1, "alpha", [1.0, 0.0, 0.0]),
        mk(2, "beta", [0.0, 1.0, 0.0]),
        mk(3, "gamma", [0.0, 0.0, 1.0]),
    ])
    .await;
    assert_eq!(row_count(&db).await, 3, "three rows inserted");

    // --- Vector search returns the nearest row ---
    let hits = lancedb::vector_search(&db, TABLE, "embedding", vec![0.0, 0.9, 0.1], 1).await?;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0]["text"], "beta", "nearest vector is beta");
    assert!(hits[0].contains_key("_distance"));

    // --- Run 2: unchanged → still 3 rows (no duplication) ---
    run(vec![
        mk(1, "alpha", [1.0, 0.0, 0.0]),
        mk(2, "beta", [0.0, 1.0, 0.0]),
        mk(3, "gamma", [0.0, 0.0, 1.0]),
    ])
    .await;
    assert_eq!(
        row_count(&db).await,
        3,
        "unchanged re-run does not duplicate"
    );

    // --- Run 3: change row 1's text → upsert updates in place (still 3 rows) ---
    run(vec![
        mk(1, "alpha-updated", [1.0, 0.0, 0.0]),
        mk(2, "beta", [0.0, 1.0, 0.0]),
        mk(3, "gamma", [0.0, 0.0, 1.0]),
    ])
    .await;
    assert_eq!(row_count(&db).await, 3, "update is in-place, not an insert");
    let hit = lancedb::vector_search(&db, TABLE, "embedding", vec![1.0, 0.0, 0.0], 1).await?;
    assert_eq!(hit[0]["text"], "alpha-updated", "row 1 text was updated");

    // --- Run 4: drop row 3 → orphan reconciled away (2 rows) ---
    run(vec![
        mk(1, "alpha-updated", [1.0, 0.0, 0.0]),
        mk(2, "beta", [0.0, 1.0, 0.0]),
    ])
    .await;
    assert_eq!(row_count(&db).await, 2, "removed row is deleted");

    // --- Run 5: add a scalar column → table is evolved, not rebuilt ---
    let db_for_v2 = db.clone();
    let coco_db_path_for_v2 = coco_db_path.clone();
    let rows_v2 = vec![
        mk_v2(1, "alpha-updated", "first", [1.0, 0.0, 0.0]),
        mk_v2(2, "beta", "second", [0.0, 1.0, 0.0]),
    ];
    let app = App::builder("LanceTargetTest")
        .db_path(&coco_db_path_for_v2)
        .provide_key(&DB, db_for_v2)
        .build()
        .await
        .unwrap();
    app.run(move |ctx| {
        let rows = rows_v2.clone();
        async move {
            let db = ctx.get_key(&DB)?;
            let table = lancedb::mount_table_target(&ctx, db, TABLE, schema_v2())?;
            for row in &rows {
                table.declare_row(&ctx, row)?;
            }
            Ok(())
        }
    })
    .await
    .unwrap();
    assert_eq!(
        row_count(&db).await,
        2,
        "schema evolution preserves existing rows"
    );
    let hit = lancedb::vector_search(&db, TABLE, "embedding", vec![0.0, 1.0, 0.0], 1).await?;
    assert_eq!(hit[0]["summary"], "second");

    Ok(())
}

#[tokio::test]
async fn lancedb_user_managed_target_does_not_create_table() -> Result<()> {
    let tempdir = tempfile::tempdir().unwrap();
    let uri = tempdir.path().join("lancedb_data");
    let db = LanceDatabase::connect(uri.to_str().unwrap()).await?;
    let app = App::builder("LanceUserManagedTargetTest")
        .db_path(tempdir.path().join(".cocoindex_db"))
        .provide_key(&DB, db.clone())
        .build()
        .await?;

    app.run(move |ctx| async move {
        let db = ctx.get_key(&DB)?;
        let _table = lancedb::mount_table_target_with_options(
            &ctx,
            db,
            TABLE,
            schema(),
            ManagedTargetOptions::user_managed(),
        )?;
        Ok(())
    })
    .await?;

    assert!(
        !table_exists(&db, TABLE).await,
        "user-managed LanceDB target should not create the table"
    );
    Ok(())
}

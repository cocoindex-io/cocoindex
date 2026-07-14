//! Integration test for the native LanceDB target connector.
//!
//! Fully hermetic: it runs a real CocoIndex pipeline into a LanceDB database in a
//! temp directory (no external services), then reads it back to assert the
//! managed-target reconcile semantics (create, upsert, skip-unchanged, search,
//! delete-orphan).
//!
//!   cargo test -p cocoindex --features lancedb --test lancedb_target
#![cfg(feature = "lancedb")]

use cocoindex::connectors::lancedb::{self, ColumnDef, ColumnType, LanceDatabase, TableSchema};
use cocoindex::{ContextKey, Environment, ManagedTargetOptions, Result};
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

#[derive(Clone, Serialize)]
struct RowV2Nullable {
    id: i64,
    text: String,
    summary: Option<String>,
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

fn schema_dim4() -> TableSchema {
    TableSchema::new(
        [
            ("id", ColumnDef::new(ColumnType::Int64)),
            ("text", ColumnDef::new(ColumnType::Text)),
            ("embedding", ColumnDef::new(ColumnType::Vector(4))),
        ],
        ["id"],
    )
    .unwrap()
}

fn schema_nullable_text() -> TableSchema {
    TableSchema::new(
        [
            ("id", ColumnDef::new(ColumnType::Int64)),
            ("text", ColumnDef::new(ColumnType::Text).nullable()),
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

async fn table_version(db: &LanceDatabase) -> u64 {
    db.connection()
        .open_table(TABLE)
        .execute()
        .await
        .unwrap()
        .version()
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
            let app = Environment::builder()
                .db_path(&coco_db_path)
                .provide_key(&DB, db)
                .build()
                .await
                .unwrap()
                .app("LanceTargetTest")
                .await
                .unwrap();
            app.run(move |ctx| {
                let rows = rows.clone();
                async move {
                    let table = lancedb::mount_table_target(&ctx, &DB, TABLE, schema()).await?;
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
    let app = Environment::builder()
        .db_path(&coco_db_path_for_v2)
        .provide_key(&DB, db_for_v2)
        .build()
        .await
        .unwrap()
        .app("LanceTargetTest")
        .await
        .unwrap();
    app.run(move |ctx| {
        let rows = rows_v2.clone();
        async move {
            let table = lancedb::mount_table_target(&ctx, &DB, TABLE, schema_v2()).await?;
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
async fn lancedb_nullable_schema_only_add_does_not_upsert_rows() -> Result<()> {
    let tempdir = tempfile::tempdir().unwrap();
    let uri = tempdir.path().join("lancedb_data");
    let db = LanceDatabase::connect(uri.to_str().unwrap()).await?;
    let coco_db_path = tempdir.path().join(".cocoindex_db");

    {
        let app = Environment::builder()
            .db_path(&coco_db_path)
            .provide_key(&DB, db.clone())
            .build()
            .await?
            .app("LanceNullableSchemaOnlyTest")
            .await?;
        app.run(move |ctx| async move {
            let table = lancedb::mount_table_target(&ctx, &DB, TABLE, schema()).await?;
            for row in [
                Row {
                    id: 1,
                    text: "alpha".to_string(),
                    embedding: vec![1.0, 0.0, 0.0],
                },
                Row {
                    id: 2,
                    text: "beta".to_string(),
                    embedding: vec![0.0, 1.0, 0.0],
                },
            ] {
                table.declare_row(&ctx, &row)?;
            }
            Ok(())
        })
        .await?;
    }

    let initial_version = table_version(&db).await;

    {
        let app = Environment::builder()
            .db_path(&coco_db_path)
            .provide_key(&DB, db.clone())
            .build()
            .await?
            .app("LanceNullableSchemaOnlyTest")
            .await?;
        app.run(move |ctx| async move {
            let table = lancedb::mount_table_target(&ctx, &DB, TABLE, schema_v2()).await?;
            for row in [
                RowV2Nullable {
                    id: 1,
                    text: "alpha".to_string(),
                    summary: None,
                    embedding: vec![1.0, 0.0, 0.0],
                },
                RowV2Nullable {
                    id: 2,
                    text: "beta".to_string(),
                    summary: None,
                    embedding: vec![0.0, 1.0, 0.0],
                },
            ] {
                table.declare_row(&ctx, &row)?;
            }
            Ok(())
        })
        .await?;
    }

    let table = db.connection().open_table(TABLE).execute().await.unwrap();
    assert!(
        table
            .schema()
            .await
            .unwrap()
            .field_with_name("summary")
            .is_ok(),
        "nullable column was added"
    );
    assert_eq!(row_count(&db).await, 2);

    let schema_only_version = table_version(&db).await;
    assert_eq!(
        schema_only_version,
        initial_version + 1,
        "schema-only change should only commit the add_columns version"
    );

    {
        let app = Environment::builder()
            .db_path(&coco_db_path)
            .provide_key(&DB, db.clone())
            .build()
            .await?
            .app("LanceNullableSchemaOnlyTest")
            .await?;
        app.run(move |ctx| async move {
            let table = lancedb::mount_table_target(&ctx, &DB, TABLE, schema_v2()).await?;
            for row in [
                RowV2Nullable {
                    id: 1,
                    text: "alpha".to_string(),
                    summary: None,
                    embedding: vec![1.0, 0.0, 0.0],
                },
                RowV2Nullable {
                    id: 2,
                    text: "beta".to_string(),
                    summary: None,
                    embedding: vec![0.0, 1.0, 0.0],
                },
            ] {
                table.declare_row(&ctx, &row)?;
            }
            Ok(())
        })
        .await?;
    }
    assert_eq!(table_version(&db).await, schema_only_version);

    Ok(())
}

#[tokio::test]
async fn lancedb_replays_unchanged_rows_after_destructive_schema_change() -> Result<()> {
    let tempdir = tempfile::tempdir().unwrap();
    let uri = tempdir.path().join("lancedb_data");
    let db = LanceDatabase::connect(uri.to_str().unwrap()).await?;
    let coco_db_path = tempdir.path().join(".cocoindex_db");
    let rows = vec![Row {
        id: 1,
        text: "same".to_string(),
        embedding: vec![1.0, 0.0, 0.0],
    }];

    let run = |schema: TableSchema| {
        let db = db.clone();
        let coco_db_path = coco_db_path.clone();
        let rows = rows.clone();
        async move {
            let app = Environment::builder()
                .db_path(&coco_db_path)
                .provide_key(&DB, db)
                .build()
                .await?
                .app("LanceTargetReplayRowsTest")
                .await?;
            app.run(move |ctx| {
                let rows = rows.clone();
                let schema = schema.clone();
                async move {
                    let table = lancedb::mount_table_target(&ctx, &DB, TABLE, schema).await?;
                    for row in &rows {
                        table.declare_row(&ctx, row)?;
                    }
                    Ok(())
                }
            })
            .await
        }
    };

    run(schema()).await?;
    assert_eq!(row_count(&db).await, 1);

    run(schema_nullable_text()).await?;
    assert_eq!(
        row_count(&db).await,
        1,
        "destructive table replacement must replay unchanged child rows"
    );
    let hits = lancedb::vector_search(&db, TABLE, "embedding", vec![1.0, 0.0, 0.0], 1).await?;
    assert_eq!(hits[0]["text"], "same");
    Ok(())
}

#[tokio::test]
async fn lancedb_replaces_table_on_destructive_schema_change() -> Result<()> {
    let tempdir = tempfile::tempdir().unwrap();
    let uri = tempdir.path().join("lancedb_data");
    let db = LanceDatabase::connect(uri.to_str().unwrap()).await?;
    let coco_db_path = tempdir.path().join(".cocoindex_db");

    let run = |schema: TableSchema, rows: Vec<Row>| {
        let db = db.clone();
        let coco_db_path = coco_db_path.clone();
        async move {
            let app = Environment::builder()
                .db_path(&coco_db_path)
                .provide_key(&DB, db)
                .build()
                .await?
                .app("LanceTargetReplaceTest")
                .await?;
            app.run(move |ctx| {
                let rows = rows.clone();
                let schema = schema.clone();
                async move {
                    let table = lancedb::mount_table_target(&ctx, &DB, TABLE, schema).await?;
                    for row in &rows {
                        table.declare_row(&ctx, row)?;
                    }
                    Ok(())
                }
            })
            .await
        }
    };

    run(
        schema(),
        vec![
            Row {
                id: 1,
                text: "old".to_string(),
                embedding: vec![1.0, 0.0, 0.0],
            },
            Row {
                id: 2,
                text: "removed".to_string(),
                embedding: vec![0.0, 1.0, 0.0],
            },
        ],
    )
    .await?;
    assert_eq!(row_count(&db).await, 2);

    run(
        schema_dim4(),
        vec![Row {
            id: 1,
            text: "new".to_string(),
            embedding: vec![1.0, 0.0, 0.0, 0.0],
        }],
    )
    .await?;

    assert_eq!(
        row_count(&db).await,
        1,
        "destructive table replacement should rebuild only declared rows"
    );
    let hits = lancedb::vector_search(&db, TABLE, "embedding", vec![1.0, 0.0, 0.0, 0.0], 1).await?;
    assert_eq!(hits[0]["text"], "new");
    Ok(())
}

#[tokio::test]
async fn lancedb_user_managed_target_does_not_create_table() -> Result<()> {
    let tempdir = tempfile::tempdir().unwrap();
    let uri = tempdir.path().join("lancedb_data");
    let db = LanceDatabase::connect(uri.to_str().unwrap()).await?;
    let app = Environment::builder()
        .db_path(tempdir.path().join(".cocoindex_db"))
        .provide_key(&DB, db.clone())
        .build()
        .await?
        .app("LanceUserManagedTargetTest")
        .await?;

    app.run(move |ctx| async move {
        let _table = lancedb::mount_table_target_with_options(
            &ctx,
            &DB,
            TABLE,
            schema(),
            ManagedTargetOptions::user_managed(),
        )
        .await?;
        Ok(())
    })
    .await?;

    assert!(
        !table_exists(&db, TABLE).await,
        "user-managed LanceDB target should not create the table"
    );
    Ok(())
}

#[derive(Clone, Serialize)]
struct RowTwoVec {
    id: i64,
    text: String,
    embedding: Vec<f32>,
    embedding2: Vec<f32>,
}

fn schema_two_vec() -> TableSchema {
    TableSchema::new(
        [
            ("id", ColumnDef::new(ColumnType::Int64)),
            ("text", ColumnDef::new(ColumnType::Text)),
            ("embedding", ColumnDef::new(ColumnType::Vector(3))),
            (
                "embedding2",
                ColumnDef::new(ColumnType::Vector(2)).nullable(),
            ),
        ],
        ["id"],
    )
    .unwrap()
}

/// Adding a new vector column must evolve the table additively (via `AllNulls`),
/// not trigger a destructive rebuild — and the rows + new vector column work.
#[tokio::test]
async fn lancedb_adds_vector_column_additively() -> Result<()> {
    let tempdir = tempfile::tempdir().unwrap();
    let uri = tempdir.path().join("lancedb_data");
    let db = LanceDatabase::connect(uri.to_str().unwrap()).await?;
    let coco_db_path = tempdir.path().join(".cocoindex_db");

    // v1: single vector column.
    {
        let app = Environment::builder()
            .db_path(&coco_db_path)
            .provide_key(&DB, db.clone())
            .build()
            .await?
            .app("LanceAddVecTest")
            .await?;
        app.run(move |ctx| async move {
            let table = lancedb::mount_table_target(&ctx, &DB, TABLE, schema()).await?;
            table.declare_row(
                &ctx,
                &Row {
                    id: 1,
                    text: "a".into(),
                    embedding: vec![1.0, 0.0, 0.0],
                },
            )?;
            table.declare_row(
                &ctx,
                &Row {
                    id: 2,
                    text: "b".into(),
                    embedding: vec![0.0, 1.0, 0.0],
                },
            )?;
            Ok(())
        })
        .await?;
    }
    assert_eq!(row_count(&db).await, 2);

    // v2: add a second (nullable) vector column. Additive evolution.
    {
        let app = Environment::builder()
            .db_path(&coco_db_path)
            .provide_key(&DB, db.clone())
            .build()
            .await?
            .app("LanceAddVecTest")
            .await?;
        app.run(move |ctx| async move {
            let table = lancedb::mount_table_target(&ctx, &DB, TABLE, schema_two_vec()).await?;
            table.declare_row(
                &ctx,
                &RowTwoVec {
                    id: 1,
                    text: "a".into(),
                    embedding: vec![1.0, 0.0, 0.0],
                    embedding2: vec![0.5, 0.5],
                },
            )?;
            table.declare_row(
                &ctx,
                &RowTwoVec {
                    id: 2,
                    text: "b".into(),
                    embedding: vec![0.0, 1.0, 0.0],
                    embedding2: vec![0.1, 0.9],
                },
            )?;
            Ok(())
        })
        .await?;
    }
    assert_eq!(row_count(&db).await, 2, "vector-column add preserves rows");
    // Both the original and the new vector column are searchable.
    let h1 = lancedb::vector_search(&db, TABLE, "embedding", vec![1.0, 0.0, 0.0], 1).await?;
    assert_eq!(h1[0]["text"], "a");
    let h2 = lancedb::vector_search(&db, TABLE, "embedding2", vec![0.1, 0.9], 1).await?;
    assert_eq!(h2[0]["text"], "b", "the added vector column is queryable");
    Ok(())
}

async fn index_names(db: &LanceDatabase, table_name: &str) -> Vec<String> {
    db.connection()
        .open_table(table_name)
        .execute()
        .await
        .unwrap()
        .list_indices()
        .await
        .unwrap()
        .into_iter()
        .map(|i| i.name)
        .collect()
}

/// An FTS index attachment is created when declared and dropped when the
/// declaration is removed (the attachment reconcile create/delete path). FTS is
/// used because, unlike PQ vector indexes, it needs no training data and so is
/// reliable on a tiny hermetic table.
#[tokio::test]
async fn lancedb_fts_index_created_then_dropped() -> Result<()> {
    let tempdir = tempfile::tempdir().unwrap();
    let uri = tempdir.path().join("lancedb_data");
    let db = LanceDatabase::connect(uri.to_str().unwrap()).await?;
    let coco_db_path = tempdir.path().join(".cocoindex_db");

    let run = |declare_index: bool| {
        let db = db.clone();
        let coco_db_path = coco_db_path.clone();
        async move {
            let app = Environment::builder()
                .db_path(&coco_db_path)
                .provide_key(&DB, db)
                .build()
                .await
                .unwrap()
                .app("LanceFtsTest")
                .await
                .unwrap();
            app.run(move |ctx| async move {
                let table = lancedb::mount_table_target(&ctx, &DB, TABLE, schema()).await?;
                for id in 0..5i64 {
                    table.declare_row(
                        &ctx,
                        &Row {
                            id,
                            text: format!("document number {id}"),
                            embedding: vec![0.0, 0.0, 0.0],
                        },
                    )?;
                }
                if declare_index {
                    table.declare_fts_index(&ctx, "text", lancedb::FtsIndexOptions::default())?;
                }
                Ok(())
            })
            .await
            .unwrap();
        }
    };

    // Run 1: declare the FTS index → it is created.
    run(true).await;
    let names = index_names(&db, TABLE).await;
    assert!(
        names.iter().any(|n| n == "text_fts_idx"),
        "fts index created, got {names:?}"
    );

    // Run 2: stop declaring it → the orphaned attachment is dropped.
    run(false).await;
    let names = index_names(&db, TABLE).await;
    assert!(
        !names.iter().any(|n| n == "text_fts_idx"),
        "fts index dropped, got {names:?}"
    );
    Ok(())
}

/// A user-managed FTS index must NOT be dropped by CocoIndex when it stops being
/// declared — CocoIndex doesn't own user-managed DDL. Regression test for the
/// drop path not carrying `managed_by`.
#[tokio::test]
async fn lancedb_user_managed_fts_index_not_dropped_on_undeclare() -> Result<()> {
    let tempdir = tempfile::tempdir().unwrap();
    let uri = tempdir.path().join("lancedb_data");
    let db = LanceDatabase::connect(uri.to_str().unwrap()).await?;

    // Stage 1: a system-managed run physically creates the table + FTS index.
    {
        let coco = tempdir.path().join(".coco_system");
        let app = Environment::builder()
            .db_path(&coco)
            .provide_key(&DB, db.clone())
            .build()
            .await?
            .app("LanceSysSetup")
            .await?;
        app.run(move |ctx| async move {
            let table = lancedb::mount_table_target(&ctx, &DB, TABLE, schema()).await?;
            table.declare_row(
                &ctx,
                &Row {
                    id: 1,
                    text: "hello world".to_string(),
                    embedding: vec![0.0, 0.0, 0.0],
                },
            )?;
            table.declare_fts_index(&ctx, "text", lancedb::FtsIndexOptions::default())?;
            Ok(())
        })
        .await?;
    }
    assert!(
        index_names(&db, TABLE)
            .await
            .iter()
            .any(|n| n == "text_fts_idx"),
        "system setup should have created the index"
    );

    // Stage 2: a *user-managed* CocoIndex deployment (separate tracking db) adopts
    // the existing table + index, then stops declaring the index. The index must
    // survive — CocoIndex doesn't own it.
    let coco_user = tempdir.path().join(".coco_user");
    let run_user = |declare_index: bool| {
        let db = db.clone();
        let coco_user = coco_user.clone();
        async move {
            let app = Environment::builder()
                .db_path(&coco_user)
                .provide_key(&DB, db)
                .build()
                .await
                .unwrap()
                .app("LanceUserManaged")
                .await
                .unwrap();
            app.run(move |ctx| async move {
                let table = lancedb::mount_table_target_with_options(
                    &ctx,
                    &DB,
                    TABLE,
                    schema(),
                    ManagedTargetOptions::user_managed(),
                )
                .await?;
                if declare_index {
                    table.declare_fts_index(&ctx, "text", lancedb::FtsIndexOptions::default())?;
                }
                Ok(())
            })
            .await
            .unwrap();
        }
    };

    run_user(true).await; // adopt the index as user-managed
    run_user(false).await; // stop declaring it
    assert!(
        index_names(&db, TABLE)
            .await
            .iter()
            .any(|n| n == "text_fts_idx"),
        "user-managed FTS index must NOT be dropped on undeclare, got {:?}",
        index_names(&db, TABLE).await
    );
    Ok(())
}

/// An FTS index can be created with an explicit stemming language (parity with
/// the Python connector's `language` option).
#[tokio::test]
async fn lancedb_fts_index_with_language() -> Result<()> {
    let tempdir = tempfile::tempdir().unwrap();
    let uri = tempdir.path().join("lancedb_data");
    let db = LanceDatabase::connect(uri.to_str().unwrap()).await?;
    let app = Environment::builder()
        .db_path(tempdir.path().join(".cocoindex_db"))
        .provide_key(&DB, db.clone())
        .build()
        .await?
        .app("LanceFtsLangTest")
        .await?;
    app.run(move |ctx| async move {
        let table = lancedb::mount_table_target(&ctx, &DB, TABLE, schema()).await?;
        table.declare_row(
            &ctx,
            &Row {
                id: 1,
                text: "running runs runner".to_string(),
                embedding: vec![0.0, 0.0, 0.0],
            },
        )?;
        table.declare_fts_index(
            &ctx,
            "text",
            lancedb::FtsIndexOptions {
                language: Some("English".to_string()),
                ..Default::default()
            },
        )?;
        Ok(())
    })
    .await?;
    assert!(
        index_names(&db, TABLE)
            .await
            .iter()
            .any(|n| n == "text_fts_idx"),
        "fts index with a language should be created"
    );
    Ok(())
}

/// A destructive table replace (dim-3 → dim-4 vector column) drops and recreates
/// the underlying table, destroying its indices. A still-declared FTS index must
/// be rebuilt on the new table — not silently lost. Regression test for the
/// index-attachment-on-the-wrong-provider bug.
#[tokio::test]
async fn lancedb_fts_index_survives_destructive_table_replace() -> Result<()> {
    let tempdir = tempfile::tempdir().unwrap();
    let uri = tempdir.path().join("lancedb_data");
    let db = LanceDatabase::connect(uri.to_str().unwrap()).await?;
    let coco_db_path = tempdir.path().join(".cocoindex_db");

    let run = |schema: TableSchema, embedding: Vec<f32>| {
        let db = db.clone();
        let coco_db_path = coco_db_path.clone();
        async move {
            let app = Environment::builder()
                .db_path(&coco_db_path)
                .provide_key(&DB, db)
                .build()
                .await
                .unwrap()
                .app("LanceFtsReplaceTest")
                .await
                .unwrap();
            app.run(move |ctx| {
                let embedding = embedding.clone();
                async move {
                    let table = lancedb::mount_table_target(&ctx, &DB, TABLE, schema).await?;
                    table.declare_row(
                        &ctx,
                        &Row {
                            id: 1,
                            text: "hello world".to_string(),
                            embedding,
                        },
                    )?;
                    table.declare_fts_index(&ctx, "text", lancedb::FtsIndexOptions::default())?;
                    Ok(())
                }
            })
            .await
            .unwrap();
        }
    };

    // Run 1: dim-3 schema + FTS index → created.
    run(schema(), vec![1.0, 0.0, 0.0]).await;
    assert!(
        index_names(&db, TABLE)
            .await
            .iter()
            .any(|n| n == "text_fts_idx"),
        "fts index should exist after the first run"
    );

    // Run 2: dim-4 schema forces a destructive table replace; index still declared.
    run(schema_dim4(), vec![1.0, 0.0, 0.0, 0.0]).await;
    let names = index_names(&db, TABLE).await;
    assert!(
        names.iter().any(|n| n == "text_fts_idx"),
        "fts index must be rebuilt after a destructive table replace, got {names:?}"
    );
    Ok(())
}

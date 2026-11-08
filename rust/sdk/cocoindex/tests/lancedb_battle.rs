//! Edge and failure cases of the LanceDB target: key forms the delete filter
//! must render, vector values that do not fit their column, batches around and
//! far beyond the sizes a run writes at once, a run that fails part way and the
//! next one that succeeds, and index definitions that change between runs.
//!
//! Hermetic (a LanceDB database in a temp directory):
//!
//!   cargo test -p cocoindex --features lancedb,testing --test lancedb_battle
#![cfg(feature = "lancedb")]

use std::sync::Arc;

use ::lancedb::query::ExecutableQuery;
use arrow_array::{Array, RecordBatch};
use cocoindex::connectors::lancedb::{
    self, ColumnDef, ColumnType, FtsIndexOptions, LanceDatabase, TableSchema, VectorIndexOptions,
};
use cocoindex::{App, Environment, Result};
use futures::TryStreamExt;
use serde_json::{Value, json};

cocoindex::context_key!(
    static DB: LanceDatabase, key = "lancedb_battle"
);

const TABLE: &str = "docs";

struct Fixture {
    _dir: tempfile::TempDir,
    db: LanceDatabase,
    app: App,
}

async fn fixture(name: &str) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let db = LanceDatabase::connect(dir.path().join("lancedb").to_str().unwrap())
        .await
        .unwrap();
    let app = Environment::builder()
        .db_path(dir.path().join(".cocoindex_db"))
        .provide_key(&DB, db.clone())
        .build()
        .await
        .unwrap()
        .app(name)
        .await
        .unwrap();
    Fixture { _dir: dir, db, app }
}

/// Run the app once, declaring `rows` (JSON objects) in a table of `schema`.
async fn run_rows(app: &App, schema: &TableSchema, rows: &[Value]) -> Result<()> {
    let schema = schema.clone();
    let rows = Arc::new(rows.to_vec());
    app.run(move |ctx| {
        let schema = schema.clone();
        let rows = rows.clone();
        async move {
            let table = lancedb::mount_table_target(&ctx, &DB, TABLE, schema).await?;
            for row in rows.iter() {
                table.declare_row(&ctx, row)?;
            }
            Ok(())
        }
    })
    .await
    .map(|_| ())
}

async fn read_all(db: &LanceDatabase) -> Vec<RecordBatch> {
    let table = db.connection().open_table(TABLE).execute().await.unwrap();
    table
        .query()
        .execute()
        .await
        .unwrap()
        .try_collect()
        .await
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

/// The values of column `col` across all rows, as strings, sorted.
async fn column_strings(db: &LanceDatabase, col: &str) -> Vec<String> {
    let mut out = Vec::new();
    for batch in read_all(db).await {
        let array = batch.column_by_name(col).unwrap();
        for i in 0..array.len() {
            out.push(arrow_value_string(array.as_ref(), i));
        }
    }
    out.sort();
    out
}

fn arrow_value_string(array: &dyn Array, i: usize) -> String {
    use arrow_array::cast::AsArray;
    use arrow_array::types::*;
    use arrow_schema::DataType;
    if array.is_null(i) {
        return "null".to_string();
    }
    match array.data_type() {
        DataType::Utf8 => array.as_string::<i32>().value(i).to_string(),
        DataType::Int64 => array.as_primitive::<Int64Type>().value(i).to_string(),
        DataType::Int32 => array.as_primitive::<Int32Type>().value(i).to_string(),
        DataType::UInt64 => array.as_primitive::<UInt64Type>().value(i).to_string(),
        other => panic!("unexpected column type {other:?}"),
    }
}

fn text_schema(pk: &str) -> TableSchema {
    TableSchema::new(
        [
            (pk, ColumnDef::new(ColumnType::Text)),
            ("body", ColumnDef::new(ColumnType::Text)),
        ],
        [pk],
    )
    .unwrap()
}

fn int_schema(pk: &str) -> TableSchema {
    TableSchema::new(
        [
            (pk, ColumnDef::new(ColumnType::Int64)),
            ("body", ColumnDef::new(ColumnType::Text)),
        ],
        [pk],
    )
    .unwrap()
}

fn vec_schema(col_type: ColumnType) -> TableSchema {
    TableSchema::new(
        [
            ("id", ColumnDef::new(ColumnType::Int64)),
            ("embedding", ColumnDef::new(col_type)),
        ],
        ["id"],
    )
    .unwrap()
}

/// String keys with characters a SQL filter must escape are inserted, updated
/// and deleted like any other key.
#[tokio::test]
async fn string_keys_with_special_characters_are_updated_and_deleted() -> Result<()> {
    let f = fixture("LanceBattleStringKeys").await;
    let schema = text_schema("id");
    let keys = [
        "plain",
        "it's",
        "''",
        "back\\slash",
        "trailing\\",
        "double\"quote",
        "new\nline",
        "tab\tchar",
        "percent%_under",
        "unicode-ключ-🔑",
        "",
        " leading space",
        "semi;colon -- comment",
        "x' OR '1'='1",
    ];
    let rows = |body: &str| -> Vec<Value> {
        keys.iter()
            .map(|k| json!({"id": k, "body": body}))
            .collect()
    };
    run_rows(&f.app, &schema, &rows("v1")).await?;
    assert_eq!(row_count(&f.db).await, keys.len());

    run_rows(&f.app, &schema, &rows("v2")).await?;
    assert_eq!(row_count(&f.db).await, keys.len(), "updates do not insert");
    assert!(
        column_strings(&f.db, "body")
            .await
            .iter()
            .all(|b| b == "v2")
    );

    run_rows(&f.app, &schema, &[json!({"id": "plain", "body": "v2"})]).await?;
    assert_eq!(
        column_strings(&f.db, "id").await,
        vec!["plain".to_string()],
        "every other key is deleted"
    );
    Ok(())
}

/// Integer keys at the ends of the i64 range are deleted.
#[tokio::test]
async fn extreme_integer_keys_are_deleted() -> Result<()> {
    let f = fixture("LanceBattleIntKeys").await;
    let schema = int_schema("id");
    let keys = [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX];
    let rows: Vec<Value> = keys.iter().map(|k| json!({"id": k, "body": "x"})).collect();
    run_rows(&f.app, &schema, &rows).await?;
    assert_eq!(row_count(&f.db).await, keys.len());
    run_rows(&f.app, &schema, &[json!({"id": 0, "body": "x"})]).await?;
    assert_eq!(column_strings(&f.db, "id").await, vec!["0".to_string()]);
    Ok(())
}

/// A key column whose name has upper-case letters is deleted by its exact name.
#[tokio::test]
async fn mixed_case_key_column_rows_are_deleted() -> Result<()> {
    let f = fixture("LanceBattleMixedCase").await;
    let schema = text_schema("docId");
    run_rows(
        &f.app,
        &schema,
        &[
            json!({"docId": "a", "body": "x"}),
            json!({"docId": "b", "body": "x"}),
        ],
    )
    .await?;
    run_rows(&f.app, &schema, &[json!({"docId": "a", "body": "x"})]).await?;
    assert_eq!(column_strings(&f.db, "docId").await, vec!["a".to_string()]);
    Ok(())
}

/// A composite key of a string and an integer: rows are matched and deleted on
/// both parts.
#[tokio::test]
async fn composite_keys_are_updated_and_deleted() -> Result<()> {
    let f = fixture("LanceBattleCompositeKey").await;
    let schema = TableSchema::new(
        [
            ("file", ColumnDef::new(ColumnType::Text)),
            ("chunk", ColumnDef::new(ColumnType::Int64)),
            ("body", ColumnDef::new(ColumnType::Text)),
        ],
        ["file", "chunk"],
    )
    .unwrap();
    let rows = |n: i64, body: &str| -> Vec<Value> {
        (0..n)
            .flat_map(|c| {
                [
                    json!({"file": "a's", "chunk": c, "body": body}),
                    json!({"file": "b", "chunk": c, "body": body}),
                ]
            })
            .collect()
    };
    run_rows(&f.app, &schema, &rows(3, "v1")).await?;
    assert_eq!(row_count(&f.db).await, 6);
    run_rows(&f.app, &schema, &rows(2, "v2")).await?;
    assert_eq!(row_count(&f.db).await, 4);
    assert!(
        column_strings(&f.db, "body")
            .await
            .iter()
            .all(|b| b == "v2")
    );
    Ok(())
}

/// A vector of the wrong length fails the run and stores nothing for it; the
/// next run, with the vector fixed, writes every row.
#[tokio::test]
async fn wrong_length_vector_fails_then_the_fixed_run_succeeds() -> Result<()> {
    let f = fixture("LanceBattleWrongLength").await;
    let schema = vec_schema(ColumnType::Vector(3));
    let mut rows: Vec<Value> = (0..20)
        .map(|i| json!({"id": i, "embedding": [i as f32, 0.0, 1.0]}))
        .collect();
    rows[7] = json!({"id": 7, "embedding": [1.0, 2.0]});
    let err = run_rows(&f.app, &schema, &rows).await.unwrap_err();
    assert!(
        err.to_string().contains("vector length 2 != schema dim 3"),
        "{err}"
    );

    rows[7] = json!({"id": 7, "embedding": [1.0, 2.0, 3.0]});
    run_rows(&f.app, &schema, &rows).await?;
    assert_eq!(row_count(&f.db).await, 20);

    // An empty vector is a wrong length too, not a null.
    rows[3] = json!({"id": 3, "embedding": []});
    let err = run_rows(&f.app, &schema, &rows).await.unwrap_err();
    assert!(err.to_string().contains("vector length 0"), "{err}");
    Ok(())
}

/// NaN and infinite elements are rejected when the row is declared, and so is
/// a finite value that overflows the column's element type, in a float32 and
/// a float16 vector alike.
#[tokio::test]
async fn non_finite_and_overflowing_vector_elements_are_rejected() -> Result<()> {
    #[derive(serde::Serialize)]
    struct F32Row {
        id: i64,
        embedding: Vec<f32>,
    }
    #[derive(serde::Serialize)]
    struct F64Row {
        id: i64,
        embedding: Vec<f64>,
    }
    let f = fixture("LanceBattleNonFinite").await;
    for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
        let schema = vec_schema(ColumnType::Vector(2));
        let err = f
            .app
            .run(move |ctx| {
                let schema = schema.clone();
                async move {
                    let table = lancedb::mount_table_target(&ctx, &DB, TABLE, schema).await?;
                    table.declare_row(
                        &ctx,
                        &F32Row {
                            id: 1,
                            embedding: vec![bad, 0.0],
                        },
                    )
                }
            })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("non-finite"), "{bad}: {err}");
    }

    // A finite f64 beyond the f32 range, and a value beyond the f16 range.
    for (col_type, value) in [
        (ColumnType::Vector(2), 1e39_f64),
        (ColumnType::HalfVector(2), 1e6_f64),
    ] {
        let schema = vec_schema(col_type.clone());
        let err = f
            .app
            .run(move |ctx| {
                let schema = schema.clone();
                async move {
                    let table = lancedb::mount_table_target(&ctx, &DB, TABLE, schema).await?;
                    table.declare_row(
                        &ctx,
                        &F64Row {
                            id: 1,
                            embedding: vec![value, 0.0],
                        },
                    )
                }
            })
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("does not fit"),
            "{col_type:?} {value}: {err}"
        );
    }
    Ok(())
}

/// The vector element type and dimension change between runs: the table is
/// rebuilt with the new column type and holds the rows of the new run.
#[tokio::test]
async fn vector_type_and_dimension_changes_rebuild_the_table() -> Result<()> {
    let f = fixture("LanceBattleVectorChange").await;
    let rows = |dim: usize| -> Vec<Value> {
        (0..5)
            .map(|i| json!({"id": i, "embedding": vec![0.5_f32; dim]}))
            .collect()
    };
    for (col_type, dim) in [
        (ColumnType::Vector(3), 3),
        (ColumnType::HalfVector(3), 3),
        (ColumnType::HalfVector(8), 8),
        (ColumnType::Vector(8), 8),
        (ColumnType::Vector(3), 3),
    ] {
        run_rows(&f.app, &vec_schema(col_type.clone()), &rows(dim)).await?;
        let table = f.db.connection().open_table(TABLE).execute().await.unwrap();
        let field_type = table
            .schema()
            .await
            .unwrap()
            .field_with_name("embedding")
            .unwrap()
            .data_type()
            .clone();
        let arrow_schema::DataType::FixedSizeList(item, n) = field_type else {
            panic!("{col_type:?}: not a fixed-size list");
        };
        assert_eq!(n as usize, dim, "{col_type:?}");
        let half = matches!(col_type, ColumnType::HalfVector(_));
        assert_eq!(
            item.data_type(),
            if half {
                &arrow_schema::DataType::Float16
            } else {
                &arrow_schema::DataType::Float32
            },
            "{col_type:?}"
        );
        assert_eq!(table.count_rows(None).await.unwrap(), 5, "{col_type:?}");
    }
    Ok(())
}

/// Every column type with values at their edges, nested JSON, binary, nulls
/// and a multi-megabyte text value round-trip.
#[tokio::test]
async fn every_column_type_and_a_huge_value_round_trip() -> Result<()> {
    let f = fixture("LanceBattleTypes").await;
    let schema = TableSchema::new(
        [
            ("id", ColumnDef::new(ColumnType::Text)),
            ("b", ColumnDef::new(ColumnType::Bool).nullable()),
            ("i8", ColumnDef::new(ColumnType::Int8).nullable()),
            ("i16", ColumnDef::new(ColumnType::Int16).nullable()),
            ("i32", ColumnDef::new(ColumnType::Int32).nullable()),
            ("i64", ColumnDef::new(ColumnType::Int64).nullable()),
            ("u8", ColumnDef::new(ColumnType::UInt8).nullable()),
            ("u16", ColumnDef::new(ColumnType::UInt16).nullable()),
            ("u32", ColumnDef::new(ColumnType::UInt32).nullable()),
            ("u64", ColumnDef::new(ColumnType::UInt64).nullable()),
            ("f16", ColumnDef::new(ColumnType::Float16).nullable()),
            ("f32", ColumnDef::new(ColumnType::Float32).nullable()),
            ("f64", ColumnDef::new(ColumnType::Float64).nullable()),
            ("bin", ColumnDef::new(ColumnType::Binary).nullable()),
            ("doc", ColumnDef::new(ColumnType::Json).nullable()),
            ("text", ColumnDef::new(ColumnType::Text).nullable()),
            ("v", ColumnDef::new(ColumnType::Vector(2)).nullable()),
        ],
        ["id"],
    )
    .unwrap();
    let huge = "x".repeat(8 << 20);
    let nested = json!({"a": [1, {"b": null, "c": "ü"}], "d": {"e": {"f": [true, 1.5]}}});
    let rows = vec![
        json!({
            "id": "max", "b": true, "i8": i8::MAX, "i16": i16::MAX, "i32": i32::MAX,
            "i64": i64::MAX, "u8": u8::MAX, "u16": u16::MAX, "u32": u32::MAX,
            "u64": u64::MAX, "f16": 65504.0, "f32": f32::MAX, "f64": f64::MAX,
            "bin": [0, 255, 1], "doc": nested, "text": huge, "v": [1.0, -1.0],
        }),
        json!({
            "id": "min", "b": false, "i8": i8::MIN, "i16": i16::MIN, "i32": i32::MIN,
            "i64": i64::MIN, "u8": 0, "u16": 0, "u32": 0, "u64": 0,
            "f16": -65504.0, "f32": f32::MIN, "f64": f64::MIN, "bin": [],
            "doc": [], "text": "", "v": [0.0, 0.0],
        }),
        json!({"id": "nulls"}),
    ];
    run_rows(&f.app, &schema, &rows).await?;
    assert_eq!(row_count(&f.db).await, 3);

    let batches = read_all(&f.db).await;
    let mut found = 0;
    for batch in &batches {
        use arrow_array::cast::AsArray;
        let ids = batch.column_by_name("id").unwrap().as_string::<i32>();
        for i in 0..batch.num_rows() {
            let text = batch.column_by_name("text").unwrap();
            let doc = batch.column_by_name("doc").unwrap();
            let u64s = batch
                .column_by_name("u64")
                .unwrap()
                .as_primitive::<arrow_array::types::UInt64Type>();
            match ids.value(i) {
                "max" => {
                    assert_eq!(text.as_string::<i32>().value(i).len(), huge.len());
                    let stored: Value =
                        serde_json::from_str(doc.as_string::<i32>().value(i)).unwrap();
                    assert_eq!(stored, nested);
                    assert_eq!(u64s.value(i), u64::MAX);
                }
                "min" => assert_eq!(text.as_string::<i32>().value(i), ""),
                "nulls" => {
                    for col in batch.schema().fields() {
                        if col.name() != "id" {
                            assert!(batch.column_by_name(col.name()).unwrap().is_null(i));
                        }
                    }
                }
                other => panic!("unexpected row {other}"),
            }
            found += 1;
        }
    }
    assert_eq!(found, 3);

    // A value that does not fit its column fails the run instead of storing a
    // null or a truncated value.
    for (col, value) in [
        ("i8", json!(128)),
        ("u8", json!(-1)),
        ("u64", json!(-1)),
        ("f16", json!(1e6)),
        ("f32", json!(1e39)),
        ("i64", json!("12")),
        ("bin", json!([256])),
        ("b", json!(1)),
    ] {
        let mut row = json!({"id": "bad"});
        row[col] = value.clone();
        let err = run_rows(&f.app, &schema, &[row]).await.unwrap_err();
        assert!(
            err.to_string().contains(&format!("{col:?}")),
            "{col}={value}: {err}"
        );
    }
    Ok(())
}

/// Row counts around the sizes a sink writes at once, inserted then all
/// deleted, in one app so each count reconciles against the previous one.
#[tokio::test]
async fn batches_around_the_chunk_boundaries() -> Result<()> {
    let f = fixture("LanceBattleBatches").await;
    let schema = int_schema("id");
    let rows = |n: i64, body: &str| -> Vec<Value> {
        (0..n).map(|i| json!({"id": i, "body": body})).collect()
    };
    for n in [499, 500, 501, 999, 1000, 1001, 2500] {
        run_rows(&f.app, &schema, &rows(n, &format!("v{n}"))).await?;
        assert_eq!(row_count(&f.db).await, n as usize, "{n} rows");
        let bodies = column_strings(&f.db, "body").await;
        assert!(bodies.iter().all(|b| *b == format!("v{n}")), "{n} rows");
    }
    run_rows(&f.app, &schema, &[]).await?;
    assert_eq!(row_count(&f.db).await, 0, "every row deleted");
    Ok(())
}

/// A large run: 100 000 rows inserted, half updated, half deleted. Slow, so
/// ignored by default:
///
///   cargo test -p cocoindex --features lancedb,testing --test lancedb_battle \
///     -- --ignored large_run
#[tokio::test]
#[ignore = "soak: 100k rows"]
async fn large_run_inserts_updates_and_deletes() -> Result<()> {
    let f = fixture("LanceBattleLarge").await;
    let schema = int_schema("id");
    const N: i64 = 100_000;
    let all: Vec<Value> = (0..N).map(|i| json!({"id": i, "body": "v1"})).collect();
    let start = std::time::Instant::now();
    run_rows(&f.app, &schema, &all).await?;
    eprintln!("insert {N}: {:?}", start.elapsed());
    assert_eq!(row_count(&f.db).await, N as usize);

    let half: Vec<Value> = (0..N / 2).map(|i| json!({"id": i, "body": "v2"})).collect();
    let start = std::time::Instant::now();
    run_rows(&f.app, &schema, &half).await?;
    eprintln!("update+delete {}: {:?}", N / 2, start.elapsed());
    assert_eq!(row_count(&f.db).await, (N / 2) as usize);
    Ok(())
}

/// A table dropped behind CocoIndex's back is recreated by the next run that
/// writes to it. Only the rows that run writes come back: like the Python
/// connector, the target trusts its tracking records and does not re-check the
/// table, so unchanged rows are not written again.
#[tokio::test]
async fn table_dropped_out_of_band_is_recreated() -> Result<()> {
    let f = fixture("LanceBattleOutOfBand").await;
    let schema = int_schema("id");
    let rows: Vec<Value> = (0..3).map(|i| json!({"id": i, "body": "x"})).collect();
    run_rows(&f.app, &schema, &rows).await?;
    f.db.connection().drop_table(TABLE, &[]).await.unwrap();

    // A run that changes a row recreates the table and writes that row.
    let mut changed = rows.clone();
    changed[0] = json!({"id": 0, "body": "y"});
    run_rows(&f.app, &schema, &changed).await?;
    assert_eq!(column_strings(&f.db, "body").await, vec!["y".to_string()]);
    Ok(())
}

async fn index_types(db: &LanceDatabase) -> Vec<(String, String)> {
    let table = db.connection().open_table(TABLE).execute().await.unwrap();
    let mut out: Vec<_> = table
        .list_indices()
        .await
        .unwrap()
        .into_iter()
        .map(|i| (i.name, format!("{:?}", i.index_type)))
        .collect();
    out.sort();
    out
}

/// Vector and FTS index definitions that change between runs are rebuilt with
/// the new definition; an index no longer declared is dropped.
#[tokio::test]
async fn index_definition_changes_rebuild_the_index() -> Result<()> {
    let f = fixture("LanceBattleIndexChange").await;
    let schema = TableSchema::new(
        [
            ("id", ColumnDef::new(ColumnType::Int64)),
            ("body", ColumnDef::new(ColumnType::Text)),
            ("embedding", ColumnDef::new(ColumnType::Vector(4))),
        ],
        ["id"],
    )
    .unwrap();
    // IVF training needs a few hundred rows.
    let rows: Arc<Vec<Value>> = Arc::new(
        (0..300)
            .map(|i| {
                let x = i as f32;
                json!({
                    "id": i,
                    "body": format!("word{} common", i % 7),
                    "embedding": [x.sin(), x.cos(), (x * 0.5).sin(), 1.0],
                })
            })
            .collect(),
    );
    let run = |vector: Option<VectorIndexOptions>, fts: Option<FtsIndexOptions>| {
        let schema = schema.clone();
        let rows = rows.clone();
        f.app.run(move |ctx| {
            let schema = schema.clone();
            let rows = rows.clone();
            let vector = vector.clone();
            let fts = fts.clone();
            async move {
                let table = lancedb::mount_table_target(&ctx, &DB, TABLE, schema).await?;
                for row in rows.iter() {
                    table.declare_row(&ctx, row)?;
                }
                if let Some(options) = vector {
                    table.declare_vector_index(&ctx, "embedding", options)?;
                }
                if let Some(options) = fts {
                    table.declare_fts_index(&ctx, "body", options)?;
                }
                Ok(())
            }
        })
    };
    let ivf_pq = VectorIndexOptions {
        num_partitions: Some(2),
        num_sub_vectors: Some(2),
        ..Default::default()
    };
    run(Some(ivf_pq.clone()), Some(FtsIndexOptions::default())).await?;
    assert_eq!(
        index_types(&f.db).await,
        vec![
            ("body_fts_idx".to_string(), "FTS".to_string()),
            ("embedding_idx".to_string(), "IvfPq".to_string()),
        ]
    );

    // A changed index type and parameters, and an FTS index without positions.
    let hnsw = VectorIndexOptions {
        index_type: "ivf_hnsw_pq",
        metric: "l2",
        m: Some(8),
        ef_construction: Some(50),
        ..ivf_pq.clone()
    };
    run(
        Some(hnsw),
        Some(FtsIndexOptions {
            with_position: false,
            ..Default::default()
        }),
    )
    .await?;
    assert_eq!(
        index_types(&f.db).await,
        vec![
            ("body_fts_idx".to_string(), "FTS".to_string()),
            ("embedding_idx".to_string(), "IvfHnswPq".to_string()),
        ]
    );

    // Both indexes undeclared: both dropped, the rows stay.
    run(None, None).await?;
    assert!(index_types(&f.db).await.is_empty());
    assert_eq!(row_count(&f.db).await, 300);
    Ok(())
}

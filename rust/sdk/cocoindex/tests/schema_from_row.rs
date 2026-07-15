//! Tests for `TableSchema::from_row::<T>()` — the derive-based schema
//! construction that mirrors Python's `TableSchema.from_class`. Each connector's
//! `from_row` must produce exactly the schema a user would write by hand, using
//! that connector's leaf-type mapping.

#[cfg(any(
    feature = "doris",
    feature = "lancedb",
    feature = "postgres",
    feature = "qdrant",
    feature = "sqlite",
    feature = "turbopuffer"
))]
fn assert_unresolved_dimension_error(result: cocoindex::Result<()>, connector: &str) {
    let message = result
        .expect_err("unresolved vector schema unexpectedly reached target construction")
        .to_string();
    assert!(message.contains(connector), "{message}");
    assert!(message.contains("embedding"), "{message}");
    assert!(message.contains("with_vector_dim"), "{message}");
}

#[cfg(any(
    feature = "doris",
    feature = "lancedb",
    feature = "postgres",
    feature = "qdrant",
    feature = "sqlite",
    feature = "turbopuffer"
))]
#[derive(cocoindex::SchemaFields)]
#[allow(dead_code)]
struct UnresolvedVectorRow {
    id: i64,
    #[coco(vector)]
    embedding: Vec<f32>,
}

#[cfg(feature = "doris")]
#[test]
fn doris_from_row_matches_explicit_schema() {
    use cocoindex::SchemaFields;
    use cocoindex::connectors::doris::{ColumnDef, TableSchema};

    #[derive(SchemaFields)]
    #[allow(dead_code)]
    struct Row {
        id: String,
        name: Option<String>,
        count: i64,
        score: f64,
        active: bool,
        blob: Vec<u8>,
        elapsed: std::time::Duration,
        #[coco(vector = 4)]
        embedding: Vec<f32>,
        #[coco(json)]
        tags: Vec<String>,
        #[coco(type = "VARCHAR(10)")]
        code: String,
    }

    let unresolved = TableSchema::from_row::<Row>(["id"]).unwrap();
    assert!(unresolved.clone().with_vector_dim("embedding", 0).is_err());
    assert!(unresolved.clone().with_vector_dim("missing", 4).is_err());
    assert!(unresolved.clone().with_vector_dim("id", 4).is_err());
    let got = unresolved;
    let want = TableSchema::new(
        [
            ("id", ColumnDef::new("TEXT").not_null()),
            ("name", ColumnDef::new("TEXT")), // Option -> nullable
            ("count", ColumnDef::new("BIGINT").not_null()),
            ("score", ColumnDef::new("DOUBLE").not_null()),
            ("active", ColumnDef::new("BOOLEAN").not_null()),
            ("blob", ColumnDef::new("STRING").not_null()),
            ("elapsed", ColumnDef::new("BIGINT").not_null()),
            ("embedding", ColumnDef::vector(4)),
            ("tags", ColumnDef::new("JSON").not_null()),
            ("code", ColumnDef::new("VARCHAR(10)").not_null()),
        ],
        ["id"],
    )
    .unwrap();
    assert_eq!(got, want);
}

#[cfg(feature = "sqlite")]
#[test]
fn sqlite_from_row_matches_explicit_schema() {
    use cocoindex::SchemaFields;
    use cocoindex::connectors::sqlite::{ColumnDef, TableSchema};

    #[derive(SchemaFields)]
    #[allow(dead_code)]
    struct Row {
        id: i64,
        name: Option<String>,
        score: f64,
        blob: Vec<u8>,
        elapsed: std::time::Duration,
        #[coco(vector)]
        embedding: Vec<f32>,
    }

    let unresolved = TableSchema::from_row::<Row>(["id"]).unwrap();
    assert!(unresolved.clone().with_vector_dim("embedding", 0).is_err());
    assert!(unresolved.clone().with_vector_dim("missing", 3).is_err());
    assert!(unresolved.clone().with_vector_dim("id", 3).is_err());
    let got = unresolved.with_vector_dim("embedding", 3).unwrap();
    let want = TableSchema::new(
        [
            ("id", ColumnDef::new("INTEGER").not_null()),
            ("name", ColumnDef::new("TEXT")),
            ("score", ColumnDef::new("REAL").not_null()),
            ("blob", ColumnDef::new("BLOB").not_null()),
            ("elapsed", ColumnDef::new("REAL").not_null()),
            ("embedding", ColumnDef::new("float[3]").not_null()),
        ],
        ["id"],
    )
    .unwrap();
    assert_eq!(got, want);
}

#[cfg(feature = "sqlite")]
#[test]
fn schema_from_row_rejects_duplicate_renamed_columns() {
    use cocoindex::SchemaFields;
    use cocoindex::connectors::sqlite::TableSchema;

    #[derive(SchemaFields)]
    #[allow(dead_code)]
    struct Row {
        id: i64,
        #[coco(rename = "id")]
        other_id: i64,
    }

    let error = TableSchema::from_row::<Row>(["id"]).unwrap_err();
    assert!(error.to_string().contains("duplicate column \"id\""));
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_from_row_matches_explicit_schema() {
    use cocoindex::SchemaFields;
    use cocoindex::connectors::postgres::{ColumnDef, TableSchema};

    #[derive(SchemaFields)]
    #[allow(dead_code)]
    struct Row {
        id: i64,
        title: Option<String>,
        views: i32,
        ratio: f32,
        elapsed: std::time::Duration,
        #[coco(vector)]
        embedding: Vec<f32>,
        #[coco(vector = 8, half)]
        embedding_half: Vec<f32>,
        #[coco(json)]
        meta: Vec<String>,
    }

    let unresolved = TableSchema::from_row::<Row>(["id"]).unwrap();
    assert!(unresolved.clone().with_vector_dim("embedding", 0).is_err());
    assert!(unresolved.clone().with_vector_dim("missing", 8).is_err());
    assert!(unresolved.clone().with_vector_dim("id", 8).is_err());
    let got = unresolved.with_vector_dim("embedding", 8).unwrap();
    // Postgres ColumnDef::new is NOT NULL by default; `.nullable()` opts in.
    let want = TableSchema::new(
        [
            ("id", ColumnDef::new("bigint")),
            ("title", ColumnDef::new("text").nullable()),
            ("views", ColumnDef::new("integer")),
            ("ratio", ColumnDef::new("real")),
            ("elapsed", ColumnDef::new("interval")),
            ("embedding", ColumnDef::new("vector(8)")),
            ("embedding_half", ColumnDef::new("halfvec(8)")),
            ("meta", ColumnDef::new("jsonb")),
        ],
        ["id"],
    )
    .unwrap();
    assert_eq!(got, want);
}

#[cfg(feature = "lancedb")]
#[test]
fn lancedb_from_row_matches_explicit_schema() {
    use cocoindex::SchemaFields;
    use cocoindex::connectors::lancedb::{ColumnDef, ColumnType, TableSchema};

    #[derive(SchemaFields)]
    #[allow(dead_code)]
    struct Row {
        id: i64,
        count: i32,
        ratio: f32,
        active: bool,
        title: Option<String>,
        #[coco(json)]
        tags: Vec<String>,
        #[coco(vector)]
        embedding: Vec<f32>,
        #[coco(vector, half)]
        half_embedding: Vec<f32>,
    }

    let unresolved = TableSchema::from_row::<Row>(["id"]).unwrap();
    assert!(unresolved.clone().with_vector_dim("embedding", 0).is_err());
    assert!(unresolved.clone().with_vector_dim("missing", 4).is_err());
    assert!(unresolved.clone().with_vector_dim("id", 4).is_err());
    let got = unresolved
        .with_vector_dim("embedding", 4)
        .unwrap()
        .with_vector_dim("half_embedding", 4)
        .unwrap();
    let want = TableSchema::new(
        [
            ("id", ColumnDef::new(ColumnType::Int64)),
            ("count", ColumnDef::new(ColumnType::Int32)),
            ("ratio", ColumnDef::new(ColumnType::Float32)),
            ("active", ColumnDef::new(ColumnType::Bool)),
            ("title", ColumnDef::new(ColumnType::Text).nullable()),
            ("tags", ColumnDef::new(ColumnType::Json)),
            ("embedding", ColumnDef::new(ColumnType::Vector(4))),
            ("half_embedding", ColumnDef::new(ColumnType::HalfVector(4))),
        ],
        ["id"],
    )
    .unwrap();
    assert_eq!(got, want);

    #[derive(SchemaFields)]
    #[allow(dead_code)]
    struct Unsupported {
        id: i64,
        #[coco(type = "struct<x: int>")]
        details: String,
    }
    let err = TableSchema::from_row::<Unsupported>(["id"]).unwrap_err();
    let message = err.to_string();
    assert!(message.contains("LanceDB"), "{message}");
    assert!(message.contains("details"), "{message}");
}

#[cfg(feature = "qdrant")]
#[test]
fn qdrant_from_row_derives_named_vectors_and_runtime_dimension() {
    use cocoindex::SchemaFields;
    use cocoindex::connectors::qdrant::{CollectionSchema, Distance, QdrantVectorDef};

    #[derive(SchemaFields)]
    #[allow(dead_code)]
    struct Row {
        id: u64,
        text: String,
        #[coco(vector)]
        embedding: Vec<f32>,
    }

    let unresolved = CollectionSchema::from_row::<Row>(Distance::Cosine).unwrap();
    assert!(unresolved.clone().with_vector_dim("embedding", 0).is_err());
    assert!(unresolved.clone().with_vector_dim("missing", 3).is_err());
    let got = unresolved.with_vector_dim("embedding", 3).unwrap();
    let want = CollectionSchema::named([(
        "embedding",
        QdrantVectorDef::f32(3, Distance::Cosine).unwrap(),
    )])
    .unwrap();
    assert_eq!(got, want);
}

#[cfg(feature = "turbopuffer")]
#[test]
fn turbopuffer_from_row_derives_named_vectors_and_runtime_dimension() {
    use cocoindex::SchemaFields;
    use cocoindex::connectors::turbopuffer::{DistanceMetric, NamespaceSchema, VectorDef};

    #[derive(SchemaFields)]
    #[allow(dead_code)]
    struct Row {
        id: String,
        text: String,
        #[coco(vector)]
        embedding: Vec<f32>,
    }

    let unresolved = NamespaceSchema::from_row::<Row>(DistanceMetric::CosineDistance).unwrap();
    assert!(unresolved.clone().with_vector_dim("embedding", 0).is_err());
    assert!(unresolved.clone().with_vector_dim("missing", 3).is_err());
    let got = unresolved.with_vector_dim("embedding", 3).unwrap();
    let want = NamespaceSchema::named(
        [("embedding", VectorDef::f32(3).unwrap())],
        DistanceMetric::CosineDistance,
    )
    .unwrap();
    assert_eq!(got, want);
}

#[cfg(any(
    feature = "doris",
    feature = "lancedb",
    feature = "postgres",
    feature = "qdrant",
    feature = "sqlite",
    feature = "turbopuffer"
))]
#[tokio::test]
async fn targets_reject_unresolved_vector_dimensions() {
    macro_rules! check_target {
        ($ctx:expr, $connector:literal, $schema:expr, $target:path $(, $extra:expr)*) => {{
            let key = cocoindex::ContextKey::new($connector);
            assert_unresolved_dimension_error(
                $target($ctx, &key, "docs", $schema $(, $extra)*).map(|_| ()),
                $connector,
            );
        }};
    }

    let tmp = tempfile::tempdir().unwrap();
    let app = cocoindex::App::open("unresolved_vector_dimensions", tmp.path().join("db"))
        .await
        .unwrap();
    app.update(|ctx| async move {
        #[cfg(feature = "sqlite")]
        check_target!(
            &ctx,
            "SQLite",
            cocoindex::connectors::sqlite::TableSchema::from_row::<UnresolvedVectorRow>(["id"])?,
            cocoindex::connectors::sqlite::table_target
        );
        #[cfg(feature = "postgres")]
        check_target!(
            &ctx,
            "Postgres",
            cocoindex::connectors::postgres::TableSchema::from_row::<UnresolvedVectorRow>(["id"])?,
            cocoindex::connectors::postgres::table_target,
            None
        );
        #[cfg(feature = "doris")]
        check_target!(
            &ctx,
            "Doris",
            cocoindex::connectors::doris::TableSchema::from_row::<UnresolvedVectorRow>(["id"])?,
            cocoindex::connectors::doris::table_target
        );
        #[cfg(feature = "lancedb")]
        check_target!(
            &ctx,
            "LanceDB",
            cocoindex::connectors::lancedb::TableSchema::from_row::<UnresolvedVectorRow>(["id"])?,
            cocoindex::connectors::lancedb::table_target
        );
        #[cfg(feature = "qdrant")]
        check_target!(
            &ctx,
            "Qdrant",
            cocoindex::connectors::qdrant::CollectionSchema::from_row::<UnresolvedVectorRow>(
                cocoindex::connectors::qdrant::Distance::Cosine
            )?,
            cocoindex::connectors::qdrant::collection_target
        );
        #[cfg(feature = "turbopuffer")]
        check_target!(
            &ctx,
            "Turbopuffer",
            cocoindex::connectors::turbopuffer::NamespaceSchema::from_row::<UnresolvedVectorRow>(
                cocoindex::connectors::turbopuffer::DistanceMetric::CosineDistance
            )?,
            cocoindex::connectors::turbopuffer::namespace_target
        );
        Ok(())
    })
    .await
    .unwrap();
}

/// End-to-end: a `from_row`-derived schema actually creates a SQLite table and
/// round-trips a row (no server needed).
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_from_row_round_trips_a_row() -> cocoindex::Result<()> {
    use cocoindex::connectors::sqlite::{self, Database, TableSchema};
    use cocoindex::{ContextKey, Environment, SchemaFields};
    use sqlx::Row as _;

    #[derive(serde::Serialize, SchemaFields, Clone)]
    struct Item {
        id: i64,
        name: String,
        score: f64,
        data: Vec<u8>,
        elapsed: std::time::Duration,
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_file = tmp.path().join("t.db");
    let db = Database::connect(db_file.to_str().unwrap()).await?;

    let rows = vec![
        Item {
            id: 1,
            name: "a".into(),
            score: 1.5,
            data: vec![0, 104, 105, 255],
            elapsed: std::time::Duration::new(1, 250_000_000),
        },
        Item {
            id: 2,
            name: "b".into(),
            score: 2.5,
            data: Vec::new(),
            elapsed: std::time::Duration::new(2, 500_000_000),
        },
    ];

    let db_key = ContextKey::<Database>::new("sqlite_from_row_db");
    let env = Environment::builder()
        .db_path(tmp.path().join("coco_db"))
        .provide_key(&db_key, db.clone())
        .build()
        .await?;
    let app = env.app("SqliteFromRow").await?;
    app.run(move |ctx| {
        let rows = rows.clone();
        async move {
            let schema = TableSchema::from_row::<Item>(["id"])?;
            let table = sqlite::mount_table_target(&ctx, &db_key, "items", schema).await?;
            for row in &rows {
                table.declare_row(&ctx, row)?;
            }
            Ok(())
        }
    })
    .await?;

    let fetched = sqlx::query("SELECT id, name, score, data, elapsed FROM items ORDER BY id")
        .fetch_all(db.pool())
        .await
        .unwrap();
    let got: Vec<(i64, String, f64, Vec<u8>, f64)> = fetched
        .iter()
        .map(|r| {
            (
                r.get::<i64, _>("id"),
                r.get::<String, _>("name"),
                r.get::<f64, _>("score"),
                r.get::<Vec<u8>, _>("data"),
                r.get::<f64, _>("elapsed"),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            (1, "a".to_string(), 1.5, vec![0, 104, 105, 255], 1.25),
            (2, "b".to_string(), 2.5, Vec::new(), 2.5),
        ]
    );
    Ok(())
}

/// End-to-end: a `from_row`-derived schema creates a live Doris table and
/// round-trips a row. Skips when `DORIS_FE_HOST` is unset.
#[cfg(feature = "doris")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn doris_from_row_round_trips_a_row() -> cocoindex::Result<()> {
    use cocoindex::connectors::doris::{self, DorisConfig, DorisConnection, TableSchema};
    use cocoindex::{ContextKey, Environment, SchemaFields};
    use sqlx::Row as _;

    let Ok(fe_host) = std::env::var("DORIS_FE_HOST") else {
        eprintln!("skipping live Doris from_row test; DORIS_FE_HOST is not set");
        return Ok(());
    };
    let database = std::env::var("DORIS_DATABASE").unwrap_or_else(|_| "cocoindex_test".to_string());
    let cfg = DorisConfig::new(fe_host, database.clone())
        .fe_http_port(
            std::env::var("DORIS_HTTP_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(8030),
        )
        .query_port(
            std::env::var("DORIS_QUERY_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(9030),
        );
    let conn = DorisConnection::connect(cfg).await?;

    #[derive(serde::Serialize, SchemaFields, Clone)]
    struct Doc {
        id: String,
        title: Option<String>,
        views: i64,
    }

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let table = format!("coco_fromrow_{nonce}");

    let rows = vec![
        Doc {
            id: "1".into(),
            title: Some("hi".into()),
            views: 10,
        },
        Doc {
            id: "2".into(),
            title: None,
            views: 20,
        },
    ];

    let tmp = tempfile::tempdir().unwrap();
    let conn_key = ContextKey::<DorisConnection>::new("doris_from_row_db");
    let table2 = table.clone();
    let app = Environment::builder()
        .db_path(tmp.path().join("db"))
        .provide_key(&conn_key, conn.clone())
        .build()
        .await?
        .app("DorisFromRow")
        .await?;
    app.run(move |ctx| {
        let table = table2.clone();
        let rows = rows.clone();
        async move {
            let schema = TableSchema::from_row::<Doc>(["id"])?;
            let target = doris::mount_table_target(&ctx, &conn_key, table, schema).await?;
            for row in &rows {
                target.declare_row(&ctx, row)?;
            }
            Ok(())
        }
    })
    .await?;

    let sql = format!("SELECT id, views FROM `{database}`.`{table}` ORDER BY id");
    let fetched = sqlx::raw_sql(&sql).fetch_all(conn.pool()).await.unwrap();
    let got: Vec<(String, i64)> = fetched
        .iter()
        .map(|r| (r.get::<String, _>("id"), r.get::<i64, _>("views")))
        .collect();
    assert_eq!(got, vec![("1".to_string(), 10), ("2".to_string(), 20)]);

    let _ = sqlx::raw_sql(&format!("DROP TABLE IF EXISTS `{database}`.`{table}`"))
        .execute(conn.pool())
        .await;
    Ok(())
}

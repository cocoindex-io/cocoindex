//! Tests for `TableSchema::from_row::<T>()` — the derive-based schema
//! construction that mirrors Python's `TableSchema.from_class`. Each connector's
//! `from_row` must produce exactly the schema a user would write by hand, using
//! that connector's leaf-type mapping.

#[cfg(feature = "doris")]
#[test]
fn doris_from_row_matches_explicit_schema() {
    use cocoindex::SchemaFields;
    use cocoindex::doris::{ColumnDef, TableSchema};

    #[derive(SchemaFields)]
    #[allow(dead_code)]
    struct Row {
        id: String,
        name: Option<String>,
        count: i64,
        score: f64,
        active: bool,
        #[coco(vector = 4)]
        embedding: Vec<f32>,
        #[coco(json)]
        tags: Vec<String>,
        #[coco(type = "VARCHAR(10)")]
        code: String,
    }

    let got = TableSchema::from_row::<Row>(["id"]).unwrap();
    let want = TableSchema::new(
        [
            ("id", ColumnDef::new("TEXT").not_null()),
            ("name", ColumnDef::new("TEXT")), // Option -> nullable
            ("count", ColumnDef::new("BIGINT").not_null()),
            ("score", ColumnDef::new("DOUBLE").not_null()),
            ("active", ColumnDef::new("BOOLEAN").not_null()),
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
    use cocoindex::sqlite::{ColumnDef, TableSchema};

    #[derive(SchemaFields)]
    #[allow(dead_code)]
    struct Row {
        id: i64,
        name: Option<String>,
        score: f64,
        blob: Vec<u8>,
        #[coco(vector = 3)]
        embedding: Vec<f32>,
    }

    let got = TableSchema::from_row::<Row>(["id"]).unwrap();
    let want = TableSchema::new(
        [
            ("id", ColumnDef::new("INTEGER").not_null()),
            ("name", ColumnDef::new("TEXT")),
            ("score", ColumnDef::new("REAL").not_null()),
            ("blob", ColumnDef::new("BLOB").not_null()),
            ("embedding", ColumnDef::new("float[3]").not_null()),
        ],
        ["id"],
    )
    .unwrap();
    assert_eq!(got, want);
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_from_row_matches_explicit_schema() {
    use cocoindex::SchemaFields;
    use cocoindex::postgres::{ColumnDef, TableSchema};

    #[derive(SchemaFields)]
    #[allow(dead_code)]
    struct Row {
        id: i64,
        title: Option<String>,
        views: i32,
        ratio: f32,
        #[coco(vector = 8)]
        embedding: Vec<f32>,
        #[coco(vector = 8, half)]
        embedding_half: Vec<f32>,
        #[coco(json)]
        meta: Vec<String>,
    }

    let got = TableSchema::from_row::<Row>(["id"]).unwrap();
    // Postgres ColumnDef::new is NOT NULL by default; `.nullable()` opts in.
    let want = TableSchema::new(
        [
            ("id", ColumnDef::new("bigint")),
            ("title", ColumnDef::new("text").nullable()),
            ("views", ColumnDef::new("integer")),
            ("ratio", ColumnDef::new("real")),
            ("embedding", ColumnDef::new("vector(8)")),
            ("embedding_half", ColumnDef::new("halfvec(8)")),
            ("meta", ColumnDef::new("jsonb")),
        ],
        ["id"],
    )
    .unwrap();
    assert_eq!(got, want);
}

/// End-to-end: a `from_row`-derived schema actually creates a SQLite table and
/// round-trips a row (no server needed).
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_from_row_round_trips_a_row() -> cocoindex::Result<()> {
    use cocoindex::sqlite::{self, Database, TableSchema};
    use cocoindex::{App, SchemaFields};
    use sqlx::Row as _;

    #[derive(serde::Serialize, SchemaFields, Clone)]
    struct Item {
        id: i64,
        name: String,
        score: f64,
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_file = tmp.path().join("t.db");
    let db = Database::connect(db_file.to_str().unwrap()).await?;

    let rows = vec![
        Item {
            id: 1,
            name: "a".into(),
            score: 1.5,
        },
        Item {
            id: 2,
            name: "b".into(),
            score: 2.5,
        },
    ];

    let app = App::builder("SqliteFromRow")
        .db_path(tmp.path().join("coco_db"))
        .build()
        .await?;
    let db2 = db.clone();
    app.run(move |ctx| {
        let db = db2.clone();
        let rows = rows.clone();
        async move {
            let schema = TableSchema::from_row::<Item>(["id"])?;
            let table = sqlite::mount_table_target(&ctx, &db, "items", schema).await?;
            for row in &rows {
                table.declare_row(&ctx, row)?;
            }
            Ok(())
        }
    })
    .await?;

    let fetched = sqlx::query("SELECT id, name, score FROM items ORDER BY id")
        .fetch_all(db.pool())
        .await
        .unwrap();
    let got: Vec<(i64, String, f64)> = fetched
        .iter()
        .map(|r| {
            (
                r.get::<i64, _>("id"),
                r.get::<String, _>("name"),
                r.get::<f64, _>("score"),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![(1, "a".to_string(), 1.5), (2, "b".to_string(), 2.5)]
    );
    Ok(())
}

/// End-to-end: a `from_row`-derived schema creates a live Doris table and
/// round-trips a row. Skips when `DORIS_FE_HOST` is unset.
#[cfg(feature = "doris")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn doris_from_row_round_trips_a_row() -> cocoindex::Result<()> {
    use cocoindex::doris::{self, DorisConfig, DorisConnection, TableSchema};
    use cocoindex::{App, SchemaFields};
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
    let conn2 = conn.clone();
    let table2 = table.clone();
    let app = App::builder("DorisFromRow")
        .db_path(tmp.path().join("db"))
        .build()
        .await?;
    app.run(move |ctx| {
        let conn = conn2.clone();
        let table = table2.clone();
        let rows = rows.clone();
        async move {
            let schema = TableSchema::from_row::<Doc>(["id"])?;
            let target = doris::mount_table_target(&ctx, &conn, table, schema).await?;
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

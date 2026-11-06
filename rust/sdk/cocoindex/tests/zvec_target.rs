//! Integration test for the zvec target connector.
//!
//! Fully hermetic: it runs real CocoIndex pipelines into zvec collections in a
//! temp directory (zvec is embedded, no external services), then reads them
//! back through the zvec API to assert the reconcile semantics.
//!
//!   cargo test -p cocoindex --features zvec --test zvec_target
#![cfg(feature = "zvec")]

use std::future::Future;
use std::time::Duration;

use cocoindex::connectors::zvec::zvec_rust::{
    self, DataType, Doc, FieldSchema, Fts, IndexParams, MetricType, SearchQuery,
};
use cocoindex::connectors::zvec::{
    self, CollectionSchema, ConnectOptions, ManagedConnection, ScalarType, VectorMetric,
    VectorQuantize, ZvecFtsType, ZvecType, ZvecVectorDef,
};
use cocoindex::{Ctx, Environment, ManagedTargetOptions, Result, SchemaFields};
use serde::Serialize;

cocoindex::context_key!(
    static DB: ManagedConnection, key = "zvec_test"
);

const EMB: [f32; 4] = [0.1, 0.2, 0.3, 0.4];

#[derive(Clone, Serialize, SchemaFields)]
struct SimpleDoc {
    id: String,
    title: String,
    year: i64,
    #[coco(vector = 4)]
    embedding: Vec<f32>,
}

fn simple(id: &str, title: &str, year: i64) -> SimpleDoc {
    SimpleDoc {
        id: id.to_string(),
        title: title.to_string(),
        year,
        embedding: EMB.to_vec(),
    }
}

fn simple_schema() -> CollectionSchema {
    CollectionSchema::from_row::<SimpleDoc>(["id"]).unwrap()
}

/// A connection in `dir` plus the CocoIndex state path the runs share.
struct Fixture {
    conn: ManagedConnection,
    tempdir: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let conn = zvec::connect(tempdir.path().join("zvec"), ConnectOptions::default()).unwrap();
        Self { conn, tempdir }
    }

    /// Run one update of the app `name`. The state path persists across runs,
    /// so reconciliation sees the previous run's tracking records.
    async fn run<F, Fut>(&self, name: &str, f: F) -> Result<()>
    where
        F: FnOnce(Ctx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let app = Environment::builder()
            .db_path(self.tempdir.path().join(".cocoindex_db"))
            .provide_key(&DB, self.conn.clone())
            .build()
            .await?
            .app(name)
            .await?;
        app.run(f).await?;
        Ok(())
    }

    /// Run one update that mounts `collection` with `schema` and declares `rows`.
    async fn run_rows<R>(
        &self,
        name: &str,
        collection: &'static str,
        schema: CollectionSchema,
        rows: Vec<R>,
    ) -> Result<()>
    where
        R: Serialize + Send + Sync + 'static,
    {
        self.run(name, move |ctx| async move {
            let target = zvec::mount_collection_target(&ctx, &DB, collection, schema).await?;
            for row in &rows {
                target.declare_row(&ctx, row)?;
            }
            Ok(())
        })
        .await
    }

    /// Read from a collection. The handle is dropped on return: a live handle
    /// keeps a later run from destroying the collection.
    async fn read<T>(&self, collection: &str, f: impl FnOnce(&zvec_rust::Collection) -> T) -> T {
        let handle = self.conn.open_existing(collection).await.unwrap();
        f(&handle)
    }

    async fn fetch(&self, collection: &str, id: &str) -> Option<Doc> {
        self.read(collection, |c| c.fetch(&[id]).unwrap().into_iter().next())
            .await
    }

    async fn doc_count(&self, collection: &str) -> u64 {
        self.read(collection, |c| c.stats().unwrap().doc_count)
            .await
    }

    fn exists(&self, collection: &str) -> bool {
        self.conn.collection_path(collection).exists()
    }
}

/// A string field's value; `None` for a null field.
fn string(doc: &Doc, field: &str) -> Option<String> {
    if doc.is_field_null(field) {
        return None;
    }
    doc.get_string(field).unwrap()
}

#[tokio::test]
async fn creates_updates_and_deletes_documents() -> Result<()> {
    let fx = Fixture::new();
    const COL: &str = "simple_docs";
    let run = |rows| fx.run_rows("SimpleDocs", COL, simple_schema(), rows);

    run(vec![simple("1", "Alice", 2020), simple("2", "Bob", 2021)]).await?;
    assert!(fx.exists(COL));
    assert_eq!(fx.doc_count(COL).await, 2);
    let doc = fx.fetch(COL, "1").await.expect("doc 1");
    assert_eq!(string(&doc, "title").as_deref(), Some("Alice"));
    assert_eq!(doc.get_i64("year").unwrap(), Some(2020));
    assert_eq!(doc.get_vector_f32("embedding").unwrap(), Some(EMB.to_vec()));
    // The primary key is the document id, not a field.
    assert!(!doc.has_field("id"));
    drop(doc);

    // A changed row is upserted in place; the other is untouched.
    run(vec![
        simple("1", "Alice v2", 2099),
        simple("2", "Bob", 2021),
    ])
    .await?;
    let doc = fx.fetch(COL, "1").await.expect("doc 1");
    assert_eq!(string(&doc, "title").as_deref(), Some("Alice v2"));
    assert_eq!(doc.get_i64("year").unwrap(), Some(2099));
    drop(doc);
    assert_eq!(fx.doc_count(COL).await, 2);

    // A row no longer declared is deleted.
    run(vec![simple("1", "Alice v2", 2099)]).await?;
    assert!(fx.fetch(COL, "1").await.is_some());
    assert!(fx.fetch(COL, "2").await.is_none());
    assert_eq!(fx.doc_count(COL).await, 1);
    Ok(())
}

#[tokio::test]
async fn unchanged_rows_are_not_written_again() -> Result<()> {
    let fx = Fixture::new();
    const COL: &str = "unchanged_docs";
    let run = |rows| fx.run_rows("UnchangedDocs", COL, simple_schema(), rows);

    run(vec![simple("1", "A", 1), simple("2", "B", 2)]).await?;
    // Remove a document behind CocoIndex's back. A run with the same rows
    // writes nothing, so the document stays gone.
    fx.read(COL, |c| c.delete(&["2"]).unwrap()).await;
    run(vec![simple("1", "A", 1), simple("2", "B", 2)]).await?;
    assert!(fx.fetch(COL, "2").await.is_none());

    // Changing only that row writes only that row.
    run(vec![simple("1", "A", 1), simple("2", "B2", 2)]).await?;
    let doc = fx.fetch(COL, "2").await.expect("doc 2 written again");
    assert_eq!(string(&doc, "title").as_deref(), Some("B2"));
    Ok(())
}

#[derive(Clone, Serialize, SchemaFields)]
struct TypedDoc {
    id: i64,
    name: String,
    score: f64,
    ratio: f32,
    count: i32,
    active: bool,
    note: Option<String>,
    blob: Vec<u8>,
    tags: Vec<String>,
    elapsed: Duration,
    small: i64,
    #[coco(type = "uint32")]
    unsigned: i64,
    #[coco(vector = 4)]
    embedding: Vec<f32>,
}

#[tokio::test]
async fn scalar_types_round_trip() -> Result<()> {
    let fx = Fixture::new();
    const COL: &str = "typed_docs";
    let schema = CollectionSchema::from_row::<TypedDoc>(["id"])?.with_column_type(
        "small",
        ZvecType {
            data_type: ScalarType::Int32,
            indexed: false,
        },
    )?;
    let row = |note: Option<&str>| TypedDoc {
        id: 7,
        name: "x".to_string(),
        score: 3.5,
        ratio: 0.25,
        count: -3,
        active: true,
        note: note.map(str::to_string),
        blob: b"hi".to_vec(),
        tags: vec!["a".to_string(), "b".to_string()],
        elapsed: Duration::from_millis(1500),
        small: 12,
        unsigned: 4_000_000_000,
        embedding: EMB.to_vec(),
    };

    fx.run_rows("TypedDocs", COL, schema.clone(), vec![row(None)])
        .await?;
    // An integer key becomes the document id as a string.
    let doc = fx.fetch(COL, "7").await.expect("doc 7");
    assert_eq!(string(&doc, "name").as_deref(), Some("x"));
    assert_eq!(doc.get_f64("score").unwrap(), Some(3.5));
    assert_eq!(doc.get_f32("ratio").unwrap(), Some(0.25));
    assert_eq!(doc.get_i32("count").unwrap(), Some(-3));
    assert_eq!(doc.get_bool("active").unwrap(), Some(true));
    assert_eq!(string(&doc, "note"), None, "a None field is stored as null");
    assert_eq!(string(&doc, "blob").as_deref(), Some("aGk="), "base64");
    assert_eq!(string(&doc, "tags").as_deref(), Some(r#"["a","b"]"#));
    assert_eq!(doc.get_f64("elapsed").unwrap(), Some(1.5), "seconds");
    assert_eq!(doc.get_i32("small").unwrap(), Some(12), "ZvecType override");
    assert_eq!(doc.get_u32("unsigned").unwrap(), Some(4_000_000_000));
    drop(doc);

    // An indexed scalar field filters a query; null becomes a value in place.
    fx.run_rows("TypedDocs", COL, schema, vec![row(Some("hello"))])
        .await?;
    let doc = fx.fetch(COL, "7").await.expect("doc 7");
    assert_eq!(string(&doc, "note").as_deref(), Some("hello"));
    drop(doc);
    let hits = fx
        .read(COL, |c| {
            let mut query = SearchQuery::new("embedding", &EMB, 5).unwrap();
            query.set_filter("count = -3").unwrap();
            let matched = c.query(&query).unwrap().len();
            query.set_filter("count = 99").unwrap();
            (matched, c.query(&query).unwrap().len())
        })
        .await;
    assert_eq!(hits, (1, 0));
    Ok(())
}

#[derive(Clone, Serialize, SchemaFields)]
struct SimpleDocV2 {
    id: String,
    title: String,
    year: i64,
    summary: Option<String>,
    #[coco(vector = 4)]
    embedding: Vec<f32>,
}

#[derive(Clone, Serialize, SchemaFields)]
struct NullableScoreDoc {
    id: String,
    score: Option<f64>,
    #[coco(vector = 4)]
    embedding: Vec<f32>,
}

/// A NaN must not be stored as the null that JSON turns it into.
#[tokio::test]
async fn non_finite_float_is_an_error_not_a_null() -> Result<()> {
    let fx = Fixture::new();
    const COL: &str = "nullable_scores";
    let schema = CollectionSchema::from_row::<NullableScoreDoc>(["id"])?;
    let row = |score: Option<f64>| NullableScoreDoc {
        id: "a".to_string(),
        score,
        embedding: EMB.to_vec(),
    };

    let err = fx
        .run_rows(
            "NullableScores",
            COL,
            schema.clone(),
            vec![row(Some(f64::NAN))],
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("non-finite"), "{err}");

    fx.run_rows("NullableScores", COL, schema, vec![row(None)])
        .await?;
    let doc = fx.fetch(COL, "a").await.expect("doc a");
    assert_eq!(doc.get_f64("score").unwrap(), None);
    Ok(())
}

#[tokio::test]
async fn schema_change_rebuilds_the_collection() -> Result<()> {
    let fx = Fixture::new();
    const COL: &str = "evolving_docs";
    const APP: &str = "EvolvingDocs";

    fx.run_rows(
        APP,
        COL,
        simple_schema(),
        vec![simple("1", "A", 1), simple("2", "B", 2)],
    )
    .await?;
    assert_eq!(fx.doc_count(COL).await, 2);

    // A new column rebuilds the collection. Row 1 is unchanged apart from the
    // new column, and row 2 is no longer declared: the rebuilt collection holds
    // exactly the rows this run declares.
    let schema_v2 = CollectionSchema::from_row::<SimpleDocV2>(["id"])?;
    fx.run_rows(
        APP,
        COL,
        schema_v2.clone(),
        vec![SimpleDocV2 {
            id: "1".to_string(),
            title: "A".to_string(),
            year: 1,
            summary: Some("first".to_string()),
            embedding: EMB.to_vec(),
        }],
    )
    .await?;
    assert_eq!(fx.doc_count(COL).await, 1);
    let doc = fx.fetch(COL, "1").await.expect("doc 1 re-declared");
    assert_eq!(string(&doc, "summary").as_deref(), Some("first"));
    assert_eq!(string(&doc, "title").as_deref(), Some("A"));
    drop(doc);

    // An index-only change (the vector metric) rebuilds too, and rows whose
    // values did not change are written into the new collection.
    let schema_l2 = schema_v2.with_vector_def(
        "embedding",
        ZvecVectorDef {
            metric: VectorMetric::L2,
            quantize: None,
        },
    )?;
    let handle_before = std::fs::metadata(fx.conn.collection_path(COL))
        .unwrap()
        .created()
        .ok();
    fx.run_rows(
        APP,
        COL,
        schema_l2,
        vec![SimpleDocV2 {
            id: "1".to_string(),
            title: "A".to_string(),
            year: 1,
            summary: Some("first".to_string()),
            embedding: EMB.to_vec(),
        }],
    )
    .await?;
    assert_eq!(fx.doc_count(COL).await, 1);
    assert!(fx.fetch(COL, "1").await.is_some());
    let handle_after = std::fs::metadata(fx.conn.collection_path(COL))
        .unwrap()
        .created()
        .ok();
    if let (Some(before), Some(after)) = (handle_before, handle_after) {
        assert!(after > before, "the collection directory was recreated");
    }
    Ok(())
}

#[derive(Clone, Serialize, SchemaFields)]
struct FtsDoc {
    id: String,
    body: String,
    #[coco(vector = 4)]
    embedding: Vec<f32>,
}

#[tokio::test]
async fn fts_field_answers_keyword_queries() -> Result<()> {
    let fx = Fixture::new();
    const COL: &str = "fts_docs";
    let schema =
        CollectionSchema::from_row::<FtsDoc>(["id"])?.with_fts("body", ZvecFtsType::default())?;
    let row = |id: &str, body: &str| FtsDoc {
        id: id.to_string(),
        body: body.to_string(),
        embedding: EMB.to_vec(),
    };
    fx.run_rows(
        "FtsDocs",
        COL,
        schema,
        vec![
            row("1", "Machine Learning is fun"),
            row("2", "deep learning uses neural networks"),
            row("3", "vector databases store embeddings"),
        ],
    )
    .await?;

    let mut ids = fx
        .read(COL, |c| {
            c.flush().unwrap();
            let mut fts = Fts::new().unwrap();
            // The default `lowercase` filter makes the match case-insensitive.
            fts.set_match_string("LEARNING").unwrap();
            let query = SearchQuery::fts("body", &fts, 10).unwrap();
            c.query(&query)
                .unwrap()
                .iter()
                .map(|d| d.get_pk().unwrap().to_string())
                .collect::<Vec<_>>()
        })
        .await;
    ids.sort();
    assert_eq!(ids, ["1", "2"]);
    Ok(())
}

#[derive(Clone, Serialize, SchemaFields)]
struct VectorDoc {
    id: String,
    #[coco(vector)]
    embedding: Vec<f32>,
    #[coco(vector = 2)]
    small: Option<Vec<f32>>,
}

#[tokio::test]
async fn dense_vectors_are_searchable() -> Result<()> {
    let fx = Fixture::new();
    const COL: &str = "vector_docs";
    let schema = CollectionSchema::from_row::<VectorDoc>(["id"])?
        .with_vector_dim("embedding", 3)?
        .with_vector_def(
            "embedding",
            ZvecVectorDef {
                metric: VectorMetric::L2,
                quantize: Some(VectorQuantize::Int8),
            },
        )?;
    let row = |id: &str, embedding: [f32; 3], small: Option<[f32; 2]>| VectorDoc {
        id: id.to_string(),
        embedding: embedding.to_vec(),
        small: small.map(|v| v.to_vec()),
    };
    fx.run_rows(
        "VectorDocs",
        COL,
        schema.clone(),
        vec![
            row("x", [1.0, 0.0, 0.0], Some([1.0, 0.0])),
            row("y", [0.0, 1.0, 0.0], None),
            row("z", [0.0, 0.0, 1.0], Some([0.0, 1.0])),
        ],
    )
    .await?;

    let (nearest, nearest_small) = fx
        .read(COL, |c| {
            let pk = |query: SearchQuery| {
                let hits = c.query(&query).unwrap();
                hits[0].get_pk().unwrap().to_string()
            };
            (
                pk(SearchQuery::new("embedding", &[0.1, 0.9, 0.1], 1).unwrap()),
                pk(SearchQuery::new("small", &[0.1, 0.9], 1).unwrap()),
            )
        })
        .await;
    assert_eq!(nearest, "y");
    assert_eq!(nearest_small, "z", "a second, nullable vector field");

    // A vector of the wrong dimension is rejected when the row is declared.
    let err = fx
        .run("VectorDocs", move |ctx| async move {
            let target = zvec::mount_collection_target(&ctx, &DB, COL, schema).await?;
            target.declare_row(
                &ctx,
                &VectorDoc {
                    id: "bad".to_string(),
                    embedding: vec![1.0, 2.0],
                    small: None,
                },
            )
        })
        .await
        .unwrap_err();
    assert!(err.to_string().contains("dimension 3"), "{err}");
    Ok(())
}

#[tokio::test]
async fn collection_is_destroyed_when_no_longer_declared() -> Result<()> {
    let fx = Fixture::new();
    const KEPT: &str = "kept_docs";
    const DROPPED: &str = "dropped_docs";
    const APP: &str = "DropCollection";
    let declare = |ctx: Ctx, collection: &'static str| async move {
        let target = zvec::mount_collection_target(&ctx, &DB, collection, simple_schema()).await?;
        target.declare_row(&ctx, &simple("1", collection, 1))
    };

    fx.run(APP, move |ctx| async move {
        declare(ctx.clone(), KEPT).await?;
        declare(ctx, DROPPED).await
    })
    .await?;
    assert!(fx.exists(KEPT) && fx.exists(DROPPED));
    let doc = fx
        .fetch(DROPPED, "1")
        .await
        .expect("doc in second collection");
    assert_eq!(string(&doc, "title").as_deref(), Some(DROPPED));
    drop(doc);

    // Only the collection that is no longer declared is destroyed.
    fx.run(APP, move |ctx| declare(ctx, KEPT)).await?;
    assert!(fx.exists(KEPT));
    assert!(!fx.exists(DROPPED));
    assert_eq!(fx.doc_count(KEPT).await, 1);

    // Declaring it again creates it afresh at the same path.
    fx.run(APP, move |ctx| async move {
        declare(ctx.clone(), KEPT).await?;
        declare(ctx, DROPPED).await
    })
    .await?;
    assert_eq!(fx.doc_count(DROPPED).await, 1);
    Ok(())
}

#[tokio::test]
async fn a_live_query_handle_blocks_destroying_the_collection() -> Result<()> {
    let fx = Fixture::new();
    const COL: &str = "held_docs";
    const APP: &str = "HeldDocs";
    fx.run_rows(APP, COL, simple_schema(), vec![simple("1", "A", 1)])
        .await?;

    let handle = fx.conn.open_existing(COL).await?;
    // The failed destroy is reported by the engine (as the run's error or in
    // its log, depending on timing); what holds is that the collection stays.
    let _ = fx.run(APP, |_ctx| async { Ok(()) }).await;
    assert!(fx.exists(COL));

    // Once the handle is gone, the next run destroys the collection.

    drop(handle);
    fx.run(APP, |_ctx| async { Ok(()) }).await?;
    assert!(!fx.exists(COL));
    Ok(())
}

#[tokio::test]
async fn user_managed_collection_is_never_created_or_destroyed() -> Result<()> {
    let fx = Fixture::new();
    const COL: &str = "user_col";
    const APP: &str = "UserManaged";

    // Pre-create the collection outside CocoIndex.
    {
        let mut title = FieldSchema::new("title", DataType::String, true, 0).unwrap();
        title
            .set_index_params(&IndexParams::invert(false, false).unwrap())
            .unwrap();
        let schema = zvec_rust::CollectionSchema::builder(COL)
            .add_field(title)
            .add_field(FieldSchema::new("year", DataType::Int64, true, 0).unwrap())
            .add_vector_field(
                "embedding",
                DataType::VectorFp32,
                4,
                IndexParams::hnsw(MetricType::Cosine, 16, 200).unwrap(),
            )
            .build()
            .unwrap();
        let path = fx.conn.collection_path(COL);
        zvec_rust::Collection::create_and_open(path.to_str().unwrap(), &schema, None)
            .unwrap()
            .try_close()
            .unwrap();
    }

    let run = |rows: Vec<SimpleDoc>| {
        fx.run(APP, move |ctx| async move {
            let target = zvec::mount_collection_target_with_options(
                &ctx,
                &DB,
                COL,
                simple_schema(),
                ManagedTargetOptions::user_managed(),
            )
            .await?;
            for row in &rows {
                target.declare_row(&ctx, row)?;
            }
            Ok(())
        })
    };
    run(vec![simple("1", "A", 1), simple("2", "B", 2)]).await?;
    let doc = fx.fetch(COL, "1").await.expect("doc 1");
    assert_eq!(string(&doc, "title").as_deref(), Some("A"));
    drop(doc);

    // Documents are still reconciled.
    run(vec![simple("1", "A", 1)]).await?;
    assert!(fx.fetch(COL, "2").await.is_none());

    // Stop declaring: a user-managed collection must not be destroyed.
    fx.run(APP, |_ctx| async { Ok(()) }).await?;
    assert!(fx.exists(COL));
    Ok(())
}

#[tokio::test]
async fn user_managed_collection_must_already_exist() -> Result<()> {
    let fx = Fixture::new();
    let err = fx
        .run("UserManagedMissing", |ctx| async move {
            let target = zvec::mount_collection_target_with_options(
                &ctx,
                &DB,
                "missing_col",
                simple_schema(),
                ManagedTargetOptions::user_managed(),
            )
            .await?;
            target.declare_row(&ctx, &simple("1", "A", 1))
        })
        .await
        .unwrap_err();
    assert!(err.to_string().contains("missing_col"), "{err}");
    assert!(!fx.exists("missing_col"));
    Ok(())
}

#[tokio::test]
async fn drop_state_destroys_the_collection() -> Result<()> {
    let fx = Fixture::new();
    const COL: &str = "state_docs";
    let app = Environment::builder()
        .db_path(fx.tempdir.path().join(".cocoindex_db"))
        .provide_key(&DB, fx.conn.clone())
        .build()
        .await?
        .app("DropState")
        .await?;
    app.run(|ctx| async move {
        let target = zvec::mount_collection_target(&ctx, &DB, COL, simple_schema()).await?;
        target.declare_row(&ctx, &simple("1", "A", 1))
    })
    .await?;
    assert!(fx.exists(COL));

    app.drop_state().await?;
    assert!(!fx.exists(COL));
    Ok(())
}

#[tokio::test]
async fn close_releases_the_collection_for_another_connection() -> Result<()> {
    let fx = Fixture::new();
    const COL: &str = "closed_docs";
    fx.run_rows(
        "ClosedDocs",
        COL,
        simple_schema(),
        vec![simple("1", "A", 1)],
    )
    .await?;

    // The first connection holds the collection's write lock until closed.
    let other = zvec::connect(fx.conn.base_path(), ConnectOptions { enable_mmap: false })?;
    assert!(other.open_existing(COL).await.is_err());
    fx.conn.close().await?;
    let handle = other.open_existing(COL).await?;
    assert_eq!(handle.stats().unwrap().doc_count, 1);
    Ok(())
}

// --- Schema validation -----------------------------------------------------

#[derive(Serialize, SchemaFields)]
struct NoVector {
    id: String,
    title: String,
}

#[derive(Serialize, SchemaFields)]
struct HalfVector {
    id: String,
    #[coco(vector = 4, half)]
    embedding: Vec<f32>,
}

#[derive(Serialize, SchemaFields)]
struct FloatKey {
    id: f64,
    #[coco(vector = 4)]
    embedding: Vec<f32>,
}

#[derive(Serialize, SchemaFields)]
struct BadType {
    id: String,
    #[coco(type = "varchar")]
    title: String,
    #[coco(vector = 4)]
    embedding: Vec<f32>,
}

#[test]
fn schema_rejects_what_zvec_cannot_store() {
    let err = |r: Result<CollectionSchema>| r.unwrap_err().to_string();

    // Exactly one primary key column, present in the row type.
    assert!(err(CollectionSchema::from_row::<SimpleDoc>(["id", "title"])).contains("exactly one"));
    assert!(err(CollectionSchema::from_row::<SimpleDoc>(["missing"])).contains("not found"));
    // The key value becomes the document id: it must be a string or an integer.
    assert!(err(CollectionSchema::from_row::<FloatKey>(["id"])).contains("primary key"));
    assert!(err(CollectionSchema::from_row::<SimpleDoc>(["embedding"])).contains("primary key"));

    assert!(err(CollectionSchema::from_row::<HalfVector>(["id"])).contains("float16"));
    assert!(err(CollectionSchema::from_row::<BadType>(["id"])).contains("varchar"));

    // Overrides must fit the column they name.
    assert!(err(simple_schema().with_fts("year", ZvecFtsType::default())).contains("string"));
    assert!(err(simple_schema().with_fts("nope", ZvecFtsType::default())).contains("unknown"));
    assert!(err(simple_schema().with_vector_dim("title", 4)).contains("not a vector"));
    assert!(err(simple_schema().with_vector_dim("embedding", 0)).contains("dimension"));
    assert!(
        err(simple_schema().with_vector_def("title", ZvecVectorDef::default()))
            .contains("not a vector")
    );
    let fts = simple_schema()
        .with_fts("title", ZvecFtsType::default())
        .unwrap();
    assert!(
        err(fts.with_column_type(
            "title",
            ZvecType {
                data_type: ScalarType::String,
                indexed: false
            }
        ))
        .contains("ZvecFtsType")
    );
}

#[tokio::test]
async fn collection_target_rejects_invalid_names_and_schemas() -> Result<()> {
    let fx = &Fixture::new();
    let mount_err = |collection: &'static str, schema: CollectionSchema| async move {
        fx.run("Validation", move |ctx| async move {
            zvec::mount_collection_target(&ctx, &DB, collection, schema).await?;
            Ok(())
        })
        .await
        .unwrap_err()
        .to_string()
    };

    assert!(
        mount_err("bad-name", simple_schema())
            .await
            .contains("collection name")
    );
    assert!(
        mount_err("ab", simple_schema())
            .await
            .contains("at least 3")
    );
    assert!(
        mount_err("no_vector", CollectionSchema::from_row::<NoVector>(["id"])?)
            .await
            .contains("at least one vector field")
    );
    assert!(
        mount_err(
            "unresolved_dim",
            CollectionSchema::from_row::<VectorDoc>(["id"])?
        )
        .await
        .contains("with_vector_dim")
    );
    Ok(())
}

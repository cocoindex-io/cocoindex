//! Edge and failure cases of the zvec target: document id forms, vector values
//! that do not fit their column, schema changes between runs, batches around
//! and far beyond the sizes a run writes at once, and a run that fails part
//! way followed by one that succeeds.
//!
//! Hermetic (zvec is embedded; collections live in a temp directory):
//!
//!   cargo test -p cocoindex --features zvec,testing --test zvec_battle
#![cfg(feature = "zvec")]

use std::sync::Arc;

use cocoindex::connectors::zvec::{
    self, CollectionSchema, ConnectOptions, ManagedConnection, ZvecType,
};
use cocoindex::{App, Environment, Result, SchemaFields};
use serde::Serialize;
use serde_json::{Value, json};

cocoindex::context_key!(
    static DB: ManagedConnection, key = "zvec_battle"
);

const COL: &str = "docs";

struct Fixture {
    _dir: tempfile::TempDir,
    conn: ManagedConnection,
    app: App,
}

async fn fixture(name: &str) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let conn = zvec::connect(dir.path().join("zvec"), ConnectOptions::default()).unwrap();
    let app = Environment::builder()
        .db_path(dir.path().join(".cocoindex_db"))
        .provide_key(&DB, conn.clone())
        .build()
        .await
        .unwrap()
        .app(name)
        .await
        .unwrap();
    Fixture {
        _dir: dir,
        conn,
        app,
    }
}

impl Fixture {
    async fn run<R: Serialize + Send + Sync + 'static>(
        &self,
        schema: &CollectionSchema,
        rows: Vec<R>,
    ) -> Result<()> {
        let schema = schema.clone();
        let rows = Arc::new(rows);
        self.app
            .run(move |ctx| {
                let schema = schema.clone();
                let rows = rows.clone();
                async move {
                    let target = zvec::mount_collection_target(&ctx, &DB, COL, schema).await?;
                    for row in rows.iter() {
                        target.declare_row(&ctx, row)?;
                    }
                    Ok(())
                }
            })
            .await
            .map(|_| ())
    }

    async fn doc_count(&self) -> u64 {
        let handle = self.conn.open_existing(COL).await.unwrap();
        handle.stats().unwrap().doc_count
    }

    async fn title(&self, id: &str) -> Option<String> {
        let handle = self.conn.open_existing(COL).await.unwrap();
        let doc = handle.fetch(&[id]).unwrap().into_iter().next()?;
        doc.get_string("title").unwrap()
    }
}

#[derive(Clone, Serialize, SchemaFields)]
struct StrDoc {
    id: String,
    title: String,
    #[coco(vector = 3)]
    embedding: Vec<f32>,
}

fn str_doc(id: &str, title: &str) -> StrDoc {
    StrDoc {
        id: id.to_string(),
        title: title.to_string(),
        embedding: vec![0.1, 0.2, 0.3],
    }
}

#[derive(Clone, Serialize, SchemaFields)]
struct IntDoc {
    id: i64,
    title: String,
    #[coco(vector = 3)]
    embedding: Vec<f32>,
}

/// String document ids with quotes, backslashes, whitespace, unicode and a
/// long id are inserted, updated and deleted.
#[tokio::test]
async fn string_ids_with_special_characters_are_updated_and_deleted() -> Result<()> {
    let f = fixture("ZvecBattleStringIds").await;
    let schema = CollectionSchema::from_row::<StrDoc>(["id"])?;
    let long = "k".repeat(512);
    let mut ids: Vec<&str> = vec![
        "plain",
        "it's",
        "back\\slash",
        "double\"quote",
        "unicode-ключ-🔑",
        " spaced ",
        "slash/and:colon",
        "x' OR '1'='1",
        &long,
    ];
    let docs =
        |ids: &[&str], title: &str| ids.iter().map(|id| str_doc(id, title)).collect::<Vec<_>>();

    // zvec refuses an empty id and one with a control character. The run
    // fails naming the id, and the next run without it succeeds.
    for bad in ["", "new\nline", "tab\tchar"] {
        let mut with_bad = ids.clone();
        with_bad.push(bad);
        let err = f.run(&schema, docs(&with_bad, "v0")).await.unwrap_err();
        assert!(err.to_string().contains("zvec upsert"), "{bad:?}: {err}");
    }
    ids.sort();
    let docs = |title: &str| docs(&ids, title);
    f.run(&schema, docs("v1")).await?;
    assert_eq!(f.doc_count().await, ids.len() as u64);
    f.run(&schema, docs("v2")).await?;
    assert_eq!(f.doc_count().await, ids.len() as u64);
    for id in &ids {
        assert_eq!(f.title(id).await.as_deref(), Some("v2"), "{id:?}");
    }
    f.run(&schema, vec![str_doc("plain", "v2")]).await?;
    assert_eq!(f.doc_count().await, 1);
    assert_eq!(f.title("plain").await.as_deref(), Some("v2"));
    Ok(())
}

/// Integer document ids at the ends of the i64 range are stored under their
/// decimal text and deleted.
#[tokio::test]
async fn extreme_integer_ids_are_updated_and_deleted() -> Result<()> {
    let f = fixture("ZvecBattleIntIds").await;
    let schema = CollectionSchema::from_row::<IntDoc>(["id"])?;
    let ids = [i64::MIN, -1, 0, 1, i64::MAX];
    let docs = |title: &str| {
        ids.iter()
            .map(|id| IntDoc {
                id: *id,
                title: title.to_string(),
                embedding: vec![0.1, 0.2, 0.3],
            })
            .collect::<Vec<_>>()
    };
    f.run(&schema, docs("v1")).await?;
    f.run(&schema, docs("v2")).await?;
    assert_eq!(f.doc_count().await, ids.len() as u64);
    assert_eq!(f.title(&i64::MIN.to_string()).await.as_deref(), Some("v2"));
    f.run(&schema, docs("v2")[2..3].to_vec()).await?;
    assert_eq!(f.doc_count().await, 1);
    assert_eq!(f.title("0").await.as_deref(), Some("v2"));
    Ok(())
}

/// A vector of the wrong length (or empty) fails the run; the next run, with
/// the vector fixed, writes every document.
#[tokio::test]
async fn wrong_length_vector_fails_then_the_fixed_run_succeeds() -> Result<()> {
    let f = fixture("ZvecBattleWrongLength").await;
    let schema = CollectionSchema::from_row::<StrDoc>(["id"])?;
    let mut docs: Vec<StrDoc> = (0..20).map(|i| str_doc(&i.to_string(), "t")).collect();
    for bad in [vec![1.0, 2.0], vec![]] {
        docs[7].embedding = bad.clone();
        let err = f.run(&schema, docs.clone()).await.unwrap_err();
        assert!(
            err.to_string()
                .contains(&format!("expects dimension 3, got {}", bad.len())),
            "{err}"
        );
    }
    docs[7].embedding = vec![1.0, 2.0, 3.0];
    f.run(&schema, docs).await?;
    assert_eq!(f.doc_count().await, 20);
    Ok(())
}

/// NaN and infinite elements are rejected when the row is declared, and so is
/// a finite `f64` beyond the `f32` range, in a vector and in a float field.
#[tokio::test]
async fn non_finite_and_overflowing_values_are_rejected() -> Result<()> {
    let f = fixture("ZvecBattleNonFinite").await;
    let schema = CollectionSchema::from_row::<StrDoc>(["id"])?;
    for bad in [f32::NAN, f32::INFINITY] {
        let mut doc = str_doc("a", "t");
        doc.embedding[0] = bad;
        let err = f.run(&schema, vec![doc]).await.unwrap_err();
        assert!(err.to_string().contains("non-finite"), "{bad}: {err}");
    }

    let schema = CollectionSchema::from_row::<StrDoc>(["id"])?.with_column_type(
        "title",
        ZvecType {
            data_type: zvec::ScalarType::Float,
            indexed: false,
        },
    )?;
    for (field, value) in [
        ("embedding", json!([1e39, 0.0, 0.0])),
        ("title", json!(1e39)),
    ] {
        let mut row = json!({"id": "a", "title": 1.0, "embedding": [0.1, 0.2, 0.3]});
        row[field] = value.clone();
        let err = f.run(&schema, vec![row]).await.unwrap_err();
        assert!(
            err.to_string()
                .contains(&format!("zvec column {field:?} expects")),
            "{field}={value}: {err}"
        );
    }
    Ok(())
}

/// A float16 vector field is refused when the schema is derived.
#[test]
fn float16_vector_field_is_refused() {
    #[derive(Serialize, SchemaFields)]
    struct HalfDoc {
        id: String,
        #[coco(vector = 3, half)]
        embedding: Vec<f32>,
    }
    let err = CollectionSchema::from_row::<HalfDoc>(["id"]).unwrap_err();
    assert!(err.to_string().contains("float16"), "{err}");
}

/// The vector dimension changes between runs: the collection is rebuilt and
/// holds the documents of the new run.
#[tokio::test]
async fn vector_dimension_change_rebuilds_the_collection() -> Result<()> {
    let f = fixture("ZvecBattleDimChange").await;
    for dim in [3usize, 8, 3] {
        let schema =
            CollectionSchema::from_row::<StrDoc>(["id"])?.with_vector_dim("embedding", dim)?;
        let docs: Vec<Value> = (0..5)
            .map(|i| json!({"id": i.to_string(), "title": format!("d{dim}"), "embedding": vec![0.5_f32; dim]}))
            .collect();
        f.run(&schema, docs).await?;
        assert_eq!(f.doc_count().await, 5, "dim {dim}");
        assert_eq!(f.title("0").await, Some(format!("d{dim}")), "dim {dim}");
    }
    Ok(())
}

/// Document counts around the sizes a sink writes at once, inserted then all
/// deleted, in one app so each count reconciles against the previous one.
#[tokio::test]
async fn batches_around_the_chunk_boundaries() -> Result<()> {
    let f = fixture("ZvecBattleBatches").await;
    let schema = CollectionSchema::from_row::<StrDoc>(["id"])?;
    for n in [499, 500, 501, 999, 1000, 1001, 2500] {
        let title = format!("v{n}");
        let docs: Vec<StrDoc> = (0..n).map(|i| str_doc(&i.to_string(), &title)).collect();
        f.run(&schema, docs).await?;
        assert_eq!(f.doc_count().await, n as u64, "{n} docs");
        assert_eq!(f.title(&(n - 1).to_string()).await, Some(title), "{n} docs");
    }
    f.run(&schema, Vec::<StrDoc>::new()).await?;
    assert_eq!(f.doc_count().await, 0, "every document deleted");
    Ok(())
}

/// A large run: 100 000 documents inserted, then half deleted. Slow, so
/// ignored by default:
///
///   cargo test -p cocoindex --features zvec,testing --test zvec_battle \
///     -- --ignored large_run
#[tokio::test]
#[ignore = "soak: 100k documents"]
async fn large_run_inserts_and_deletes() -> Result<()> {
    let f = fixture("ZvecBattleLarge").await;
    let schema = CollectionSchema::from_row::<StrDoc>(["id"])?;
    const N: usize = 100_000;
    let docs: Vec<StrDoc> = (0..N).map(|i| str_doc(&i.to_string(), "v1")).collect();
    let start = std::time::Instant::now();
    f.run(&schema, docs.clone()).await?;
    eprintln!("insert {N}: {:?}", start.elapsed());
    assert_eq!(f.doc_count().await, N as u64);
    let start = std::time::Instant::now();
    f.run(&schema, docs[..N / 2].to_vec()).await?;
    eprintln!("delete {}: {:?}", N / 2, start.elapsed());
    assert_eq!(f.doc_count().await, (N / 2) as u64);
    Ok(())
}

/// Nested JSON, a multi-megabyte string and bytes round-trip.
#[tokio::test]
async fn nested_json_and_huge_values_round_trip() -> Result<()> {
    #[derive(Clone, Serialize, SchemaFields)]
    struct RichDoc {
        id: String,
        title: String,
        meta: serde_json::Value,
        blob: Vec<u8>,
        #[coco(vector = 3)]
        embedding: Vec<f32>,
    }
    let f = fixture("ZvecBattleRich").await;
    let schema = CollectionSchema::from_row::<RichDoc>(["id"])?;
    let nested = json!({"a": [1, {"b": null, "c": "ü"}], "d": {"e": [true, 1.5]}});
    let huge = "x".repeat(4 << 20);
    f.run(
        &schema,
        vec![RichDoc {
            id: "r".to_string(),
            title: huge.clone(),
            meta: nested.clone(),
            blob: vec![0, 255, 7],
            embedding: vec![0.1, 0.2, 0.3],
        }],
    )
    .await?;
    let handle = f.conn.open_existing(COL).await.unwrap();
    let doc = handle.fetch(&["r"]).unwrap().into_iter().next().unwrap();
    assert_eq!(doc.get_string("title").unwrap().unwrap().len(), huge.len());
    let meta: Value = serde_json::from_str(&doc.get_string("meta").unwrap().unwrap()).unwrap();
    assert_eq!(meta, nested);
    assert_eq!(doc.get_string("blob").unwrap().as_deref(), Some("AP8H"));
    Ok(())
}

/// A document removed behind CocoIndex's back is deleted without error when a
/// later run no longer declares it.
#[tokio::test]
async fn undeclaring_a_document_removed_out_of_band_succeeds() -> Result<()> {
    let f = fixture("ZvecBattleOutOfBandDelete").await;
    let schema = CollectionSchema::from_row::<StrDoc>(["id"])?;
    f.run(&schema, vec![str_doc("a", "t"), str_doc("b", "t")])
        .await?;
    {
        let handle = f.conn.open_existing(COL).await.unwrap();
        handle.delete(&["b"]).unwrap();
    }
    f.run(&schema, vec![str_doc("a", "t")]).await?;
    assert_eq!(f.doc_count().await, 1);
    Ok(())
}

static TARGET_ARG_CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[cocoindex::function]
async fn declare_doc_in_component(
    ctx: &cocoindex::Ctx,
    id: String,
    target: zvec::CollectionTarget,
) -> Result<()> {
    TARGET_ARG_CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    target.declare_row(ctx, &str_doc(&id, "t"))
}

/// A collection handle as a mount argument: it serializes to its connection
/// and collection names, so an unchanged child is skipped and a child given
/// another collection runs again.
#[tokio::test]
async fn collection_target_is_a_mount_argument_keyed_by_its_collection() -> Result<()> {
    let f = fixture("ZvecBattleTargetArg").await;
    let schema = CollectionSchema::from_row::<StrDoc>(["id"])?;
    let run = |name: &'static str| {
        let schema = schema.clone();
        let app = f.app.clone();
        async move {
            app.run(move |ctx| async move {
                let target = zvec::mount_collection_target(&ctx, &DB, name, schema).await?;
                assert_eq!(
                    serde_json::to_value(&target).unwrap(),
                    json!({"db_key": "zvec_battle", "collection_name": name})
                );
                cocoindex::use_mount_each!(vec![("1".to_string(), "1".to_string())], |id| {
                    declare_doc_in_component(ctx, id, target.clone())
                })
                .await?;
                Ok(())
            })
            .await
        }
    };
    let calls = || TARGET_ARG_CALLS.load(std::sync::atomic::Ordering::SeqCst);
    run(COL).await?;
    assert_eq!(calls(), 1);
    assert_eq!(f.doc_count().await, 1);
    run(COL).await?;
    assert_eq!(calls(), 1, "same collection: the child is skipped");
    run("other_docs").await?;
    assert_eq!(calls(), 2, "another collection: the child runs");
    Ok(())
}

//! Live Valkey (RediSearch) target e2e. Skips when no server is reachable. Run:
//!   VALKEY_URI=redis://localhost:6379 \
//!     cargo test -p cocoindex --features valkey --test valkey_target
#![cfg(feature = "valkey")]

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use cocoindex::Environment;
use cocoindex::connectors::valkey::{
    self, Distance, Document, FieldDef, FieldType, IndexSchema, PayloadValue, VectorAlgorithm,
    VectorDef,
};
use cocoindex::resources::schema::VectorSchema;
use cocoindex::statediff::ManagedTargetOptions;

cocoindex::context_key!(static VK: valkey::Valkey, key = "valkey_target_conn");

fn nonce() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

async fn try_connect() -> Option<valkey::Valkey> {
    let uri = std::env::var("VALKEY_URI").ok()?;
    let vk = valkey::Valkey::connect(&uri).await.ok()?;
    // Probe with a cheap command so an unreachable / non-RediSearch server skips.
    let client = redis::Client::open(uri).ok()?;
    let mut conn = client.get_multiplexed_async_connection().await.ok()?;
    let _: Vec<String> = redis::cmd("FT._LIST").query_async(&mut conn).await.ok()?;
    Some(vk)
}

async fn schema() -> IndexSchema {
    IndexSchema::create(
        VectorDef {
            schema: &VectorSchema::f32(3),
            distance: Distance::Cosine,
            algorithm: VectorAlgorithm::Hnsw,
        },
        vec![
            FieldDef::new("text", FieldType::Text),
            FieldDef::new("price", FieldType::Numeric),
        ],
    )
    .await
    .unwrap()
}

fn doc(id: &str, vector: Vec<f32>, text: &str) -> Document {
    let mut payload = BTreeMap::new();
    payload.insert("text".to_string(), text.to_string());
    Document::new(id, vector).with_payload(payload)
}

/// Read a single text payload field. (The `vector` field is binary, so a
/// full `HGETALL` into `String`s would fail to decode — fetch just `field`.)
async fn hget(uri: &str, key: &str, field: &str) -> Option<String> {
    let client = redis::Client::open(uri).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    redis::cmd("HGET")
        .arg(key)
        .arg(field)
        .query_async(&mut conn)
        .await
        .unwrap()
}

async fn key_exists(uri: &str, key: &str) -> bool {
    let client = redis::Client::open(uri).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    redis::cmd("EXISTS")
        .arg(key)
        .query_async::<i64>(&mut conn)
        .await
        .unwrap()
        == 1
}

/// The number of documents under the index's key prefix. Counted by key, not
/// with `FT.SEARCH idx *`, which valkey-search rejects.
async fn doc_count(uri: &str, index: &str) -> usize {
    let client = redis::Client::open(uri).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("{index}:*"))
        .query_async(&mut conn)
        .await
        .unwrap();
    keys.len()
}

async fn index_exists(uri: &str, index: &str) -> bool {
    let client = redis::Client::open(uri).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let names: Vec<String> = redis::cmd("FT._LIST").query_async(&mut conn).await.unwrap();
    names.iter().any(|n| n == index)
}

#[tokio::test]
async fn valkey_index_upsert_update_and_cleanup_when_available() {
    let Some(vk) = try_connect().await else {
        eprintln!("skipping live Valkey target test; VALKEY_URI is not set or unavailable");
        return;
    };
    let uri = std::env::var("VALKEY_URI").unwrap();
    let index_name = format!("idx_{}", nonce());

    let tmp = tempfile::tempdir().unwrap();
    let app = Environment::builder()
        .db_path(tmp.path().join("db"))
        .provide_key(&VK, vk.clone())
        .build()
        .await
        .unwrap()
        .app("ValkeyTargetE2ETest")
        .await
        .unwrap();

    // Run 1: create the index and declare 2 documents.
    app.run({
        let index_name = index_name.clone();
        move |ctx| {
            let index_name = index_name.clone();
            async move {
                let index =
                    valkey::mount_index_target(&ctx, &VK, index_name, schema().await).await?;
                index.declare_document(&ctx, doc("d1", vec![0.1, 0.2, 0.3], "hello"))?;
                index.declare_document(&ctx, doc("d2", vec![0.4, 0.5, 0.6], "world"))?;
                // A numeric payload value for the NUMERIC field.
                index.declare_document(
                    &ctx,
                    Document::new("d3", vec![0.7, 0.8, 0.9]).with_payload([
                        ("text", PayloadValue::from("priced")),
                        ("price", PayloadValue::from(42i64)),
                    ]),
                )?;
                Ok(())
            }
        }
    })
    .await
    .expect("creating a Valkey index + documents should succeed");

    assert!(index_exists(&uri, &index_name).await);
    assert_eq!(doc_count(&uri, &index_name).await, 3);
    assert_eq!(
        hget(&uri, &format!("{index_name}:d1"), "text")
            .await
            .as_deref(),
        Some("hello")
    );
    assert_eq!(
        hget(&uri, &format!("{index_name}:d3"), "price")
            .await
            .as_deref(),
        Some("42"),
        "a numeric payload value is stored as its text form"
    );

    // Run 2: update d1's payload, drop d2 and d3.
    app.run({
        let index_name = index_name.clone();
        move |ctx| {
            let index_name = index_name.clone();
            async move {
                let index =
                    valkey::mount_index_target(&ctx, &VK, index_name, schema().await).await?;
                index.declare_document(&ctx, doc("d1", vec![0.1, 0.2, 0.3], "updated"))?;
                Ok(())
            }
        }
    })
    .await
    .expect("updating a Valkey document should succeed");

    assert_eq!(doc_count(&uri, &index_name).await, 1);
    assert_eq!(
        hget(&uri, &format!("{index_name}:d1"), "text")
            .await
            .as_deref(),
        Some("updated")
    );
    assert!(!key_exists(&uri, &format!("{index_name}:d2")).await);

    // Run 3: build the index target state but don't declare it → the orphaned
    // system-managed index is dropped and its docs purged.
    app.run({
        let index_name = index_name.clone();
        move |ctx| {
            let index_name = index_name.clone();
            async move {
                let _ = valkey::index_target(&ctx, &VK, index_name, schema().await)?;
                Ok(())
            }
        }
    })
    .await
    .expect("dropping the orphaned Valkey index should succeed");

    assert!(!index_exists(&uri, &index_name).await);
    assert!(!key_exists(&uri, &format!("{index_name}:d1")).await);
}

#[tokio::test]
async fn valkey_undeclared_and_dropped_indexes_are_removed_when_available() {
    let Some(vk) = try_connect().await else {
        eprintln!("skipping live Valkey index-removal test; VALKEY_URI is not set or unavailable");
        return;
    };
    let uri = std::env::var("VALKEY_URI").unwrap();
    let index_name = format!("idx_removal_{}", nonce());
    let doc_key = format!("{index_name}:d1");

    let tmp = tempfile::tempdir().unwrap();
    let app = Environment::builder()
        .db_path(tmp.path().join("db"))
        .provide_key(&VK, vk.clone())
        .build()
        .await
        .unwrap()
        .app("ValkeyIndexRemovalE2ETest")
        .await
        .unwrap();
    let declare = |index_name: String| {
        move |ctx: cocoindex::Ctx| async move {
            let index = valkey::mount_index_target(&ctx, &VK, index_name, schema().await).await?;
            index.declare_document(&ctx, doc("d1", vec![0.1, 0.2, 0.3], "hello"))?;
            Ok::<_, cocoindex::Error>(())
        }
    };

    app.run(declare(index_name.clone())).await.unwrap();
    assert!(index_exists(&uri, &index_name).await);
    assert!(key_exists(&uri, &doc_key).await);

    // A run that no longer declares the index drops it and purges its docs.
    app.run(|_ctx| async { Ok(()) }).await.unwrap();
    assert!(
        !index_exists(&uri, &index_name).await,
        "an index no longer declared is dropped"
    );
    assert!(!key_exists(&uri, &doc_key).await);

    app.run(declare(index_name.clone())).await.unwrap();
    assert!(index_exists(&uri, &index_name).await);
    assert!(key_exists(&uri, &doc_key).await);

    app.drop_state().await.unwrap();
    assert!(
        !index_exists(&uri, &index_name).await,
        "drop_state drops the index"
    );
    assert!(!key_exists(&uri, &doc_key).await);
}

/// A user-managed index is never created, dropped or recreated by CocoIndex:
/// only its documents are. The test creates the index itself, as the user
/// would, and checks it survives both an undeclaring run and `drop_state`.
#[tokio::test]
async fn valkey_user_managed_index_is_left_in_place_when_available() {
    let Some(vk) = try_connect().await else {
        eprintln!("skipping live Valkey user-managed test; VALKEY_URI is not set or unavailable");
        return;
    };
    let uri = std::env::var("VALKEY_URI").unwrap();
    let index_name = format!("idx_user_{}", nonce());
    let doc_key = format!("{index_name}:d1");

    // The user creates the index.
    let client = redis::Client::open(uri.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let mut create = redis::cmd("FT.CREATE");
    create.arg(&index_name);
    for word in ["ON", "HASH", "PREFIX", "1"] {
        create.arg(word);
    }
    create.arg(format!("{index_name}:"));
    for word in [
        "SCHEMA",
        "vector",
        "VECTOR",
        "HNSW",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "3",
        "DISTANCE_METRIC",
        "COSINE",
        "text",
        "TEXT",
        "price",
        "NUMERIC",
    ] {
        create.arg(word);
    }
    let _: redis::Value = create.query_async(&mut conn).await.unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let app = Environment::builder()
        .db_path(tmp.path().join("db"))
        .provide_key(&VK, vk.clone())
        .build()
        .await
        .unwrap()
        .app("ValkeyUserManagedE2ETest")
        .await
        .unwrap();
    let declare = |index_name: String| {
        move |ctx: cocoindex::Ctx| async move {
            let index = valkey::mount_index_target_with_options(
                &ctx,
                &VK,
                index_name,
                schema().await,
                ManagedTargetOptions::user_managed(),
            )
            .await?;
            index.declare_document(&ctx, doc("d1", vec![0.1, 0.2, 0.3], "hello"))?;
            Ok::<_, cocoindex::Error>(())
        }
    };

    app.run(declare(index_name.clone())).await.unwrap();
    assert!(key_exists(&uri, &doc_key).await, "documents are written");

    app.run(|_ctx| async { Ok(()) }).await.unwrap();
    assert!(
        index_exists(&uri, &index_name).await,
        "a user-managed index no longer declared is left in place"
    );
    // As with the other connectors' user-managed containers (see the LanceDB
    // test `lancedb_user_managed_target_does_not_create_table`), the user owns
    // what is in the index too: undeclaring it leaves its documents.
    assert!(
        key_exists(&uri, &doc_key).await,
        "its documents are left in place too"
    );

    app.run(declare(index_name.clone())).await.unwrap();
    app.drop_state().await.unwrap();
    assert!(
        index_exists(&uri, &index_name).await,
        "drop_state leaves a user-managed index in place"
    );
    assert!(key_exists(&uri, &doc_key).await, "and its documents");

    let _: redis::Value = redis::cmd("DEL")
        .arg(&doc_key)
        .query_async(&mut conn)
        .await
        .unwrap();

    let _: redis::Value = redis::cmd("FT.DROPINDEX")
        .arg(&index_name)
        .query_async(&mut conn)
        .await
        .unwrap();
}

/// An index that exists without a tracking record (e.g. state from an earlier
/// version that keyed target states differently) is adopted, not re-created.
#[tokio::test]
async fn valkey_adopts_untracked_existing_index_when_available() {
    let Some(vk) = try_connect().await else {
        eprintln!("skipping live Valkey index-adoption test; VALKEY_URI is not set or unavailable");
        return;
    };
    let uri = std::env::var("VALKEY_URI").unwrap();
    let index_name = format!("idx_adopt_{}", nonce());
    let declare = |index_name: String| {
        move |ctx: cocoindex::Ctx| async move {
            let index = valkey::mount_index_target(&ctx, &VK, index_name, schema().await).await?;
            index.declare_document(&ctx, doc("d1", vec![0.1, 0.2, 0.3], "hello"))?;
            Ok::<_, cocoindex::Error>(())
        }
    };
    let new_app = |db: std::path::PathBuf| {
        let vk = vk.clone();
        async move {
            Environment::builder()
                .db_path(db)
                .provide_key(&VK, vk)
                .build()
                .await
                .unwrap()
                .app("ValkeyIndexAdoptionE2ETest")
                .await
                .unwrap()
        }
    };

    let tmp = tempfile::tempdir().unwrap();
    let first = new_app(tmp.path().join("first")).await;
    first.run(declare(index_name.clone())).await.unwrap();
    assert!(index_exists(&uri, &index_name).await);

    // A fresh state db has no record of the index.
    let second = new_app(tmp.path().join("second")).await;
    second
        .run(declare(index_name.clone()))
        .await
        .expect("declaring an index that already exists should adopt it");
    assert!(index_exists(&uri, &index_name).await);

    second.drop_state().await.unwrap();
    assert!(!index_exists(&uri, &index_name).await);
}

/// A connection made from a `ClientConfig`, and one made from a caller-built
/// `redis::Client`, both carry a target: the first is named on the server, and
/// each writes the documents declared through it.
#[tokio::test]
async fn valkey_connects_with_a_client_config_and_a_caller_built_client_when_available() {
    if try_connect().await.is_none() {
        eprintln!("skipping live Valkey connection test; VALKEY_URI is not set or unavailable");
        return;
    }
    let uri = std::env::var("VALKEY_URI").unwrap();
    let info: redis::ConnectionInfo = uri.parse().unwrap();
    let redis::ConnectionAddr::Tcp(host, port) = info.addr.clone() else {
        eprintln!("skipping live Valkey connection test; VALKEY_URI is not a plain TCP address");
        return;
    };

    let client_name = format!("coco_test_{}", nonce());
    let from_config = valkey::Valkey::connect_with_config(valkey::ClientConfig {
        host: host.clone(),
        port,
        password: info.redis.password.clone(),
        client_name: Some(client_name.clone()),
        ..valkey::ClientConfig::default()
    })
    .await
    .expect("connecting with a ClientConfig should succeed");
    assert_eq!(from_config.state_id(), format!("redis://{host}:{port}"));

    let observer = redis::Client::open(uri.as_str()).unwrap();
    let mut conn = observer.get_multiplexed_async_connection().await.unwrap();
    let clients: String = redis::cmd("CLIENT")
        .arg("LIST")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(
        clients.contains(&format!("name={client_name}")),
        "the configured client name is set on the connection: {clients}"
    );

    let from_client = valkey::Valkey::from_client("valkey-under-test", &observer)
        .await
        .expect("wrapping a redis::Client should succeed");
    assert_eq!(from_client.state_id(), "valkey-under-test");

    for (label, vk) in [("config", from_config), ("client", from_client)] {
        let index_name = format!("idx_{label}_{}", nonce());
        let tmp = tempfile::tempdir().unwrap();
        let app = Environment::builder()
            .db_path(tmp.path().join("db"))
            .provide_key(&VK, vk)
            .build()
            .await
            .unwrap()
            .app("ValkeyConnectionTest")
            .await
            .unwrap();
        app.run({
            let index_name = index_name.clone();
            move |ctx| {
                let index_name = index_name.clone();
                async move {
                    let index =
                        valkey::mount_index_target(&ctx, &VK, index_name, schema().await).await?;
                    index.declare_document(&ctx, doc("d1", vec![0.1, 0.2, 0.3], "hello"))?;
                    Ok(())
                }
            }
        })
        .await
        .expect("declaring a document over the connection should succeed");
        assert_eq!(
            hget(&uri, &format!("{index_name}:d1"), "text")
                .await
                .as_deref(),
            Some("hello"),
            "connection made from a {label}"
        );
        app.drop_state().await.unwrap();
        assert!(!index_exists(&uri, &index_name).await);
    }
}

// ---------------------------------------------------------------------------
// Battle tests: malformed vectors, doc ids, payload values, schema changes,
// out-of-band drops, batch sizes, handles as mount arguments.
// ---------------------------------------------------------------------------

use std::sync::Arc;

use cocoindex::connectors::valkey::IndexTarget;
use cocoindex::{Ctx, Result, UpdateOptions};

type Declare = Arc<dyn Fn(&Ctx, &IndexTarget) -> Result<()> + Send + Sync>;

/// One live index with its own CocoIndex app.
struct Live {
    uri: String,
    index: String,
    app: cocoindex::App,
    _dir: tempfile::TempDir,
}

impl Live {
    async fn new(tag: &str) -> Option<Self> {
        let Some(vk) = try_connect().await else {
            eprintln!("skipping live Valkey test; VALKEY_URI is not set or unavailable");
            return None;
        };
        let dir = tempfile::tempdir().unwrap();
        let app = Environment::builder()
            .db_path(dir.path().join("db"))
            .provide_key(&VK, vk)
            .build()
            .await
            .unwrap()
            .app(&format!("ValkeyBattle_{tag}"))
            .await
            .unwrap();
        Some(Self {
            uri: std::env::var("VALKEY_URI").unwrap(),
            index: format!("idx_{tag}_{}", nonce()),
            app,
            _dir: dir,
        })
    }

    async fn run_with(
        &self,
        options: UpdateOptions,
        schema: IndexSchema,
        declare: Declare,
    ) -> Result<()> {
        let index = self.index.clone();
        self.app
            .update_with_options(options, move |ctx| async move {
                let target = valkey::mount_index_target(&ctx, &VK, index, schema).await?;
                declare(&ctx, &target)
            })
            .await
    }

    async fn run(&self, schema: IndexSchema, declare: Declare) -> Result<()> {
        self.run_with(UpdateOptions::default(), schema, declare)
            .await
    }

    async fn conn(&self) -> redis::aio::MultiplexedConnection {
        redis::Client::open(self.uri.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap()
    }

    /// `(num_docs, hash_indexing_failures)` from `FT.INFO`.
    async fn indexed(&self) -> (i64, i64) {
        let info: Vec<redis::Value> = redis::cmd("FT.INFO")
            .arg(&self.index)
            .query_async(&mut self.conn().await)
            .await
            .unwrap();
        let mut num_docs = -1;
        let mut failures = -1;
        for pair in info.chunks(2) {
            let Ok(key) = redis::from_redis_value::<String>(&pair[0]) else {
                continue;
            };
            let number = || {
                let text: String = redis::from_redis_value(&pair[1]).unwrap();
                text.parse::<f64>().unwrap() as i64
            };
            match key.as_str() {
                "num_docs" => num_docs = number(),
                "hash_indexing_failures" => failures = number(),
                _ => {}
            }
        }
        (num_docs, failures)
    }

    async fn drop_index_out_of_band(&self) {
        let _: redis::Value = redis::cmd("FT.DROPINDEX")
            .arg(&self.index)
            .query_async(&mut self.conn().await)
            .await
            .unwrap();
    }

    async fn cleanup(self) {
        let _: redis::RedisResult<redis::Value> = redis::cmd("FT.DROPINDEX")
            .arg(&self.index)
            .arg("DD")
            .query_async(&mut self.conn().await)
            .await;
    }
}

async fn schema_with(dim: usize, fields: Vec<FieldDef>) -> IndexSchema {
    IndexSchema::create(
        VectorDef {
            schema: &VectorSchema::f32(dim),
            distance: Distance::Cosine,
            algorithm: VectorAlgorithm::Hnsw,
        },
        fields,
    )
    .await
    .unwrap()
}

fn n_docs(n: usize, tag: &'static str) -> Declare {
    Arc::new(move |ctx, target| {
        for i in 0..n {
            target.declare_document(
                ctx,
                doc(&format!("d{i}"), vec![1.0, (i % 13) as f32, 0.5], tag),
            )?;
        }
        Ok(())
    })
}

/// A vector of the wrong length, an empty one, or one with a NaN/Inf fails at
/// declare time: the search module would store the hash but never index it.
#[tokio::test]
async fn valkey_malformed_vectors_fail_at_declare_when_available() {
    let Some(live) = Live::new("badvec").await else {
        return;
    };
    for (name, v) in [
        ("short", vec![1.0, 0.0]),
        ("long", vec![1.0, 0.0, 0.0, 0.0]),
        ("empty", vec![]),
        ("nan", vec![f32::NAN, 0.0, 0.0]),
        ("inf", vec![0.0, f32::NEG_INFINITY, 0.0]),
    ] {
        let err = live
            .run(
                schema().await,
                Arc::new(move |ctx, target| {
                    target.declare_document(ctx, doc("ok", vec![1.0, 0.0, 0.0], "ok"))?;
                    target.declare_document(ctx, doc("bad", v.clone(), "bad"))
                }),
            )
            .await
            .expect_err(name);
        assert!(err.to_string().contains("\"bad\""), "{name}: {err}");
    }
    live.run(schema().await, n_docs(3, "v1")).await.unwrap();
    assert_eq!(
        live.indexed().await,
        (3, 0),
        "every stored document is indexed"
    );
    live.cleanup().await;
}

/// Document ids are restricted to `[A-Za-z0-9_-]+` (as in Python): an id with
/// a separator, a space, a glob character or non-ASCII text is rejected.
#[tokio::test]
async fn valkey_document_ids_are_validated_when_available() {
    let Some(live) = Live::new("ids").await else {
        return;
    };
    live.run(
        schema().await,
        Arc::new(|ctx, target| {
            for id in ["a", "A-b_9", "0", "x".repeat(512).as_str()] {
                target.declare_document(ctx, doc(id, vec![1.0, 0.0, 0.0], id))?;
            }
            Ok(())
        }),
    )
    .await
    .unwrap();
    assert_eq!(live.indexed().await.0, 4);
    for bad in ["", "a:b", "a b", "a*", "ü", "a/b", "{a}"] {
        let err = live
            .run(
                schema().await,
                Arc::new(move |ctx, target| {
                    target.declare_document(ctx, doc(bad, vec![1.0, 0.0, 0.0], "bad"))
                }),
            )
            .await
            .expect_err(bad);
        assert!(err.to_string().contains("doc_id"), "{bad}: {err}");
    }
    live.cleanup().await;
}

/// Payload values of each kind are stored as their text form, a type change
/// of an unchanged-looking value (`1` vs `"1"`) is a change, and a large
/// payload is stored whole.
#[tokio::test]
async fn valkey_payload_values_are_stored_as_text_when_available() {
    let Some(live) = Live::new("payload").await else {
        return;
    };
    let big = "y".repeat(2 * 1024 * 1024);
    let declare = |price: PayloadValue, big: String| -> Declare {
        Arc::new(move |ctx, target| {
            target.declare_document(
                ctx,
                Document::new("p", vec![1.0, 0.0, 0.0]).with_payload([
                    ("text", PayloadValue::from("héllo 🙂 \"q\" \n")),
                    ("price", price.clone()),
                    ("neg", PayloadValue::from(i64::MIN)),
                    ("float", PayloadValue::from(0.1f64)),
                    ("empty", PayloadValue::from("")),
                    ("big", PayloadValue::from(big.clone())),
                ]),
            )
        })
    };
    live.run(
        schema().await,
        declare(PayloadValue::from(1i64), big.clone()),
    )
    .await
    .unwrap();
    let key = format!("{}:p", live.index);
    assert_eq!(
        hget(&live.uri, &key, "text").await.as_deref(),
        Some("héllo 🙂 \"q\" \n")
    );
    assert_eq!(hget(&live.uri, &key, "price").await.as_deref(), Some("1"));
    assert_eq!(
        hget(&live.uri, &key, "neg").await.as_deref(),
        Some("-9223372036854775808")
    );
    assert_eq!(hget(&live.uri, &key, "float").await.as_deref(), Some("0.1"));
    assert_eq!(hget(&live.uri, &key, "empty").await.as_deref(), Some(""));
    assert_eq!(
        hget(&live.uri, &key, "big").await.map(|s| s.len()),
        Some(big.len())
    );

    // `1.5` replaces `1`: the document is rewritten.
    live.run(schema().await, declare(PayloadValue::from(1.5f64), big))
        .await
        .unwrap();
    assert_eq!(hget(&live.uri, &key, "price").await.as_deref(), Some("1.5"));
    assert_eq!(live.indexed().await, (1, 0));
    live.cleanup().await;
}

/// Changing the dimension or the indexed fields recreates the index and
/// writes its documents again; an unchanged schema keeps it.
#[tokio::test]
async fn valkey_index_schema_change_recreates_and_refills_when_available() {
    let Some(live) = Live::new("schema").await else {
        return;
    };
    live.run(schema().await, n_docs(5, "v1")).await.unwrap();
    assert_eq!(live.indexed().await, (5, 0));

    // A new TAG field: the index is rebuilt, the documents written again.
    let with_tag = || async {
        schema_with(
            3,
            vec![
                FieldDef::new("text", FieldType::Text),
                FieldDef::new("price", FieldType::Numeric),
                FieldDef::new("kind", FieldType::Tag).sortable(),
            ],
        )
        .await
    };
    live.run(with_tag().await, n_docs(5, "v1")).await.unwrap();
    assert_eq!(live.indexed().await, (5, 0));

    // Dimension 3 -> 4: every document carries the new dimension.
    live.run(
        schema_with(4, vec![FieldDef::new("text", FieldType::Text)]).await,
        Arc::new(|ctx, target| {
            for i in 0..4 {
                target.declare_document(
                    ctx,
                    doc(&format!("d{i}"), vec![1.0, 0.0, 0.0, i as f32], "v2"),
                )?;
            }
            Ok(())
        }),
    )
    .await
    .unwrap();
    assert_eq!(live.indexed().await, (4, 0), "old documents are purged");
    assert_eq!(doc_count(&live.uri, &live.index).await, 4);
    live.cleanup().await;
}

/// An index dropped behind CocoIndex's back leaves its documents unindexed
/// until a full reprocess, which recreates it over the stored hashes.
#[tokio::test]
async fn valkey_index_dropped_out_of_band_is_restored_by_full_reprocess_when_available() {
    let Some(live) = Live::new("oob").await else {
        return;
    };
    live.run(schema().await, n_docs(4, "v1")).await.unwrap();
    live.drop_index_out_of_band().await;
    assert!(!index_exists(&live.uri, &live.index).await);
    live.run_with(
        UpdateOptions {
            full_reprocess: true,
            ..Default::default()
        },
        schema().await,
        n_docs(4, "v1"),
    )
    .await
    .unwrap();
    assert!(index_exists(&live.uri, &live.index).await);
    // The search module indexes existing hashes in the background.
    for _ in 0..100 {
        if live.indexed().await.0 == 4 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(live.indexed().await, (4, 0));
    live.cleanup().await;
}

/// Batches on both sides of the 1000-document pipeline chunk, growing and
/// shrinking.
#[tokio::test]
async fn valkey_batches_around_pipeline_chunks_when_available() {
    let Some(live) = Live::new("batch").await else {
        return;
    };
    for n in [999usize, 1000, 1001, 2500, 500, 0, 1001] {
        live.run(schema().await, n_docs(n, "v1")).await.unwrap();
        assert_eq!(doc_count(&live.uri, &live.index).await, n, "{n} documents");
        assert_eq!(live.indexed().await, (n as i64, 0), "{n} documents indexed");
    }
    live.run(schema().await, n_docs(1001, "v2")).await.unwrap();
    assert_eq!(
        hget(&live.uri, &format!("{}:d1000", live.index), "text")
            .await
            .as_deref(),
        Some("v2")
    );
    live.cleanup().await;
}

/// A very large batch: 50k documents in one run, then all deleted. Slow; run
/// with
///   VALKEY_URI=redis://localhost:6379 cargo test -p cocoindex --features valkey \
///     --test valkey_target valkey_very_large_batch -- --ignored
#[tokio::test]
#[ignore]
async fn valkey_very_large_batch_when_available() {
    let Some(live) = Live::new("huge").await else {
        return;
    };
    let start = std::time::Instant::now();
    live.run(schema().await, n_docs(50_000, "v1"))
        .await
        .unwrap();
    eprintln!("wrote 50k documents in {:?}", start.elapsed());
    assert_eq!(doc_count(&live.uri, &live.index).await, 50_000);
    let start = std::time::Instant::now();
    live.run(schema().await, n_docs(0, "v1")).await.unwrap();
    eprintln!("deleted 50k documents in {:?}", start.elapsed());
    assert_eq!(doc_count(&live.uri, &live.index).await, 0);
    live.cleanup().await;
}

static VALKEY_ARG_CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[cocoindex::function]
async fn declare_valkey_doc_in_component(ctx: &Ctx, id: String, index: IndexTarget) -> Result<()> {
    VALKEY_ARG_CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    index.declare_document(ctx, doc(&id, vec![1.0, 0.0, 0.0], "arg"))
}

/// An index handle is a `mount_each!` argument, keyed by its index.
#[tokio::test]
async fn valkey_index_target_is_a_mount_argument_when_available() {
    let Some(live) = Live::new("arg").await else {
        return;
    };
    let calls = || VALKEY_ARG_CALLS.load(std::sync::atomic::Ordering::SeqCst);
    for round in 0..2 {
        let index = live.index.clone();
        live.app
            .run(move |ctx| async move {
                let target = valkey::mount_index_target(&ctx, &VK, index, schema().await).await?;
                cocoindex::use_mount_each!(
                    vec![
                        ("a".to_string(), "a".to_string()),
                        ("b".to_string(), "b".to_string())
                    ],
                    |id| declare_valkey_doc_in_component(ctx, id, target.clone())
                )
                .await?;
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(calls(), 2, "round {round}: unchanged children are skipped");
        assert_eq!(doc_count(&live.uri, &live.index).await, 2);
    }
    live.cleanup().await;
}

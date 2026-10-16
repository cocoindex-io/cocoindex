//! Live Valkey (RediSearch) target e2e. Skips when no server is reachable. Run:
//!   VALKEY_URI=redis://localhost:6379 \
//!     cargo test -p cocoindex --features valkey --test valkey_target
#![cfg(feature = "valkey")]

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use cocoindex::Environment;
use cocoindex::connectors::valkey::{
    self, Distance, Document, FieldDef, FieldType, IndexSchema, VectorAlgorithm, VectorDef,
};
use cocoindex::resources::schema::VectorSchema;

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
        vec![FieldDef::new("text", FieldType::Text)],
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
                Ok(())
            }
        }
    })
    .await
    .expect("creating a Valkey index + documents should succeed");

    assert!(index_exists(&uri, &index_name).await);
    assert_eq!(doc_count(&uri, &index_name).await, 2);
    assert_eq!(
        hget(&uri, &format!("{index_name}:d1"), "text")
            .await
            .as_deref(),
        Some("hello")
    );

    // Run 2: update d1's payload, drop d2.
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

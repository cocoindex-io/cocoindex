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

cocoindex::context_key!(static VK: valkey::Valkey = "valkey_target_conn");

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

async fn ft_search_count(uri: &str, index: &str) -> i64 {
    let client = redis::Client::open(uri).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    // `FT.SEARCH idx * LIMIT 0 0` returns the total doc count as the first reply.
    let value: redis::Value = redis::cmd("FT.SEARCH")
        .arg(index)
        .arg("*")
        .arg("LIMIT")
        .arg(0)
        .arg(0)
        .query_async(&mut conn)
        .await
        .unwrap();
    match value {
        redis::Value::Array(items) => match items.first() {
            Some(redis::Value::Int(n)) => *n,
            _ => -1,
        },
        _ => -1,
    }
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
    assert_eq!(ft_search_count(&uri, &index_name).await, 2);
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

    assert_eq!(ft_search_count(&uri, &index_name).await, 1);
    assert_eq!(
        hget(&uri, &format!("{index_name}:d1"), "text")
            .await
            .as_deref(),
        Some("updated")
    );
    assert!(!key_exists(&uri, &format!("{index_name}:d2")).await);

    // Run 3: re-register the provider but declare nothing on it → the orphaned
    // system-managed index is dropped and its docs purged (handler present,
    // index state absent).
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

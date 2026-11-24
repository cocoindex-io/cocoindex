//! The number of components an app runs at once: 1024 by default, as in
//! Python, or the builder's `max_inflight_components`. The environment
//! variable is covered by `admission_env.rs`, which owns its process.
//!
//!   cargo test -p cocoindex --test admission

mod admission_common;

use admission_common::assert_admits;
use cocoindex::Environment;

#[tokio::test]
async fn default_limit_is_1024_components() {
    let dir = tempfile::tempdir().unwrap();
    let app = Environment::builder()
        .db_path(dir.path().join(".cocoindex_db"))
        .build()
        .await
        .unwrap()
        .app("admission_default")
        .await
        .unwrap();
    assert_admits(&app, 1024, 1030).await;
}

#[tokio::test]
async fn builder_limit_bounds_components() {
    let dir = tempfile::tempdir().unwrap();
    let app = Environment::builder()
        .db_path(dir.path().join(".cocoindex_db"))
        .max_inflight_components(3)
        .build()
        .await
        .unwrap()
        .app("admission_builder")
        .await
        .unwrap();
    assert_admits(&app, 3, 10).await;
}

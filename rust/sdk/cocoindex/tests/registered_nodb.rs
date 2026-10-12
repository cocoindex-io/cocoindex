//! A binary with no `#[cocoindex::lifespan]`: `Environment::from_registered`
//! falls back to `COCOINDEX_DB` (and the LMDB variables) and errors clearly
//! when the db path is unset too.

use cocoindex::Environment;

#[tokio::test]
async fn from_registered_without_lifespan() {
    assert!(cocoindex::registry::lifespans().is_empty());
    assert!(cocoindex::registry::apps().is_empty());

    // SAFETY: single test in this binary; nothing else reads the environment.
    unsafe { std::env::remove_var("COCOINDEX_DB") };
    let err = Environment::from_registered()
        .await
        .err()
        .expect("no db path configured");
    let message = err.to_string();
    assert!(message.contains("COCOINDEX_DB"), "{message}");
    assert!(message.contains("#[cocoindex::lifespan]"), "{message}");

    let dir = tempfile::tempdir().unwrap();
    unsafe { std::env::set_var("COCOINDEX_DB", dir.path().join("db")) };

    // LMDB settings come from the environment too, and are validated.
    unsafe { std::env::set_var("COCOINDEX_LMDB_MAX_DBS", "lots") };
    let err = Environment::from_registered()
        .await
        .err()
        .expect("invalid integer");
    assert!(err.to_string().contains("COCOINDEX_LMDB_MAX_DBS"), "{err}");
    unsafe { std::env::set_var("COCOINDEX_LMDB_MAX_DBS", "64") };
    let env = Environment::from_registered().await.unwrap();
    assert_eq!(env.db_path(), dir.path().join("db"));
    env.shutdown().await;
}

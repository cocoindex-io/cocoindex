//! A binary with no `#[cocoindex::lifespan]`: `Environment::from_registered`
//! falls back to `COCOINDEX_DB` and errors clearly when that is unset too.

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
    let env = Environment::from_registered().await.unwrap();
    assert_eq!(env.db_path(), dir.path().join("db"));
    env.shutdown().await;
}

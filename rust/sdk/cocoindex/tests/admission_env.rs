//! `COCOINDEX_MAX_INFLIGHT_COMPONENTS` sets the number of components an app
//! runs at once when the builder sets none, read when the app is created, as
//! in Python. One test, alone in its process: it sets the variable.
//!
//!   cargo test -p cocoindex --test admission_env

mod admission_common;

use admission_common::assert_admits;
use cocoindex::Environment;

#[tokio::test]
async fn environment_variable_sets_the_limit() {
    // SAFETY: this is the only test in this binary, so no other thread reads
    // the environment while it is written.
    unsafe { std::env::set_var("COCOINDEX_MAX_INFLIGHT_COMPONENTS", "lots") };
    let dir = tempfile::tempdir().unwrap();
    let env = Environment::builder()
        .db_path(dir.path().join(".cocoindex_db"))
        .build()
        .await
        .unwrap();
    let Err(err) = env.app("admission_env_bad").await else {
        panic!("an unparsable limit must be an error");
    };
    assert!(
        err.to_string()
            .contains("COCOINDEX_MAX_INFLIGHT_COMPONENTS"),
        "{err}"
    );

    unsafe { std::env::set_var("COCOINDEX_MAX_INFLIGHT_COMPONENTS", "5") };
    let app = env.app("admission_env").await.unwrap();
    assert_admits(&app, 5, 12).await;

    // The builder's value wins over the variable.
    let dir = tempfile::tempdir().unwrap();
    let app = Environment::builder()
        .db_path(dir.path().join(".cocoindex_db"))
        .max_inflight_components(2)
        .build()
        .await
        .unwrap()
        .app("admission_env_builder")
        .await
        .unwrap();
    assert_admits(&app, 2, 12).await;
}

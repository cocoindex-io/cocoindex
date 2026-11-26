//! Integration tests for `Ctx::use_state` persistent per-component state.

use std::path::Path;

use cocoindex::App;

/// Build an app over a fixed LMDB directory, so successive builds share state.
fn app_at(name: &str, db_path: &Path) -> App {
    App::builder(name)
        .db_path(db_path)
        .build_blocking()
        .unwrap()
}

#[test]
fn use_state_persists_across_runs() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("lmdb");

    // Run 1: no stored state yet — the handle holds the initial value.
    let app = app_at("use_state_persist", &db_path);
    app.update_blocking(|ctx| async move {
        let mut counter = ctx.use_state("counter", 0i64)?;
        assert_eq!(*counter.value(), 0);
        counter.set(*counter.value() + 1)?;
        assert_eq!(*counter.value(), 1);
        Ok(())
    })
    .unwrap();
    drop(app); // release the LMDB env before reopening the same path

    // Run 2: the value persisted at the end of run 1 is read back.
    let app = app_at("use_state_persist", &db_path);
    app.update_blocking(|ctx| async move {
        let mut counter = ctx.use_state("counter", 0i64)?;
        assert_eq!(*counter.value(), 1);
        counter.set(42)?;
        Ok(())
    })
    .unwrap();
    drop(app);

    // Run 3: a non-default value round-trips too.
    let app = app_at("use_state_persist", &db_path);
    app.update_blocking(|ctx| async move {
        let counter = ctx.use_state("counter", 0i64)?;
        assert_eq!(counter.into_value(), 42);
        Ok(())
    })
    .unwrap();
}

#[test]
fn use_state_duplicate_key_errors() {
    let dir = tempfile::tempdir().unwrap();
    let app = app_at("use_state_dup", &dir.path().join("lmdb"));
    let result = app.update_blocking(|ctx| async move {
        let _a = ctx.use_state("k", 1i64)?;
        // Declaring the same key twice in one run is an error.
        let _b = ctx.use_state("k", 2i64)?;
        Ok(())
    });
    assert!(result.is_err());
}

#[test]
fn use_state_distinct_keys_are_independent() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("lmdb");

    let app = app_at("use_state_multi", &db_path);
    app.update_blocking(|ctx| async move {
        let mut name = ctx.use_state("name", String::new())?;
        let mut count = ctx.use_state("count", 0u32)?;
        name.set("coco".to_string())?;
        count.set(7)?;
        Ok(())
    })
    .unwrap();
    drop(app);

    let app = app_at("use_state_multi", &db_path);
    app.update_blocking(|ctx| async move {
        let name = ctx.use_state("name", String::new())?;
        let count = ctx.use_state("count", 0u32)?;
        assert_eq!(name.value(), "coco");
        assert_eq!(*count.value(), 7);
        Ok(())
    })
    .unwrap();
}

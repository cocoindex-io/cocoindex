//! Integration test for the in-memory `LiveMap` resource.

use cocoindex::{App, LiveMap};

/// Smoke test: `LiveMap::create` mounts its backing container target and
/// `declare_entry` declares entries that sync (run the entry sink) when the
/// component commits — the whole producer-side target machinery runs end to end.
#[test]
fn live_map_create_and_declare() {
    let dir = tempfile::tempdir().unwrap();
    let app = App::builder("live_map_smoke")
        .db_path(dir.path().join("lmdb"))
        .build_blocking()
        .unwrap();

    app.update_blocking(|ctx| async move {
        let lm: LiveMap<String> = LiveMap::create(&ctx).await?;
        lm.declare_entry(&ctx, "a", "alpha".to_string())?;
        lm.declare_entry(&ctx, "b", "beta".to_string())?;
        Ok(())
    })
    .unwrap();
}

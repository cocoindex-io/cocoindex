//! Integration tests for the pipeline: App::update, memo::cached, sync API.

use cocoindex::{App, ContextKey, IdGenerator, UuidGenerator, generate_id, generate_uuid};
use tokio::time::{Duration, sleep};

/// Helper: create an App with a temp LMDB directory (async tests).
async fn temp_app(name: &str) -> (App, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let app = App::builder(name)
        .db_path(dir.path().join("lmdb"))
        .build()
        .await
        .unwrap();
    (app, dir)
}

/// Helper: create an App with a max-inflight component cap.
async fn temp_app_with_max_inflight(name: &str, max_inflight: usize) -> (App, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let app = App::builder(name)
        .db_path(dir.path().join("lmdb"))
        .max_inflight_components(max_inflight)
        .build()
        .await
        .unwrap();
    (app, dir)
}

/// Helper: create an App with a temp LMDB directory (sync tests). Uses
/// the `_blocking` variant so it can be called from `#[test]` functions.
fn temp_app_blocking(name: &str) -> (App, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let app = App::builder(name)
        .db_path(dir.path().join("lmdb"))
        .build_blocking()
        .unwrap();
    (app, dir)
}

// ---------------------------------------------------------------------------
// Sync API: update_blocking
// ---------------------------------------------------------------------------

#[test]
fn update_blocking_runs_closure() {
    let (app, _dir) = temp_app_blocking("sync_basic");
    let result = app.update_blocking(|_ctx| async move { Ok(()) });
    assert!(result.is_ok());
}

#[test]
fn update_blocking_provides_pipeline_context() {
    let (app, _dir) = temp_app_blocking("sync_ctx");
    app.update_blocking(|ctx| async move {
        assert!(ctx.has_pipeline_context());
        Ok(())
    })
    .unwrap();
}

#[test]
fn update_blocking_provide_and_get() {
    #[derive(Debug, Clone, PartialEq)]
    struct Config {
        value: i32,
    }

    let dir = tempfile::tempdir().unwrap();
    let app = App::builder("sync_provide")
        .db_path(dir.path().join("lmdb"))
        .provide(Config { value: 42 })
        .build_blocking()
        .unwrap();

    app.update_blocking(|ctx| async move {
        let config = ctx.get_or_err::<Config>().unwrap();
        assert_eq!(config.value, 42);
        Ok(())
    })
    .unwrap();
}

#[test]
fn update_blocking_missing_context_returns_typed_error() {
    #[derive(Debug)]
    struct MissingConfig;

    let (app, _dir) = temp_app_blocking("sync_missing_context");
    app.update_blocking(|ctx| async move {
        let err = ctx.get_or_err::<MissingConfig>().unwrap_err();
        assert!(
            err.to_string()
                .contains("`pipeline::update_blocking_missing_context_returns_typed_error::MissingConfig` not provided")
        );
        Ok(())
    })
    .unwrap();
}

#[test]
fn update_blocking_returns_main_result() {
    let (app, _dir) = temp_app_blocking("sync_return_value");
    let result = app
        .update_blocking(|_ctx| async move { Ok::<_, cocoindex::Error>("done".to_string()) })
        .unwrap();
    assert_eq!(result, "done");
}

// ---------------------------------------------------------------------------
// Async API: update
// ---------------------------------------------------------------------------

#[tokio::test]
async fn update_async_runs_closure() {
    let (app, _dir) = temp_app("async_basic").await;
    app.update(|_ctx| async move { Ok(()) }).await.unwrap();
}

#[tokio::test]
async fn update_async_provides_pipeline_context() {
    let (app, _dir) = temp_app("async_ctx").await;
    app.update(|ctx| async move {
        assert!(ctx.has_pipeline_context());
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn update_async_returns_main_result() {
    let (app, _dir) = temp_app("async_return_value").await;
    let result = app
        .update(|_ctx| async move { Ok::<_, cocoindex::Error>(42i32) })
        .await
        .unwrap();
    assert_eq!(result, 42);
}

#[tokio::test]
async fn start_update_handle_returns_main_result() {
    let (app, _dir) = temp_app("async_handle_return_value").await;
    let mut handle = app
        .start_update(|_ctx| async move { Ok::<_, cocoindex::Error>(7i32) })
        .unwrap();

    loop {
        if handle.changed().await.unwrap().is_done() {
            break;
        }
    }

    assert_eq!(handle.result().await.unwrap(), 7);
}

#[tokio::test]
async fn start_update_handle_stats_snapshot_after_completion() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let (app, _dir) = temp_app("async_handle_stats_snapshot").await;
    let call_count = Arc::new(AtomicUsize::new(0));

    let count = call_count.clone();
    app.update(|ctx| async move {
        let _: i32 = ctx
            .memo(&"stable", move |_ctx| {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(1)
                }
            })
            .await?;
        Ok(())
    })
    .await
    .unwrap();

    let count = call_count.clone();
    let mut handle = app
        .start_update(|ctx| async move {
            let value: i32 = ctx
                .memo(&"stable", move |_ctx| {
                    let count = count.clone();
                    async move {
                        count.fetch_add(1, Ordering::SeqCst);
                        Ok(2)
                    }
                })
                .await?;
            Ok::<_, cocoindex::Error>(value)
        })
        .unwrap();

    loop {
        if handle.changed().await.unwrap().is_done() {
            break;
        }
    }

    let stats = handle.stats_snapshot();
    assert!(
        stats.processed + stats.skipped + stats.written + stats.deleted > 0,
        "expected completed handle to expose non-empty stats, got {stats:?}"
    );
    assert_eq!(call_count.load(Ordering::SeqCst), 1);
    assert_eq!(handle.result().await.unwrap(), 1);
}

#[tokio::test]
async fn start_update_handle_result_propagates_errors() {
    let (app, _dir) = temp_app("async_handle_error").await;
    let handle = app
        .start_update(|_ctx| async move { Err::<(), _>(cocoindex::Error::engine("handle boom")) })
        .unwrap();

    let err = handle.result().await.unwrap_err().to_string();
    assert!(err.contains("handle boom"), "unexpected error: {err}");
}

#[tokio::test]
async fn start_drop_state_handle_reports_termination() {
    let (app, _dir) = temp_app("drop_handle_termination").await;
    let mut handle = app.start_drop_state().unwrap();

    loop {
        if handle.changed().await.unwrap().is_done() {
            break;
        }
    }

    handle.result().await.unwrap();
}

#[tokio::test]
async fn generate_id_returns_same_id_for_same_dep_across_runs() {
    let (app, _dir) = temp_app("generate_id_stable").await;

    async fn generate(app: &App) -> Vec<u64> {
        app.update(|ctx| async move {
            Ok::<_, cocoindex::Error>(vec![
                generate_id(&ctx, &"A").await?,
                generate_id(&ctx, &"B").await?,
                generate_id(&ctx, &"A").await?,
            ])
        })
        .await
        .unwrap()
    }

    let first = generate(&app).await;
    let second = generate(&app).await;
    assert_eq!(first, second);
    assert_eq!(first[0], first[2]);
    assert_ne!(first[0], first[1]);
}

#[tokio::test]
async fn generate_uuid_returns_same_uuid_for_same_dep_across_runs() {
    let (app, _dir) = temp_app("generate_uuid_stable").await;

    async fn generate(app: &App) -> Vec<uuid::Uuid> {
        app.update(|ctx| async move {
            Ok::<_, cocoindex::Error>(vec![
                generate_uuid(&ctx, &"A").await?,
                generate_uuid(&ctx, &"B").await?,
                generate_uuid(&ctx, &"A").await?,
            ])
        })
        .await
        .unwrap()
    }

    let first = generate(&app).await;
    let second = generate(&app).await;
    assert_eq!(first, second);
    assert_eq!(first[0], first[2]);
    assert_ne!(first[0], first[1]);
}

#[tokio::test]
async fn id_generator_returns_distinct_ids_for_repeated_dep() {
    let (app, _dir) = temp_app("id_generator_distinct_repeated_dep").await;

    let ids = app
        .update(|ctx| async move {
            let mut id_gen = IdGenerator::new();
            let mut ids = Vec::new();
            for _ in 0..5 {
                ids.push(id_gen.next_id(&ctx, &"same").await?);
            }
            Ok::<_, cocoindex::Error>(ids)
        })
        .await
        .unwrap();

    assert_eq!(ids.len(), 5);
    assert_eq!(
        ids.iter().collect::<std::collections::HashSet<_>>().len(),
        5
    );
}

#[tokio::test]
async fn id_generator_is_stable_across_runs() {
    let (app, _dir) = temp_app("id_generator_stable").await;

    async fn generate(app: &App) -> Vec<(String, Vec<u64>)> {
        app.update(|ctx| async move {
            let mut results = Vec::new();
            for dep in ["A", "B"] {
                let mut id_gen = IdGenerator::new();
                let mut ids = Vec::new();
                for _ in 0..3 {
                    ids.push(id_gen.next_id(&ctx, &dep).await?);
                }
                results.push((dep.to_string(), ids));
            }
            Ok::<_, cocoindex::Error>(results)
        })
        .await
        .unwrap()
    }

    let first = generate(&app).await;
    let second = generate(&app).await;
    assert_eq!(first, second);
    assert_eq!(first[0].1.len(), 3);
    assert_eq!(first[1].1.len(), 3);
    assert!(first[0].1.iter().all(|id| !first[1].1.contains(id)));
}

#[tokio::test]
async fn id_generator_constructor_deps_split_sequences() {
    let (app, _dir) = temp_app("id_generator_constructor_deps").await;

    async fn generate(app: &App) -> Vec<(String, Vec<u64>)> {
        app.update(|ctx| async move {
            let mut results = Vec::new();
            for dep in ["A", "B"] {
                let mut id_gen = IdGenerator::with_deps(&dep)?;
                let mut ids = Vec::new();
                for _ in 0..3 {
                    ids.push(id_gen.next_id_default(&ctx).await?);
                }
                results.push((dep.to_string(), ids));
            }
            Ok::<_, cocoindex::Error>(results)
        })
        .await
        .unwrap()
    }

    let first = generate(&app).await;
    let second = generate(&app).await;
    assert_eq!(first, second);
    assert!(first[0].1.iter().all(|id| !first[1].1.contains(id)));
}

#[tokio::test]
async fn uuid_generator_returns_distinct_uuids_for_repeated_dep() {
    let (app, _dir) = temp_app("uuid_generator_distinct_repeated_dep").await;

    let uuids = app
        .update(|ctx| async move {
            let mut uuid_gen = UuidGenerator::new();
            let mut uuids = Vec::new();
            for _ in 0..5 {
                uuids.push(uuid_gen.next_uuid(&ctx, &"same").await?);
            }
            Ok::<_, cocoindex::Error>(uuids)
        })
        .await
        .unwrap();

    assert_eq!(uuids.len(), 5);
    assert_eq!(
        uuids.iter().collect::<std::collections::HashSet<_>>().len(),
        5
    );
}

#[tokio::test]
async fn uuid_generator_is_stable_across_runs() {
    let (app, _dir) = temp_app("uuid_generator_stable").await;

    async fn generate(app: &App) -> Vec<(String, Vec<uuid::Uuid>)> {
        app.update(|ctx| async move {
            let mut results = Vec::new();
            for dep in ["A", "B"] {
                let mut uuid_gen = UuidGenerator::new();
                let mut uuids = Vec::new();
                for _ in 0..3 {
                    uuids.push(uuid_gen.next_uuid(&ctx, &dep).await?);
                }
                results.push((dep.to_string(), uuids));
            }
            Ok::<_, cocoindex::Error>(results)
        })
        .await
        .unwrap()
    }

    let first = generate(&app).await;
    let second = generate(&app).await;
    assert_eq!(first, second);
    assert!(first[0].1.iter().all(|uuid| !first[1].1.contains(uuid)));
}

#[tokio::test]
async fn uuid_generator_constructor_deps_split_sequences() {
    let (app, _dir) = temp_app("uuid_generator_constructor_deps").await;

    async fn generate(app: &App) -> Vec<(String, Vec<uuid::Uuid>)> {
        app.update(|ctx| async move {
            let mut results = Vec::new();
            for dep in ["A", "B"] {
                let mut uuid_gen = UuidGenerator::with_deps(&dep)?;
                let mut uuids = Vec::new();
                for _ in 0..3 {
                    uuids.push(uuid_gen.next_uuid_default(&ctx).await?);
                }
                results.push((dep.to_string(), uuids));
            }
            Ok::<_, cocoindex::Error>(results)
        })
        .await
        .unwrap()
    }

    let first = generate(&app).await;
    let second = generate(&app).await;
    assert_eq!(first, second);
    assert!(first[0].1.iter().all(|uuid| !first[1].1.contains(uuid)));
}

#[tokio::test]
async fn named_context_key_accepts_non_serializable_resource() {
    struct Resource {
        value: i32,
    }

    let key = ContextKey::<Resource>::new("pipeline/non_serializable_resource");
    let dir = tempfile::tempdir().unwrap();
    let app = App::builder("named_context_resource")
        .db_path(dir.path().join("lmdb"))
        .provide_key(&key, Resource { value: 123 })
        .build()
        .await
        .unwrap();

    app.update(move |ctx| async move {
        assert_eq!(ctx.get_key(&key)?.value, 123);
        Ok(())
    })
    .await
    .unwrap();
}

#[test]
fn duplicate_named_context_key_panics() {
    let _first = ContextKey::<i32>::new("pipeline/duplicate_context_key");
    let duplicate = std::panic::catch_unwind(|| {
        let _second = ContextKey::<i32>::new("pipeline/duplicate_context_key");
    });
    assert!(duplicate.is_err());
}

#[test]
fn context_key_exposes_stable_memo_identity_metadata() {
    let key = ContextKey::<i32>::new("pipeline/context_key_identity");
    let detect_change_key =
        ContextKey::<String>::new_detect_change("pipeline/context_key_identity_detect_change");

    assert_eq!(key.name(), "pipeline/context_key_identity");
    assert!(!key.detect_change());
    assert_eq!(
        detect_change_key.name(),
        "pipeline/context_key_identity_detect_change"
    );
    assert!(detect_change_key.detect_change());
}

#[tokio::test]
async fn detect_change_context_key_invalidates_memo() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let key = ContextKey::<String>::new_detect_change("pipeline/detect_change_context");
    let dir = tempfile::tempdir().unwrap();
    let call_count = Arc::new(AtomicUsize::new(0));

    let app = App::builder("context_change_invalidates_memo")
        .db_path(dir.path().join("lmdb"))
        .provide_key(&key, "v1".to_string())
        .build()
        .await
        .unwrap();
    let count = call_count.clone();
    let key_for_first = key.clone();
    app.update(move |ctx| async move {
        let result: String = ctx
            .memo(&"stable", move |ctx| {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(ctx.get_key(&key_for_first)?.clone())
                }
            })
            .await?;
        assert_eq!(result, "v1");
        Ok(())
    })
    .await
    .unwrap();
    drop(app);

    let app = App::builder("context_change_invalidates_memo")
        .db_path(dir.path().join("lmdb"))
        .provide_key(&key, "v2".to_string())
        .build()
        .await
        .unwrap();
    let count = call_count.clone();
    let key_for_second = key.clone();
    app.update(move |ctx| async move {
        let result: String = ctx
            .memo(&"stable", move |ctx| {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(ctx.get_key(&key_for_second)?.clone())
                }
            })
            .await?;
        assert_eq!(result, "v2");
        Ok(())
    })
    .await
    .unwrap();

    assert_eq!(call_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn detect_change_context_key_read_outside_memo_invalidates_component() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let key = ContextKey::<String>::new_detect_change("pipeline/detect_change_component");
    let dir = tempfile::tempdir().unwrap();
    let call_count = Arc::new(AtomicUsize::new(0));

    let app = App::builder("context_change_component")
        .db_path(dir.path().join("lmdb"))
        .provide_key(&key, "v1".to_string())
        .build()
        .await
        .unwrap();
    let count = call_count.clone();
    let key_for_first = key.clone();
    app.update(move |ctx| async move {
        let value = ctx.get_key(&key_for_first)?.clone();
        count.fetch_add(1, Ordering::SeqCst);
        assert_eq!(value, "v1");
        Ok(())
    })
    .await
    .unwrap();
    drop(app);

    let app = App::builder("context_change_component")
        .db_path(dir.path().join("lmdb"))
        .provide_key(&key, "v2".to_string())
        .build()
        .await
        .unwrap();
    let count = call_count.clone();
    let key_for_second = key.clone();
    app.update(move |ctx| async move {
        let value = ctx.get_key(&key_for_second)?.clone();
        count.fetch_add(1, Ordering::SeqCst);
        assert_eq!(value, "v2");
        Ok(())
    })
    .await
    .unwrap();

    assert_eq!(call_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn no_detect_change_context_key_does_not_invalidate_memo() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let key = ContextKey::<String>::new("pipeline/no_detect_change_context");
    let dir = tempfile::tempdir().unwrap();
    let call_count = Arc::new(AtomicUsize::new(0));

    let app = App::builder("context_no_change_does_not_invalidate_memo")
        .db_path(dir.path().join("lmdb"))
        .provide_key(&key, "v1".to_string())
        .build()
        .await
        .unwrap();
    let count = call_count.clone();
    let key_for_first = key.clone();
    app.update(move |ctx| async move {
        let result: String = ctx
            .memo(&"stable", move |ctx| {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(ctx.get_key(&key_for_first)?.clone())
                }
            })
            .await?;
        assert_eq!(result, "v1");
        Ok(())
    })
    .await
    .unwrap();
    drop(app);

    let app = App::builder("context_no_change_does_not_invalidate_memo")
        .db_path(dir.path().join("lmdb"))
        .provide_key(&key, "v2".to_string())
        .build()
        .await
        .unwrap();
    let count = call_count.clone();
    let key_for_second = key.clone();
    app.update(move |ctx| async move {
        let result: String = ctx
            .memo(&"stable", move |ctx| {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(ctx.get_key(&key_for_second)?.clone())
                }
            })
            .await?;
        assert_eq!(result, "v1");
        Ok(())
    })
    .await
    .unwrap();

    assert_eq!(call_count.load(Ordering::SeqCst), 1);
}

/// `ContextKey::new_with_state` drives memo invalidation from a derived state,
/// not the whole value. Here the resource is non-serializable and only its
/// `version` is tracked:
///   - changing a non-state field (`_tag`) must NOT invalidate the memo,
///   - changing the state field (`version`) MUST invalidate it.
#[tokio::test]
async fn state_fn_context_key_invalidates_on_state_change_only() {
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Deliberately NOT `Serialize` — proves new_with_state works for resources
    // that cannot be fingerprinted directly.
    struct Resource {
        version: u64,
        _tag: &'static str,
    }

    static KEY: OnceLock<ContextKey<Resource>> = OnceLock::new();
    let key = KEY
        .get_or_init(|| ContextKey::new_with_state("pipeline/state_fn", |r: &Resource| r.version));

    let dir = tempfile::tempdir().unwrap();
    let call_count = std::sync::Arc::new(AtomicUsize::new(0));

    async fn run(
        path: std::path::PathBuf,
        key: ContextKey<Resource>,
        version: u64,
        tag: &'static str,
        call_count: std::sync::Arc<AtomicUsize>,
    ) -> u64 {
        let app = App::builder("state_fn_context_key")
            .db_path(path)
            .provide_key(&key, Resource { version, _tag: tag })
            .build()
            .await
            .unwrap();
        let count = call_count.clone();
        app.update(move |ctx| async move {
            let v: u64 = ctx
                .memo(&"stable", move |ctx| {
                    let count = count.clone();
                    async move {
                        count.fetch_add(1, Ordering::SeqCst);
                        Ok(ctx.get_key(&key)?.version)
                    }
                })
                .await?;
            Ok::<_, cocoindex::Error>(v)
        })
        .await
        .unwrap()
    }

    let path = dir.path().join("lmdb");

    // Run 1: version=1, tag="a" -> miss (executes).
    assert_eq!(
        run(path.clone(), key.clone(), 1, "a", call_count.clone()).await,
        1
    );
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    // Run 2: version=1, tag="b" -> state (version) unchanged -> cache HIT.
    assert_eq!(
        run(path.clone(), key.clone(), 1, "b", call_count.clone()).await,
        1
    );
    assert_eq!(
        call_count.load(Ordering::SeqCst),
        1,
        "changing a non-state field must not invalidate the memo"
    );

    // Run 3: version=2 -> state changed -> cache MISS (re-executes).
    assert_eq!(run(path, key.clone(), 2, "b", call_count.clone()).await, 2);
    assert_eq!(
        call_count.load(Ordering::SeqCst),
        2,
        "changing the tracked state must invalidate the memo"
    );
}

/// Regression test: two memo bodies running concurrently, each reading a
/// *different* `detect_change` key, must each record their dependency against
/// their own memo entry. A previous implementation tracked the "current"
/// function-call context in a single shared slot, which races under
/// concurrency and misattributes dependencies — so changing one key would
/// fail to invalidate its memo (stale results). Here we change only key A and
/// assert memo A re-runs while memo B stays cached.
#[tokio::test(flavor = "current_thread")]
async fn concurrent_detect_change_keys_invalidate_independently() {
    use std::sync::Arc;
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static KEY_A: OnceLock<ContextKey<String>> = OnceLock::new();
    static KEY_B: OnceLock<ContextKey<String>> = OnceLock::new();
    let key_a = KEY_A.get_or_init(|| ContextKey::new_detect_change("pipeline/concurrent_A"));
    let key_b = KEY_B.get_or_init(|| ContextKey::new_detect_change("pipeline/concurrent_B"));

    let dir = tempfile::tempdir().unwrap();
    let a_calls = Arc::new(AtomicUsize::new(0));
    let b_calls = Arc::new(AtomicUsize::new(0));

    // Drive one full update with the given key values; returns memo A's result.
    async fn drive(
        path: std::path::PathBuf,
        key_a: ContextKey<String>,
        key_b: ContextKey<String>,
        a_val: &'static str,
        b_val: &'static str,
        a_calls: Arc<AtomicUsize>,
        b_calls: Arc<AtomicUsize>,
    ) -> String {
        let app = App::builder("concurrent_detect_change")
            .db_path(path)
            .provide_key(&key_a, a_val.to_string())
            .provide_key(&key_b, b_val.to_string())
            .build()
            .await
            .unwrap();
        app.update(move |ctx| async move {
            let ka = key_a.clone();
            let kb = key_b.clone();
            // memo A yields BEFORE reading key A, so a shared "current" slot
            // would have been overwritten by memo B by the time A reads it.
            let fut_a = ctx.memo(&"memoA", move |ctx| async move {
                tokio::task::yield_now().await;
                a_calls.fetch_add(1, Ordering::SeqCst);
                Ok::<_, cocoindex::Error>(ctx.get_key(&ka)?.clone())
            });
            let fut_b = ctx.memo(&"memoB", move |ctx| async move {
                b_calls.fetch_add(1, Ordering::SeqCst);
                let v = ctx.get_key(&kb)?.clone();
                tokio::task::yield_now().await;
                Ok::<_, cocoindex::Error>(v)
            });
            let (ra, _rb): (String, String) = futures::future::try_join(fut_a, fut_b).await?;
            Ok(ra)
        })
        .await
        .unwrap()
    }

    let path = dir.path().join("lmdb");
    let r1 = drive(
        path.clone(),
        key_a.clone(),
        key_b.clone(),
        "a1",
        "b1",
        a_calls.clone(),
        b_calls.clone(),
    )
    .await;
    assert_eq!(r1, "a1");
    assert_eq!(a_calls.load(Ordering::SeqCst), 1);
    assert_eq!(b_calls.load(Ordering::SeqCst), 1);

    // Change ONLY key A. Memo A must re-run (dependency correctly attributed),
    // memo B must stay cached.
    let r2 = drive(
        path,
        key_a.clone(),
        key_b.clone(),
        "a2",
        "b1",
        a_calls.clone(),
        b_calls.clone(),
    )
    .await;
    assert_eq!(
        r2, "a2",
        "memo A served a stale value — dependency misattributed"
    );
    assert_eq!(a_calls.load(Ordering::SeqCst), 2, "memo A should re-run");
    assert_eq!(
        b_calls.load(Ordering::SeqCst),
        1,
        "memo B should stay cached"
    );
}

// ---------------------------------------------------------------------------
// Memoization: cache hit / miss
// ---------------------------------------------------------------------------

#[tokio::test]
async fn memo_cached_executes_on_first_call() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let (app, _dir) = temp_app("memo_first").await;
    let call_count = Arc::new(AtomicUsize::new(0));

    let count = call_count.clone();
    app.update(|ctx| async move {
        let _result: i32 = cocoindex::memo::cached(&ctx, &"key1", move |_ctx| {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(42)
            }
        })
        .await?;
        Ok(())
    })
    .await
    .unwrap();

    assert_eq!(call_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn memo_cached_returns_cached_on_second_run() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let (app, _dir) = temp_app("memo_cache_hit").await;
    let call_count = Arc::new(AtomicUsize::new(0));

    // First run: should execute the closure.
    let count = call_count.clone();
    app.update(|ctx| async move {
        let result: i32 = cocoindex::memo::cached(&ctx, &"stable_key", move |_ctx| {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(100)
            }
        })
        .await?;
        assert_eq!(result, 100);
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    // Second run with same key: should return cached result.
    let count = call_count.clone();
    app.update(|ctx| async move {
        let result: i32 = cocoindex::memo::cached(&ctx, &"stable_key", move |_ctx| {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(999) // Different value — should NOT be returned.
            }
        })
        .await?;
        assert_eq!(result, 100); // Cached value from first run.
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(call_count.load(Ordering::SeqCst), 1); // Closure was NOT called.
}

#[tokio::test]
async fn update_with_options_full_reprocess_forces_memo_execution() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let (app, _dir) = temp_app("full_reprocess_forces_memo").await;
    let call_count = Arc::new(AtomicUsize::new(0));

    let count = call_count.clone();
    app.update(|ctx| async move {
        let _: i32 = ctx
            .memo(&"stable_key", move |_ctx| {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(1)
                }
            })
            .await?;
        Ok(())
    })
    .await
    .unwrap();

    let count = call_count.clone();
    app.update_with_options(
        cocoindex::UpdateOptions {
            full_reprocess: true,
            live: false,
            ..cocoindex::UpdateOptions::default()
        },
        |ctx| async move {
            let result: i32 = ctx
                .memo(&"stable_key", move |_ctx| {
                    let count = count.clone();
                    async move {
                        count.fetch_add(1, Ordering::SeqCst);
                        Ok(2)
                    }
                })
                .await?;
            assert_eq!(result, 2);
            Ok(())
        },
    )
    .await
    .unwrap();

    assert_eq!(call_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn full_reprocess_forces_child_scope_memo_execution() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let (app, _dir) = temp_app("full_reprocess_child_scope_memo").await;
    let call_count = Arc::new(AtomicUsize::new(0));

    let count = call_count.clone();
    app.update(|ctx| async move {
        ctx.scope(&"child", move |child| {
            let count = count.clone();
            async move {
                let result: i32 = child
                    .memo(&"stable_child_key", move |_ctx| {
                        let count = count.clone();
                        async move {
                            count.fetch_add(1, Ordering::SeqCst);
                            Ok(10)
                        }
                    })
                    .await?;
                assert_eq!(result, 10);
                Ok(())
            }
        })
        .await
    })
    .await
    .unwrap();

    let count = call_count.clone();
    app.update(|ctx| async move {
        ctx.scope(&"child", move |child| {
            let count = count.clone();
            async move {
                let result: i32 = child
                    .memo(&"stable_child_key", move |_ctx| {
                        let count = count.clone();
                        async move {
                            count.fetch_add(1, Ordering::SeqCst);
                            Ok(999)
                        }
                    })
                    .await?;
                assert_eq!(result, 10);
                Ok(())
            }
        })
        .await
    })
    .await
    .unwrap();
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    let count = call_count.clone();
    app.update_with_options(
        cocoindex::UpdateOptions {
            full_reprocess: true,
            live: false,
            ..cocoindex::UpdateOptions::default()
        },
        |ctx| async move {
            ctx.scope(&"child", move |child| {
                let count = count.clone();
                async move {
                    let result: i32 = child
                        .memo(&"stable_child_key", move |_ctx| {
                            let count = count.clone();
                            async move {
                                count.fetch_add(1, Ordering::SeqCst);
                                Ok(20)
                            }
                        })
                        .await?;
                    assert_eq!(result, 20);
                    Ok(())
                }
            })
            .await
        },
    )
    .await
    .unwrap();

    assert_eq!(call_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn memo_cached_reexecutes_on_key_change() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let (app, _dir) = temp_app("memo_cache_miss").await;
    let call_count = Arc::new(AtomicUsize::new(0));

    // First run with key "v1".
    let count = call_count.clone();
    app.update(|ctx| async move {
        let result: i32 = cocoindex::memo::cached(&ctx, &"v1", move |_ctx| {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(10)
            }
        })
        .await?;
        assert_eq!(result, 10);
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    // Second run with key "v2": different key means cache miss.
    let count = call_count.clone();
    app.update(|ctx| async move {
        let result: i32 = cocoindex::memo::cached(&ctx, &"v2", move |_ctx| {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(20)
            }
        })
        .await?;
        assert_eq!(result, 20); // New value from second execution.
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(call_count.load(Ordering::SeqCst), 2); // Closure was called again.
}

#[test]
fn memo_cached_blocking() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let (app, _dir) = temp_app_blocking("memo_blocking");
    let call_count = Arc::new(AtomicUsize::new(0));

    // First run.
    let count = call_count.clone();
    app.update_blocking(|ctx| async move {
        let result: String = cocoindex::memo::cached(&ctx, &("file.rs", 42u64), move |_ctx| {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok("extracted entities".to_string())
            }
        })
        .await?;
        assert_eq!(result, "extracted entities");
        Ok(())
    })
    .unwrap();
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    // Second run with same key — should be cached.
    let count = call_count.clone();
    app.update_blocking(|ctx| async move {
        let result: String = cocoindex::memo::cached(&ctx, &("file.rs", 42u64), move |_ctx| {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok("should not run".to_string())
            }
        })
        .await?;
        assert_eq!(result, "extracted entities"); // Cached.
        Ok(())
    })
    .unwrap();
    assert_eq!(call_count.load(Ordering::SeqCst), 1); // Not called.
}

// ---------------------------------------------------------------------------
// Drop state
// ---------------------------------------------------------------------------

#[test]
fn drop_state_blocking_clears_memoization() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let (app, _dir) = temp_app_blocking("drop_state");
    let call_count = Arc::new(AtomicUsize::new(0));

    // First run: populate cache.
    let count = call_count.clone();
    app.update_blocking(|ctx| async move {
        let _: i32 = cocoindex::memo::cached(&ctx, &"key", move |_ctx| {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(1)
            }
        })
        .await?;
        Ok(())
    })
    .unwrap();
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    // Drop state.
    app.drop_state_blocking().unwrap();

    // Third run: cache should be empty, closure re-executes.
    let count = call_count.clone();
    app.update_blocking(|ctx| async move {
        let result: i32 = cocoindex::memo::cached(&ctx, &"key", move |_ctx| {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(2)
            }
        })
        .await?;
        assert_eq!(result, 2); // Fresh execution.
        Ok(())
    })
    .unwrap();
    assert_eq!(call_count.load(Ordering::SeqCst), 2);
}

// ---------------------------------------------------------------------------
// Error propagation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn memo_cached_propagates_closure_error() {
    let (app, _dir) = temp_app("memo_error").await;
    let result = app
        .update(|ctx| async move {
            let _: i32 = cocoindex::memo::cached(&ctx, &"key", |_ctx| async {
                Err(cocoindex::Error::engine("test error"))
            })
            .await?;
            Ok(())
        })
        .await;
    assert!(result.is_err());
}

#[test]
fn update_blocking_propagates_closure_error() {
    let (app, _dir) = temp_app_blocking("sync_error");
    let result = app.update_blocking(|_ctx| async move {
        Err::<(), _>(cocoindex::Error::engine("sync test error"))
    });
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// Serialization roundtrip via memoization
// ---------------------------------------------------------------------------

#[tokio::test]
async fn memo_cached_complex_types() {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Entity {
        name: String,
        line: usize,
    }

    let (app, _dir) = temp_app("memo_complex").await;

    // Run 1: store complex type.
    app.update(|ctx| async move {
        let entities: Vec<Entity> = cocoindex::memo::cached(&ctx, &"complex_key", |_ctx| async {
            Ok(vec![
                Entity {
                    name: "foo".into(),
                    line: 10,
                },
                Entity {
                    name: "bar".into(),
                    line: 20,
                },
            ])
        })
        .await?;
        assert_eq!(entities.len(), 2);
        assert_eq!(entities[0].name, "foo");
        Ok(())
    })
    .await
    .unwrap();

    // Run 2: retrieve from cache.
    app.update(|ctx| async move {
        let entities: Vec<Entity> = cocoindex::memo::cached(&ctx, &"complex_key", |_ctx| async {
            panic!("should not be called — cache hit");
        })
        .await?;
        assert_eq!(entities.len(), 2);
        assert_eq!(entities[1].name, "bar");
        assert_eq!(entities[1].line, 20);
        Ok(())
    })
    .await
    .unwrap();
}

// ---------------------------------------------------------------------------
// App::open convenience
// ---------------------------------------------------------------------------

#[test]
fn app_open_convenience() {
    let dir = tempfile::tempdir().unwrap();
    let app = App::open_blocking("open_test", dir.path().join("lmdb")).unwrap();
    app.update_blocking(|ctx| async move {
        assert!(ctx.has_pipeline_context());
        Ok(())
    })
    .unwrap();
}

// ---------------------------------------------------------------------------
// App::run returns RunStats
// ---------------------------------------------------------------------------

#[tokio::test]
async fn app_run_returns_stats() {
    let dir = tempfile::tempdir().unwrap();
    let app = App::open("run_stats", dir.path().join("lmdb"))
        .await
        .unwrap();
    let stats = app.run(|_ctx| async move { Ok(()) }).await.unwrap();
    // Stats should have a non-zero elapsed time
    assert!(stats.elapsed.as_nanos() > 0);
    // Display impl works
    let display = format!("{stats}");
    assert!(display.contains("processed"));
}

// ---------------------------------------------------------------------------
// ctx.memo() method (convenience wrapper)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ctx_memo_method() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let (app, _dir) = temp_app("ctx_memo").await;
    let call_count = Arc::new(AtomicUsize::new(0));

    // First run
    let count = call_count.clone();
    app.update(|ctx| async move {
        let result: i32 = ctx
            .memo(&"memo_key", move |_ctx| {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(77)
                }
            })
            .await?;
        assert_eq!(result, 77);
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    // Second run — cached
    let count = call_count.clone();
    app.update(|ctx| async move {
        let result: i32 = ctx
            .memo(&"memo_key", move |_ctx| {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(999)
                }
            })
            .await?;
        assert_eq!(result, 77); // Cached
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(call_count.load(Ordering::SeqCst), 1); // Not called
}

// ---------------------------------------------------------------------------
// ctx.scope() method
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ctx_scope_runs_child() {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Output {
        value: i32,
    }

    let (app, _dir) = temp_app("ctx_scope").await;
    app.update(|ctx| async move {
        let result: Output = ctx
            .scope(
                &"child1",
                |_child_ctx| async move { Ok(Output { value: 42 }) },
            )
            .await?;
        assert_eq!(result.value, 42);
        Ok(())
    })
    .await
    .unwrap();
}

// ---------------------------------------------------------------------------
// ctx.write_file() method
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ctx_write_file_creates_file() {
    let (app, dir) = temp_app("ctx_write").await;
    let output_dir = dir.path().join("output");

    let out = output_dir.clone();
    app.update(|ctx| async move {
        ctx.write_file(out.join("hello.txt"), b"world")?;
        Ok(())
    })
    .await
    .unwrap();

    let content = std::fs::read_to_string(output_dir.join("hello.txt")).unwrap();
    assert_eq!(content, "world");
}

#[tokio::test]
async fn ctx_write_file_creates_nested_dirs_and_overwrites() {
    let (app, dir) = temp_app("ctx_write_nested").await;
    let output_dir = dir.path().join("output");
    let nested = output_dir.join("sub/hello.txt");

    std::fs::create_dir_all(nested.parent().unwrap()).unwrap();
    std::fs::write(&nested, "old").unwrap();

    let out = nested.clone();
    app.update(|ctx| async move {
        ctx.write_file(&out, b"new")?;
        Ok(())
    })
    .await
    .unwrap();

    let content = std::fs::read_to_string(&nested).unwrap();
    assert_eq!(content, "new");
}

// ---------------------------------------------------------------------------
// #[cocoindex::function] macro — bare (L0, no memo)
// ---------------------------------------------------------------------------

#[cocoindex::function]
async fn bare_fn(ctx: &cocoindex::Ctx, _val: &str) -> cocoindex::Result<i32> {
    let _ = ctx;
    Ok(1)
}

#[cocoindex::function]
async fn bare_logic_v1(_ctx: &cocoindex::Ctx) -> cocoindex::Result<i32> {
    Ok(1)
}

#[cocoindex::function]
async fn bare_logic_v2(_ctx: &cocoindex::Ctx) -> cocoindex::Result<i32> {
    Ok(2)
}

#[cocoindex::function]
async fn bare_read_context_key(
    ctx: &cocoindex::Ctx,
    key: &ContextKey<String>,
) -> cocoindex::Result<String> {
    Ok(ctx.get_key(key)?.clone())
}

#[tokio::test]
async fn function_macro_bare() {
    // The macro should emit a code hash constant.
    let hash = __COCO_FN_HASH_BARE_FN;
    assert_ne!(hash, 0);

    // The function itself should still work normally.
    let (app, _dir) = temp_app("fn_bare").await;
    app.update(|ctx| async move {
        let result = bare_fn(&ctx, "hello").await?;
        assert_eq!(result, 1);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn bare_function_context_key_invalidates_manual_memo_callers() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let key = ContextKey::<String>::new_detect_change("pipeline/bare_context_transitive");
    let dir = tempfile::tempdir().unwrap();
    let call_count = Arc::new(AtomicUsize::new(0));

    let app = App::builder("bare_context_key_invalidates_manual_memo")
        .db_path(dir.path().join("lmdb"))
        .provide_key(&key, "v1".to_string())
        .build()
        .await
        .unwrap();
    let count = call_count.clone();
    let key_for_first = key.clone();
    app.update(move |ctx| async move {
        let result: String = ctx
            .memo(&"stable", move |ctx| {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    bare_read_context_key(&ctx, &key_for_first).await
                }
            })
            .await?;
        assert_eq!(result, "v1");
        Ok(())
    })
    .await
    .unwrap();
    drop(app);

    let app = App::builder("bare_context_key_invalidates_manual_memo")
        .db_path(dir.path().join("lmdb"))
        .provide_key(&key, "v2".to_string())
        .build()
        .await
        .unwrap();
    let count = call_count.clone();
    let key_for_second = key.clone();
    app.update(move |ctx| async move {
        let result: String = ctx
            .memo(&"stable", move |ctx| {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    bare_read_context_key(&ctx, &key_for_second).await
                }
            })
            .await?;
        assert_eq!(result, "v2");
        Ok(())
    })
    .await
    .unwrap();

    assert_eq!(call_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn bare_function_logic_invalidates_manual_memo_callers() {
    let (app, _dir) = temp_app("bare_logic_invalidates_manual_memo").await;

    app.update(|ctx| async move {
        let result: i32 = ctx
            .memo(&"stable", |ctx| async move { bare_logic_v1(&ctx).await })
            .await?;
        assert_eq!(result, 1);
        Ok(())
    })
    .await
    .unwrap();

    app.update(|ctx| async move {
        let result: i32 = ctx
            .memo(&"stable", |ctx| async move { bare_logic_v2(&ctx).await })
            .await?;
        assert_eq!(
            result, 2,
            "bare #[function] helper logic did not invalidate its memoized caller"
        );
        Ok(())
    })
    .await
    .unwrap();
}

// ---------------------------------------------------------------------------
// #[cocoindex::function(memo)] macro
// ---------------------------------------------------------------------------

mod memo_test_basic {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static CALLS: AtomicUsize = AtomicUsize::new(0);

    #[cocoindex::function(memo)]
    async fn the_fn(_ctx: &cocoindex::Ctx, key: &String) -> cocoindex::Result<i32> {
        CALLS.fetch_add(1, Ordering::SeqCst);
        let _ = &key;
        Ok(42)
    }

    #[tokio::test]
    async fn function_macro_memo() {
        // Code hash constant should exist.
        let hash = __COCO_FN_HASH_THE_FN;
        assert_ne!(hash, 0);

        CALLS.store(0, Ordering::SeqCst);
        let (app, _dir) = temp_app("fn_memo").await;

        app.update(|ctx| async move {
            let result = the_fn(&ctx, &"k1".to_string()).await?;
            assert_eq!(result, 42);
            Ok(())
        })
        .await
        .unwrap();

        assert_eq!(CALLS.load(Ordering::SeqCst), 1);
    }
}

mod memo_test_cache_hit {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static CALLS: AtomicUsize = AtomicUsize::new(0);

    #[cocoindex::function(memo)]
    async fn the_fn(_ctx: &cocoindex::Ctx, key: &String) -> cocoindex::Result<i32> {
        CALLS.fetch_add(1, Ordering::SeqCst);
        let _ = &key;
        Ok(42)
    }

    #[tokio::test]
    async fn function_macro_memo_cache_hit() {
        CALLS.store(0, Ordering::SeqCst);
        let (app, _dir) = temp_app("fn_memo_hit").await;

        // First run — executes.
        app.update(|ctx| async move {
            let result = the_fn(&ctx, &"same_key".to_string()).await?;
            assert_eq!(result, 42);
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(CALLS.load(Ordering::SeqCst), 1);

        // Second run with same key — cached.
        app.update(|ctx| async move {
            let result = the_fn(&ctx, &"same_key".to_string()).await?;
            assert_eq!(result, 42);
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(CALLS.load(Ordering::SeqCst), 1); // Not called again.
    }
}

mod memo_test_borrowed_str {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static CALLS: AtomicUsize = AtomicUsize::new(0);

    #[cocoindex::function(memo)]
    async fn len(_ctx: &cocoindex::Ctx, _input: &str) -> cocoindex::Result<usize> {
        CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(_input.len())
    }

    #[tokio::test]
    async fn function_macro_memo_accepts_borrowed_str() {
        CALLS.store(0, Ordering::SeqCst);
        let (app, _dir) = temp_app("fn_memo_borrowed_str").await;

        app.update(|ctx| async move {
            assert_eq!(len(&ctx, "same").await?, 4);
            Ok(())
        })
        .await
        .unwrap();
        app.update(|ctx| async move {
            assert_eq!(len(&ctx, "same").await?, 4);
            Ok(())
        })
        .await
        .unwrap();

        assert_eq!(CALLS.load(Ordering::SeqCst), 1);
    }
}

mod memo_test_file_state {
    use super::*;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, OnceLock};
    use std::time::Duration;

    use cocoindex::fs::{FileEntry, FilePath, walk_dir};
    use cocoindex::{ContextKey, Ctx, FileLike, Result};

    fn calls_key() -> &'static ContextKey<Arc<AtomicUsize>> {
        static KEY: OnceLock<ContextKey<Arc<AtomicUsize>>> = OnceLock::new();
        KEY.get_or_init(|| ContextKey::new("pipeline/file_memo_state_calls"))
    }

    fn read_source_file(root: &Path) -> FileEntry {
        walk_dir(FilePath::with_base_dir(
            "docs",
            root.to_path_buf(),
            PathBuf::new(),
        ))
        .recursive(true)
        .walk()
        .unwrap()
        .into_iter()
        .find(|file| file.key() == "note.txt")
        .unwrap()
    }

    fn read_source_files(root: &Path) -> Vec<FileEntry> {
        walk_dir(FilePath::with_base_dir(
            "docs",
            root.to_path_buf(),
            PathBuf::new(),
        ))
        .recursive(true)
        .walk()
        .unwrap()
    }

    #[cocoindex::function(memo)]
    async fn read_file(ctx: &Ctx, file: &FileEntry) -> Result<String> {
        ctx.get_key(calls_key())?.fetch_add(1, Ordering::SeqCst);
        file.read_text().await
    }

    #[cocoindex::function(memo, batching)]
    async fn read_files(ctx: &Ctx, files: Vec<FileEntry>) -> Result<Vec<String>> {
        ctx.get_key(calls_key())?
            .fetch_add(files.len(), Ordering::SeqCst);
        let mut out = Vec::with_capacity(files.len());
        for file in files {
            out.push(file.read_text().await?);
        }
        Ok(out)
    }

    #[tokio::test]
    async fn file_memo_state_reuses_cache_when_mtime_changes_but_content_is_same() {
        let source = tempfile::tempdir().unwrap();
        let source_path = source.path().to_path_buf();
        let file_path = source_path.join("note.txt");
        std::fs::write(&file_path, "same").unwrap();

        let calls = Arc::new(AtomicUsize::new(0));
        let dir = tempfile::tempdir().unwrap();
        let app = App::builder("file_memo_state_reuses_same_content")
            .db_path(dir.path().join("lmdb"))
            .provide_key(calls_key(), calls.clone())
            .build()
            .await
            .unwrap();

        app.update({
            let source_path = source_path.clone();
            |ctx| async move {
                let file = read_source_file(&source_path);
                assert_eq!(read_file(&ctx, &file).await?, "same");
                Ok(())
            }
        })
        .await
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        tokio::time::sleep(Duration::from_millis(20)).await;
        std::fs::write(&file_path, "same").unwrap();
        app.update({
            let source_path = source_path.clone();
            |ctx| async move {
                let file = read_source_file(&source_path);
                assert_eq!(read_file(&ctx, &file).await?, "same");
                Ok(())
            }
        })
        .await
        .unwrap();
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "same content with a new mtime should reuse cached work"
        );

        tokio::time::sleep(Duration::from_millis(20)).await;
        std::fs::write(&file_path, "changed").unwrap();
        app.update({
            let source_path = source_path.clone();
            |ctx| async move {
                let file = read_source_file(&source_path);
                assert_eq!(read_file(&ctx, &file).await?, "changed");
                Ok(())
            }
        })
        .await
        .unwrap();
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "changed content must invalidate the memoized result"
        );
    }

    #[tokio::test]
    async fn batch_file_memo_state_reuses_cache_when_mtime_changes_but_content_is_same() {
        let source = tempfile::tempdir().unwrap();
        let source_path = source.path().to_path_buf();
        let a_path = source_path.join("a.txt");
        let b_path = source_path.join("b.txt");
        std::fs::write(&a_path, "same").unwrap();
        std::fs::write(&b_path, "other").unwrap();

        let calls = Arc::new(AtomicUsize::new(0));
        let dir = tempfile::tempdir().unwrap();
        let app = App::builder("batch_file_memo_state_reuses_same_content")
            .db_path(dir.path().join("lmdb"))
            .provide_key(calls_key(), calls.clone())
            .build()
            .await
            .unwrap();

        app.update({
            let source_path = source_path.clone();
            |ctx| async move {
                assert_eq!(
                    read_files(&ctx, read_source_files(&source_path)).await?,
                    vec!["same".to_string(), "other".to_string()]
                );
                Ok(())
            }
        })
        .await
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        tokio::time::sleep(Duration::from_millis(20)).await;
        std::fs::write(&a_path, "same").unwrap();
        app.update({
            let source_path = source_path.clone();
            |ctx| async move {
                assert_eq!(
                    read_files(&ctx, read_source_files(&source_path)).await?,
                    vec!["same".to_string(), "other".to_string()]
                );
                Ok(())
            }
        })
        .await
        .unwrap();
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "same content with a new mtime should not be a batch miss"
        );

        tokio::time::sleep(Duration::from_millis(20)).await;
        std::fs::write(&a_path, "changed").unwrap();
        app.update({
            let source_path = source_path.clone();
            |ctx| async move {
                assert_eq!(
                    read_files(&ctx, read_source_files(&source_path)).await?,
                    vec!["changed".to_string(), "other".to_string()]
                );
                Ok(())
            }
        })
        .await
        .unwrap();
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "only the changed file should be a batch miss"
        );
    }
}

// Two functions with same signature but different bodies produce different code hashes.
#[cocoindex::function(memo)]
async fn hash_fn_a(_ctx: &cocoindex::Ctx, _key: &String) -> cocoindex::Result<i32> {
    Ok(111)
}

#[cocoindex::function(memo)]
async fn hash_fn_b(_ctx: &cocoindex::Ctx, _key: &String) -> cocoindex::Result<i32> {
    Ok(222)
}

#[test]
fn function_macro_memo_code_hash_invalidation() {
    // Different function bodies must produce different code hashes.
    assert_ne!(__COCO_FN_HASH_HASH_FN_A, __COCO_FN_HASH_HASH_FN_B);
}

mod function_macro_identity_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, OnceLock};

    use cocoindex::{ContextKey, Ctx, Result};

    fn calls_key() -> &'static ContextKey<Arc<AtomicUsize>> {
        static KEY: OnceLock<ContextKey<Arc<AtomicUsize>>> = OnceLock::new();
        KEY.get_or_init(|| ContextKey::new("pipeline/function_macro_identity_calls"))
    }

    #[cocoindex::function(memo)]
    async fn first(ctx: &Ctx, input: &String) -> Result<String> {
        ctx.get_key(calls_key())?.fetch_add(1, Ordering::SeqCst);
        Ok(input.clone())
    }

    #[cocoindex::function(memo)]
    async fn second(ctx: &Ctx, input: &String) -> Result<String> {
        ctx.get_key(calls_key())?.fetch_add(1, Ordering::SeqCst);
        Ok(input.clone())
    }

    #[tokio::test]
    async fn identical_body_functions_do_not_share_memo_entries() {
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let app = cocoindex::App::builder("function_identity_memo")
            .db_path(dir.path().join("lmdb"))
            .provide_key(calls_key(), calls.clone())
            .build()
            .await
            .unwrap();

        app.update(|ctx| async move {
            let input = "same-key".to_string();
            assert_eq!(first(&ctx, &input).await?, "same-key");
            assert_eq!(second(&ctx, &input).await?, "same-key");
            Ok(())
        })
        .await
        .unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}

mod function_macro_memo_key_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, OnceLock};

    use cocoindex::{ContextKey, Ctx, Result};
    use serde::Serialize;

    #[derive(Clone, Serialize)]
    struct Entry {
        name: String,
        version: u32,
        content: String,
    }

    fn calls_key() -> &'static ContextKey<Arc<AtomicUsize>> {
        static KEY: OnceLock<ContextKey<Arc<AtomicUsize>>> = OnceLock::new();
        KEY.get_or_init(|| ContextKey::new("pipeline/function_macro_memo_key_calls"))
    }

    fn entry_name_version(entry: &Entry) -> (String, u32) {
        (entry.name.clone(), entry.version)
    }

    fn item_name_version(entry: &Entry) -> (String, u32) {
        (entry.name.clone(), entry.version)
    }

    #[cocoindex::function(memo, memo_key(entry = entry_name_version, extra = skip))]
    async fn transform_entry(ctx: &Ctx, entry: &Entry, extra: &String) -> Result<String> {
        ctx.get_key(calls_key())?.fetch_add(1, Ordering::SeqCst);
        Ok(format!("{}:{extra}", entry.content))
    }

    #[cocoindex::function(memo, batching, memo_key(item = item_name_version, extra = skip))]
    async fn transform_batch(
        ctx: &Ctx,
        entries: Vec<Entry>,
        extra: &String,
    ) -> Result<Vec<String>> {
        ctx.get_key(calls_key())?.fetch_add(1, Ordering::SeqCst);
        Ok(entries
            .into_iter()
            .map(|entry| format!("{}:{extra}", entry.content))
            .collect())
    }

    #[tokio::test]
    async fn memo_key_transform_and_skip_for_function_macro() {
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let app = cocoindex::App::builder("function_macro_memo_key")
            .db_path(dir.path().join("lmdb"))
            .provide_key(calls_key(), calls.clone())
            .build()
            .await
            .unwrap();

        app.update(|ctx| async move {
            let first = Entry {
                name: "A".into(),
                version: 1,
                content: "content-v1".into(),
            };
            let changed_unkeyed = Entry {
                name: "A".into(),
                version: 1,
                content: "content-v1-changed".into(),
            };
            let changed_keyed = Entry {
                name: "A".into(),
                version: 2,
                content: "content-v2".into(),
            };

            assert_eq!(
                transform_entry(&ctx, &first, &"debug-a".to_string()).await?,
                "content-v1:debug-a"
            );
            assert_eq!(
                transform_entry(&ctx, &changed_unkeyed, &"debug-b".to_string()).await?,
                "content-v1:debug-a"
            );
            assert_eq!(
                transform_entry(&ctx, &changed_keyed, &"debug-c".to_string()).await?,
                "content-v2:debug-c"
            );
            Ok(())
        })
        .await
        .unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn memo_key_transform_and_skip_for_batch_macro() {
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let app = cocoindex::App::builder("function_macro_batch_memo_key")
            .db_path(dir.path().join("lmdb"))
            .provide_key(calls_key(), calls.clone())
            .build()
            .await
            .unwrap();

        app.update(|ctx| async move {
            let initial = vec![
                Entry {
                    name: "A".into(),
                    version: 1,
                    content: "a1".into(),
                },
                Entry {
                    name: "B".into(),
                    version: 1,
                    content: "b1".into(),
                },
            ];
            assert_eq!(
                transform_batch(&ctx, initial, &"first".to_string()).await?,
                vec!["a1:first".to_string(), "b1:first".to_string()]
            );

            let changed = vec![
                Entry {
                    name: "A".into(),
                    version: 1,
                    content: "a1-changed".into(),
                },
                Entry {
                    name: "B".into(),
                    version: 2,
                    content: "b2".into(),
                },
            ];
            assert_eq!(
                transform_batch(&ctx, changed, &"second".to_string()).await?,
                vec!["a1:first".to_string(), "b2:second".to_string()]
            );
            Ok(())
        })
        .await
        .unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}

// ---------------------------------------------------------------------------
// ctx.mount_each()
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ctx_mount_each() {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Output {
        name: String,
        value: i32,
    }

    let (app, _dir) = temp_app("mount_each").await;
    app.update(|ctx| async move {
        let items = vec![("alpha", 1), ("beta", 2), ("gamma", 3)];
        let results: Vec<Output> = ctx
            .mount_each(
                items,
                |&(name, _)| name.to_string(),
                |_child_ctx, (name, value)| async move {
                    Ok(Output {
                        name: name.to_string(),
                        value: value * 10,
                    })
                },
            )
            .await?;

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].name, "alpha");
        assert_eq!(results[0].value, 10);
        assert_eq!(results[1].name, "beta");
        assert_eq!(results[1].value, 20);
        assert_eq!(results[2].name, "gamma");
        assert_eq!(results[2].value, 30);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn ctx_mount_each_independent_memo() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let (app, _dir) = temp_app("mount_each_memo").await;
    let call_count = Arc::new(AtomicUsize::new(0));

    // First run: two children, each memos.
    let count = call_count.clone();
    app.update(|ctx| async move {
        let items = vec!["child_a", "child_b"];
        let results: Vec<i32> = ctx
            .mount_each(items, |name| name.to_string(), {
                let count = count.clone();
                move |child_ctx, name| {
                    let count = count.clone();
                    async move {
                        child_ctx
                            .memo(&name, {
                                let count = count.clone();
                                move |_ctx| async move {
                                    count.fetch_add(1, Ordering::SeqCst);
                                    Ok(1i32)
                                }
                            })
                            .await
                    }
                }
            })
            .await?;

        assert_eq!(results, vec![1, 1]);
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(call_count.load(Ordering::SeqCst), 2); // Both executed.

    // Second run with same keys — both cached.
    let count = call_count.clone();
    app.update(|ctx| async move {
        let items = vec!["child_a", "child_b"];
        let results: Vec<i32> = ctx
            .mount_each(items, |name| name.to_string(), {
                let count = count.clone();
                move |child_ctx, name| {
                    let count = count.clone();
                    async move {
                        child_ctx
                            .memo(&name, {
                                let count = count.clone();
                                move |_ctx| async move {
                                    count.fetch_add(1, Ordering::SeqCst);
                                    Ok(999i32)
                                }
                            })
                            .await
                    }
                }
            })
            .await?;

        assert_eq!(results, vec![1, 1]); // Cached values.
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(call_count.load(Ordering::SeqCst), 2); // Not called again.
}

#[tokio::test]
async fn ctx_mount_each_preserves_input_order_under_concurrency() {
    let (app, _dir) = temp_app("mount_each_order").await;
    app.update(|ctx| async move {
        let items = vec![("slow", 30_u64), ("fast", 0_u64), ("mid", 10_u64)];
        let results: Vec<String> = ctx
            .mount_each(
                items.clone(),
                |&(name, _)| name.to_string(),
                |_child_ctx, (name, delay)| async move {
                    sleep(Duration::from_millis(delay)).await;
                    Ok(name.to_string())
                },
            )
            .await?;

        assert_eq!(results, vec!["slow", "fast", "mid"]);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn max_inflight_components_limits_mount_each_concurrency() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn record_peak(current: &AtomicUsize, peak: &AtomicUsize) {
        let now = current.fetch_add(1, Ordering::SeqCst) + 1;
        let mut observed = peak.load(Ordering::SeqCst);
        while now > observed {
            match peak.compare_exchange(observed, now, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => break,
                Err(next) => observed = next,
            }
        }
    }

    let (app, _dir) = temp_app_with_max_inflight("mount_each_max_inflight", 2).await;
    let current = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let total = Arc::new(AtomicUsize::new(0));

    app.update({
        let current = current.clone();
        let peak = peak.clone();
        let total = total.clone();
        move |ctx| async move {
            let items: Vec<usize> = (0..6).collect();
            ctx.mount_each(items, |i| i.to_string(), {
                let current = current.clone();
                let peak = peak.clone();
                let total = total.clone();
                move |_child_ctx, _i| {
                    let current = current.clone();
                    let peak = peak.clone();
                    let total = total.clone();
                    async move {
                        total.fetch_add(1, Ordering::SeqCst);
                        record_peak(&current, &peak);
                        sleep(Duration::from_millis(50)).await;
                        current.fetch_sub(1, Ordering::SeqCst);
                        Ok(())
                    }
                }
            })
            .await?;
            Ok(())
        }
    })
    .await
    .unwrap();

    assert_eq!(total.load(Ordering::SeqCst), 6);
    assert!(
        peak.load(Ordering::SeqCst) <= 2,
        "peak concurrency exceeded max_inflight_components"
    );
}

#[tokio::test]
async fn max_inflight_components_allows_nested_scope_with_single_permit() {
    let (app, _dir) = temp_app_with_max_inflight("nested_scope_max_inflight_one", 1).await;

    tokio::time::timeout(Duration::from_secs(3), async {
        app.update(|ctx| async move {
            for i in 0..4 {
                let key = format!("child-{i}");
                ctx.scope(&key, |child| async move {
                    child
                        .scope(&"grandchild", |_grandchild| async move { Ok(()) })
                        .await?;
                    Ok(())
                })
                .await?;
            }
            Ok(())
        })
        .await
    })
    .await
    .expect("nested scopes should not deadlock with max_inflight_components=1")
    .unwrap();
}

#[tokio::test]
async fn public_target_state_api_reconciles_typed_actions_and_tracking_records() {
    use cocoindex::{
        StableKey, TargetAction, TargetActionSink, TargetHandler, TargetReconcileOutput,
        declare_target_state, register_root_target_states_provider,
    };
    use serde::{Deserialize, Serialize};
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct WriteAction {
        key: String,
        value: String,
    }

    #[derive(Clone)]
    struct MemoryHandler {
        sink: TargetActionSink<WriteAction>,
    }

    impl TargetHandler<String> for MemoryHandler {
        type TrackingRecord = String;
        type Action = WriteAction;

        fn reconcile(
            &self,
            key: StableKey,
            desired_target_state: Option<String>,
            prev_possible_records: Vec<Self::TrackingRecord>,
            _prev_may_be_missing: bool,
        ) -> cocoindex::Result<Option<TargetReconcileOutput<Self::Action, Self::TrackingRecord>>>
        {
            let Some(value) = desired_target_state else {
                return Ok(None);
            };
            if prev_possible_records.iter().any(|prev| prev == &value) {
                return Ok(None);
            }
            Ok(Some(TargetReconcileOutput {
                action: TargetAction::Update(WriteAction {
                    key: key.to_string(),
                    value: value.clone(),
                }),
                sink: self.sink.clone(),
                tracking_record: Some(value),
                child_invalidation: None,
            }))
        }
    }

    let (app, _dir) = temp_app("public_target_state_api").await;
    let applied = Arc::new(Mutex::new(Vec::<WriteAction>::new()));

    async fn run(
        app: &App,
        applied: Arc<Mutex<Vec<WriteAction>>>,
        value: &'static str,
    ) -> cocoindex::Result<()> {
        app.update(move |ctx| {
            let applied = applied.clone();
            async move {
                let sink = TargetActionSink::from_async_fn(move |actions| {
                    let applied = applied.clone();
                    async move {
                        let mut applied = applied.lock().unwrap();
                        for action in actions {
                            if let TargetAction::Update(action) = action {
                                applied.push(action);
                            }
                        }
                        Ok(())
                    }
                });
                let provider = register_root_target_states_provider(
                    &ctx,
                    "test/public_target_state_api",
                    MemoryHandler { sink },
                )?;
                declare_target_state(&ctx, provider.target_state("row-1", value.to_string()))?;
                Ok(())
            }
        })
        .await
    }

    run(&app, applied.clone(), "v1").await.unwrap();
    run(&app, applied.clone(), "v1").await.unwrap();
    run(&app, applied.clone(), "v2").await.unwrap();

    let applied = applied.lock().unwrap();
    assert_eq!(
        applied.as_slice(),
        &[
            WriteAction {
                key: "\"row-1\"".to_string(),
                value: "v1".to_string(),
            },
            WriteAction {
                key: "\"row-1\"".to_string(),
                value: "v2".to_string(),
            },
        ]
    );
}

#[tokio::test]
async fn ctx_stats_group_reports_scoped_child_stats() {
    use std::sync::{Arc, Mutex};

    let (app, _dir) = temp_app("stats_group_scoped_child_stats").await;
    let group_handle_slot = Arc::new(Mutex::new(None));
    let slot_for_update = group_handle_slot.clone();

    app.update(move |ctx| async move {
        let (sum, handle) = ctx
            .stats_group("Indexing docs", |group_ctx, _handle| async move {
                let values = group_ctx
                    .mount_each(
                        vec![1, 2],
                        |item| *item,
                        |child, item| async move {
                            child
                                .memo(&item, move |_ctx| async move { Ok(item * 10) })
                                .await
                        },
                    )
                    .await?;
                Ok::<_, cocoindex::Error>(values.into_iter().sum::<i32>())
            })
            .await?;
        assert_eq!(sum, 30);
        *slot_for_update.lock().unwrap() = Some(handle);
        Ok::<_, cocoindex::Error>(())
    })
    .await
    .unwrap();

    let mut group_handle = group_handle_slot.lock().unwrap().take().unwrap();
    loop {
        if group_handle.changed().await.unwrap().is_done() {
            break;
        }
    }
    let stats = group_handle.stats_snapshot();
    assert!(
        stats.processed > 0,
        "expected scoped group to report processed work, got {stats:?}"
    );
}

#[tokio::test]
async fn ctx_stats_group_with_options_reports_scoped_child_stats() {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    let (app, _dir) = temp_app("stats_group_with_options").await;
    let group_handle_slot = Arc::new(Mutex::new(None));
    let slot_for_update = group_handle_slot.clone();

    app.update(move |ctx| async move {
        let options = cocoindex::StatsGroupOptions {
            report_to_stdout: true,
            refresh_interval: Some(Duration::from_millis(50)),
        };
        let (sum, handle) = ctx
            .stats_group_with_options(
                "Indexing docs (reported)",
                options,
                |group_ctx, _h| async move {
                    let values = group_ctx
                        .mount_each(
                            vec![1, 2, 3],
                            |item| *item,
                            |child, item| async move {
                                child
                                    .memo(&item, move |_ctx| async move { Ok(item * 10) })
                                    .await
                            },
                        )
                        .await?;
                    Ok::<_, cocoindex::Error>(values.into_iter().sum::<i32>())
                },
            )
            .await?;
        assert_eq!(sum, 60);
        *slot_for_update.lock().unwrap() = Some(handle);
        Ok::<_, cocoindex::Error>(())
    })
    .await
    .unwrap();

    let mut group_handle = group_handle_slot.lock().unwrap().take().unwrap();
    loop {
        if group_handle.changed().await.unwrap().is_done() {
            break;
        }
    }
    let stats = group_handle.stats_snapshot();
    assert!(
        stats.processed > 0,
        "expected reported group to process work, got {stats:?}"
    );
}

#[tokio::test]
async fn ctx_stats_group_terminates_when_body_errors() {
    use std::sync::{Arc, Mutex};

    let (app, _dir) = temp_app("stats_group_error_terminates").await;
    let group_handle_slot = Arc::new(Mutex::new(None));
    let slot_for_update = group_handle_slot.clone();

    let result = app
        .update(move |ctx| async move {
            let _: ((), cocoindex::StatsGroupHandle) = ctx
                .stats_group("Failing group", move |_group_ctx, handle| {
                    let slot = slot_for_update.clone();
                    async move {
                        *slot.lock().unwrap() = Some(handle);
                        Err::<(), _>(cocoindex::Error::engine("group failed"))
                    }
                })
                .await?;
            Ok::<_, cocoindex::Error>(())
        })
        .await;

    assert!(result.unwrap_err().to_string().contains("group failed"));

    let mut group_handle = group_handle_slot.lock().unwrap().take().unwrap();
    loop {
        if group_handle.changed().await.unwrap().is_done() {
            break;
        }
    }
}

#[tokio::test]
async fn ctx_auto_refresh_runs_once_in_catchup_mode() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let (app, _dir) = temp_app("auto_refresh_catchup").await;
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_update = calls.clone();

    app.update(move |ctx| async move {
        ctx.auto_refresh(&"poller", Duration::from_millis(1), move |_ctx| {
            let calls = calls_for_update.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .await
    })
    .await
    .unwrap();

    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn ctx_auto_refresh_live_continues_after_post_ready_cycle_error() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;

    let (app, _dir) = temp_app("auto_refresh_live_error_continue").await;
    let calls = Arc::new(AtomicUsize::new(0));
    let third_call = Arc::new(Notify::new());
    let calls_for_update = calls.clone();
    let notify_for_update = third_call.clone();

    let handle = app
        .start_update_with_options(
            cocoindex::UpdateOptions {
                full_reprocess: false,
                live: true,
                ..cocoindex::UpdateOptions::default()
            },
            move |ctx| async move {
                ctx.auto_refresh(&"poller", Duration::from_millis(5), move |_ctx| {
                    let calls = calls_for_update.clone();
                    let notify = notify_for_update.clone();
                    async move {
                        let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
                        if call == 2 {
                            return Err(cocoindex::Error::engine("cycle failed"));
                        }
                        if call >= 3 {
                            notify.notify_waiters();
                        }
                        Ok(())
                    }
                })
                .await
            },
        )
        .unwrap();

    tokio::time::timeout(Duration::from_secs(5), third_call.notified())
        .await
        .unwrap();
    assert!(calls.load(Ordering::SeqCst) >= 3);

    let _ = app.drop_state().await;
    let _ = tokio::time::timeout(Duration::from_secs(5), handle.result()).await;
}

// ---------------------------------------------------------------------------
// ctx.map()
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ctx_map() {
    let (app, _dir) = temp_app("ctx_map").await;
    app.update(|ctx| async move {
        let items = vec![1, 2, 3, 4, 5];
        let results: Vec<i32> = ctx.map(items, |x| async move { Ok(x * x) }).await?;
        assert_eq!(results, vec![1, 4, 9, 16, 25]);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn ctx_map_error_propagation() {
    let (app, _dir) = temp_app("ctx_map_err").await;
    let result = app
        .update(|ctx| async move {
            let items = vec![1, 2, 3];
            let _: Vec<i32> = ctx
                .map(items, |x| async move {
                    if x == 2 {
                        Err(cocoindex::Error::engine("boom"))
                    } else {
                        Ok(x)
                    }
                })
                .await?;
            Ok(())
        })
        .await;
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// ctx.batch()
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ctx_batch_all_miss() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let (app, _dir) = temp_app("batch_miss").await;
    let batch_calls = Arc::new(AtomicUsize::new(0));

    let calls = batch_calls.clone();
    app.update(|ctx| async move {
        let items = vec![("a", 10), ("b", 20), ("c", 30)];
        let results: Vec<i32> = ctx
            .batch(items, |(key, _)| key.to_string(), {
                let calls = calls.clone();
                move |_ctx, misses| async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    // Process all misses in one batch call
                    Ok(misses.iter().map(|(_, v)| v * 2).collect())
                }
            })
            .await?;
        assert_eq!(results, vec![20, 40, 60]);
        Ok(())
    })
    .await
    .unwrap();

    // Batch function called exactly once (not per-item).
    assert_eq!(batch_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn ctx_batch_cache_hit() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let (app, _dir) = temp_app("batch_hit").await;
    let batch_calls = Arc::new(AtomicUsize::new(0));

    // First run: all misses.
    let calls = batch_calls.clone();
    app.update(|ctx| async move {
        let items = vec![("x", 1), ("y", 2)];
        let results: Vec<i32> = ctx
            .batch(items, |(key, _)| key.to_string(), {
                let calls = calls.clone();
                move |_ctx, misses| async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(misses.iter().map(|(_, v)| v * 100).collect())
                }
            })
            .await?;
        assert_eq!(results, vec![100, 200]);
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(batch_calls.load(Ordering::SeqCst), 1);

    // Second run: all hits — batch function should NOT be called.
    let calls = batch_calls.clone();
    app.update(|ctx| async move {
        let items = vec![("x", 1), ("y", 2)];
        let results: Vec<i32> = ctx
            .batch(items, |(key, _)| key.to_string(), {
                let calls = calls.clone();
                move |_ctx, misses| async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(misses.iter().map(|(_, v)| v * 999).collect())
                }
            })
            .await?;
        assert_eq!(results, vec![100, 200]); // Cached values from first run.
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(batch_calls.load(Ordering::SeqCst), 1); // Not called again.
}

#[tokio::test]
async fn ctx_batch_partial_hit() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let (app, _dir) = temp_app("batch_partial").await;
    let batch_calls = Arc::new(AtomicUsize::new(0));

    // First run: cache "a" and "b".
    let calls = batch_calls.clone();
    app.update(|ctx| async move {
        let items = vec![("a", 1), ("b", 2)];
        let results: Vec<i32> = ctx
            .batch(items, |(key, _)| key.to_string(), {
                let calls = calls.clone();
                move |_ctx, misses| async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(misses.iter().map(|(_, v)| v * 10).collect())
                }
            })
            .await?;
        assert_eq!(results, vec![10, 20]);
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(batch_calls.load(Ordering::SeqCst), 1);

    // Second run: "a" hit, "c" miss → batch receives only "c".
    let calls = batch_calls.clone();
    app.update(|ctx| async move {
        let items = vec![("a", 1), ("c", 3)];
        let results: Vec<i32> = ctx
            .batch(items, |(key, _)| key.to_string(), {
                let calls = calls.clone();
                move |_ctx, misses| async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    // Only "c" should be here.
                    assert_eq!(misses.len(), 1);
                    assert_eq!(misses[0].0, "c");
                    Ok(misses.iter().map(|(_, v)| v * 10).collect())
                }
            })
            .await?;
        assert_eq!(results[0], 10); // "a" from cache
        assert_eq!(results[1], 30); // "c" freshly computed
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(batch_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn ctx_batch_error_propagation() {
    let (app, _dir) = temp_app("batch_err").await;
    let result = app
        .update(|ctx| async move {
            let items = vec![1, 2, 3];
            let _: Vec<i32> = ctx
                .batch(
                    items,
                    |x| *x,
                    |_ctx, _misses| async move { Err(cocoindex::Error::engine("batch failed")) },
                )
                .await?;
            Ok(())
        })
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn ctx_batch_serialization_error_clears_pending_memo() {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct ItemValue {
        value: i32,
        fail: bool,
    }

    impl Serialize for ItemValue {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            if self.fail {
                Err(serde::ser::Error::custom("forced serialization failure"))
            } else {
                self.value.serialize(serializer)
            }
        }
    }

    impl<'de> Deserialize<'de> for ItemValue {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            let value = i32::deserialize(deserializer)?;
            Ok(Self { value, fail: false })
        }
    }

    let (app, _dir) = temp_app("batch_serialize_fail").await;
    let first_calls = Arc::new(AtomicUsize::new(0));
    let first_calls_for_run = first_calls.clone();
    let result = app
        .update(|ctx| async move {
            let items = vec![1, 2, 3];
            let _: Vec<ItemValue> = ctx
                .batch(items, |x| *x, {
                    let calls = first_calls_for_run.clone();
                    move |_ctx, misses| {
                        calls.fetch_add(1, Ordering::SeqCst);
                        let out = misses
                            .into_iter()
                            .map(|value| ItemValue {
                                value: value * 2,
                                fail: value == 2,
                            })
                            .collect();
                        async move { Ok(out) }
                    }
                })
                .await?;
            Ok(())
        })
        .await;
    assert!(result.is_err());
    assert_eq!(first_calls.load(Ordering::SeqCst), 1);

    let second_calls = Arc::new(AtomicUsize::new(0));
    let second_calls_for_run = second_calls.clone();
    app.update(|ctx| async move {
        let items = vec![1, 2, 3];
        let results: Vec<ItemValue> = ctx
            .batch(items, |x| *x, {
                let calls = second_calls_for_run.clone();
                move |_ctx, misses| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let out = misses
                        .into_iter()
                        .map(|value| ItemValue {
                            value: value * 2,
                            fail: false,
                        })
                        .collect();
                    async move { Ok(out) }
                }
            })
            .await?;
        let values: Vec<i32> = results.into_iter().map(|value| value.value).collect();
        assert_eq!(values, vec![2, 4, 6]);
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(second_calls.load(Ordering::SeqCst), 1);

    let cached_calls = Arc::new(AtomicUsize::new(0));
    let cached_calls_for_run = cached_calls.clone();
    app.update(|ctx| async move {
        let items = vec![1, 2, 3];
        let results: Vec<ItemValue> = ctx
            .batch(items, |x| *x, {
                let calls = cached_calls_for_run.clone();
                move |_ctx, misses| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let out = misses
                        .into_iter()
                        .map(|value| ItemValue {
                            value: value * 99,
                            fail: false,
                        })
                        .collect();
                    async move { Ok(out) }
                }
            })
            .await?;
        let values: Vec<i32> = results.into_iter().map(|value| value.value).collect();
        assert_eq!(values, vec![2, 4, 6]);
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(cached_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn ctx_batch_fingerprint_error_clears_pending_memo() {
    use serde::{Serialize, Serializer};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Copy)]
    struct Key {
        value: i32,
        fail: bool,
    }

    impl Serialize for Key {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            if self.fail {
                Err(serde::ser::Error::custom(
                    "forced key serialization failure",
                ))
            } else {
                self.value.serialize(serializer)
            }
        }
    }

    let (app, _dir) = temp_app("batch_fingerprint_fail").await;
    let first_calls = Arc::new(AtomicUsize::new(0));
    let first_calls_for_run = first_calls.clone();
    let result = app
        .update(|ctx| async move {
            let items = vec![
                Key {
                    value: 1,
                    fail: false,
                },
                Key {
                    value: 2,
                    fail: true,
                },
            ];
            let _: Vec<i32> = ctx
                .batch(items, |key| *key, {
                    let calls = first_calls_for_run.clone();
                    move |_ctx, misses| {
                        calls.fetch_add(1, Ordering::SeqCst);
                        let out = misses.into_iter().map(|value| value.value * 2).collect();
                        async move { Ok(out) }
                    }
                })
                .await?;
            Ok(())
        })
        .await;
    assert!(result.is_err());
    assert_eq!(first_calls.load(Ordering::SeqCst), 0);

    let second_calls = Arc::new(AtomicUsize::new(0));
    let second_calls_for_run = second_calls.clone();
    app.update(|ctx| async move {
        let items = vec![Key {
            value: 1,
            fail: false,
        }];
        let values: Vec<i32> = ctx
            .batch(items, |key| *key, {
                let calls = second_calls_for_run.clone();
                move |_ctx, misses| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let out = misses.into_iter().map(|value| value.value * 2).collect();
                    async move { Ok(out) }
                }
            })
            .await?;
        assert_eq!(values, vec![2]);
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(second_calls.load(Ordering::SeqCst), 1);
}

// ---------------------------------------------------------------------------
// #[cocoindex::function(memo, batching)] macro
// ---------------------------------------------------------------------------

mod batch_macro_miss {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static BATCH_CALLS: AtomicUsize = AtomicUsize::new(0);

    /// The macro wraps this so `items` receives only cache misses.
    /// `ctx` is available inside (unlike `memo`).
    #[cocoindex::function(memo, batching)]
    async fn process_items(
        ctx: &cocoindex::Ctx,
        items: Vec<i32>,
    ) -> cocoindex::Result<Vec<String>> {
        BATCH_CALLS.fetch_add(1, Ordering::SeqCst);
        assert!(ctx.has_pipeline_context());
        Ok(items.iter().map(|x| format!("v{x}")).collect())
    }

    #[tokio::test]
    async fn function_macro_batch_all_miss() {
        // Code hash constant should exist.
        let hash = __COCO_FN_HASH_PROCESS_ITEMS;
        assert_ne!(hash, 0);

        let before = BATCH_CALLS.load(Ordering::SeqCst);
        let (app, _dir) = temp_app("fn_batch_miss").await;

        app.update(|ctx| async move {
            let results = process_items(&ctx, vec![1, 2, 3]).await?;
            assert_eq!(results, vec!["v1", "v2", "v3"]);
            Ok(())
        })
        .await
        .unwrap();

        let delta = BATCH_CALLS.load(Ordering::SeqCst) - before;
        assert_eq!(delta, 1);
    }
}

mod batch_macro_hit {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static BATCH_CALLS: AtomicUsize = AtomicUsize::new(0);

    #[cocoindex::function(memo, batching)]
    async fn process_items(
        ctx: &cocoindex::Ctx,
        items: Vec<i32>,
    ) -> cocoindex::Result<Vec<String>> {
        BATCH_CALLS.fetch_add(1, Ordering::SeqCst);
        assert!(ctx.has_pipeline_context());
        Ok(items.iter().map(|x| format!("v{x}")).collect())
    }

    #[tokio::test]
    async fn function_macro_batch_cache_hit() {
        let (app, _dir) = temp_app("fn_batch_hit").await;

        // First run — all misses.
        let before = BATCH_CALLS.load(Ordering::SeqCst);
        app.update(|ctx| async move {
            let results = process_items(&ctx, vec![10, 20]).await?;
            assert_eq!(results, vec!["v10", "v20"]);
            Ok(())
        })
        .await
        .unwrap();
        let delta = BATCH_CALLS.load(Ordering::SeqCst) - before;
        assert_eq!(delta, 1);

        // Second run — all cached.
        let before = BATCH_CALLS.load(Ordering::SeqCst);
        app.update(|ctx| async move {
            let results = process_items(&ctx, vec![10, 20]).await?;
            assert_eq!(results, vec!["v10", "v20"]);
            Ok(())
        })
        .await
        .unwrap();
        // Batch function should NOT be called again.
        let delta = BATCH_CALLS.load(Ordering::SeqCst) - before;
        assert_eq!(delta, 0);
    }
}

mod batch_macro_partial {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static BATCH_CALLS: AtomicUsize = AtomicUsize::new(0);

    #[cocoindex::function(memo, batching)]
    async fn process(ctx: &cocoindex::Ctx, items: Vec<i32>) -> cocoindex::Result<Vec<i32>> {
        BATCH_CALLS.fetch_add(1, Ordering::SeqCst);
        let _ = ctx;
        Ok(items.iter().map(|x| x * 100).collect())
    }

    #[tokio::test]
    async fn function_macro_batch_partial_hit() {
        let (app, _dir) = temp_app("fn_batch_partial").await;

        // First run: cache items 1 and 2.
        let before = BATCH_CALLS.load(Ordering::SeqCst);
        app.update(|ctx| async move {
            let results = process(&ctx, vec![1, 2]).await?;
            assert_eq!(results, vec![100, 200]);
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(BATCH_CALLS.load(Ordering::SeqCst) - before, 1);

        // Second run: 1 (hit) + 3 (miss) → batch only gets [3].
        let before = BATCH_CALLS.load(Ordering::SeqCst);
        app.update(|ctx| async move {
            let results = process(&ctx, vec![1, 3]).await?;
            assert_eq!(results[0], 100); // cached
            assert_eq!(results[1], 300); // fresh
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(BATCH_CALLS.load(Ordering::SeqCst) - before, 1);
    }
}

mod batch_macro_code_hash {
    #[cocoindex::function(memo, batching)]
    async fn batch_a(ctx: &cocoindex::Ctx, items: Vec<i32>) -> cocoindex::Result<Vec<i32>> {
        let _ = ctx;
        Ok(items.iter().map(|x| x + 1).collect())
    }

    #[cocoindex::function(memo, batching)]
    async fn batch_b(ctx: &cocoindex::Ctx, items: Vec<i32>) -> cocoindex::Result<Vec<i32>> {
        let _ = ctx;
        Ok(items.iter().map(|x| x + 2).collect())
    }

    #[test]
    fn function_macro_batch_code_hash_differs() {
        // Different bodies produce different code hashes.
        assert_ne!(__COCO_FN_HASH_BATCH_A, __COCO_FN_HASH_BATCH_B);
    }
}

mod batch_macro_ctx_access {
    /// Prove that `ctx.get()` works inside a batch body.
    #[cocoindex::function(memo, batching)]
    async fn batch_with_resource(
        ctx: &cocoindex::Ctx,
        items: Vec<String>,
    ) -> cocoindex::Result<Vec<String>> {
        let prefix = ctx.get_or_err::<String>().unwrap();
        Ok(items.iter().map(|s| format!("{prefix}:{s}")).collect())
    }

    #[tokio::test]
    async fn function_macro_batch_ctx_get() {
        let dir = tempfile::tempdir().unwrap();
        let app = cocoindex::App::builder("fn_batch_ctx")
            .db_path(dir.path().join("lmdb"))
            .provide("hello".to_string())
            .build()
            .await
            .unwrap();

        app.update(|ctx| async move {
            let results = batch_with_resource(&ctx, vec!["a".into(), "b".into()]).await?;
            assert_eq!(results, vec!["hello:a", "hello:b"]);
            Ok(())
        })
        .await
        .unwrap();
    }
}

// ---------------------------------------------------------------------------
// #[cocoindex::function(batching)] — batch without memo
// ---------------------------------------------------------------------------

mod batching_no_memo {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static CALLS: AtomicUsize = AtomicUsize::new(0);

    /// `batching` alone: no per-item caching. Body gets ALL items every time.
    #[cocoindex::function(batching)]
    async fn process_all(ctx: &cocoindex::Ctx, items: Vec<i32>) -> cocoindex::Result<Vec<String>> {
        CALLS.fetch_add(1, Ordering::SeqCst);
        assert!(ctx.has_pipeline_context());
        Ok(items.iter().map(|x| format!("v{x}")).collect())
    }

    #[tokio::test]
    async fn batching_only_processes_all_items() {
        let hash = __COCO_FN_HASH_PROCESS_ALL;
        assert_ne!(hash, 0);

        let (app, _dir) = temp_app("batching_only").await;

        let before = CALLS.load(Ordering::SeqCst);
        app.update(|ctx| async move {
            let results = process_all(&ctx, vec![1, 2, 3]).await?;
            assert_eq!(results, vec!["v1", "v2", "v3"]);
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(CALLS.load(Ordering::SeqCst) - before, 1);
    }
}

mod batching_no_memo_no_cache {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static CALLS: AtomicUsize = AtomicUsize::new(0);

    #[cocoindex::function(batching)]
    async fn process_all(ctx: &cocoindex::Ctx, items: Vec<i32>) -> cocoindex::Result<Vec<String>> {
        CALLS.fetch_add(1, Ordering::SeqCst);
        let _ = ctx;
        Ok(items.iter().map(|x| format!("v{x}")).collect())
    }

    #[tokio::test]
    async fn batching_only_no_cache_on_second_run() {
        let (app, _dir) = temp_app("batching_no_cache").await;

        // First run.
        let before = CALLS.load(Ordering::SeqCst);
        app.update(|ctx| async move {
            let results = process_all(&ctx, vec![10, 20]).await?;
            assert_eq!(results, vec!["v10", "v20"]);
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(CALLS.load(Ordering::SeqCst) - before, 1);

        // Second run — body is called again (no caching).
        let before = CALLS.load(Ordering::SeqCst);
        app.update(|ctx| async move {
            let results = process_all(&ctx, vec![10, 20]).await?;
            assert_eq!(results, vec!["v10", "v20"]);
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(
            CALLS.load(Ordering::SeqCst) - before,
            1,
            "batching without memo should call body again"
        );
    }
}

// ---------------------------------------------------------------------------
// Mock-API integration tests: one per macro mode
// ---------------------------------------------------------------------------

/// Mock API client: counts calls, transforms input → output.
#[derive(Clone)]
struct MockApi {
    calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl MockApi {
    fn new() -> Self {
        Self {
            calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }
    fn call_count(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::SeqCst)
    }
    /// Simulate an API call: increment counter, return "api:<input>".
    async fn call(&self, input: &str) -> cocoindex::Result<String> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // Simulate latency
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        Ok(format!("api:{input}"))
    }
    /// Batch API call: increment counter once, return "api:<input>" for each.
    async fn call_batch(&self, inputs: &[String]) -> cocoindex::Result<Vec<String>> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        Ok(inputs.iter().map(|s| format!("api:{s}")).collect())
    }
}

// -- Mode 1: #[cocoindex::function] (bare — change tracking only) -----------

mod mock_bare {
    use super::*;

    /// Bare function: no memo, no batching. Body runs every time.
    #[cocoindex::function]
    async fn analyze(ctx: &cocoindex::Ctx, input: &str) -> cocoindex::Result<String> {
        let api = ctx.get_or_err::<MockApi>().unwrap().clone();
        api.call(input).await
    }

    #[tokio::test]
    async fn bare_calls_api_every_time() {
        let api = MockApi::new();
        let dir = tempfile::tempdir().unwrap();
        let app = cocoindex::App::builder("mock_bare")
            .db_path(dir.path().join("lmdb"))
            .provide(api.clone())
            .build()
            .await
            .unwrap();

        // First run.
        app.update(|ctx| async move {
            let result = analyze(&ctx, "hello").await?;
            assert_eq!(result, "api:hello");
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(api.call_count(), 1);

        // Second run — same input, body runs again (no caching).
        app.update(|ctx| async move {
            let result = analyze(&ctx, "hello").await?;
            assert_eq!(result, "api:hello");
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(
            api.call_count(),
            2,
            "bare function should call API every time"
        );
    }
}

// -- Mode 2: #[cocoindex::function(memo)] -----------------------------------

mod mock_memo {
    use super::*;

    /// Memo function: cached by args. The body is a `'static` closure,
    /// so `ctx` is NOT available inside. Clone the API before the body,
    /// then use bare `#[cocoindex::function]` + manual `ctx.memo()`.
    ///
    /// This is the realistic pattern for memoizing an API call.
    #[cocoindex::function]
    async fn analyze(ctx: &cocoindex::Ctx, input: &str) -> cocoindex::Result<String> {
        let api = ctx.get_or_err::<MockApi>().unwrap().clone();
        let key = (__COCO_FN_HASH_ANALYZE, input.to_owned());
        let input = input.to_owned();
        ctx.memo(&key, move |_ctx| async move { api.call(&input).await })
            .await
    }

    #[tokio::test]
    async fn memo_caches_on_second_run() {
        let api = MockApi::new();
        let dir = tempfile::tempdir().unwrap();
        let app = cocoindex::App::builder("mock_memo")
            .db_path(dir.path().join("lmdb"))
            .provide(api.clone())
            .build()
            .await
            .unwrap();

        // First run — cache miss, API called.
        app.update(|ctx| async move {
            let result = analyze(&ctx, "hello").await?;
            assert_eq!(result, "api:hello");
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(api.call_count(), 1);

        // Second run — same input, cache hit, API NOT called.
        app.update(|ctx| async move {
            let result = analyze(&ctx, "hello").await?;
            assert_eq!(result, "api:hello");
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(api.call_count(), 1, "memo should return cached result");

        // Third run — different input, cache miss, API called again.
        app.update(|ctx| async move {
            let result = analyze(&ctx, "world").await?;
            assert_eq!(result, "api:world");
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(api.call_count(), 2, "different key should miss cache");
    }
}

// -- Mode 3: #[cocoindex::function(batching)] — no memo ---------------------

mod mock_batching {
    use super::*;

    /// Batching only: processes all items, no per-item caching.
    #[cocoindex::function(batching)]
    async fn analyze_batch(
        ctx: &cocoindex::Ctx,
        items: Vec<String>,
    ) -> cocoindex::Result<Vec<String>> {
        let api = ctx.try_get::<MockApi>().unwrap().clone();
        api.call_batch(&items).await
    }

    #[tokio::test]
    async fn batching_calls_api_every_time() {
        let api = MockApi::new();
        let dir = tempfile::tempdir().unwrap();
        let app = cocoindex::App::builder("mock_batching")
            .db_path(dir.path().join("lmdb"))
            .provide(api.clone())
            .build()
            .await
            .unwrap();

        // First run.
        app.update(|ctx| async move {
            let results = analyze_batch(&ctx, vec!["a".into(), "b".into(), "c".into()]).await?;
            assert_eq!(results, vec!["api:a", "api:b", "api:c"]);
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(api.call_count(), 1);

        // Second run — same items, API called again (no caching).
        app.update(|ctx| async move {
            let results = analyze_batch(&ctx, vec!["a".into(), "b".into(), "c".into()]).await?;
            assert_eq!(results, vec!["api:a", "api:b", "api:c"]);
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(
            api.call_count(),
            2,
            "batching without memo should call API every time"
        );
    }
}

// -- Mode 4: #[cocoindex::function(memo, batching)] -------------------------

mod mock_memo_batching {
    use super::*;

    /// Memo + batching: per-item caching, body gets only misses.
    #[cocoindex::function(memo, batching)]
    async fn analyze_batch(
        ctx: &cocoindex::Ctx,
        items: Vec<String>,
    ) -> cocoindex::Result<Vec<String>> {
        let api = ctx.try_get::<MockApi>().unwrap().clone();
        api.call_batch(&items).await
    }

    #[tokio::test]
    async fn memo_batching_caches_per_item() {
        let api = MockApi::new();
        let dir = tempfile::tempdir().unwrap();
        let app = cocoindex::App::builder("mock_memo_batch")
            .db_path(dir.path().join("lmdb"))
            .provide(api.clone())
            .build()
            .await
            .unwrap();

        // First run — all misses, API called once with all 3 items.
        app.update(|ctx| async move {
            let results = analyze_batch(&ctx, vec!["a".into(), "b".into(), "c".into()]).await?;
            assert_eq!(results, vec!["api:a", "api:b", "api:c"]);
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(api.call_count(), 1);

        // Second run — same items, all cached, API NOT called.
        app.update(|ctx| async move {
            let results = analyze_batch(&ctx, vec!["a".into(), "b".into(), "c".into()]).await?;
            assert_eq!(results, vec!["api:a", "api:b", "api:c"]);
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(
            api.call_count(),
            1,
            "all items cached — API should not be called"
        );

        // Third run — 2 cached + 1 new, API called once with only the new item.
        app.update(|ctx| async move {
            let results = analyze_batch(&ctx, vec!["a".into(), "d".into(), "c".into()]).await?;
            assert_eq!(results, vec!["api:a", "api:d", "api:c"]);
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(
            api.call_count(),
            2,
            "only 1 miss — API called once for [\"d\"]"
        );
    }
}

// ---------------------------------------------------------------------------
// fs::walk + FileEntry
// ---------------------------------------------------------------------------

#[test]
fn fs_walk_integration() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("main.rs"), "fn main() {}").unwrap();
    std::fs::write(dir.path().join("lib.rs"), "pub mod foo;").unwrap();
    std::fs::write(dir.path().join("readme.md"), "# Hello").unwrap();

    let files = cocoindex::fs::walk(dir.path(), &["**/*.rs"]).unwrap();
    assert_eq!(files.len(), 2);

    let file = &files[0]; // lib.rs (sorted)
    assert_eq!(file.stem(), "lib");
    assert_eq!(file.content_str().unwrap(), "pub mod foo;");
}

// ---------------------------------------------------------------------------
// Error Handling and API Constraints
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ctx_mount_each_rejects_duplicate_keys() {
    let (app, _dir) = temp_app("mount_each_dupes").await;
    let result = app
        .update(|ctx| async move {
            // "alpha" is duplicated
            let items = vec![("alpha", 1), ("beta", 2), ("alpha", 3)];
            let _: Vec<i32> = ctx
                .mount_each(
                    items,
                    |&(name, _)| name.to_string(), // duplicate 'alpha' keys
                    |_child_ctx, (_, value)| async move { Ok(value) },
                )
                .await?;
            Ok(())
        })
        .await;

    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("duplicate key `alpha`")
    );
}

#[tokio::test]
async fn ctx_batch_rejects_duplicate_keys() {
    let (app, _dir) = temp_app("batch_each_dupes").await;
    let result = app
        .update(|ctx| async move {
            // "gamma" is duplicated
            let items = vec![("gamma", 1), ("gamma", 2)];
            let _: Vec<i32> = ctx
                .batch(
                    items,
                    |&(name, _)| name.to_string(), // duplicate 'gamma' keys
                    |_ctx, _misses| async move { Ok(vec![1, 2]) },
                )
                .await?;
            Ok(())
        })
        .await;

    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("duplicate cache keys generated")
    );
}

// ---------------------------------------------------------------------------
// DirTarget — declarative directory target (write / skip / orphan-delete)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dir_target_writes_skips_unchanged_and_reconciles_orphans() {
    use cocoindex::DirTarget;
    use std::fs;
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("lmdb");
    let out = dir.path().join("out");

    // Run 1: declare a.txt and a nested sub/b.txt.
    let app = App::builder("dir_target_test")
        .db_path(&db)
        .build()
        .await
        .unwrap();
    let out1 = out.clone();
    app.update(move |ctx| async move {
        let t = DirTarget::mount(&ctx, &out1)?;
        t.declare_file(&ctx, "a.txt", b"AAA")?;
        t.declare_file(&ctx, "sub/b.txt", b"BBB")?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(fs::read(out.join("a.txt")).unwrap(), b"AAA");
    assert_eq!(fs::read(out.join("sub/b.txt")).unwrap(), b"BBB");
    let a_mtime = fs::metadata(out.join("a.txt")).unwrap().modified().unwrap();
    drop(app);

    // Run 2: declare only a.txt (unchanged). b.txt is now orphaned → deleted;
    // a.txt is unchanged → must NOT be rewritten (mtime preserved).
    tokio::time::sleep(Duration::from_millis(40)).await;
    let app = App::builder("dir_target_test")
        .db_path(&db)
        .build()
        .await
        .unwrap();
    let out2 = out.clone();
    app.update(move |ctx| async move {
        let t = DirTarget::mount(&ctx, &out2)?;
        t.declare_file(&ctx, "a.txt", b"AAA")?;
        Ok(())
    })
    .await
    .unwrap();
    assert!(out.join("a.txt").exists(), "kept file should remain");
    assert!(
        !out.join("sub/b.txt").exists(),
        "orphaned file (no longer declared) must be deleted"
    );
    let a_mtime2 = fs::metadata(out.join("a.txt")).unwrap().modified().unwrap();
    assert_eq!(
        a_mtime, a_mtime2,
        "unchanged file must be skipped (not rewritten)"
    );
    drop(app);

    // Run 3: change a.txt content → rewritten.
    let app = App::builder("dir_target_test")
        .db_path(&db)
        .build()
        .await
        .unwrap();
    let out3 = out.clone();
    app.update(move |ctx| async move {
        let t = DirTarget::mount(&ctx, &out3)?;
        t.declare_file(&ctx, "a.txt", b"ZZZ")?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(fs::read(out.join("a.txt")).unwrap(), b"ZZZ");
}

#[tokio::test]
async fn dir_target_deletes_file_when_source_disappears_via_mount_each() {
    use cocoindex::DirTarget;

    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("lmdb");
    let out = dir.path().join("out");

    // Helper run: mount the target at the root and declare one output file per
    // input from child components.
    async fn run(db: &std::path::Path, out: &std::path::Path, names: Vec<&'static str>) {
        let app = App::builder("dir_target_mount_each")
            .db_path(db)
            .build()
            .await
            .unwrap();
        let out = out.to_path_buf();
        app.update(move |ctx| async move {
            let t = DirTarget::mount(&ctx, &out)?;
            ctx.mount_each(
                names,
                |n| n.to_string(),
                move |child, n| {
                    let t = t.clone();
                    async move {
                        t.declare_file(&child, &format!("{n}.out"), n.as_bytes())?;
                        Ok(())
                    }
                },
            )
            .await?;
            Ok(())
        })
        .await
        .unwrap();
    }

    run(&db, &out, vec!["alpha", "beta"]).await;
    assert!(out.join("alpha.out").exists());
    assert!(out.join("beta.out").exists());

    // Drop "beta" from the input set → beta.out must be reconciled away.
    run(&db, &out, vec!["alpha"]).await;
    assert!(out.join("alpha.out").exists());
    assert!(
        !out.join("beta.out").exists(),
        "output for a removed source must be deleted"
    );
}

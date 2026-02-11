//! Integration tests for the pipeline: App::update, memo::cached, sync API.

use cocoindex::App;

/// Helper: create an App with a temp LMDB directory.
fn temp_app(name: &str) -> (App, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let app = App::builder(name)
        .db_path(dir.path().join("lmdb"))
        .build()
        .unwrap();
    (app, dir)
}

// ---------------------------------------------------------------------------
// Sync API: update_blocking
// ---------------------------------------------------------------------------

#[test]
fn update_blocking_runs_closure() {
    let (app, _dir) = temp_app("sync_basic");
    let result = app.update_blocking(|_ctx| async move { Ok(()) });
    assert!(result.is_ok());
}

#[test]
fn update_blocking_provides_pipeline_context() {
    let (app, _dir) = temp_app("sync_ctx");
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
        .build()
        .unwrap();

    app.update_blocking(|ctx| async move {
        let config = ctx.get::<Config>();
        assert_eq!(config.value, 42);
        Ok(())
    })
    .unwrap();
}

// ---------------------------------------------------------------------------
// Async API: update
// ---------------------------------------------------------------------------

#[tokio::test]
async fn update_async_runs_closure() {
    let (app, _dir) = temp_app("async_basic");
    app.update(|_ctx| async move { Ok(()) }).await.unwrap();
}

#[tokio::test]
async fn update_async_provides_pipeline_context() {
    let (app, _dir) = temp_app("async_ctx");
    app.update(|ctx| async move {
        assert!(ctx.has_pipeline_context());
        Ok(())
    })
    .await
    .unwrap();
}

// ---------------------------------------------------------------------------
// Memoization: cache hit / miss
// ---------------------------------------------------------------------------

#[tokio::test]
async fn memo_cached_executes_on_first_call() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let (app, _dir) = temp_app("memo_first");
    let call_count = Arc::new(AtomicUsize::new(0));

    let count = call_count.clone();
    app.update(|ctx| async move {
        let _result: i32 = cocoindex::memo::cached(&ctx, &"key1", move || {
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let (app, _dir) = temp_app("memo_cache_hit");
    let call_count = Arc::new(AtomicUsize::new(0));

    // First run: should execute the closure.
    let count = call_count.clone();
    app.update(|ctx| async move {
        let result: i32 = cocoindex::memo::cached(&ctx, &"stable_key", move || {
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
        let result: i32 = cocoindex::memo::cached(&ctx, &"stable_key", move || {
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
async fn memo_cached_reexecutes_on_key_change() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let (app, _dir) = temp_app("memo_cache_miss");
    let call_count = Arc::new(AtomicUsize::new(0));

    // First run with key "v1".
    let count = call_count.clone();
    app.update(|ctx| async move {
        let result: i32 = cocoindex::memo::cached(&ctx, &"v1", move || {
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
        let result: i32 = cocoindex::memo::cached(&ctx, &"v2", move || {
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let (app, _dir) = temp_app("memo_blocking");
    let call_count = Arc::new(AtomicUsize::new(0));

    // First run.
    let count = call_count.clone();
    app.update_blocking(|ctx| async move {
        let result: String = cocoindex::memo::cached(&ctx, &("file.rs", 42u64), move || {
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
        let result: String = cocoindex::memo::cached(&ctx, &("file.rs", 42u64), move || {
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let (app, _dir) = temp_app("drop_state");
    let call_count = Arc::new(AtomicUsize::new(0));

    // First run: populate cache.
    let count = call_count.clone();
    app.update_blocking(|ctx| async move {
        let _: i32 = cocoindex::memo::cached(&ctx, &"key", move || {
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
        let result: i32 = cocoindex::memo::cached(&ctx, &"key", move || {
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
    let (app, _dir) = temp_app("memo_error");
    let result = app
        .update(|ctx| async move {
            let _: i32 = cocoindex::memo::cached(&ctx, &"key", || async {
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
    let (app, _dir) = temp_app("sync_error");
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

    let (app, _dir) = temp_app("memo_complex");

    // Run 1: store complex type.
    app.update(|ctx| async move {
        let entities: Vec<Entity> = cocoindex::memo::cached(&ctx, &"complex_key", || async {
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
        let entities: Vec<Entity> = cocoindex::memo::cached(&ctx, &"complex_key", || async {
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

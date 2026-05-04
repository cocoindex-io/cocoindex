//! Integration tests for the pipeline: App::update, memo::cached, sync API.

use cocoindex::App;
use tokio::time::{Duration, sleep};

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

    let (app, _dir) = temp_app("sync_missing_context");
    app.update_blocking(|ctx| async move {
        let err = ctx.get_or_err::<MissingConfig>().unwrap_err();
        assert!(
            err.to_string()
                .contains("type `pipeline::update_blocking_missing_context_returns_typed_error::MissingConfig` not provided")
        );
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

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

// ---------------------------------------------------------------------------
// App::open convenience
// ---------------------------------------------------------------------------

#[test]
fn app_open_convenience() {
    let dir = tempfile::tempdir().unwrap();
    let app = App::open("open_test", dir.path().join("lmdb")).unwrap();
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
    let app = App::open("run_stats", dir.path().join("lmdb")).unwrap();
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

    let (app, _dir) = temp_app("ctx_memo");
    let call_count = Arc::new(AtomicUsize::new(0));

    // First run
    let count = call_count.clone();
    app.update(|ctx| async move {
        let result: i32 = ctx
            .memo(&"memo_key", move || {
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
            .memo(&"memo_key", move || {
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

    let (app, _dir) = temp_app("ctx_scope");
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
    let (app, dir) = temp_app("ctx_write");
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
    let (app, dir) = temp_app("ctx_write_nested");
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

#[tokio::test]
async fn function_macro_bare() {
    // The macro should emit a code hash constant.
    let hash = __COCO_FN_HASH_BARE_FN;
    assert_ne!(hash, 0);

    // The function itself should still work normally.
    let (app, _dir) = temp_app("fn_bare");
    app.update(|ctx| async move {
        let result = bare_fn(&ctx, "hello").await?;
        assert_eq!(result, 1);
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
    async fn the_fn(ctx: &cocoindex::Ctx, key: &String) -> cocoindex::Result<i32> {
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
        let (app, _dir) = temp_app("fn_memo");

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
    async fn the_fn(ctx: &cocoindex::Ctx, key: &String) -> cocoindex::Result<i32> {
        CALLS.fetch_add(1, Ordering::SeqCst);
        let _ = &key;
        Ok(42)
    }

    #[tokio::test]
    async fn function_macro_memo_cache_hit() {
        CALLS.store(0, Ordering::SeqCst);
        let (app, _dir) = temp_app("fn_memo_hit");

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

// Two functions with same signature but different bodies produce different code hashes.
#[cocoindex::function(memo)]
async fn hash_fn_a(ctx: &cocoindex::Ctx, _key: &String) -> cocoindex::Result<i32> {
    Ok(111)
}

#[cocoindex::function(memo)]
async fn hash_fn_b(ctx: &cocoindex::Ctx, _key: &String) -> cocoindex::Result<i32> {
    Ok(222)
}

#[test]
fn function_macro_memo_code_hash_invalidation() {
    // Different function bodies must produce different code hashes.
    assert_ne!(__COCO_FN_HASH_HASH_FN_A, __COCO_FN_HASH_HASH_FN_B);
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

    let (app, _dir) = temp_app("mount_each");
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

    let (app, _dir) = temp_app("mount_each_memo");
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
                                move || async move {
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
                                move || async move {
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
    let (app, _dir) = temp_app("mount_each_order");
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

// ---------------------------------------------------------------------------
// ctx.map()
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ctx_map() {
    let (app, _dir) = temp_app("ctx_map");
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
    let (app, _dir) = temp_app("ctx_map_err");
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

    let (app, _dir) = temp_app("batch_miss");
    let batch_calls = Arc::new(AtomicUsize::new(0));

    let calls = batch_calls.clone();
    app.update(|ctx| async move {
        let items = vec![("a", 10), ("b", 20), ("c", 30)];
        let results: Vec<i32> = ctx
            .batch(items, |(key, _)| key.to_string(), {
                let calls = calls.clone();
                move |misses| async move {
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

    let (app, _dir) = temp_app("batch_hit");
    let batch_calls = Arc::new(AtomicUsize::new(0));

    // First run: all misses.
    let calls = batch_calls.clone();
    app.update(|ctx| async move {
        let items = vec![("x", 1), ("y", 2)];
        let results: Vec<i32> = ctx
            .batch(items, |(key, _)| key.to_string(), {
                let calls = calls.clone();
                move |misses| async move {
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
                move |misses| async move {
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

    let (app, _dir) = temp_app("batch_partial");
    let batch_calls = Arc::new(AtomicUsize::new(0));

    // First run: cache "a" and "b".
    let calls = batch_calls.clone();
    app.update(|ctx| async move {
        let items = vec![("a", 1), ("b", 2)];
        let results: Vec<i32> = ctx
            .batch(items, |(key, _)| key.to_string(), {
                let calls = calls.clone();
                move |misses| async move {
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
                move |misses| async move {
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
    let (app, _dir) = temp_app("batch_err");
    let result = app
        .update(|ctx| async move {
            let items = vec![1, 2, 3];
            let _: Vec<i32> = ctx
                .batch(
                    items,
                    |x| *x,
                    |_misses| async move { Err(cocoindex::Error::engine("batch failed")) },
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

    let (app, _dir) = temp_app("batch_serialize_fail");
    let first_calls = Arc::new(AtomicUsize::new(0));
    let first_calls_for_run = first_calls.clone();
    let result = app
        .update(|ctx| async move {
            let items = vec![1, 2, 3];
            let _: Vec<ItemValue> = ctx
                .batch(items, |x| *x, {
                    let calls = first_calls_for_run.clone();
                    move |misses| {
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
                move |misses| {
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
                move |misses| {
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

    let (app, _dir) = temp_app("batch_fingerprint_fail");
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
                    move |misses| {
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
                move |misses| {
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
        let (app, _dir) = temp_app("fn_batch_miss");

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
        let (app, _dir) = temp_app("fn_batch_hit");

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
        let (app, _dir) = temp_app("fn_batch_partial");

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

        let (app, _dir) = temp_app("batching_only");

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
        let (app, _dir) = temp_app("batching_no_cache");

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
        ctx.memo(&key, move || async move { api.call(&input).await })
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
    let (app, _dir) = temp_app("mount_each_dupes");
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
    let (app, _dir) = temp_app("batch_each_dupes");
    let result = app
        .update(|ctx| async move {
            // "gamma" is duplicated
            let items = vec![("gamma", 1), ("gamma", 2)];
            let _: Vec<i32> = ctx
                .batch(
                    items,
                    |&(name, _)| name.to_string(), // duplicate 'gamma' keys
                    |_misses| async move { Ok(vec![1, 2]) },
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

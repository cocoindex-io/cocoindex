//! `Ctx::mount_each` / `mount_each!` as background, streaming mounts (the
//! Python `coco.mount_each` shape), `use_mount_each` as their value-returning
//! form, and typed mount keys.
//!
//!   cargo test -p cocoindex --test mount_each

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cocoindex::prelude::*;
use cocoindex::{Environment, inspect};
use tokio::sync::Semaphore;

async fn temp_app(name: &str) -> (App, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let app = App::builder(name)
        .db_path(dir.path().join(".cocoindex_db"))
        .build()
        .await
        .unwrap();
    (app, dir)
}

/// The failing paths and their parents, as the environment's exception
/// handler saw them.
type SeenPaths = Arc<Mutex<Vec<(String, Option<String>)>>>;

async fn temp_app_recording_failures(name: &str) -> (App, tempfile::TempDir, SeenPaths) {
    let seen: SeenPaths = Arc::new(Mutex::new(Vec::new()));
    let dir = tempfile::tempdir().unwrap();
    let app = Environment::builder()
        .db_path(dir.path().join(".cocoindex_db"))
        .exception_handler({
            let seen = seen.clone();
            move |_err, exc| {
                seen.lock()
                    .unwrap()
                    .push((exc.stable_path.clone(), exc.parent_stable_path.clone()));
                Ok(())
            }
        })
        .build()
        .await
        .unwrap()
        .app(name)
        .await
        .unwrap();
    (app, dir, seen)
}

fn sorted(seen: &SeenPaths) -> Vec<(String, Option<String>)> {
    let mut v = seen.lock().unwrap().clone();
    v.sort();
    v
}

// ---------------------------------------------------------------------------
// Background semantics
// ---------------------------------------------------------------------------

/// The parent returns from `mount_each` while the children are still
/// running: the gate the children wait on is only opened afterwards, so a
/// foreground `mount_each` would never return. `ready()` then waits for all.
#[tokio::test]
async fn mount_each_mounts_children_in_the_background() {
    let (app, _dir) = temp_app("mount_each_background").await;
    let gate = Arc::new(Semaphore::new(0));
    let finished = Arc::new(AtomicUsize::new(0));

    let (gate_outer, finished_outer) = (gate.clone(), finished.clone());
    app.update(move |ctx| async move {
        let handle = ctx
            .mount_each(vec!["a", "b", "c"], |k| k.to_string(), {
                let gate = gate_outer.clone();
                let finished = finished_outer.clone();
                move |_child, _k| {
                    let gate = gate.clone();
                    let finished = finished.clone();
                    async move {
                        let _permit = gate.acquire().await.unwrap();
                        finished.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                }
            })
            .await?;
        assert_eq!(finished_outer.load(Ordering::SeqCst), 0);
        gate_outer.add_permits(3);
        handle.ready().await?;
        assert_eq!(finished_outer.load(Ordering::SeqCst), 3);
        Ok(())
    })
    .await
    .unwrap();
}

/// A lazy iterator that counts how many items were pulled.
struct Counted {
    next: usize,
    len: usize,
    pulled: Arc<AtomicUsize>,
}

impl Iterator for Counted {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        if self.next == self.len {
            return None;
        }
        self.next += 1;
        self.pulled.fetch_add(1, Ordering::SeqCst);
        Some(self.next - 1)
    }
}

/// `mount_each` consumes its items as children are admitted: with
/// `max_inflight_components = 2` and children that block, the iterator is
/// pulled at most three times (two running children, one waiting for a slot)
/// however many items it holds.
#[tokio::test]
async fn mount_each_streams_items_as_children_are_admitted() {
    let dir = tempfile::tempdir().unwrap();
    let app = Environment::builder()
        .db_path(dir.path().join(".cocoindex_db"))
        .max_inflight_components(2)
        .build()
        .await
        .unwrap()
        .app("mount_each_streams")
        .await
        .unwrap();
    let pulled = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Semaphore::new(0));
    let pulled_when_blocked = Arc::new(AtomicUsize::new(usize::MAX));

    // Opens the gate once the admitted children stop increasing, recording
    // how far the parent had read into its items by then.
    let observer = tokio::spawn({
        let (pulled, started, gate, pulled_when_blocked) = (
            pulled.clone(),
            started.clone(),
            gate.clone(),
            pulled_when_blocked.clone(),
        );
        async move {
            let mut last = usize::MAX;
            let mut stable_since = Instant::now();
            loop {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let now = started.load(Ordering::SeqCst);
                if now != last {
                    last = now;
                    stable_since = Instant::now();
                } else if now >= 2 && stable_since.elapsed() > Duration::from_millis(300) {
                    break;
                }
            }
            pulled_when_blocked.store(pulled.load(Ordering::SeqCst), Ordering::SeqCst);
            gate.add_permits(20);
        }
    });

    let (pulled_outer, started_outer, gate_outer) = (pulled.clone(), started.clone(), gate.clone());
    app.update(move |ctx| async move {
        let items = Counted {
            next: 0,
            len: 20,
            pulled: pulled_outer,
        };
        let handle = ctx
            .mount_each(items, |i| *i as i64, {
                let (started, gate) = (started_outer, gate_outer);
                move |_child, _i| {
                    let (started, gate) = (started.clone(), gate.clone());
                    async move {
                        started.fetch_add(1, Ordering::SeqCst);
                        let _permit = gate.acquire().await.unwrap();
                        Ok(())
                    }
                }
            })
            .await?;
        handle.ready().await?;
        Ok(())
    })
    .await
    .unwrap();
    observer.await.unwrap();

    let when_blocked = pulled_when_blocked.load(Ordering::SeqCst);
    assert!(
        (2..=3).contains(&when_blocked),
        "the parent must read only as far as admission allows, read {when_blocked} of 20"
    );
    assert_eq!(pulled.load(Ordering::SeqCst), 20);
    assert_eq!(started.load(Ordering::SeqCst), 20);
}

#[tokio::test]
async fn mount_each_rejects_duplicate_keys() {
    let (app, _dir) = temp_app("mount_each_dup").await;
    let err = app
        .update(|ctx| async move {
            ctx.mount_each(
                vec![("alpha", 1), ("beta", 2), ("alpha", 3)],
                |&(name, _)| name.to_string(),
                |_child, _item| async move { Ok(()) },
            )
            .await?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("duplicate key"),
        "unexpected error: {err}"
    );
}

/// An async source of items: a child is mounted per pair as it arrives.
#[tokio::test]
async fn mount_each_stream_mounts_each_pair() {
    let (app, _dir) = temp_app("mount_each_stream").await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_outer = seen.clone();
    app.update(move |ctx| async move {
        let items = futures::stream::iter(vec![("x", 1u32), ("y", 2)]);
        ctx.mount_each_stream(items, {
            let seen = seen_outer.clone();
            move |_child, value| {
                let seen = seen.clone();
                async move {
                    seen.lock().unwrap().push(value);
                    Ok(())
                }
            }
        })
        .await?
        .ready()
        .await?;
        Ok(())
    })
    .await
    .unwrap();
    let mut values = seen.lock().unwrap().clone();
    values.sort();
    assert_eq!(values, vec![1, 2]);
}

// ---------------------------------------------------------------------------
// Paths: `mount_each!` prefix and typed keys
// ---------------------------------------------------------------------------

#[cocoindex::function]
async fn fail_item(_ctx: &Ctx, _n: u32) -> Result<()> {
    Err(Error::engine("boom"))
}

/// `mount_each!(prefix, items, ...)` mounts each item at `prefix/key`: two
/// path parts, the prefix being the parent, as Python's `mount_each` subpath.
/// The prefix defaults to the function's name.
#[tokio::test]
async fn mount_each_macro_mounts_under_the_prefix() {
    let (app, _dir, seen) = temp_app_recording_failures("mount_each_prefix").await;
    app.update(|ctx| async move {
        let ctx = &ctx;
        let items = vec![("k".to_string(), 1u32)];
        cocoindex::mount_each!("items", items, |n| fail_item(ctx, n))
            .await?
            .ready()
            .await?;
        let items = vec![(7i64, 2u32)];
        cocoindex::mount_each!(items, |n| fail_item(ctx, n))
            .await?
            .ready()
            .await?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(
        sorted(&seen),
        vec![
            (
                "/\"fail_item\"/7".to_string(),
                Some("/\"fail_item\"".to_string())
            ),
            (
                "/\"items\"/\"k\"".to_string(),
                Some("/\"items\"".to_string())
            ),
        ]
    );
}

/// Mount keys are any `IntoStableKey`: an integer stores as `StableKey::Int`,
/// a UUID as `StableKey::Uuid`, bytes as `StableKey::Bytes`, a `StableKey`
/// as itself; a string still stores as `StableKey::Str`.
#[tokio::test]
async fn typed_mount_keys_store_as_typed_stable_keys() {
    let (app, _dir) = temp_app("typed_keys").await;
    let id = uuid::Uuid::from_u128(0x1234);
    app.update(move |ctx| async move {
        ctx.mount(7i64, |_child| async move { Ok(()) }).await?;
        ctx.mount(id, |_child| async move { Ok(()) }).await?;
        ctx.mount(&"name", |_child| async move { Ok(()) }).await?;
        ctx.with_subpath(StableKey::Symbol("sym".into()))
            .mount(b"xy".as_slice(), |_child| async move { Ok(()) })
            .await?;
        ctx.scope(3u32, |_child| async move { Ok(1u8) }).await?;
        Ok(())
    })
    .await
    .unwrap();

    let paths = inspect::list_stable_paths(&app).await.unwrap();
    let parts: Vec<Vec<StableKey>> = paths.iter().map(|p| p.to_vec()).collect();
    for expected in [
        vec![StableKey::Int(7)],
        vec![StableKey::Uuid(id)],
        vec![StableKey::Str("name".into())],
        vec![
            StableKey::Symbol("sym".into()),
            StableKey::Bytes(Arc::from(&b"xy"[..])),
        ],
        vec![StableKey::Int(3)],
    ] {
        assert!(
            parts.contains(&expected),
            "missing {expected:?} in {parts:?}"
        );
    }
}

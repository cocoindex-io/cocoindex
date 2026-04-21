use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use cocoindex::App;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Focused regression tests for `memo::batch` edge cases.
fn temp_app(name: &str) -> (App, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let app = App::builder(name)
        .db_path(dir.path().join("lmdb"))
        .build()
        .unwrap();
    (app, dir)
}

#[tokio::test]
async fn batch_serialization_error_releases_previous_pending_entries() {
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
async fn batch_fingerprint_error_releases_previous_pending_entries() {
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

#[tokio::test]
async fn batch_returns_error_when_mismatch_count() {
    let (app, _dir) = temp_app("batch_result_mismatch");
    let result = app
        .update(|ctx| async move {
            let items = vec![1, 2, 3];
            let _values: Vec<i32> = ctx
                .batch(
                    items,
                    |x| *x,
                    |misses| {
                        let out = misses.into_iter().take(2).map(|v| v * 2).collect();
                        async move { Ok(out) }
                    },
                )
                .await?;
            Ok(())
        })
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("batch function returned"));
}

//! Contract tests for the byte-oriented internal state backend seam.
//!
//! LMDB always runs. Postgres runs when `COCOINDEX_TEST_POSTGRES_URL` points
//! at an isolated test database; without it the test reports the skip and
//! exits successfully so default CI does not require a server.

use cocoindex_core::state::stable_path::{StableKey, StablePath};
use cocoindex_core::state_store::{Storage, StorageSettings};
use cocoindex_utils::fingerprint::Fingerprint;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;

fn test_path(name: &str) -> StablePath {
    StablePath(Arc::from(vec![StableKey::Symbol(Arc::from(name))]))
}

fn settings(db_path: PathBuf) -> StorageSettings {
    StorageSettings {
        db_path,
        lmdb_max_dbs: 64,
        lmdb_map_size: 1 << 24,
    }
}

async fn run_contract(storage: &Storage, app_name: &str) {
    let app = storage
        .create_app_store(app_name)
        .await
        .expect("create app store");
    let tracking_path = test_path("tracking");
    assert!(
        app.read_tracking_info(&tracking_path)
            .await
            .unwrap()
            .is_none()
    );

    let app_for_write = app.clone();
    let path_for_write = tracking_path.clone();
    storage
        .run_txn(move |wtxn| {
            let app = app_for_write.clone();
            let path = path_for_write.clone();
            Box::pin(async move {
                app.write_tracking_info_raw(wtxn, &path, b"version-one")
                    .await
            })
        })
        .await
        .expect("commit tracking info");
    assert_eq!(
        app.read_tracking_info(&tracking_path).await.unwrap(),
        Some(b"version-one".to_vec())
    );

    // A body failure must roll back every write in the batch.
    let app_for_rollback = app.clone();
    let path_for_rollback = tracking_path.clone();
    let rollback = storage
        .run_txn(move |wtxn| {
            let app = app_for_rollback.clone();
            let path = path_for_rollback.clone();
            Box::pin(async move {
                app.write_tracking_info_raw(wtxn, &path, b"must-not-commit")
                    .await?;
                Err::<(), _>(std::io::Error::other("intentional contract-test rollback").into())
            })
        })
        .await;
    assert!(rollback.is_err());
    assert_eq!(
        app.read_tracking_info(&tracking_path).await.unwrap(),
        Some(b"version-one".to_vec())
    );

    // Memoization state uses the same byte-preserving seam.
    let memo_path = test_path("memo");
    let app_for_memo = app.clone();
    let memo_path_for_write = memo_path.clone();
    storage
        .run_txn(move |wtxn| {
            let app = app_for_memo.clone();
            let path = memo_path_for_write.clone();
            Box::pin(async move {
                app.write_component_memo_raw(wtxn, &path, b"memo-bytes")
                    .await
            })
        })
        .await
        .expect("commit memo");
    assert_eq!(
        app.read_component_memo(&memo_path).await.unwrap(),
        Some(b"memo-bytes".to_vec())
    );

    // Prefix scans and prefix deletes are part of the same backend seam.
    let fp_a = Fingerprint::from(&"contract-fn-a").unwrap();
    let fp_b = Fingerprint::from(&"contract-fn-b").unwrap();
    let app_for_fn_memos = app.clone();
    let memo_scan_path = memo_path.clone();
    storage
        .run_txn(move |wtxn| {
            let app = app_for_fn_memos.clone();
            let path = memo_scan_path.clone();
            Box::pin(async move {
                app.write_fn_memo_raw(wtxn, &path, fp_a, b"a").await?;
                app.write_fn_memo_raw(wtxn, &path, fp_b, b"b").await
            })
        })
        .await
        .expect("commit fn memos");
    let (memos, _) = app.prefetch_fn_processing_states(&memo_path).await.unwrap();
    assert_eq!(memos.len(), 2);

    let app_for_delete = app.clone();
    let memo_delete_path = memo_path.clone();
    storage
        .run_txn(move |wtxn| {
            let app = app_for_delete.clone();
            let path = memo_delete_path.clone();
            Box::pin(async move { app.delete_all_fn_memos(wtxn, &path).await })
        })
        .await
        .expect("delete fn memos");
    let (memos, _) = app.prefetch_fn_processing_states(&memo_path).await.unwrap();
    assert!(memos.is_empty());

    // Concurrent read-modify-write must serialize without lost updates.
    let counter_path = test_path("counter");
    let app_for_seed = app.clone();
    let counter_for_seed = counter_path.clone();
    storage
        .run_txn(move |wtxn| {
            let app = app_for_seed.clone();
            let path = counter_for_seed.clone();
            Box::pin(async move {
                app.write_tracking_info_raw(wtxn, &path, &0_u64.to_be_bytes())
                    .await
            })
        })
        .await
        .expect("seed counter");

    let writers = 24_u64;
    let mut handles = Vec::new();
    for _ in 0..writers {
        let storage = storage.clone();
        let app = app.clone();
        let path = counter_path.clone();
        handles.push(tokio::spawn(async move {
            storage
                .run_txn(move |wtxn| {
                    let app = app.clone();
                    let path = path.clone();
                    Box::pin(async move {
                        let current = app
                            .read_tracking_info_in_txn(wtxn, &path)
                            .await?
                            .ok_or_else(|| std::io::Error::other("counter missing"))?;
                        let current = u64::from_be_bytes(
                            current
                                .as_slice()
                                .try_into()
                                .map_err(|_| std::io::Error::other("counter width changed"))?,
                        );
                        app.write_tracking_info_raw(wtxn, &path, &(current + 1).to_be_bytes())
                            .await
                    })
                })
                .await
        }));
    }
    for handle in handles {
        handle.await.unwrap().expect("concurrent writer");
    }
    let final_counter = app
        .read_tracking_info(&counter_path)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        u64::from_be_bytes(final_counter.as_slice().try_into().unwrap()),
        writers
    );

    // Standalone reads must see everything committed above from a brand-new
    // transaction. This is the read side used by memo/incremental checks.
    assert_eq!(
        app.read_tracking_info(&tracking_path).await.unwrap(),
        Some(b"version-one".to_vec())
    );
}

#[cfg(feature = "postgres")]
async fn run_cross_instance_counter(storage_a: &Storage, storage_b: &Storage, app_name: &str) {
    let app_a = storage_a
        .open_app_store_by_name(app_name)
        .await
        .unwrap()
        .unwrap();
    let app_b = storage_b
        .open_app_store_by_name(app_name)
        .await
        .unwrap()
        .unwrap();
    let counter_path = test_path("cross_instance_counter");
    let app_seed = app_a.clone();
    let path_seed = counter_path.clone();
    storage_a
        .run_txn(move |wtxn| {
            let app = app_seed.clone();
            let path = path_seed.clone();
            Box::pin(async move {
                app.write_tracking_info_raw(wtxn, &path, &0_u64.to_be_bytes())
                    .await
            })
        })
        .await
        .unwrap();

    let writers = 24_u64;
    let mut handles = Vec::new();
    for i in 0..writers {
        let storage = if i % 2 == 0 {
            storage_a.clone()
        } else {
            storage_b.clone()
        };
        let app = if i % 2 == 0 {
            app_a.clone()
        } else {
            app_b.clone()
        };
        let path = counter_path.clone();
        handles.push(tokio::spawn(async move {
            storage
                .run_txn(move |wtxn| {
                    let app = app.clone();
                    let path = path.clone();
                    Box::pin(async move {
                        let current = app
                            .read_tracking_info_in_txn(wtxn, &path)
                            .await?
                            .ok_or_else(|| std::io::Error::other("counter missing"))?;
                        let current = u64::from_be_bytes(
                            current
                                .as_slice()
                                .try_into()
                                .map_err(|_| std::io::Error::other("counter width changed"))?,
                        );
                        app.write_tracking_info_raw(wtxn, &path, &(current + 1).to_be_bytes())
                            .await
                    })
                })
                .await
        }));
    }
    for handle in handles {
        handle.await.unwrap().expect("cross-instance writer");
    }
    let final_counter = app_a
        .read_tracking_info(&counter_path)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        u64::from_be_bytes(final_counter.as_slice().try_into().unwrap()),
        writers
    );
}

async fn assert_persisted(storage: &Storage, app_name: &str) {
    let app = storage
        .open_app_store_by_name(app_name)
        .await
        .expect("open app store")
        .expect("app store exists");
    assert_eq!(
        app.read_tracking_info(&test_path("tracking"))
            .await
            .unwrap(),
        Some(b"version-one".to_vec())
    );
    assert_eq!(
        app.read_component_memo(&test_path("memo")).await.unwrap(),
        Some(b"memo-bytes".to_vec())
    );
}

#[tokio::test]
async fn lmdb_backend_contract() {
    let dir = TempDir::new().unwrap();
    let app_name = "contract_lmdb";
    let storage = Storage::new(&settings(dir.path().to_path_buf()))
        .await
        .unwrap();
    run_contract(&storage, app_name).await;
    drop(storage);

    let reopened = Storage::new(&settings(dir.path().to_path_buf()))
        .await
        .unwrap();
    assert_persisted(&reopened, app_name).await;
    reopened.drop_app(app_name).await.unwrap();
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_backend_contract() {
    let Ok(url) = std::env::var("COCOINDEX_TEST_POSTGRES_URL") else {
        eprintln!("skipping Postgres backend contract: COCOINDEX_TEST_POSTGRES_URL is not set");
        return;
    };
    let app_name = format!("contract_pg_{}", uuid::Uuid::new_v4().simple());
    let storage = Storage::new(&settings(PathBuf::from(&url)))
        .await
        .expect("open Postgres state backend");
    run_contract(&storage, &app_name).await;
    let storage_b = Storage::new(&settings(PathBuf::from(&url)))
        .await
        .expect("open second Postgres state backend");
    run_cross_instance_counter(&storage, &storage_b, &app_name).await;

    let reopened = Storage::new(&settings(PathBuf::from(&url)))
        .await
        .expect("reopen Postgres state backend");
    assert_persisted(&reopened, &app_name).await;
    reopened.drop_app(&app_name).await.unwrap();

    // Schema version is explicit and fail-closed.
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    sqlx::query("UPDATE cocoindex_schema_meta SET value = '999' WHERE key = 'schema_version'")
        .execute(&pool)
        .await
        .unwrap();
    let err = match Storage::new(&settings(PathBuf::from(&url))).await {
        Ok(_) => panic!("unsupported schema version unexpectedly opened"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("unsupported Postgres state schema version"),
        "unexpected error: {err}"
    );
    sqlx::query("UPDATE cocoindex_schema_meta SET value = '1' WHERE key = 'schema_version'")
        .execute(&pool)
        .await
        .unwrap();
}

//! Integration tests for staged target ownership preemption and recovery.

use std::sync::Arc;

use cocoindex_core::state::stable_path::{StableKey, StablePath};
use cocoindex_core::state::target_state_path::TargetStatePath;
use cocoindex_core::state_store::{
    AppStore, CommitPlan, ExistenceReconciler, Storage, StorageSettings,
};
use cocoindex_utils::fingerprint::Fingerprint;
use tempfile::TempDir;

async fn make_test_store() -> (Storage, AppStore, TempDir) {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("mdb");
    std::fs::create_dir_all(&db_path).unwrap();
    let settings = StorageSettings {
        db_path,
        lmdb_max_dbs: 4,
        lmdb_map_size: 1 << 24, // 16 MiB
    };
    let storage = Storage::new(&settings).await.unwrap();
    let store = storage.create_app_store("test_app").await.unwrap();
    (storage, store, dir)
}

fn comp_path(name: &str) -> StablePath {
    StablePath(Arc::from(vec![StableKey::Str(Arc::from(name))]))
}

fn target_path(name: &str) -> TargetStatePath {
    TargetStatePath::new(Fingerprint::from(name).unwrap(), None)
}

async fn read_owner(
    storage: &Storage,
    store: &AppStore,
    path: &TargetStatePath,
) -> Option<StablePath> {
    let store = store.clone();
    let path = path.clone();
    storage
        .run_txn(move |wtxn| {
            let store = store.clone();
            let path = path.clone();
            Box::pin(async move {
                Ok(store
                    .read_target_state_owner_in_txn(wtxn, &path)
                    .await?
                    .map(|info| info.component_path))
            })
        })
        .await
        .unwrap()
}

async fn commit_owner_upserts(
    store: &AppStore,
    component_path: &StablePath,
    upserts: Vec<(TargetStatePath, StablePath)>,
) {
    let plan = CommitPlan {
        new_tracking_info: None,
        target_owners_to_upsert: upserts,
        target_owners_to_delete: Vec::new(),
        fn_memo_clear_all_first: false,
        fn_memo_writes: Vec::new(),
        fn_memo_deletes: Vec::new(),
        user_state_clear_all_first: false,
        user_state_writes: Vec::new(),
        user_state_deletes: Vec::new(),
        user_state_clear_live: false,
        child_path_set: None,
    };
    let reconciler: ExistenceReconciler = Box::new(|_wtxn| Box::pin(async { Ok(()) }));
    store
        .commit(component_path, plan, reconciler)
        .await
        .unwrap();
}

#[tokio::test]
async fn sink_failure_preserves_previous_owner_and_prevents_uncommitted_target_claim() {
    let (storage, store, _dir) = make_test_store().await;
    let owner_a = comp_path("CompA");
    let owner_b = comp_path("CompB");
    let tsp = target_path("table/row1");

    // Phase 1: CompA commits ownership of tsp
    commit_owner_upserts(&store, &owner_a, vec![(tsp.clone(), owner_a.clone())]).await;
    assert_eq!(
        read_owner(&storage, &store, &tsp).await,
        Some(owner_a.clone())
    );

    // Phase 2: CompB precommits (simulated sink failure before commit)
    // clear_stage_marker is called on failure
    store.clear_stage_marker(&owner_b, 100).await.unwrap();

    // Assert __target STILL points to CompA
    assert_eq!(read_owner(&storage, &store, &tsp).await, Some(owner_a));
}

#[tokio::test]
async fn successful_commit_transfers_target_ownership() {
    let (storage, store, _dir) = make_test_store().await;
    let owner_a = comp_path("CompA");
    let owner_b = comp_path("CompB");
    let tsp = target_path("table/row1");

    // CompA commits initial ownership
    commit_owner_upserts(&store, &owner_a, vec![(tsp.clone(), owner_a.clone())]).await;
    assert_eq!(read_owner(&storage, &store, &tsp).await, Some(owner_a));

    // CompB precommits -> sink.apply succeeds -> CompB commits
    commit_owner_upserts(&store, &owner_b, vec![(tsp.clone(), owner_b.clone())]).await;

    // Assert __target is now transferred to CompB
    assert_eq!(read_owner(&storage, &store, &tsp).await, Some(owner_b));
}

#[tokio::test]
async fn retry_after_sink_failure_recovers_and_transfers_ownership() {
    let (storage, store, _dir) = make_test_store().await;
    let owner_a = comp_path("CompA");
    let owner_b = comp_path("CompB");
    let tsp = target_path("table/row1");

    // CompA initial ownership
    commit_owner_upserts(&store, &owner_a, vec![(tsp.clone(), owner_a.clone())]).await;

    // Attempt 1 for CompB fails during sink apply -> stage marker cleared
    store.clear_stage_marker(&owner_b, 101).await.unwrap();
    assert_eq!(
        read_owner(&storage, &store, &tsp).await,
        Some(owner_a.clone())
    );

    // Attempt 2 for CompB succeeds -> commit transfers ownership
    commit_owner_upserts(&store, &owner_b, vec![(tsp.clone(), owner_b.clone())]).await;
    assert_eq!(read_owner(&storage, &store, &tsp).await, Some(owner_b));
}

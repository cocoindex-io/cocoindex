//! Integration test for the inverted `__target` owner-index guard: a
//! component's delete must not drop an owner row another component has
//! since claimed. Uses only the public `cocoindex_core` API so the test
//! lives outside production sources.

use std::sync::Arc;

use cocoindex_core::state::stable_path::{StableKey, StablePath};
use cocoindex_core::state::target_state_path::TargetStatePath;
use cocoindex_core::state_store::{AppStore, CommitPlan, Storage, StorageSettings};
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

/// `/"process"/<segment>`: a component path whose last segment is not a `Str`.
fn typed_comp_path(segment: StableKey) -> StablePath {
    StablePath(Arc::from(vec![
        StableKey::Str(Arc::from("process")),
        segment,
    ]))
}

fn target_path(name: &str) -> TargetStatePath {
    TargetStatePath::new(Fingerprint::from(name).unwrap(), None)
}

async fn upsert_owner(
    storage: &Storage,
    store: &AppStore,
    path: &TargetStatePath,
    owner: &StablePath,
) {
    let store = store.clone();
    let path = path.clone();
    let owner = owner.clone();
    storage
        .run_txn(move |wtxn| {
            let store = store.clone();
            let path = path.clone();
            let owner = owner.clone();
            Box::pin(async move { store.upsert_target_state_owner(wtxn, &path, &owner).await })
        })
        .await
        .unwrap();
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

async fn commit_owner_deletes(
    store: &AppStore,
    component_path: &StablePath,
    deletes: Vec<TargetStatePath>,
) {
    let plan = CommitPlan {
        new_tracking_info: None,
        target_owners_to_upsert: Vec::new(),
        target_owners_to_delete: deletes,
        fn_memo_clear_all_first: false,
        fn_memo_writes: Vec::new(),
        fn_memo_deletes: Vec::new(),
        user_state_clear_all_first: false,
        user_state_writes: Vec::new(),
        user_state_deletes: Vec::new(),
        user_state_clear_live: false,
    };
    store.commit(component_path, plan, None).await.unwrap();
}

#[tokio::test]
async fn delete_preserves_owner_row_preempted_by_other_component() {
    let (storage, store, _dir) = make_test_store().await;
    let old_owner = comp_path("comp");
    let new_owner = comp_path("comp_new");
    let tsp = target_path("mystore/D1");

    // A different component has since claimed the path.
    upsert_owner(&storage, &store, &tsp, &new_owner).await;

    // The old owner's delete must not orphan the new owner.
    commit_owner_deletes(&store, &old_owner, vec![tsp.clone()]).await;

    assert_eq!(read_owner(&storage, &store, &tsp).await, Some(new_owner));
}

/// A component whose path contains a tuple segment must be able to delete its
/// own owner row. Before the msgpack codec fix the stored path decoded as
/// bytes, the equality guard failed, and the row leaked.
#[tokio::test]
async fn delete_removes_own_owner_row_with_non_str_segments() {
    let (storage, store, _dir) = make_test_store().await;
    for (label, owner) in [
        (
            "tuple",
            typed_comp_path(StableKey::Array(Arc::from(vec![
                StableKey::Int(1),
                StableKey::Int(2),
            ]))),
        ),
        (
            "utf8 bytes",
            typed_comp_path(StableKey::Bytes(Arc::from(&b"abc"[..]))),
        ),
        (
            "empty tuple",
            typed_comp_path(StableKey::Array(Arc::from(vec![]))),
        ),
    ] {
        let tsp = target_path(label);
        upsert_owner(&storage, &store, &tsp, &owner).await;
        assert_eq!(
            read_owner(&storage, &store, &tsp).await,
            Some(owner.clone()),
            "{label}: owner path must read back unchanged"
        );

        commit_owner_deletes(&store, &owner, vec![tsp.clone()]).await;
        assert_eq!(
            read_owner(&storage, &store, &tsp).await,
            None,
            "{label}: own owner row must be deleted"
        );
    }
}

/// Paths that differ only by segment *type* are different components, so a
/// delete issued from one must never drop the other's owner row. The msgpack
/// codec used to collapse array->bytes and bytes->string, making the
/// ownership guard treat these pairs as equal.
#[tokio::test]
async fn delete_preserves_owner_row_differing_only_by_segment_type() {
    let (storage, store, _dir) = make_test_store().await;
    let tuple = StableKey::Array(Arc::from(vec![StableKey::Int(1), StableKey::Int(2)]));
    let tuple_as_bytes = StableKey::Bytes(Arc::from(&[1u8, 2u8][..]));
    let text = StableKey::Str(Arc::from("abc"));
    let text_as_bytes = StableKey::Bytes(Arc::from(&b"abc"[..]));

    for (label, owner_segment, deleter_segment) in [
        ("tuple owner, bytes deleter", &tuple, &tuple_as_bytes),
        ("bytes owner, tuple deleter", &tuple_as_bytes, &tuple),
        ("str owner, bytes deleter", &text, &text_as_bytes),
        ("bytes owner, str deleter", &text_as_bytes, &text),
    ] {
        let owner = typed_comp_path(owner_segment.clone());
        let deleter = typed_comp_path(deleter_segment.clone());
        assert_ne!(owner, deleter);
        let tsp = target_path(label);

        upsert_owner(&storage, &store, &tsp, &owner).await;
        commit_owner_deletes(&store, &deleter, vec![tsp.clone()]).await;

        assert_eq!(
            read_owner(&storage, &store, &tsp).await,
            Some(owner),
            "{label}: another component's owner row must survive"
        );
    }
}

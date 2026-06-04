use std::io::Cursor;
use std::pin::Pin;

use crate::prelude::*;

use crate::engine::environment::Environment;
use crate::engine::{app::App, profile::EngineProfile};
use crate::state::db_schema::{self, ChildExistenceInfo, DbEntryKey, StablePathEntryKey};
use crate::state_store::AppStore;
use crate::state::stable_path::{StableKey, StablePath, StablePathPrefix, StablePathRef};
use cocoindex_utils::deser::from_msgpack_slice;
use futures::stream::{Stream, StreamExt};
use tokio_stream::wrappers::ReceiverStream;

pub async fn list_stable_paths<Prof: EngineProfile>(app: &App<Prof>) -> Result<Vec<StablePath>> {
    app.app_ctx().app_store().list_all_stable_paths().await
}

/// Represents a stable path with metadata (e.g. node type); more properties may be added.
#[derive(Clone, Debug)]
pub struct StablePathInfo {
    pub path: StablePath,
    pub node_type: db_schema::StablePathNodeType,
}

// Re-export StablePathNodeType for use in Python bindings
pub use db_schema::StablePathNodeType;

/// Returns a stream of stable paths with their metadata (e.g. node type).
/// Iteration runs on a dedicated thread (read txns/cursors are !Send); items are sent over a channel.
pub async fn iter_stable_paths<Prof: EngineProfile>(
    app: &App<Prof>,
) -> impl Stream<Item = Result<StablePathInfo>> + Send + 'static + use<Prof> {
    let app_store = app.app_ctx().app_store().clone();
    let rx = app_store.spawn_stable_path_iter().await;
    receiver_to_stable_path_info_stream(rx)
}

/// Like [`iter_stable_paths`], but takes an environment and an app name instead of a full `App`.
/// Opens the app's database read-only; returns an empty stream if the database doesn't exist.
pub async fn iter_stable_paths_by_name<Prof: EngineProfile>(
    env: &Environment<Prof>,
    app_name: &str,
) -> Result<Pin<Box<dyn Stream<Item = Result<StablePathInfo>> + Send + 'static>>> {
    let storage = env.storage();
    match storage.spawn_stable_path_iter_by_name(app_name).await? {
        Some(rx) => Ok(Box::pin(receiver_to_stable_path_info_stream(rx))),
        None => Ok(Box::pin(futures::stream::empty())),
    }
}

fn receiver_to_stable_path_info_stream(
    rx: tokio::sync::mpsc::Receiver<Result<(StablePath, StablePathNodeType)>>,
) -> impl Stream<Item = Result<StablePathInfo>> + Send + 'static {
    ReceiverStream::new(rx)
        .map(|item| item.map(|(path, node_type)| StablePathInfo { path, node_type }))
}

pub async fn list_app_names<Prof: EngineProfile>(env: &Environment<Prof>) -> Result<Vec<String>> {
    env.storage().list_app_names().await
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct TargetStateInfoItemSummary {
    pub target_state_path: String,
    pub key: String,
    pub states: Vec<(u64, String)>,
    pub provider_schema_version: u64,
    pub provider_generation: Option<(u64, u64)>,
}

fn decode_target_state_key(key_bytes: &[u8]) -> String {
    match storekey::decode::<Cursor<&[u8]>, StableKey>(Cursor::new(key_bytes)) {
        Ok(key) => key.to_string(),
        Err(_) => {
            let mut hex_string = String::from("0x");
            for byte in key_bytes {
                hex_string.push_str(&format!("{:02x}", byte));
            }
            hex_string
        }
    }
}

fn summarize_target_state_items(
    target_state_items: &std::collections::BTreeMap<
        crate::state::target_state_path::TargetStatePathWithProviderId,
        db_schema::TargetStateInfoItem,
    >,
) -> Vec<TargetStateInfoItemSummary> {
    target_state_items
        .iter()
        .map(|(path_with_pid, item)| {
            let key = decode_target_state_key(item.key.as_ref());
            let states = item
                .states
                .iter()
                .map(|(version, state)| {
                    let state_name = match state {
                        db_schema::TargetStateInfoItemState::Deleted => "Deleted".to_string(),
                        db_schema::TargetStateInfoItemState::Existing(_) => "Existing".to_string(),
                    };
                    (*version, state_name)
                })
                .collect();
            let provider_generation = item
                .provider_generation
                .as_ref()
                .map(|generation| (generation.provider_id, generation.provider_schema_version));

            TargetStateInfoItemSummary {
                target_state_path: path_with_pid.to_string(),
                key,
                states,
                provider_schema_version: item.provider_schema_version,
                provider_generation,
            }
        })
        .collect()
}

/// Detailed information about a single stable path stored in LMDB
#[derive(Clone, Debug, serde::Serialize)]
pub struct StablePathDetail {
    pub path: StablePath,
    pub node_type: db_schema::StablePathNodeType,
    pub version: u64,
    pub processor_name: String,
    pub target_state_count: usize,
    pub has_memoization: bool,
    pub target_state_items: Vec<TargetStateInfoItemSummary>,
}

type Database = heed::Database<heed::types::Bytes, heed::types::Bytes>;

async fn get_stable_path_detail_from_store(
    store: &AppStore,
    path: &StablePath,
) -> Result<Option<StablePathDetail>> {
    let db = store.db();
    let txn = store.read_txn().await?;

    // Get TrackingInfo (version, processor_name, target_state_items)
    let tracking_key = db_schema::DbEntryKey::StablePath(
        path.clone(),
        db_schema::StablePathEntryKey::TrackingInfo,
    )
    .encode()?;

    let (version, processor_name, target_state_count, target_state_items) =
        if let Some(value) = db.get(&txn, tracking_key.as_slice())? {
            let info: db_schema::StablePathEntryTrackingInfo = from_msgpack_slice(value)?;
            (
                info.version,
                info.processor_name.to_string(),
                info.target_state_items.len(),
                summarize_target_state_items(&info.target_state_items),
            )
        } else {
            (0, String::new(), 0, Vec::new())
        };

    // Check for ComponentMemoization (has memoization)
    let mem_key = db_schema::DbEntryKey::StablePath(
        path.clone(),
        db_schema::StablePathEntryKey::ComponentMemoization,
    )
    .encode()?;

    let has_memoization = db.get(&txn, mem_key.as_slice())?.is_some();

    // Get node_type from ChildExistence
    let node_type = if path.as_ref().is_empty() {
        db_schema::StablePathNodeType::Component
    } else {
        let path_ref: StablePathRef<'_> = path.as_ref();
        if let Some((parent_ref, key)) = path_ref.split_parent() {
            let parent_owned: StablePath = parent_ref.into();
            let cex_key = DbEntryKey::StablePath(
                parent_owned,
                StablePathEntryKey::ChildExistence(key.clone()),
            )
            .encode()?;
            if let Some(bytes) = db.get(&txn, cex_key.as_slice())? {
                let info: ChildExistenceInfo = from_msgpack_slice(bytes)?;
                info.node_type
            } else {
                db_schema::StablePathNodeType::Directory
            }
        } else {
            db_schema::StablePathNodeType::Component
        }
    };

    Ok(Some(StablePathDetail {
        path: path.clone(),
        node_type,
        version,
        processor_name,
        target_state_count,
        has_memoization,
        target_state_items,
    }))
}

/// Get detailed information about a single stable path from LMDB
pub async fn get_stable_path_detail<Prof: EngineProfile>(
    app: &App<Prof>,
    path: &StablePath,
) -> Result<Option<StablePathDetail>> {
    get_stable_path_detail_from_store(app.app_ctx().app_store(), path).await
}

pub async fn get_stable_path_detail_by_name<Prof: EngineProfile>(
    env: &Environment<Prof>,
    app_name: &str,
    path: &StablePath,
) -> Result<Option<StablePathDetail>> {
    let store = match env.storage().open_app_store_by_name(app_name).await? {
        Some(store) => store,
        None => return Ok(None),
    };
    get_stable_path_detail_from_store(&store, path).await
}

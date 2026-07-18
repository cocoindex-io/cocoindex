use std::io::Cursor;
use std::pin::Pin;

use crate::prelude::*;

use crate::engine::environment::Environment;
use crate::engine::{app::App, profile::EngineProfile};
use crate::state::db_schema::{self, ChildExistenceInfo, DbEntryKey, StablePathEntryKey};
use crate::state::stable_path::{StableKey, StablePath, StablePathRef};
use crate::state::target_state_path::{TargetStatePath, TargetStatePathWithProviderId};
use crate::state_store::AppStore;
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

/// Version and state label for a single target state entry.
#[derive(Clone, Debug, serde::Serialize)]
pub struct TargetStateVersion {
    pub version: u64,
    pub state: String,
}

/// Provider generation info for a target state item.
#[derive(Clone, Debug, serde::Serialize)]
pub struct ProviderGeneration {
    pub provider_id: u64,
    pub provider_schema_version: u64,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct TargetStateInfoItemSummary {
    pub target_state_path: String,
    pub key: StableKey,
    pub states: Vec<TargetStateVersion>,
    pub provider_schema_version: u64,
    pub provider_generation: Option<ProviderGeneration>,
}

fn decode_target_state_key(key_bytes: &[u8]) -> StableKey {
    match storekey::decode::<Cursor<&[u8]>, StableKey>(Cursor::new(key_bytes)) {
        Ok(key) => key,
        Err(_) => {
            let mut hex_string = String::from("0x");
            for byte in key_bytes {
                hex_string.push_str(&format!("{:02x}", byte));
            }
            StableKey::Str(hex_string.into())
        }
    }
}

/// Resolves fingerprinted target state path segments back to their original
/// `StableKey`s via the inverted owner index + the owner's tracking info.
/// Caches per path prefix, including misses, so items sharing ancestors are
/// resolved once per read txn.
struct TargetKeyResolver {
    cache: std::collections::HashMap<TargetStatePath, Option<StableKey>>,
}

impl TargetKeyResolver {
    fn new() -> Self {
        Self {
            cache: std::collections::HashMap::new(),
        }
    }

    /// Resolve the original `StableKey` for the last segment of `prefix`.
    /// Returns `None` when the prefix has no owner entry or tracking item:
    /// provider-only path segments (root providers, provider attachments) are
    /// never declared as target states so they have neither, and a crashed or
    /// in-flight run can leave either record missing.
    fn resolve_segment(
        &mut self,
        db: &heed::Database<heed::types::Bytes, heed::types::Bytes>,
        txn: &heed::RoTxn<'_, heed::WithoutTls>,
        prefix: &TargetStatePath,
    ) -> Result<Option<StableKey>> {
        if let Some(cached) = self.cache.get(prefix) {
            return Ok(cached.clone());
        }
        let resolved = Self::resolve_segment_uncached(db, txn, prefix)?;
        self.cache.insert(prefix.clone(), resolved.clone());
        Ok(resolved)
    }

    fn resolve_segment_uncached(
        db: &heed::Database<heed::types::Bytes, heed::types::Bytes>,
        txn: &heed::RoTxn<'_, heed::WithoutTls>,
        prefix: &TargetStatePath,
    ) -> Result<Option<StableKey>> {
        let owner_key = DbEntryKey::TargetState(prefix.clone()).encode()?;
        let owner_bytes = match db.get(txn, owner_key.as_slice())? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };
        let owner: db_schema::TargetStateOwnerInfo = from_msgpack_slice(owner_bytes)?;
        let tracking_key =
            DbEntryKey::StablePath(owner.component_path, StablePathEntryKey::TrackingInfo)
                .encode()?;
        let tracking_bytes = match db.get(txn, tracking_key.as_slice())? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };
        let info: db_schema::StablePathEntryTrackingInfo = from_msgpack_slice(tracking_bytes)?;
        // Items are ordered by (target_state_path, provider_id) with `None < Some`,
        // so the first item at or after (prefix, None) has the matching path, if any.
        let start = TargetStatePathWithProviderId {
            target_state_path: prefix.clone(),
            provider_id: None,
        };
        Ok(info
            .target_state_items
            .range(start..)
            .next()
            .filter(|(path_with_pid, _)| path_with_pid.target_state_path == *prefix)
            .map(|(_, item)| decode_target_state_key(item.key.as_ref())))
    }

    /// Render a readable path like `/@target/"file.md"/[13]`, falling back to
    /// the `#hex` fingerprint for any segment that cannot be resolved.
    /// `leaf_key` supplies the already-decoded key for the last segment.
    fn render_path(
        &mut self,
        db: &heed::Database<heed::types::Bytes, heed::types::Bytes>,
        txn: &heed::RoTxn<'_, heed::WithoutTls>,
        path: &TargetStatePath,
        leaf_key: Option<&StableKey>,
    ) -> Result<String> {
        let num_segments = path.as_slice().len();
        let mut rendered = String::new();
        for i in 0..num_segments {
            let key = if i + 1 == num_segments {
                match leaf_key {
                    Some(key) => {
                        // Seed the cache: a provider item's full path is an
                        // ancestor prefix of its child items' paths.
                        self.cache.insert(path.clone(), Some(key.clone()));
                        Some(key.clone())
                    }
                    None => self.resolve_segment(db, txn, path)?,
                }
            } else {
                self.resolve_segment(db, txn, &path.prefix(i + 1))?
            };
            match key {
                Some(key) => rendered.push_str(&format!("/{key}")),
                None => rendered.push_str(&format!("/{}", path.as_slice()[i])),
            }
        }
        Ok(rendered)
    }
}

fn summarize_target_state_items(
    db: &heed::Database<heed::types::Bytes, heed::types::Bytes>,
    txn: &heed::RoTxn<'_, heed::WithoutTls>,
    resolver: &mut TargetKeyResolver,
    target_state_items: &std::collections::BTreeMap<
        TargetStatePathWithProviderId,
        db_schema::TargetStateInfoItem,
    >,
) -> Result<Vec<TargetStateInfoItemSummary>> {
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
                    TargetStateVersion {
                        version: *version,
                        state: state_name,
                    }
                })
                .collect();
            let provider_generation =
                item.provider_generation
                    .as_ref()
                    .map(|generation| ProviderGeneration {
                        provider_id: generation.provider_id,
                        provider_schema_version: generation.provider_schema_version,
                    });

            let mut target_state_path =
                resolver.render_path(db, txn, &path_with_pid.target_state_path, Some(&key))?;
            if let Some(id) = path_with_pid.provider_id {
                target_state_path.push_str(&format!("[provider_id={id}]"));
            }

            Ok(TargetStateInfoItemSummary {
                target_state_path,
                key,
                states,
                provider_schema_version: item.provider_schema_version,
                provider_generation,
            })
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

/// Look up the node type for a path via its parent's ChildExistence entry.
fn lookup_node_type(
    db: &heed::Database<heed::types::Bytes, heed::types::Bytes>,
    txn: &heed::RoTxn<'_, heed::WithoutTls>,
    path: &StablePath,
) -> Result<db_schema::StablePathNodeType> {
    if path.as_ref().is_empty() {
        return Ok(db_schema::StablePathNodeType::Component);
    }
    let path_ref: StablePathRef<'_> = path.as_ref();
    if let Some((parent_ref, key)) = path_ref.split_parent() {
        let parent_owned: StablePath = parent_ref.into();
        let cex_key = DbEntryKey::StablePath(
            parent_owned,
            StablePathEntryKey::ChildExistence(key.clone()),
        )
        .encode()?;
        if let Some(bytes) = db.get(txn, cex_key.as_slice())? {
            let info: ChildExistenceInfo = from_msgpack_slice(bytes)?;
            Ok(info.node_type)
        } else {
            Ok(db_schema::StablePathNodeType::Directory)
        }
    } else {
        Ok(db_schema::StablePathNodeType::Component)
    }
}

/// Read the detail for a single path within an existing read transaction.
fn read_detail_in_txn(
    db: &heed::Database<heed::types::Bytes, heed::types::Bytes>,
    txn: &heed::RoTxn<'_, heed::WithoutTls>,
    resolver: &mut TargetKeyResolver,
    path: &StablePath,
) -> Result<StablePathDetail> {
    // Get TrackingInfo (version, processor_name, target_state_items)
    let tracking_key =
        DbEntryKey::StablePath(path.clone(), StablePathEntryKey::TrackingInfo).encode()?;

    let (version, processor_name, target_state_count, target_state_items) =
        if let Some(value) = db.get(txn, tracking_key.as_slice())? {
            let info: db_schema::StablePathEntryTrackingInfo = from_msgpack_slice(value)?;
            (
                info.version,
                info.processor_name.to_string(),
                info.target_state_items.len(),
                summarize_target_state_items(db, txn, resolver, &info.target_state_items)?,
            )
        } else {
            (0, String::new(), 0, Vec::new())
        };

    // Check for ComponentMemoization
    let mem_key =
        DbEntryKey::StablePath(path.clone(), StablePathEntryKey::ComponentMemoization).encode()?;
    let has_memoization = db.get(txn, mem_key.as_slice())?.is_some();

    let node_type = lookup_node_type(db, txn, path)?;

    Ok(StablePathDetail {
        path: path.clone(),
        node_type,
        version,
        processor_name,
        target_state_count,
        has_memoization,
        target_state_items,
    })
}

async fn get_stable_path_detail_from_store(
    store: &AppStore,
    path: &StablePath,
) -> Result<Option<StablePathDetail>> {
    let db = store.db();
    let txn = store.read_txn().await?;
    let mut resolver = TargetKeyResolver::new();
    Ok(Some(read_detail_in_txn(&db, &*txn, &mut resolver, path)?))
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

/// List direct children of a path. With `recursive=true`, walks the full subtree.
fn list_children_in_txn(
    db: &heed::Database<heed::types::Bytes, heed::types::Bytes>,
    txn: &heed::RoTxn<'_, heed::WithoutTls>,
    resolver: &mut TargetKeyResolver,
    path: &StablePath,
    recursive: bool,
) -> Result<Vec<StablePathDetail>> {
    let mut results = Vec::new();
    let mut queue = std::collections::VecDeque::new();
    queue.push_back(path.clone());

    while let Some(parent) = queue.pop_front() {
        let prefix =
            DbEntryKey::StablePath(parent.clone(), StablePathEntryKey::ChildExistencePrefix)
                .encode()?;
        for entry in db.prefix_iter(txn, &prefix)? {
            let (raw_key, raw_value) = entry?;
            let child_key: StableKey = storekey::decode(raw_key[prefix.len()..].as_ref())?;
            let child_path = parent.as_ref().concat_part(child_key);
            let info: ChildExistenceInfo = from_msgpack_slice(raw_value)?;

            // Only recurse into directories
            if recursive && info.node_type == db_schema::StablePathNodeType::Directory {
                queue.push_back(child_path.clone());
            }

            results.push(read_detail_in_txn(db, txn, resolver, &child_path)?);
        }
    }
    Ok(results)
}

/// Collect details for all ancestor paths.
fn list_parents_in_txn(
    db: &heed::Database<heed::types::Bytes, heed::types::Bytes>,
    txn: &heed::RoTxn<'_, heed::WithoutTls>,
    resolver: &mut TargetKeyResolver,
    path: &StablePath,
) -> Result<Vec<StablePathDetail>> {
    let mut results = Vec::new();
    let mut current: StablePathRef<'_> = path.as_ref();
    while let Some((parent_ref, _key)) = current.split_parent() {
        let parent_path: StablePath = parent_ref.into();
        results.push(read_detail_in_txn(db, txn, resolver, &parent_path)?);
        current = parent_ref;
    }
    // Reverse so parents appear root-first
    results.reverse();
    Ok(results)
}

/// Query details for a path with optional children/parents, all in a single read txn.
async fn query_details_from_store(
    store: &AppStore,
    path: &StablePath,
    include_children: bool,
    recursive: bool,
    include_parents: bool,
) -> Result<Vec<StablePathDetail>> {
    let db = store.db();
    let txn = store.read_txn().await?;
    let mut resolver = TargetKeyResolver::new();

    let mut results = Vec::new();

    if include_parents {
        results.extend(list_parents_in_txn(&db, &*txn, &mut resolver, path)?);
    }

    results.push(read_detail_in_txn(&db, &*txn, &mut resolver, path)?);

    if include_children {
        results.extend(list_children_in_txn(
            &db,
            &*txn,
            &mut resolver,
            path,
            recursive,
        )?);
    }

    Ok(results)
}

/// Query details for a path with optional children/parents from a live App.
pub async fn query_stable_path_details<Prof: EngineProfile>(
    app: &App<Prof>,
    path: &StablePath,
    include_children: bool,
    recursive: bool,
    include_parents: bool,
) -> Result<Vec<StablePathDetail>> {
    query_details_from_store(
        app.app_ctx().app_store(),
        path,
        include_children,
        recursive,
        include_parents,
    )
    .await
}

/// Query details for a path with optional children/parents from an Environment + app name.
pub async fn query_stable_path_details_by_name<Prof: EngineProfile>(
    env: &Environment<Prof>,
    app_name: &str,
    path: &StablePath,
    include_children: bool,
    recursive: bool,
    include_parents: bool,
) -> Result<Vec<StablePathDetail>> {
    let store = match env.storage().open_app_store_by_name(app_name).await? {
        Some(store) => store,
        None => return Ok(Vec::new()),
    };
    query_details_from_store(&store, path, include_children, recursive, include_parents).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_store::test_support::make_test_store;
    use cocoindex_utils::fingerprint::Fingerprint;
    use std::sync::Arc;

    #[tokio::test]
    async fn render_path_falls_back_to_fingerprints() {
        let (store, _dir) = make_test_store().await;
        let path = TargetStatePath::new(Fingerprint::from(&"root_target").unwrap(), None)
            .concat(&StableKey::Str(Arc::from("file.md")));

        let db = store.db();
        let txn = store.read_txn().await.unwrap();
        let mut resolver = TargetKeyResolver::new();
        let rendered = resolver.render_path(&db, &txn, &path, None).unwrap();

        // Nothing is resolvable in an empty store: both segments stay fingerprints.
        assert_eq!(rendered, path.to_string());
        assert_eq!(rendered.matches("/#").count(), 2);
    }
}

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
    /// Readable rendering, e.g. `/@target/"file.md"/[13][provider_id=0]`.
    pub target_state_path: String,
    /// Raw fingerprint rendering as stored, e.g. `/#96..../#48....[provider_id=0]`.
    pub fingerprint_path: String,
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
    /// Owner components whose tracking items have already been folded into
    /// `cache`, so each tracking blob is deserialized at most once per txn.
    loaded_owners: std::collections::HashSet<StablePath>,
}

impl TargetKeyResolver {
    /// `provider_keys` seeds the cache with the original keys of live
    /// registered providers (see [`provider_key_seed`]); root provider
    /// segments are resolvable only through this seed.
    fn new(provider_keys: std::collections::HashMap<TargetStatePath, StableKey>) -> Self {
        Self {
            cache: provider_keys
                .into_iter()
                .map(|(path, key)| (path, Some(key)))
                .collect(),
            loaded_owners: std::collections::HashSet::new(),
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
        self.load_owner_items(db, txn, prefix)?;
        let resolved = self.cache.get(prefix).cloned().flatten();
        // Also cache misses so unresolvable prefixes aren't retried per item.
        self.cache.insert(prefix.clone(), resolved.clone());
        Ok(resolved)
    }

    /// Look up `prefix`'s owner component and fold all of the owner's
    /// tracking items into the cache. Caching the whole blob at once keeps
    /// resolution linear when one component owns many target states.
    fn load_owner_items(
        &mut self,
        db: &heed::Database<heed::types::Bytes, heed::types::Bytes>,
        txn: &heed::RoTxn<'_, heed::WithoutTls>,
        prefix: &TargetStatePath,
    ) -> Result<()> {
        let owner_key = DbEntryKey::TargetState(prefix.clone()).encode()?;
        let owner_bytes = match db.get(txn, owner_key.as_slice())? {
            Some(bytes) => bytes,
            None => return Ok(()),
        };
        let owner: db_schema::TargetStateOwnerInfo = from_msgpack_slice(owner_bytes)?;
        if !self.loaded_owners.insert(owner.component_path.clone()) {
            return Ok(());
        }
        let tracking_key =
            DbEntryKey::StablePath(owner.component_path, StablePathEntryKey::TrackingInfo)
                .encode()?;
        let tracking_bytes = match db.get(txn, tracking_key.as_slice())? {
            Some(bytes) => bytes,
            None => return Ok(()),
        };
        let info: db_schema::StablePathEntryTrackingInfo = from_msgpack_slice(tracking_bytes)?;
        for (path_with_pid, item) in &info.target_state_items {
            self.cache
                .entry(path_with_pid.target_state_path.clone())
                .or_insert_with(|| Some(decode_target_state_key(item.key.as_ref())));
        }
        Ok(())
    }

    /// Render a readable path like `/@target/"file.md"/[13]`, falling back to
    /// the `#hex` fingerprint for any segment that cannot be resolved.
    /// `leaf_key` supplies the already-decoded key for the last segment.
    /// Render each segment of `path` (readable key, or `#hex` fallback),
    /// without leading slashes. Kept per-segment because readable keys may
    /// themselves contain `/` (e.g. list keys), so a joined string can't be
    /// split back reliably.
    fn render_segments(
        &mut self,
        db: &heed::Database<heed::types::Bytes, heed::types::Bytes>,
        txn: &heed::RoTxn<'_, heed::WithoutTls>,
        path: &TargetStatePath,
        leaf_key: Option<&StableKey>,
    ) -> Result<Vec<String>> {
        let num_segments = path.as_slice().len();
        let mut rendered = Vec::with_capacity(num_segments);
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
            rendered.push(match key {
                Some(key) => key.to_string(),
                None => path.as_slice()[i].to_string(),
            });
        }
        Ok(rendered)
    }

    fn render_path(
        &mut self,
        db: &heed::Database<heed::types::Bytes, heed::types::Bytes>,
        txn: &heed::RoTxn<'_, heed::WithoutTls>,
        path: &TargetStatePath,
        leaf_key: Option<&StableKey>,
    ) -> Result<String> {
        Ok(join_segments(
            &self.render_segments(db, txn, path, leaf_key)?,
        ))
    }
}

fn join_segments(segments: &[String]) -> String {
    segments.iter().map(|s| format!("/{s}")).collect()
}

/// Collect the original keys of all target-state providers registered in the
/// live environment: root providers and their attachments, registered by name
/// at module import time and never persisted, so this registry is the only
/// source that can render their path segments readably. Two classes of
/// provider-only segments remain unresolvable and keep the fingerprint
/// fallback: providers whose modules aren't imported in this process (e.g.
/// inspecting a database without loading the app), and attachments of nested
/// providers, which live in build-local registries that are discarded when
/// the run finishes.
fn provider_key_seed<Prof: EngineProfile>(
    env: &Environment<Prof>,
) -> std::collections::HashMap<TargetStatePath, StableKey> {
    let registry = env.target_states_providers().lock().unwrap();
    registry
        .providers
        .iter()
        .map(|(path, provider)| (path.clone(), provider.stable_key().clone()))
        .collect()
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
                fingerprint_path: path_with_pid.to_string(),
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

/// Store-scoped detail query: one fresh read txn + resolver per call —
/// the per-path shape `show -l` drives via the App/Environment wrappers
/// below. Public so benchmarks can measure it directly against a store.
pub async fn get_stable_path_detail_from_store(
    store: &AppStore,
    provider_keys: std::collections::HashMap<TargetStatePath, StableKey>,
    path: &StablePath,
) -> Result<Option<StablePathDetail>> {
    let db = store.db();
    let txn = store.read_txn().await?;
    let mut resolver = TargetKeyResolver::new(provider_keys);
    Ok(Some(read_detail_in_txn(&db, &*txn, &mut resolver, path)?))
}

/// Get detailed information about a single stable path from LMDB
pub async fn get_stable_path_detail<Prof: EngineProfile>(
    app: &App<Prof>,
    path: &StablePath,
) -> Result<Option<StablePathDetail>> {
    let seed = provider_key_seed(app.app_ctx().env());
    get_stable_path_detail_from_store(app.app_ctx().app_store(), seed, path).await
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
    get_stable_path_detail_from_store(&store, provider_key_seed(env), path).await
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
    provider_keys: std::collections::HashMap<TargetStatePath, StableKey>,
    path: &StablePath,
    include_children: bool,
    recursive: bool,
    include_parents: bool,
) -> Result<Vec<StablePathDetail>> {
    let db = store.db();
    let txn = store.read_txn().await?;
    let mut resolver = TargetKeyResolver::new(provider_keys);

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
        provider_key_seed(app.app_ctx().env()),
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
    query_details_from_store(
        &store,
        provider_key_seed(env),
        path,
        include_children,
        recursive,
        include_parents,
    )
    .await
}

/// A tracked target state entry from the inverted owner index.
#[derive(Clone, Debug, serde::Serialize)]
pub struct TargetStateEntry {
    /// Raw fingerprint rendering, e.g. `/#96330b5a.../#4866...`.
    pub fingerprint_path: String,
    /// Readable rendering, e.g. `/@doc_store/"file.md"/[13]`; falls back to
    /// `#hex` for segments that cannot be resolved.
    pub readable_path: String,
    /// Per-segment form of `readable_path` (no leading slashes). Readable
    /// keys may contain `/`, so consumers that need segments (e.g. tree
    /// rendering) must use this instead of splitting the joined string.
    pub readable_segments: Vec<String>,
    pub owner_component_path: StablePath,
    /// True when the owner index entry has no matching item in the owner
    /// component's tracking info — an inconsistency, e.g. left behind by a
    /// crashed run or an interrupted cleanup.
    pub dangling: bool,
}

/// Iterate the target-state keyspace in stored (fingerprint) order, calling
/// `emit` for each entry. `emit` returns `false` to stop early (e.g. when the
/// receiving end of a channel is gone). Stored order keeps children of the
/// same parent adjacent, but has no global human-meaningful ordering.
fn for_each_target_state_in_txn(
    db: &heed::Database<heed::types::Bytes, heed::types::Bytes>,
    txn: &heed::RoTxn<'_, heed::WithoutTls>,
    resolver: &mut TargetKeyResolver,
    mut emit: impl FnMut(TargetStateEntry) -> bool,
) -> Result<()> {
    let prefix = DbEntryKey::TargetStatePrefix.encode()?;
    for entry in db.prefix_iter(txn, &prefix)? {
        let (raw_key, raw_value) = entry?;
        let path = match DbEntryKey::decode(raw_key)? {
            DbEntryKey::TargetState(path) => path,
            key => {
                return Err(internal_error!(
                    "Unexpected key in target state keyspace: {key:?}"
                ));
            }
        };
        let owner: db_schema::TargetStateOwnerInfo = from_msgpack_slice(raw_value)?;
        // The leaf segment must be recorded in the owner's tracking info;
        // ancestors may legitimately be unresolvable (root providers).
        let dangling = resolver.resolve_segment(db, txn, &path)?.is_none();
        let readable_segments = resolver.render_segments(db, txn, &path, None)?;
        let entry = TargetStateEntry {
            fingerprint_path: path.to_string(),
            readable_path: join_segments(&readable_segments),
            readable_segments,
            owner_component_path: owner.component_path,
            dangling,
        };
        if !emit(entry) {
            break;
        }
    }
    Ok(())
}

/// Store-scoped target-state listing: one read txn + one shared resolver
/// for the whole iteration. Public so benchmarks can measure it directly
/// against a store (the App/Environment wrappers below add only the
/// provider-key seed).
pub async fn spawn_target_state_iter(
    store: AppStore,
    provider_keys: std::collections::HashMap<TargetStatePath, StableKey>,
) -> impl Stream<Item = Result<TargetStateEntry>> + Send + 'static {
    let storage = store.storage.clone();
    let rx = storage
        .spawn_read_txn_receiver(store, move |db, txn, tx| {
            let mut resolver = TargetKeyResolver::new(provider_keys);
            for_each_target_state_in_txn(db, txn, &mut resolver, |entry| {
                tx.blocking_send(Ok(entry)).is_ok()
            })
        })
        .await;
    ReceiverStream::new(rx)
}

/// Stream all tracked target states with their owner components from a live
/// App, in stored (fingerprint) order. Iteration runs on a dedicated thread
/// (read txns/cursors are !Send); items are sent over a channel.
pub async fn iter_target_states<Prof: EngineProfile>(
    app: &App<Prof>,
) -> impl Stream<Item = Result<TargetStateEntry>> + Send + 'static + use<Prof> {
    let seed = provider_key_seed(app.app_ctx().env());
    spawn_target_state_iter(app.app_ctx().app_store().clone(), seed).await
}

/// Like [`iter_target_states`], but takes an environment and an app name.
/// Returns an empty stream if the app's database doesn't exist.
pub async fn iter_target_states_by_name<Prof: EngineProfile>(
    env: &Environment<Prof>,
    app_name: &str,
) -> Result<Pin<Box<dyn Stream<Item = Result<TargetStateEntry>> + Send + 'static>>> {
    let store = match env.storage().open_app_store_by_name(app_name).await? {
        Some(store) => store,
        None => return Ok(Box::pin(futures::stream::empty())),
    };
    Ok(Box::pin(
        spawn_target_state_iter(store, provider_key_seed(env)).await,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_store::test_support::make_test_store;
    use cocoindex_utils::fingerprint::Fingerprint;
    use std::borrow::Cow;
    use std::sync::Arc;

    fn comp_path(name: &str) -> StablePath {
        StablePath(Arc::from(vec![StableKey::Str(Arc::from(name))]))
    }

    fn with_pid(path: &TargetStatePath, provider_id: Option<u64>) -> TargetStatePathWithProviderId {
        TargetStatePathWithProviderId {
            target_state_path: path.clone(),
            provider_id,
        }
    }

    fn tracking_bytes(items: Vec<(TargetStatePathWithProviderId, StableKey)>) -> Vec<u8> {
        let mut info = db_schema::StablePathEntryTrackingInfo::new(Cow::Borrowed("test"));
        for (path_with_pid, key) in items {
            info.target_state_items.insert(
                path_with_pid,
                db_schema::TargetStateInfoItem {
                    key: Cow::Owned(storekey::encode_vec(&key).unwrap()),
                    states: Vec::new(),
                    provider_schema_version: 0,
                    provider_generation: None,
                },
            );
        }
        rmp_serde::to_vec_named(&info).unwrap()
    }

    async fn commit_writes(
        store: &AppStore,
        tracking: Vec<(StablePath, Vec<u8>)>,
        owners: Vec<(TargetStatePath, StablePath)>,
    ) {
        let store2 = store.clone();
        store
            .storage
            .run_txn(move |wtxn| {
                let store = store2.clone();
                let tracking = tracking.clone();
                let owners = owners.clone();
                Box::pin(async move {
                    for (path, bytes) in &tracking {
                        store.write_tracking_info_raw(wtxn, path, bytes).await?;
                    }
                    for (ts_path, owner) in &owners {
                        store
                            .upsert_target_state_owner(wtxn, ts_path, owner)
                            .await?;
                    }
                    Ok(())
                })
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn lists_target_states_via_open_by_name() {
        use crate::state_store::{Storage, StorageSettings};
        let dir = tempfile::TempDir::new().unwrap();
        let settings = StorageSettings {
            db_path: dir.path().to_path_buf(),
            lmdb_max_dbs: 8,
            lmdb_map_size: 4 * 1024 * 1024,
        };
        let storage = Storage::new(&settings).await.unwrap();
        let store = storage.create_app_store("TestApp").await.unwrap();
        let root_path = TargetStatePath::new(Fingerprint::from(&"root").unwrap(), None);
        let key = StableKey::Str(Arc::from("x"));
        let ts_path = root_path.concat(&key);
        let comp = comp_path("comp");
        commit_writes(
            &store,
            vec![(
                comp.clone(),
                tracking_bytes(vec![(with_pid(&ts_path, None), key.clone())]),
            )],
            vec![(ts_path.clone(), comp.clone())],
        )
        .await;

        let store2 = storage
            .open_app_store_by_name("TestApp")
            .await
            .unwrap()
            .unwrap();
        let entries: Vec<_> = spawn_target_state_iter(store2, Default::default())
            .await
            .collect()
            .await;
        assert_eq!(entries.len(), 1);
        assert!(entries[0].is_ok());
    }

    fn collect_target_states(
        db: &heed::Database<heed::types::Bytes, heed::types::Bytes>,
        txn: &heed::RoTxn<'_, heed::WithoutTls>,
        resolver: &mut TargetKeyResolver,
    ) -> Vec<TargetStateEntry> {
        let mut entries = Vec::new();
        for_each_target_state_in_txn(db, txn, resolver, |e| {
            entries.push(e);
            true
        })
        .unwrap();
        entries
    }

    #[tokio::test]
    async fn render_path_falls_back_to_fingerprints() {
        let (store, _dir) = make_test_store().await;
        let path = TargetStatePath::new(Fingerprint::from(&"root_target").unwrap(), None)
            .concat(&StableKey::Str(Arc::from("file.md")));

        let db = store.db();
        let txn = store.read_txn().await.unwrap();
        let mut resolver = TargetKeyResolver::new(Default::default());
        let rendered = resolver.render_path(&db, &txn, &path, None).unwrap();

        // Nothing is resolvable in an empty store: both segments stay fingerprints.
        assert_eq!(rendered, path.to_string());
        assert_eq!(rendered.matches("/#").count(), 2);
    }

    #[tokio::test]
    async fn resolves_root_segment_from_provider_seed() {
        let (store, _dir) = make_test_store().await;
        let root_path = TargetStatePath::new(Fingerprint::from(&"my_root").unwrap(), None);
        let key = StableKey::Str(Arc::from("file.md"));
        let path = root_path.concat(&key);
        let comp = comp_path("comp");
        commit_writes(
            &store,
            vec![(
                comp.clone(),
                tracking_bytes(vec![(with_pid(&path, None), key.clone())]),
            )],
            vec![(path.clone(), comp)],
        )
        .await;

        let db = store.db();
        let txn = store.read_txn().await.unwrap();
        // Seed the root provider's key, as provider_key_seed does from the
        // live registry (root providers are never persisted).
        let seed = std::collections::HashMap::from([(
            root_path.clone(),
            StableKey::Symbol(Arc::from("my_root")),
        )]);
        let mut resolver = TargetKeyResolver::new(seed);
        let rendered = resolver.render_path(&db, &txn, &path, None).unwrap();
        assert_eq!(rendered, "/@my_root/\"file.md\"");
    }

    #[tokio::test]
    async fn flags_dangling_entries_without_tracking_items() {
        let (store, _dir) = make_test_store().await;
        let root_path = TargetStatePath::new(Fingerprint::from(&"root").unwrap(), None);
        let good_key = StableKey::Str(Arc::from("good"));
        let good_path = root_path.concat(&good_key);
        let dangling_path = root_path.concat(&StableKey::Str(Arc::from("gone")));
        let comp = comp_path("comp");
        // Owner entries for both paths, but tracking info only records "good".
        commit_writes(
            &store,
            vec![(
                comp.clone(),
                tracking_bytes(vec![(with_pid(&good_path, None), good_key.clone())]),
            )],
            vec![
                (good_path.clone(), comp.clone()),
                (dangling_path.clone(), comp.clone()),
            ],
        )
        .await;

        let db = store.db();
        let txn = store.read_txn().await.unwrap();
        let mut resolver = TargetKeyResolver::new(Default::default());
        let entries = collect_target_states(&db, &txn, &mut resolver);
        assert_eq!(entries.len(), 2);
        let good = entries
            .iter()
            .find(|e| e.fingerprint_path == good_path.to_string())
            .unwrap();
        assert!(!good.dangling);
        let dangling = entries
            .iter()
            .find(|e| e.fingerprint_path == dangling_path.to_string())
            .unwrap();
        assert!(dangling.dangling);
    }

    #[tokio::test]
    async fn resolves_readable_paths_and_lists_target_states() {
        let (store, _dir) = make_test_store().await;

        // comp_c declares a "table" target state under an (unresolvable) root
        // provider; the table creates a provider under which comp_d declares
        // a row keyed by the integer 13.
        let root_path = TargetStatePath::new(Fingerprint::from(&"root_target").unwrap(), None);
        let table_key = StableKey::Str(Arc::from("table"));
        let table_path = root_path.concat(&table_key);
        let row_key = StableKey::Int(13);
        let row_path = table_path.concat(&row_key);

        let comp_c = comp_path("comp_c");
        let comp_d = comp_path("comp_d");

        commit_writes(
            &store,
            vec![
                (
                    comp_c.clone(),
                    tracking_bytes(vec![(with_pid(&table_path, None), table_key.clone())]),
                ),
                (
                    comp_d.clone(),
                    tracking_bytes(vec![(with_pid(&row_path, Some(0)), row_key.clone())]),
                ),
            ],
            vec![
                (table_path.clone(), comp_c.clone()),
                (row_path.clone(), comp_d.clone()),
            ],
        )
        .await;

        let db = store.db();
        let txn = store.read_txn().await.unwrap();

        let root_seg = root_path.to_string();
        let mut resolver = TargetKeyResolver::new(Default::default());
        let rendered = resolver.render_path(&db, &txn, &row_path, None).unwrap();
        assert_eq!(rendered, format!("{root_seg}/\"table\"/13"));

        // Detail summary renders readable paths (with provider_id suffix).
        let mut resolver = TargetKeyResolver::new(Default::default());
        let detail = read_detail_in_txn(&db, &txn, &mut resolver, &comp_d).unwrap();
        assert_eq!(detail.target_state_items.len(), 1);
        assert_eq!(
            detail.target_state_items[0].target_state_path,
            format!("{root_seg}/\"table\"/13[provider_id=0]")
        );
        assert_eq!(
            detail.target_state_items[0].fingerprint_path,
            format!("{row_path}[provider_id=0]")
        );
        assert_eq!(detail.target_state_items[0].key, row_key);

        // Listing returns every owner-index entry with both path forms.
        let mut resolver = TargetKeyResolver::new(Default::default());
        let entries = collect_target_states(&db, &txn, &mut resolver);
        assert_eq!(entries.len(), 2);
        // Stored (bytewise) order places a parent before its children.
        assert_eq!(entries[0].fingerprint_path, table_path.to_string());
        let table_entry = entries
            .iter()
            .find(|e| e.fingerprint_path == table_path.to_string())
            .unwrap();
        assert_eq!(table_entry.readable_path, format!("{root_seg}/\"table\""));
        assert_eq!(table_entry.owner_component_path, comp_c);
        let row_entry = entries
            .iter()
            .find(|e| e.fingerprint_path == row_path.to_string())
            .unwrap();
        assert_eq!(row_entry.readable_path, format!("{root_seg}/\"table\"/13"));
        assert_eq!(
            row_entry.readable_segments,
            vec![
                root_seg.trim_start_matches('/').to_string(),
                "\"table\"".to_string(),
                "13".to_string(),
            ]
        );
        assert_eq!(row_entry.owner_component_path, comp_d);
    }
}

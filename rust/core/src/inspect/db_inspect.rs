use std::pin::Pin;

use crate::prelude::*;

use crate::engine::environment::Environment;
use crate::engine::{app::App, profile::EngineProfile};
use crate::state::db_schema::{self, DbEntryKey};
use crate::state::stable_path::{StablePath, StablePathPrefix, StablePathRef};
use cocoindex_utils::deser::from_msgpack_slice;
use futures::stream::Stream;
use heed::types::{DecodeIgnore, Str};
use tokio_stream::wrappers::ReceiverStream;

pub fn list_stable_paths<Prof: EngineProfile>(app: &App<Prof>) -> Result<Vec<StablePath>> {
    let encoded_key_prefix =
        DbEntryKey::StablePathPrefixPrefix(StablePathPrefix::default()).encode()?;
    let db = app.app_ctx().db();
    let txn = app.app_ctx().env().db_env().read_txn()?;

    let mut result = Vec::new();
    let mut last_prefix: Option<Vec<u8>> = None;
    for entry in db.prefix_iter(&txn, encoded_key_prefix.as_ref())? {
        let (raw_key, _) = entry?;
        if let Some(last_prefix) = &last_prefix
            && raw_key.starts_with(last_prefix)
        {
            continue;
        }
        let key: DbEntryKey = DbEntryKey::decode(raw_key)?;
        let DbEntryKey::StablePath(path, _) = key else {
            internal_bail!("Expected StablePath, got {key:?}");
        };
        last_prefix = Some(DbEntryKey::StablePathPrefix(path.as_ref()).encode()?);
        result.push(path);
    }
    Ok(result)
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
/// LMDB iteration runs on a dedicated thread (RoTxn/cursors are !Send); items are sent over a channel.
pub fn iter_stable_paths<Prof: EngineProfile>(
    app: &App<Prof>,
) -> impl Stream<Item = Result<StablePathInfo>> + Send + 'static {
    let db = *app.app_ctx().db();
    let db_env = app.app_ctx().env().db_env().clone();
    iter_stable_paths_impl(db, db_env)
}

/// Like [`iter_stable_paths`], but takes an environment and an app name instead of a full `App`.
/// Opens the app's database read-only; returns an empty stream if the database doesn't exist.
pub fn iter_stable_paths_by_name<Prof: EngineProfile>(
    env: &Environment<Prof>,
    app_name: &str,
) -> Result<Pin<Box<dyn Stream<Item = Result<StablePathInfo>> + Send + 'static>>> {
    let db_env = env.db_env().clone();
    let rtxn = db_env.read_txn()?;
    let db =
        db_env.open_database::<heed::types::Bytes, heed::types::Bytes>(&rtxn, Some(app_name))?;
    drop(rtxn);
    match db {
        Some(db) => Ok(Box::pin(iter_stable_paths_impl(db, db_env))),
        None => Ok(Box::pin(futures::stream::empty())),
    }
}

fn iter_stable_paths_impl(
    db: db_schema::Database,
    db_env: heed::Env<heed::WithoutTls>,
) -> impl Stream<Item = Result<StablePathInfo>> + Send + 'static {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<StablePathInfo>>(128);

    std::thread::spawn(move || {
        let result: Result<()> = (|| {
            let encoded_key_prefix =
                DbEntryKey::StablePathPrefixPrefix(StablePathPrefix::default()).encode()?;
            let txn = db_env.read_txn()?;

            let mut last_prefix: Option<Vec<u8>> = None;
            for entry in db.prefix_iter(&txn, encoded_key_prefix.as_ref())? {
                let (raw_key, _) = entry?;
                if let Some(last_prefix) = &last_prefix
                    && raw_key.starts_with(last_prefix)
                {
                    continue;
                }
                let key: DbEntryKey = DbEntryKey::decode(raw_key)?;
                let path = match key {
                    DbEntryKey::StablePath(path, _) => path,
                    other => return Err(internal_error!("Expected StablePath, got {other:?}")),
                };
                last_prefix = Some(DbEntryKey::StablePathPrefix(path.as_ref()).encode()?);

                let node_type = if path.as_ref().is_empty() {
                    db_schema::StablePathNodeType::Component
                } else {
                    let path_ref: StablePathRef<'_> = path.as_ref();
                    if let Some((parent_ref, key)) = path_ref.split_parent() {
                        get_path_node_type(&db, &txn, parent_ref, key)?
                            .unwrap_or(db_schema::StablePathNodeType::Directory)
                    } else {
                        db_schema::StablePathNodeType::Component
                    }
                };

                let item = StablePathInfo { path, node_type };
                if tx.blocking_send(Ok(item)).is_err() {
                    break;
                }
            }

            Ok(())
        })();

        if let Err(err) = result {
            let _ = tx.blocking_send(Err(err));
        }
    });

    ReceiverStream::new(rx)
}

fn get_path_node_type(
    db: &db_schema::Database,
    rtxn: &heed::RoTxn<'_>,
    parent_path: StablePathRef<'_>,
    key: &crate::state::stable_path::StableKey,
) -> Result<Option<db_schema::StablePathNodeType>> {
    let encoded_db_key = db_schema::DbEntryKey::StablePath(
        parent_path.into(),
        db_schema::StablePathEntryKey::ChildExistence(key.clone()),
    )
    .encode()?;
    let db_value = db.get(rtxn, encoded_db_key.as_slice())?;
    let Some(db_value) = db_value else {
        return Ok(None);
    };
    let child_existence_info: db_schema::ChildExistenceInfo = from_msgpack_slice(db_value)?;
    Ok(Some(child_existence_info.node_type))
}

pub fn list_app_names<Prof: EngineProfile>(env: &Environment<Prof>) -> Result<Vec<String>> {
    let db_env = env.db_env();
    let rtxn = db_env.read_txn()?;

    let unnamed: heed::Database<Str, DecodeIgnore> = db_env
        .open_database(&rtxn, None)?
        .expect("the unnamed database always exists");

    let mut names = Vec::new();
    for result in unnamed.iter(&rtxn)? {
        let (name, ()) = result?;

        if let Ok(Some(db)) =
            db_env.open_database::<heed::types::Bytes, heed::types::Bytes>(&rtxn, Some(name))
        {
            // Only include databases that have entries (non-empty).
            // Cleared databases are treated as deleted.
            if db.first(&rtxn)?.is_some() {
                names.push(name.to_string());
            }
        }
    }

    Ok(names)
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
}

/// Get detailed information about a single stable path from LMDB
pub fn get_stable_path_detail<Prof: EngineProfile>(
    app: &App<Prof>,
    path: &StablePath,
) -> Result<Option<StablePathDetail>> {
    let db = app.app_ctx().db();
    let txn = app.app_ctx().env().db_env().read_txn()?;

    // Get TrackingInfo (version, processor_name, target_state_items)
    let tracking_key = db_schema::DbEntryKey::StablePath(
        path.clone(),
        db_schema::StablePathEntryKey::TrackingInfo,
    )
    .encode()?;

    let (version, processor_name, target_state_count) =
        if let Some(value) = db.get(&txn, tracking_key.as_slice())? {
            let info: db_schema::StablePathEntryTrackingInfo = from_msgpack_slice(value)?;
            (
                info.version,
                info.processor_name.to_string(),
                info.target_state_items.len(),
            )
        } else {
            (0, String::new(), 0)
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
            get_path_node_type(&db, &txn, parent_ref, key)?
                .unwrap_or(db_schema::StablePathNodeType::Directory)
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
    }))
}

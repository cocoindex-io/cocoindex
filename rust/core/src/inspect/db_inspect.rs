use crate::prelude::*;

use crate::engine::environment::Environment;
use crate::engine::{app::App, profile::EngineProfile};
use crate::state::db_schema::{self, DbEntryKey};
use crate::state::stable_path::{StablePath, StablePathPrefix, StablePathRef};
use async_stream::try_stream;
use cocoindex_utils::deser::from_msgpack_slice;
use futures::stream::Stream;
use heed::types::{DecodeIgnore, Str};

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

/// Represents a stable path with its node type information.
#[derive(Clone, Debug)]
pub struct StablePathWithType {
    pub path: StablePath,
    pub node_type: db_schema::StablePathNodeType,
}

// Re-export StablePathNodeType for use in Python bindings
pub use db_schema::StablePathNodeType;

/// List stable paths along with their node types as an async stream.
pub fn list_stable_paths_with_types_stream<Prof: EngineProfile>(
    app: &App<Prof>,
) -> impl Stream<Item = Result<StablePathWithType>> + Send + 'static {
    // Run the LMDB iteration on a dedicated thread (RoTxn and cursors are !Send),
    // and forward results over a Tokio mpsc channel. The returned stream is then
    // driven from the receiver side, which is Send.
    let app_ctx = app.app_ctx().clone();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<StablePathWithType>>(128);

    std::thread::spawn(move || {
        let result: Result<()> = (|| {
            let encoded_key_prefix =
                DbEntryKey::StablePathPrefixPrefix(StablePathPrefix::default()).encode()?;
            let db = app_ctx.db();
            let txn = app_ctx.env().db_env().read_txn()?;

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
                        get_path_node_type(db, &txn, parent_ref, key)?
                            .unwrap_or(db_schema::StablePathNodeType::Directory)
                    } else {
                        db_schema::StablePathNodeType::Component
                    }
                };

                let item = StablePathWithType { path, node_type };
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

    try_stream! {
        while let Some(item) = rx.recv().await {
            yield item?;
        }
    }
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

use std::pin::Pin;

use crate::prelude::*;

use crate::engine::environment::Environment;
use crate::engine::{app::App, profile::EngineProfile};
use crate::state::db_schema;
use crate::state::stable_path::StablePath;
use crate::state_store::ops;
use futures::stream::{Stream, StreamExt};
use tokio_stream::wrappers::ReceiverStream;

pub fn list_stable_paths<Prof: EngineProfile>(app: &App<Prof>) -> Result<Vec<StablePath>> {
    let store = app.app_ctx().store();
    let rtxn = ops::open_read_txn_for_inspect(app.app_ctx().env().db_env())?;
    ops::list_all_stable_paths(&rtxn, store)
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
    let store = app.app_ctx().store().clone();
    let db_env = app.app_ctx().env().db_env().clone();
    let rx = ops::spawn_iter_stable_paths_with_node_type(store, db_env);
    receiver_to_stable_path_info_stream(rx)
}

/// Like [`iter_stable_paths`], but takes an environment and an app name instead of a full `App`.
/// Opens the app's database read-only; returns an empty stream if the database doesn't exist.
pub fn iter_stable_paths_by_name<Prof: EngineProfile>(
    env: &Environment<Prof>,
    app_name: &str,
) -> Result<Pin<Box<dyn Stream<Item = Result<StablePathInfo>> + Send + 'static>>> {
    let db_env = env.db_env().clone();
    match ops::spawn_iter_stable_paths_with_node_type_for_app_name(db_env, app_name)? {
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

pub fn list_app_names<Prof: EngineProfile>(env: &Environment<Prof>) -> Result<Vec<String>> {
    ops::list_app_names(env.db_env())
}

use std::pin::Pin;

use crate::prelude::*;

use crate::engine::environment::Environment;
use crate::engine::{app::App, profile::EngineProfile};
use crate::state::db_schema;
use crate::state::stable_path::StablePath;
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

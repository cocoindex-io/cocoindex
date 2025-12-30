use crate::prelude::*;

use crate::engine::{app::App, profile::EngineProfile};
use crate::state::db_schema::DbEntryKey;
use crate::state::stable_path::{StablePath, StablePathPrefix};

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

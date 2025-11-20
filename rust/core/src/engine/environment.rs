use crate::prelude::*;

use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::OnceLock};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct EnvironmentSettings {
    pub db_path: PathBuf,
}

struct EnvironmentState {
    db_env: heed::Env<heed::WithoutTls>,
}

#[derive(Clone)]
pub struct Environment {
    state: Arc<EnvironmentState>,
}

impl Environment {
    pub fn new(settings: EnvironmentSettings) -> Result<Self> {
        // Create the directory if not exists.
        std::fs::create_dir_all(&settings.db_path)?;

        let state = Arc::new(EnvironmentState {
            db_env: unsafe {
                heed::EnvOpenOptions::new()
                    .read_txn_without_tls()
                    .open(settings.db_path.clone())
            }?,
        });
        Ok(Self { state })
    }
}

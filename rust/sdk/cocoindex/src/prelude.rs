//! Convenience re-exports for cocoindex pipelines.

pub use crate::ctx::Ctx;
pub use crate::error::{Error, Result};
pub use crate::fs::FileEntry;
pub use crate::stats::RunStats;
pub use crate::{
    App, ContextKey, DropHandle, Progress, StatsGroupHandle, StatsGroupOptions, UpdateHandle,
    UpdateOptions,
};

pub use serde::{Deserialize, Serialize};

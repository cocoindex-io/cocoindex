//! Convenience re-exports for cocoindex pipelines.

pub use crate::ctx::Ctx;
pub use crate::error::{Error, Result};
pub use crate::fs::{
    DirTarget, DirTargetState, DirWalker, FileEntry, FileLike, FileMetadata, FilePath,
    FilePathMatcher, MatchAllFilePathMatcher, PatternFilePathMatcher, declare_dir_target,
    dir_target, mount_dir_target, walk_dir,
};
pub use crate::id::{
    IdGenerator, UuidGenerator, generate_id, generate_id_default, generate_uuid,
    generate_uuid_default,
};
pub use crate::stats::RunStats;
pub use crate::target_state::{
    IntoStableKey, StableKey, TargetAction, TargetActionSink, TargetChildInvalidation,
    TargetHandler, TargetReconcileOutput, TargetState, TargetStateProvider, declare_target_state,
    declare_target_state_with_child, mount_target, register_root_target_states_provider,
};
pub use crate::{
    App, ContextKey, DropHandle, Progress, StatsGroupHandle, StatsGroupOptions, UpdateHandle,
    UpdateOptions,
};

pub use serde::{Deserialize, Serialize};

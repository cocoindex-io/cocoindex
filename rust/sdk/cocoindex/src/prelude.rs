//! Convenience re-exports for cocoindex pipelines.

pub use crate::ctx::Ctx;
pub use crate::entity_resolution::{
    CanonicalSide, EntityEmbedder, ExistingCanonicalPolicy, PairDecision, PairResolver,
    ResolutionEvent, ResolveOptions, ResolvedEntities, resolve_entities,
    resolve_entities_with_events,
};
pub use crate::error::{Error, Result};
pub use crate::file::{
    FileContentCache, FileLike, FileMetadata, FilePath, FilePathMatcher, FileSourceItem,
    MatchAllFilePathMatcher, PatternFilePathMatcher,
};
pub use crate::fs::{
    DirTarget, DirTargetState, DirWalker, FileEntry, declare_dir_target, dir_target,
    mount_dir_target, walk_dir,
};
pub use crate::id::{
    IdGenerator, UuidGenerator, generate_id, generate_id_default, generate_uuid,
    generate_uuid_default,
};
pub use crate::live_component::{
    ExceptionContext, ExceptionHandler, LiveComponent, LiveComponentOperator, LiveMapFeed,
    LiveMapSubscriber, LiveMapView, MountKind,
};
pub use crate::resources::chunk::{Chunk, TextPosition};
pub use crate::resources::embedder::Embedder;
pub use crate::resources::schema::{
    MultiVectorSchema, MultiVectorSchemaProvider, VectorElementType, VectorSchema,
    VectorSchemaProvider,
};
pub use crate::statediff::{
    DiffAction, ManagedBy, ManagedTargetOptions, MutualTrackingRecord, TrackingRecordTransition,
    diff, resolve_system_transition,
};
pub use crate::stats::RunStats;
pub use crate::target_state::{
    ChildTargetDef, IntoStableKey, StableKey, TargetAction, TargetActionSink,
    TargetChildInvalidation, TargetHandler, TargetReconcileOutput, TargetState,
    TargetStateProvider, declare_target_state, declare_target_state_with_child, mount_target,
    register_root_target_states_provider,
};
pub use crate::{
    App, ContextKey, DropHandle, PreviewAction, PreviewValue, Progress, StatsGroupHandle,
    StatsGroupOptions, UpdateHandle, UpdateOptions,
};

pub use serde::{Deserialize, Serialize};

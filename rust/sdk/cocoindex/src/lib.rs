pub mod app;
pub mod batched;
pub mod connectors;
pub mod ctx;
#[cfg(any(feature = "neo4j", feature = "falkordb"))]
mod cypher_graph;
pub mod entity_resolution;
pub mod error;
// Rejects non-finite floats before the JSON round-trip in target connectors that
// serialize rows through `serde_json` (which maps NaN/±Inf to null).
#[cfg(any(
    feature = "sqlite",
    feature = "postgres",
    feature = "doris",
    feature = "surrealdb",
    feature = "neo4j",
    feature = "falkordb"
))]
mod finite;
pub mod live_component;
#[doc(hidden)]
pub mod logic;
pub mod memo;
pub mod mount;
pub mod ops;
pub mod prelude;
pub(crate) mod profile;
pub mod resources;
pub mod row_schema;
#[cfg(any(feature = "doris", feature = "sqlite", feature = "surrealdb"))]
pub(crate) mod sql_ident;
pub mod statediff;
mod stats;
pub mod target_state;
mod typemap;
pub mod user_state;

// Flat re-exports — the public API surface
pub use app::{
    App, AppBuilder, DropHandle, Environment, EnvironmentBuilder, PreviewAction, PreviewValue,
    Progress, StatsGroupHandle, StatsGroupOptions, UpdateHandle, UpdateOptions,
};
pub use batched::Batched;
pub use ctx::{ContextKey, Ctx};
pub use entity_resolution::{
    CanonicalSide, EntityEmbedder, ExistingCanonicalPolicy, PairDecision, PairResolver,
    ResolutionEvent, ResolveOptions, ResolvedEntities, resolve_entities,
    resolve_entities_with_events,
};
pub use error::{Error, Result};
// Re-exported so `#[cocoindex::function]` output can register each function's
// logic fingerprint without the user crate needing a direct `linkme` dependency.
#[doc(hidden)]
pub use linkme;
pub use live_component::{
    ExceptionContext, ExceptionHandler, LiveComponent, LiveComponentOperator, LiveMapFeed,
    LiveMapSubscriber, LiveMapView, MountKind, SingleWatcherGuard, SingleWatcherToken,
};
#[doc(hidden)]
pub use logic::{COCO_FN_LOGIC, FnLogicEntry};
pub use resources::chunk::{Chunk, TextPosition};
pub use resources::embedder::Embedder;
pub use resources::live_map::LiveMap;
pub use resources::rate_limit::RateLimiter;
pub use resources::schema::{
    MultiVectorSchema, MultiVectorSchemaProvider, VectorElementType, VectorSchema,
    VectorSchemaProvider,
};
pub use statediff::{
    CompositeTrackingRecord, DiffAction, ManagedBy, ManagedTargetOptions, MutualTrackingRecord,
    TrackingRecordTransition, diff, diff_composite, resolve_system_transition,
};
pub use stats::{ComponentStats, RunStats, UpdateStats, UpdateStatus};
pub use target_state::{
    ChildTargetDef, IntoStableKey, StableKey, TargetAction, TargetActionSink,
    TargetChildInvalidation, TargetHandler, TargetReconcileOutput, TargetState,
    TargetStateProvider, declare_target_state, declare_target_state_with_child, mount_target,
    register_root_target_states_provider,
};
pub use user_state::{IntoStateKey, StateHandle};

// Re-export proc macros
pub use cocoindex_macros::{SchemaFields, function, mount_each, use_mount};
pub use row_schema::{LogicalType, SchemaField, SchemaFields};

// Re-exported so users can implement the async `LiveComponent` / `LiveMapFeed`
// / `LiveMapView` traits as `#[cocoindex::async_trait]` without taking their own
// dependency on `async-trait`.
pub use async_trait::async_trait;

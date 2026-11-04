use std::{fmt::Debug, hash::Hash, sync::Arc};

use crate::engine::{
    component::ComponentProcessor,
    context::DataDepRecord,
    target_state::{TargetActionSink, TargetHandler},
};
use crate::prelude::*;
use crate::state::stable_path::StablePath;

pub trait Persist: Sized {
    fn to_bytes(&self) -> Result<bytes::Bytes>;

    fn from_bytes(data: &[u8]) -> Result<Self>;
}

impl<T: Persist> Persist for Arc<T> {
    fn to_bytes(&self) -> Result<bytes::Bytes> {
        (**self).to_bytes()
    }

    fn from_bytes(data: &[u8]) -> Result<Self> {
        Ok(Arc::new(T::from_bytes(data)?))
    }
}

pub trait StableFingerprint {
    fn stable_fingerprint(&self) -> utils::fingerprint::Fingerprint;
}

impl<T: StableFingerprint> StableFingerprint for Arc<T> {
    fn stable_fingerprint(&self) -> utils::fingerprint::Fingerprint {
        (**self).stable_fingerprint()
    }
}

pub trait EngineProfile: Debug + Clone + PartialEq + Eq + Hash + Default + 'static {
    type HostRuntimeCtx: Clone + Send + Sync + Eq + Hash + 'static;
    type HostCtx: Send + Sync + 'static;

    type ComponentProc: ComponentProcessor<Self>;
    type FunctionData: Clone + Send + Sync + Persist + 'static;

    type TargetHdl: TargetHandler<Self>;
    type TargetStateTrackingRecord: Send + Persist + 'static;
    type TargetAction: Send + Sync + 'static;
    type TargetActionSink: TargetActionSink<Self>;
    type TargetStateValue: Send + 'static;

    /// Validate the data-dependency records of a stored component memo from
    /// the records alone, through the kinds registered in the host.
    ///
    /// A component's own probe validates its records through its processor's
    /// memo state handler. This entry has no processor: it serves the probe of
    /// a memoized *ancestor*, which validates the rows of the open descendants
    /// its memo lists (see `db_schema::ComponentMemoizationInfo::open_children`)
    /// before reusing its memo, while none of those descendants is mounted. So
    /// a validator runs here outside any component context, with the stored
    /// key and state as its only inputs. `path` names the row for logging.
    ///
    /// Must hold what a handler returns for a hit: records whose kind is no
    /// longer registered, or whose key or state no longer decodes, make
    /// `can_reuse` false (logged, no validator called); a validator's own
    /// error propagates.
    fn validate_data_deps(
        host_runtime_ctx: &Self::HostRuntimeCtx,
        path: &StablePath,
        records: Vec<DataDepRecord>,
    ) -> Result<impl Future<Output = Result<DataDepsValidation>> + Send + 'static>;
}

/// The result of [`EngineProfile::validate_data_deps`]: the shape a memo state
/// handler returns for a hit, for the data-dependency part alone.
pub struct DataDepsValidation {
    /// The records after validation: an unchanged one exactly as stored, a
    /// changed-but-valid one with its new state.
    pub records: Vec<DataDepRecord>,
    /// Every record validated.
    pub can_reuse: bool,
    /// Some record's state moved while staying valid; `records` should
    /// replace the stored ones.
    pub states_changed: bool,
}

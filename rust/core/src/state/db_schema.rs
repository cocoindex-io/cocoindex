use crate::{
    prelude::*,
    state::{
        stable_path::{StablePathPrefix, StablePathRef},
        target_state_path::{TargetStatePathWithProviderId, TargetStateProviderGeneration},
    },
};

use std::{borrow::Cow, collections::BTreeMap, io::Write};

use cocoindex_utils::fingerprint::Fingerprint;
use serde::{Deserialize, Serialize};
use serde_with::{Bytes, serde_as};

use crate::state::{
    stable_path::{StableKey, StablePath},
    target_state_path::TargetStatePath,
};

/// Which writer owns a user-state entry. The two kinds share the `0x34`
/// `UserState*` keyspace but are isolated by a discriminant byte so they
/// never collide on prefix scans (see the layout note on
/// [`StablePathEntryKey::UserState`]).
///
/// * `Regular` — declared by `coco.use_state()` during a component build and
///   subject to set-reduction at flush time (prefetch-all, then prune every
///   loaded-but-not-redeclared key).
/// * `Live` — committed by the live-component machinery (e.g. a bootstrap
///   flag + logic version) and read back via `read_committed_state`. Exempt
///   from the regular flush's prune so a live component's own `process()`
///   (which may itself call `coco.use_state`) can't delete it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateKind {
    Regular,
    Live,
}

impl storekey::Encode for StateKind {
    fn encode<W: Write>(&self, e: &mut storekey::Writer<W>) -> Result<(), storekey::EncodeError> {
        // Avoid 0x00/0x01: `storekey` reserves 0x00 as a delimiter and escapes
        // it (and the 0x01 escape byte itself) with a preceding 0x01, which
        // would expand the tag to two bytes and break the single-byte per-kind
        // prefix. 0x02/0x03 encode as a clean single byte.
        match self {
            StateKind::Regular => e.write_u8(0x02),
            StateKind::Live => e.write_u8(0x03),
        }
    }
}

impl storekey::Decode for StateKind {
    fn decode<D: std::io::BufRead>(
        d: &mut storekey::Reader<D>,
    ) -> Result<Self, storekey::DecodeError> {
        match d.read_u8()? {
            0x02 => Ok(StateKind::Regular),
            0x03 => Ok(StateKind::Live),
            _ => Err(storekey::DecodeError::InvalidFormat),
        }
    }
}

#[derive(Debug)]
pub enum StablePathEntryKey {
    /// Value type: ComponentMemoizationInfo
    ComponentMemoization,

    FunctionMemoizationPrefix,
    /// Value type: FunctionMemoizationEntry
    FunctionMemoization(Fingerprint),

    /// Scan prefix for all user-state entries of one [`StateKind`].
    /// Encodes as `0x34` + the kind byte, a strict prefix of every
    /// `UserState(kind, *)` that never matches the other kind's entries.
    UserStatePrefix(StateKind),
    /// Layout: `0x34` + [`StateKind`] byte + the encoded `StableKey`.
    /// Value type: opaque bytes (msgpack-serialized by the caller).
    UserState(StateKind, StableKey),

    /// Required.
    /// Value type: StablePathEntryTargetStateInfo
    TrackingInfo,

    ChildExistencePrefix,
    /// Value type: ChildExistenceInfo
    ChildExistence(StableKey),

    ChildComponentTombstonePrefix,
    /// Relative path to the parent component.
    ChildComponentTombstone(StablePath),
}

impl storekey::Encode for StablePathEntryKey {
    fn encode<W: Write>(&self, e: &mut storekey::Writer<W>) -> Result<(), storekey::EncodeError> {
        match self {
            // Should not be less than 2.
            StablePathEntryKey::ComponentMemoization => e.write_u8(0x20),
            StablePathEntryKey::FunctionMemoizationPrefix => e.write_u8(0x30),
            StablePathEntryKey::FunctionMemoization(fp) => {
                e.write_u8(0x30)?;
                fp.encode(e)
            }
            StablePathEntryKey::UserStatePrefix(kind) => {
                e.write_u8(0x34)?;
                kind.encode(e)
            }
            StablePathEntryKey::UserState(kind, key) => {
                e.write_u8(0x34)?;
                kind.encode(e)?;
                key.encode(e)
            }
            StablePathEntryKey::TrackingInfo => e.write_u8(0x40),
            StablePathEntryKey::ChildExistencePrefix => e.write_u8(0xa0),
            StablePathEntryKey::ChildExistence(key) => {
                e.write_u8(0xa0)?;
                key.encode(e)
            }
            StablePathEntryKey::ChildComponentTombstonePrefix => e.write_u8(0xb0),
            StablePathEntryKey::ChildComponentTombstone(path) => {
                e.write_u8(0xb0)?;
                path.encode(e)
            }
        }
    }
}

impl storekey::Decode for StablePathEntryKey {
    fn decode<D: std::io::BufRead>(
        d: &mut storekey::Reader<D>,
    ) -> Result<Self, storekey::DecodeError> {
        let key = match d.read_u8()? {
            0x20 => StablePathEntryKey::ComponentMemoization,
            0x30 => {
                let fp = Fingerprint::decode(d)?;
                StablePathEntryKey::FunctionMemoization(fp)
            }
            0x34 => {
                let kind: StateKind = storekey::Decode::decode(d)?;
                let key: StableKey = storekey::Decode::decode(d)?;
                StablePathEntryKey::UserState(kind, key)
            }
            0x40 => StablePathEntryKey::TrackingInfo,
            0xa0 => {
                let key: StableKey = storekey::Decode::decode(d)?;
                StablePathEntryKey::ChildExistence(key)
            }
            0xb0 => {
                let path: StablePath = storekey::Decode::decode(d)?;
                StablePathEntryKey::ChildComponentTombstone(path)
            }
            _ => return Err(storekey::DecodeError::InvalidFormat),
        };
        Ok(key)
    }
}

#[derive(Debug)]
pub enum DbEntryKey<'a> {
    StablePathPrefixPrefix(StablePathPrefix<'a>),
    StablePathPrefix(StablePathRef<'a>),
    StablePath(StablePath, StablePathEntryKey),
    /// Prefix covering all `TargetState` entries, for prefix scans.
    TargetStatePrefix,
    TargetState(TargetStatePath),

    /// Readable name for one target-state path segment, keyed by the lone
    /// segment fingerprint (a pure function of the key, so one entry serves
    /// every path sharing the segment). Written idempotently (write-once) at
    /// precommit time for provider segments, so inspection can resolve
    /// provider-only segments (root providers, attachments) that have no
    /// owner-index/tracking record. Never cleaned up: entries are tiny and
    /// shared across paths.
    /// Value type: StableKey (msgpack)
    TargetSegmentName(Fingerprint),
    /// Prefix covering all `TargetSegmentName` entries, for prefix scans.
    /// Only used by the bench-support store hooks today.
    #[cfg(feature = "bench-support")]
    TargetSegmentNamePrefix,

    /// Value type: IdSequencerInfo
    IdSequencer(StableKey),

    /// Test-only diagnostic prefix over every recovery marker. Runtime
    /// recovery is writer-scoped through `OptimisticWriteWriterPrefix`.
    #[cfg(test)]
    OptimisticWritePrefix,
    /// Scan prefix over recovery markers owned by one component. Recovery
    /// uses this scoped prefix when that component is next executed; it never
    /// scans the app-wide marker keyspace at update startup.
    OptimisticWriteWriterPrefix(StablePath),
    /// Layout: `0x40` + writer `StablePath` + `TargetStatePath` + the
    /// operation UUID (as a tagged [`StableKey::Uuid`], so the trailing
    /// 16 bytes stay unambiguous against `storekey`'s escaping). One entry
    /// per optimistic *operation* — the UUID makes a retry by the same
    /// writer on the same path a distinct record, so a stale cleanup or
    /// commit can never clear a later operation's marker (ABA protection).
    /// Value type: [`OptimisticWriteMarker`]
    OptimisticWrite(StablePath, TargetStatePath, uuid::Uuid),

    /// Layout: `0x50` + the encoded `TargetStatePath`. At most one entry
    /// per target-state path — the CAS slot elected by
    /// `try_claim_optimistic`. The claim is the per-target CAS slot; its
    /// matching `0x40` marker is indexed by writer for lazy recovery.
    /// Value type: [`OptimisticCasClaim`]
    OptimisticCas(TargetStatePath),
}

impl<'a> storekey::Encode for DbEntryKey<'a> {
    fn encode<W: Write>(&self, e: &mut storekey::Writer<W>) -> Result<(), storekey::EncodeError> {
        match self {
            // Should not be less than 2.
            DbEntryKey::StablePathPrefixPrefix(path_prefix) => {
                e.write_u8(0x10)?;
                path_prefix.encode(e)?;
            }
            DbEntryKey::StablePathPrefix(path) => {
                e.write_u8(0x10)?;
                path.encode(e)?;
            }
            DbEntryKey::StablePath(path, key) => {
                e.write_u8(0x10)?;
                path.encode(e)?;
                key.encode(e)?;
            }

            DbEntryKey::TargetStatePrefix => {
                e.write_u8(0x20)?;
            }
            DbEntryKey::TargetState(path) => {
                e.write_u8(0x20)?;
                path.encode(e)?;
            }

            DbEntryKey::TargetSegmentName(fp) => {
                e.write_u8(0x28)?;
                fp.encode(e)?;
            }
            #[cfg(feature = "bench-support")]
            DbEntryKey::TargetSegmentNamePrefix => {
                e.write_u8(0x28)?;
            }

            DbEntryKey::IdSequencer(key) => {
                e.write_u8(0x30)?;
                key.encode(e)?;
            }

            #[cfg(test)]
            DbEntryKey::OptimisticWritePrefix => {
                e.write_u8(0x40)?;
            }
            DbEntryKey::OptimisticWriteWriterPrefix(writer) => {
                e.write_u8(0x40)?;
                writer.encode(e)?;
            }
            DbEntryKey::OptimisticWrite(writer, path, operation_id) => {
                e.write_u8(0x40)?;
                writer.encode(e)?;
                path.encode(e)?;
                StableKey::Uuid(*operation_id).encode(e)?;
            }

            DbEntryKey::OptimisticCas(path) => {
                e.write_u8(0x50)?;
                path.encode(e)?;
            }
        }
        Ok(())
    }
}

impl<'a> storekey::Decode for DbEntryKey<'a> {
    fn decode<D: std::io::BufRead>(
        d: &mut storekey::Reader<D>,
    ) -> Result<Self, storekey::DecodeError> {
        let key = match d.read_u8()? {
            0x10 => {
                let path: StablePath = storekey::Decode::decode(d)?;
                let key: StablePathEntryKey = storekey::Decode::decode(d)?;
                DbEntryKey::StablePath(path, key)
            }
            0x20 => {
                let path: TargetStatePath = storekey::Decode::decode(d)?;
                DbEntryKey::TargetState(path)
            }
            0x28 => {
                let fp: Fingerprint = storekey::Decode::decode(d)?;
                DbEntryKey::TargetSegmentName(fp)
            }
            0x40 => {
                let writer: StablePath = storekey::Decode::decode(d)?;
                let path: TargetStatePath = storekey::Decode::decode(d)?;
                let StableKey::Uuid(operation_id) = storekey::Decode::decode(d)? else {
                    return Err(storekey::DecodeError::InvalidFormat);
                };
                DbEntryKey::OptimisticWrite(writer, path, operation_id)
            }
            0x50 => {
                let path: TargetStatePath = storekey::Decode::decode(d)?;
                DbEntryKey::OptimisticCas(path)
            }
            _ => return Err(storekey::DecodeError::InvalidFormat),
        };
        Ok(key)
    }
}

impl<'a> DbEntryKey<'a> {
    pub fn encode(&self) -> Result<Vec<u8>> {
        storekey::encode_vec(self)
            .map_err(|e| internal_error!("Failed to encode DbEntryKey: {}", e))
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        Ok(storekey::decode(data)?)
    }
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug)]
pub enum MemoizedValue<'a> {
    #[serde(untagged, borrow)]
    Inlined(#[serde_as(as = "Bytes")] Cow<'a, [u8]>),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ComponentMemoizationInfo<'a> {
    #[serde(rename = "F")]
    pub processor_fp: Fingerprint,
    #[serde(rename = "R", borrow)]
    pub return_value: MemoizedValue<'a>,
    #[serde(rename = "L", default, skip_serializing_if = "Vec::is_empty")]
    pub logic_deps: Vec<Fingerprint>,
    /// Generations of the target-state providers this component declared
    /// against, sorted by path. Re-checked on the next probe so a lossy or
    /// destructive schema change invalidates the memo even when the target
    /// isn't part of the memo key. See `TargetProviderDeps`.
    #[serde(rename = "TP", default, skip_serializing_if = "Vec::is_empty")]
    pub target_provider_deps: Vec<(TargetStatePath, TargetStateProviderGeneration)>,
    #[serde(rename = "S", default, skip_serializing_if = "Vec::is_empty", borrow)]
    pub memo_states: Vec<MemoizedValue<'a>>,
    /// Context-borne memo states, keyed by the tracked-context value's fingerprint.
    /// Stored as `Vec<(Fingerprint, _)>` rather than `HashMap` because no one looks up
    /// by fingerprint inside this container — both Rust and Python iterate it linearly
    /// at validation time.
    #[serde(rename = "CS", default, skip_serializing_if = "Vec::is_empty", borrow)]
    pub context_memo_states: Vec<(Fingerprint, Vec<MemoizedValue<'a>>)>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FunctionMemoizationEntry<'a> {
    /// Memoization info is stored in the component metadata
    #[serde(rename = "R", borrow)]
    pub return_value: MemoizedValue<'a>,
    #[serde(rename = "L", default, skip_serializing_if = "Vec::is_empty")]
    pub logic_deps: Vec<Fingerprint>,

    /// Relative paths to the parent components (legacy field, no longer written).
    #[serde(rename = "C", default, skip_serializing_if = "Vec::is_empty")]
    pub child_components: Vec<StablePath>,
    /// Target states that are declared by the function.
    #[serde(rename = "E", default, skip_serializing_if = "Vec::is_empty")]
    pub target_state_paths: Vec<TargetStatePath>,
    /// Generations of the providers those target states were declared against,
    /// sorted by path. See `ComponentMemoizationInfo::target_provider_deps`.
    #[serde(rename = "TP", default, skip_serializing_if = "Vec::is_empty")]
    pub target_provider_deps: Vec<(TargetStatePath, TargetStateProviderGeneration)>,
    /// Dependency entries that are declared by the function.
    /// Only needs to keep dependencies with side effects other than return value (child components / target states / dependency entries with side effects).
    #[serde(rename = "D", default, skip_serializing_if = "Vec::is_empty")]
    pub dependency_memo_entries: Vec<Fingerprint>,
    #[serde(rename = "S", default, skip_serializing_if = "Vec::is_empty", borrow)]
    pub memo_states: Vec<MemoizedValue<'a>>,
    /// Context-borne memo states, keyed by the tracked-context value's fingerprint.
    /// See `ComponentMemoizationInfo::context_memo_states`.
    #[serde(rename = "CS", default, skip_serializing_if = "Vec::is_empty", borrow)]
    pub context_memo_states: Vec<(Fingerprint, Vec<MemoizedValue<'a>>)>,
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug)]
pub enum TargetStateInfoItemState<'a> {
    #[serde(rename = "D")]
    Deleted,
    #[serde(untagged)]
    Existing(
        #[serde_as(as = "Bytes")]
        #[serde(borrow)]
        Cow<'a, [u8]>,
    ),
}

impl<'a> TargetStateInfoItemState<'a> {
    pub fn is_deleted(&self) -> bool {
        matches!(self, TargetStateInfoItemState::Deleted)
    }

    pub fn as_ref(&self) -> Option<&[u8]> {
        match self {
            TargetStateInfoItemState::Deleted => None,
            TargetStateInfoItemState::Existing(s) => Some(s.as_ref()),
        }
    }

    pub fn into_owned(self) -> TargetStateInfoItemState<'static> {
        match self {
            TargetStateInfoItemState::Deleted => TargetStateInfoItemState::Deleted,
            TargetStateInfoItemState::Existing(s) => {
                TargetStateInfoItemState::Existing(Cow::Owned(s.into_owned()))
            }
        }
    }
}

fn u64_is_zero(v: &u64) -> bool {
    *v == 0
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug)]
pub struct TargetStateInfoItem<'a> {
    #[serde_as(as = "Bytes")]
    #[serde(rename = "P", borrow)]
    pub key: Cow<'a, [u8]>,
    #[serde(rename = "S", borrow, default, skip_serializing_if = "Vec::is_empty")]
    pub states: Vec<(/*version*/ u64, TargetStateInfoItemState<'a>)>,

    /// Schema version for the current target state's provider.
    /// It's updated only after commit done. So it reflects the earliest schema version in `states`, if multiple.
    #[serde(rename = "V", default, skip_serializing_if = "u64_is_zero")]
    pub provider_schema_version: u64,

    /// Available when the current item is for a target state creating a provider for child states (e.g. a table).
    /// It decides the generation of the provider.
    #[serde(rename = "G", default, skip_serializing_if = "Option::is_none")]
    pub provider_generation: Option<TargetStateProviderGeneration>,
}

impl<'a> TargetStateInfoItem<'a> {
    pub fn into_owned(self) -> TargetStateInfoItem<'static> {
        TargetStateInfoItem {
            key: Cow::Owned(self.key.into_owned()),
            states: self
                .states
                .into_iter()
                .map(|(v, s)| (v, s.into_owned()))
                .collect(),
            provider_schema_version: self.provider_schema_version,
            provider_generation: self.provider_generation,
        }
    }

    /// True iff this item's `states` carries an unsettled push from a
    /// pre_commit that hasn't been finalized by `commit_in_txn`'s retention
    /// pass — either an in-flight modification by *this* process, a crashed
    /// prior process, or a rolled-back failed attempt.
    ///
    /// Used in the pre_commit detection sub-pass to recognize a *live*
    /// in-flight lifecycle (paired with `pending_process_token == self`).
    /// It does NOT drive `prev_may_be_missing`: multi-state means the sink
    /// holds one of the enumerated `states`, all of which are passed to
    /// reconcile as `prev_states`, so the handler's own `all(prev == desired)`
    /// check decides whether an action is needed. The "sink may be absent"
    /// case is signalled separately by a `Deleted` entry among the states.
    ///
    /// Invariant: at rest (after a successful `commit_in_txn`), every item
    /// has `states.len() <= 1`. Retention always reduces the vec by dropping
    /// pre-curr_version entries and curr_version-Deleted entries. Multi-state
    /// only exists during the write→commit window or after a crash/rollback
    /// of a prior lifecycle.
    pub fn is_pending(&self) -> bool {
        self.states.len() > 1
    }
}

/// Inverted tracking: maps a `TargetStatePath` to the component that owns it.
/// Stored under `DbEntryKey::TargetState(target_state_path)`.
#[derive(Serialize, Deserialize, Debug)]
pub struct TargetStateOwnerInfo {
    #[serde(rename = "C")]
    pub component_path: StablePath,
}

pub const UNKNOWN_PROCESSOR_NAME: &'static str = "<unknown>";

fn unknown_processor_name() -> Cow<'static, str> {
    Cow::Borrowed(UNKNOWN_PROCESSOR_NAME)
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StablePathEntryTrackingInfo<'a> {
    #[serde(rename = "V")]
    pub version: u64,
    #[serde(rename = "I", borrow)]
    pub target_state_items: BTreeMap<TargetStatePathWithProviderId, TargetStateInfoItem<'a>>,
    #[serde(rename = "N", borrow, default = "unknown_processor_name")]
    pub processor_name: Cow<'a, str>,
    /// Set by `pre_commit` when it queues at least one sink action against
    /// this component; cleared by `commit_in_txn` and by
    /// `rollback_pending_tokens` on failure. Distinguishes a live in-flight
    /// lifecycle in *this* process (token equals the process's startup token
    /// → preempting components must back off and retry) from one left by a
    /// crashed prior process (token is something else → observers proceed,
    /// using the per-item multi-state signal to force
    /// `prev_may_be_missing = true`). At-rest value is `None`.
    #[serde(rename = "T", default, skip_serializing_if = "Option::is_none")]
    pub pending_process_token: Option<u128>,
}

impl<'a> StablePathEntryTrackingInfo<'a> {
    pub fn new(processor_name: Cow<'a, str>) -> Self {
        Self {
            version: 0,
            target_state_items: BTreeMap::new(),
            processor_name,
            pending_process_token: None,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Copy, Debug)]
pub enum StablePathNodeType {
    #[serde(rename = "D")]
    Directory,
    #[serde(rename = "C")]
    Component,
}

#[derive(Serialize, Deserialize)]
pub struct ChildExistenceInfo {
    #[serde(rename = "T")]
    pub node_type: StablePathNodeType,
    // TODO: Add a generation, to avoid race conditions during deletion,
    // e.g. when the parent is cleaning up the child asynchronously, there's
    // incremental reinsertion (based on change stream) for the child, which
    // makes another generation of the child appear again.
}

#[derive(Serialize, Deserialize, Debug)]
pub struct IdSequencerInfo {
    #[serde(rename = "N")]
    pub next_id: u64,
}

/// Lifecycle stage of one optimistic write, stored in its recovery marker.
///
/// The phase is what makes the marker safe to hand between the writing
/// task, normal submit, same-run cleanup, and a later process's recovery
/// sweep: every transition is a compare-and-set on the exact marker key,
/// so only one of them can own the operation at a time.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimisticWritePhase {
    /// The eager sink call may be in flight. Set before any external I/O.
    #[serde(rename = "W")]
    Writing,
    /// The eager attempt finished (successfully or not); normal submit is
    /// now the authoritative writer. A caught eager error leaves the
    /// operation here so submit can still heal it.
    #[serde(rename = "P")]
    PendingSubmit,
    /// Normal submit's sink apply may be in flight. Only an operation in
    /// this phase can be confirmed by `commit`.
    #[serde(rename = "S")]
    Submitting,
    /// Cleanup owns the right to delete this operation's external row.
    /// Durable so a crash mid-cleanup resumes rather than restarts.
    #[serde(rename = "C")]
    Cleaning,
}

/// Per-operation crash bookkeeping for one optimistic write, stored under
/// [`DbEntryKey::OptimisticWrite`].
///
/// Target-state path, writer path and operation ID all live in the key, so
/// the value only carries what the key can't express.
#[derive(Serialize, Deserialize, Debug)]
pub struct OptimisticWriteMarker {
    /// Liveness token of the process that created the marker. Recovery
    /// skips markers belonging to the current process — those are owned by
    /// an in-flight task, not stranded.
    #[serde(rename = "T")]
    pub process_token: u128,
    /// Item key within the provider, needed to derive the delete action
    /// during cleanup without reversing the path fingerprints.
    #[serde(rename = "K")]
    pub item_key: StableKey,
    #[serde(rename = "P")]
    pub phase: OptimisticWritePhase,
}

/// Identity of one optimistic write operation — exactly the parts that
/// make up its [`DbEntryKey::OptimisticWrite`] key.
///
/// Every AppStore mutation of a marker or claim is guarded on the full
/// identity, so an operation can only ever advance or clear *its own*
/// records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimisticOperationId {
    pub target_state_path: TargetStatePath,
    /// Stable path of the component that issued the write.
    pub writer: StablePath,
    pub operation_id: uuid::Uuid,
}

impl std::fmt::Display for OptimisticOperationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}@{}#{}",
            self.target_state_path, self.writer, self.operation_id
        )
    }
}

/// The conditional-write CAS slot for one target-state path, stored under
/// [`DbEntryKey::OptimisticCas`].
///
/// Repeats the winner's exact operation identity so a stale cleanup or
/// commit can never clear a *replacement* claim taken by a later winner.
#[derive(Serialize, Deserialize, Debug)]
pub struct OptimisticCasClaim {
    #[serde(rename = "O")]
    pub operation_id: uuid::Uuid,
    #[serde(rename = "T")]
    pub process_token: u128,
    #[serde(rename = "W")]
    pub writer: StablePath,
}

#[cfg(test)]
mod tests {
    use super::*;
    use cocoindex_utils::deser::from_msgpack_slice;
    use std::io::Cursor;

    fn roundtrip_entry_key(key: &StablePathEntryKey) -> StablePathEntryKey {
        let bytes = storekey::encode_vec(key).expect("encode");
        storekey::decode(Cursor::new(bytes)).expect("decode")
    }

    /// Roundtrip test for every decodable `StablePathEntryKey` variant,
    /// including both pre-existing and the new `UserState` variants.
    /// `*Prefix` variants are encode-only (used as raw LMDB scan prefixes)
    /// and are not included here.
    #[test]
    fn stable_path_entry_key_roundtrip() {
        let fp = utils::fingerprint::Fingerprint([0xAB; 16]);
        let child_path = StablePath(Arc::from(vec![StableKey::Str(Arc::from("child"))]));

        assert!(matches!(
            roundtrip_entry_key(&StablePathEntryKey::ComponentMemoization),
            StablePathEntryKey::ComponentMemoization
        ));

        let decoded = roundtrip_entry_key(&StablePathEntryKey::FunctionMemoization(fp));
        assert!(matches!(decoded, StablePathEntryKey::FunctionMemoization(f) if f == fp));

        assert!(matches!(
            roundtrip_entry_key(&StablePathEntryKey::TrackingInfo),
            StablePathEntryKey::TrackingInfo
        ));

        let decoded = roundtrip_entry_key(&StablePathEntryKey::ChildExistence(StableKey::Str(
            Arc::from("child"),
        )));
        assert!(
            matches!(decoded, StablePathEntryKey::ChildExistence(StableKey::Str(s)) if s.as_ref() == "child")
        );

        let decoded = roundtrip_entry_key(&StablePathEntryKey::ChildComponentTombstone(
            child_path.clone(),
        ));
        assert!(
            matches!(decoded, StablePathEntryKey::ChildComponentTombstone(p) if p == child_path)
        );

        // UserState with several StableKey types, across both kinds.
        let user_keys: Vec<StableKey> = vec![
            StableKey::Str(Arc::from("counter")),
            StableKey::Int(42),
            StableKey::Symbol(Arc::from("sys/state")),
            StableKey::Bytes(Arc::from(&b"raw\x00key"[..])),
        ];
        for kind in [StateKind::Regular, StateKind::Live] {
            for user_key in &user_keys {
                let decoded =
                    roundtrip_entry_key(&StablePathEntryKey::UserState(kind, user_key.clone()));
                assert!(
                    matches!(&decoded, StablePathEntryKey::UserState(k, key) if *k == kind && key == user_key),
                    "UserState({kind:?}, {user_key:?}) did not roundtrip correctly"
                );
            }
        }
    }

    /// `StateKind` roundtrips through storekey as a single discriminant byte.
    #[test]
    fn state_kind_roundtrip() {
        for kind in [StateKind::Regular, StateKind::Live] {
            let bytes = storekey::encode_vec(&kind).expect("encode");
            assert_eq!(bytes.len(), 1, "StateKind must encode to one byte");
            let decoded: StateKind = storekey::decode(Cursor::new(bytes)).expect("decode");
            assert_eq!(decoded, kind);
        }
        // Distinct discriminants so the two keyspaces never alias.
        assert_ne!(
            storekey::encode_vec(&StateKind::Regular).unwrap(),
            storekey::encode_vec(&StateKind::Live).unwrap(),
        );
    }

    /// `UserStatePrefix(kind)` must encode as `0x34` followed by the kind
    /// byte. Documents the wire format and guards against accidental
    /// discriminant collisions.
    #[test]
    fn user_state_prefix_discriminant_is_0x34() {
        // NOTE: `0x34u8` uses an explicit primitive suffix to force a 1-byte allocation.
        // Without `u8`, Rust infers `0x34` as `i32` (4 bytes), causing a compile-time type
        // mismatch with `bytes` (`Vec<u8>`).
        let regular =
            storekey::encode_vec(&StablePathEntryKey::UserStatePrefix(StateKind::Regular))
                .expect("encode");
        assert_eq!(regular, &[0x34u8, 0x02]);
        let live = storekey::encode_vec(&StablePathEntryKey::UserStatePrefix(StateKind::Live))
            .expect("encode");
        assert_eq!(live, &[0x34u8, 0x03]);
    }

    /// Every `UserState(kind, key)` encoding must start with the matching
    /// `UserStatePrefix(kind)` encoding. This is the invariant that makes
    /// LMDB prefix scans correct: `prefix_iter` with the prefix key will hit
    /// exactly the right entries.
    #[test]
    fn user_state_key_starts_with_prefix() {
        let cases: Vec<StableKey> = vec![
            StableKey::Str(Arc::from("my_state")),
            StableKey::Int(0),
            StableKey::Null,
            StableKey::Bytes(Arc::from(&b""[..])),
        ];
        for kind in [StateKind::Regular, StateKind::Live] {
            let prefix_bytes =
                storekey::encode_vec(&StablePathEntryKey::UserStatePrefix(kind)).expect("encode");
            for user_key in &cases {
                let key_bytes =
                    storekey::encode_vec(&StablePathEntryKey::UserState(kind, user_key.clone()))
                        .expect("encode");
                assert!(
                    key_bytes.starts_with(&prefix_bytes),
                    "UserState({kind:?}, {user_key:?}) bytes don't start with UserStatePrefix({kind:?}) bytes"
                );
            }
        }
    }

    /// A `UserStatePrefix(Regular)` scan must never match a `Live` entry (and
    /// vice versa). This is the isolation guarantee that lets a live
    /// component's regular flush prune `Regular` keys without touching the
    /// `Live` bootstrap state committed by the live machinery.
    #[test]
    fn user_state_prefix_does_not_cross_kinds() {
        let user_key = StableKey::Str(Arc::from("bootstrap"));
        let regular_prefix =
            storekey::encode_vec(&StablePathEntryKey::UserStatePrefix(StateKind::Regular))
                .expect("encode");
        let live_prefix =
            storekey::encode_vec(&StablePathEntryKey::UserStatePrefix(StateKind::Live))
                .expect("encode");
        let live_key = storekey::encode_vec(&StablePathEntryKey::UserState(
            StateKind::Live,
            user_key.clone(),
        ))
        .expect("encode");
        let regular_key = storekey::encode_vec(&StablePathEntryKey::UserState(
            StateKind::Regular,
            user_key.clone(),
        ))
        .expect("encode");

        assert!(
            !live_key.starts_with(&regular_prefix),
            "Live entry must not match the Regular prefix"
        );
        assert!(
            !regular_key.starts_with(&live_prefix),
            "Regular entry must not match the Live prefix"
        );
    }

    /// Full `DbEntryKey::StablePath(path, UserState(key))` roundtrip.
    #[test]
    fn db_entry_key_user_state_roundtrip() {
        let path = StablePath(Arc::from(vec![
            StableKey::Str(Arc::from("docs")),
            StableKey::Str(Arc::from("intro.md")),
        ]));
        let user_key = StableKey::Str(Arc::from("visit_count"));

        let entry = DbEntryKey::StablePath(
            path.clone(),
            StablePathEntryKey::UserState(StateKind::Live, user_key.clone()),
        );
        let bytes = entry.encode().expect("encode");
        let decoded = DbEntryKey::decode(&bytes).expect("decode");

        match decoded {
            DbEntryKey::StablePath(p, StablePathEntryKey::UserState(kind, k)) => {
                assert_eq!(p, path);
                assert_eq!(kind, StateKind::Live);
                assert_eq!(k, user_key);
            }
            other => panic!("expected StablePath/UserState, got {other:?}"),
        }
    }

    /// `key_user_state_prefix(path)` bytes are a strict prefix of
    /// `key_user_state(path, key)` bytes. Validates the LMDB scan
    /// boundary at the full `DbEntryKey` level.
    #[test]
    fn db_entry_key_user_state_prefix_scan() {
        let path = StablePath(Arc::from(vec![StableKey::Str(Arc::from("docs/intro.md"))]));

        let prefix_bytes = DbEntryKey::StablePath(
            path.clone(),
            StablePathEntryKey::UserStatePrefix(StateKind::Regular),
        )
        .encode()
        .expect("encode");
        let state_bytes = DbEntryKey::StablePath(
            path.clone(),
            StablePathEntryKey::UserState(StateKind::Regular, StableKey::Str(Arc::from("counter"))),
        )
        .encode()
        .expect("encode");

        assert!(
            state_bytes.starts_with(&prefix_bytes),
            "UserState key bytes don't start with UserStatePrefix bytes in DbEntryKey context"
        );
        assert!(
            state_bytes.len() > prefix_bytes.len(),
            "UserState key bytes should be strictly longer than prefix bytes"
        );
    }

    /// `TargetSegmentName` roundtrips and never matches the `TargetState`
    /// prefix scan (its `0x28` discriminant is not an extension of `0x20`).
    #[test]
    fn target_segment_name_roundtrip_and_isolation() {
        let fp = utils::fingerprint::Fingerprint([0xCD; 16]);
        let bytes = DbEntryKey::TargetSegmentName(fp).encode().expect("encode");
        match DbEntryKey::decode(&bytes).expect("decode") {
            DbEntryKey::TargetSegmentName(decoded) => assert_eq!(decoded, fp),
            other => panic!("expected TargetSegmentName, got {other:?}"),
        }

        let target_state_prefix = DbEntryKey::TargetStatePrefix.encode().expect("encode");
        assert!(!bytes.starts_with(&target_state_prefix));
    }

    /// Prefix for path A must not match entries under path B.
    /// Guards the scoping guarantee: a user-state prefix scan for path_a
    /// never returns entries that belong to path_b.
    #[test]
    fn user_state_prefix_does_not_cross_paths() {
        let path_a = StablePath(Arc::from(vec![StableKey::Str(Arc::from("file_a.md"))]));
        let path_b = StablePath(Arc::from(vec![StableKey::Str(Arc::from("file_b.md"))]));

        let prefix_a = DbEntryKey::StablePath(
            path_a.clone(),
            StablePathEntryKey::UserStatePrefix(StateKind::Regular),
        )
        .encode()
        .expect("encode");
        let state_b = DbEntryKey::StablePath(
            path_b,
            StablePathEntryKey::UserState(StateKind::Regular, StableKey::Str(Arc::from("x"))),
        )
        .encode()
        .expect("encode");

        assert!(
            !state_b.starts_with(&prefix_a),
            "path_b UserState key incorrectly starts with path_a's prefix"
        );
    }

    // --- Optimistic write markers / CAS claims -----------------------------

    fn tsp(seed: u8) -> TargetStatePath {
        TargetStatePath::new(utils::fingerprint::Fingerprint([seed; 16]), None)
    }

    fn writer(name: &str) -> StablePath {
        StablePath(Arc::from(vec![StableKey::Str(Arc::from(name))]))
    }

    const ALL_PHASES: [OptimisticWritePhase; 4] = [
        OptimisticWritePhase::Writing,
        OptimisticWritePhase::PendingSubmit,
        OptimisticWritePhase::Submitting,
        OptimisticWritePhase::Cleaning,
    ];

    /// `OptimisticWrite` keys roundtrip with every component intact, and
    /// the three identity parts are all discriminating.
    #[test]
    fn optimistic_write_key_roundtrip() {
        let path = tsp(0x11);
        let w = writer("comp/a");
        let op = uuid::Uuid::from_bytes([0x7; 16]);

        let bytes = DbEntryKey::OptimisticWrite(w.clone(), path.clone(), op)
            .encode()
            .expect("encode");
        match DbEntryKey::decode(&bytes).expect("decode") {
            DbEntryKey::OptimisticWrite(wr, p, id) => {
                assert_eq!(p, path);
                assert_eq!(wr, w);
                assert_eq!(id, op);
            }
            other => panic!("expected OptimisticWrite, got {other:?}"),
        }

        // Each identity part changes the key.
        let other_op =
            DbEntryKey::OptimisticWrite(w.clone(), path.clone(), uuid::Uuid::from_bytes([0x8; 16]))
                .encode()
                .unwrap();
        let other_writer = DbEntryKey::OptimisticWrite(writer("comp/b"), path.clone(), op)
            .encode()
            .unwrap();
        let other_path = DbEntryKey::OptimisticWrite(w, tsp(0x12), op)
            .encode()
            .unwrap();
        assert_ne!(bytes, other_op);
        assert_ne!(bytes, other_writer);
        assert_ne!(bytes, other_path);
    }

    #[test]
    fn optimistic_cas_key_roundtrip() {
        let path = tsp(0x21);
        let bytes = DbEntryKey::OptimisticCas(path.clone())
            .encode()
            .expect("encode");
        match DbEntryKey::decode(&bytes).expect("decode") {
            DbEntryKey::OptimisticCas(p) => assert_eq!(p, path),
            other => panic!("expected OptimisticCas, got {other:?}"),
        }
    }

    /// The app-wide prefix is the bare `0x40` tag and the writer-scoped
    /// prefix is `0x40` + component path. Runtime recovery uses only the
    /// latter.
    #[test]
    fn optimistic_write_prefix_scans() {
        let all = DbEntryKey::OptimisticWritePrefix.encode().unwrap();
        assert_eq!(all, vec![0x40u8]);

        let writer_a = writer("w1");
        let writer_b = writer("w2");
        let prefix_a = DbEntryKey::OptimisticWriteWriterPrefix(writer_a.clone())
            .encode()
            .unwrap();

        for (i, path) in [tsp(0x31), tsp(0x32)].into_iter().enumerate() {
            let key_a = DbEntryKey::OptimisticWrite(
                writer_a.clone(),
                path.clone(),
                uuid::Uuid::from_bytes([i as u8; 16]),
            )
            .encode()
            .unwrap();
            assert!(key_a.starts_with(&all));
            assert!(key_a.starts_with(&prefix_a));
            assert!(key_a.len() > prefix_a.len());

            let key_b = DbEntryKey::OptimisticWrite(
                writer_b.clone(),
                path,
                uuid::Uuid::from_bytes([i as u8; 16]),
            )
            .encode()
            .unwrap();
            assert!(key_b.starts_with(&all));
            assert!(
                !key_b.starts_with(&prefix_a),
                "writer-scoped prefix must not match another component's marker"
            );
        }
    }

    /// The recovery-marker keyspace (`0x40`) and the CAS keyspace (`0x50`)
    /// never alias, in either direction, even for the same path.
    #[test]
    fn optimistic_keyspaces_are_isolated() {
        let path = tsp(0x41);
        let marker_prefix = DbEntryKey::OptimisticWritePrefix.encode().unwrap();
        let marker =
            DbEntryKey::OptimisticWrite(writer("w"), path.clone(), uuid::Uuid::from_bytes([1; 16]))
                .encode()
                .unwrap();
        let cas = DbEntryKey::OptimisticCas(path.clone()).encode().unwrap();

        assert!(!cas.starts_with(&marker_prefix));
        assert_ne!(marker, cas);

        // Neither collides with the pre-existing top-level keyspaces.
        let target_state = DbEntryKey::TargetState(path).encode().unwrap();
        let id_seq = DbEntryKey::IdSequencer(StableKey::Int(1)).encode().unwrap();
        for existing in [&target_state, &id_seq] {
            assert!(!existing.starts_with(&marker_prefix));
            assert!(!existing.starts_with(&[0x50u8][..]));
        }
    }

    /// Marker values roundtrip through msgpack for every phase.
    #[test]
    fn optimistic_marker_value_roundtrip() {
        for phase in ALL_PHASES {
            let marker = OptimisticWriteMarker {
                process_token: 0xdead_beef_u128,
                item_key: StableKey::Str(Arc::from("Albert Einstein")),
                phase,
            };
            let bytes = rmp_serde::to_vec_named(&marker).unwrap();
            let decoded: OptimisticWriteMarker = from_msgpack_slice(&bytes).unwrap();
            assert_eq!(decoded.process_token, marker.process_token);
            assert_eq!(decoded.item_key, marker.item_key);
            assert_eq!(decoded.phase, phase);
        }
    }

    #[test]
    fn optimistic_cas_claim_value_roundtrip() {
        let claim = OptimisticCasClaim {
            operation_id: uuid::Uuid::from_bytes([9; 16]),
            process_token: 7,
            writer: writer("comp/x"),
        };
        let bytes = rmp_serde::to_vec_named(&claim).unwrap();
        let decoded: OptimisticCasClaim = from_msgpack_slice(&bytes).unwrap();
        assert_eq!(decoded.operation_id, claim.operation_id);
        assert_eq!(decoded.process_token, claim.process_token);
        assert_eq!(decoded.writer, claim.writer);
    }
}

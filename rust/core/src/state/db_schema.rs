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
            _ => return Err(storekey::DecodeError::InvalidFormat),
        };
        Ok(key)
    }
}

impl<'a> DbEntryKey<'a> {
    pub fn encode(&self) -> Result<Vec<u8>> {
        // Room for a typical key up front: `storekey` writes byte by byte, so
        // an empty buffer would grow (and reallocate) several times per key.
        let mut buf = Vec::with_capacity(64);
        storekey::encode(&mut buf, self).map_err(|e| match e {
            storekey::EncodeError::Custom(e) => internal_error!("Failed to encode DbEntryKey: {e}"),
            storekey::EncodeError::Io(e) => internal_error!("Failed to encode DbEntryKey: {e}"),
        })?;
        Ok(buf)
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

/// One data dependency a body recorded while it ran (`use_data_dep`): what it
/// read (`key`) and what it observed (`state`), both as the serialized
/// snapshot taken at the call, with the fingerprints of those bytes. Validated
/// on the next probe by the kind registered under `kind_fp`.
#[derive(Serialize, Deserialize, Debug)]
pub struct DataDepEntry<'a> {
    #[serde(rename = "K")]
    pub kind_fp: Fingerprint,
    #[serde(rename = "KF")]
    pub key_fp: Fingerprint,
    #[serde(rename = "SF")]
    pub state_fp: Fingerprint,
    #[serde(rename = "KV", borrow)]
    pub key: MemoizedValue<'a>,
    #[serde(rename = "SV", borrow)]
    pub state: MemoizedValue<'a>,
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
    /// Data dependencies the component's own run recorded, sorted by
    /// `(kind_fp, key_fp)`. Never those of its descendants: see
    /// `has_open_descendants`.
    #[serde(rename = "DD", default, skip_serializing_if = "Vec::is_empty", borrow)]
    pub data_deps: Vec<DataDepEntry<'a>>,
    /// A descendant component recorded data dependencies when this memo was
    /// stored. A reuse of this memo carries the descendant over, so it is
    /// allowed only once the descendant's own records have been validated from
    /// its row, reached through `open_children`; a flagged memo whose list is
    /// empty (one written before the list existed) is never reused.
    #[serde(rename = "OD", default, skip_serializing_if = "std::ops::Not::not")]
    pub has_open_descendants: bool,
    /// The direct children (paths relative to this component) that reported
    /// data dependencies in their subtree when this memo was stored, sorted.
    /// They are the entry points of the transitive validation a probe runs
    /// before reusing a flagged memo: each one's row is read, its own records
    /// validated, and its list followed in turn. A child with no row (a
    /// non-memoized one, or one whose run observed a key in two states) makes
    /// the probe a miss. Closed children are not listed: a reuse carries them
    /// over unvalidated, as it always has.
    #[serde(rename = "OC", default, skip_serializing_if = "Vec::is_empty")]
    pub open_children: Vec<StablePath>,
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
    /// Data dependencies the call recorded (its own and its callees'), sorted
    /// by `(kind_fp, key_fp)`. See `ComponentMemoizationInfo::data_deps`.
    #[serde(rename = "DD", default, skip_serializing_if = "Vec::is_empty", borrow)]
    pub data_deps: Vec<DataDepEntry<'a>>,
}

/// Stored as the string `"D"` (`Deleted`) or as the state's bytes
/// (`Existing`, a msgpack `bin`). Deserialized by hand: the derived form of
/// this tagged-plus-untagged enum buffered every value and first tried it as
/// the tagged variant, building (and discarding) a serde error for every
/// `Existing` state of every tracking record it decoded.
#[serde_as]
#[derive(Serialize, Debug)]
pub enum TargetStateInfoItemState<'a> {
    #[serde(rename = "D")]
    Deleted,
    #[serde(untagged)]
    Existing(#[serde_as(as = "Bytes")] Cow<'a, [u8]>),
}

impl<'de: 'a, 'a> Deserialize<'de> for TargetStateInfoItemState<'a> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct StateVisitor<'a>(std::marker::PhantomData<&'a ()>);

        impl<'de: 'a, 'a> serde::de::Visitor<'de> for StateVisitor<'a> {
            type Value = TargetStateInfoItemState<'a>;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("the deleted marker \"D\" or a target state's bytes")
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                if v == "D" {
                    Ok(TargetStateInfoItemState::Deleted)
                } else {
                    Err(E::invalid_value(serde::de::Unexpected::Str(v), &self))
                }
            }

            fn visit_borrowed_bytes<E: serde::de::Error>(
                self,
                v: &'de [u8],
            ) -> Result<Self::Value, E> {
                Ok(TargetStateInfoItemState::Existing(Cow::Borrowed(v)))
            }

            fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
                Ok(TargetStateInfoItemState::Existing(Cow::Owned(v.to_vec())))
            }

            fn visit_byte_buf<E: serde::de::Error>(self, v: Vec<u8>) -> Result<Self::Value, E> {
                Ok(TargetStateInfoItemState::Existing(Cow::Owned(v)))
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                // `serde_with::Bytes` also reads a sequence of byte values.
                let mut bytes = Vec::with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(byte) = seq.next_element::<u8>()? {
                    bytes.push(byte);
                }
                Ok(TargetStateInfoItemState::Existing(Cow::Owned(bytes)))
            }
        }

        deserializer.deserialize_any(StateVisitor(std::marker::PhantomData))
    }
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

pub const UNKNOWN_PROCESSOR_NAME: &str = "<unknown>";

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// The stored forms of both `TargetStateInfoItemState` variants, as the
    /// derived (tagged-plus-untagged) implementation wrote them: `"D"` as a
    /// msgpack fixstr, a state as a msgpack `bin`. Serialization still
    /// produces exactly these bytes, and the hand-written deserializer reads
    /// them back, borrowing the state bytes.
    #[test]
    fn target_state_info_item_state_stored_form() {
        let deleted_bytes: &[u8] = &[0xa1, b'D'];
        let existing_bytes: &[u8] = &[0xc4, 2, b'x', b'y'];

        assert_eq!(
            rmp_serde::to_vec_named(&TargetStateInfoItemState::Deleted).unwrap(),
            deleted_bytes
        );
        assert_eq!(
            rmp_serde::to_vec_named(&TargetStateInfoItemState::Existing(Cow::Borrowed(b"xy")))
                .unwrap(),
            existing_bytes
        );

        let deleted: TargetStateInfoItemState<'_> = rmp_serde::from_slice(deleted_bytes).unwrap();
        assert!(deleted.is_deleted());
        let existing: TargetStateInfoItemState<'_> = rmp_serde::from_slice(existing_bytes).unwrap();
        assert!(matches!(
            existing,
            TargetStateInfoItemState::Existing(Cow::Borrowed(b"xy"))
        ));

        let other_str: &[u8] = &[0xa1, b'X'];
        assert!(rmp_serde::from_slice::<TargetStateInfoItemState<'_>>(other_str).is_err());
    }

    /// A whole tracking item round-trips, versions and both states included.
    #[test]
    fn target_state_info_item_roundtrip() {
        let item = TargetStateInfoItem {
            key: Cow::Borrowed(b"k"),
            states: vec![
                (1, TargetStateInfoItemState::Deleted),
                (
                    2,
                    TargetStateInfoItemState::Existing(Cow::Borrowed(b"\x00\x01state")),
                ),
            ],
            provider_schema_version: 3,
            provider_generation: None,
        };
        let bytes = rmp_serde::to_vec_named(&item).unwrap();
        let decoded: TargetStateInfoItem<'_> = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded.key.as_ref(), b"k");
        assert_eq!(decoded.provider_schema_version, 3);
        assert_eq!(decoded.states.len(), 2);
        assert!(decoded.states[0].1.is_deleted() && decoded.states[0].0 == 1);
        assert_eq!(decoded.states[1].0, 2);
        assert_eq!(decoded.states[1].1.as_ref(), Some(&b"\x00\x01state"[..]));
        assert_eq!(rmp_serde::to_vec_named(&decoded).unwrap(), bytes);
    }

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

    /// A component memo flagged `has_open_descendants` before `open_children`
    /// existed decodes with an empty list (the engine then never reuses it),
    /// and a memo that lists its open children round-trips.
    #[test]
    fn component_memo_open_children_roundtrip_and_legacy_flag() {
        let fp = utils::fingerprint::Fingerprint([0x11; 16]);
        let child = StablePath(Arc::from(vec![
            StableKey::Str(Arc::from("child")),
            StableKey::Str(Arc::from("plan")),
        ]));
        let info = ComponentMemoizationInfo {
            processor_fp: fp,
            return_value: MemoizedValue::Inlined(Cow::Borrowed(b"ret")),
            logic_deps: vec![],
            target_provider_deps: vec![],
            memo_states: vec![],
            context_memo_states: vec![],
            data_deps: vec![],
            has_open_descendants: true,
            open_children: vec![child.clone()],
        };
        let bytes = rmp_serde::to_vec_named(&info).expect("encode");
        let decoded: ComponentMemoizationInfo<'_> =
            cocoindex_utils::deser::from_msgpack_slice(&bytes).expect("decode");
        assert!(decoded.has_open_descendants);
        assert_eq!(decoded.open_children, vec![child]);

        // The same row as written before the list existed: only the flag.
        let legacy = rmp_serde::to_vec_named(&ComponentMemoizationInfo {
            open_children: vec![],
            ..info
        })
        .expect("encode");
        let decoded: ComponentMemoizationInfo<'_> =
            cocoindex_utils::deser::from_msgpack_slice(&legacy).expect("decode");
        assert!(decoded.has_open_descendants);
        assert!(decoded.open_children.is_empty());
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
}

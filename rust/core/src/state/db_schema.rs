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

#[derive(Debug)]
pub enum StablePathEntryKey {
    /// Value type: ComponentMemoizationInfo
    ComponentMemoization,

    FunctionMemoizationPrefix,
    /// Value type: FunctionMemoizationEntry
    FunctionMemoization(Fingerprint),

    /// Required.
    /// Value type: StablePathEntryTargetStateInfo
    TrackingInfo,

    ChildExistencePrefix,
    /// Value type: ChildExistenceInfo
    ChildExistence(StableKey),

    ChildComponentTombstonePrefix,
    /// Relative path to the parent component.
    ChildComponentTombstone(StablePath),

    UserStatePrefix,
    /// Value type: opaque bytes (msgpack-serialized by the caller).
    UserState(StableKey),
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
            StablePathEntryKey::UserStatePrefix => e.write_u8(0xc0),
            StablePathEntryKey::UserState(key) => {
                e.write_u8(0xc0)?;
                key.encode(e)
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
            0x40 => StablePathEntryKey::TrackingInfo,
            0xa0 => {
                let key: StableKey = storekey::Decode::decode(d)?;
                StablePathEntryKey::ChildExistence(key)
            }
            0xb0 => {
                let path: StablePath = storekey::Decode::decode(d)?;
                StablePathEntryKey::ChildComponentTombstone(path)
            }
            0xc0 => {
                let key: StableKey = storekey::Decode::decode(d)?;
                StablePathEntryKey::UserState(key)
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
    TargetState(TargetStatePath),

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

            DbEntryKey::TargetState(path) => {
                e.write_u8(0x20)?;
                path.encode(e)?;
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

#[cfg(test)]
mod tests {
    use super::*;
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

        // UserState with several StableKey types.
        let user_keys: Vec<StableKey> = vec![
            StableKey::Str(Arc::from("counter")),
            StableKey::Int(42),
            StableKey::Symbol(Arc::from("sys/state")),
            StableKey::Bytes(Arc::from(&b"raw\x00key"[..])),
        ];
        for user_key in user_keys {
            let decoded = roundtrip_entry_key(&StablePathEntryKey::UserState(user_key.clone()));
            assert!(
                matches!(&decoded, StablePathEntryKey::UserState(k) if k == &user_key),
                "UserState({user_key:?}) did not roundtrip correctly"
            );
        }
    }

    /// `UserStatePrefix` must encode as a single byte `0xc0`.
    /// Documents the wire format and guards against accidental discriminant collisions.
    #[test]
    fn user_state_prefix_discriminant_is_0xc0() {
        let bytes = storekey::encode_vec(&StablePathEntryKey::UserStatePrefix).expect("encode");
        // NOTE: `0xc0u8` uses an explicit primitive suffix to force a 1-byte allocation.
        // Without `u8`, Rust infers `0xc0` as `i32` (4 bytes), causing a compile-time type
        // mismatch with `bytes` (`Vec<u8>`).
        assert_eq!(bytes, &[0xc0u8]);
    }

    /// Every `UserState(key)` encoding must start with the `UserStatePrefix`
    /// encoding. This is the invariant that makes LMDB prefix scans correct:
    /// `prefix_iter` with the prefix key will hit exactly the right entries.
    #[test]
    fn user_state_key_starts_with_prefix() {
        let prefix_bytes =
            storekey::encode_vec(&StablePathEntryKey::UserStatePrefix).expect("encode");

        let cases: Vec<StableKey> = vec![
            StableKey::Str(Arc::from("my_state")),
            StableKey::Int(0),
            StableKey::Null,
            StableKey::Bytes(Arc::from(&b""[..])),
        ];
        for user_key in cases {
            let key_bytes = storekey::encode_vec(&StablePathEntryKey::UserState(user_key.clone()))
                .expect("encode");
            assert!(
                key_bytes.starts_with(&prefix_bytes),
                "UserState({user_key:?}) bytes don't start with UserStatePrefix bytes"
            );
        }
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
            StablePathEntryKey::UserState(user_key.clone()),
        );
        let bytes = entry.encode().expect("encode");
        let decoded = DbEntryKey::decode(&bytes).expect("decode");

        match decoded {
            DbEntryKey::StablePath(p, StablePathEntryKey::UserState(k)) => {
                assert_eq!(p, path);
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

        let prefix_bytes =
            DbEntryKey::StablePath(path.clone(), StablePathEntryKey::UserStatePrefix)
                .encode()
                .expect("encode");
        let state_bytes = DbEntryKey::StablePath(
            path.clone(),
            StablePathEntryKey::UserState(StableKey::Str(Arc::from("counter"))),
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

    /// Prefix for path A must not match entries under path B.
    /// Guards the scoping guarantee: `list_user_states(path_a)` never
    /// returns entries that belong to path_b.
    #[test]
    fn user_state_prefix_does_not_cross_paths() {
        let path_a = StablePath(Arc::from(vec![StableKey::Str(Arc::from("file_a.md"))]));
        let path_b = StablePath(Arc::from(vec![StableKey::Str(Arc::from("file_b.md"))]));

        let prefix_a = DbEntryKey::StablePath(path_a.clone(), StablePathEntryKey::UserStatePrefix)
            .encode()
            .expect("encode");
        let state_b = DbEntryKey::StablePath(
            path_b,
            StablePathEntryKey::UserState(StableKey::Str(Arc::from("x"))),
        )
        .encode()
        .expect("encode");

        assert!(
            !state_b.starts_with(&prefix_a),
            "path_b UserState key incorrectly starts with path_a's prefix"
        );
    }
}

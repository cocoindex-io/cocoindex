use crate::{
    prelude::*,
    state::stable_path::{StablePathPrefix, StablePathRef},
};

use std::{borrow::Cow, collections::BTreeMap, io::Write};

use cocoindex_utils::fingerprint::Fingerprint;
use serde::{Deserialize, Serialize};
use serde_with::{Bytes, serde_as};

use crate::state::{
    stable_path::{StableKey, StablePath},
    target_state_path::TargetStatePath,
};

pub type Database = heed::Database<heed::types::Bytes, heed::types::Bytes>;

#[derive(Debug)]
pub enum StablePathEntryKey {
    /// Value type: ComponentMemoizationInfo
    ComponentMemoization,

    FunctionMemoizationPrefix,
    /// Value type: FunctionMemoizationEntry
    FunctionMemoization(Fingerprint),

    /// Required.
    /// Value type: StablePathEntryEffectInfo
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
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FunctionMemoizationEntry<'a> {
    /// Memoization info is stored in the component metadata
    #[serde(rename = "R", borrow)]
    pub return_value: MemoizedValue<'a>,

    /// Relative paths to the parent components.
    #[serde(rename = "C")]
    pub child_components: Vec<StablePath>,
    /// Target states that are declared by the function.
    #[serde(rename = "E")]
    pub target_state_paths: Vec<TargetStatePath>,
    /// Dependency entries that are declared by the function.
    /// Only needs to keep dependencies with side effects other than return value (child components / target states / dependency entries with side effects).
    #[serde(rename = "D")]
    pub dependency_memo_entries: Vec<Fingerprint>,
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug)]
pub enum EffectInfoItemState<'a> {
    #[serde(rename = "D")]
    Deleted,
    #[serde(untagged)]
    Existing(
        #[serde_as(as = "Bytes")]
        #[serde(borrow)]
        Cow<'a, [u8]>,
    ),
}

impl<'a> EffectInfoItemState<'a> {
    pub fn is_deleted(&self) -> bool {
        matches!(self, EffectInfoItemState::Deleted)
    }

    pub fn as_ref(&self) -> Option<&[u8]> {
        match self {
            EffectInfoItemState::Deleted => None,
            EffectInfoItemState::Existing(s) => Some(s.as_ref()),
        }
    }
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug)]
pub struct EffectInfoItem<'a> {
    #[serde_as(as = "Bytes")]
    #[serde(rename = "P", borrow)]
    pub key: Cow<'a, [u8]>,
    #[serde(rename = "S", borrow)]
    pub states: Vec<(/*version*/ u64, EffectInfoItemState<'a>)>,
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
    pub effect_items: BTreeMap<TargetStatePath, EffectInfoItem<'a>>,
    #[serde(rename = "N", borrow, default = "unknown_processor_name")]
    pub processor_name: Cow<'a, str>,
}

impl<'a> StablePathEntryTrackingInfo<'a> {
    pub fn new(processor_name: Cow<'a, str>) -> Self {
        Self {
            version: 0,
            effect_items: BTreeMap::new(),
            processor_name,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Copy)]
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

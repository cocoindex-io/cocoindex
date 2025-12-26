use crate::{
    prelude::*,
    state::stable_path::{StablePathPrefix, StablePathRef},
};

use std::{borrow::Cow, collections::BTreeMap, io::Write};

use serde::{Deserialize, Serialize};

use crate::state::{
    effect_path::EffectPath,
    stable_path::{StableKey, StablePath},
};

pub type Database = heed::Database<heed::types::Bytes, heed::types::Bytes>;

#[derive(Debug)]
pub enum StablePathEntryKey {
    Metadata,

    ChildExistencePrefix,
    ChildExistence(StableKey),

    Effects,

    ChildComponentTombstonePrefix,
    ChildComponentTombstone(StablePath),
}

impl storekey::Encode for StablePathEntryKey {
    fn encode<W: Write>(&self, e: &mut storekey::Writer<W>) -> Result<(), storekey::EncodeError> {
        match self {
            // Should not be less than 2.
            StablePathEntryKey::Metadata => e.write_u8(0x10),
            StablePathEntryKey::ChildExistencePrefix => e.write_u8(0x20),
            StablePathEntryKey::ChildExistence(key) => {
                e.write_u8(0x20)?;
                key.encode(e)
            }
            StablePathEntryKey::Effects => e.write_u8(0x30),
            StablePathEntryKey::ChildComponentTombstonePrefix => e.write_u8(0xa0),
            StablePathEntryKey::ChildComponentTombstone(path) => {
                e.write_u8(0xa0)?;
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
            0x10 => StablePathEntryKey::Metadata,
            0x20 => {
                let key: StableKey = storekey::Decode::decode(d)?;
                StablePathEntryKey::ChildExistence(key)
            }
            0x30 => StablePathEntryKey::Effects,
            0xa0 => {
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
    Effect(EffectPath),
}

impl<'a> storekey::Encode for DbEntryKey<'a> {
    fn encode<W: Write>(&self, e: &mut storekey::Writer<W>) -> Result<(), storekey::EncodeError> {
        match self {
            // Should not be less than 2.
            DbEntryKey::StablePathPrefixPrefix(prefix) => {
                e.write_u8(0x10)?;
                prefix.encode(e)?;
            }
            DbEntryKey::StablePathPrefix(prefix) => {
                e.write_u8(0x10)?;
                prefix.encode(e)?;
            }
            DbEntryKey::StablePath(path, key) => {
                e.write_u8(0x10)?;
                path.encode(e)?;
                key.encode(e)?;
            }

            DbEntryKey::Effect(path) => {
                e.write_u8(0x20)?;
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
                let path: EffectPath = storekey::Decode::decode(d)?;
                DbEntryKey::Effect(path)
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

#[derive(Serialize, Deserialize, Debug)]
pub struct EffectInfoItem<'a> {
    #[serde(rename = "P")]
    pub key: Cow<'a, [u8]>,
    #[serde(rename = "S", borrow)]
    pub states: Vec<(/*version*/ u64, Option<Cow<'a, [u8]>>)>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct StablePathEntryEffectInfo<'a> {
    #[serde(rename = "V")]
    pub version: u64,
    #[serde(rename = "I", borrow)]
    pub items: BTreeMap<EffectPath, EffectInfoItem<'a>>,
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

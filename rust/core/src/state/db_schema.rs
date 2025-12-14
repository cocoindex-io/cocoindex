use crate::prelude::*;

use std::{borrow::Cow, collections::BTreeMap, io::Write};

use serde::{Deserialize, Serialize};

use crate::state::{
    effect_path::EffectPath,
    state_path::{StateKey, StatePath},
};

pub type Database = heed::Database<heed::types::Bytes, heed::types::Bytes>;

#[derive(Debug)]
pub enum StateEntryKey {
    Metadata,

    ChildExistencePrefix,
    ChildExistence(StateKey),

    Effects,

    ChildComponentTombstonePrefix,
    ChildComponentTombstone(StatePath),
}

impl storekey::Encode for StateEntryKey {
    fn encode<W: Write>(&self, e: &mut storekey::Writer<W>) -> Result<(), storekey::EncodeError> {
        match self {
            // Should not be less than 2.
            StateEntryKey::Metadata => e.write_u8(0x10),
            StateEntryKey::ChildExistencePrefix => e.write_u8(0x20),
            StateEntryKey::ChildExistence(key) => {
                e.write_u8(0x20)?;
                key.encode(e)
            }
            StateEntryKey::Effects => e.write_u8(0x30),
            StateEntryKey::ChildComponentTombstonePrefix => e.write_u8(0xa0),
            StateEntryKey::ChildComponentTombstone(path) => {
                e.write_u8(0xa0)?;
                path.encode(e)
            }
        }
    }
}

impl storekey::Decode for StateEntryKey {
    fn decode<D: std::io::BufRead>(
        d: &mut storekey::Reader<D>,
    ) -> Result<Self, storekey::DecodeError> {
        let key = match d.read_u8()? {
            0x10 => StateEntryKey::Metadata,
            0x20 => {
                let key: StateKey = storekey::Decode::decode(d)?;
                StateEntryKey::ChildExistence(key)
            }
            0x30 => StateEntryKey::Effects,
            0xa0 => {
                let path: StatePath = storekey::Decode::decode(d)?;
                StateEntryKey::ChildComponentTombstone(path)
            }
            _ => return Err(storekey::DecodeError::InvalidFormat),
        };
        Ok(key)
    }
}

#[derive(Debug)]
pub enum DbEntryKey {
    State(StatePath, StateEntryKey),
    Effect(EffectPath),
}

impl storekey::Encode for DbEntryKey {
    fn encode<W: Write>(&self, e: &mut storekey::Writer<W>) -> Result<(), storekey::EncodeError> {
        match self {
            // Should not be less than 2.
            DbEntryKey::State(path, key) => {
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

impl storekey::Decode for DbEntryKey {
    fn decode<D: std::io::BufRead>(
        d: &mut storekey::Reader<D>,
    ) -> Result<Self, storekey::DecodeError> {
        let key = match d.read_u8()? {
            0x10 => {
                let path: StatePath = storekey::Decode::decode(d)?;
                let key: StateEntryKey = storekey::Decode::decode(d)?;
                DbEntryKey::State(path, key)
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

impl DbEntryKey {
    pub fn encode(&self) -> Result<Vec<u8>> {
        storekey::encode_vec(self)
            .map_err(|e| anyhow::anyhow!("Failed to encode DbEntryKey: {}", e))
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
pub struct StateEntryEffectInfo<'a> {
    #[serde(rename = "V")]
    pub version: u64,
    #[serde(rename = "I", borrow)]
    pub items: BTreeMap<EffectPath, EffectInfoItem<'a>>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub enum StatePathNodeType {
    #[serde(rename = "D")]
    Directory,
    #[serde(rename = "C")]
    Component,
}

#[derive(Serialize, Deserialize)]
pub struct ChildExistenceInfo {
    #[serde(rename = "T")]
    pub node_type: StatePathNodeType,
    // TODO: Add a generation, to avoid race conditions during deletion,
    // e.g. when the parent is cleaning up the child asynchronously, there's
    // incremental reinsertion (based on change stream) for the child, which
    // makes another generation of the child appear again.
}

use std::{borrow::Cow, collections::BTreeMap, io::Write};

use serde::{Deserialize, Serialize};

use crate::state::{effect_path::EffectPath, state_path::StatePath};

pub enum StateEntryKey {
    Metadata,
    Effects,
}

impl storekey::Encode for StateEntryKey {
    fn encode<W: Write>(&self, e: &mut storekey::Writer<W>) -> Result<(), storekey::EncodeError> {
        match self {
            StateEntryKey::Metadata => e.write_u8(2),
            StateEntryKey::Effects => e.write_u8(3),
        }
    }
}

impl storekey::Decode for StateEntryKey {
    fn decode<D: std::io::BufRead>(
        d: &mut storekey::Reader<D>,
    ) -> Result<Self, storekey::DecodeError> {
        match d.read_u8()? {
            2 => Ok(StateEntryKey::Metadata),
            3 => Ok(StateEntryKey::Effects),
            _ => Err(storekey::DecodeError::InvalidFormat),
        }
    }
}

pub enum DbEntryKey {
    State(StatePath, StateEntryKey),
    Effect(EffectPath),
}

impl storekey::Encode for DbEntryKey {
    fn encode<W: Write>(&self, e: &mut storekey::Writer<W>) -> Result<(), storekey::EncodeError> {
        match self {
            DbEntryKey::State(path, key) => {
                e.write_u8(2)?;
                path.encode(e)?;
                key.encode(e)?;
            }
            DbEntryKey::Effect(path) => {
                e.write_u8(3)?;
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
        match d.read_u8()? {
            2 => {
                let path: StatePath = storekey::Decode::decode(d)?;
                let key: StateEntryKey = storekey::Decode::decode(d)?;
                Ok(DbEntryKey::State(path, key))
            }
            3 => {
                let path: EffectPath = storekey::Decode::decode(d)?;
                Ok(DbEntryKey::Effect(path))
            }
            _ => Err(storekey::DecodeError::InvalidFormat),
        }
    }
}

impl<'a> heed::BytesEncode<'a> for DbEntryKey {
    type EItem = Self;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, heed::BoxedError> {
        let buf = storekey::encode_vec(item)?;
        Ok(Cow::Owned(buf))
    }
}

impl<'a> heed::BytesDecode<'a> for DbEntryKey {
    type DItem = Self;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, heed::BoxedError> {
        let cursor = std::io::Cursor::new(bytes);
        let key: DbEntryKey = storekey::decode(cursor)?;
        Ok(key)
    }
}

#[derive(Serialize, Deserialize)]
pub struct EffectInfoItem<'a> {
    #[serde(rename = "P")]
    pub key: Cow<'a, [u8]>,
    #[serde(rename = "S", borrow)]
    pub states: Vec<(/*version*/ u64, Option<Cow<'a, [u8]>>)>,
}

#[derive(Serialize, Deserialize, Default)]
pub struct StateEntryEffectInfo<'a> {
    #[serde(rename = "V")]
    pub version: u64,
    #[serde(rename = "I", borrow)]
    pub items: BTreeMap<EffectPath, EffectInfoItem<'a>>,
}

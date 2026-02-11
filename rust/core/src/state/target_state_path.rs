use crate::prelude::*;
use serde::{Deserialize, Serialize};

use crate::state::stable_path::StableKey;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TargetStatePath(pub Arc<[StableKey]>);

impl std::borrow::Borrow<[StableKey]> for TargetStatePath {
    fn borrow(&self) -> &[StableKey] {
        &self.0
    }
}

impl std::fmt::Display for TargetStatePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for part in self.0.iter() {
            write!(f, "/{part}")?;
        }
        Ok(())
    }
}

impl storekey::Encode for TargetStatePath {
    fn encode<W: std::io::Write>(
        &self,
        e: &mut storekey::Writer<W>,
    ) -> Result<(), storekey::EncodeError> {
        self.0.as_ref().encode(e)
    }
}

impl storekey::Decode for TargetStatePath {
    fn decode<D: std::io::BufRead>(
        d: &mut storekey::Reader<D>,
    ) -> Result<Self, storekey::DecodeError> {
        let parts: Vec<StableKey> = storekey::Decode::decode(d)?;
        Ok(Self(Arc::from(parts)))
    }
}

impl TargetStatePath {
    pub fn new(key_part: StableKey, parent: Option<&Self>) -> Self {
        let inner: Arc<[StableKey]> = match parent {
            Some(parent) => parent
                .0
                .iter()
                .chain(std::iter::once(&key_part))
                .cloned()
                .collect(),
            None => Arc::new([key_part]),
        };
        Self(inner)
    }

    pub fn concat(&self, part: StableKey) -> Self {
        Self(
            self.0
                .iter()
                .chain(std::iter::once(&part))
                .cloned()
                .collect(),
        )
    }

    pub fn provider_path(&self) -> &[StableKey] {
        &self.0[..self.0.len() - 1]
    }

    pub fn as_slice(&self) -> &[StableKey] {
        &self.0
    }
}

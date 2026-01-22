use crate::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TargetStatePath(Arc<[utils::fingerprint::Fingerprint]>);

impl std::borrow::Borrow<[utils::fingerprint::Fingerprint]> for TargetStatePath {
    fn borrow(&self) -> &[utils::fingerprint::Fingerprint] {
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
        self.0.encode(e)
    }
}

impl storekey::Decode for TargetStatePath {
    fn decode<D: std::io::BufRead>(
        d: &mut storekey::Reader<D>,
    ) -> Result<Self, storekey::DecodeError> {
        let parts: Vec<utils::fingerprint::Fingerprint> = storekey::Decode::decode(d)?;
        Ok(Self(Arc::from(parts)))
    }
}

impl TargetStatePath {
    pub fn new(key_part: utils::fingerprint::Fingerprint, parent: Option<&Self>) -> Self {
        let inner: Arc<[utils::fingerprint::Fingerprint]> = match parent {
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

    pub fn concat(&self, part: utils::fingerprint::Fingerprint) -> Self {
        Self(
            self.0
                .iter()
                .chain(std::iter::once(&part))
                .cloned()
                .collect(),
        )
    }

    pub fn provider_path(&self) -> &[utils::fingerprint::Fingerprint] {
        &self.0[..self.0.len() - 1]
    }

    pub fn as_slice(&self) -> &[utils::fingerprint::Fingerprint] {
        &self.0
    }
}

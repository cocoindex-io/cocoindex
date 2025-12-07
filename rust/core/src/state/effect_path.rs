use crate::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EffectPath {
    inner: Arc<[utils::fingerprint::Fingerprint]>,
}

impl std::borrow::Borrow<[utils::fingerprint::Fingerprint]> for EffectPath {
    fn borrow(&self) -> &[utils::fingerprint::Fingerprint] {
        &self.inner
    }
}

impl std::fmt::Display for EffectPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for part in self.inner.iter() {
            write!(f, "/#{:x?}", part.as_slice())?;
        }
        Ok(())
    }
}

impl storekey::Encode for EffectPath {
    fn encode<W: std::io::Write>(
        &self,
        e: &mut storekey::Writer<W>,
    ) -> Result<(), storekey::EncodeError> {
        self.inner.encode(e)
    }
}

impl storekey::Decode for EffectPath {
    fn decode<D: std::io::BufRead>(
        d: &mut storekey::Reader<D>,
    ) -> Result<Self, storekey::DecodeError> {
        let parts: Vec<utils::fingerprint::Fingerprint> = storekey::Decode::decode(d)?;
        Ok(Self {
            inner: Arc::from(parts),
        })
    }
}

impl EffectPath {
    pub fn new(key_part: utils::fingerprint::Fingerprint, parent: Option<&Self>) -> Self {
        let inner: Arc<[utils::fingerprint::Fingerprint]> = match parent {
            Some(parent) => parent
                .inner
                .iter()
                .chain(std::iter::once(&key_part))
                .cloned()
                .collect(),
            None => Arc::new([key_part]),
        };
        Self { inner }
    }

    pub fn concat(&self, part: utils::fingerprint::Fingerprint) -> Self {
        Self {
            inner: self
                .inner
                .iter()
                .chain(std::iter::once(&part))
                .cloned()
                .collect(),
        }
    }

    pub fn provider_path(&self) -> &[utils::fingerprint::Fingerprint] {
        &self.inner[..self.inner.len() - 1]
    }

    pub fn as_slice(&self) -> &[utils::fingerprint::Fingerprint] {
        &self.inner
    }
}

use serde::{Deserialize, Serialize};

use base64::prelude::*;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MemoizationKey(Vec<u8>);

impl Serialize for MemoizationKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&BASE64_STANDARD.encode(&self.0))
    }
}

impl<'de> Deserialize<'de> for MemoizationKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let bytes = BASE64_STANDARD
            .decode(s)
            .map_err(serde::de::Error::custom)?;
        Ok(MemoizationKey(bytes))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoizationInfo {
    pub cache: HashMap<MemoizationKey, serde_json::Value>,
}

impl Default for MemoizationInfo {
    fn default() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }
}

use anyhow::Result;
use serde::{Deserialize, Serialize};

use base64::prelude::*;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::base::{schema, value};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey(Vec<u8>);

impl Serialize for CacheKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&BASE64_STANDARD.encode(&self.0))
    }
}

impl<'de> Deserialize<'de> for CacheKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let bytes = BASE64_STANDARD
            .decode(s)
            .map_err(serde::de::Error::custom)?;
        Ok(CacheKey(bytes))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoizationInfo {
    pub cache: HashMap<CacheKey, serde_json::Value>,
}

impl Default for MemoizationInfo {
    fn default() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }
}

enum EvaluationCacheEntry {
    /// Existing entry in previous runs, but not in current run yet.
    Previous(serde_json::Value),
    /// Value appeared in current run.
    Current(Arc<async_lock::OnceCell<value::Value>>),
}

#[derive(Default)]
pub struct EvaluationCache {
    cache: Mutex<HashMap<CacheKey, EvaluationCacheEntry>>,
}

impl EvaluationCache {
    pub fn from_stored(cache: HashMap<CacheKey, serde_json::Value>) -> Self {
        Self {
            cache: Mutex::new(
                cache
                    .into_iter()
                    .map(|(k, v)| (k, EvaluationCacheEntry::Previous(v)))
                    .collect(),
            ),
        }
    }

    pub fn into_stored(self) -> Result<HashMap<CacheKey, serde_json::Value>> {
        Ok(self
            .cache
            .into_inner()?
            .into_iter()
            .filter_map(|(k, v)| match v {
                EvaluationCacheEntry::Previous(_) => None,
                EvaluationCacheEntry::Current(entry) => {
                    entry.get().map(|v| Ok((k, serde_json::to_value(v)?)))
                }
            })
            .collect::<Result<_>>()?)
    }

    pub fn get(
        &self,
        key: CacheKey,
        typ: &schema::ValueType,
    ) -> Result<Arc<async_lock::OnceCell<value::Value>>> {
        let mut cache = self.cache.lock().unwrap();
        let result = match cache.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut entry) => match &mut entry.get_mut() {
                EvaluationCacheEntry::Previous(value) => {
                    let value = value::Value::from_json(std::mem::take(value), typ)?;
                    let cell = Arc::new(async_lock::OnceCell::from(value));
                    entry.insert(EvaluationCacheEntry::Current(cell.clone()));
                    cell
                }
                EvaluationCacheEntry::Current(cell) => cell.clone(),
            },
            std::collections::hash_map::Entry::Vacant(entry) => {
                let cell = Arc::new(async_lock::OnceCell::new());
                entry.insert(EvaluationCacheEntry::Current(cell.clone()));
                cell
            }
        };
        Ok(result)
    }
}

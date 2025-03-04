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
pub struct CacheEntry {
    time_sec: i64,
    value: serde_json::Value,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoizationInfo {
    pub cache: HashMap<CacheKey, CacheEntry>,
}

impl Default for MemoizationInfo {
    fn default() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }
}

struct EvaluationCacheEntry {
    time: chrono::DateTime<chrono::Utc>,
    data: EvaluationCacheData,
}

enum EvaluationCacheData {
    /// Existing entry in previous runs, but not in current run yet.
    Previous(serde_json::Value),
    /// Value appeared in current run.
    Current(Arc<async_lock::OnceCell<value::Value>>),
}

pub struct EvaluationCache {
    current_time: chrono::DateTime<chrono::Utc>,
    cache: Mutex<HashMap<CacheKey, EvaluationCacheEntry>>,
}

impl EvaluationCache {
    pub fn new(
        current_time: chrono::DateTime<chrono::Utc>,
        existing_cache: Option<HashMap<CacheKey, CacheEntry>>,
    ) -> Self {
        Self {
            current_time,
            cache: Mutex::new(
                existing_cache
                    .into_iter()
                    .map(|e| e.into_iter())
                    .flatten()
                    .map(|(k, e)| {
                        (
                            k,
                            EvaluationCacheEntry {
                                time: chrono::DateTime::from_timestamp(e.time_sec, 0)
                                    .unwrap_or(chrono::DateTime::<chrono::Utc>::MIN_UTC),
                                data: EvaluationCacheData::Previous(e.value),
                            },
                        )
                    })
                    .collect(),
            ),
        }
    }

    pub fn into_stored(self) -> Result<HashMap<CacheKey, CacheEntry>> {
        Ok(self
            .cache
            .into_inner()?
            .into_iter()
            .filter_map(|(k, e)| match e.data {
                EvaluationCacheData::Previous(_) => None,
                EvaluationCacheData::Current(entry) => entry.get().map(|v| {
                    Ok((
                        k,
                        CacheEntry {
                            time_sec: e.time.timestamp(),
                            value: serde_json::to_value(v)?,
                        },
                    ))
                }),
            })
            .collect::<Result<_>>()?)
    }

    pub fn get(
        &self,
        key: CacheKey,
        typ: &schema::ValueType,
        ttl: Option<chrono::Duration>,
    ) -> Result<Arc<async_lock::OnceCell<value::Value>>> {
        let mut cache = self.cache.lock().unwrap();
        let result = {
            match cache.entry(key) {
                std::collections::hash_map::Entry::Occupied(mut entry)
                    if !ttl
                        .map(|ttl| entry.get().time + ttl < self.current_time)
                        .unwrap_or(false) =>
                {
                    let entry_mut = &mut entry.get_mut();
                    match &mut entry_mut.data {
                        EvaluationCacheData::Previous(value) => {
                            let value = value::Value::from_json(std::mem::take(value), typ)?;
                            let cell = Arc::new(async_lock::OnceCell::from(value));
                            let time = entry_mut.time;
                            entry.insert(EvaluationCacheEntry {
                                time,
                                data: EvaluationCacheData::Current(cell.clone()),
                            });
                            cell
                        }
                        EvaluationCacheData::Current(cell) => cell.clone(),
                    }
                }
                entry => {
                    let cell = Arc::new(async_lock::OnceCell::new());
                    entry.insert_entry(EvaluationCacheEntry {
                        time: self.current_time,
                        data: EvaluationCacheData::Current(cell.clone()),
                    });
                    cell
                }
            }
        };
        Ok(result)
    }
}

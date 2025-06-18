use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex},
};

use crate::{
    base::{schema, value},
    service::error::{SharedError, SharedResultExtRef},
    utils::fingerprint::{Fingerprint, Fingerprinter},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredCacheEntry {
    time_sec: i64,
    value: serde_json::Value,
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StoredMemoizationInfo {
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub cache: HashMap<Fingerprint, StoredCacheEntry>,

    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub uuids: HashMap<Fingerprint, Vec<uuid::Uuid>>,
}

pub type CacheEntryCell = Arc<tokio::sync::OnceCell<Result<value::Value, SharedError>>>;
enum CacheData {
    /// Existing entry in previous runs, but not in current run yet.
    Previous(serde_json::Value),
    /// Value appeared in current run.
    Current(CacheEntryCell),
}

struct CacheEntry {
    time: chrono::DateTime<chrono::Utc>,
    data: CacheData,
}

#[derive(Default)]
struct UuidEntry {
    uuids: Vec<uuid::Uuid>,
    num_current: usize,
}

impl UuidEntry {
    fn new(uuids: Vec<uuid::Uuid>) -> Self {
        Self {
            uuids,
            num_current: 0,
        }
    }

    fn into_stored(self) -> Option<Vec<uuid::Uuid>> {
        if self.num_current == 0 {
            return None;
        }
        let mut uuids = self.uuids;
        if self.num_current < uuids.len() {
            uuids.truncate(self.num_current);
        }
        Some(uuids)
    }
}

pub struct EvaluationMemoryOptions {
    pub enable_cache: bool,

    /// If true, it's for evaluation only.
    /// In this mode, we don't memoize anything.
    pub evaluation_only: bool,
}

pub struct EvaluationMemory {
    current_time: chrono::DateTime<chrono::Utc>,
    cache: Option<Mutex<HashMap<Fingerprint, CacheEntry>>>,
    uuids: Mutex<HashMap<Fingerprint, UuidEntry>>,
    evaluation_only: bool,
}

impl EvaluationMemory {
    pub fn new(
        current_time: chrono::DateTime<chrono::Utc>,
        stored_info: Option<StoredMemoizationInfo>,
        options: EvaluationMemoryOptions,
    ) -> Self {
        let (stored_cache, stored_uuids) = stored_info
            .map(|stored_info| (stored_info.cache, stored_info.uuids))
            .unzip();
        Self {
            current_time,
            cache: options.enable_cache.then(|| {
                Mutex::new(
                    stored_cache
                        .into_iter()
                        .flat_map(|iter| iter.into_iter())
                        .map(|(k, e)| {
                            (
                                k,
                                CacheEntry {
                                    time: chrono::DateTime::from_timestamp(e.time_sec, 0)
                                        .unwrap_or(chrono::DateTime::<chrono::Utc>::MIN_UTC),
                                    data: CacheData::Previous(e.value),
                                },
                            )
                        })
                        .collect(),
                )
            }),
            uuids: Mutex::new(
                (!options.evaluation_only)
                    .then_some(stored_uuids)
                    .flatten()
                    .into_iter()
                    .flat_map(|iter| iter.into_iter())
                    .map(|(k, v)| (k, UuidEntry::new(v)))
                    .collect(),
            ),
            evaluation_only: options.evaluation_only,
        }
    }

    pub fn into_stored(self) -> Result<StoredMemoizationInfo> {
        if self.evaluation_only {
            bail!("For evaluation only, cannot convert to stored MemoizationInfo");
        }
        let cache = if let Some(cache) = self.cache {
            cache
                .into_inner()?
                .into_iter()
                .filter_map(|(k, e)| match e.data {
                    CacheData::Previous(_) => None,
                    CacheData::Current(entry) => match entry.get() {
                        Some(Ok(v)) => Some(serde_json::to_value(v).map(|value| {
                            (
                                k,
                                StoredCacheEntry {
                                    time_sec: e.time.timestamp(),
                                    value,
                                },
                            )
                        })),
                        _ => None,
                    },
                })
                .collect::<Result<_, _>>()?
        } else {
            bail!("Cache is disabled, cannot convert to stored MemoizationInfo");
        };
        let uuids = self
            .uuids
            .into_inner()?
            .into_iter()
            .filter_map(|(k, v)| v.into_stored().map(|uuids| (k, uuids)))
            .collect();
        Ok(StoredMemoizationInfo { cache, uuids })
    }

    pub fn get_cache_entry(
        &self,
        key: impl FnOnce() -> Result<Fingerprint>,
        typ: &schema::ValueType,
        ttl: Option<chrono::Duration>,
    ) -> Result<Option<CacheEntryCell>> {
        let mut cache = if let Some(cache) = &self.cache {
            cache.lock().unwrap()
        } else {
            return Ok(None);
        };
        let result = match cache.entry(key()?) {
            std::collections::hash_map::Entry::Occupied(mut entry)
                if !ttl
                    .map(|ttl| entry.get().time + ttl < self.current_time)
                    .unwrap_or(false) =>
            {
                let entry_mut = &mut entry.get_mut();
                match &mut entry_mut.data {
                    CacheData::Previous(value) => {
                        let value = value::Value::from_json(std::mem::take(value), typ)?;
                        let cell = Arc::new(tokio::sync::OnceCell::from(Ok(value)));
                        let time = entry_mut.time;
                        entry.insert(CacheEntry {
                            time,
                            data: CacheData::Current(cell.clone()),
                        });
                        cell
                    }
                    CacheData::Current(cell) => cell.clone(),
                }
            }
            entry => {
                let cell = Arc::new(tokio::sync::OnceCell::new());
                entry.insert_entry(CacheEntry {
                    time: self.current_time,
                    data: CacheData::Current(cell.clone()),
                });
                cell
            }
        };
        Ok(Some(result))
    }

    pub fn next_uuid(&self, key: Fingerprint) -> Result<uuid::Uuid> {
        let mut uuids = self.uuids.lock().unwrap();

        let entry = uuids.entry(key).or_default();
        let uuid = if self.evaluation_only {
            let fp = Fingerprinter::default()
                .with(&key)?
                .with(&entry.num_current)?
                .into_fingerprint();
            uuid::Uuid::new_v8(fp.0)
        } else if entry.num_current < entry.uuids.len() {
            entry.uuids[entry.num_current]
        } else {
            let uuid = uuid::Uuid::new_v4();
            entry.uuids.push(uuid);
            uuid
        };
        entry.num_current += 1;
        Ok(uuid)
    }
}

pub async fn evaluate_with_cell<Fut>(
    cell: Option<&CacheEntryCell>,
    compute: impl FnOnce() -> Fut,
) -> Result<Cow<'_, value::Value>>
where
    Fut: Future<Output = Result<value::Value>>,
{
    let result = match cell {
        Some(cell) => Cow::Borrowed(
            cell.get_or_init(|| {
                let fut = compute();
                async move { fut.await.map_err(SharedError::new) }
            })
            .await
            .std_result()?,
        ),
        None => Cow::Owned(compute().await?),
    };
    Ok(result)
}

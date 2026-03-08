//! ID sequencer with exponential batching for efficient ID generation.
//!
//! This module provides stable ID generation with the following properties:
//! - IDs are unique within an app for a given key
//! - IDs start from 1 (0 is reserved)
//! - IDs are allocated in batches to minimize database transactions
//! - Batch sizes grow exponentially (2, 4, 8, ..., 256) for better performance

use std::collections::HashMap;

use crate::engine::txn_batcher::TxnBatcher;
use crate::prelude::*;
use crate::state::db_schema;
use crate::state::stable_path::StableKey;
use cocoindex_utils::deser::from_msgpack_slice;

/// Initial batch size for ID allocation.
const INITIAL_BATCH_SIZE: u64 = 2;

/// Maximum batch size for ID allocation.
const MAX_BATCH_SIZE: u64 = 256;

/// In-memory state for a single ID sequencer.
struct SequencerState {
    /// Next ID to return from the local buffer.
    next_local_id: u64,
    /// End of the local buffer (exclusive).
    buffer_end: u64,
    /// Batch size to use when refilling.
    next_batch_size: u64,
}

impl SequencerState {
    fn new() -> Self {
        Self {
            next_local_id: 0,
            buffer_end: 0,
            next_batch_size: INITIAL_BATCH_SIZE,
        }
    }

    fn needs_refill(&self) -> bool {
        self.next_local_id >= self.buffer_end
    }

    fn take_id(&mut self) -> u64 {
        let id = self.next_local_id;
        self.next_local_id += 1;
        id
    }

    fn refill(&mut self, start_id: u64, count: u64) {
        self.next_local_id = start_id;
        self.buffer_end = start_id + count;
        // Grow batch size exponentially, capped at MAX_BATCH_SIZE
        self.next_batch_size = (self.next_batch_size * 2).min(MAX_BATCH_SIZE);
    }
}

/// Manages ID sequencers for an app, providing batched ID allocation.
///
/// Uses a two-layer locking strategy:
/// - Main mutex protects the map of sequencers (held briefly)
/// - Per-key tokio mutex protects each sequencer's state (can be held across await points)
///
/// This allows concurrent ID generation for different keys while serializing
/// operations for the same key.
#[derive(Default)]
pub struct IdSequencerManager {
    sequencers: Mutex<HashMap<StableKey, Arc<tokio::sync::Mutex<SequencerState>>>>,
}

impl IdSequencerManager {
    pub fn new() -> Self {
        Self {
            sequencers: Mutex::new(HashMap::new()),
        }
    }

    /// Get the next ID for the given key, refilling from the database if needed.
    ///
    /// This function is thread-safe and handles concurrent access properly.
    /// Different keys can be processed in parallel, while same-key operations
    /// are serialized.
    pub async fn next_id(
        &self,
        txn_batcher: &TxnBatcher,
        db: &db_schema::Database,
        key: &StableKey,
    ) -> Result<u64> {
        // Get or create the per-key state (brief lock on main map)
        let state_arc = {
            let mut sequencers = self.sequencers.lock().unwrap();
            sequencers
                .entry(key.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(SequencerState::new())))
                .clone()
        };

        // Lock the per-key state (tokio mutex can be held across await)
        let mut state = state_arc.lock().await;

        if state.needs_refill() {
            let batch_size = state.next_batch_size;
            let db = db.clone();
            let key = key.clone();
            let start_id = txn_batcher
                .run(move |wtxn| Self::reserve_ids_in_txn(wtxn, &db, &key, batch_size))
                .await?;
            state.refill(start_id, batch_size);
        }

        Ok(state.take_id())
    }

    /// Reserve `count` consecutive IDs in the given transaction, returning the first ID.
    fn reserve_ids_in_txn(
        wtxn: &mut heed::RwTxn<'_>,
        db: &db_schema::Database,
        key: &StableKey,
        count: u64,
    ) -> Result<u64> {
        let db_key = db_schema::DbEntryKey::IdSequencer(key.clone()).encode()?;

        // Read current value (IDs start from 1, 0 is reserved)
        let current_next_id = if let Some(data) = db.get(wtxn, db_key.as_slice())? {
            let info: db_schema::IdSequencerInfo = from_msgpack_slice(&data)?;
            info.next_id
        } else {
            1
        };

        // Write updated value
        let info = db_schema::IdSequencerInfo {
            next_id: current_next_id + count,
        };
        let encoded = rmp_serde::to_vec_named(&info)?;
        db.put(wtxn, db_key.as_slice(), encoded.as_slice())?;

        Ok(current_next_id)
    }
}

/// Deferred ID allocation that reads via `RoTxn` and commits writes later.
///
/// This splits the read and write phases of ID allocation so that the read
/// (via `&RoTxn`) doesn't conflict with other immutable borrows of the transaction.
/// Writes are applied in [`commit()`](IdReservation::commit).
///
/// Each reservation is scoped to a single key. At most one reservation per key
/// should be live at a time, which is naturally enforced by LMDB's single-writer
/// constraint.
pub struct IdReservation {
    key: &'static StableKey,
    /// Next ID to hand out (initialized from DB on first `next_id` call).
    next_id_state: Option<u64>,
}

impl IdReservation {
    pub fn new(key: &'static StableKey) -> Self {
        Self {
            key,
            next_id_state: None,
        }
    }

    /// Allocate the next ID. Reads from DB on first call, then tracks locally.
    /// Only needs `&RoTxn` (no mutable borrow).
    pub fn next_id(&mut self, rtxn: &heed::RoTxn<'_>, db: &db_schema::Database) -> Result<u64> {
        let next_id = match &mut self.next_id_state {
            Some(n) => n,
            slot @ None => {
                let db_key = db_schema::DbEntryKey::IdSequencer(self.key.clone()).encode()?;
                let current = if let Some(data) = db.get(rtxn, db_key.as_slice())? {
                    let info: db_schema::IdSequencerInfo = from_msgpack_slice(&data)?;
                    info.next_id
                } else {
                    1
                };
                slot.insert(current)
            }
        };
        let id = *next_id;
        *next_id += 1;
        Ok(id)
    }

    /// Write the reserved ID range back to DB. Call once at end of transaction.
    pub fn commit(self, wtxn: &mut heed::RwTxn<'_>, db: &db_schema::Database) -> Result<()> {
        if let Some(next_id) = self.next_id_state {
            let db_key = db_schema::DbEntryKey::IdSequencer(self.key.clone()).encode()?;
            let info = db_schema::IdSequencerInfo { next_id };
            let encoded = rmp_serde::to_vec_named(&info)?;
            db.put(wtxn, db_key.as_slice(), encoded.as_slice())?;
        }
        Ok(())
    }
}

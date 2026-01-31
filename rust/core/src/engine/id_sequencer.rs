//! ID sequencer with exponential batching for efficient ID generation.
//!
//! This module provides stable ID generation with the following properties:
//! - IDs are unique within an app for a given key
//! - IDs start from 1 (0 is reserved)
//! - IDs are allocated in batches to minimize database transactions
//! - Batch sizes grow exponentially (2, 4, 8, ..., 256) for better performance

use std::collections::HashMap;

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
/// - Per-key mutex protects each sequencer's state (held during DB operations)
///
/// This allows concurrent ID generation for different keys while serializing
/// operations for the same key.
#[derive(Default)]
pub struct IdSequencerManager {
    sequencers: Mutex<HashMap<StableKey, Arc<Mutex<SequencerState>>>>,
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
    pub fn next_id(
        &self,
        db_env: &heed::Env,
        db: &db_schema::Database,
        key: &StableKey,
    ) -> Result<u64> {
        // Get or create the per-key state (brief lock on main map)
        let state_arc = {
            let mut sequencers = self.sequencers.lock().unwrap();
            sequencers
                .entry(key.clone())
                .or_insert_with(|| Arc::new(Mutex::new(SequencerState::new())))
                .clone()
        };

        // Now work with the per-key state (main map lock is released)
        let mut state = state_arc.lock().unwrap();

        if state.needs_refill() {
            let batch_size = state.next_batch_size;
            let start_id = Self::reserve_batch(db_env, db, key, batch_size)?;
            state.refill(start_id, batch_size);
        }

        Ok(state.take_id())
    }

    /// Reserve a batch of IDs from the database.
    fn reserve_batch(
        db_env: &heed::Env,
        db: &db_schema::Database,
        key: &StableKey,
        batch_size: u64,
    ) -> Result<u64> {
        let db_key = db_schema::DbEntryKey::IdSequencer(key.clone()).encode()?;

        let mut wtxn = db_env.write_txn()?;

        // Read current value (IDs start from 1, 0 is reserved)
        let current_next_id = if let Some(data) = db.get(&wtxn, db_key.as_slice())? {
            let info: db_schema::IdSequencerInfo = from_msgpack_slice(&data)?;
            info.next_id
        } else {
            1
        };

        // Write new value
        let new_next_id = current_next_id + batch_size;
        let info = db_schema::IdSequencerInfo {
            next_id: new_next_id,
        };
        let encoded = rmp_serde::to_vec_named(&info)?;
        db.put(&mut wtxn, db_key.as_slice(), encoded.as_slice())?;

        wtxn.commit()?;

        Ok(current_next_id)
    }
}

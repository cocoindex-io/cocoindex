//! ID sequencer with exponential batching for efficient ID generation.
//!
//! This module provides stable ID generation with the following properties:
//! - IDs are unique within an app for a given key
//! - IDs start from 1 (0 is reserved)
//! - IDs are allocated in batches to minimize database transactions
//! - Batch sizes grow exponentially (2, 4, 8, ..., 256) for better performance
//!
//! Storage I/O goes through methods on `AppStore` / `Storage`; this module
//! is the in-memory caching half and never touches the storage backend
//! directly.

use std::collections::HashMap;

use crate::prelude::*;
use crate::state::stable_path::StableKey;
use crate::state_store::{AppStore, Storage};

// Deferred ID allocation (the old `IdReservation` struct) is gone:
// the session-driven `submit()` reconcile body no longer carries a
// `WriteTxn`, so each fresh ID call goes straight to the standalone
// autocommit primitive `app_store.reserve_id_range_standalone`. Only
// the rare `ChildInvalidation::Destructive` branch issues these calls.

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
        storage: &Storage,
        app_store: &AppStore,
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
            let app_store = app_store.clone();
            let key = key.clone();
            // `reserve_id_range` is idempotent under retry: each attempt
            // reads the current counter, computes `start_id`, writes
            // `start_id + batch_size`.
            let start_id = storage
                .run_txn(move |wtxn| {
                    let app_store = app_store.clone();
                    let key = key.clone();
                    Box::pin(async move {
                        app_store
                            .reserve_id_range_in_txn(wtxn, &key, batch_size)
                            .await
                    })
                })
                .await?;
            state.refill(start_id, batch_size);
        }

        Ok(state.take_id())
    }
}

//! LMDB transaction wrappers and the shared transaction/resize coordinator.
//!
//! The coordinator is an [`EnvSlot`]: a lock around the LMDB env itself.
//! Every read or write LMDB transaction in this process holds a read guard on
//! it for its full lifetime, and reaches the env only through that guard.
//! The write guard is taken to call `unsafe Env::resize()` and to close the
//! env, guaranteeing no participating transaction is active.
//!
//! Both wrappers read through an optional [`Overlay`]: the writes of a run
//! that persists nothing (a preview), kept in memory and layered over the
//! LMDB snapshot, so such a run reads its own writes back exactly as a
//! persisting run would. See [`super::AppStore::discarding_writes`].

use crate::prelude::*;

use std::collections::BTreeMap;
use std::ops::Deref;

pub(crate) type Env = heed::Env<heed::WithoutTls>;

/// LMDB database handle. Keys and values are opaque bytes; logical
/// key/value schemas live in [`crate::state::db_schema`].
pub(crate) type Database = heed::Database<heed::types::Bytes, heed::types::Bytes>;

/// The LMDB env behind the transaction/resize coordinator. `None` once the
/// storage is closed.
pub(crate) type EnvSlot = tokio::sync::RwLock<Option<Env>>;

/// The env in a guarded [`EnvSlot`], or an error if the storage is closed.
pub(crate) fn opened_env(slot: &Option<Env>) -> Result<&Env> {
    slot.as_ref()
        .ok_or_else(|| client_error!("The CocoIndex environment is closed"))
}

/// Writes by encoded key: `Some` for a value put, `None` for a key deleted.
type Writes = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

/// Writes in key order, as a slice of a [`Writes`] map.
type WriteEntries = Vec<(Vec<u8>, Option<Vec<u8>>)>;

/// The entries of `writes` under `prefix`, in key order.
fn writes_with_prefix(writes: &Writes, prefix: &[u8]) -> WriteEntries {
    writes
        .range(prefix.to_vec()..)
        .take_while(|(key, _)| key.starts_with(prefix))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

/// The writes of one run that persists nothing, by encoded key: `Some` for a
/// value put, `None` for a key deleted. Shared by every transaction the run
/// opens, so the run reads its own writes back; dropped with the run, so
/// nothing reaches LMDB. One overlay belongs to one database: the handle that
/// owns it reads and writes a single [`Database`].
#[derive(Default)]
pub(crate) struct Overlay {
    entries: parking_lot::Mutex<Writes>,
    /// Held for each body the run executes, so the run's bodies take turns
    /// as a batch's do: a body reads the overlay as the bodies before it left
    /// it, and no two bodies' read-modify-writes interleave.
    body_turn: tokio::sync::Mutex<()>,
}

impl Overlay {
    /// Waits for the run's turn to execute a body; the turn lasts as long as
    /// the returned guard.
    pub(crate) async fn body_turn(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.body_turn.lock().await
    }

    /// The overlay's entries under `prefix`, in key order.
    fn entries_with_prefix(&self, prefix: &[u8]) -> WriteEntries {
        writes_with_prefix(&self.entries.lock(), prefix)
    }

    /// Takes a finished body's writes into the run.
    fn absorb(&self, writes: Writes) {
        self.entries.lock().extend(writes);
    }
}

/// `key`'s value as the overlay (if any) over the LMDB `snapshot` has it.
fn get_through_overlay<'txn>(
    snapshot: &'txn heed::RoTxn<'_, heed::WithoutTls>,
    overlay: Option<&Overlay>,
    db: Database,
    key: &[u8],
) -> Result<Option<Cow<'txn, [u8]>>> {
    if let Some(overlay) = overlay
        && let Some(value) = overlay.entries.lock().get(key)
    {
        return Ok(value.clone().map(Cow::Owned));
    }
    Ok(db.get(snapshot, key)?.map(Cow::Borrowed))
}

/// Calls `f` with every `(key, value)` under `prefix`, in key order, as the
/// LMDB `snapshot` has them with `overlaid` (writes under `prefix`, in key
/// order, if any) layered over it.
fn for_each_prefix_through_overlay(
    snapshot: &heed::RoTxn<'_, heed::WithoutTls>,
    overlaid: Option<impl IntoIterator<Item = (Vec<u8>, Option<Vec<u8>>)>>,
    db: Database,
    prefix: &[u8],
    mut f: impl FnMut(&[u8], &[u8]) -> Result<()>,
) -> Result<()> {
    let mut stored = db.prefix_iter(snapshot, prefix)?;
    let Some(overlaid) = overlaid else {
        for entry in stored {
            let (key, value) = entry?;
            f(key, value)?;
        }
        return Ok(());
    };
    // Merge the two sorted sequences; on a key both have, the overlay wins
    // (a `None` there hides the stored entry).
    let mut overlaid = overlaid.into_iter().peekable();
    let mut next_stored = stored.next().transpose()?;
    loop {
        let overlay_first = match (&next_stored, overlaid.peek()) {
            (None, None) => return Ok(()),
            (Some(_), None) => false,
            (None, Some(_)) => true,
            (Some((stored_key, _)), Some((overlaid_key, _))) => {
                *stored_key >= overlaid_key.as_slice()
            }
        };
        if overlay_first {
            let Some((key, value)) = overlaid.next() else {
                continue;
            };
            if let Some(value) = value {
                f(&key, &value)?;
            }
            // The same key in the store is replaced by the overlay's entry.
            if next_stored.is_some_and(|(stored_key, _)| stored_key == key.as_slice()) {
                next_stored = stored.next().transpose()?;
            }
        } else if let Some((key, value)) = next_stored {
            f(key, value)?;
            next_stored = stored.next().transpose()?;
        }
    }
}

/// Guarded LMDB read transaction returned to callers. Holds a coordinator read
/// lock for the lifetime of the inner `RoTxn`, which owns a clone of the env.
///
/// Reads go through [`Self::get`] and [`Self::for_each_prefix`], which see the
/// handle's [`Overlay`] when it has one. The `Deref` to the raw `RoTxn` is for
/// inspection code that walks a persisted store with cursors; it bypasses the
/// overlay.
pub struct ReadTxn {
    // Must be declared before `_guard` so the LMDB transaction (and its env
    // clone) is dropped before the coordinator guard is released (Rust drops
    // fields in declaration order).
    txn: heed::RoTxn<'static, heed::WithoutTls>,
    _guard: tokio::sync::OwnedRwLockReadGuard<Option<Env>>,
    overlay: Option<Arc<Overlay>>,
}

impl ReadTxn {
    pub(crate) fn new(
        guard: tokio::sync::OwnedRwLockReadGuard<Option<Env>>,
        txn: heed::RoTxn<'static, heed::WithoutTls>,
        overlay: Option<Arc<Overlay>>,
    ) -> Self {
        Self {
            txn,
            _guard: guard,
            overlay,
        }
    }

    /// `key`'s value in `db`, if any.
    pub(crate) fn get(&self, db: Database, key: &[u8]) -> Result<Option<Cow<'_, [u8]>>> {
        get_through_overlay(&self.txn, self.overlay.as_deref(), db, key)
    }

    /// Calls `f` with every `(key, value)` of `db` under `prefix`, in key
    /// order.
    pub(crate) fn for_each_prefix(
        &self,
        db: Database,
        prefix: &[u8],
        f: impl FnMut(&[u8], &[u8]) -> Result<()>,
    ) -> Result<()> {
        let overlaid = self
            .overlay
            .as_ref()
            .map(|overlay| overlay.entries_with_prefix(prefix));
        for_each_prefix_through_overlay(&self.txn, overlaid, db, prefix, f)
    }
}

impl Deref for ReadTxn {
    type Target = heed::RoTxn<'static, heed::WithoutTls>;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}

enum WriteTxnInner<'env> {
    /// A body of the single-writer batcher: writes commit with the txn.
    Applying(heed::RwTxn<'env>),
    /// A body of a run that persists nothing: reads see the snapshot under
    /// the run's overlay (the snapshot carries it too) and this body's own
    /// `pending` writes, which join the overlay once the body succeeds — a
    /// failed body's writes are dropped, as a batched body's are by the
    /// aborted txn.
    Discarding {
        snapshot: ReadTxn,
        overlay: Arc<Overlay>,
        pending: Writes,
    },
}

/// The transaction a `run_txn` body runs against. Threaded through
/// `AppStore::run_txn` closures.
///
/// It is the one way to read and write LMDB inside a body. Writes go through
/// [`Self::put`], [`Self::delete`] and [`Self::delete_prefix`]; reads through
/// [`Self::get`] and [`Self::for_each_prefix`]. A body of a persisting handle
/// runs in a batched LMDB write txn; a body of a discarding handle runs
/// against a read snapshot and the run's [`Overlay`], and reads back what the
/// run wrote, exactly as the persisting body would.
pub struct WriteTxn<'env> {
    inner: WriteTxnInner<'env>,
}

impl<'env> WriteTxn<'env> {
    pub(crate) fn applying(txn: heed::RwTxn<'env>) -> Self {
        Self {
            inner: WriteTxnInner::Applying(txn),
        }
    }

    /// A txn over `snapshot`, whose overlay takes every write. The snapshot
    /// must carry an overlay: writes have nowhere else to go.
    pub(crate) fn discarding(snapshot: ReadTxn) -> Result<Self> {
        let Some(overlay) = snapshot.overlay.clone() else {
            return Err(internal_error!(
                "a discarding write txn needs a snapshot with an overlay"
            ));
        };
        Ok(Self {
            inner: WriteTxnInner::Discarding {
                snapshot,
                overlay,
                pending: Writes::new(),
            },
        })
    }

    /// Commits the writes: to LMDB if this txn persists them, else into the
    /// run's overlay, where the run's later bodies and reads see them.
    pub(crate) fn commit(self) -> Result<()> {
        match self.inner {
            WriteTxnInner::Applying(txn) => txn.commit()?,
            WriteTxnInner::Discarding {
                overlay, pending, ..
            } => overlay.absorb(pending),
        }
        Ok(())
    }

    /// `key`'s value in `db`, including this txn's own writes.
    pub(crate) fn get(&self, db: Database, key: &[u8]) -> Result<Option<Cow<'_, [u8]>>> {
        match &self.inner {
            WriteTxnInner::Applying(txn) => Ok(db.get(txn, key)?.map(Cow::Borrowed)),
            WriteTxnInner::Discarding {
                snapshot, pending, ..
            } => match pending.get(key) {
                Some(value) => Ok(value.clone().map(Cow::Owned)),
                None => snapshot.get(db, key),
            },
        }
    }

    /// Calls `f` with every `(key, value)` of `db` under `prefix`, in key
    /// order, including this txn's own writes.
    pub(crate) fn for_each_prefix(
        &self,
        db: Database,
        prefix: &[u8],
        f: impl FnMut(&[u8], &[u8]) -> Result<()>,
    ) -> Result<()> {
        match &self.inner {
            WriteTxnInner::Applying(txn) => {
                for_each_prefix_through_overlay(txn, None::<WriteEntries>, db, prefix, f)
            }
            WriteTxnInner::Discarding {
                snapshot,
                overlay,
                pending,
            } => {
                // This body's writes over the run's.
                let mut overlaid: Writes =
                    overlay.entries_with_prefix(prefix).into_iter().collect();
                overlaid.extend(writes_with_prefix(pending, prefix));
                for_each_prefix_through_overlay(&snapshot.txn, Some(overlaid), db, prefix, f)
            }
        }
    }

    /// Store `value` under `key` in `db`.
    pub(crate) fn put(&mut self, db: Database, key: &[u8], value: &[u8]) -> Result<()> {
        match &mut self.inner {
            WriteTxnInner::Applying(txn) => db.put(txn, key, value)?,
            WriteTxnInner::Discarding { pending, .. } => {
                pending.insert(key.to_vec(), Some(value.to_vec()));
            }
        }
        Ok(())
    }

    /// Delete `key` from `db`, if present.
    pub(crate) fn delete(&mut self, db: Database, key: &[u8]) -> Result<()> {
        match &mut self.inner {
            WriteTxnInner::Applying(txn) => {
                db.delete(txn, key)?;
            }
            WriteTxnInner::Discarding { pending, .. } => {
                pending.insert(key.to_vec(), None);
            }
        }
        Ok(())
    }

    /// Delete every entry of `db` whose key starts with `prefix`.
    pub(crate) fn delete_prefix(&mut self, db: Database, prefix: &[u8]) -> Result<()> {
        match &mut self.inner {
            WriteTxnInner::Applying(txn) => {
                let mut iter = db.prefix_iter_mut(txn, prefix)?;
                while iter.next().transpose()?.is_some() {
                    // Safety: the borrowed key/value are dropped before the
                    // next `next()`.
                    unsafe {
                        iter.del_current()?;
                    }
                }
            }
            WriteTxnInner::Discarding { .. } => {
                let mut keys = Vec::new();
                self.for_each_prefix(db, prefix, |key, _| {
                    keys.push(key.to_vec());
                    Ok(())
                })?;
                let WriteTxnInner::Discarding { pending, .. } = &mut self.inner else {
                    return Ok(());
                };
                for key in keys {
                    pending.insert(key, None);
                }
            }
        }
        Ok(())
    }
}

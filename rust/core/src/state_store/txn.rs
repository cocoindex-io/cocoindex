//! LMDB transaction wrappers and the shared transaction/resize coordinator.
//!
//! The coordinator is an [`EnvSlot`]: a lock around the LMDB env itself.
//! Every read or write LMDB transaction in this process holds a read guard on
//! it for its full lifetime, and reaches the env only through that guard.
//! The write guard is taken to call `unsafe Env::resize()` and to close the
//! env, guaranteeing no participating transaction is active.
//!
//! Both wrappers read through an optional view of an [`Overlay`]: the writes
//! of a run that persists nothing (a preview), kept in memory and layered over
//! the LMDB snapshot, so such a run reads its own writes back exactly as a
//! persisting run would. See [`super::AppStore::discarding_writes`].

use crate::prelude::*;
use crate::state_store::app_store::FIRST_ID_OF_A_RUN_THAT_PERSISTS_NOTHING;

pub(crate) type Env = heed::Env<heed::WithoutTls>;

/// LMDB database handle. Keys and values are opaque bytes; logical
/// key/value schemas live in [`crate::state::db_schema`].
pub(crate) type Database = heed::Database<heed::types::Bytes, heed::types::Bytes>;

/// An app's sub-database and the name that identifies it in the env. Reads
/// and writes inside a body take a `Db`, so an overlay, which holds one
/// database's writes, can refuse another's.
#[derive(Clone)]
pub(crate) struct Db {
    raw: Database,
    name: Arc<str>,
}

impl Db {
    pub(crate) fn new(raw: Database, name: &str) -> Self {
        Self {
            raw,
            name: Arc::from(name),
        }
    }

    /// The heed handle, for code that walks the committed store directly.
    pub(crate) fn raw(&self) -> Database {
        self.raw
    }
}

/// The LMDB env behind the transaction/resize coordinator. `None` once the
/// storage is closed.
pub(crate) type EnvSlot = tokio::sync::RwLock<Option<Env>>;

/// The env in a guarded [`EnvSlot`], or an error if the storage is closed.
pub(crate) fn opened_env(slot: &Option<Env>) -> Result<&Env> {
    slot.as_ref()
        .ok_or_else(|| client_error!("The CocoIndex environment is closed"))
}

/// A run's writes by encoded key: `Some` for a value put, `None` for a key
/// deleted. Persistent: a clone is O(1) and shares structure, so a read txn
/// keeps the writes it opened with, and a body edits its own copy.
type OverlayMap = rpds::RedBlackTreeMapSync<Vec<u8>, Option<Vec<u8>>>;

/// Source of [`Overlay::id`].
static NEXT_OVERLAY_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The writes of one run that persists nothing, to one database, as of the
/// run's last finished body. Every transaction the run opens reads through a
/// copy of them, so the run reads its own writes back; they are dropped with
/// the run, so nothing reaches LMDB.
pub(crate) struct Overlay {
    /// Tells this run's writes apart from every other run's.
    id: u64,
    /// The database the writes belong to.
    db: Arc<str>,
    writes: parking_lot::Mutex<OverlayMap>,
    /// Held for each body the run executes, so the run's bodies take turns
    /// as a batch's do: a body starts from the writes of the bodies before
    /// it, and its finished copy replaces the run's.
    body_turn: Arc<tokio::sync::Mutex<()>>,
}

impl Overlay {
    pub(crate) fn new(db: &Db) -> Self {
        Self {
            id: NEXT_OVERLAY_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            db: db.name.clone(),
            writes: parking_lot::Mutex::new(OverlayMap::new_sync()),
            body_turn: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    /// Waits for the run's turn to execute a body; the turn lasts as long as
    /// the returned guard.
    pub(crate) async fn body_turn(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.body_turn.clone().lock_owned().await
    }

    /// The run's writes as of now.
    fn writes(&self) -> OverlayMap {
        self.writes.lock().clone()
    }
}

/// A copy of a run's writes, with the database they belong to.
struct View {
    db: Arc<str>,
    writes: OverlayMap,
}

impl View {
    fn of(overlay: &Overlay) -> Self {
        Self {
            db: overlay.db.clone(),
            writes: overlay.writes(),
        }
    }

    /// The writes, if `db` is the database they belong to.
    fn writes_of(&self, db: &Db) -> Result<&OverlayMap> {
        if !Arc::ptr_eq(&self.db, &db.name) && *self.db != *db.name {
            return Err(internal_error!(
                "a run's overlay holds the writes of database `{}`, not `{}`",
                self.db,
                db.name
            ));
        }
        Ok(&self.writes)
    }
}

/// `key`'s value in `db` as `writes` (if any) over the LMDB `snapshot` has it.
fn get_through<'t>(
    snapshot: &'t heed::RoTxn<'_, heed::WithoutTls>,
    writes: Option<&'t OverlayMap>,
    db: &Db,
    key: &[u8],
) -> Result<Option<&'t [u8]>> {
    if let Some(value) = writes.and_then(|writes| writes.get(key)) {
        return Ok(value.as_deref());
    }
    Ok(db.raw.get(snapshot, key)?)
}

/// Calls `f` with every `(key, value)` of `db` under `prefix`, in key order,
/// as `writes` (if any) over the LMDB `snapshot` has them.
fn for_each_prefix_through(
    snapshot: &heed::RoTxn<'_, heed::WithoutTls>,
    writes: Option<&OverlayMap>,
    db: &Db,
    prefix: &[u8],
    mut f: impl FnMut(&[u8], &[u8]) -> Result<()>,
) -> Result<()> {
    use std::ops::Bound;
    let mut stored = db.raw.prefix_iter(snapshot, prefix)?;
    let Some(writes) = writes else {
        for entry in stored {
            let (key, value) = entry?;
            f(key, value)?;
        }
        return Ok(());
    };
    // Merge the two sorted sequences; on a key both have, the overlay wins
    // (a `None` there hides the stored entry).
    let mut overlaid = writes
        .range::<[u8], _>((Bound::Included(prefix), Bound::Unbounded))
        .take_while(|(key, _)| key.starts_with(prefix))
        .peekable();
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
                f(key, value)?;
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
/// Reads go through [`Self::get`] and [`Self::for_each_prefix`]. On a handle
/// that persists nothing they see the run's writes as of when the txn opened,
/// over the LMDB snapshot, so the txn is one consistent snapshot of what the
/// run sees. [`Self::committed`] reads LMDB alone.
pub struct ReadTxn {
    // Must be declared before `_guard` so the LMDB transaction (and its env
    // clone) is dropped before the coordinator guard is released (Rust drops
    // fields in declaration order).
    txn: heed::RoTxn<'static, heed::WithoutTls>,
    _guard: tokio::sync::OwnedRwLockReadGuard<Option<Env>>,
    view: Option<View>,
}

impl ReadTxn {
    pub(crate) fn new(
        guard: tokio::sync::OwnedRwLockReadGuard<Option<Env>>,
        txn: heed::RoTxn<'static, heed::WithoutTls>,
        overlay: Option<&Overlay>,
    ) -> Self {
        Self {
            txn,
            _guard: guard,
            view: overlay.map(View::of),
        }
    }

    fn writes_of(&self, db: &Db) -> Result<Option<&OverlayMap>> {
        self.view
            .as_ref()
            .map(|view| view.writes_of(db))
            .transpose()
    }

    /// `key`'s value in `db`, if any.
    pub(crate) fn get(&self, db: &Db, key: &[u8]) -> Result<Option<&[u8]>> {
        get_through(&self.txn, self.writes_of(db)?, db, key)
    }

    /// Calls `f` with every `(key, value)` of `db` under `prefix`, in key
    /// order.
    pub(crate) fn for_each_prefix(
        &self,
        db: &Db,
        prefix: &[u8],
        f: impl FnMut(&[u8], &[u8]) -> Result<()>,
    ) -> Result<()> {
        for_each_prefix_through(&self.txn, self.writes_of(db)?, db, prefix, f)
    }

    /// The LMDB snapshot alone, without any run's writes: for code that walks
    /// the committed store with cursors, such as inspection.
    pub(crate) fn committed(&self) -> &heed::RoTxn<'static, heed::WithoutTls> {
        &self.txn
    }
}

enum WriteTxnInner<'env> {
    /// A body of the single-writer batcher: writes commit with the txn.
    Applying(heed::RwTxn<'env>),
    /// A body of a run that persists nothing: it reads the LMDB `snapshot`
    /// under `view`, a copy of the run's writes, and writes to `view`, which
    /// replaces the run's in `overlay` once the body succeeds — a failed
    /// body's writes are dropped, as a batched body's are by the aborted txn.
    Discarding {
        snapshot: ReadTxn,
        view: View,
        overlay: Arc<Overlay>,
    },
}

/// The transaction a `run_txn` body runs against. Threaded through
/// `AppStore::run_txn` closures.
///
/// It is the one way to read and write LMDB inside a body. Writes go through
/// [`Self::put`], [`Self::delete`] and [`Self::delete_prefix`]; reads through
/// [`Self::get`] and [`Self::for_each_prefix`]. A body of a persisting handle
/// runs in a batched LMDB write txn; a body of a discarding handle runs
/// against a read snapshot and a copy of the run's [`Overlay`], and reads
/// back what the run wrote, exactly as the persisting body would.
pub struct WriteTxn<'env> {
    inner: WriteTxnInner<'env>,
}

impl<'env> WriteTxn<'env> {
    pub(crate) fn applying(txn: heed::RwTxn<'env>) -> Self {
        Self {
            inner: WriteTxnInner::Applying(txn),
        }
    }

    /// A txn reading `snapshot` (opened without an overlay) under the run's
    /// writes in `overlay`, which takes this txn's writes on [`Self::commit`].
    pub(crate) fn discarding(snapshot: ReadTxn, overlay: Arc<Overlay>) -> Self {
        Self {
            inner: WriteTxnInner::Discarding {
                snapshot,
                view: View::of(&overlay),
                overlay,
            },
        }
    }

    /// The lowest id this txn's id reservations may hand out: 1 when they
    /// persist; past every id a persisting run will reach when they belong to
    /// a run that persists nothing, so the two never hand out the same id.
    pub(crate) fn first_reservable_id(&self) -> u64 {
        match self.inner {
            WriteTxnInner::Applying(_) => 1,
            WriteTxnInner::Discarding { .. } => FIRST_ID_OF_A_RUN_THAT_PERSISTS_NOTHING,
        }
    }

    /// Commits the writes: to LMDB if this txn persists them, else into the
    /// run's overlay, where the run's later bodies and reads see them.
    pub(crate) fn commit(self) -> Result<()> {
        match self.inner {
            WriteTxnInner::Applying(txn) => txn.commit()?,
            WriteTxnInner::Discarding { view, overlay, .. } => {
                *overlay.writes.lock() = view.writes;
            }
        }
        Ok(())
    }

    /// `key`'s value in `db`, including this txn's own writes.
    pub(crate) fn get(&self, db: &Db, key: &[u8]) -> Result<Option<&[u8]>> {
        match &self.inner {
            WriteTxnInner::Applying(txn) => Ok(db.raw.get(txn, key)?),
            WriteTxnInner::Discarding { snapshot, view, .. } => {
                get_through(&snapshot.txn, Some(view.writes_of(db)?), db, key)
            }
        }
    }

    /// Calls `f` with every `(key, value)` of `db` under `prefix`, in key
    /// order, including this txn's own writes.
    pub(crate) fn for_each_prefix(
        &self,
        db: &Db,
        prefix: &[u8],
        f: impl FnMut(&[u8], &[u8]) -> Result<()>,
    ) -> Result<()> {
        match &self.inner {
            WriteTxnInner::Applying(txn) => for_each_prefix_through(txn, None, db, prefix, f),
            WriteTxnInner::Discarding { snapshot, view, .. } => {
                for_each_prefix_through(&snapshot.txn, Some(view.writes_of(db)?), db, prefix, f)
            }
        }
    }

    /// Records `value` for `key` in a discarding txn's copy of the run's
    /// writes.
    fn record_put(view: &mut View, db: &Db, key: &[u8], value: &[u8]) -> Result<()> {
        view.writes_of(db)?;
        view.writes.insert_mut(key.to_vec(), Some(value.to_vec()));
        Ok(())
    }

    /// Hides `key` from the rest of a discarding run: a deletion over the
    /// snapshot's entry if it has one, otherwise just dropping the run's own
    /// write, so the run's writes hold no entries for keys LMDB never had.
    fn record_forget(snapshot: &ReadTxn, view: &mut View, db: &Db, key: &[u8]) -> Result<()> {
        view.writes_of(db)?;
        if db.raw.get(&snapshot.txn, key)?.is_some() {
            view.writes.insert_mut(key.to_vec(), None);
        } else {
            view.writes.remove_mut(key);
        }
        Ok(())
    }

    /// Store `value` under `key` in `db`.
    pub(crate) fn put(&mut self, db: &Db, key: &[u8], value: &[u8]) -> Result<()> {
        match &mut self.inner {
            WriteTxnInner::Applying(txn) => db.raw.put(txn, key, value)?,
            WriteTxnInner::Discarding { view, .. } => Self::record_put(view, db, key, value)?,
        }
        Ok(())
    }

    /// Store `value` under `key` in `db` if this txn persists its writes;
    /// otherwise forget `key` for the rest of the run, as if it had been
    /// deleted. For large values that a run can recompute when it misses
    /// them (function memos), so a run that persists nothing does not hold
    /// every one of them in memory.
    pub(crate) fn put_or_forget(&mut self, db: &Db, key: &[u8], value: &[u8]) -> Result<()> {
        match &mut self.inner {
            WriteTxnInner::Applying(txn) => db.raw.put(txn, key, value)?,
            WriteTxnInner::Discarding { snapshot, view, .. } => {
                Self::record_forget(snapshot, view, db, key)?
            }
        }
        Ok(())
    }

    /// Delete `key` from `db`, if present.
    pub(crate) fn delete(&mut self, db: &Db, key: &[u8]) -> Result<()> {
        match &mut self.inner {
            WriteTxnInner::Applying(txn) => {
                db.raw.delete(txn, key)?;
            }
            WriteTxnInner::Discarding { snapshot, view, .. } => {
                Self::record_forget(snapshot, view, db, key)?
            }
        }
        Ok(())
    }

    /// Delete every entry of `db` whose key starts with `prefix`.
    pub(crate) fn delete_prefix(&mut self, db: &Db, prefix: &[u8]) -> Result<()> {
        match &mut self.inner {
            WriteTxnInner::Applying(txn) => {
                let mut iter = db.raw.prefix_iter_mut(txn, prefix)?;
                while iter.next().transpose()?.is_some() {
                    // Safety: the borrowed key/value are dropped before the
                    // next `next()`.
                    unsafe {
                        iter.del_current()?;
                    }
                }
            }
            WriteTxnInner::Discarding { snapshot, view, .. } => {
                use std::ops::Bound;
                // The run's own writes under the prefix go; the snapshot's
                // entries are hidden behind deletions.
                let overlaid: Vec<Vec<u8>> = view
                    .writes_of(db)?
                    .range::<[u8], _>((Bound::Included(prefix), Bound::Unbounded))
                    .map(|(key, _)| key)
                    .take_while(|key| key.starts_with(prefix))
                    .cloned()
                    .collect();
                for key in overlaid {
                    view.writes.remove_mut(key.as_slice());
                }
                for entry in db.raw.prefix_iter(&snapshot.txn, prefix)? {
                    let (key, _) = entry?;
                    view.writes.insert_mut(key.to_vec(), None);
                }
            }
        }
        Ok(())
    }
}

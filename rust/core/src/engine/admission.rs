//! Admission of component runs to the app's pool of in-flight tokens.
//!
//! An app has `max_inflight_components` tokens. A component run needs a
//! token to execute its body and holds it until the run ends, children and
//! commit included. A token comes from one of two places:
//!
//! - **the pool**, which hands out a free token or makes the mount wait; or
//! - **the parent's token, on loan.** Every admitted run offers its own token
//!   to its children through a [`Lender`], one child at a time. The first
//!   child to ask runs on the loan; when it finishes, the token passes to the
//!   parent's next waiting child. Children that find the token lent compete
//!   for pool tokens like everyone else.
//!
//! The loan keeps the hierarchy deadlock-free: a parent that is blocked on its
//! children always has one child that can run, however empty the pool is, so
//! `max_inflight_components` can be as small as one. It also bounds memory:
//! every chain of loans starts at a pool token, so runs in flight never exceed
//! the pool size times the depth of the component tree, and runs that hold a
//! token not currently lent out never exceed the pool size.
//!
//! A parent keeps lending for as long as it runs. When it ends while its token
//! is still out on loan (only live components do this, see
//! [`Lender::close`]), the loan takes over the parent's token and returns it
//! to the pool when it ends, so the count stays exact.
//!
//! # Which waiter a released pool token goes to
//!
//! The pool is shared fairly between **independent updates**, so a large
//! update's backlog never makes a small one wait for all of it.
//!
//! - **An independent update** is a run with no parent run, together with
//!   every run below it: an app update, one cycle of a live component, or
//!   one per-key update of a live component. Its runs count their pool
//!   tokens against its [`UpdateShare`]; a child run inherits its parent's.
//! - **A live component's updates divide the live component's
//!   [`LiveShare`].** A live component that an update mounts sits beside
//!   that update, under the same live component, or under the app.
//! - **A released token goes down this tree from the app.** At each level it
//!   goes to the child whose subtree holds the fewest pool tokens; among
//!   equals, to the one that started waiting first. Within one update,
//!   waiters are served in arrival order.
//!
//! Fairness acts only when a released token is handed to a waiter. A run
//! that finds a free token and nobody waiting takes it at once, and no token
//! is reserved, so an update that runs alone can take the whole pool. With
//! one update waiting, the order is arrival order. The loan is unaffected:
//! a run takes its parent's token first when it is free, and the loan alone
//! keeps the hierarchy deadlock-free, whichever waiter the pool serves.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::prelude::*;

/// The app's pool of in-flight tokens. Unbounded when created with `None`:
/// every [`Admission`] then admits at once and lends nothing.
#[derive(Clone)]
pub struct AdmissionPool {
    inner: Arc<PoolInner>,
}

impl AdmissionPool {
    pub fn new(max_inflight_components: Option<usize>) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                bounded: max_inflight_components.is_some(),
                state: Mutex::new(PoolState {
                    free: max_inflight_components.unwrap_or(0),
                    waiting: 0,
                    next_seq: 0,
                }),
                root: Arc::new(ShareNode::new(0, None)),
                next_node_id: AtomicU64::new(1),
            }),
        }
    }

    /// Tokens not held by any run; `None` for an unbounded pool.
    pub fn available_tokens(&self) -> Option<usize> {
        self.inner.bounded.then(|| self.inner.state.lock().free)
    }

    /// The share of a new app-level update: an `App` update or drop, or any
    /// other run built without a parent run or a live component.
    pub(crate) fn new_update(&self) -> UpdateShare {
        UpdateShare {
            pool: self.clone(),
            node: self.inner.new_node(self.inner.root.clone()),
        }
    }

    #[cfg(test)]
    fn waiting_runs(&self) -> usize {
        self.inner.state.lock().waiting
    }
}

/// The independent update a run belongs to: its pool tokens count here.
/// Every run of the update shares it, because a child run inherits its
/// parent's.
#[derive(Clone)]
pub(crate) struct UpdateShare {
    pool: AdmissionPool,
    node: Arc<ShareNode>,
}

impl UpdateShare {
    /// The share of a live component that a run of this update mounts. It
    /// sits beside this update, under the live component (or the app) this
    /// update belongs to: the live component outlives the update and
    /// competes with that live component's other updates.
    pub(crate) fn new_live(&self) -> LiveShare {
        let enclosing = self
            .node
            .parent
            .clone()
            .expect("an update's share always has a parent");
        LiveShare {
            pool: self.pool.clone(),
            node: self.pool.inner.new_node(enclosing),
        }
    }
}

/// A live component's share of the pool, divided between its updates (its
/// cycles and per-key updates) and the live components they mount.
#[derive(Clone)]
pub(crate) struct LiveShare {
    pool: AdmissionPool,
    node: Arc<ShareNode>,
}

impl LiveShare {
    /// The share of one new update of this live component.
    pub(crate) fn new_update(&self) -> UpdateShare {
        UpdateShare {
            pool: self.pool.clone(),
            node: self.pool.inner.new_node(self.node.clone()),
        }
    }
}

struct PoolInner {
    /// `false` for an unbounded pool, which admits at once.
    bounded: bool,
    state: Mutex<PoolState>,
    /// The app: every share hangs below it.
    root: Arc<ShareNode>,
    next_node_id: AtomicU64,
}

struct PoolState {
    free: usize,
    /// Runs waiting for a pool token, in the whole tree.
    waiting: usize,
    /// Numbers the waiters and orders "started waiting" among equals.
    next_seq: u64,
}

/// A node of the share tree: the app (the root), a live component, or an
/// update (a leaf, the only kind with waiters). Its state is read and
/// written only under the pool's lock, so the node's own mutex never
/// contends; one node's state is locked at a time.
struct ShareNode {
    id: u64,
    /// `None` for the root only.
    parent: Option<Arc<ShareNode>>,
    state: Mutex<ShareState>,
}

#[derive(Default)]
struct ShareState {
    /// Pool tokens held by the runs of this subtree.
    held: usize,
    /// Runs of this subtree waiting for a pool token.
    waiting: usize,
    /// While `waiting > 0`, this node's key in its parent's `ready`.
    key: Option<ReadyKey>,
    /// The children with waiting runs, the next to serve first.
    ready: BTreeMap<ReadyKey, Arc<ShareNode>>,
    /// An update's waiting runs, in arrival order.
    waiters: VecDeque<Waiter>,
}

/// `(held, started waiting, node id)`: fewest held first, then the first to
/// start waiting. The id keeps keys unique.
type ReadyKey = (usize, u64, u64);

struct Waiter {
    seq: u64,
    grant: oneshot::Sender<PoolPermit>,
}

impl ShareNode {
    fn new(id: u64, parent: Option<Arc<ShareNode>>) -> Self {
        Self {
            id,
            parent,
            state: Mutex::new(ShareState::default()),
        }
    }
}

impl PoolInner {
    fn new_node(&self, parent: Arc<ShareNode>) -> Arc<ShareNode> {
        let id = self.next_node_id.fetch_add(1, Ordering::Relaxed);
        Arc::new(ShareNode::new(id, Some(parent)))
    }

    /// Take a pool token for a run of `share`'s update: at once when one is
    /// free and nobody waits, else when a release hands it over.
    ///
    /// Cancel-safe: dropped while queued, the run leaves the queue; dropped
    /// after a grant it did not see, the token goes to the next waiter.
    async fn acquire(self: &Arc<Self>, share: &Arc<ShareNode>) -> Result<PoolPermit> {
        let mut wait = {
            let mut state = self.state.lock();
            if state.free > 0 && state.waiting == 0 {
                state.free -= 1;
                change_held(share, true);
                return Ok(PoolPermit {
                    pool: self.clone(),
                    share: share.clone(),
                });
            }
            // A release hands every free token to a waiter, so a run that
            // has to wait finds none free.
            debug_assert_eq!(state.free, 0, "a free token beside a waiting run");
            let (grant, receiver) = oneshot::channel();
            let seq = state.next_seq;
            state.next_seq += 1;
            state.waiting += 1;
            enqueue(share, Waiter { seq, grant }, &mut state.next_seq);
            Wait {
                pool: self.clone(),
                share: share.clone(),
                seq,
                receiver,
                received: false,
            }
        };
        let permit = (&mut wait.receiver)
            .await
            .map_err(|_| internal_error!("in-flight component pool closed"))?;
        wait.received = true;
        Ok(permit)
    }

    /// Give `share`'s token back and hand the free tokens to waiters.
    fn release(self: &Arc<Self>, share: &Arc<ShareNode>) {
        let mut unclaimed = Vec::new();
        {
            let mut state = self.state.lock();
            change_held(share, false);
            state.free += 1;
            self.grant_waiting(&mut state, &mut unclaimed);
        }
        // Each returns its token through `release`, which takes the lock.
        drop(unclaimed);
    }

    /// Hand free tokens to waiters down the share tree. A permit whose
    /// waiter has gone is put in `unclaimed`, for the caller to drop once
    /// the lock is released.
    fn grant_waiting(self: &Arc<Self>, state: &mut PoolState, unclaimed: &mut Vec<PoolPermit>) {
        while state.free > 0 && state.waiting > 0 {
            let Some(share) = self.next_to_serve() else {
                break;
            };
            let Some(waiter) = dequeue_front(&share) else {
                break;
            };
            state.waiting -= 1;
            state.free -= 1;
            change_held(&share, true);
            let permit = PoolPermit {
                pool: self.clone(),
                share,
            };
            if let Err(permit) = waiter.grant.send(permit) {
                unclaimed.push(permit);
            }
        }
    }

    /// The update whose first waiter the next token goes to: down from the
    /// root, the ready child that holds the fewest tokens at each level.
    fn next_to_serve(&self) -> Option<Arc<ShareNode>> {
        let mut node = self.root.clone();
        loop {
            let next = {
                let state = node.state.lock();
                if !state.waiters.is_empty() {
                    None
                } else {
                    Some(state.ready.values().next().cloned())
                }
            };
            match next {
                None => return Some(node),
                Some(Some(child)) => node = child,
                Some(None) => return None,
            }
        }
    }
}

/// Count a pool token taken (`taken`) or given back by a run of `share`'s
/// update, in `share` and every ancestor, moving each waiting node to its
/// new place in its parent's `ready`. Called under the pool's lock.
fn change_held(share: &Arc<ShareNode>, taken: bool) {
    let mut node = share.clone();
    loop {
        let moved = {
            let mut state = node.state.lock();
            if taken {
                state.held += 1;
            } else {
                state.held -= 1;
            }
            let held = state.held;
            state.key.as_mut().map(|key| {
                let old = *key;
                key.0 = held;
                (old, *key)
            })
        };
        let Some(parent) = node.parent.clone() else {
            break;
        };
        if let Some((old, new)) = moved {
            let mut parent_state = parent.state.lock();
            if let Some(child) = parent_state.ready.remove(&old) {
                parent_state.ready.insert(new, child);
            }
        }
        node = parent;
    }
}

/// Queue `waiter` in `share`'s update and count it in every ancestor; a node
/// that starts waiting takes its place in its parent's `ready`. Called under
/// the pool's lock.
fn enqueue(share: &Arc<ShareNode>, waiter: Waiter, next_seq: &mut u64) {
    share.state.lock().waiters.push_back(waiter);
    let mut node = share.clone();
    loop {
        let parent = node.parent.clone();
        let started = {
            let mut state = node.state.lock();
            state.waiting += 1;
            if state.waiting == 1 && parent.is_some() {
                let key = (state.held, *next_seq, node.id);
                *next_seq += 1;
                state.key = Some(key);
                Some(key)
            } else {
                None
            }
        };
        let Some(parent) = parent else {
            break;
        };
        if let Some(key) = started {
            parent.state.lock().ready.insert(key, node.clone());
        }
        node = parent;
    }
}

/// Take the first waiter of `share`'s update out of its queue. Called under
/// the pool's lock.
fn dequeue_front(share: &Arc<ShareNode>) -> Option<Waiter> {
    let waiter = share.state.lock().waiters.pop_front()?;
    count_waiter_out(share);
    Some(waiter)
}

/// Take the waiter numbered `seq` out of `share`'s queue; `false` when it is
/// no longer queued because a release granted it. Called under the pool's
/// lock.
fn remove_waiter(share: &Arc<ShareNode>, seq: u64) -> bool {
    let removed = {
        let mut state = share.state.lock();
        match state.waiters.iter().position(|w| w.seq == seq) {
            Some(index) => state.waiters.remove(index).is_some(),
            None => false,
        }
    };
    if removed {
        count_waiter_out(share);
    }
    removed
}

/// Count one waiter out of `share` and its ancestors, after it left
/// `share`'s queue; a node with no waiter left leaves its parent's `ready`.
fn count_waiter_out(share: &Arc<ShareNode>) {
    let mut node = share.clone();
    loop {
        let left = {
            let mut state = node.state.lock();
            state.waiting -= 1;
            if state.waiting == 0 {
                state.key.take()
            } else {
                None
            }
        };
        let Some(parent) = node.parent.clone() else {
            break;
        };
        if let Some(key) = left {
            parent.state.lock().ready.remove(&key);
        }
        node = parent;
    }
}

/// A run's place in its update's queue. Dropped before the grant was seen,
/// it leaves the queue; a grant already sent is a permit in `receiver`,
/// which returns its token when `receiver` drops, after the lock below is
/// released.
struct Wait {
    pool: Arc<PoolInner>,
    share: Arc<ShareNode>,
    seq: u64,
    receiver: oneshot::Receiver<PoolPermit>,
    received: bool,
}

impl Drop for Wait {
    fn drop(&mut self) {
        if self.received {
            return;
        }
        let mut state = self.pool.state.lock();
        if remove_waiter(&self.share, self.seq) {
            state.waiting -= 1;
        }
    }
}

/// A token taken from the pool, counted against its update until dropped.
pub(crate) struct PoolPermit {
    pool: Arc<PoolInner>,
    share: Arc<ShareNode>,
}

impl Drop for PoolPermit {
    fn drop(&mut self) {
        self.pool.release(&self.share);
    }
}

/// One in-flight token held by a run.
enum Slot {
    /// A token taken from the pool.
    Pool(#[allow(dead_code)] PoolPermit),
    /// The parent's token, on loan from its [`Lender`].
    Loan(#[allow(dead_code)] Loan),
}

struct LenderState {
    /// The owner's token is out on loan.
    lent: bool,
    /// The owner's run has ended; no further loans.
    closed: bool,
    /// The owner's own token, parked here when the owner ended while `lent`.
    /// Released when the loan returns.
    parked: Option<Slot>,
}

struct LenderInner {
    state: Mutex<LenderState>,
    /// `notify_one` when the token returns from a loan; `notify_waiters` when
    /// the lender closes.
    returned: Notify,
}

/// A run's token offered to its children, one at a time.
#[derive(Clone)]
pub struct Lender(Arc<LenderInner>);

/// The parent's token, held by a child run. Returning it (drop) passes the
/// token to the next waiting child.
pub(crate) struct Loan(Arc<LenderInner>);

impl Drop for Loan {
    fn drop(&mut self) {
        let parked = {
            let mut state = self.0.state.lock();
            debug_assert!(state.lent, "loan returned to a lender that is not lending");
            state.lent = false;
            state.parked.take()
        };
        // The owner ended while lending: its token goes back to where it
        // came from now that nothing runs on it.
        drop(parked);
        self.0.returned.notify_one();
    }
}

impl Lender {
    fn new() -> Self {
        Self(Arc::new(LenderInner {
            state: Mutex::new(LenderState {
                lent: false,
                closed: false,
                parked: None,
            }),
            returned: Notify::new(),
        }))
    }

    /// Wait for the owner's token and take it. Waiters are served in order.
    /// `None` once the lender is closed, immediately or while waiting.
    ///
    /// Cancel-safe: a dropped `borrow` never holds the token, and a wake-up
    /// it received but did not consume is passed to the next waiter.
    pub(crate) async fn borrow(&self) -> Option<Loan> {
        loop {
            // Register before checking so a return between the check and the
            // await still wakes us (`enable` makes `notify_one` see us).
            let mut returned = std::pin::pin!(self.0.returned.notified());
            returned.as_mut().enable();
            {
                let mut state = self.0.state.lock();
                if state.closed {
                    return None;
                }
                if !state.lent {
                    state.lent = true;
                    return Some(Loan(self.0.clone()));
                }
            }
            returned.await;
        }
    }

    /// Stop lending: waiting borrowers get `None`. `owner_slot` is the owner's
    /// own token; it is released now, or parked until the outstanding loan
    /// returns if the token is lent at this moment.
    fn close(&self, owner_slot: Option<Slot>) {
        let release_now = {
            let mut state = self.0.state.lock();
            state.closed = true;
            if state.lent {
                debug_assert!(state.parked.is_none());
                state.parked = owner_slot;
                None
            } else {
                owner_slot
            }
        };
        drop(release_now);
        self.0.returned.notify_waiters();
    }

    #[cfg(test)]
    fn is_lent(&self) -> bool {
        self.0.state.lock().lent
    }
}

struct AdmissionState {
    slot: Option<Slot>,
    finished: bool,
}

/// A run's admission state: its token once admitted, its [`Lender`], and
/// the update its pool tokens count against.
pub(crate) struct Admission {
    share: UpdateShare,
    /// The parent run's lender, if the run has a parent (or, for a live
    /// component's runs, the lender of the run that mounted the live
    /// component).
    parent: Option<Lender>,
    state: Mutex<AdmissionState>,
    lender: Lender,
}

impl Admission {
    pub(crate) fn new(share: UpdateShare, parent: Option<Lender>) -> Self {
        Self {
            share,
            parent,
            state: Mutex::new(AdmissionState {
                slot: None,
                finished: false,
            }),
            lender: Lender::new(),
        }
    }

    /// This run's token, offered to its children.
    pub(crate) fn lender(&self) -> &Lender {
        &self.lender
    }

    /// The update this run belongs to, which its children inherit.
    pub(crate) fn share(&self) -> &UpdateShare {
        &self.share
    }

    /// Take a token: the parent's if it is free, else the first of the
    /// parent's or a pool token to become available, the pool's handed out
    /// fairly between updates. Returns at once when already admitted or when
    /// the pool is unbounded. Cancel-safe.
    pub(crate) async fn admit(&self) -> Result<()> {
        let pool = &self.share.pool.inner;
        if !pool.bounded {
            return Ok(());
        }
        {
            let state = self.state.lock();
            if state.finished {
                internal_bail!("component run already finished");
            }
            if state.slot.is_some() {
                return Ok(());
            }
        }
        let slot = acquire_slot(self.parent.as_ref(), pool, &self.share.node).await?;
        let mut state = self.state.lock();
        if state.finished {
            internal_bail!("component run already finished");
        }
        state.slot = Some(slot);
        Ok(())
    }

    /// Whether the run still has to take a token before it may execute:
    /// false once admitted, and always for an unbounded pool.
    pub(crate) fn needs_token(&self) -> bool {
        if !self.share.pool.inner.bounded {
            return false;
        }
        let state = self.state.lock();
        state.slot.is_none() && !state.finished
    }

    /// End the run: stop lending and give the token back. Idempotent; also
    /// run on drop.
    pub(crate) fn finish(&self) {
        let slot = {
            let mut state = self.state.lock();
            if state.finished {
                return;
            }
            state.finished = true;
            state.slot.take()
        };
        self.lender.close(slot);
    }

    #[cfg(test)]
    pub(crate) fn is_admitted(&self) -> bool {
        self.state.lock().slot.is_some()
    }
}

impl Drop for Admission {
    fn drop(&mut self) {
        self.finish();
    }
}

async fn acquire_slot(
    parent: Option<&Lender>,
    pool: &Arc<PoolInner>,
    share: &Arc<ShareNode>,
) -> Result<Slot> {
    let borrow = async {
        match parent {
            Some(lender) => lender.borrow().await,
            None => None,
        }
    };
    tokio::select! {
        // The parent's token first when both are free, so pool tokens stay
        // with runs that have no parent to borrow from.
        biased;
        Some(loan) = borrow => Ok(Slot::Loan(loan)),
        permit = pool.acquire(share) => Ok(Slot::Pool(permit?)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn bounded(n: usize) -> AdmissionPool {
        AdmissionPool::new(Some(n))
    }

    /// Give concurrently spawned tasks a chance to run.
    async fn settle() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    #[tokio::test]
    async fn first_child_runs_on_the_parents_token() {
        let pool = bounded(4);
        let parent = Admission::new(pool.new_update(), None);
        parent.admit().await.unwrap();
        assert_eq!(pool.available_tokens(), Some(3));

        let child = Admission::new(parent.share().clone(), Some(parent.lender().clone()));
        child.admit().await.unwrap();
        assert!(parent.lender().is_lent());
        assert_eq!(
            pool.available_tokens(),
            Some(3),
            "the loan takes no pool token"
        );

        let sibling = Admission::new(parent.share().clone(), Some(parent.lender().clone()));
        sibling.admit().await.unwrap();
        assert_eq!(pool.available_tokens(), Some(2), "the token is lent: pool");

        child.finish();
        assert!(!parent.lender().is_lent());
        sibling.finish();
        parent.finish();
        assert_eq!(pool.available_tokens(), Some(4));
    }

    #[tokio::test]
    async fn the_token_passes_to_the_next_waiting_child_in_order() {
        let pool = bounded(1);
        let parent = Arc::new(Admission::new(pool.new_update(), None));
        parent.admit().await.unwrap();
        let first = Admission::new(parent.share().clone(), Some(parent.lender().clone()));
        first.admit().await.unwrap();

        let order = Arc::new(Mutex::new(Vec::new()));
        let mut waiters = Vec::new();
        for i in 0..3 {
            let admission = Arc::new(Admission::new(
                parent.share().clone(),
                Some(parent.lender().clone()),
            ));
            let order = order.clone();
            let task = {
                let admission = admission.clone();
                tokio::spawn(async move {
                    admission.admit().await.unwrap();
                    order.lock().push(i);
                })
            };
            // Register in order.
            settle().await;
            waiters.push((admission, task));
        }
        assert!(
            order.lock().is_empty(),
            "pool empty and token lent: all wait"
        );

        first.finish();
        for (admission, task) in waiters {
            task.await.unwrap();
            assert!(admission.is_admitted());
            admission.finish();
        }
        assert_eq!(*order.lock(), vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn a_waiter_takes_whichever_token_frees_first() {
        let pool = bounded(2);
        let parent = Admission::new(pool.new_update(), None);
        parent.admit().await.unwrap();
        let lent = Admission::new(parent.share().clone(), Some(parent.lender().clone()));
        lent.admit().await.unwrap();
        let pooled = Admission::new(parent.share().clone(), Some(parent.lender().clone()));
        pooled.admit().await.unwrap();
        assert_eq!(pool.available_tokens(), Some(0));

        let waiter = Arc::new(Admission::new(
            parent.share().clone(),
            Some(parent.lender().clone()),
        ));
        let task = {
            let waiter = waiter.clone();
            tokio::spawn(async move { waiter.admit().await.unwrap() })
        };
        settle().await;
        assert!(!waiter.is_admitted());

        // A pool token frees first: the waiter takes it and leaves the
        // lender's queue, so the loan stays with `lent`.
        pooled.finish();
        task.await.unwrap();
        assert!(waiter.is_admitted());
        assert_eq!(pool.available_tokens(), Some(0));
        assert!(parent.lender().is_lent());
        lent.finish();
        assert!(!parent.lender().is_lent());
    }

    #[tokio::test]
    async fn a_parent_that_ends_while_lending_hands_its_token_to_the_loan() {
        let pool = bounded(1);
        let parent = Admission::new(pool.new_update(), None);
        parent.admit().await.unwrap();
        let child = Admission::new(parent.share().clone(), Some(parent.lender().clone()));
        child.admit().await.unwrap();

        parent.finish();
        assert_eq!(
            pool.available_tokens(),
            Some(0),
            "the loan still runs on the token"
        );
        // Nobody borrows from a closed lender.
        let late = Admission::new(parent.share().clone(), Some(parent.lender().clone()));
        let late_task = tokio::spawn(async move {
            late.admit().await.unwrap();
            late
        });
        settle().await;
        assert!(!late_task.is_finished());

        child.finish();
        let late = late_task.await.unwrap();
        assert!(late.is_admitted(), "the returned token went to the pool");
        assert_eq!(pool.available_tokens(), Some(0));
        late.finish();
        assert_eq!(pool.available_tokens(), Some(1));
    }

    #[tokio::test]
    async fn closing_the_lender_sends_waiters_to_the_pool() {
        let pool = bounded(1);
        let parent = Admission::new(pool.new_update(), None);
        parent.admit().await.unwrap();
        let child = Admission::new(parent.share().clone(), Some(parent.lender().clone()));
        child.admit().await.unwrap();
        let waiter = Arc::new(Admission::new(
            parent.share().clone(),
            Some(parent.lender().clone()),
        ));
        let task = {
            let waiter = waiter.clone();
            tokio::spawn(async move { waiter.admit().await.unwrap() })
        };
        settle().await;
        assert!(!waiter.is_admitted());

        // The parent ends first, then the lent child: its token (now the
        // loan's) returns to the pool, where the waiter has queued.
        parent.finish();
        settle().await;
        assert!(!waiter.is_admitted());
        child.finish();
        task.await.unwrap();
        assert!(waiter.is_admitted());
    }

    #[tokio::test]
    async fn admit_is_idempotent_and_unbounded_pools_admit_at_once() {
        let pool = bounded(1);
        let run = Admission::new(pool.new_update(), None);
        run.admit().await.unwrap();
        run.admit().await.unwrap();
        assert_eq!(pool.available_tokens(), Some(0));
        drop(run);
        assert_eq!(pool.available_tokens(), Some(1), "drop releases");

        let unbounded = AdmissionPool::new(None);
        let run = Admission::new(unbounded.new_update(), None);
        run.admit().await.unwrap();
        assert!(!run.is_admitted());
        assert_eq!(unbounded.available_tokens(), None);
    }

    #[tokio::test]
    async fn a_cancelled_wait_passes_its_wake_up_on() {
        let pool = bounded(1);
        let parent = Admission::new(pool.new_update(), None);
        parent.admit().await.unwrap();
        let child = Admission::new(parent.share().clone(), Some(parent.lender().clone()));
        child.admit().await.unwrap();

        let abandoned = Admission::new(parent.share().clone(), Some(parent.lender().clone()));
        let abandoned_task = tokio::spawn(async move { abandoned.admit().await });
        settle().await;
        let waiter = Arc::new(Admission::new(
            parent.share().clone(),
            Some(parent.lender().clone()),
        ));
        let waiter_task = {
            let waiter = waiter.clone();
            tokio::spawn(async move { waiter.admit().await.unwrap() })
        };
        settle().await;

        // The first waiter gives up before the token comes back.
        abandoned_task.abort();
        let _ = abandoned_task.await;
        child.finish();
        waiter_task.await.unwrap();
        assert!(waiter.is_admitted());
    }

    // --- Which waiter a released pool token goes to ---------------------

    /// A run of `share`'s update holding a pool token.
    async fn holding(share: &UpdateShare) -> Admission {
        let run = Admission::new(share.clone(), None);
        run.admit().await.unwrap();
        run
    }

    /// A child of `parent`, which borrows its token first.
    fn child_of(parent: &Admission) -> Admission {
        Admission::new(parent.share().clone(), Some(parent.lender().clone()))
    }

    /// A run of `share`'s update waiting for a pool token in a task, which
    /// records `name` in `order` once admitted.
    async fn wait_in(
        share: &UpdateShare,
        name: &'static str,
        order: &Arc<Mutex<Vec<&'static str>>>,
    ) -> (Arc<Admission>, tokio::task::JoinHandle<()>) {
        let run = Arc::new(Admission::new(share.clone(), None));
        let task = {
            let run = run.clone();
            let order = order.clone();
            tokio::spawn(async move {
                run.admit().await.unwrap();
                order.lock().push(name);
            })
        };
        settle().await;
        (run, task)
    }

    /// Wait for a waiting run's task, failing instead of hanging when the
    /// run is never admitted: a wrong order leaves it waiting for a release
    /// that never comes.
    async fn joined(task: tokio::task::JoinHandle<()>) {
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("the run was never admitted")
            .unwrap();
    }

    #[tokio::test]
    async fn a_released_token_goes_to_the_update_holding_the_fewest() {
        let pool = bounded(2);
        let big = pool.new_update();
        let small = pool.new_update();
        let first = holding(&big).await;
        let second = holding(&big).await;
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut waiting = Vec::new();
        for name in ["big0", "big1", "big2"] {
            waiting.push(wait_in(&big, name, &order).await);
        }
        // Arrives after the big update's backlog, holding nothing.
        waiting.push(wait_in(&small, "small", &order).await);
        assert!(order.lock().is_empty());
        assert_eq!(pool.waiting_runs(), 4);

        first.finish();
        settle().await;
        assert_eq!(*order.lock(), vec!["small"]);
        second.finish();
        settle().await;
        assert_eq!(*order.lock(), vec!["small", "big0"]);
        for (run, task) in waiting {
            joined(task).await;
            run.finish();
        }
        assert_eq!(*order.lock(), vec!["small", "big0", "big1", "big2"]);
        assert_eq!(pool.available_tokens(), Some(2));
        assert_eq!(pool.waiting_runs(), 0);
    }

    #[tokio::test]
    async fn one_update_is_served_in_arrival_order() {
        let pool = bounded(1);
        let update = pool.new_update();
        let holder = holding(&update).await;
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut waiting = Vec::new();
        for name in ["a", "b", "c"] {
            waiting.push(wait_in(&update, name, &order).await);
        }
        holder.finish();
        for (run, task) in waiting {
            joined(task).await;
            run.finish();
        }
        assert_eq!(*order.lock(), vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn updates_holding_as_many_are_served_in_the_order_they_started_waiting() {
        let pool = bounded(1);
        let holder = holding(&pool.new_update()).await;
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut waiting = Vec::new();
        for name in ["first", "second", "third"] {
            waiting.push(wait_in(&pool.new_update(), name, &order).await);
        }
        holder.finish();
        for (run, task) in waiting {
            joined(task).await;
            run.finish();
        }
        assert_eq!(*order.lock(), vec!["first", "second", "third"]);
    }

    #[tokio::test]
    async fn a_live_component_competes_with_everything_below_it() {
        let pool = bounded(4);
        let app = pool.new_update();
        let live_a = app.new_live();
        let live_b = app.new_live();
        let a_cycle = live_a.new_update();
        let a_key = live_a.new_update();
        // A live component that a cycle of `live_a` mounts: its tokens count
        // toward `live_a`.
        let a_inner = a_cycle.new_live().new_update();
        let b_cycle = live_b.new_update();
        let a0 = holding(&a_cycle).await;
        let a1 = holding(&a_cycle).await;
        let a2 = holding(&a_inner).await;
        let b0 = holding(&b_cycle).await;

        let order = Arc::new(Mutex::new(Vec::new()));
        // `a_key` holds nothing and starts waiting first, but its live
        // component holds more than `live_b`.
        let (a_key_run, a_key_task) = wait_in(&a_key, "a_key", &order).await;
        let (b_run, b_task) = wait_in(&b_cycle, "b_cycle", &order).await;

        a0.finish();
        settle().await;
        assert_eq!(*order.lock(), vec!["b_cycle"]);
        a1.finish();
        settle().await;
        assert_eq!(*order.lock(), vec!["b_cycle", "a_key"]);
        joined(b_task).await;
        joined(a_key_task).await;

        for run in [a2, b0] {
            run.finish();
        }
        a_key_run.finish();
        b_run.finish();
        assert_eq!(pool.available_tokens(), Some(4));
        assert_eq!(pool.waiting_runs(), 0);
    }

    #[tokio::test]
    async fn a_waiter_that_gives_up_leaves_the_queue() {
        let pool = bounded(1);
        let holder = holding(&pool.new_update()).await;
        let order = Arc::new(Mutex::new(Vec::new()));
        let (_gone, gone_task) = wait_in(&pool.new_update(), "gone", &order).await;
        let (next, next_task) = wait_in(&pool.new_update(), "next", &order).await;
        assert_eq!(pool.waiting_runs(), 2);

        gone_task.abort();
        let _ = gone_task.await;
        assert_eq!(pool.waiting_runs(), 1);
        holder.finish();
        joined(next_task).await;
        assert_eq!(*order.lock(), vec!["next"]);
        next.finish();
        assert_eq!(pool.available_tokens(), Some(1));
        assert_eq!(pool.waiting_runs(), 0);
    }

    #[tokio::test]
    async fn a_token_granted_to_a_waiter_that_left_goes_to_the_next() {
        let pool = bounded(1);
        let holder = holding(&pool.new_update()).await;
        let leaving = Admission::new(pool.new_update(), None);
        let mut admit = Box::pin(leaving.admit());
        assert!(futures::poll!(&mut admit).is_pending());
        let order = Arc::new(Mutex::new(Vec::new()));
        let (next, next_task) = wait_in(&pool.new_update(), "next", &order).await;

        // The release grants `leaving`, which started waiting first; it
        // drops its wait before it sees the grant.
        holder.finish();
        drop(admit);
        joined(next_task).await;
        assert!(!leaving.is_admitted());
        assert_eq!(pool.available_tokens(), Some(0));
        next.finish();
        assert_eq!(pool.available_tokens(), Some(1));
        assert_eq!(pool.waiting_runs(), 0);
    }

    #[tokio::test]
    async fn a_waiter_served_by_the_loan_leaves_the_pool_queue() {
        let pool = bounded(1);
        let parent = holding(&pool.new_update()).await;
        let lent = child_of(&parent);
        lent.admit().await.unwrap();
        let waiter = Arc::new(child_of(&parent));
        let task = {
            let waiter = waiter.clone();
            tokio::spawn(async move { waiter.admit().await.unwrap() })
        };
        settle().await;
        assert!(!waiter.is_admitted());
        assert_eq!(pool.waiting_runs(), 1);

        lent.finish();
        joined(task).await;
        assert!(waiter.is_admitted());
        assert!(parent.lender().is_lent());
        assert_eq!(pool.waiting_runs(), 0);
        assert_eq!(pool.available_tokens(), Some(0));
        waiter.finish();
        parent.finish();
        assert_eq!(pool.available_tokens(), Some(1));
    }
}

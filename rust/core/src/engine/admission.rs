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

use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use crate::prelude::*;

/// The app's pool of in-flight tokens. Unbounded when created with `None`:
/// every [`Admission`] then admits at once and lends nothing.
#[derive(Clone)]
pub struct AdmissionPool {
    semaphore: Option<Arc<Semaphore>>,
}

impl AdmissionPool {
    pub fn new(max_inflight_components: Option<usize>) -> Self {
        Self {
            semaphore: max_inflight_components.map(|n| Arc::new(Semaphore::new(n))),
        }
    }

    /// Tokens not held by any run; `None` for an unbounded pool.
    pub fn available_tokens(&self) -> Option<usize> {
        self.semaphore.as_ref().map(|s| s.available_permits())
    }
}

/// One in-flight token held by a run.
enum Slot {
    /// A token taken from the pool.
    Pool(#[allow(dead_code)] OwnedSemaphorePermit),
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

/// A run's admission state: its token once admitted, and its [`Lender`].
pub(crate) struct Admission {
    pool: AdmissionPool,
    /// The parent run's lender, if the run has a parent (or, for a live
    /// component's runs, the lender of the run that mounted the live
    /// component).
    parent: Option<Lender>,
    state: Mutex<AdmissionState>,
    lender: Lender,
}

impl Admission {
    pub(crate) fn new(pool: AdmissionPool, parent: Option<Lender>) -> Self {
        Self {
            pool,
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

    /// Take a token: the parent's if it is free, else the first of the
    /// parent's or a pool token to become available. Returns at once when
    /// already admitted or when the pool is unbounded. Cancel-safe.
    pub(crate) async fn admit(&self) -> Result<()> {
        let Some(pool) = &self.pool.semaphore else {
            return Ok(());
        };
        {
            let state = self.state.lock();
            if state.finished {
                internal_bail!("component run already finished");
            }
            if state.slot.is_some() {
                return Ok(());
            }
        }
        let slot = acquire_slot(self.parent.as_ref(), pool).await?;
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
        if self.pool.semaphore.is_none() {
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

async fn acquire_slot(parent: Option<&Lender>, pool: &Arc<Semaphore>) -> Result<Slot> {
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
        Ok(permit) = pool.clone().acquire_owned() => Ok(Slot::Pool(permit)),
        else => Err(internal_error!("in-flight component pool closed")),
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
        let parent = Admission::new(pool.clone(), None);
        parent.admit().await.unwrap();
        assert_eq!(pool.available_tokens(), Some(3));

        let child = Admission::new(pool.clone(), Some(parent.lender().clone()));
        child.admit().await.unwrap();
        assert!(parent.lender().is_lent());
        assert_eq!(
            pool.available_tokens(),
            Some(3),
            "the loan takes no pool token"
        );

        let sibling = Admission::new(pool.clone(), Some(parent.lender().clone()));
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
        let parent = Arc::new(Admission::new(pool.clone(), None));
        parent.admit().await.unwrap();
        let first = Admission::new(pool.clone(), Some(parent.lender().clone()));
        first.admit().await.unwrap();

        let order = Arc::new(Mutex::new(Vec::new()));
        let mut waiters = Vec::new();
        for i in 0..3 {
            let admission = Arc::new(Admission::new(pool.clone(), Some(parent.lender().clone())));
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
        let parent = Admission::new(pool.clone(), None);
        parent.admit().await.unwrap();
        let lent = Admission::new(pool.clone(), Some(parent.lender().clone()));
        lent.admit().await.unwrap();
        let pooled = Admission::new(pool.clone(), Some(parent.lender().clone()));
        pooled.admit().await.unwrap();
        assert_eq!(pool.available_tokens(), Some(0));

        let waiter = Arc::new(Admission::new(pool.clone(), Some(parent.lender().clone())));
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
        let parent = Admission::new(pool.clone(), None);
        parent.admit().await.unwrap();
        let child = Admission::new(pool.clone(), Some(parent.lender().clone()));
        child.admit().await.unwrap();

        parent.finish();
        assert_eq!(
            pool.available_tokens(),
            Some(0),
            "the loan still runs on the token"
        );
        // Nobody borrows from a closed lender.
        let late = Admission::new(pool.clone(), Some(parent.lender().clone()));
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
        let parent = Admission::new(pool.clone(), None);
        parent.admit().await.unwrap();
        let child = Admission::new(pool.clone(), Some(parent.lender().clone()));
        child.admit().await.unwrap();
        let waiter = Arc::new(Admission::new(pool.clone(), Some(parent.lender().clone())));
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
        let run = Admission::new(pool.clone(), None);
        run.admit().await.unwrap();
        run.admit().await.unwrap();
        assert_eq!(pool.available_tokens(), Some(0));
        drop(run);
        assert_eq!(pool.available_tokens(), Some(1), "drop releases");

        let unbounded = AdmissionPool::new(None);
        let run = Admission::new(unbounded.clone(), None);
        run.admit().await.unwrap();
        assert!(!run.is_admitted());
        assert_eq!(unbounded.available_tokens(), None);
    }

    #[tokio::test]
    async fn a_cancelled_wait_passes_its_wake_up_on() {
        let pool = bounded(1);
        let parent = Admission::new(pool.clone(), None);
        parent.admit().await.unwrap();
        let child = Admission::new(pool.clone(), Some(parent.lender().clone()));
        child.admit().await.unwrap();

        let abandoned = Admission::new(pool.clone(), Some(parent.lender().clone()));
        let abandoned_task = tokio::spawn(async move { abandoned.admit().await });
        settle().await;
        let waiter = Arc::new(Admission::new(pool.clone(), Some(parent.lender().clone())));
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
}

//! Cooperative deadline state shared by all SDKs.
//!
//! [`DeadlineContext`] is intentionally just an immutable, copyable handle:
//! an absolute monotonic [`Instant`], or [`DeadlineContext::NONE`]. Lexical
//! scoping belongs in SDK carriers (`ContextVar`, task-local context,
//! `AsyncLocalStorage`, etc.); the core only receives and checks the value
//! at engine checkpoints — it is never stored on engine contexts.
//!
//! Tests use a process-global virtual clock. Conformance tests that advance
//! this clock must run sequentially because every handle observes the same
//! clock.

use std::sync::{
    OnceLock,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use crate::prelude::*;

static MONOTONIC_ANCHOR: OnceLock<Instant> = OnceLock::new();
static TEST_OFFSET_NS: AtomicU64 = AtomicU64::new(0);
static TEST_CLOCK_ENABLED: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static TEST_CLOCK_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Immutable deadline handle carried through engine calls.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct DeadlineContext(Option<Instant>);

impl DeadlineContext {
    /// No active deadline.
    pub const NONE: Self = Self(None);

    pub const fn has_deadline(self) -> bool {
        self.0.is_some()
    }

    /// Return a context with `timeout` applied, preserving the earlier
    /// existing deadline when nested.
    pub fn with_timeout(self, timeout: Duration) -> Self {
        let now = now_instant();
        let deadline = checked_add_saturating(now, timeout);
        Self(Some(match self.0 {
            Some(existing) => existing.min(deadline),
            None => deadline,
        }))
    }

    /// Raise a structured deadline error if this context has expired.
    pub fn check(self) -> Result<()> {
        if self.is_expired() {
            return Err(Error::deadline_exceeded());
        }
        Ok(())
    }

    pub fn remaining(self) -> Option<Duration> {
        self.0
            .map(|deadline| deadline.saturating_duration_since(now_instant()))
    }

    pub fn is_expired(self) -> bool {
        matches!(self.0, Some(deadline) if now_instant() > deadline)
    }
}

fn now_instant() -> Instant {
    let anchor = *MONOTONIC_ANCHOR.get_or_init(Instant::now);
    if TEST_CLOCK_ENABLED.load(Ordering::Relaxed) {
        // Fully virtual in test mode: the anchor plus the test offset, with
        // no real-clock component, so conformance tests are deterministic.
        return checked_add_saturating(
            anchor,
            Duration::from_nanos(TEST_OFFSET_NS.load(Ordering::Relaxed)),
        );
    }
    Instant::now()
}

fn checked_add_saturating(anchor: Instant, duration: Duration) -> Instant {
    if let Some(deadline) = anchor.checked_add(duration) {
        return deadline;
    }

    // `Instant` has platform-dependent range. If the requested duration
    // overflows it, keep halving until we find the farthest representable
    // future instant for this platform instead of panicking.
    let mut fallback = duration;
    loop {
        fallback = Duration::new(fallback.as_secs() / 2, fallback.subsec_nanos() / 2);
        if fallback.is_zero() {
            return anchor;
        }
        if let Some(deadline) = anchor.checked_add(fallback) {
            return deadline;
        }
    }
}

/// Enable the deterministic test clock and reset it to zero.
pub fn testing_reset_deadline_clock() {
    // Initialize the anchor before enabling, so virtual time never observes
    // an anchor newer than a previously computed deadline.
    MONOTONIC_ANCHOR.get_or_init(Instant::now);
    TEST_OFFSET_NS.store(0, Ordering::Relaxed);
    TEST_CLOCK_ENABLED.store(true, Ordering::Relaxed);
}

/// Disable the deterministic test clock and reset any offset.
pub fn testing_disable_deadline_clock() {
    TEST_CLOCK_ENABLED.store(false, Ordering::Relaxed);
    TEST_OFFSET_NS.store(0, Ordering::Relaxed);
}

/// Advance the deterministic test clock.
pub fn testing_advance_deadline_clock(duration: Duration) {
    let delta_ns = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
    let mut current = TEST_OFFSET_NS.load(Ordering::Relaxed);
    loop {
        let next = current.saturating_add(delta_ns);
        match TEST_OFFSET_NS.compare_exchange_weak(
            current,
            next,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

#[cfg(test)]
pub(crate) fn testing_deadline_clock_lock() -> std::sync::MutexGuard<'static, ()> {
    TEST_CLOCK_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestClockGuard {
        _guard: std::sync::MutexGuard<'static, ()>,
    }

    impl TestClockGuard {
        fn new() -> Self {
            let guard = testing_deadline_clock_lock();
            testing_reset_deadline_clock();
            Self { _guard: guard }
        }
    }

    impl Drop for TestClockGuard {
        fn drop(&mut self) {
            testing_disable_deadline_clock();
        }
    }

    #[test]
    fn none_never_expires() {
        let _clock = TestClockGuard::new();
        assert!(!DeadlineContext::NONE.has_deadline());
        assert!(!DeadlineContext::NONE.is_expired());
        DeadlineContext::NONE.check().unwrap();
        assert_eq!(DeadlineContext::NONE.remaining(), None);
    }

    #[test]
    fn nested_deadline_uses_min_without_mutating_parent() {
        let _clock = TestClockGuard::new();
        let outer = DeadlineContext::NONE.with_timeout(Duration::from_secs(10));
        let wider = outer.with_timeout(Duration::from_secs(20));
        assert_eq!(wider, outer);

        testing_advance_deadline_clock(Duration::from_secs(5));
        let narrower = outer.with_timeout(Duration::from_secs(1));
        assert_eq!(outer.remaining(), Some(Duration::from_secs(5)));
        assert_eq!(narrower.remaining(), Some(Duration::from_secs(1)));
    }

    #[test]
    fn deadline_expires_only_after_timestamp() {
        let _clock = TestClockGuard::new();
        let deadline = DeadlineContext::NONE.with_timeout(Duration::from_secs(10));

        testing_advance_deadline_clock(Duration::from_secs(10));
        assert!(!deadline.is_expired());
        deadline.check().unwrap();

        testing_advance_deadline_clock(Duration::from_nanos(1));
        assert!(deadline.is_expired());
        assert!(deadline.check().unwrap_err().is_deadline_exceeded());
    }

    #[test]
    fn duration_max_saturates_to_far_future() {
        let _clock = TestClockGuard::new();
        let deadline = DeadlineContext::NONE.with_timeout(Duration::MAX);
        assert!(deadline.has_deadline());
        assert!(!deadline.is_expired());
        deadline.check().unwrap();
    }

    #[test]
    fn huge_virtual_clock_offset_does_not_panic() {
        let _clock = TestClockGuard::new();
        testing_advance_deadline_clock(Duration::from_nanos(u64::MAX));

        let deadline = DeadlineContext::NONE.with_timeout(Duration::MAX);
        assert!(deadline.has_deadline());
    }
}

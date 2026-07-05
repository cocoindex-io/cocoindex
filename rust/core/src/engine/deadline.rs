//! Cooperative deadline state shared by all SDKs.
//!
//! [`DeadlineContext`] is intentionally just an immutable, copyable handle:
//! an absolute monotonic-clock timestamp in nanoseconds, or [`DeadlineContext::NONE`].
//! Lexical scoping belongs in SDK carriers (`ContextVar`, task-local context,
//! `AsyncLocalStorage`, etc.); the core only receives and checks the 8-byte
//! value at engine checkpoints.
//!
//! Tests use a process-global clock offset. Conformance tests that advance this
//! clock must run sequentially because every SDK handle observes the same clock.

use std::sync::{
    OnceLock,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use crate::prelude::*;

const NO_DEADLINE_NS: u64 = u64::MAX;
const MAX_DEADLINE_NS: u64 = u64::MAX - 1;

static MONOTONIC_ANCHOR: OnceLock<Instant> = OnceLock::new();
static TEST_OFFSET_NS: AtomicU64 = AtomicU64::new(0);
static TEST_CLOCK_ENABLED: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static TEST_CLOCK_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Immutable deadline handle carried through engine contexts.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct DeadlineContext(u64);

const _: () = assert!(std::mem::size_of::<DeadlineContext>() == 8);

impl DeadlineContext {
    /// No active deadline.
    pub const NONE: Self = Self(NO_DEADLINE_NS);

    /// Recreate a handle from its raw monotonic-nanosecond representation.
    ///
    /// This is used only by FFI wrappers that need to carry the opaque value.
    pub const fn from_raw_ns(raw_ns: u64) -> Self {
        Self(raw_ns)
    }

    /// Return the raw monotonic-nanosecond representation.
    pub const fn raw_ns(self) -> u64 {
        self.0
    }

    pub const fn has_deadline(self) -> bool {
        self.0 != NO_DEADLINE_NS
    }

    /// Return a context with `timeout` applied, preserving the earlier
    /// existing deadline when nested.
    pub fn with_timeout(self, timeout: Duration) -> Self {
        let timeout_ns = duration_to_ns(timeout);
        let deadline = monotonic_ns()
            .saturating_add(timeout_ns)
            .min(MAX_DEADLINE_NS);
        if self.has_deadline() {
            Self(self.0.min(deadline))
        } else {
            Self(deadline)
        }
    }

    /// Raise a structured deadline error if this context has expired.
    pub fn check(self) -> Result<()> {
        if !self.has_deadline() {
            return Ok(());
        }
        if monotonic_ns() > self.0 {
            return Err(Error::deadline_exceeded());
        }
        Ok(())
    }

    pub fn remaining(self) -> Option<Duration> {
        if !self.has_deadline() {
            return None;
        }
        let now = monotonic_ns();
        let remaining_ns = self.0.saturating_sub(now);
        Some(Duration::from_nanos(remaining_ns))
    }

    pub fn is_expired(self) -> bool {
        self.has_deadline() && monotonic_ns() > self.0
    }
}

fn duration_to_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(MAX_DEADLINE_NS as u128) as u64
}

fn monotonic_ns() -> u64 {
    if TEST_CLOCK_ENABLED.load(Ordering::Relaxed) {
        return TEST_OFFSET_NS.load(Ordering::Relaxed).min(MAX_DEADLINE_NS);
    }
    let elapsed = MONOTONIC_ANCHOR
        .get_or_init(Instant::now)
        .elapsed()
        .as_nanos()
        .min(MAX_DEADLINE_NS as u128) as u64;
    elapsed
        .saturating_add(TEST_OFFSET_NS.load(Ordering::Relaxed))
        .min(MAX_DEADLINE_NS)
}

/// Enable the deterministic test clock and reset it to zero.
pub fn testing_reset_deadline_clock() {
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
    let delta_ns = duration_to_ns(duration);
    let mut current = TEST_OFFSET_NS.load(Ordering::Relaxed);
    loop {
        let next = current.saturating_add(delta_ns).min(MAX_DEADLINE_NS);
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
    fn duration_max_cannot_create_none_sentinel() {
        let _clock = TestClockGuard::new();
        let deadline = DeadlineContext::NONE.with_timeout(Duration::MAX);
        assert!(deadline.has_deadline());
        assert_eq!(deadline.raw_ns(), MAX_DEADLINE_NS);
    }
}

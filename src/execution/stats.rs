use crate::prelude::*;

use std::{
    ops::AddAssign,
    sync::atomic::{AtomicI64, Ordering::Relaxed},
};

#[derive(Default, Serialize)]
pub struct Counter(pub AtomicI64);

impl Counter {
    pub fn inc(&self, by: i64) {
        self.0.fetch_add(by, Relaxed);
    }

    pub fn get(&self) -> i64 {
        self.0.load(Relaxed)
    }

    pub fn delta(&self, base: &Self) -> Counter {
        Counter(AtomicI64::new(self.get() - base.get()))
    }

    pub fn into_inner(self) -> i64 {
        self.0.into_inner()
    }

    pub fn merge(&self, delta: &Self) {
        self.0.fetch_add(delta.get(), Relaxed);
    }
}

impl AddAssign for Counter {
    fn add_assign(&mut self, rhs: Self) {
        self.0.fetch_add(rhs.into_inner(), Relaxed);
    }
}

impl Clone for Counter {
    fn clone(&self) -> Self {
        Self(AtomicI64::new(self.get()))
    }
}

impl std::fmt::Display for Counter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get())
    }
}

impl std::fmt::Debug for Counter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get())
    }
}

#[derive(Debug, Serialize, Default, Clone)]
pub struct UpdateStats {
    pub num_no_change: Counter,
    pub num_insertions: Counter,
    pub num_deletions: Counter,
    /// Number of source rows that were updated.
    pub num_updates: Counter,
    /// Number of source rows that were reprocessed because of logic change.
    pub num_reprocesses: Counter,
    pub num_errors: Counter,
}

impl UpdateStats {
    pub fn delta(&self, base: &Self) -> Self {
        UpdateStats {
            num_no_change: self.num_no_change.delta(&base.num_no_change),
            num_insertions: self.num_insertions.delta(&base.num_insertions),
            num_deletions: self.num_deletions.delta(&base.num_deletions),
            num_updates: self.num_updates.delta(&base.num_updates),
            num_reprocesses: self.num_reprocesses.delta(&base.num_reprocesses),
            num_errors: self.num_errors.delta(&base.num_errors),
        }
    }

    pub fn merge(&self, delta: &Self) {
        self.num_no_change.merge(&delta.num_no_change);
        self.num_insertions.merge(&delta.num_insertions);
        self.num_deletions.merge(&delta.num_deletions);
        self.num_updates.merge(&delta.num_updates);
        self.num_reprocesses.merge(&delta.num_reprocesses);
        self.num_errors.merge(&delta.num_errors);
    }

    pub fn has_any_change(&self) -> bool {
        self.num_insertions.get() > 0
            || self.num_deletions.get() > 0
            || self.num_updates.get() > 0
            || self.num_reprocesses.get() > 0
            || self.num_errors.get() > 0
    }
}

impl std::fmt::Display for UpdateStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut messages = Vec::new();
        let num_errors = self.num_errors.get();
        if num_errors > 0 {
            messages.push(format!("{num_errors} source rows FAILED"));
        }

        let num_skipped = self.num_no_change.get();
        if num_skipped > 0 {
            messages.push(format!("{num_skipped} source rows NO CHANGE"));
        }

        let num_insertions = self.num_insertions.get();
        let num_deletions = self.num_deletions.get();
        let num_updates = self.num_updates.get();
        let num_reprocesses = self.num_reprocesses.get();
        let num_source_rows = num_insertions + num_deletions + num_updates + num_reprocesses;
        if num_source_rows > 0 {
            let mut sub_messages = Vec::new();
            if num_insertions > 0 {
                sub_messages.push(format!("{num_insertions} ADDED"));
            }
            if num_deletions > 0 {
                sub_messages.push(format!("{num_deletions} REMOVED"));
            }
            if num_reprocesses > 0 {
                sub_messages.push(format!(
                    "{num_reprocesses} REPROCESSED on flow/logic changes or reexport"
                ));
            }
            if num_updates > 0 {
                sub_messages.push(format!("{num_updates} UPDATED in source content only"));
            }
            messages.push(format!(
                "{num_source_rows} source rows processed ({})",
                sub_messages.join(", "),
            ));
        }

        if !messages.is_empty() {
            write!(f, "{}", messages.join("; "))?;
        } else {
            write!(f, "No changes")?;
        }

        Ok(())
    }
}

#[derive(Debug, Serialize)]
pub struct SourceUpdateInfo {
    pub source_name: String,
    pub stats: UpdateStats,
}

impl std::fmt::Display for SourceUpdateInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.source_name, self.stats)
    }
}

#[derive(Debug, Serialize)]
pub struct IndexUpdateInfo {
    pub sources: Vec<SourceUpdateInfo>,
}

impl std::fmt::Display for IndexUpdateInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for source in self.sources.iter() {
            writeln!(f, "{source}")?;
        }
        Ok(())
    }
}

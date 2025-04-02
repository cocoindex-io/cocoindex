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
    pub num_skipped: Counter,
    pub num_insertions: Counter,
    pub num_deletions: Counter,
    pub num_repreocesses: Counter,
    pub num_errors: Counter,
}

impl UpdateStats {
    pub fn delta(&self, base: &Self) -> Self {
        UpdateStats {
            num_skipped: self.num_skipped.delta(&base.num_skipped),
            num_insertions: self.num_insertions.delta(&base.num_insertions),
            num_deletions: self.num_deletions.delta(&base.num_deletions),
            num_repreocesses: self.num_repreocesses.delta(&base.num_repreocesses),
            num_errors: self.num_errors.delta(&base.num_errors),
        }
    }

    pub fn is_zero(&self) -> bool {
        self.num_skipped.get() == 0
            && self.num_insertions.get() == 0
            && self.num_deletions.get() == 0
            && self.num_repreocesses.get() == 0
            && self.num_errors.get() == 0
    }
}

impl std::fmt::Display for UpdateStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let num_skipped = self.num_skipped.get();
        if num_skipped > 0 {
            write!(f, "{} rows skipped", num_skipped)?;
        }

        let num_insertions = self.num_insertions.get();
        let num_deletions = self.num_deletions.get();
        let num_reprocesses = self.num_repreocesses.get();
        let num_source_rows = num_insertions + num_deletions + num_reprocesses;
        if num_source_rows > 0 {
            if num_skipped > 0 {
                write!(f, "; ")?;
            }
            write!(f, "{num_source_rows} source rows processed",)?;

            let num_errors = self.num_errors.get();
            if num_errors > 0 {
                write!(f, " with {num_errors} ERRORS",)?;
            }
            write!(
                f,
                ": {num_insertions} added, {num_deletions} removed, {num_reprocesses} repocessed",
            )?;
        }

        if num_skipped == 0 && num_source_rows == 0 {
            write!(f, "no changes")?;
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
            writeln!(f, "{}", source)?;
        }
        Ok(())
    }
}

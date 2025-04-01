use crate::prelude::*;

use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

#[derive(Debug, Serialize, Default)]
pub struct UpdateStats {
    pub num_skipped: AtomicUsize,
    pub num_insertions: AtomicUsize,
    pub num_deletions: AtomicUsize,
    pub num_repreocesses: AtomicUsize,
    pub num_errors: AtomicUsize,
}

impl Clone for UpdateStats {
    fn clone(&self) -> Self {
        Self {
            num_skipped: self.num_skipped.load(Relaxed).into(),
            num_insertions: self.num_insertions.load(Relaxed).into(),
            num_deletions: self.num_deletions.load(Relaxed).into(),
            num_repreocesses: self.num_repreocesses.load(Relaxed).into(),
            num_errors: self.num_errors.load(Relaxed).into(),
        }
    }
}

impl std::fmt::Display for UpdateStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let num_skipped = self.num_skipped.load(Relaxed);
        if num_skipped > 0 {
            write!(f, "{} rows skipped", num_skipped)?;
        }

        let num_insertions = self.num_insertions.load(Relaxed);
        let num_deletions = self.num_deletions.load(Relaxed);
        let num_reprocesses = self.num_repreocesses.load(Relaxed);
        let num_source_rows = num_insertions + num_deletions + num_reprocesses;
        if num_source_rows > 0 {
            if num_skipped > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{num_source_rows} source rows processed",)?;

            let num_errors = self.num_errors.load(Relaxed);
            if num_errors > 0 {
                write!(f, " with {num_errors} ERRORS",)?;
            }
            write!(
                f,
                ": {num_insertions} added, {num_deletions} removed, {num_reprocesses} repocessed",
            )?;
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

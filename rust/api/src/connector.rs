//! Source and Target connector traits.

use crate::error::Result;

/// Source: provides items to process.
pub trait Source {
    type Item;
    fn items(&self) -> Result<Vec<Self::Item>>;
}

/// Target: accepts declared state. CocoIndex handles reconciliation.
pub trait Target: Clone + Send + Sync + 'static {
    type Row: serde::Serialize;
    fn declare(&self, key: &str, row: &Self::Row) -> Result<()>;
}

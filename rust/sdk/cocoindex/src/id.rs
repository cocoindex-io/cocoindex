//! Stable ID generation utilities.

use std::collections::HashMap;

use cocoindex_utils::fingerprint::Fingerprint;
use serde::Serialize;
use uuid::Uuid;

use crate::ctx::Ctx;
use crate::error::{Error, Result};

/// Generate a stable unique ID for a dependency value.
///
/// Returns the same ID for the same `dep` value within a processing component.
/// Use [`IdGenerator`] when repeated identical inputs need distinct IDs.
pub async fn generate_id<D: Serialize + ?Sized>(ctx: &Ctx, dep: &D) -> Result<u64> {
    let dep_fp = memo_fingerprint(dep)?;
    let key = ("cocoindex_generate_id", dep_fp);
    ctx.memo(&key, |ctx| async move { ctx.next_raw_id().await })
        .await
}

/// Generate a stable unique ID with no dependency value.
pub async fn generate_id_default(ctx: &Ctx) -> Result<u64> {
    generate_id(ctx, &()).await
}

/// Generate a stable unique UUID for a dependency value.
///
/// Returns the same UUID for the same `dep` value within a processing component.
/// Use [`UuidGenerator`] when repeated identical inputs need distinct UUIDs.
pub async fn generate_uuid<D: Serialize + ?Sized>(ctx: &Ctx, dep: &D) -> Result<Uuid> {
    let dep_fp = memo_fingerprint(dep)?;
    let key = ("cocoindex_generate_uuid", dep_fp);
    ctx.memo(&key, |_ctx| async move { Ok(Uuid::new_v4()) })
        .await
}

/// Generate a stable unique UUID with no dependency value.
pub async fn generate_uuid_default(ctx: &Ctx) -> Result<Uuid> {
    generate_uuid(ctx, &()).await
}

/// Generator for stable unique IDs.
///
/// Repeated calls with the same dependency return distinct IDs in deterministic
/// order. IDs stay stable across runs because allocation is memoized by
/// `(generator deps, dep, occurrence ordinal)`.
#[derive(Debug, Clone)]
pub struct IdGenerator {
    deps_fp: Fingerprint,
    ordinals: HashMap<Fingerprint, u64>,
}

impl Default for IdGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl IdGenerator {
    /// Create a generator with no constructor dependency.
    pub fn new() -> Self {
        Self {
            deps_fp: Fingerprint::from(&()).expect("unit fingerprint should be infallible"),
            ordinals: HashMap::new(),
        }
    }

    /// Create a generator scoped by constructor dependencies.
    ///
    /// Use this when multiple generators in the same component should maintain
    /// independent stable sequences.
    pub fn with_deps<D: Serialize + ?Sized>(deps: &D) -> Result<Self> {
        Ok(Self {
            deps_fp: memo_fingerprint(deps)?,
            ordinals: HashMap::new(),
        })
    }

    /// Generate the next stable ID for `dep`.
    ///
    /// Multiple calls with the same `dep` produce distinct IDs by folding in an
    /// occurrence ordinal. The same call sequence in the same component returns
    /// the same IDs on later runs.
    pub async fn next_id<D: Serialize + ?Sized>(&mut self, ctx: &Ctx, dep: &D) -> Result<u64> {
        let dep_fp = memo_fingerprint(dep)?;
        let ordinal = self.ordinals.entry(dep_fp).or_insert(0);
        let current_ordinal = *ordinal;
        *ordinal += 1;

        let deps_fp = self.deps_fp;
        let key = ("cocoindex_id_generator", deps_fp, dep_fp, current_ordinal);
        ctx.memo(&key, |ctx| async move { ctx.next_raw_id().await })
            .await
    }

    /// Generate the next stable ID with no per-call dependency.
    pub async fn next_id_default(&mut self, ctx: &Ctx) -> Result<u64> {
        self.next_id(ctx, &()).await
    }
}

/// Generator for stable unique UUIDs.
///
/// Repeated calls with the same dependency return distinct UUIDs in
/// deterministic order.
#[derive(Debug, Clone)]
pub struct UuidGenerator {
    deps_fp: Fingerprint,
    ordinals: HashMap<Fingerprint, u64>,
}

impl Default for UuidGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl UuidGenerator {
    /// Create a generator with no constructor dependency.
    pub fn new() -> Self {
        Self {
            deps_fp: Fingerprint::from(&()).expect("unit fingerprint should be infallible"),
            ordinals: HashMap::new(),
        }
    }

    /// Create a generator scoped by constructor dependencies.
    ///
    /// Use this when multiple generators in the same component should maintain
    /// independent stable sequences.
    pub fn with_deps<D: Serialize + ?Sized>(deps: &D) -> Result<Self> {
        Ok(Self {
            deps_fp: memo_fingerprint(deps)?,
            ordinals: HashMap::new(),
        })
    }

    /// Generate the next stable UUID for `dep`.
    ///
    /// Multiple calls with the same `dep` produce distinct UUIDs by folding in
    /// an occurrence ordinal. The same call sequence in the same component
    /// returns the same UUIDs on later runs.
    pub async fn next_uuid<D: Serialize + ?Sized>(&mut self, ctx: &Ctx, dep: &D) -> Result<Uuid> {
        let dep_fp = memo_fingerprint(dep)?;
        let ordinal = self.ordinals.entry(dep_fp).or_insert(0);
        let current_ordinal = *ordinal;
        *ordinal += 1;

        let deps_fp = self.deps_fp;
        let key = ("cocoindex_uuid_generator", deps_fp, dep_fp, current_ordinal);
        ctx.memo(&key, |_ctx| async move { Ok(Uuid::new_v4()) })
            .await
    }

    /// Generate the next stable UUID with no per-call dependency.
    pub async fn next_uuid_default(&mut self, ctx: &Ctx) -> Result<Uuid> {
        self.next_uuid(ctx, &()).await
    }
}

fn memo_fingerprint<T: Serialize + ?Sized>(value: &T) -> Result<Fingerprint> {
    Fingerprint::from(value)
        .map_err(|e| Error::engine(format!("id dependency fingerprint error: {e}")))
}

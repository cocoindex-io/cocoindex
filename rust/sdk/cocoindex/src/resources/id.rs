//! Stable ID generation utilities.

use std::collections::HashMap;
use std::sync::Mutex;

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
///
/// Takes `&self`, so one generator can be shared by the concurrent calls of a
/// `ctx.map` (as Python's is). Not `Clone`: a copy would replay the same
/// ordinals and hand out the same IDs again. Create one generator per scope
/// that needs distinct IDs.
#[derive(Debug)]
pub struct IdGenerator {
    deps_fp: Fingerprint,
    ordinals: Mutex<HashMap<Fingerprint, u64>>,
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
            ordinals: Mutex::new(HashMap::new()),
        }
    }

    /// Create a generator scoped by constructor dependencies.
    ///
    /// Use this when multiple generators in the same component should maintain
    /// independent stable sequences.
    pub fn with_deps<D: Serialize + ?Sized>(deps: &D) -> Result<Self> {
        Ok(Self {
            deps_fp: memo_fingerprint(deps)?,
            ordinals: Mutex::new(HashMap::new()),
        })
    }

    /// Generate the next stable ID for `dep`.
    ///
    /// Multiple calls with the same `dep` produce distinct IDs by folding in an
    /// occurrence ordinal. The same call sequence in the same component returns
    /// the same IDs on later runs.
    pub async fn next_id<D: Serialize + ?Sized>(&self, ctx: &Ctx, dep: &D) -> Result<u64> {
        let dep_fp = memo_fingerprint(dep)?;
        let current_ordinal = next_ordinal(&self.ordinals, dep_fp);

        let deps_fp = self.deps_fp;
        let key = ("cocoindex_id_generator", deps_fp, dep_fp, current_ordinal);
        ctx.memo(&key, |ctx| async move { ctx.next_raw_id().await })
            .await
    }

    /// Generate the next stable ID with no per-call dependency.
    pub async fn next_id_default(&self, ctx: &Ctx) -> Result<u64> {
        self.next_id(ctx, &()).await
    }
}

/// Generator for stable unique UUIDs.
///
/// Repeated calls with the same dependency return distinct UUIDs in
/// deterministic order.
///
/// Takes `&self` and is not `Clone`, for the same reasons as [`IdGenerator`].
#[derive(Debug)]
pub struct UuidGenerator {
    deps_fp: Fingerprint,
    ordinals: Mutex<HashMap<Fingerprint, u64>>,
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
            ordinals: Mutex::new(HashMap::new()),
        }
    }

    /// Create a generator scoped by constructor dependencies.
    ///
    /// Use this when multiple generators in the same component should maintain
    /// independent stable sequences.
    pub fn with_deps<D: Serialize + ?Sized>(deps: &D) -> Result<Self> {
        Ok(Self {
            deps_fp: memo_fingerprint(deps)?,
            ordinals: Mutex::new(HashMap::new()),
        })
    }

    /// Generate the next stable UUID for `dep`.
    ///
    /// Multiple calls with the same `dep` produce distinct UUIDs by folding in
    /// an occurrence ordinal. The same call sequence in the same component
    /// returns the same UUIDs on later runs.
    pub async fn next_uuid<D: Serialize + ?Sized>(&self, ctx: &Ctx, dep: &D) -> Result<Uuid> {
        let dep_fp = memo_fingerprint(dep)?;
        let current_ordinal = next_ordinal(&self.ordinals, dep_fp);

        let deps_fp = self.deps_fp;
        let key = ("cocoindex_uuid_generator", deps_fp, dep_fp, current_ordinal);
        ctx.memo(&key, |_ctx| async move { Ok(Uuid::new_v4()) })
            .await
    }

    /// Generate the next stable UUID with no per-call dependency.
    pub async fn next_uuid_default(&self, ctx: &Ctx) -> Result<Uuid> {
        self.next_uuid(ctx, &()).await
    }
}

/// The occurrence ordinal of `dep_fp` so far, then advance it. Synchronous,
/// so concurrent callers get distinct ordinals in the order they reach it.
fn next_ordinal(ordinals: &Mutex<HashMap<Fingerprint, u64>>, dep_fp: Fingerprint) -> u64 {
    let mut ordinals = ordinals.lock().unwrap_or_else(|e| e.into_inner());
    let ordinal = ordinals.entry(dep_fp).or_insert(0);
    let current = *ordinal;
    *ordinal += 1;
    current
}

fn memo_fingerprint<T: Serialize + ?Sized>(value: &T) -> Result<Fingerprint> {
    Fingerprint::from(value)
        .map_err(|e| Error::engine(format!("id dependency fingerprint error: {e}")))
}

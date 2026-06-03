//! Vector schema resources.
//!
//! Mirrors Python's `cocoindex.resources.schema`: [`VectorSchema`],
//! [`VectorSchemaProvider`], [`MultiVectorSchema`], and
//! [`MultiVectorSchemaProvider`]. Connectors that need out-of-band vector
//! metadata (dimension and element type) beyond the static Rust type accept a
//! [`VectorSchemaProvider`]; embedders implement it so a column definition can
//! be derived without the caller hard-coding the dimension.
//!
//! Python carries the element type as a NumPy `dtype`; Rust uses the
//! [`VectorElementType`] enum, which covers the element types the connectors
//! support today (`f32` and `f16`).

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::Result;

/// The element type stored in a vector column.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum VectorElementType {
    /// 32-bit IEEE-754 float (`f32`).
    Float32,
    /// 16-bit IEEE-754 float (`f16` / half precision).
    Float16,
}

/// Out-of-band information for a vector column: its element type and dimension.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorSchema {
    /// Element type of each component of the vector.
    pub element_type: VectorElementType,
    /// Number of components (the embedding dimension).
    pub size: usize,
}

impl VectorSchema {
    /// Convenience constructor for an `f32` vector of the given dimension.
    pub fn f32(size: usize) -> Self {
        Self {
            element_type: VectorElementType::Float32,
            size,
        }
    }

    /// Convenience constructor for an `f16` (half-precision) vector of the
    /// given dimension.
    pub fn f16(size: usize) -> Self {
        Self {
            element_type: VectorElementType::Float16,
            size,
        }
    }
}

/// Something that can describe the vector column it produces — typically an
/// embedder. Implemented by [`VectorSchema`] itself so a fixed schema can be
/// passed wherever a provider is expected.
#[async_trait]
pub trait VectorSchemaProvider: Send + Sync {
    /// Resolve the vector schema (may perform I/O, e.g. a probe embedding to
    /// discover the dimension).
    async fn vector_schema(&self) -> Result<VectorSchema>;
}

#[async_trait]
impl VectorSchemaProvider for VectorSchema {
    async fn vector_schema(&self) -> Result<VectorSchema> {
        Ok(*self)
    }
}

/// Out-of-band information for a multi-vector column (a list of vectors that
/// share one underlying [`VectorSchema`], e.g. ColPali-style late-interaction
/// embeddings).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiVectorSchema {
    /// The schema each vector in the list conforms to.
    pub vector_schema: VectorSchema,
}

/// Something that can describe the multi-vector column it produces.
/// Implemented by [`MultiVectorSchema`] itself.
#[async_trait]
pub trait MultiVectorSchemaProvider: Send + Sync {
    /// Resolve the multi-vector schema.
    async fn multi_vector_schema(&self) -> Result<MultiVectorSchema>;
}

#[async_trait]
impl MultiVectorSchemaProvider for MultiVectorSchema {
    async fn multi_vector_schema(&self) -> Result<MultiVectorSchema> {
        Ok(*self)
    }
}

pub use super::factory_bases::*;
pub use super::interface::*;
pub use crate::base::schema::*;
pub use crate::base::value::*;
pub use anyhow::Result;
pub use axum::async_trait;
pub use serde::Deserialize;

/// Defined for all types convertible to ValueType, to ease creation for ValueType in various operation factories.
pub trait TypeCore {
    fn into_type(self) -> ValueType;
}

impl TypeCore for BasicValueType {
    fn into_type(self) -> ValueType {
        ValueType::Basic(self)
    }
}

impl TypeCore for StructSchema {
    fn into_type(self) -> ValueType {
        ValueType::Struct(self)
    }
}

impl TypeCore for CollectionSchema {
    fn into_type(self) -> ValueType {
        ValueType::Collection(self)
    }
}

pub fn make_output_type<Type: TypeCore>(value_type: Type) -> EnrichedValueType {
    EnrichedValueType {
        typ: value_type.into_type(),
        attrs: Default::default(),
        nullable: false,
    }
}

#[derive(Debug, Deserialize)]
pub struct EmptySpec {}

#[macro_export]
macro_rules! fields_value {
    ($($field:expr), +) => {
        $crate::base::value::FieldValues { fields: std::vec![ $(($field).into()),+ ] }
    };
}

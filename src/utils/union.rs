use crate::{base::schema::BasicValueType, prelude::*};

/// Union type helper storing an auto-sorted set of types excluding `Union`
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct UnionType {
    types: Vec<BasicValueType>,
}

impl UnionType {
    pub fn types(&self) -> &[BasicValueType] {
        self.types.as_slice()
    }
}

impl Into<Vec<BasicValueType>> for UnionType {
    fn into(self) -> Vec<BasicValueType> {
        self.types
    }
}

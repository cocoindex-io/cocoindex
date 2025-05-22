use crate::{base::schema::BasicValueType, prelude::*};

/// Union type helper storing an auto-sorted set of types excluding `Union`
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct UnionType {
    types: BTreeSet<BasicValueType>,
}

impl UnionType {
    pub fn types(&self) -> &BTreeSet<BasicValueType> {
        &self.types
    }
}

impl Into<BTreeSet<BasicValueType>> for UnionType {
    fn into(self) -> BTreeSet<BasicValueType> {
        self.types
    }
}

use std::{str::FromStr, sync::Arc};

use crate::{base::{schema::BasicValueType, value::BasicValue}, prelude::*};

/// Union type helper storing an auto-sorted set of types excluding `Union`
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct UnionType {
    types: BTreeSet<BasicValueType>,
}

impl UnionType {
    pub fn types(&self) -> &BTreeSet<BasicValueType> {
        &self.types
    }

    pub fn insert(&mut self, value: BasicValueType) -> bool {
        match value {
            BasicValueType::Union(union_type) => {
                let mut inserted = false;

                // Unpack nested union
                for item in union_type.types.into_iter() {
                    // Recursively insert underlying types
                    inserted = self.insert(item) || inserted;
                }

                inserted
            }

            other => self.types.insert(other),
        }
    }

    pub fn unpack(self) -> Self {
        self.types.into()
    }
}

impl From<Vec<BasicValueType>> for UnionType {
    fn from(value: Vec<BasicValueType>) -> Self {
        let mut union = Self::default();

        for typ in value {
            union.insert(typ);
        }

        union
    }
}

impl From<BTreeSet<BasicValueType>> for UnionType {
    fn from(value: BTreeSet<BasicValueType>) -> Self {
        let mut union = Self::default();

        for typ in value {
            union.insert(typ);
        }

        union
    }
}

pub trait ParseStr {
    type Out;
    type Err;

    fn parse_str(&self, value: &str) -> Result<Self::Out, Self::Err>;
}

impl ParseStr for BTreeSet<BasicValueType> {
    type Out = BasicValue;
    type Err = anyhow::Error;

    /// Try parsing the str value to each possible type, and return the first successful result
    fn parse_str(&self, value: &str) -> Result<BasicValue> {
        // Try parsing the value in the reversed order of the enum elements
        for typ in self.iter().rev() {
            match typ {
                BasicValueType::Uuid => {
                    match value.parse().map(BasicValue::Uuid) {
                        Ok(ret) => return Ok(ret),
                        Err(_) => {}
                    }
                }
                BasicValueType::OffsetDateTime => {
                    match value.parse().map(BasicValue::OffsetDateTime) {
                        Ok(ret) => return Ok(ret),
                        Err(_) => {}
                    }
                }
                BasicValueType::LocalDateTime => {
                    match value.parse().map(BasicValue::LocalDateTime) {
                        Ok(ret) => return Ok(ret),
                        Err(_) => {}
                    }
                }
                BasicValueType::Date => {
                    match value.parse().map(BasicValue::Date) {
                        Ok(ret) => return Ok(ret),
                        Err(_) => {}
                    }
                }
                BasicValueType::Time => {
                    match value.parse().map(BasicValue::Time) {
                        Ok(ret) => return Ok(ret),
                        Err(_) => {}
                    }
                }
                BasicValueType::Json => {
                    match serde_json::Value::from_str(value) {
                        Ok(ret) => return Ok(BasicValue::Json(ret.into())),
                        Err(_) => {}
                    }
                }
                BasicValueType::Str => {
                    return Ok(BasicValue::Str(Arc::from(value)));
                }
                _ => {}
            }
        }

        anyhow::bail!("Cannot parse \"{}\"", value)
    }
}

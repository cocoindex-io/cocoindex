use std::{str::FromStr, sync::Arc};

use crate::{base::{schema::BasicValueType, value::BasicValue}, prelude::*};

#[derive(Debug, Clone)]
pub enum UnionParseResult {
    Union(UnionType),
    Single(BasicValueType),
}

/// Union type helper storing an auto-sorted set of types excluding `Union`
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct UnionType {
    types: BTreeSet<BasicValueType>,
}

impl UnionType {
    fn new() -> Self {
        Self { types: BTreeSet::new() }
    }

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

    fn resolve(self) -> Result<UnionParseResult> {
        if self.types().is_empty() {
            anyhow::bail!("The union is empty");
        }

        if self.types().len() == 1 {
            let mut type_tree: BTreeSet<BasicValueType> = self.into();
            return Ok(UnionParseResult::Single(type_tree.pop_first().unwrap()));
        }

        Ok(UnionParseResult::Union(self))
    }

    /// Move an iterable and parse it into a union type.
    /// If there is only one single unique type, it returns a single `BasicValueType`.
    pub fn parse_from<T>(
        input: impl IntoIterator<Item = BasicValueType, IntoIter = T>,
    ) -> Result<UnionParseResult>
    where
        T: Iterator<Item = BasicValueType>,
    {
        let mut union = Self::new();

        for typ in input {
            union.insert(typ);
        }

        union.resolve()
    }

    /// Assume the input already contains multiple unique types, panic otherwise.
    ///
    /// This method is meant for streamlining the code for test cases.
    /// Use `parse_from()` instead unless you know the input.
    pub fn coerce_from<T>(
        input: impl IntoIterator<Item = BasicValueType, IntoIter = T>,
    ) -> Self
    where
        T: Iterator<Item = BasicValueType>,
    {
        match Self::parse_from(input) {
            Ok(UnionParseResult::Union(union)) => union,
            _ => panic!("Do not use `coerce_from()` for basic type lists that can possibly be one type."),
        }
    }
}

impl Into<BTreeSet<BasicValueType>> for UnionType {
    fn into(self) -> BTreeSet<BasicValueType> {
        self.types
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

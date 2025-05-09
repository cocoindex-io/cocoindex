use std::{str::FromStr, sync::Arc};

use crate::{base::{schema::BasicValueType, value::BasicValue}, prelude::*};

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
        if self.contains(&BasicValueType::Uuid) {
            match value.parse().map(BasicValue::Uuid) {
                Ok(ret) => return Ok(ret),
                Err(_) => {}
            }
        }

        if self.contains(&BasicValueType::OffsetDateTime) {
            match value.parse().map(BasicValue::OffsetDateTime) {
                Ok(ret) => return Ok(ret),
                Err(_) => {}
            }
        }

        if self.contains(&BasicValueType::LocalDateTime) {
            match value.parse().map(BasicValue::LocalDateTime) {
                Ok(ret) => return Ok(ret),
                Err(_) => {}
            }
        }

        if self.contains(&BasicValueType::Date) {
            match value.parse().map(BasicValue::Date) {
                Ok(ret) => return Ok(ret),
                Err(_) => {}
            }
        }

        if self.contains(&BasicValueType::Time) {
            match value.parse().map(BasicValue::Time) {
                Ok(ret) => return Ok(ret),
                Err(_) => {}
            }
        }

        if self.contains(&BasicValueType::Json) {
            match serde_json::Value::from_str(value) {
                Ok(ret) => return Ok(BasicValue::Json(ret.into())),
                Err(_) => {}
            }
        }

        if self.contains(&BasicValueType::Str) {
            return Ok(BasicValue::Str(Arc::from(value)));
        }

        anyhow::bail!("Cannot parse \"{}\"", value)
    }
}

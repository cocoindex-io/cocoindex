use std::{str::FromStr, sync::Arc};

use crate::{base::{schema::BasicValueType, value::BasicValue}, prelude::*};

/// Try parsing the str value to each possible type, and return the first successful result
pub fn parse_str(types: &[BasicValueType], val: &str) -> Result<BasicValue> {
    if types.contains(&BasicValueType::Uuid) {
        match val.parse().map(BasicValue::Uuid) {
            Ok(ret) => return Ok(ret),
            Err(_) => {}
        }
    }

    if types.contains(&BasicValueType::OffsetDateTime) {
        match val.parse().map(BasicValue::OffsetDateTime) {
            Ok(ret) => return Ok(ret),
            Err(_) => {}
        }
    }

    if types.contains(&BasicValueType::LocalDateTime) {
        match val.parse().map(BasicValue::LocalDateTime) {
            Ok(ret) => return Ok(ret),
            Err(_) => {}
        }
    }

    if types.contains(&BasicValueType::Date) {
        match val.parse().map(BasicValue::Date) {
            Ok(ret) => return Ok(ret),
            Err(_) => {}
        }
    }

    if types.contains(&BasicValueType::Time) {
        match val.parse().map(BasicValue::Time) {
            Ok(ret) => return Ok(ret),
            Err(_) => {}
        }
    }

    if types.contains(&BasicValueType::Json) {
        match serde_json::Value::from_str(val) {
            Ok(ret) => return Ok(BasicValue::Json(ret.into())),
            Err(_) => {}
        }
    }

    if types.contains(&BasicValueType::Str) {
        return Ok(BasicValue::Str(Arc::from(val)));
    }

    anyhow::bail!("Cannot parse \"{}\"", val)
}

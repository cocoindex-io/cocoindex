//! Shared SQL-style identifier validation for the relational target connectors
//! (`doris`, `sqlite`, `surrealdb`), which each build DDL/DML by interpolating
//! table/column names and so must reject anything but a safe identifier.

use crate::error::{Error, Result};

/// Validate that `value` is a safe SQL identifier: a non-empty ASCII
/// alphanumeric/underscore string that does not start with a digit. `label`
/// names the field for error messages.
pub(crate) fn validate_ident(value: &str, label: &str) -> Result<()> {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return Err(Error::engine(format!("{label} cannot be empty")));
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(Error::engine(format!("invalid {label}: {value}")));
    }
    if !chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(Error::engine(format!("invalid {label}: {value}")));
    }
    Ok(())
}

//! CLI errors carry the process exit code: 1 for failures, 2 for usage and
//! protocol errors (the same codes as the Python CLI and the protocol child).

use std::fmt::Display;

#[derive(Debug)]
pub struct CliError {
    pub code: u8,
    /// Printed as `Error: {message}`; empty when the cause was already shown.
    pub message: String,
}

pub type CliResult<T> = Result<T, CliError>;

impl CliError {
    pub fn usage(message: impl Into<String>) -> Self {
        Self {
            code: 2,
            message: message.into(),
        }
    }

    pub fn failure(message: impl Into<String>) -> Self {
        Self {
            code: 1,
            message: message.into(),
        }
    }
}

/// `result.map_err(fail("opening the db"))` → `Error: opening the db: <cause>`.
pub fn fail<E: Display>(context: &str) -> impl FnOnce(E) -> CliError + '_ {
    move |err| CliError::failure(format!("{context}: {err}"))
}

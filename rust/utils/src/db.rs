use crate::error::{Error, Result};

pub enum WriteAction {
    Insert,
    Update,
}

pub fn sanitize_identifier(s: &str) -> String {
    let mut result = String::new();
    for c in s.chars() {
        if c.is_alphanumeric() || c == '_' {
            result.push(c);
        } else {
            result.push_str("__");
        }
    }
    result
}

/// Validates that `s` is a safe SQL/Cypher identifier: `[a-zA-Z_][a-zA-Z0-9_]*`.
/// Returns a client error if invalid.
pub fn validate_identifier(s: &str) -> Result<()> {
    if s.is_empty() {
        return Err(Error::client("Identifier must not be empty".to_string()));
    }
    let mut chars = s.chars();
    let first = chars.next().unwrap();
    if !first.is_ascii_alphabetic() && first != '_' {
        return Err(Error::client(format!(
            "Invalid identifier '{s}': must start with a letter or underscore"
        )));
    }
    for c in chars {
        if !c.is_ascii_alphanumeric() && c != '_' {
            return Err(Error::client(format!(
                "Invalid identifier '{s}': contains invalid character '{c}'"
            )));
        }
    }
    Ok(())
}

/// Quote a SQL identifier with double quotes, escaping any embedded double quotes.
pub fn pg_quote_identifier(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Quote a Cypher identifier with backticks, escaping any embedded backticks.
pub fn cypher_quote_identifier(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_identifier_valid() {
        assert!(validate_identifier("my_table").is_ok());
        assert!(validate_identifier("_private").is_ok());
        assert!(validate_identifier("Table123").is_ok());
        assert!(validate_identifier("a").is_ok());
        assert!(validate_identifier("ALL_CAPS_123").is_ok());
    }

    #[test]
    fn test_validate_identifier_invalid() {
        assert!(validate_identifier("").is_err());
        assert!(validate_identifier("123abc").is_err());
        assert!(validate_identifier("my-table").is_err());
        assert!(validate_identifier("my table").is_err());
        assert!(validate_identifier("table;DROP").is_err());
        assert!(validate_identifier("name\"quoted").is_err());
        assert!(validate_identifier("label`tick").is_err());
    }

    #[test]
    fn test_pg_quote_identifier() {
        assert_eq!(pg_quote_identifier("my_table"), "\"my_table\"");
        assert_eq!(pg_quote_identifier("has\"quote"), "\"has\"\"quote\"");
    }

    #[test]
    fn test_cypher_quote_identifier() {
        assert_eq!(cypher_quote_identifier("my_label"), "`my_label`");
        assert_eq!(cypher_quote_identifier("has`tick"), "`has``tick`");
    }

    #[test]
    fn test_sanitize_identifier() {
        assert_eq!(sanitize_identifier("hello_world"), "hello_world");
        assert_eq!(sanitize_identifier("hello-world"), "hello__world");
        assert_eq!(sanitize_identifier("hello world"), "hello__world");
    }
}

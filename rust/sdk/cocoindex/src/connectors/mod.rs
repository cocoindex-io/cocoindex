//! External-system connectors for CocoIndex pipelines.

#[cfg(feature = "amazon_s3")]
pub mod amazon_s3;
#[cfg(feature = "azure_blob")]
pub mod azure_blob;
#[cfg(feature = "bigquery")]
pub mod bigquery;
#[cfg(feature = "doris")]
pub mod doris;
#[cfg(feature = "falkordb")]
pub mod falkordb;
#[cfg(feature = "google_drive")]
pub mod gdrive;
#[cfg(all(test, any(feature = "neo4j", feature = "falkordb")))]
mod graph_live_tests;
#[cfg(feature = "iggy")]
pub mod iggy;
#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(feature = "lancedb")]
pub mod lancedb;
#[cfg(feature = "neo4j")]
pub mod neo4j;
#[cfg(feature = "oci_object_storage")]
pub mod oci_object_storage;
#[cfg(any(feature = "kafka", feature = "iggy"))]
mod partition_tail;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "qdrant")]
pub mod qdrant;
#[cfg(feature = "snowflake")]
pub mod snowflake;
#[cfg(feature = "sqlite")]
pub mod sqlite;
#[cfg(feature = "surrealdb")]
pub mod surrealdb;
#[cfg(feature = "turbopuffer")]
pub mod turbopuffer;
#[cfg(feature = "valkey")]
pub mod valkey;
#[cfg(feature = "zvec")]
pub mod zvec;

/// Cut a server's response body to at most `max` bytes for an error message,
/// on a character boundary: a body is arbitrary text, and slicing inside a
/// multi-byte character panics.
#[cfg(any(feature = "bigquery", feature = "doris", feature = "snowflake"))]
fn truncate_for_error(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..s.floor_char_boundary(max)])
    }
}

#[cfg(all(
    test,
    any(feature = "bigquery", feature = "doris", feature = "snowflake")
))]
mod tests {
    use super::truncate_for_error;

    #[test]
    fn truncate_for_error_cuts_on_a_character_boundary() {
        assert_eq!(truncate_for_error("short", 10), "short");
        assert_eq!(truncate_for_error("abcdef", 3), "abc…");
        // "é" is two bytes; byte 2 falls inside it.
        assert_eq!(truncate_for_error("aébc", 2), "a…");
        assert_eq!(truncate_for_error("日本語", 4), "日…");
    }
}

//! Shared support for the connector integration tests.
#![allow(dead_code)]

use std::sync::{Mutex, OnceLock};

use tracing::field::{Field, Visit};
use tracing::{Event, Level, Metadata, Subscriber, span};

static STATEMENTS: OnceLock<Mutex<Vec<String>>> = OnceLock::new();

/// Record every SQL statement `sqlx` executes from now on, in this test
/// binary. `sqlx` reports each executed statement as a `tracing` event on the
/// `sqlx::query` target; this installs a global subscriber for that target
/// once per binary, so tests running in parallel share the log and pick their
/// statements by table name ([`sql_statements_containing`]). The subscriber
/// takes the place of the engine's, so it prints the engine's warnings and
/// errors to stderr, which is where a background mount's failure goes.
pub fn record_sql_statements() {
    STATEMENTS.get_or_init(|| {
        // A second `set_global_default` (another test called this first)
        // fails harmlessly: the subscriber is already ours.
        let _ = tracing::subscriber::set_global_default(SqlStatementLog);
        Mutex::new(Vec::new())
    });
}

/// The recorded statements whose text contains `needle`.
pub fn sql_statements_containing(needle: &str) -> Vec<String> {
    STATEMENTS
        .get()
        .expect("record_sql_statements() was not called")
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .filter(|statement| statement.contains(needle))
        .cloned()
        .collect()
}

struct SqlStatementLog;

impl Subscriber for SqlStatementLog {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target() == "sqlx::query" || *metadata.level() <= Level::WARN
    }

    fn new_span(&self, _attributes: &span::Attributes<'_>) -> span::Id {
        span::Id::from_u64(1)
    }

    fn record(&self, _span: &span::Id, _values: &span::Record<'_>) {}

    fn record_follows_from(&self, _span: &span::Id, _follows: &span::Id) {}

    fn event(&self, event: &Event<'_>) {
        if event.metadata().target() != "sqlx::query" {
            let mut message = String::new();
            event.record(&mut MessageText(&mut message));
            eprintln!(
                "{} {}: {message}",
                event.metadata().level(),
                event.metadata().target()
            );
            return;
        }
        let mut text = String::new();
        event.record(&mut StatementText(&mut text));
        if let Some(statements) = STATEMENTS.get() {
            statements
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(text);
        }
    }

    fn enter(&self, _span: &span::Id) {}

    fn exit(&self, _span: &span::Id) {}
}

/// Collects the statement text of a `sqlx::query` event: its `summary` (the
/// first words) and, for a longer statement, `db.statement` (the whole text).
struct StatementText<'a>(&'a mut String);

impl Visit for StatementText<'_> {
    fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "summary" || field.name() == "db.statement" {
            self.0.push_str(value);
            self.0.push(' ');
        }
    }
}

/// Collects every field of an event as text.
struct MessageText<'a>(&'a mut String);

impl Visit for MessageText<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if !self.0.is_empty() {
            self.0.push_str("; ");
        }
        self.0.push_str(&format!("{}={value:?}", field.name()));
    }
}

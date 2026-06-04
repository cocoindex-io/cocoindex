# Input Safety: Identifiers & Values

Target connectors build queries from user-provided names and values. Follow these guidelines to prevent injection and ensure correctness.

## 1. Identifier Validation

User-provided names (table, column, index) are interpolated into queries as identifiers. Validate them **early at API entry points** — not at query construction time.

Use a regex allowlist to reject anything that isn't a simple identifier:

```python
import re

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

def _validate_identifier(name: str, kind: str) -> None:
    """Raise ValueError if name is not a safe identifier."""
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(
            f"Invalid {kind}: {name!r}. "
            f"Must match [a-zA-Z_][a-zA-Z0-9_]*."
        )
```

Call at every public method that accepts a name:

```python
def table_target(self, table_name: str, ...) -> ...:
    _validate_identifier(table_name, "table name")
    ...

class TableSchema:
    def __init__(self, columns: dict[str, ColumnDef], ...) -> None:
        for col_name in columns:
            _validate_identifier(col_name, "column name")
        ...
```

## 2. Parameterized Queries for Values

**Always** use parameterized queries (bind variables) for data values. Never interpolate values directly into query strings.

```python
# Good — parameterized
await conn.execute("INSERT INTO t (name) VALUES ($1)", value)       # PostgreSQL
conn.execute("INSERT INTO t (name) VALUES (?)", (value,))           # SQLite
await conn.query("UPSERT t:id CONTENT $content", {"content": val})  # SurrealDB

# Bad — string interpolation
await conn.execute(f"INSERT INTO t (name) VALUES ('{value}')")
```

## 3. Value Escaping (When Parameterization Isn't Possible)

Some query languages require inline values in certain positions (e.g., SurrealDB record IDs in `table:id` syntax). In these cases:

- **Preserve type distinctions.** Integer `123` and string `"123"` may be semantically different (e.g., `table:123` vs `` table:`123` `` in SurrealDB).
- **Escape the quoting character.** If the target uses backtick quoting, escape backslashes and backticks inside the value.

```python
def _format_record_id(value: Any) -> str:
    """Format a record ID for inline use, preserving type."""
    if isinstance(value, (int, float)):
        return str(value)                              # bare numeric: 123, 3.14
    s = str(value)
    s = s.replace("\\", "\\\\").replace("`", "\\`")
    return f"`{s}`"                                    # quoted string: `alice`
```

## 4. Testing

Add tests for safety helpers that **don't require a database**:

- Valid identifiers pass, invalid ones raise `ValueError`
- Escaping produces correct output for special characters, empty strings, numeric types
- API entry points reject bad names (e.g., `TableSchema(columns={"bad-name": ...})`)

Add integration tests for round-tripping values with special characters through the full upsert/select cycle when a database is available.

**Reference:** See `python/cocoindex/connectors/surrealdb/_target.py` and `python/tests/connectors/test_surrealdb_target.py` for the canonical implementation.

---
title: Postgres
toc_max_heading_level: 4
description: CocoIndex Postgres Built-in Sources
---

The `Postgres` source imports rows from a PostgreSQL table.

### Setup for PostgreSQL

* Ensure the table exists and has a primary key. Tables without a primary key are not supported.
* Grant the connecting user read permissions on the target table (e.g. `SELECT`).
* Provide a database connection. You can:
  * Use CocoIndex's default database connection, or
  * Provide an explicit connection via a transient auth entry referencing a `DatabaseConnectionSpec` with a `url`, for example:

        ```python
        cocoindex.add_transient_auth_entry(
            cocoindex.sources.DatabaseConnectionSpec(
                url="postgres://user:password@host:5432/dbname?sslmode=require",
            )
        )
        ```

### Spec

The spec takes the following fields:

* `table_name` (`str`): the PostgreSQL table to read from.
* `database` (`cocoindex.TransientAuthEntryReference[DatabaseConnectionSpec]`, optional): database connection reference. If not provided, the default CocoIndex database is used.
* `included_columns` (`list[str]`, optional): non-primary-key columns to include. If not specified, all non-PK columns are included.
* `ordinal_column` (`str`, optional): to specify a non-primary-key column used for change tracking and ordering, e.g. can be a modified timestamp or a monotonic version number. Supported types are integer-like (`bigint`/`integer`) and timestamps (`timestamp`, `timestamptz`).
    `ordinal_column` must not be a primary key column.

* `filter` (`str`, optional): arbitrary SQL boolean expression to filter rows. Only rows satisfying this condition will be included. For example: `"age > 18"`, `"status = 'active'"`, or `"created_at > '2023-01-01'"`. The expression is added as a WHERE clause to the SQL queries.

    :::info

    The `filter` expression is inserted directly into SQL queries. Ensure that:
  * The expression uses valid PostgreSQL syntax
  * Column names and values are properly quoted if needed
  * The expression evaluates to a boolean result
  * You trust the source of the filter expression to avoid SQL injection

    :::

* `notification` (`cocoindex.sources.PostgresNotification`, optional): when present, enable change capture based on Postgres LISTEN/NOTIFY. It has the following fields:
  * `channel_name` (`str`, optional): the Postgres notification channel to listen on. CocoIndex will automatically create the channel with the given name. If omitted, CocoIndex uses `{flow_name}__{source_name}__cocoindex`.

    :::info

    If `notification` is provided, CocoIndex listens for row changes using Postgres LISTEN/NOTIFY and creates the required database objects on demand when the flow starts listening:

  * Function to create notification message: `{channel_name}_n`.
  * Trigger to react to table changes: `{channel_name}_t` on the specified `table_name`.

    Creation is automatic when listening begins.

    :::info

    Currently CocoIndex doesn't automatically clean up these objects when the flow is dropped (unlike targets)
    It's usually OK to leave them as they are, but if you want to clean them up, you can run the following SQL statements to manually drop them:

    ```sql
    DROP TRIGGER IF EXISTS {channel_name}_t ON "{table_name}";
    DROP FUNCTION IF EXISTS {channel_name}_n();
    ```

    :::

### Schema

The output is a [*KTable*](/docs/core/data_types#ktable) with straightforward 1 to 1 mapping from Postgres table columns to CocoIndex table fields:

* Key fields: All primary key columns in the Postgres table will be included automatically as key fields.
* Value fields: All non-primary-key columns in the Postgres table (included by `included_columns` or all when not specified) appear as value fields.

### Example

An example of using `filter` to filter rows:

```python
data_scope["products"] = flow_builder.add_source(
    cocoindex.sources.Postgres(
        table_name="source_products",
        # Optional. Use the default CocoIndex database if not specified.
        database=cocoindex.add_transient_auth_entry(
            cocoindex.DatabaseConnectionSpec(
                url=os.environ["SOURCE_DATABASE_URL"],
            )
        ),
        # Optional
        ordinal_column="modified_time",
        # Optional
        filter="amount > 0",
        # Optional
        notification=cocoindex.sources.PostgresNotification(),
    ),
)
```

You can find end-to-end example using Postgres source at:

* [examples/postgres_source](https://github.com/cocoindex-io/cocoindex/tree/main/examples/postgres_source)

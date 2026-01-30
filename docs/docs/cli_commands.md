<!-- markdownlint-disable MD041 -->

## Subcommands Reference

### `drop`

Drop an app and all its target states.

This will:

- Revert all target states created by the app (e.g., drop tables, delete rows)
- Clear the app's internal state database

`APP_TARGET`: `path/to/app.py`, `module`, `path/to/app.py:app_name`, or
`module:app_name`.


**Usage:**

```bash
cocoindex drop [OPTIONS] APP_TARGET
```

**Options:**

| Option | Description |
|--------|-------------|
| `-f, --force` | Skip confirmation prompt. |
| `--help` | Show this message and exit. |

---

### `init`

Initialize a new CocoIndex project.

Creates a new project directory with starter files: 1. main.py (Main
application file) 2. pyproject.toml (Project metadata and dependencies) 3.
README.md (Quick start guide)

`PROJECT_NAME`: Name of the project (defaults to current directory name if
not specified).


**Usage:**

```bash
cocoindex init [OPTIONS] [PROJECT_NAME]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--dir DIRECTORY` | Directory to create the project in. |
| `--help` | Show this message and exit. |

---

### `ls`

List all apps.

If `APP_TARGET` (`path/to/app.py` or `module`) is provided, lists apps
defined in that module and their persisted status, grouped by environment.

If `APP_TARGET` is omitted and `--db` is provided, lists all apps from the
specified database.


**Usage:**

```bash
cocoindex ls [OPTIONS] [APP_TARGET]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--db TEXT` | Path to database to list apps from (only used when APP_TARGET is not specified). |
| `--help` | Show this message and exit. |

---

### `show`

Show the app's stable paths.

`APP_TARGET`: `path/to/app.py`, `module`, `path/to/app.py:app_name`, or
`module:app_name`.


**Usage:**

```bash
cocoindex show [OPTIONS] APP_TARGET
```

**Options:**

| Option | Description |
|--------|-------------|
| `--help` | Show this message and exit. |

---

### `update`

Run a v1 app once (one-time update).

`APP_TARGET`: `path/to/app.py`, `module`, `path/to/app.py:app_name`, or
`module:app_name`.


**Usage:**

```bash
cocoindex update [OPTIONS] APP_TARGET
```

**Options:**

| Option | Description |
|--------|-------------|
| `--help` | Show this message and exit. |

---

<!-- markdownlint-disable MD041 -->

## Subcommands Reference

### `drop`

Drop the backend setup for flows.

Modes of operation:
1. Drop all flows defined in an app: `cocoindex drop <APP_TARGET>`
2. Drop specific named flows: `cocoindex drop <APP_TARGET> [FLOW_NAME...]`


**Usage:**

```bash
cocoindex drop [OPTIONS] [APP_TARGET] [FLOW_NAME]...
```

**Options:**

| Option | Description |
|--------|-------------|
| `-f, --force` | Force drop without confirmation prompts. |
| `--help` | Show this message and exit. |

---

### `evaluate`

Evaluate the flow and dump flow outputs to files.

Instead of updating the index, it dumps what should be indexed to files.
Mainly used for evaluation purpose.

`APP_FLOW_SPECIFIER`: Specifies the application and optionally the target flow. Can be one of the following formats:
- `path/to/your_app.py`
- `an_installed.module_name`
- `path/to/your_app.py:SpecificFlowName`
- `an_installed.module_name:SpecificFlowName`

`:SpecificFlowName` can be omitted only if the application defines a single
flow.


**Usage:**

```bash
cocoindex evaluate [OPTIONS] APP_FLOW_SPECIFIER
```

**Options:**

| Option | Description |
|--------|-------------|
| `-o, --output-dir TEXT` | The directory to dump the output to. |
| `--cache / --no-cache` | Use already-cached intermediate data if available. [default: cache] |
| `--help` | Show this message and exit. |

---

### `ls`

List all flows.

If `APP_TARGET` (`path/to/app.py` or a module) is provided, lists flows
defined in the app and their backend setup status.

If `APP_TARGET` is omitted, lists all flows that have a persisted setup in
the backend.


**Usage:**

```bash
cocoindex ls [OPTIONS] [APP_TARGET]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--help` | Show this message and exit. |

---

### `server`

Start a HTTP server providing REST APIs.

It will allow tools like CocoInsight to access the server.

`APP_TARGET`: `path/to/app.py` or `installed_module`.


**Usage:**

```bash
cocoindex server [OPTIONS] APP_TARGET
```

**Options:**

| Option | Description |
|--------|-------------|
| `-a, --address TEXT` | The address to bind the server to, in the format of IP:PORT. If unspecified, the address specified in COCOINDEX_SERVER_ADDRESS will be used. |
| `-c, --cors-origin TEXT` | The origins of the clients (e.g. CocoInsight UI) to allow CORS from. Multiple origins can be specified as a comma-separated list. e.g. `https://cocoindex.io,http://localhost:3000`. Origins specified in COCOINDEX_SERVER_CORS_ORIGINS will also be included. |
| `-ci, --cors-cocoindex` | Allow `https://cocoindex.io` to access the server. |
| `-cl, --cors-local INTEGER` | Allow `http://localhost:<port>` to access the server. |
| `-L, --live-update` | Continuously watch changes from data sources and apply to the target index. |
| `--setup` | (DEPRECATED) Automatically setup backends for the flow if it's not setup yet. This is now the default behavior.  [default: True] |
| `--reset` | Drop existing setup before starting server (equivalent to running 'cocoindex drop' first). `--reset` implies `--setup`. |
| `--reexport` | Reexport to targets even if there's no change. |
| `-f, --force` | Force setup without confirmation prompts. |
| `-q, --quiet` | Avoid printing anything to the standard output, e.g. statistics. |
| `-r, --reload` | Enable auto-reload on code changes. |
| `--help` | Show this message and exit. |

---

### `setup`

Check and apply backend setup changes for flows, including the internal
storage and target (to export to).

`APP_TARGET`: `path/to/app.py` or `installed_module`.


**Usage:**

```bash
cocoindex setup [OPTIONS] APP_TARGET
```

**Options:**

| Option | Description |
|--------|-------------|
| `-f, --force` | Force setup without confirmation prompts. |
| `--reset` | Drop existing setup before running setup (equivalent to running 'cocoindex drop' first). |
| `--help` | Show this message and exit. |

---

### `show`

Show the flow spec and schema.

`APP_FLOW_SPECIFIER`: Specifies the application and optionally the target
flow. Can be one of the following formats:

- `path/to/your_app.py`
- `an_installed.module_name`
- `path/to/your_app.py:SpecificFlowName`
- `an_installed.module_name:SpecificFlowName`

`:SpecificFlowName` can be omitted only if the application defines a single
flow.


**Usage:**

```bash
cocoindex show [OPTIONS] APP_FLOW_SPECIFIER
```

**Options:**

| Option | Description |
|--------|-------------|
| `--color / --no-color` | Enable or disable colored output. |
| `--verbose` | Show verbose output with full details. |
| `--help` | Show this message and exit. |

---

### `update`

Update the index to reflect the latest data from data sources.

`APP_FLOW_SPECIFIER`: `path/to/app.py`, module, `path/to/app.py:FlowName`,
or `module:FlowName`. If `:FlowName` is omitted, updates all flows.


**Usage:**

```bash
cocoindex update [OPTIONS] APP_FLOW_SPECIFIER
```

**Options:**

| Option | Description |
|--------|-------------|
| `-L, --live` | Continuously watch changes from data sources and apply to the target index. |
| `--reexport` | Reexport to targets even if there's no change. |
| `--setup` | (DEPRECATED) Automatically setup backends for the flow if it's not setup yet. This is now the default behavior.  [default: True] |
| `--reset` | Drop existing setup before updating (equivalent to running 'cocoindex drop' first). `--reset` implies `--setup`. |
| `-f, --force` | Force setup without confirmation prompts. |
| `-q, --quiet` | Avoid printing anything to the standard output, e.g. statistics. |
| `--help` | Show this message and exit. |

---

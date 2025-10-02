# Development Scripts

This directory contains development and maintenance scripts for the CocoIndex project.

## Scripts

### `generate_cli_docs.py`

Automatically generates CLI documentation from the CocoIndex Click commands.

**Usage:**

```bash
python dev/generate_cli_docs.py
```

**What it does:**

- Extracts help messages from all Click commands in `python/cocoindex/cli.py`
- Generates comprehensive Markdown documentation with properly formatted tables
- Saves the output to `docs/docs/core/cli-commands.md` for direct import into CLI documentation
- Only updates the file if content has changed (avoids unnecessary git diffs)
- Automatically escapes HTML-like tags to prevent MDX parsing issues
- Wraps URLs with placeholders in code blocks for proper rendering

**Integration:**

- Runs automatically as a pre-commit hook when `python/cocoindex/cli.py` is modified
- The generated documentation is directly imported into `docs/docs/core/cli.mdx` via MDX import
- Provides seamless single-page CLI documentation experience without separate reference pages

**Dependencies:**

- `md-click` package for extracting Click help information
- `cocoindex` package must be importable (the CLI module)

This ensures that CLI documentation is always kept in sync with the actual command-line interface.

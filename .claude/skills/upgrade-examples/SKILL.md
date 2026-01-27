---
name: upgrade-examples
description: This skill should be used when upgrading the cocoindex package version in all example pyproject.toml files. It uses regex-based sed commands to efficiently update version constraints across multiple files at once.
---

# Upgrade Example Dependencies

Upgrade the cocoindex version in all example `pyproject.toml` files.

## Usage

```
/upgrade-examples <version>
```

Example: `/upgrade-examples 1.0.0a5`

## Instructions

To upgrade all example dependencies:

1. Parse the version from the user's argument (e.g., `1.0.0a5`)

2. Run the following command to update all example pyproject.toml files (replace `NEW_VERSION` with the actual version):

```bash
find examples -name "pyproject.toml" -not -path "*/.venv/*" -exec grep -l "cocoindex" {} \; | xargs sed -i '' 's/cocoindex\([^>]*\)>=[0-9][0-9a-zA-Z.]*/cocoindex\1>=NEW_VERSION/g'
```

3. Verify the changes:

```bash
grep -r "cocoindex.*>=" examples --include="pyproject.toml" | grep -v ".venv"
```

4. Report the number of files updated and confirm all versions match the target.

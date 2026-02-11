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

To upgrade all example dependencies (replace `VERSION` with the actual version, e.g., `1.0.0a10`):

1. Create a new branch for the changes:

```bash
git checkout -b ex-dep-VERSION
```

2. Run the following command to update all example pyproject.toml files.

**Important:** Use the literal version string directly in the sed command. Do not use shell variables as quoting issues can cause the version to be omitted.

```bash
find examples -name "pyproject.toml" -not -path "*/.venv/*" -exec grep -l "cocoindex" {} \; | xargs sed -i '' 's/cocoindex\([^>]*\)>=[0-9][0-9a-zA-Z.]*/cocoindex\1>=VERSION/g'
```

3. Verify the changes show the correct version:

```bash
grep -r "cocoindex.*>=" examples --include="pyproject.toml" | grep -v ".venv"
```

4. Commit and push the changes:

```bash
git add examples/*/pyproject.toml
git commit -m "chore: upgrade examples deps to cocoindex-VERSION"
git push -u origin ex-dep-VERSION
```

5. Report the number of files updated and provide the branch name for creating a PR.

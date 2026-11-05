#!/bin/bash
# PostToolUse hook: set a flag when a file the docs build reads is changed:
# anything under docs/, the agent skill under skills/, examples/AGENTS.md, and
# examples/<dir>/.env and .env.example. The paths: filters in
# .github/workflows/docs_test.yml list the same build inputs; keep them in sync.
# The actual build runs in the Stop hook (docs-build-run.sh).

INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')

if [ -z "$FILE_PATH" ]; then
  exit 0
fi

# Match absolute paths under the project and repo-relative paths alike.
REL_PATH=${FILE_PATH#"$CLAUDE_PROJECT_DIR"/}

# Unlike the workflow filters, `*` here also matches `/`, so nested files like
# examples/rust/<dir>/.env.example flag too: an unneeded but harmless build.
case "$REL_PATH" in
  docs/*|skills/*|examples/AGENTS.md|examples/*/.env|examples/*/.env.example)
    touch "$CLAUDE_PROJECT_DIR/.claude/hooks/.docs-changed"
    ;;
esac

exit 0

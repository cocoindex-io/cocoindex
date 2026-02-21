#!/bin/bash
# PostToolUse hook: set a flag when any file under docs/ is changed.
# The actual build runs in the Stop hook (docs-build-run.sh).

INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')

if [ -z "$FILE_PATH" ]; then
  exit 0
fi

case "$FILE_PATH" in
  "$CLAUDE_PROJECT_DIR"/docs/*|docs/*)
    touch "$CLAUDE_PROJECT_DIR/.claude/hooks/.docs-changed"
    ;;
esac

exit 0

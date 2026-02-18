#!/bin/bash
# PostToolUse hook: set a flag when any .py or .pyi file is changed.
# The actual typecheck runs in the Stop hook (py-typecheck-run.sh).

INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')

if [ -z "$FILE_PATH" ]; then
  exit 0
fi

case "$FILE_PATH" in
  *.py|*.pyi)
    touch "$CLAUDE_PROJECT_DIR/.claude/hooks/.py-changed"
    ;;
esac

exit 0

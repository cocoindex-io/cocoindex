#!/bin/bash
# Stop hook: if .py/.pyi files were changed during this turn, run mypy.

FLAG="$CLAUDE_PROJECT_DIR/.claude/hooks/.py-changed"

if [ ! -f "$FLAG" ]; then
  exit 0
fi

rm -f "$FLAG"

cd "$CLAUDE_PROJECT_DIR" && uv run mypy 2>&1
MYPY_EXIT=$?
if [ $MYPY_EXIT -ne 0 ]; then
  echo "mypy typecheck failed" >&2
  exit 2
fi

exit 0

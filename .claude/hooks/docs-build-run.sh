#!/bin/bash
# Stop hook: if docs/ files were changed during this turn, run yarn build.

FLAG="$CLAUDE_PROJECT_DIR/.claude/hooks/.docs-changed"

if [ ! -f "$FLAG" ]; then
  exit 0
fi

rm -f "$FLAG"

cd "$CLAUDE_PROJECT_DIR/docs" && yarn build 2>&1
BUILD_EXIT=$?
if [ $BUILD_EXIT -ne 0 ]; then
  echo "docs build failed" >&2
  exit 2
fi

exit 0

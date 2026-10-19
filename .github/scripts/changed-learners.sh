#!/usr/bin/env bash
# stdin: changed paths, one per line. stdout: JSON array of learner handles to test.
# _template is included when it changed (everyone copies it). Folders without a
# pyproject.toml (deleted, or not a learner folder) are skipped.
set -euo pipefail
sed -n 's#^learners/\([^/]\+\)/.*#\1#p' | sort -u | while read -r h; do
  [ -f "learners/$h/pyproject.toml" ] && echo "$h"
done | jq -R . | jq -sc .

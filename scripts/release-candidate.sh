#!/usr/bin/env bash
set -euo pipefail

git fetch origin main --no-tags
version=$(node -p "require('./package.json').version")
subject=$(git log -1 --pretty=%s)
if [ "$GITHUB_EVENT_NAME" = "workflow_dispatch" ]; then
  echo "release=true" >> "$GITHUB_OUTPUT"
elif [ "$(git rev-parse HEAD)" != "$(git rev-parse origin/main)" ]; then
  echo "A newer main commit exists; its push will publish the canary instead."
  echo "release=false" >> "$GITHUB_OUTPUT"
elif [ "$subject" = "v$version" ]; then
  echo "Skipping stable release commit v$version."
  echo "release=false" >> "$GITHUB_OUTPUT"
elif [[ "$subject" == *'[skip canary]'* ]]; then
  echo "Skipping canary while new package names are bootstrapped."
  echo "release=false" >> "$GITHUB_OUTPUT"
else
  echo "release=true" >> "$GITHUB_OUTPUT"
fi

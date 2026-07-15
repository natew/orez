#!/usr/bin/env bash
# landing-debt.sh — list branches carrying work that never reached origin/main.
#
# "landing debt" is a branch (local or origin) with commits not on origin/main.
# reviewed-and-parked work is the failure mode this catches: the branch exists,
# the work is done, nobody merged it. read-only; safe to run anywhere, anytime.
#
# usage:  scripts/landing-debt.sh            # human table, oldest debt first
#         scripts/landing-debt.sh --tsv      # machine-readable (agentbus cron)
#
# columns: BRANCH  AHEAD(commits not on main)  AGE(days since last commit)
#          WORKTREE(checked out somewhere?)    MERGE(clean vs main?)
# a worktree=no + old + clean row is almost always forgotten, mergeable work.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
git fetch --quiet --prune origin 2>/dev/null || true
base=origin/main
now=$(date +%s)
tsv=${1:-}

worktree_branches=$(git worktree list --porcelain \
  | awk '/^branch /{sub("refs/heads/","",$2); print $2}')

emit() { # branch ahead age inwt merge
  if [ "$tsv" = "--tsv" ]; then
    printf '%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" "$5"
  else
    printf '%-48s %6s %6s %-9s %-8s\n' "$1" "$2" "$3" "$4" "$5"
  fi
}

[ "$tsv" = "--tsv" ] || emit BRANCH AHEAD AGEd WORKTREE MERGE
{
  for ref in $(git for-each-ref --format='%(refname:short)' \
      refs/heads refs/remotes/origin | grep -vE '(^|/)(main|HEAD)$'); do
    ahead=$(git rev-list --count "$base..$ref" 2>/dev/null || echo 0)
    [ "$ahead" = "0" ] && continue          # fully landed or ancestor: no debt
    last=$(git log -1 --format=%ct "$ref")
    age=$(( (now - last) / 86400 ))
    inwt=no
    echo "$worktree_branches" | grep -qx "${ref#origin/}" && inwt=yes
    if git merge-tree --write-tree "$base" "$ref" >/dev/null 2>&1; then
      merge=clean
    else
      merge=CONFLICT
    fi
    emit "$ref" "$ahead" "$age" "$inwt" "$merge"
  done
} | sort -k3 -rn

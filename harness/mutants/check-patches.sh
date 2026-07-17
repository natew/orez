#!/usr/bin/env bash
# compile-check every mutant patch: apply -> cargo check -> revert.
# a mutant that does not compile is not a mutant.
set -uo pipefail
cd "$(dirname "$0")/../.."

fail=0
for patch in harness/mutants/patches/*.patch; do
  id="$(basename "$patch" .patch)"
  if ! git apply "$patch"; then
    echo "APPLY-FAIL $id"
    fail=1
    continue
  fi
  if cargo check -p sync-core --quiet 2>/tmp/mutant-check-$$.log; then
    echo "OK $id"
  else
    echo "CHECK-FAIL $id"
    tail -20 /tmp/mutant-check-$$.log
    fail=1
  fi
  git apply -R "$patch" || { echo "REVERT-FAIL $id"; exit 2; }
done
[ -z "$(git status --porcelain -- crates/)" ] || { echo "DIRTY TREE"; exit 2; }
exit $fail

#!/usr/bin/env bash
# nightly conformance + load lanes (plans/zero-conformance-harness.md M6).
# runner: mini-16 via cron — `cd ~/orez && git pull --ff-only && bash
# harness/scripts/nightly.sh`. writes a dated log under harness/results/
# (untracked). heavier than CI on purpose: bigger sweep, bench grid.
set -euo pipefail
export PATH="$HOME/.bun/bin:/opt/homebrew/bin:$PATH"

cd "$(dirname "$0")/../.."
bun install --frozen-lockfile
cd harness
bun install
mkdir -p results
STAMP=$(date +%Y%m%d-%H%M)
LOG="results/nightly-$STAMP.log"

{
  echo "== nightly $STAMP on $(hostname) =="
  bun src/smoke.ts --target orez-local --clients 50 --projects 5
  bun src/shapes.ts
  bun src/sweep.ts --rounds 40
  for clients in 10 25 50 100; do
    bun src/bench.ts --target orez-local --clients "$clients" --writers 5 --rate 10 --duration 30 --label nightly
  done
  bun src/bench.ts --target stock-zero --clients 25 --writers 5 --rate 10 --duration 30 --label nightly
  echo "== nightly $STAMP PASS =="
} 2>&1 | tee "$LOG"

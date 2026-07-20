#!/usr/bin/env bash
# nightly conformance + load lanes (plans/zero-conformance-harness.md M6).
# runner: mini-16 via cron — `cd ~/orez && git pull --ff-only && bash
# harness/scripts/nightly.sh`. writes a dated log under harness/results/
# (untracked). heavier than CI on purpose: bigger sweep, bench grid.
set -euo pipefail
export PATH="$HOME/.bun/bin:$HOME/.cargo/bin:$HOME/.local/bin:$HOME/.local/share/mise/shims:/opt/homebrew/bin:$PATH"

cd "$(dirname "$0")/../.."
bun install --frozen-lockfile
cargo build --release -p sync-native --bin sync-native-fixture
cd harness
bun install
mkdir -p results
STAMP=$(date +%Y%m%d-%H%M)
SEED=$(date +%s)
LOG="results/nightly-$STAMP.log"

{
  echo "== nightly $STAMP on $(hostname), seed $SEED =="
  bun src/corpus-check.ts
  bun src/upstream-corpus.ts --hosts typescript-oracle,stock-zero,sync-native,rust-cf
  bun src/smoke.ts --target orez-local --clients 50 --projects 5
  bun src/shapes.ts
  bun src/sweep.ts --rounds 40 --seed "$SEED"
  bun src/smoke.ts --target rust-local --clients 50 --projects 5
  bun src/shapes.ts --against rust-local
  bun src/sweep.ts --against rust-local --rounds 80 --seed "$SEED"
  bun src/shapes.ts --against rust-cf
  bun src/sweep.ts --against rust-cf --rounds 80 --seed "$SEED"
  bun src/m6-runner.ts --suite all
  for state_seed in 1 7 42 "$SEED"; do
    bun src/state-machine.ts --against rust-local --seed "$state_seed" --steps 80
    bun src/state-machine.ts --against rust-cf --seed "$state_seed" --steps 80
  done
  for clients in 10 25 50 100; do
    bun src/bench.ts --target orez-local --clients "$clients" --writers 5 --rate 10 --duration 30 --label nightly
    bun src/bench.ts --target rust-local --clients "$clients" --writers 5 --rate 10 --duration 30 --label nightly
  done
  bun src/bench.ts --target stock-zero --clients 25 --writers 5 --rate 10 --duration 30 --label nightly
  echo "== nightly $STAMP PASS =="
} 2>&1 | tee "$LOG"

#!/usr/bin/env bash
# orez perf suite runner
# Usage:
#   ./perf/run.sh              # full quick check (~2 min)
#   ./perf/run.sh quick        # fastest check: correctness + overhead
#   ./perf/run.sh full         # full suite: bench + overhead + correctness + crash
#   ./perf/run.sh load         # load test (default 60s)
#   ./perf/run.sh memory       # memory profile (default 120s)
#   ./perf/run.sh soak         # long soak test (1h)
#   ./perf/run.sh diagnose     # diagnostic on a running instance
#   ./perf/run.sh chat-e2e     # full chat e2e test validation gate

set -euo pipefail

MODE="${1:-quick}"
shift || true

cd "$(dirname "$0")/.."

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[1;36m'
NC='\033[0m'

log() { echo -e "${CYAN}[perf]${NC} $*"; }
pass() { echo -e "${GREEN}[PASS]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*"; }

mkdir -p perf/reports

case "$MODE" in
  quick)
    log "=== QUICK CHECK ==="
    
    log "--- Correctness Tests ---"
    if bun test perf/stability/correctness.test.ts "$@" 2>&1; then
      pass "correctness"
    else
      fail "correctness"
    fi

    log "--- Proxy Overhead ---"
    if bun run perf/scripts/bench-proxy-overhead.ts "$@" 2>&1; then
      pass "overhead"
    else
      fail "overhead"
    fi

    log "=== QUICK CHECK DONE ==="
    ;;

  full)
    log "=== FULL SUITE ==="

    log "--- Performance Benchmarks ---"
    bun run perf/scripts/bench-all.ts "$@" --output=perf/reports/bench.json 2>&1 || fail "benchmarks"

    log "--- Proxy Overhead ---"
    bun run perf/scripts/bench-proxy-overhead.ts "$@" 2>&1 || fail "overhead"

    log "--- Correctness ---"
    bun test perf/stability/correctness.test.ts "$@" 2>&1 || fail "correctness"

    log "--- Crash Recovery ---"
    bun run perf/stability/crash-recovery.ts "$@" 2>&1 || fail "crash"

    log "=== FULL SUITE DONE ==="
    ;;

  load)
    log "=== LOAD TEST ==="
    bun run perf/load/harness.ts "$@"
    ;;

  memory)
    log "=== MEMORY PROFILE ==="
    bun run perf/memory/profile.ts "$@"
    ;;

  soak)
    log "=== SOAK TEST ==="
    bun run perf/stability/soak.ts "$@"
    ;;

  crash)
    log "=== CRASH RECOVERY ==="
    bun run perf/stability/crash-recovery.ts "$@"
    ;;

  diagnose)
    log "=== DIAGNOSTIC ==="
    bun run perf/scripts/diagnose.ts "$@"
    ;;

  chat-e2e)
    log "=== CHAT E2E (VALIDATION GATE) ==="
    log "This runs the full chat e2e test suite against a local orez build."
    log "Duration: ~20 minutes"
    bun run test:chat:e2e
    ;;

  single-db)
    log "=== SINGLE-DB FULL SUITE ==="
    log "--- Benchmarks ---"
    bun run perf/scripts/bench-all.ts --single-db --output=perf/reports/bench-singledb.json 2>&1 || fail "bench"
    log "--- Overhead ---"
    bun run perf/scripts/bench-proxy-overhead.ts --single-db 2>&1 || fail "overhead"
    log "--- Correctness ---"
    bun test perf/stability/correctness.test.ts -- --single-db 2>&1 || fail "correctness"
    log "--- Crash Recovery ---"
    bun run perf/stability/crash-recovery.ts --single-db 2>&1 || fail "crash"
    log "=== SINGLE-DB DONE ==="
    ;;

  *)
    echo "Usage: perf/run.sh [quick|full|load|memory|soak|crash|diagnose|chat-e2e|single-db]"
    echo ""
    echo "  quick     - Fast check: correctness + overhead (~60s)"
    echo "  full      - Full suite: benchmarks + overhead + correctness + crash (~5min)"
    echo "  load      - Load test (configurable duration)"
    echo "  memory    - Memory profile (configurable duration)"
    echo "  soak      - Long soak test (1h default)"
    echo "  crash     - Crash recovery tests"
    echo "  diagnose  - Diagnostic on running instance"
    echo "  chat-e2e  - Full chat e2e validation gate (~20min)"
    echo "  single-db - Full suite in singleDb mode"
    echo ""
    echo "All modes pass extra arguments through to the underlying script."
    echo ""
    echo "Examples:"
    echo "  perf/run.sh quick"
    echo "  perf/run.sh load -- --duration=300 --concurrency=20"
    echo "  perf/run.sh memory -- --duration=600 --check-leaks"
    echo "  perf/run.sh soak -- --duration=86400 --single-db"
    echo "  perf/run.sh single-db"
    exit 1
    ;;
esac

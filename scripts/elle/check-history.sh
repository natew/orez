#!/usr/bin/env bash
set -euo pipefail

# Runs the pinned elle-cli list-append analysis on a REAL recorded Orez workload
# history (history.jsonl produced by a consistency lane, e.g.
# atomic-visibility-lane.ts). Projects the history to Jepsen/Elle JSON, checks it
# against the serializable model, and fails on false, unknown, or malformed
# output. Saves the projected history and the elle report as artifacts.
#
# The model is `serializable`. The atomic-visibility workload records a single
# authoritative multi-key append plus non-writing complete-list reads with
# none-or-all visibility, so an empty read orders before the append and a full
# read after it: a serial order exists exactly because visibility is atomic.
# Realtime/strict variants are deliberately NOT claimed for asynchronous cache
# reads (see plans/consistency-validation-architecture.md).
#
#   scripts/elle/check-history.sh <history.jsonl> [results-dir]
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
source "$ROOT/scripts/elle/lib.sh"

# resolve relative paths before any cd; the projection step runs inside harness/.
elle_abspath() {
  case "$1" in
    /*) printf '%s\n' "$1" ;;
    *) printf '%s\n' "$PWD/$1" ;;
  esac
}

HISTORY=${1:-}
if [[ -z "$HISTORY" ]]; then
  # default to the newest atomic-visibility lane run under the harness results.
  # run dirs are controlled hex-suffixed names, so an mtime-sorted ls is safe.
  # shellcheck disable=SC2012
  HISTORY=$(ls -t "$ROOT"/harness/target/consistency/atomic-visibility/*/history.jsonl 2>/dev/null | head -1 || true)
  [[ -n "$HISTORY" ]] || elle_fail "no history.jsonl given and none found under harness/target/consistency/atomic-visibility"
fi
HISTORY=$(elle_abspath "$HISTORY")
[[ -f "$HISTORY" ]] || elle_fail "history not found: $HISTORY"

RESULTS_DIR=$(elle_abspath "${2:-"$ROOT/target/elle-workload"}")
rm -rf "$RESULTS_DIR"
mkdir -p "$RESULTS_DIR"

elle_require_java "$RESULTS_DIR"
JAR_PATH=$(elle_ensure_jar)

PROJECTED="$RESULTS_DIR/history.elle.json"
REPORT="$RESULTS_DIR/elle-report.json"

# project the recorded history to list-append JSON (non-vacuity enforced there).
( cd "$ROOT/harness" && bun src/consistency/elle-project.ts --history "$HISTORY" --out "$PROJECTED" ) \
  2>"$RESULTS_DIR/project.stderr"

MODEL=list-append
CONSISTENCY=serializable

set +e
java -jar "$JAR_PATH" \
  --model "$MODEL" \
  --consistency-models "$CONSISTENCY" \
  --verbose \
  "$PROJECTED" >"$REPORT" 2>"$RESULTS_DIR/elle.stderr"
elle_exit=$?
set -e
printf '%s\n' "$elle_exit" >"$RESULTS_DIR/elle.exit"

cat >"$RESULTS_DIR/checked-workload.txt" <<EOF
checker=elle-cli
version=$ELLE_VERSION
model=$MODEL
consistency-model=$CONSISTENCY
jar-sha256=$ELLE_JAR_SHA256
history=$HISTORY
scope=real Orez workload history projected via projectElleListAppend
EOF

python3 - "$REPORT" <<'PY'
import json
import pathlib
import sys

report_path = pathlib.Path(sys.argv[1])
try:
    report = json.loads(report_path.read_text())
except Exception as error:
    raise SystemExit(f"elle returned malformed JSON: {error}") from error
verdict = report.get("valid?")
if verdict != True:  # noqa: E712 -- must be boolean true, not "unknown"/false/None
    raise SystemExit(
        f"elle did not return valid=true (got {verdict!r}); "
        f"anomalies: {report.get('anomaly-types')!r}"
    )
PY

echo "elle workload check: PASS (list-append/serializable, valid=true) on $HISTORY"

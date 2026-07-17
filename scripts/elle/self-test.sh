#!/usr/bin/env bash
set -euo pipefail

# Checker-boundary self-test: proves the pinned elle-cli binary accepts a
# known-valid list-append history and rejects a known G1c cycle. It does not
# check an Orez workload history; scripts/elle/check-history.sh does that against
# a recorded lane history.
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
source "$ROOT/scripts/elle/lib.sh"

RESULTS_DIR=${ELLE_RESULTS_DIR:-"$ROOT/target/elle-self-test"}
FIXTURES="$ROOT/harness/src/consistency/fixtures"

rm -rf "$RESULTS_DIR"
mkdir -p "$RESULTS_DIR"

elle_require_java "$RESULTS_DIR"
JAR_PATH=$(elle_ensure_jar)

cat >"$RESULTS_DIR/checker-boundary.txt" <<EOF
checker=elle-cli
version=$ELLE_VERSION
model=list-append
consistency-model=serializable
zip-sha256=$ELLE_ZIP_SHA256
jar-sha256=$ELLE_JAR_SHA256
scope=checker self-tests only; no Orez workload history was checked
EOF
cp "$FIXTURES/elle-valid.json" "$RESULTS_DIR/"
cp "$FIXTURES/elle-invalid.json" "$RESULTS_DIR/"

common=(
  java -jar "$JAR_PATH"
  --model list-append
  --consistency-models serializable
  --anomalies G1c
)

set +e
"${common[@]}" "$FIXTURES/elle-valid.json" \
  >"$RESULTS_DIR/valid.stdout" 2>"$RESULTS_DIR/valid.stderr"
valid_exit=$?
"${common[@]}" --verbose "$FIXTURES/elle-invalid.json" \
  >"$RESULTS_DIR/g1c-invalid.stdout.json" 2>"$RESULTS_DIR/g1c-invalid.stderr"
invalid_exit=$?
set -e
printf '%s\n' "$valid_exit" >"$RESULTS_DIR/valid.exit"
printf '%s\n' "$invalid_exit" >"$RESULTS_DIR/g1c-invalid.exit"

[[ "$valid_exit" -eq 0 ]] || elle_fail "known-valid fixture exited $valid_exit, expected 0"
[[ "$invalid_exit" -eq 1 ]] || elle_fail "known-invalid fixture exited $invalid_exit, expected 1"

python3 - "$RESULTS_DIR/valid.stdout" "$RESULTS_DIR/g1c-invalid.stdout.json" <<'PY'
import json
import pathlib
import sys

valid_path, invalid_path = map(pathlib.Path, sys.argv[1:])
valid_lines = [line.strip() for line in valid_path.read_text().splitlines() if line.strip()]
if len(valid_lines) != 1 or valid_lines[0].split()[-1:] != ["true"]:
    raise SystemExit(f"known-valid fixture returned malformed/unexpected output: {valid_lines!r}")
if any(value in valid_lines[0].lower() for value in ("false", "unknown")):
    raise SystemExit(f"known-valid fixture was not valid: {valid_lines[0]!r}")

try:
    invalid = json.loads(invalid_path.read_text())
except Exception as error:
    raise SystemExit(f"known-invalid fixture returned malformed JSON: {error}") from error
if invalid.get("valid?") is not False:
    raise SystemExit(f"known-invalid fixture did not return false: {invalid.get('valid?')!r}")
if "G1c" not in invalid.get("anomaly-types", []):
    raise SystemExit(f"known-invalid fixture did not report G1c: {invalid.get('anomaly-types')!r}")
prohibited = invalid.get("not", []) + invalid.get("also-not", [])
if "serializable" not in prohibited:
    raise SystemExit(f"G1c fixture did not violate serializable: {prohibited!r}")
PY

echo "elle self-test: PASS (official $ELLE_VERSION, list-append/serializable, valid=true, invalid=G1c)"

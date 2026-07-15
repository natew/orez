#!/usr/bin/env bash
set -euo pipefail

# This is a checker-boundary self-test. It does not check an Orez workload.
VERSION=0.1.9
ZIP_NAME="elle-cli-bin-${VERSION}.zip"
ZIP_SHA256=7bb21b1c68580cd63816abee7655c68023b837bcca91eac9025674e4fe1ff12c
JAR_SHA256=c9ba9b9fd32640e73d632cb5f15069c162ba6528a67f27a878767187c59f539a
RELEASE_URL="https://github.com/ligurio/elle-cli/releases/download/${VERSION}/${ZIP_NAME}"

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
CACHE_DIR=${ELLE_CACHE_DIR:-"${XDG_CACHE_HOME:-$HOME/.cache}/orez/elle-cli"}
VERSION_DIR="$CACHE_DIR/$VERSION"
ZIP_PATH="$VERSION_DIR/$ZIP_NAME"
JAR_PATH="$VERSION_DIR/release/target/elle-cli-${VERSION}-standalone.jar"
RESULTS_DIR=${ELLE_RESULTS_DIR:-"$ROOT/target/elle-self-test"}
FIXTURES="$ROOT/harness/src/consistency/fixtures"

fail() {
  echo "elle self-test: $*" >&2
  exit 1
}

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

require_hash() {
  local path=$1 expected=$2 label=$3 actual
  actual=$(sha256 "$path")
  [[ "$actual" == "$expected" ]] || fail "$label SHA-256 mismatch: expected $expected, got $actual"
}

rm -rf "$RESULTS_DIR"
mkdir -p "$RESULTS_DIR" "$VERSION_DIR"

java -version >"$RESULTS_DIR/java-version.txt" 2>&1 || fail "java is unavailable"
grep -Eq 'version "21([.]|\")' "$RESULTS_DIR/java-version.txt" ||
  fail "Java 21 is required (see $RESULTS_DIR/java-version.txt)"

if [[ -f "$ZIP_PATH" ]] && [[ "$(sha256 "$ZIP_PATH")" != "$ZIP_SHA256" ]]; then
  rm -f "$ZIP_PATH"
fi
if [[ ! -f "$ZIP_PATH" ]]; then
  download="$ZIP_PATH.downloading.$$"
  rm -f "$download"
  curl --fail --location --retry 3 --output "$download" "$RELEASE_URL"
  require_hash "$download" "$ZIP_SHA256" "official release ZIP"
  mv "$download" "$ZIP_PATH"
fi
require_hash "$ZIP_PATH" "$ZIP_SHA256" "official release ZIP"

if [[ ! -f "$JAR_PATH" ]] || [[ "$(sha256 "$JAR_PATH")" != "$JAR_SHA256" ]]; then
  extract="$VERSION_DIR/release.extracting.$$"
  rm -rf "$extract" "$VERSION_DIR/release"
  mkdir -p "$extract"
  unzip -q "$ZIP_PATH" -d "$extract"
  mv "$extract" "$VERSION_DIR/release"
fi
[[ -f "$JAR_PATH" ]] || fail "release did not contain the expected standalone JAR"
require_hash "$JAR_PATH" "$JAR_SHA256" "embedded standalone JAR"

cat >"$RESULTS_DIR/checker-boundary.txt" <<EOF
checker=elle-cli
version=$VERSION
model=list-append
consistency-model=serializable
zip-sha256=$ZIP_SHA256
jar-sha256=$JAR_SHA256
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

[[ "$valid_exit" -eq 0 ]] || fail "known-valid fixture exited $valid_exit, expected 0"
[[ "$invalid_exit" -eq 1 ]] || fail "known-invalid fixture exited $invalid_exit, expected 1"

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

echo "elle self-test: PASS (official $VERSION, list-append/serializable, valid=true, invalid=G1c)"

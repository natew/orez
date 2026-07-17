# shellcheck shell=bash
# shared provisioning for the pinned elle-cli checker. sourced by self-test.sh
# (checker-boundary fixtures) and check-history.sh (real Orez workload history).
# not executable on its own.

# pinned official elle-cli 0.1.9 release. source revision
# 0e3fd6ea923f8c2f1ee89f153e0e413530b1fa43, embedding Elle 0.2.6 and Jepsen
# 0.3.11. the standalone jar hash is verified before every run.
ELLE_VERSION=0.1.9
ELLE_ZIP_NAME="elle-cli-bin-${ELLE_VERSION}.zip"
ELLE_ZIP_SHA256=7bb21b1c68580cd63816abee7655c68023b837bcca91eac9025674e4fe1ff12c
ELLE_JAR_SHA256=c9ba9b9fd32640e73d632cb5f15069c162ba6528a67f27a878767187c59f539a
ELLE_RELEASE_URL="https://github.com/ligurio/elle-cli/releases/download/${ELLE_VERSION}/${ELLE_ZIP_NAME}"

elle_fail() {
  echo "elle: $*" >&2
  exit 1
}

elle_sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

elle_require_hash() {
  local path=$1 expected=$2 label=$3 actual
  actual=$(elle_sha256 "$path")
  [[ "$actual" == "$expected" ]] || elle_fail "$label SHA-256 mismatch: expected $expected, got $actual"
}

# Java 17 cannot load the transitive Jepsen dependency graph; the jar needs Java
# 21 or newer (CI pins temurin 21, verified working with 21 and 25).
elle_require_java() {
  local results_dir=$1 major
  java -version >"$results_dir/java-version.txt" 2>&1 || elle_fail "java is unavailable"
  major=$(grep -oE 'version "[0-9]+' "$results_dir/java-version.txt" | grep -oE '[0-9]+$' | head -1)
  [[ -n "$major" ]] || elle_fail "could not parse java version (see $results_dir/java-version.txt)"
  [[ "$major" -ge 21 ]] ||
    elle_fail "Java 21+ required, found major $major (see $results_dir/java-version.txt)"
}

# downloads (if needed), verifies, and extracts the pinned jar into the cache.
# echoes the verified jar path on stdout.
elle_ensure_jar() {
  local cache_dir=${ELLE_CACHE_DIR:-"${XDG_CACHE_HOME:-$HOME/.cache}/orez/elle-cli"}
  local version_dir="$cache_dir/$ELLE_VERSION"
  local zip_path="$version_dir/$ELLE_ZIP_NAME"
  local jar_path="$version_dir/release/target/elle-cli-${ELLE_VERSION}-standalone.jar"
  mkdir -p "$version_dir"

  if [[ -f "$zip_path" ]] && [[ "$(elle_sha256 "$zip_path")" != "$ELLE_ZIP_SHA256" ]]; then
    rm -f "$zip_path"
  fi
  if [[ ! -f "$zip_path" ]]; then
    local download="$zip_path.downloading.$$"
    rm -f "$download"
    curl --fail --location --retry 3 --output "$download" "$ELLE_RELEASE_URL" >&2
    elle_require_hash "$download" "$ELLE_ZIP_SHA256" "official release ZIP"
    mv "$download" "$zip_path"
  fi
  elle_require_hash "$zip_path" "$ELLE_ZIP_SHA256" "official release ZIP"

  if [[ ! -f "$jar_path" ]] || [[ "$(elle_sha256 "$jar_path")" != "$ELLE_JAR_SHA256" ]]; then
    local extract="$version_dir/release.extracting.$$"
    rm -rf "$extract" "$version_dir/release"
    mkdir -p "$extract"
    unzip -q "$zip_path" -d "$extract"
    mv "$extract" "$version_dir/release"
  fi
  [[ -f "$jar_path" ]] || elle_fail "release did not contain the expected standalone JAR"
  elle_require_hash "$jar_path" "$ELLE_JAR_SHA256" "embedded standalone JAR"
  echo "$jar_path"
}

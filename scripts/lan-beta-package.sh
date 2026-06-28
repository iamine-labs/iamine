#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/lan-beta-package.sh [--output-dir DIR] [--binary PATH] [--no-build]

Creates a portable IAMINE LAN beta package under DIR without installing,
starting, stopping, or configuring services.
USAGE
}

OUTPUT_DIR="target/iamine-lan-beta-packages"
BINARY_PATH=""
BUILD_BINARY=1

while [ "$#" -gt 0 ]; do
  case "$1" in
    --output-dir)
      [ "$#" -ge 2 ] || { echo "missing value for --output-dir" >&2; exit 2; }
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --binary)
      [ "$#" -ge 2 ] || { echo "missing value for --binary" >&2; exit 2; }
      BINARY_PATH="$2"
      shift 2
      ;;
    --no-build)
      BUILD_BINARY=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

if [ "$BUILD_BINARY" -eq 1 ]; then
  cargo build -p iamine-node
fi

if [ -z "$BINARY_PATH" ]; then
  BINARY_PATH="target/debug/iamine-node"
fi

if [ ! -f "$BINARY_PATH" ]; then
  echo "binary not found: $BINARY_PATH" >&2
  exit 3
fi

GIT_SHA="$(git rev-parse HEAD)"
GIT_TREE="$(git rev-parse 'HEAD^{tree}')"
GIT_SHORT="$(git rev-parse --short HEAD)"
PACKAGE_NAME="iamine-lan-beta-${GIT_SHORT}"
PACKAGE_ROOT="${OUTPUT_DIR}/${PACKAGE_NAME}"
ARCHIVE_PATH="${OUTPUT_DIR}/${PACKAGE_NAME}.tar.gz"

if [ -e "$PACKAGE_ROOT" ]; then
  echo "package directory already exists: $PACKAGE_ROOT" >&2
  exit 4
fi

if [ -e "$ARCHIVE_PATH" ]; then
  echo "package archive already exists: $ARCHIVE_PATH" >&2
  exit 4
fi

mkdir -p "$PACKAGE_ROOT/bin"
mkdir -p "$PACKAGE_ROOT/docs"
mkdir -p "$PACKAGE_ROOT/env"
mkdir -p "$PACKAGE_ROOT/systemd"
mkdir -p "$PACKAGE_ROOT/launchd"

cp "$BINARY_PATH" "$PACKAGE_ROOT/bin/iamine-node"
cp packaging/lan-beta/README.md "$PACKAGE_ROOT/docs/README.md"
cp packaging/lan-beta/env/iamine-worker.env.example "$PACKAGE_ROOT/env/iamine-worker.env.example"
cp packaging/lan-beta/systemd/iamine-worker@.service "$PACKAGE_ROOT/systemd/iamine-worker@.service"
cp packaging/lan-beta/launchd/com.iamine.worker.plist.template "$PACKAGE_ROOT/launchd/com.iamine.worker.plist.template"

hash_file() {
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  elif command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    echo "no sha256 tool found" >&2
    exit 5
  fi
}

BINARY_SHA256="$(hash_file "$PACKAGE_ROOT/bin/iamine-node")"

cat > "$PACKAGE_ROOT/manifest.json" <<MANIFEST
{
  "schema_version": "1.0.0",
  "feature": "LAN-INFERENCE-BETA-PACKAGING-001",
  "package": "${PACKAGE_NAME}",
  "git_commit": "${GIT_SHA}",
  "git_tree": "${GIT_TREE}",
  "binary": {
    "path": "bin/iamine-node",
    "sha256": "${BINARY_SHA256}"
  },
  "artifacts": [
    "docs/README.md",
    "env/iamine-worker.env.example",
    "systemd/iamine-worker@.service",
    "launchd/com.iamine.worker.plist.template"
  ],
  "runtime_effects": {
    "worker_started": false,
    "worker_stopped": false,
    "p2p_started": false,
    "pubsub_started": false,
    "model_download_started": false,
    "model_load_started": false,
    "inference_started": false
  }
}
MANIFEST

tar -C "$OUTPUT_DIR" -czf "$ARCHIVE_PATH" "$PACKAGE_NAME"

echo "package_dir=$PACKAGE_ROOT"
echo "archive=$ARCHIVE_PATH"
echo "binary_sha256=$BINARY_SHA256"

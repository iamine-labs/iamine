#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/install.sh [--prefix DIR] [--bin-dir DIR] [--dry-run] [--yes]

Installs the IAMINE LAN beta package into an operator-controlled prefix.
It copies only package files. It does not start workers, load services,
download models, mutate node config, or run inference.
USAGE
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PACKAGE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PREFIX="${IAMINE_INSTALL_PREFIX:-$HOME/.local}"
BIN_DIR=""
DRY_RUN=0
YES=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --prefix)
      [ "$#" -ge 2 ] || { echo "missing value for --prefix" >&2; exit 2; }
      PREFIX="$2"
      shift 2
      ;;
    --bin-dir)
      [ "$#" -ge 2 ] || { echo "missing value for --bin-dir" >&2; exit 2; }
      BIN_DIR="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    --yes)
      YES=1
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

if [ -z "$BIN_DIR" ]; then
  BIN_DIR="$PREFIX/bin"
fi
SHARE_DIR="$PREFIX/share/iamine"

require_file() {
  [ -f "$PACKAGE_ROOT/$1" ] || {
    echo "package is missing required file: $1" >&2
    exit 3
  }
}

copy_file() {
  src="$1"
  dst="$2"
  if [ "$DRY_RUN" -eq 1 ]; then
    printf 'copy %s -> %s\n' "$src" "$dst"
    return 0
  fi
  mkdir -p "$(dirname "$dst")"
  cp "$src" "$dst"
}

require_file "bin/iamine-node"
require_file "docs/README.md"
require_file "docs/INSTALL.md"
require_file "env/iamine-worker.env.example"
require_file "systemd/iamine-worker@.service"
require_file "launchd/com.iamine.worker.plist.template"
require_file "manifest.json"

if [ "$DRY_RUN" -eq 0 ] && [ "$YES" -eq 0 ]; then
  echo "refusing to install without --yes; use --dry-run to preview" >&2
  exit 4
fi

cat <<PLAN
IAMINE LAN beta install plan
package: $PACKAGE_ROOT
prefix: $PREFIX
binary: $BIN_DIR/iamine-node
share: $SHARE_DIR
dry_run: $DRY_RUN
PLAN

copy_file "$PACKAGE_ROOT/bin/iamine-node" "$BIN_DIR/iamine-node"
if [ "$DRY_RUN" -eq 0 ]; then
  chmod 0755 "$BIN_DIR/iamine-node"
fi
copy_file "$PACKAGE_ROOT/manifest.json" "$SHARE_DIR/manifest.json"
copy_file "$PACKAGE_ROOT/docs/README.md" "$SHARE_DIR/docs/README.md"
copy_file "$PACKAGE_ROOT/docs/INSTALL.md" "$SHARE_DIR/docs/INSTALL.md"
copy_file "$PACKAGE_ROOT/env/iamine-worker.env.example" "$SHARE_DIR/env/iamine-worker.env.example"
copy_file "$PACKAGE_ROOT/systemd/iamine-worker@.service" "$SHARE_DIR/systemd/iamine-worker@.service"
copy_file "$PACKAGE_ROOT/launchd/com.iamine.worker.plist.template" "$SHARE_DIR/launchd/com.iamine.worker.plist.template"

cat <<NEXT
install_status=success
runtime_effects=false

Next checks:
  $BIN_DIR/iamine-node --help
  $BIN_DIR/iamine-node lan doctor --json
  $BIN_DIR/iamine-node worker lifecycle readiness --json

Service templates were copied for review only. Load or enable services manually
with your operating-system tooling after editing local paths.
NEXT

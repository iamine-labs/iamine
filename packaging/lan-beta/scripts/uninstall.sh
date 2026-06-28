#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/uninstall.sh [--prefix DIR] [--bin-dir DIR] [--dry-run] [--yes]

Removes files installed by the IAMINE LAN beta package from the selected prefix.
It does not stop workers, unload services, delete models, delete node config,
delete hardware profiles, or delete logs.
USAGE
}

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

if [ "$DRY_RUN" -eq 0 ] && [ "$YES" -eq 0 ]; then
  echo "refusing to uninstall without --yes; use --dry-run to preview" >&2
  exit 4
fi

remove_file() {
  path="$1"
  if [ "$DRY_RUN" -eq 1 ]; then
    printf 'remove %s\n' "$path"
    return 0
  fi
  rm -f "$path"
}

remove_empty_dir() {
  path="$1"
  if [ "$DRY_RUN" -eq 1 ]; then
    printf 'rmdir-if-empty %s\n' "$path"
    return 0
  fi
  rmdir "$path" 2>/dev/null || true
}

cat <<PLAN
IAMINE LAN beta uninstall plan
prefix: $PREFIX
binary: $BIN_DIR/iamine-node
share: $SHARE_DIR
dry_run: $DRY_RUN
PLAN

remove_file "$BIN_DIR/iamine-node"
remove_file "$SHARE_DIR/manifest.json"
remove_file "$SHARE_DIR/docs/README.md"
remove_file "$SHARE_DIR/docs/INSTALL.md"
remove_file "$SHARE_DIR/env/iamine-worker.env.example"
remove_file "$SHARE_DIR/systemd/iamine-worker@.service"
remove_file "$SHARE_DIR/launchd/com.iamine.worker.plist.template"

remove_empty_dir "$SHARE_DIR/docs"
remove_empty_dir "$SHARE_DIR/env"
remove_empty_dir "$SHARE_DIR/systemd"
remove_empty_dir "$SHARE_DIR/launchd"
remove_empty_dir "$SHARE_DIR"

cat <<DONE
uninstall_status=success
runtime_effects=false

Preserved by design:
  model stores
  node configuration
  hardware profiles
  logs
  service-manager state
DONE

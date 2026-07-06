#!/usr/bin/env bash
set -u

usage() {
  cat <<'USAGE'
Usage:
  scripts/first-run-preflight.sh [--binary PATH] [--port N] [--skip-lan-smoke]

Runs the IAMINE LAN beta first-run preflight. The preflight validates the
package binary, local diagnostics, node config status, worker lifecycle
readiness, model catalog access, and a bounded cluster-status LAN smoke.

It does not start workers, install services, download models, load models, or
run inference. Use --skip-lan-smoke to skip the bounded cluster-status smoke.
USAGE
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PACKAGE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BINARY_PATH="$PACKAGE_ROOT/bin/iamine-node"
WORKER_PORT="${IAMINE_FIRST_RUN_PREFLIGHT_PORT:-9000}"
RUN_LAN_SMOKE=1

while [ "$#" -gt 0 ]; do
  case "$1" in
    --binary)
      [ "$#" -ge 2 ] || { echo "missing value for --binary" >&2; exit 2; }
      BINARY_PATH="$2"
      shift 2
      ;;
    --port)
      [ "$#" -ge 2 ] || { echo "missing value for --port" >&2; exit 2; }
      WORKER_PORT="$2"
      shift 2
      ;;
    --skip-lan-smoke)
      RUN_LAN_SMOKE=0
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

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/iamine-first-run-preflight.XXXXXX")"
cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

PASS_COUNT=0
WARN_COUNT=0
FAIL_COUNT=0

record() {
  id="$1"
  status="$2"
  message="$3"

  case "$status" in
    pass) PASS_COUNT=$((PASS_COUNT + 1)) ;;
    warn) WARN_COUNT=$((WARN_COUNT + 1)) ;;
    fail) FAIL_COUNT=$((FAIL_COUNT + 1)) ;;
    *) FAIL_COUNT=$((FAIL_COUNT + 1)); status="fail"; message="invalid preflight status" ;;
  esac

  printf 'check=%s status=%s message=%s\n' "$id" "$status" "$message"
}

require_python() {
  if command -v python3 >/dev/null 2>&1; then
    record "json_parser" "pass" "python3 is available for JSON validation"
    return 0
  fi

  record "json_parser" "fail" "python3 is required for JSON validation"
  return 1
}

json_probe() {
  file="$1"
  mode="$2"
  python3 - "$file" "$mode" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
mode = sys.argv[2]
lines = path.read_text(errors="replace").splitlines()
start = next((i for i, line in enumerate(lines) if line.lstrip().startswith(("{", "["))), None)
if start is None:
    print("no_json_payload")
    sys.exit(3)

payload = "\n".join(lines[start:])
obj = json.loads(payload)

if mode == "runtime_effects_false":
    effects = obj.get("runtime_side_effects") or obj.get("runtime_effects")
    if not isinstance(effects, dict):
        print("missing_runtime_effects")
        sys.exit(4)
    bad = [key for key, value in effects.items() if value is not False]
    if bad:
        print("runtime_effects_not_false:" + ",".join(sorted(bad)))
        sys.exit(5)
    print("runtime_effects_false")
elif mode == "overall_status":
    status = obj.get("overall_status") or obj.get("status")
    if isinstance(status, str):
        print(status.lower())
    else:
        print("unknown")
elif mode == "parse":
    print("json_parse_ok")
else:
    print("unknown_mode")
    sys.exit(6)
PY
}

run_command() {
  id="$1"
  success_message="$2"
  shift 2
  output="$TMP_DIR/$id.out"

  if "$@" >"$output" 2>&1; then
    record "$id" "pass" "$success_message"
    return 0
  fi

  record "$id" "fail" "command failed"
  return 1
}

run_json_command() {
  id="$1"
  status_policy="$2"
  shift 2
  output="$TMP_DIR/$id.out"

  if ! "$@" >"$output" 2>&1; then
    record "$id" "fail" "command failed"
    return 1
  fi

  if ! json_probe "$output" parse >/dev/null; then
    record "$id" "fail" "JSON output is not parseable"
    return 1
  fi

  if [ "$status_policy" = "runtime_effects_false" ]; then
    if ! json_probe "$output" runtime_effects_false >/dev/null; then
      record "$id" "fail" "runtime effect contract was not false"
      return 1
    fi
  fi

  if [ "$status_policy" = "overall_status" ]; then
    status="$(json_probe "$output" overall_status || printf 'fail')"
    case "$status" in
      pass|manual|not_run)
        record "$id" "pass" "JSON parsed with overall status $status"
        ;;
      warn)
        record "$id" "warn" "JSON parsed with overall status warn"
        ;;
      fail|blocked)
        record "$id" "fail" "JSON parsed with blocking status $status"
        return 1
        ;;
      *)
        record "$id" "warn" "JSON parsed with unknown status"
        ;;
    esac
    return 0
  fi

  record "$id" "pass" "JSON output parsed"
}

check_port() {
  case "$WORKER_PORT" in
    ''|*[!0-9]*)
      record "worker_port" "fail" "worker port must be numeric"
      return 1
      ;;
    *)
      if [ "$WORKER_PORT" -lt 1 ] || [ "$WORKER_PORT" -gt 65535 ]; then
        record "worker_port" "fail" "worker port is outside TCP range"
        return 1
      fi
      record "worker_port" "pass" "worker port is valid"
      ;;
  esac
}

check_package_manifest() {
  manifest="$PACKAGE_ROOT/manifest.json"
  if [ ! -f "$manifest" ]; then
    record "package_manifest" "warn" "package manifest is not present in inferred package root"
    return 0
  fi

  if json_probe "$manifest" parse >/dev/null; then
    record "package_manifest" "pass" "package manifest JSON is parseable"
  else
    record "package_manifest" "fail" "package manifest JSON is not parseable"
    return 1
  fi
}

check_scripts() {
  if [ ! -x "$SCRIPT_DIR/first-run-preflight.sh" ]; then
    record "package_scripts" "warn" "first-run preflight script is not executable"
    return 0
  fi

  non_executable_helpers=0
  for script in install.sh uninstall.sh; do
    if [ -e "$SCRIPT_DIR/$script" ] && [ ! -x "$SCRIPT_DIR/$script" ]; then
      non_executable_helpers=1
    fi
  done

  if [ "$non_executable_helpers" -eq 1 ]; then
    record "package_scripts" "warn" "one or more package helper scripts are not executable"
    return 0
  fi

  record "package_scripts" "pass" "available package helper scripts are executable"
}

echo "IAMINE LAN beta first-run preflight"
echo "binary=$BINARY_PATH"
echo "worker_port=$WORKER_PORT"
echo "lan_smoke=$RUN_LAN_SMOKE"

require_python || true
check_port || true
check_package_manifest || true
check_scripts || true

if [ ! -x "$BINARY_PATH" ]; then
  record "binary_executable" "fail" "iamine-node binary is missing or not executable"
else
  record "binary_executable" "pass" "iamine-node binary is executable"
  run_command "binary_help" "binary help command returned success" "$BINARY_PATH" --help || true
  run_json_command "node_config_status" "runtime_effects_false" \
    "$BINARY_PATH" node config status --json || true
  run_json_command "lan_doctor" "overall_status" \
    "$BINARY_PATH" lan doctor --json || true
  run_json_command "worker_lifecycle_status" "overall_status" \
    "$BINARY_PATH" worker lifecycle status --port="$WORKER_PORT" --json || true
  run_json_command "worker_lifecycle_readiness" "overall_status" \
    "$BINARY_PATH" worker lifecycle readiness --port="$WORKER_PORT" --json || true
  run_command "models_catalog" "model catalog command returned success" \
    "$BINARY_PATH" models catalog || true

  if [ "$RUN_LAN_SMOKE" -eq 1 ]; then
    run_json_command "cluster_status_json" "parse" \
      "$BINARY_PATH" cluster status --json || true
  else
    record "cluster_status_json" "warn" "cluster-status LAN smoke skipped by operator"
  fi
fi

if [ "$FAIL_COUNT" -gt 0 ]; then
  FINAL_STATUS="fail"
elif [ "$WARN_COUNT" -gt 0 ]; then
  FINAL_STATUS="warn"
else
  FINAL_STATUS="pass"
fi

printf 'FIRST_RUN_PREFLIGHT_STATUS=%s\n' "$FINAL_STATUS"
printf 'FIRST_RUN_PREFLIGHT_PASS_COUNT=%s\n' "$PASS_COUNT"
printf 'FIRST_RUN_PREFLIGHT_WARN_COUNT=%s\n' "$WARN_COUNT"
printf 'FIRST_RUN_PREFLIGHT_FAIL_COUNT=%s\n' "$FAIL_COUNT"

if [ "$FAIL_COUNT" -gt 0 ]; then
  exit 1
fi

exit 0

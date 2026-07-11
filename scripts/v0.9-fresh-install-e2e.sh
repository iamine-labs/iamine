#!/usr/bin/env bash
set -u

usage() {
  cat <<'USAGE'
Usage:
  scripts/v0.9-fresh-install-e2e.sh [--binary PATH] [--skip-cargo-tests] [--skip-lan-smoke] [--keep-artifacts]

Builds or reuses an iamine-node binary, creates a LAN beta package, installs it
into an isolated temporary prefix, and runs the v0.9 fresh-install E2E smoke.

The harness does not install system services, start workers, download models,
load models, or run real inference. It uses temporary state paths and mock-safe
CLI checks.
USAGE
}

BINARY_PATH=""
RUN_CARGO_TESTS=1
RUN_LAN_SMOKE=1
KEEP_ARTIFACTS=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --binary)
      [ "$#" -ge 2 ] || { echo "missing value for --binary" >&2; exit 2; }
      BINARY_PATH="$2"
      shift 2
      ;;
    --skip-cargo-tests)
      RUN_CARGO_TESTS=0
      shift
      ;;
    --skip-lan-smoke)
      RUN_LAN_SMOKE=0
      shift
      ;;
    --keep-artifacts)
      KEEP_ARTIFACTS=1
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
cd "$REPO_ROOT" || exit 2

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/iamine-v09-fresh-install-e2e.XXXXXX")"
cleanup() {
  if [ "$KEEP_ARTIFACTS" -eq 1 ]; then
    printf 'artifacts=%s\n' "$TMP_DIR"
  else
    rm -rf "$TMP_DIR"
  fi
}
trap cleanup EXIT

PASS_COUNT=0
WARN_COUNT=0
FAIL_COUNT=0

record() {
  local id="$1"
  local status="$2"
  local message="$3"

  case "$status" in
    pass) PASS_COUNT=$((PASS_COUNT + 1)) ;;
    warn) WARN_COUNT=$((WARN_COUNT + 1)) ;;
    fail) FAIL_COUNT=$((FAIL_COUNT + 1)) ;;
    *) FAIL_COUNT=$((FAIL_COUNT + 1)); status="fail"; message="invalid harness status" ;;
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
  local file="$1"
  local mode="$2"
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

if mode == "parse":
    print("json_parse_ok")
elif mode == "runtime_effects_false":
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
    print(status.lower() if isinstance(status, str) else "unknown")
elif mode == "cluster_stress_zero":
    if obj.get("passed") is not True:
        print("cluster_stress_not_passed")
        sys.exit(6)
    metrics = obj.get("metrics")
    if not isinstance(metrics, dict):
        print("cluster_stress_missing_metrics")
        sys.exit(7)
    total = metrics.get("total_requests")
    observed = metrics.get("observed_requests")
    failed = metrics.get("failed")
    timed_out = metrics.get("timed_out")
    if total != 0 or observed != 0 or failed != 0 or timed_out != 0:
        print("cluster_stress_expected_zero_requests")
        sys.exit(8)
    print("cluster_stress_zero_passed")
else:
    print("unknown_mode")
    sys.exit(9)
PY
}

run_command() {
  local id="$1"
  local success_message="$2"
  shift 2
  local output="$TMP_DIR/$id.out"

  if "$@" >"$output" 2>&1; then
    record "$id" "pass" "$success_message"
    return 0
  fi

  record "$id" "fail" "command failed; see artifact output"
  return 1
}

run_json_command() {
  local id="$1"
  local mode="$2"
  shift 2
  local output="$TMP_DIR/$id.out"

  if ! "$@" >"$output" 2>&1; then
    record "$id" "fail" "command failed; see artifact output"
    return 1
  fi

  if ! json_probe "$output" parse >/dev/null; then
    record "$id" "fail" "JSON output is not parseable"
    return 1
  fi

  case "$mode" in
    parse)
      record "$id" "pass" "JSON output parsed"
      ;;
    runtime_effects_false)
      if json_probe "$output" runtime_effects_false >/dev/null; then
        record "$id" "pass" "JSON parsed and runtime effects were false"
      else
        record "$id" "fail" "runtime effect contract was not false"
        return 1
      fi
      ;;
    overall_status)
      local status
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
      ;;
    cluster_stress_zero)
      if json_probe "$output" cluster_stress_zero >/dev/null; then
        record "$id" "pass" "cluster stress zero-request smoke passed"
      else
        record "$id" "fail" "cluster stress zero-request payload failed validation"
        return 1
      fi
      ;;
    *)
      record "$id" "fail" "unknown JSON validation mode"
      return 1
      ;;
  esac
}

run_preflight() {
  local preflight="$1"
  local binary="$2"
  local output="$TMP_DIR/first_run_preflight.out"

  local args
  args=(--binary "$binary")
  if [ "$RUN_LAN_SMOKE" -eq 0 ]; then
    args+=(--skip-lan-smoke)
  fi

  if ! "$preflight" "${args[@]}" >"$output" 2>&1; then
    record "first_run_preflight" "fail" "first-run preflight failed"
    return 1
  fi

  local status
  status="$(awk -F= '/^FIRST_RUN_PREFLIGHT_STATUS=/ {print $2}' "$output" | tail -n 1)"
  case "$status" in
    pass)
      record "first_run_preflight" "pass" "first-run preflight passed"
      ;;
    warn)
      record "first_run_preflight" "warn" "first-run preflight completed with warnings"
      ;;
    *)
      record "first_run_preflight" "fail" "first-run preflight did not report pass/warn"
      return 1
      ;;
  esac
}

echo "IAMINE v0.9 fresh-install E2E"
echo "run_cargo_tests=$RUN_CARGO_TESTS"
echo "lan_smoke=$RUN_LAN_SMOKE"

require_python || true

if [ "$RUN_CARGO_TESTS" -eq 1 ]; then
  run_command "network_protocol_version_tests" "network protocol-version tests passed" \
    cargo test -p iamine-network protocol_version || true
  run_command "network_secure_transport_tests" "secure transport tests passed" \
    cargo test -p iamine-network secure_transport || true
  run_command "network_testnet_admission_tests" "testnet admission tests passed" \
    cargo test -p iamine-network testnet_admission || true
  run_command "network_bootnode_tests" "bootnode tests passed" \
    cargo test -p iamine-network bootnode || true
  run_command "node_remote_inference_api_tests" "remote inference API tests passed" \
    cargo test -p iamine-node remote_inference_api || true
  run_command "node_testnet_observability_tests" "testnet observability tests passed" \
    cargo test -p iamine-node testnet_observability || true
  run_command "node_cluster_stress_tests" "cluster stress tests passed" \
    cargo test -p iamine-node cluster_stress || true
else
  record "cargo_tests" "warn" "cargo tests skipped by operator"
fi

if [ -z "$BINARY_PATH" ]; then
  if run_command "cargo_build_iamine_node" "iamine-node debug binary built" \
    cargo build -p iamine-node; then
    BINARY_PATH="target/debug/iamine-node"
  else
    BINARY_PATH="target/debug/iamine-node"
  fi
else
  record "cargo_build_iamine_node" "warn" "using operator-provided binary"
fi

if [ ! -x "$BINARY_PATH" ]; then
  record "source_binary" "fail" "iamine-node binary is missing or not executable"
else
  record "source_binary" "pass" "iamine-node binary is executable"
fi

PACKAGE_OUTPUT="$TMP_DIR/package.out"
PACKAGE_DIR=""
if [ -x "$BINARY_PATH" ]; then
  if scripts/lan-beta-package.sh --no-build --binary "$BINARY_PATH" --output-dir "$TMP_DIR/packages" >"$PACKAGE_OUTPUT" 2>&1; then
    PACKAGE_DIR="$(awk -F= '/^package_dir=/ {print $2}' "$PACKAGE_OUTPUT" | tail -n 1)"
    if [ -n "$PACKAGE_DIR" ] && [ -d "$PACKAGE_DIR" ]; then
      record "lan_beta_package" "pass" "LAN beta package assembled"
    else
      record "lan_beta_package" "fail" "package directory was not reported"
    fi
  else
    record "lan_beta_package" "fail" "LAN beta package assembly failed"
  fi
fi

INSTALL_PREFIX="$TMP_DIR/install-prefix"
INSTALLED_BINARY="$INSTALL_PREFIX/bin/iamine-node"
INSTALLED_PREFLIGHT="$INSTALL_PREFIX/share/iamine/scripts/first-run-preflight.sh"

if [ -n "$PACKAGE_DIR" ] && [ -x "$PACKAGE_DIR/scripts/install.sh" ]; then
  run_command "isolated_install" "package installed into isolated prefix" \
    "$PACKAGE_DIR/scripts/install.sh" --prefix "$INSTALL_PREFIX" --yes || true
else
  record "isolated_install" "fail" "package install script is unavailable"
fi

if [ -x "$INSTALLED_BINARY" ]; then
  record "installed_binary" "pass" "installed iamine-node binary is executable"
  run_command "installed_binary_help" "installed binary help returned success" \
    "$INSTALLED_BINARY" --help || true
else
  record "installed_binary" "fail" "installed binary missing or not executable"
fi

if [ -x "$INSTALLED_PREFLIGHT" ] && [ -x "$INSTALLED_BINARY" ]; then
  run_preflight "$INSTALLED_PREFLIGHT" "$INSTALLED_BINARY" || true
else
  record "first_run_preflight" "fail" "installed first-run preflight unavailable"
fi

STATE_DIR="$TMP_DIR/state"
IDENTITY_PATH="$STATE_DIR/identity/node.key"
CONFIG_PATH="$STATE_DIR/config/node_config.json"
mkdir -p "$STATE_DIR/identity" "$STATE_DIR/config"

if [ -x "$INSTALLED_BINARY" ]; then
  run_json_command "node_identity_init" "runtime_effects_false" \
    "$INSTALLED_BINARY" node identity init --path "$IDENTITY_PATH" --json || true
  run_json_command "node_identity_status" "runtime_effects_false" \
    "$INSTALLED_BINARY" node identity status --path "$IDENTITY_PATH" --json || true
  run_json_command "node_config_status" "runtime_effects_false" \
    "$INSTALLED_BINARY" node config status --path "$CONFIG_PATH" --json || true
  run_json_command "cluster_status_json" "parse" \
    "$INSTALLED_BINARY" cluster status --json || true
  run_json_command "cluster_stress_zero" "cluster_stress_zero" \
    "$INSTALLED_BINARY" cluster stress --requests 0 --profile testnet-load-resilience --json || true
else
  record "fresh_install_cli_smokes" "fail" "installed binary unavailable for CLI smokes"
fi

if [ "$FAIL_COUNT" -gt 0 ]; then
  FINAL_STATUS="fail"
elif [ "$WARN_COUNT" -gt 0 ]; then
  FINAL_STATUS="warn"
else
  FINAL_STATUS="pass"
fi

printf 'V09_FRESH_INSTALL_E2E_STATUS=%s\n' "$FINAL_STATUS"
printf 'V09_FRESH_INSTALL_E2E_PASS_COUNT=%s\n' "$PASS_COUNT"
printf 'V09_FRESH_INSTALL_E2E_WARN_COUNT=%s\n' "$WARN_COUNT"
printf 'V09_FRESH_INSTALL_E2E_FAIL_COUNT=%s\n' "$FAIL_COUNT"

if [ "$FAIL_COUNT" -gt 0 ]; then
  exit 1
fi

exit 0

#!/usr/bin/env bash
set -euo pipefail

required_failures=0
warning_count=0
skip_count=0

main_warn_lines=5000
main_fail_lines=5500
main_delta_fail_lines=150
large_file_warn_lines=900
large_file_fail_lines=1500

required_results=()
optional_results=()
guard_results=()

section() {
  printf '\n===== %s =====\n' "$1"
}

record_result() {
  local group="$1"
  local label="$2"
  local status="$3"

  case "$group" in
  required) required_results+=("$label: $status") ;;
  optional) optional_results+=("$label: $status") ;;
  guard) guard_results+=("$label: $status") ;;
  esac
}

mark_required_pass() {
  printf 'PASS: %s\n' "$1"
  record_result required "$1" PASS
}

mark_required_fail() {
  printf 'FAIL: %s\n' "$1"
  record_result required "$1" FAIL
  required_failures=$((required_failures + 1))
}

mark_optional_pass() {
  printf 'PASS: %s\n' "$1"
  record_result optional "$1" PASS
}

mark_optional_warn() {
  printf 'WARN: %s\n' "$1"
  record_result optional "$1" WARN
  warning_count=$((warning_count + 1))
}

mark_optional_skip() {
  printf 'SKIPPED: %s\n' "$1"
  record_result optional "$1" SKIPPED
  skip_count=$((skip_count + 1))
}

mark_guard_pass() {
  printf 'PASS: %s\n' "$1"
  record_result guard "$1" PASS
}

mark_guard_warn() {
  printf 'WARN: %s\n' "$1"
  record_result guard "$1" WARN
  warning_count=$((warning_count + 1))
}

mark_guard_skip() {
  printf 'SKIPPED: %s\n' "$1"
  record_result guard "$1" SKIPPED
  skip_count=$((skip_count + 1))
}

mark_guard_fail() {
  printf 'FAIL: %s\n' "$1"
  record_result guard "$1" FAIL
  required_failures=$((required_failures + 1))
}

run_required() {
  local label="$1"
  shift

  printf '+ %s\n' "$*"
  if "$@"; then
    mark_required_pass "$label"
  else
    mark_required_fail "$label"
  fi
}

run_optional_warn() {
  local label="$1"
  local probe="$2"
  local install_hint="$3"
  shift 3

  if eval "$probe" >/dev/null 2>&1; then
    printf '+ %s\n' "$*"
    if "$@"; then
      mark_optional_pass "$label"
    else
      mark_optional_warn "$label failed; informational until its baseline is classified"
    fi
  else
    mark_optional_skip "$label not available; install with: $install_hint"
  fi
}

run_optional_blocking() {
  local label="$1"
  local probe="$2"
  local install_hint="$3"
  shift 3

  if eval "$probe" >/dev/null 2>&1; then
    printf '+ %s\n' "$*"
    if "$@"; then
      mark_optional_pass "$label"
    else
      printf 'FAIL: %s detected a blocking issue\n' "$label"
      record_result optional "$label" FAIL
      required_failures=$((required_failures + 1))
    fi
  else
    mark_optional_skip "$label not available; install with: $install_hint"
  fi
}

line_count() {
  wc -l < "$1" | tr -d '[:space:]'
}

resolve_base_commit() {
  local base_ref="${QUALITY_GATE_BASE_REF:-origin/develop}"

  if git rev-parse --verify "$base_ref^{commit}" >/dev/null 2>&1; then
    git merge-base HEAD "$base_ref" 2>/dev/null ||
      git rev-parse "$base_ref^{commit}"
    return
  fi

  if git rev-parse --verify HEAD^ >/dev/null 2>&1; then
    git rev-parse HEAD^
    return
  fi

  return 1
}

repository_guard() {
  section "REPOSITORY GUARDS"

  local generated_matches
  generated_matches="$(
    git ls-files |
      rg -i '(^|/)(target|iamine-logs)(/|$)|(^|/)\.DS_Store$|\.(log|ndjson|gguf|safetensors)$' ||
      true
  )"
  if [ -n "$generated_matches" ]; then
    printf '%s\n' "$generated_matches"
    mark_guard_fail "tracked generated artifacts or local model binaries detected"
  else
    mark_guard_pass "no tracked generated artifacts or local model binaries"
  fi

  local sensitive_matches
  sensitive_matches="$(
    git ls-files |
      rg -i '(^|/)\.env($|\.)|\.(pem|key)$|(^|/)id_(rsa|ed25519)$|(^|/)secrets(\.|/|$)|wallet.*(key|secret)|private.*key' |
      rg -vi '(^|/)\.env\.(example|sample|template)$' ||
      true
  )"
  if [ -n "$sensitive_matches" ]; then
    printf '%s\n' "$sensitive_matches"
    mark_guard_fail "tracked sensitive files detected"
  else
    mark_guard_pass "no tracked sensitive files"
  fi
}

architecture_guard() {
  section "ARCHITECTURE GUARDS"

  local main_file="iamine-node/src/main.rs"
  if [ ! -f "$main_file" ]; then
    mark_guard_warn "$main_file missing; architecture guard could not inspect main.rs"
    return
  fi

  local main_lines
  main_lines="$(line_count "$main_file")"
  printf 'main.rs lines: %s\n' "$main_lines"
  if [ "$main_lines" -gt "$main_fail_lines" ]; then
    mark_guard_fail "main.rs exceeds the $main_fail_lines line hard limit"
  elif [ "$main_lines" -gt "$main_warn_lines" ]; then
    mark_guard_warn "main.rs exceeds the $main_warn_lines line warning threshold"
  else
    mark_guard_pass "main.rs is within the $main_warn_lines line warning threshold"
  fi

  local base_commit
  if base_commit="$(resolve_base_commit)"; then
    local base_lines
    local delta
    base_lines="$(git show "$base_commit:$main_file" | wc -l | tr -d '[:space:]')"
    delta=$((main_lines - base_lines))
    printf 'main.rs base commit: %s (%s lines)\n' "$base_commit" "$base_lines"
    printf 'main.rs delta: %s\n' "$delta"
    if [ "$delta" -gt "$main_delta_fail_lines" ]; then
      mark_guard_fail "main.rs grew by more than $main_delta_fail_lines lines relative to merge-base"
    else
      mark_guard_pass "main.rs growth is within the +$main_delta_fail_lines line hard limit"
    fi
  else
    mark_guard_skip "main.rs delta check; no merge-base is available"
  fi

  local large_warn_files=""
  local large_fail_files=""
  while IFS= read -r file; do
    [ "$file" = "$main_file" ] && continue
    local lines
    lines="$(line_count "$file")"
    if [ "$lines" -gt "$large_file_fail_lines" ]; then
      large_fail_files+="$lines $file"$'\n'
    elif [ "$lines" -gt "$large_file_warn_lines" ]; then
      large_warn_files+="$lines $file"$'\n'
    fi
  done < <(git ls-files '*.rs')

  if [ -n "$large_fail_files" ]; then
    printf '%s' "$large_fail_files"
    mark_guard_fail "one or more non-main Rust files exceed $large_file_fail_lines lines"
  elif [ -n "$large_warn_files" ]; then
    printf '%s' "$large_warn_files"
    mark_guard_warn "one or more non-main Rust files exceed $large_file_warn_lines lines"
  else
    mark_guard_pass "no non-main Rust file exceeds $large_file_warn_lines lines"
  fi

  local monitored_file
  for monitored_file in \
    "iamine-node/src/broadcast_runtime.rs" \
    "iamine-network/src/prompt_analyzer.rs"; do
    if [ -f "$monitored_file" ]; then
      local monitored_lines
      monitored_lines="$(line_count "$monitored_file")"
      printf '%s lines: %s\n' "$monitored_file" "$monitored_lines"
      if [ "$monitored_lines" -gt "$large_file_warn_lines" ]; then
        mark_guard_warn "$monitored_file regressed above $large_file_warn_lines lines"
      else
        mark_guard_pass "$monitored_file remains below $large_file_warn_lines lines"
      fi
    fi
  done

  if base_commit="$(resolve_base_commit)"; then
    local new_debt
    new_debt="$(
      git diff --unified=0 "$base_commit" -- '*.rs' |
        rg '^\+[^+].*(TODO|FIXME|\.unwrap\(\)|\.expect\(|panic!\()' ||
        true
    )"
    if [ -n "$new_debt" ]; then
      printf '%s\n' "$new_debt"
      mark_guard_warn "new Rust TODO/FIXME/unwrap/expect/panic markers detected; review before merge"
    else
      mark_guard_pass "no new Rust TODO/FIXME/unwrap/expect/panic markers detected"
    fi
  else
    mark_guard_skip "new Rust debt diff check; no merge-base is available"
  fi
}

print_result_group() {
  local heading="$1"
  shift

  printf '%s:\n' "$heading"
  if [ "$#" -eq 0 ]; then
    printf -- '- none\n'
    return
  fi

  local result
  for result in "$@"; do
    printf -- '- %s\n' "$result"
  done
}

print_summary() {
  section "QUALITY GATE SUMMARY"
  print_result_group "Required checks" "${required_results[@]+"${required_results[@]}"}"
  print_result_group "Optional checks" "${optional_results[@]+"${optional_results[@]}"}"
  print_result_group \
    "Architecture and repository guards" \
    "${guard_results[@]+"${guard_results[@]}"}"
  printf 'required_failures=%s\n' "$required_failures"
  printf 'warnings=%s\n' "$warning_count"
  printf 'skipped=%s\n' "$skip_count"

  if [ "$required_failures" -gt 0 ]; then
    printf 'QUALITY GATE RESULT: FAIL\n'
    return 1
  fi

  if [ "$warning_count" -gt 0 ] || [ "$skip_count" -gt 0 ]; then
    printf 'QUALITY GATE RESULT: PASS WITH WARNINGS\n'
  else
    printf 'QUALITY GATE RESULT: PASS\n'
  fi
}

printf 'IAMINE QUALITY GATE\n'

repository_guard
architecture_guard

if [ "${QUALITY_GATE_GUARDS_ONLY:-0}" = "1" ]; then
  printf '\nGuard-only mode enabled; command checks were not executed.\n'
  print_summary
  exit $?
fi

section "REQUIRED CHECKS"
run_required "cargo fmt --all -- --check" cargo fmt --all -- --check
run_required \
  "cargo test -p iamine-models" \
  env RUST_TEST_THREADS="${RUST_TEST_THREADS:-1}" cargo test -p iamine-models
run_required \
  "cargo test -p iamine-network" \
  env RUST_TEST_THREADS="${RUST_TEST_THREADS:-1}" cargo test -p iamine-network
run_required \
  "cargo test -p iamine-node" \
  env RUST_TEST_THREADS="${RUST_TEST_THREADS:-1}" cargo test -p iamine-node
run_required "cargo build -p iamine-node" cargo build -p iamine-node
run_required \
  "cargo test --workspace" \
  env RUST_TEST_THREADS="${RUST_TEST_THREADS:-1}" cargo test --workspace
run_required "git diff --check" git diff --check
run_required "git diff --cached --check" git diff --cached --check

section "OPTIONAL CHECKS"
run_optional_warn \
  "cargo clippy --workspace --all-targets" \
  "cargo clippy --version" \
  "rustup component add clippy" \
  cargo clippy --workspace --all-targets

run_optional_warn \
  "cargo audit" \
  "cargo audit --version" \
  "cargo install cargo-audit" \
  cargo audit

run_optional_warn \
  "cargo deny check" \
  "cargo deny --version" \
  "cargo install cargo-deny" \
  cargo deny check

run_optional_blocking \
  "gitleaks secret scan" \
  "gitleaks version" \
  "install gitleaks from https://github.com/gitleaks/gitleaks" \
  gitleaks detect --source . --no-git --redact

print_summary

#!/usr/bin/env bash
set -euo pipefail

required_failures=0
warning_count=0
skip_count=0

section() {
  printf '\n===== %s =====\n' "$1"
}

mark_pass() {
  printf 'PASS: %s\n' "$1"
}

mark_fail() {
  printf 'FAIL: %s\n' "$1"
  required_failures=$((required_failures + 1))
}

mark_warn() {
  printf 'WARN: %s\n' "$1"
  warning_count=$((warning_count + 1))
}

mark_skip() {
  printf 'SKIPPED: %s\n' "$1"
  skip_count=$((skip_count + 1))
}

run_required() {
  local label="$1"
  shift

  printf '+ %s\n' "$*"
  if "$@"; then
    mark_pass "$label"
  else
    mark_fail "$label"
  fi
}

run_optional() {
  local label="$1"
  local probe="$2"
  local install_hint="$3"
  shift 3

  if eval "$probe" >/dev/null 2>&1; then
    printf '+ %s\n' "$*"
    if "$@"; then
      mark_pass "$label"
    else
      mark_warn "$label failed; informational in QUALITY-GATE-CODE-001"
    fi
  else
    mark_skip "$label not available; install with: $install_hint"
  fi
}

line_count() {
  wc -l < "$1" | tr -d '[:space:]'
}

architecture_guard() {
  section "ARCHITECTURE GUARD"

  local main_file="iamine-node/src/main.rs"
  if [ ! -f "$main_file" ]; then
    mark_warn "$main_file missing; architecture guard could not inspect main.rs"
    return
  fi

  local main_lines
  main_lines="$(line_count "$main_file")"
  printf 'main.rs lines: %s\n' "$main_lines"

  local base_ref="${QUALITY_GATE_BASE_REF:-origin/develop}"
  if git rev-parse --verify "$base_ref" >/dev/null 2>&1 &&
    git cat-file -e "$base_ref:$main_file" >/dev/null 2>&1; then
    local base_lines
    local delta
    base_lines="$(git show "$base_ref:$main_file" | wc -l | tr -d '[:space:]')"
    delta=$((main_lines - base_lines))
    printf 'main.rs base ref: %s (%s lines)\n' "$base_ref" "$base_lines"
    printf 'main.rs delta: %s\n' "$delta"

    if [ "$delta" -gt 250 ]; then
      mark_warn "main.rs grew by more than 250 lines; review module boundaries"
    else
      mark_pass "main.rs growth is within the warning threshold"
    fi
  else
    mark_skip "main.rs delta check; base ref $base_ref is unavailable"
  fi

  local large_files
  large_files="$(
    find . \
      -path './target' -prune -o \
      -path './.git' -prune -o \
      -name '*.rs' -type f -print |
      while IFS= read -r file; do
        [ "$file" = "./$main_file" ] && continue
        local lines
        lines="$(line_count "$file")"
        if [ "$lines" -gt 1000 ]; then
          printf '%s %s\n' "$lines" "$file"
        fi
      done
  )"

  if [ -n "$large_files" ]; then
    printf '%s\n' "$large_files"
    mark_warn "one or more Rust files exceed 1000 lines; review future extraction opportunities"
  else
    mark_pass "no non-main Rust file exceeds 1000 lines"
  fi

  mark_skip "large function heuristic; not implemented in first quality gate iteration"
}

section "FORMAT"
run_required "cargo fmt" cargo fmt --all -- --check

section "TEST IAMINE NODE"
run_required \
  "cargo test -p iamine-node" \
  env RUST_TEST_THREADS="${RUST_TEST_THREADS:-1}" cargo test -p iamine-node

section "TEST IAMINE NETWORK"
run_required \
  "cargo test -p iamine-network" \
  env RUST_TEST_THREADS="${RUST_TEST_THREADS:-1}" cargo test -p iamine-network

section "BUILD IAMINE NODE"
run_required "cargo build -p iamine-node" cargo build -p iamine-node

section "DIFF CHECK"
run_required "git diff --check" git diff --check
run_required "git diff --cached --check" git diff --cached --check

architecture_guard

section "OPTIONAL CHECKS"
run_optional \
  "cargo clippy --workspace --all-targets" \
  "cargo clippy --version" \
  "rustup component add clippy" \
  cargo clippy --workspace --all-targets

run_optional \
  "cargo audit" \
  "cargo audit --version" \
  "cargo install cargo-audit" \
  cargo audit

run_optional \
  "cargo deny check" \
  "cargo deny --version" \
  "cargo install cargo-deny" \
  cargo deny check

run_optional \
  "gitleaks secret scan" \
  "gitleaks version" \
  "install gitleaks from https://github.com/gitleaks/gitleaks" \
  gitleaks detect --source . --no-git --redact

section "QUALITY GATE SUMMARY"
printf 'required_failures=%s\n' "$required_failures"
printf 'warnings=%s\n' "$warning_count"
printf 'skipped=%s\n' "$skip_count"

if [ "$required_failures" -gt 0 ]; then
  printf 'QUALITY GATE RESULT: FAIL\n'
  exit 1
fi

if [ "$warning_count" -gt 0 ] || [ "$skip_count" -gt 0 ]; then
  printf 'QUALITY GATE RESULT: PASS WITH WARNINGS\n'
else
  printf 'QUALITY GATE RESULT: PASS\n'
fi

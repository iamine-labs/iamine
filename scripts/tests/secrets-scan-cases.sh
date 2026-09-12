#!/usr/bin/env bash
#
# Controlled-case harness for scripts/secrets-scan.sh
# (subject: SECRETS-SCAN-CI-GATE-REPAIR-001).
#
# Every case builds its own temporary Git repository directly under `mktemp -d`,
# runs the real adapter, and asserts the result semantics - exit code plus
# observed output - instead of printing values and hoping for the best.
#
# Cases:
#   clean                 the scanner executes over history and reports nothing
#   positive-detection    a synthetic token is found, blocks, and stays redacted
#   checksum-failure      a corrupted expected checksum aborts before the scan
#   unavailable-tool      unsupported platform and download failure both abort
#                         without ever producing a scan verdict
#
# The synthetic token is assembled at run time from two ordinary string
# literals, so no value matching a scanner rule is stored in this file.

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(git -C "$script_dir" rev-parse --show-toplevel 2>/dev/null)" ||
  { printf 'harness: not inside a Git work tree\n' >&2; exit 1; }
adapter="${repo_root}/scripts/secrets-scan.sh"

if [ ! -x "$adapter" ]; then
  printf 'harness: adapter is missing or not executable: %s\n' "$adapter" >&2
  exit 1
fi

# Synthetic, non-sensitive value that matches the default gitleaks
# `github-pat` rule. Kept in two literals on purpose (see header).
synthetic_prefix="ghp_"
synthetic_body="9f2c8a71b3e54d60a1c2f3b4d5e6f7089a1b"

work_root="$(mktemp -d "${TMPDIR:-/tmp}/iamine-secrets-scan-cases.XXXXXX")"
cleanup() {
  if [ -n "${work_root-}" ] && [ -d "$work_root" ]; then
    rm -rf -- "$work_root"
  fi
}
trap cleanup EXIT HUP INT TERM

pass_count=0
fail_count=0
pass_names=""
fail_names=""

record_pass() {
  printf 'PASS  %s: %s\n' "$1" "$2"
  pass_names="${pass_names} ${1}"
  pass_count=$((pass_count + 1))
}

record_fail() {
  printf 'FAIL  %s: %s\n' "$1" "$2"
  fail_names="${fail_names} ${1}"
  fail_count=$((fail_count + 1))
}

show_output() {
  printf '      --- captured output ---\n'
  printf '%s\n' "$1" | sed 's/^/      | /'
  printf '      --- end output ---\n'
}

make_repo() {
  local dir="${work_root}/repo-$1"
  mkdir -p "$dir"
  git -C "$dir" init --quiet
  git -C "$dir" config user.email "secrets-scan-harness@example.invalid"
  git -C "$dir" config user.name "secrets scan harness"
  git -C "$dir" config commit.gpgsign false
  printf '%s\n' "$dir"
}

case_clean() {
  local dir status out
  dir="$(make_repo clean)"
  printf 'release notes\n' >"${dir}/README.md"
  printf 'fn main() {}\n' >"${dir}/main.rs"
  git -C "$dir" add README.md main.rs
  git -C "$dir" commit --quiet -m "clean baseline"

  set +e
  out="$("$adapter" "$dir" 2>&1)"
  status=$?
  set -e

  if [ "$status" -ne 0 ]; then
    record_fail clean "expected exit 0, got ${status}"
    show_output "$out"
    return
  fi

  local missing=""
  case "$out" in
  *"gitleaks version:"*) : ;;
  *) missing="${missing} scanner-version" ;;
  esac
  case "$out" in
  *"commits scanned"*) : ;;
  *) missing="${missing} commits-scanned" ;;
  esac
  case "$out" in
  *"no leaks found"*) : ;;
  *) missing="${missing} no-leaks-report" ;;
  esac
  if [ -n "$missing" ]; then
    record_fail clean "exit 0 without execution evidence (missing:${missing})"
    show_output "$out"
    return
  fi

  record_pass clean "exit 0 with executed-scan evidence"
}

case_positive_detection() {
  local dir status out token
  token="${synthetic_prefix}${synthetic_body}"
  dir="$(make_repo positive)"
  printf 'service configuration\n' >"${dir}/config.env"
  printf 'GITHUB_TOKEN=%s\n' "$token" >>"${dir}/config.env"
  git -C "$dir" add config.env
  git -C "$dir" commit --quiet -m "synthetic controlled finding"

  set +e
  out="$("$adapter" "$dir" 2>&1)"
  status=$?
  set -e

  if [ "$status" -eq 0 ]; then
    record_fail positive-detection "expected a blocking non-zero exit, got 0"
    show_output "$out"
    return
  fi
  case "$out" in
  *"leaks found:"*) : ;;
  *)
    record_fail positive-detection "non-zero exit ${status} without finding evidence"
    show_output "$out"
    return
    ;;
  esac
  if printf '%s' "$out" | grep -F -- "$token" >/dev/null 2>&1; then
    record_fail positive-detection "plaintext synthetic token appeared in captured output"
    return
  fi

  record_pass positive-detection "blocked with a redacted finding (exit ${status})"
}

case_checksum_failure() {
  local dir status out mutated
  dir="$(make_repo checksum)"
  printf 'harmless\n' >"${dir}/README.md"
  git -C "$dir" add README.md
  git -C "$dir" commit --quiet -m "baseline"

  # A mutated copy with a corrupted pinned digest is the only deterministic way
  # to prove the verification path blocks a downloaded archive.
  mutated="${work_root}/secrets-scan-mutated-checksum.sh"
  sed 's/[0-9a-f]\{64\}/0000000000000000000000000000000000000000000000000000000000000000/g' \
    "$adapter" >"$mutated"
  chmod +x "$mutated"
  if cmp -s "$adapter" "$mutated"; then
    record_fail checksum-failure "harness mutation did not alter the pinned checksum"
    return
  fi

  set +e
  out="$("$mutated" "$dir" 2>&1)"
  status=$?
  set -e

  if [ "$status" -eq 0 ]; then
    record_fail checksum-failure "expected a non-zero exit, got 0"
    show_output "$out"
    return
  fi
  case "$out" in
  *"checksum mismatch"*) : ;;
  *)
    record_fail checksum-failure "expected a checksum mismatch, got exit ${status}"
    show_output "$out"
    return
    ;;
  esac
  if printf '%s' "$out" | grep -q -e "verified" -e "no leaks found" -e "leaks found:"; then
    record_fail checksum-failure "scan verdict or verification appeared despite the mismatch"
    show_output "$out"
    return
  fi

  record_pass checksum-failure "checksum mismatch blocked before scanning (exit ${status})"
}

case_unavailable_tool() {
  local dir status out fake_bin
  dir="$(make_repo unavailable)"
  printf 'harmless\n' >"${dir}/README.md"
  git -C "$dir" add README.md
  git -C "$dir" commit --quiet -m "baseline"

  # (a) unsupported platform: no pinned asset exists for the reported OS/arch.
  fake_bin="${work_root}/bin-unsupported"
  mkdir -p "$fake_bin"
  {
    printf '#!/bin/sh\n'
    printf 'case "$1" in\n'
    printf '%s\n' '-s) echo Linux ;;'
    printf '%s\n' '-m) echo i686 ;;'
    printf '*) echo Linux ;;\n'
    printf 'esac\n'
  } >"${fake_bin}/uname"
  chmod +x "${fake_bin}/uname"

  set +e
  out="$(PATH="${fake_bin}:${PATH}" "$adapter" "$dir" 2>&1)"
  status=$?
  set -e

  if [ "$status" -eq 0 ]; then
    record_fail unavailable-tool "unsupported platform exited 0"
    show_output "$out"
    return
  fi
  case "$out" in
  *"unsupported platform"*) : ;;
  *)
    record_fail unavailable-tool "unsupported platform was not reported (exit ${status})"
    show_output "$out"
    return
    ;;
  esac
  if printf '%s' "$out" | grep -q -e "no leaks found" -e "leaks found:"; then
    record_fail unavailable-tool "unsupported platform produced a scan verdict"
    show_output "$out"
    return
  fi

  # (b) the download tool is present but cannot fetch the pinned asset.
  fake_bin="${work_root}/bin-curl-failure"
  mkdir -p "$fake_bin"
  {
    printf '#!/bin/sh\n'
    printf 'echo "curl: (6) could not resolve host" >&2\n'
    printf 'exit 6\n'
  } >"${fake_bin}/curl"
  chmod +x "${fake_bin}/curl"

  set +e
  out="$(PATH="${fake_bin}:${PATH}" "$adapter" "$dir" 2>&1)"
  status=$?
  set -e

  if [ "$status" -eq 0 ]; then
    record_fail unavailable-tool "failed download exited 0"
    show_output "$out"
    return
  fi
  case "$out" in
  *"failed to download"*) : ;;
  *)
    record_fail unavailable-tool "download failure was not reported (exit ${status})"
    show_output "$out"
    return
    ;;
  esac
  if printf '%s' "$out" | grep -q -e "no leaks found" -e "leaks found:"; then
    record_fail unavailable-tool "failed download produced a scan verdict"
    show_output "$out"
    return
  fi

  record_pass unavailable-tool "unsupported platform and failed download both blocked"
}

printf 'IAMINE secrets scan controlled cases\n'
printf 'adapter: %s\n\n' "$adapter"

case_clean
case_positive_detection
case_checksum_failure
case_unavailable_tool

printf '\ncontrolled case summary\n'
printf '  passed:%s\n' "${pass_names:- none}"
printf '  failed:%s\n' "${fail_names:- none}"
printf 'cases_passed=%s cases_failed=%s\n' "$pass_count" "$fail_count"

if [ "$fail_count" -ne 0 ]; then
  printf 'RESULT: FAIL\n'
  exit 1
fi

printf 'RESULT: PASS\n'
exit 0

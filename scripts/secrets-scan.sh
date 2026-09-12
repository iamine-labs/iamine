#!/usr/bin/env bash
#
# IAMINE secret scan adapter (subject: SECRETS-SCAN-CI-GATE-REPAIR-001).
#
# Runs a pinned, checksum-verified Gitleaks CLI scan over the full Git history of
# the target repository. The adapter exists because the license-gated
# `gitleaks/gitleaks-action` wrapper could not execute on this organization and
# could therefore never produce scan evidence.
#
# Fail-closed rules implemented here:
#   1. the Gitleaks version and the SHA-256 of every supported release asset are
#      pinned in this file; nothing is fetched from "latest" and no environment
#      override for the URL, asset, or checksum exists;
#   2. the release asset is fetched over HTTPS only (curl --proto '=https'
#      --tlsv1.2) from the official GitHub release URL;
#   3. the archive must match the pinned SHA-256 before it is extracted;
#   4. unsupported platforms, missing tools, download failures, checksum
#      mismatches, extraction failures, and scanner failures all exit non-zero;
#   5. findings are redacted by the scanner (--redact) and block the run
#      (--exit-code 1);
#   6. there is no fallback, skip, or "|| true" path that can turn a failed or
#      unexecuted scan into a pass.
#
# Usage:
#   scripts/secrets-scan.sh [<repository-path>]
#
# <repository-path> defaults to the Git work tree that contains this script.
# The scan covers every ref available in the local clone (--log-opts=--all),
# which includes the full history of the checked-out commit.

set -euo pipefail

readonly GITLEAKS_VERSION="8.30.1"
readonly GITLEAKS_RELEASE_BASE="https://github.com/gitleaks/gitleaks/releases/download/v${GITLEAKS_VERSION}"

# Every adapter failure exits with a code >= 2 so it can never be confused with
# the scanner's own "leaks found" exit code (1).
readonly EXIT_USAGE=2
readonly EXIT_UNSUPPORTED_PLATFORM=3
readonly EXIT_TOOL_UNAVAILABLE=4
readonly EXIT_DOWNLOAD_FAILED=5
readonly EXIT_CHECKSUM_MISMATCH=6
readonly EXIT_EXTRACT_FAILED=7
readonly EXIT_SCANNER_UNAVAILABLE=8
readonly EXIT_CONFIG_REFUSED=9

log() {
  printf 'secrets-scan: %s\n' "$*" >&2
}

fail() {
  local code="$1"
  shift
  log "ERROR: $*"
  exit "$code"
}

sha256_of() {
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  elif command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    return 1
  fi
}

if [ "$#" -gt 1 ]; then
  fail "$EXIT_USAGE" "usage: scripts/secrets-scan.sh [<repository-path>]"
fi

target_repo="${1-}"
if [ -z "$target_repo" ]; then
  script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
  target_repo="$(git -C "$script_dir" rev-parse --show-toplevel 2>/dev/null)" ||
    fail "$EXIT_USAGE" "not inside a Git work tree; pass an explicit repository path"
fi

[ -d "$target_repo" ] ||
  fail "$EXIT_USAGE" "repository path is not a directory: $target_repo"
git -C "$target_repo" rev-parse --git-dir >/dev/null 2>&1 ||
  fail "$EXIT_USAGE" "not a Git repository: $target_repo"

# A substituted scan configuration would silently change coverage, so the
# environment is not allowed to inject one.
if [ -n "${GITLEAKS_CONFIG-}" ] || [ -n "${GITLEAKS_CONFIG_TOML-}" ]; then
  fail "$EXIT_CONFIG_REFUSED" \
    "GITLEAKS_CONFIG or GITLEAKS_CONFIG_TOML is set; refusing to substitute scan configuration from the environment"
fi

os_name="$(uname -s)"
arch_name="$(uname -m)"
case "${os_name}/${arch_name}" in
Darwin/arm64)
  platform="darwin_arm64"
  ;;
Linux/x86_64 | Linux/amd64)
  platform="linux_x64"
  ;;
*)
  fail "$EXIT_UNSUPPORTED_PLATFORM" \
    "unsupported platform ${os_name}/${arch_name}; pinned assets exist only for Darwin/arm64 and Linux/x86_64"
  ;;
esac

# Pinned official release assets. Both digests were verified against
# https://github.com/gitleaks/gitleaks/releases/download/v8.30.1/.
case "$platform" in
darwin_arm64)
  asset="gitleaks_${GITLEAKS_VERSION}_darwin_arm64.tar.gz"
  expected_sha256="b40ab0ae55c505963e365f271a8d3846efbc170aa17f2607f13df610a9aeb6a5"
  ;;
linux_x64)
  asset="gitleaks_${GITLEAKS_VERSION}_linux_x64.tar.gz"
  expected_sha256="551f6fc83ea457d62a0d98237cbad105af8d557003051f41f3e7ca7b3f2470eb"
  ;;
*)
  fail "$EXIT_UNSUPPORTED_PLATFORM" "no pinned asset is recorded for platform $platform"
  ;;
esac

for tool in curl tar mktemp; do
  command -v "$tool" >/dev/null 2>&1 ||
    fail "$EXIT_TOOL_UNAVAILABLE" "$tool is required to fetch and unpack the pinned scanner"
done

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/iamine-secrets-scan.XXXXXX")" ||
  fail "$EXIT_TOOL_UNAVAILABLE" "could not create a temporary working directory"
cleanup() {
  if [ -n "${work_dir-}" ] && [ -d "$work_dir" ]; then
    rm -rf -- "$work_dir"
  fi
}
trap cleanup EXIT HUP INT TERM

url="${GITLEAKS_RELEASE_BASE}/${asset}"
archive="${work_dir}/${asset}"

log "platform ${platform}; fetching ${url}"
if ! curl --proto '=https' --tlsv1.2 --location --silent --show-error --fail \
  --output "$archive" "$url"; then
  fail "$EXIT_DOWNLOAD_FAILED" "failed to download the pinned scanner asset: $url"
fi
[ -s "$archive" ] ||
  fail "$EXIT_DOWNLOAD_FAILED" "downloaded scanner archive is empty: $asset"

actual_sha256="$(sha256_of "$archive")" ||
  fail "$EXIT_TOOL_UNAVAILABLE" "no SHA-256 tool available (neither shasum nor sha256sum)"
if [ "$actual_sha256" != "$expected_sha256" ]; then
  fail "$EXIT_CHECKSUM_MISMATCH" \
    "checksum mismatch for ${asset}: expected ${expected_sha256}, actual ${actual_sha256}"
fi
log "verified ${asset} (sha256 ${actual_sha256})"

extract_dir="${work_dir}/extract"
mkdir -p "$extract_dir" ||
  fail "$EXIT_EXTRACT_FAILED" "could not create the extraction directory"
if ! tar -xzf "$archive" -C "$extract_dir"; then
  fail "$EXIT_EXTRACT_FAILED" "failed to extract ${asset}"
fi

scanner="${extract_dir}/gitleaks"
[ -f "$scanner" ] ||
  fail "$EXIT_EXTRACT_FAILED" "${asset} did not contain a gitleaks binary"
[ -x "$scanner" ] ||
  fail "$EXIT_EXTRACT_FAILED" "extracted gitleaks binary is not executable"

version_output="$("$scanner" version 2>&1)" ||
  fail "$EXIT_SCANNER_UNAVAILABLE" "the pinned gitleaks binary could not report its version"
case "$version_output" in
*"$GITLEAKS_VERSION"*) : ;;
*)
  fail "$EXIT_SCANNER_UNAVAILABLE" \
    "unexpected scanner version output: ${version_output}"
  ;;
esac
log "gitleaks version: ${version_output}"

if [ -f "${target_repo}/.gitleaks.toml" ]; then
  log "notice: ${target_repo}/.gitleaks.toml is present and will be used by the scanner; review it, no configuration file was added by this subject"
fi

log "scanning full Git history of ${target_repo} (gitleaks git --redact --exit-code 1 --log-opts=--all)"
set +e
"$scanner" git "$target_repo" --redact --exit-code 1 --log-opts=--all
scan_status=$?
set -e

if [ "$scan_status" -eq 0 ]; then
  log "scan executed and reported no findings"
  exit 0
fi

log "scan failed or reported findings (scanner exit ${scan_status}); the gate must not pass"
exit "$scan_status"

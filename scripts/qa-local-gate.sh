#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
base_ref="${IAMINE_QA_BASE_REF:-origin/develop}"
expected_branch="${IAMINE_QA_EXPECTED_BRANCH:-}"
identity_only=0
require_clean=0

usage() {
  cat <<'USAGE'
Usage: scripts/qa-local-gate.sh [options]

Options:
  --base-ref REF          Base ref to validate (default: origin/develop)
  --expected-branch NAME  Require an exact branch name
  --identity-only         Run identity and scope checks without the quality gate
  --require-clean         Require clean tracked worktree and staging
  -h, --help              Show this help
USAGE
}

fail() {
  printf 'LOCAL_GATE=FAIL\nREASON=%s\n' "$1" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "required_command_missing:$1"
}

hash_stream() {
  git hash-object --stdin
}

count_nul_records() {
  tr -cd '\0' | wc -c | tr -d '[:space:]'
}

normalize_origin() {
  case "$1" in
    https://github.com/iamine-labs/iamine|\
    https://github.com/iamine-labs/iamine.git|\
    git@github.com:iamine-labs/iamine.git|\
    ssh://git@github.com/iamine-labs/iamine.git)
      printf 'github.com/iamine-labs/iamine\n'
      ;;
    *)
      return 1
      ;;
  esac
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --base-ref)
      [ "$#" -ge 2 ] || fail "missing_value:--base-ref"
      base_ref="$2"
      shift 2
      ;;
    --expected-branch)
      [ "$#" -ge 2 ] || fail "missing_value:--expected-branch"
      expected_branch="$2"
      shift 2
      ;;
    --identity-only)
      identity_only=1
      shift
      ;;
    --require-clean)
      require_clean=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      fail "unknown_option:$1"
      ;;
  esac
done

require_command git

git -C "$repo_root" rev-parse --is-inside-work-tree >/dev/null 2>&1 ||
  fail "not_a_git_worktree"
git -C "$repo_root" rev-parse --verify "$base_ref^{commit}" >/dev/null 2>&1 ||
  fail "base_ref_not_found:$base_ref"

branch="$(git -C "$repo_root" branch --show-current)"
[ -n "$branch" ] || fail "detached_head"
[ "$branch" != "main" ] || fail "main_branch_is_not_a_development_target"
if [ -n "$expected_branch" ] && [ "$branch" != "$expected_branch" ]; then
  fail "wrong_branch:expected=$expected_branch:actual=$branch"
fi

head_sha="$(git -C "$repo_root" rev-parse HEAD)"
tree_sha="$(git -C "$repo_root" rev-parse 'HEAD^{tree}')"
base_sha="$(git -C "$repo_root" rev-parse "$base_ref^{commit}")"
merge_base="$(git -C "$repo_root" merge-base HEAD "$base_ref")"
[ "$merge_base" = "$base_sha" ] ||
  fail "branch_does_not_contain_base:$base_ref"

origin_url="$(git -C "$repo_root" remote get-url origin 2>/dev/null)" ||
  fail "origin_remote_missing"
origin_display="$(normalize_origin "$origin_url")" ||
  fail "noncanonical_origin"

if git -C "$repo_root" rev-parse --verify origin/main^{commit} >/dev/null 2>&1; then
  main_missing_from_develop="$(
    git -C "$repo_root" rev-list --count "$base_ref..origin/main"
  )"
  [ "$main_missing_from_develop" -eq 0 ] ||
    fail "origin_main_contains_commits_missing_from_base"
else
  fail "origin_main_not_found"
fi

tracked_count="$(
  git -C "$repo_root" diff --name-only -z | count_nul_records
)"
tracked_hash="$(
  git -C "$repo_root" diff --name-only -z | hash_stream
)"
staged_count="$(
  git -C "$repo_root" diff --cached --name-only -z | count_nul_records
)"
staged_hash="$(
  git -C "$repo_root" diff --cached --name-only -z | hash_stream
)"
untracked_count="$(
  git -C "$repo_root" ls-files --others --exclude-standard -z | count_nul_records
)"
untracked_hash="$(
  git -C "$repo_root" ls-files --others --exclude-standard -z | hash_stream
)"
scope_count="$(
  git -C "$repo_root" diff --name-only -z "$base_ref"...HEAD |
    count_nul_records
)"
scope_hash="$(
  git -C "$repo_root" diff --name-only -z "$base_ref"...HEAD |
    hash_stream
)"

if [ "$require_clean" -eq 1 ]; then
  [ "$tracked_count" -eq 0 ] || fail "tracked_worktree_not_clean"
  [ "$staged_count" -eq 0 ] || fail "staging_not_clean"
fi

printf 'IAMINE LOCAL QA GATE\n'
printf 'BRANCH=%s\n' "$branch"
printf 'HEAD=%s\n' "$head_sha"
printf 'TREE=%s\n' "$tree_sha"
printf 'BASE_REF=%s\n' "$base_ref"
printf 'BASE=%s\n' "$base_sha"
printf 'MERGE_BASE=%s\n' "$merge_base"
printf 'ORIGIN=%s\n' "$origin_display"
printf 'TRACKED_COUNT=%s\n' "$tracked_count"
printf 'TRACKED_PATHS_HASH=%s\n' "$tracked_hash"
printf 'STAGED_COUNT=%s\n' "$staged_count"
printf 'STAGED_PATHS_HASH=%s\n' "$staged_hash"
printf 'UNTRACKED_COUNT=%s\n' "$untracked_count"
printf 'UNTRACKED_PATHS_HASH=%s\n' "$untracked_hash"
printf 'COMMITTED_SCOPE_COUNT=%s\n' "$scope_count"
printf 'COMMITTED_SCOPE_HASH=%s\n' "$scope_hash"
printf 'IDENTITY_CHECK=PASS\n'

if [ "$identity_only" -eq 1 ]; then
  printf 'QUALITY_GATE=SKIPPED_IDENTITY_ONLY\n'
  printf 'LOCAL_GATE=PASS\n'
  exit 0
fi

printf '\nRUNNING_QUALITY_GATE=1\n'
if (
  cd "$repo_root"
  QUALITY_GATE_BASE_REF="$base_ref" ./scripts/quality-gate.sh
); then
  printf 'QUALITY_GATE=PASS\n'
  printf 'LOCAL_GATE=PASS\n'
else
  fail "quality_gate_failed"
fi

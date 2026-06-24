#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
target_dir="${IAMINE_LEGACY_CPU_TARGET_DIR:-"$repo_root/target/legacy-cpu"}"
cargo_bin="${CARGO:-cargo}"

export CARGO_TARGET_DIR="$target_dir"
export CMAKE_PROJECT_INCLUDE="$repo_root/cmake/iamine-legacy-cpu.cmake"

"$cargo_bin" build \
  -p iamine-node \
  --bin iamine-node \
  --no-default-features \
  --features legacy-cpu-daemon \
  "$@"

printf 'Legacy CPU daemon binary built under: %s\n' "$target_dir"

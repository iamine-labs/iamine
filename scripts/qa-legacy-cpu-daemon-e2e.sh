#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
remote_host="${IAMINE_QA_REMOTE_HOST:-iamine-heavy}"
remote_repo="${IAMINE_QA_REMOTE_REPO:-code/iamine}"
remote_port="${IAMINE_QA_WORKER_PORT:-4103}"
model_id="${IAMINE_QA_MODEL_ID:-tinyllama-1b}"
max_tokens="${IAMINE_QA_MAX_TOKENS:-8}"
client_timeout_secs="${IAMINE_QA_CLIENT_TIMEOUT_SECS:-240}"
expected_remote_head="${IAMINE_QA_EXPECTED_REMOTE_HEAD:-}"
preflight_only="${IAMINE_QA_PREFLIGHT_ONLY:-0}"
run_id="${IAMINE_QA_RUN_ID:-$(date +%s)}"
remote_qa_dir="/tmp/iamine-legacy-e2e-${run_id}"
local_qa_dir="${IAMINE_QA_OUTPUT_DIR:-/tmp/iamine-legacy-e2e-${run_id}}"
local_binary="${IAMINE_QA_LOCAL_BINARY:-$repo_root/target/debug/iamine-node}"
prompt="${IAMINE_QA_PROMPT:-Reply with exactly: IAMINE LEGACY E2E PASS}"

remote_cleanup_needed=0
cleanup_complete=0

log() {
  printf '[legacy-e2e] %s\n' "$*"
}

fail() {
  printf '[legacy-e2e] ERROR: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

validate_positive_integer() {
  case "$2" in
    ''|*[!0-9]*)
      fail "$1 must be a positive integer"
      ;;
  esac
  [ "$2" -gt 0 ] || fail "$1 must be greater than zero"
}

remote_cleanup() {
  [ "$remote_cleanup_needed" -eq 1 ] || return 0
  [ "$cleanup_complete" -eq 0 ] || return 0

  log "cleaning up QA-owned remote processes"
  if ! ssh -o LogLevel=QUIET "$remote_host" bash -s -- \
    "$remote_qa_dir" \
    "$remote_repo" \
    "$remote_port" <<'REMOTE_CLEANUP'
set -u

qa_dir="$1"
repo_arg="$2"
worker_port="$3"
case "$repo_arg" in
  /*) repo="$repo_arg" ;;
  *) repo="$HOME/$repo_arg" ;;
esac

stop_owned_process() {
  name="$1"
  pid_file="$qa_dir/$name.pid"
  expected_file="$qa_dir/$name.exe"
  [ -f "$pid_file" ] || return 0
  [ -f "$expected_file" ] || return 0

  pid="$(cat "$pid_file")"
  expected="$(cat "$expected_file")"
  case "$pid" in
    ''|*[!0-9]*) return 0 ;;
  esac
  kill -0 "$pid" 2>/dev/null || return 0

  actual="$(readlink -f "/proc/$pid/exe" 2>/dev/null || true)"
  expected="$(readlink -f "$expected" 2>/dev/null || true)"
  if [ -z "$actual" ] || [ "$actual" != "$expected" ]; then
    printf 'Refusing to stop %s PID %s: executable mismatch\n' "$name" "$pid" >&2
    return 1
  fi

  kill -TERM "$pid"
  attempts=0
  while kill -0 "$pid" 2>/dev/null && [ "$attempts" -lt 20 ]; do
    sleep 1
    attempts=$((attempts + 1))
  done
  if kill -0 "$pid" 2>/dev/null; then
    printf '%s PID %s did not stop after TERM\n' "$name" "$pid" >&2
    return 1
  fi
}

stop_owned_process worker

socket="$qa_dir/daemon.sock"
if [ -S "$socket" ] && command -v nc >/dev/null 2>&1; then
  printf '{"type":"Shutdown"}\n' | nc -U "$socket" >/dev/null 2>&1 || true
fi
stop_owned_process daemon

rm -f "$socket"
for evidence_file in "$qa_dir/"*.stdout "$qa_dir/"*.stderr; do
  [ -f "$evidence_file" ] || continue
  sed -E \
    -e 's#/home/[^/[:space:]]+#<home>#g' \
    -e 's#([0-9]{1,3}\.){3}[0-9]{1,3}#<ip>#g' \
    "$evidence_file" >"$evidence_file.sanitized"
  mv "$evidence_file.sanitized" "$evidence_file"
done
test -d "$repo"
if ss -H -ltn | awk -v suffix=":$worker_port" '$4 ~ suffix"$" {found=1} END {exit found ? 0 : 1}'; then
  printf 'Worker port %s remains in use after cleanup\n' "$worker_port" >&2
  exit 1
fi
REMOTE_CLEANUP
  then
    printf '[legacy-e2e] WARNING: remote cleanup was not fully confirmed\n' >&2
  fi
  cleanup_complete=1
}

on_exit() {
  status=$?
  remote_cleanup
  if [ "$status" -ne 0 ]; then
    printf '[legacy-e2e] FAILED; local evidence: %s\n' "$local_qa_dir" >&2
    printf '[legacy-e2e] remote evidence: %s:%s\n' "$remote_host" "$remote_qa_dir" >&2
  fi
  exit "$status"
}

trap on_exit EXIT
trap 'exit 130' INT TERM

validate_positive_integer IAMINE_QA_WORKER_PORT "$remote_port"
validate_positive_integer IAMINE_QA_MAX_TOKENS "$max_tokens"
validate_positive_integer IAMINE_QA_CLIENT_TIMEOUT_SECS "$client_timeout_secs"
require_command git
require_command jq
require_command shasum
require_command ssh
require_command scp

[ -x "$local_binary" ] || fail "local binary is not executable: $local_binary"
mkdir -p "$local_qa_dir"

local_branch="$(git -C "$repo_root" branch --show-current)"
local_head="$(git -C "$repo_root" rev-parse HEAD)"
local_tree="$(git -C "$repo_root" rev-parse 'HEAD^{tree}')"
{
  printf 'branch=%s\n' "$local_branch"
  printf 'head=%s\n' "$local_head"
  printf 'tree=%s\n' "$local_tree"
  printf 'remote_host=%s\n' "$remote_host"
  printf 'remote_repo=%s\n' "$remote_repo"
  printf 'remote_port=%s\n' "$remote_port"
  printf 'model_id=%s\n' "$model_id"
  printf 'max_tokens=%s\n' "$max_tokens"
} >"$local_qa_dir/local-identity.txt"

git -C "$repo_root" diff --quiet || fail "local tracked worktree is not clean"
git -C "$repo_root" diff --cached --quiet || fail "local staging area is not clean"
: >"$local_qa_dir/local-untracked-sha256.txt"
while IFS= read -r -d '' path; do
  if [ -f "$repo_root/$path" ]; then
    (
      cd "$repo_root"
      shasum -a 256 "$path"
    ) >>"$local_qa_dir/local-untracked-sha256.txt"
  else
    printf 'non_file  %s\n' "$path" >>"$local_qa_dir/local-untracked-sha256.txt"
  fi
done < <(git -C "$repo_root" ls-files --others --exclude-standard -z)

log "CHECK 5: remote identity and legacy CPU preflight on $remote_host"
ssh -o LogLevel=QUIET "$remote_host" bash -s -- \
  "$remote_repo" \
  "$remote_qa_dir" \
  "$remote_port" \
  "$model_id" \
  "$expected_remote_head" <<'REMOTE_PREFLIGHT'
set -euo pipefail

repo_arg="$1"
qa_dir="$2"
worker_port="$3"
model_id="$4"
expected_head="$5"
case "$repo_arg" in
  /*) repo="$repo_arg" ;;
  *) repo="$HOME/$repo_arg" ;;
esac

command -v git >/dev/null
command -v jq >/dev/null
command -v nc >/dev/null
command -v sha256sum >/dev/null
command -v ss >/dev/null
[ -d "$repo/.git" ]

mkdir -p "$qa_dir"
cd "$repo"

branch="$(git branch --show-current)"
head="$(git rev-parse HEAD)"
tree="$(git rev-parse 'HEAD^{tree}')"
origin="$(git remote get-url origin)"
tracked="$(git diff --name-status)"
staged="$(git diff --cached --name-status)"
case "$origin" in
  https://github.com/iamine-labs/iamine|\
  https://github.com/iamine-labs/iamine.git|\
  git@github.com:iamine-labs/iamine.git)
    canonical_origin="github.com/iamine-labs/iamine"
    ;;
  *)
    printf 'Unexpected origin for canonical QA repository\n' >&2
    exit 1
    ;;
esac
case "$repo_arg" in
  /*) repo_display="<absolute-path-redacted>" ;;
  *) repo_display="~/$repo_arg" ;;
esac

{
  printf 'repo=%s\n' "$repo_display"
  printf 'branch=%s\n' "$branch"
  printf 'head=%s\n' "$head"
  printf 'tree=%s\n' "$tree"
  printf 'origin=%s\n' "$canonical_origin"
  printf 'tracked_clean=%s\n' "$([ -z "$tracked" ] && printf 1 || printf 0)"
  printf 'staging_clean=%s\n' "$([ -z "$staged" ] && printf 1 || printf 0)"
} >"$qa_dir/remote-identity.txt"

: >"$qa_dir/remote-untracked-sha256.txt"
while IFS= read -r -d '' path; do
  if [ -f "$path" ]; then
    sha256sum "$path" >>"$qa_dir/remote-untracked-sha256.txt"
  else
    printf 'non_file  %s\n' "$path" >>"$qa_dir/remote-untracked-sha256.txt"
  fi
done < <(git ls-files --others --exclude-standard -z)

[ -z "$tracked" ]
[ -z "$staged" ]
if [ -n "$expected_head" ]; then
  [ "$head" = "$expected_head" ]
fi

if grep -qw avx2 /proc/cpuinfo; then
  printf 'Expected a legacy CPU without AVX2\n' >&2
  exit 1
fi

standard_binary="$repo/target/debug/iamine-node"
daemon_binary="$repo/target/legacy-cpu/debug/iamine-node"
[ -x "$standard_binary" ]
[ -x "$daemon_binary" ]

if ss -H -ltn | awk -v suffix=":$worker_port" '$4 ~ suffix"$" {found=1} END {exit found ? 0 : 1}'; then
  printf 'Worker port %s is already in use\n' "$worker_port" >&2
  exit 1
fi

"$standard_binary" models list >"$qa_dir/models-list.txt" 2>&1
grep -F "$model_id" "$qa_dir/models-list.txt" >/dev/null

printf 'PRECHECK=PASS\n'
printf 'REMOTE_BRANCH=%s\n' "$branch"
printf 'REMOTE_HEAD=%s\n' "$head"
printf 'REMOTE_TREE=%s\n' "$tree"
printf 'LEGACY_CPU_NO_AVX2=1\n'
printf 'MODEL_INSTALLED=%s\n' "$model_id"
REMOTE_PREFLIGHT

if [ "$preflight_only" = "1" ]; then
  cleanup_complete=1
  log "preflight-only run passed"
  log "remote evidence: $remote_host:$remote_qa_dir"
  exit 0
fi

remote_cleanup_needed=1
log "CHECK 6: starting isolated daemon and worker"
ssh -o LogLevel=QUIET "$remote_host" bash -s -- \
  "$remote_repo" \
  "$remote_qa_dir" \
  "$remote_port" <<'REMOTE_START'
set -euo pipefail

repo_arg="$1"
qa_dir="$2"
worker_port="$3"
case "$repo_arg" in
  /*) repo="$repo_arg" ;;
  *) repo="$HOME/$repo_arg" ;;
esac

standard_binary="$repo/target/debug/iamine-node"
daemon_binary="$repo/target/legacy-cpu/debug/iamine-node"
socket="$qa_dir/daemon.sock"

printf '%s\n' "$daemon_binary" >"$qa_dir/daemon.exe"
printf '%s\n' "$standard_binary" >"$qa_dir/worker.exe"

nohup env \
  IAMINE_DAEMON_SOCKET="$socket" \
  IAMINE_LOG_FORMAT=ndjson \
  IAMINE_LOG_PATH="$qa_dir/daemon.ndjson" \
  "$daemon_binary" --daemon \
  >"$qa_dir/daemon.stdout" \
  2>"$qa_dir/daemon.stderr" \
  </dev/null &
daemon_pid=$!
printf '%s\n' "$daemon_pid" >"$qa_dir/daemon.pid"

attempts=0
until [ -S "$socket" ]; do
  kill -0 "$daemon_pid"
  attempts=$((attempts + 1))
  [ "$attempts" -lt 60 ]
  sleep 1
done

pong="$(printf '{"type":"Ping"}\n' | nc -U "$socket")"
printf '%s\n' "$pong" >"$qa_dir/daemon-ping.json"
printf '%s\n' "$pong" | jq -e 'select(.type == "Pong")' >/dev/null

nohup env \
  IAMINE_DAEMON_SOCKET="$socket" \
  IAMINE_INFERENCE_BACKEND=real \
  IAMINE_LEGACY_CPU_REAL_BACKEND=daemon_only \
  IAMINE_SKIP_MODEL_LOAD_ON_STARTUP=0 \
  IAMINE_LOG_FORMAT=ndjson \
  IAMINE_LOG_PATH="$qa_dir/worker.ndjson" \
  IAMINE_TASK_LIFECYCLE_PATH="$qa_dir/worker-tasks.ndjson" \
  "$standard_binary" --worker "--port=$worker_port" \
  >"$qa_dir/worker.stdout" \
  2>"$qa_dir/worker.stderr" \
  </dev/null &
worker_pid=$!
printf '%s\n' "$worker_pid" >"$qa_dir/worker.pid"

attempts=0
until jq -e \
  'select(.event == "worker_startup_ready" and .fields.backend == "real" and .fields.legacy_cpu_real_backend_mode == "daemon_only" and .fields.real_inference_available == true)' \
  "$qa_dir/worker.ndjson" >/dev/null 2>&1
do
  kill -0 "$worker_pid"
  attempts=$((attempts + 1))
  [ "$attempts" -lt 90 ]
  sleep 1
done

if jq -e 'select(.event == "worker_model_load_attempt")' "$qa_dir/worker.ndjson" >/dev/null 2>&1; then
  printf 'Worker attempted a local model load during startup\n' >&2
  exit 1
fi

printf 'REMOTE_START=PASS\n'
printf 'DAEMON_PID=%s\n' "$daemon_pid"
printf 'WORKER_PID=%s\n' "$worker_pid"
REMOTE_START

log "running Mac client through LAN worker and legacy daemon"
client_status=0
env \
  IAMINE_LOG_FORMAT=ndjson \
  IAMINE_LOG_PATH="$local_qa_dir/client.ndjson" \
  IAMINE_TASK_LIFECYCLE_PATH="$local_qa_dir/client-tasks.ndjson" \
  "$local_binary" infer "$prompt" \
  --model "$model_id" \
  --max-tokens "$max_tokens" \
  --force-network \
  --no-local \
  >"$local_qa_dir/client.stdout" \
  2>"$local_qa_dir/client.stderr" &
client_pid=$!
deadline=$((SECONDS + client_timeout_secs))

while kill -0 "$client_pid" 2>/dev/null; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    kill -TERM "$client_pid" 2>/dev/null || true
    wait "$client_pid" 2>/dev/null || true
    client_status=124
    break
  fi
  sleep 1
done

if [ "$client_status" -ne 124 ]; then
  if wait "$client_pid"; then
    client_status=0
  else
    client_status=$?
  fi
fi
[ "$client_status" -eq 0 ] || fail "client exited with status $client_status"

log "collecting remote evidence"
mkdir -p "$local_qa_dir/remote"
ssh -o LogLevel=QUIET "$remote_host" \
  tar -C "$remote_qa_dir" --exclude=daemon.sock -cf - . |
  tar -C "$local_qa_dir/remote" -xf -

for evidence_file in "$local_qa_dir/remote/"*.stdout "$local_qa_dir/remote/"*.stderr; do
  [ -f "$evidence_file" ] || continue
  sed -E \
    -e 's#/home/[^/[:space:]]+#<home>#g' \
    -e 's#([0-9]{1,3}\.){3}[0-9]{1,3}#<ip>#g' \
    "$evidence_file" >"$evidence_file.sanitized"
  mv "$evidence_file.sanitized" "$evidence_file"
done

worker_log="$local_qa_dir/remote/worker.ndjson"
daemon_stdout="$local_qa_dir/remote/daemon.stdout"

jq -e --arg model_id "$model_id" \
  'select(.event == "task_message_received" and .model_id == $model_id)' \
  "$worker_log" >/dev/null
jq -e 'select(.event == "task_completed" and .fields.success == true)' \
  "$worker_log" >/dev/null
jq -e 'select(.event == "result_published")' "$worker_log" >/dev/null
jq -e 'select(.event == "result_received")' "$local_qa_dir/client.ndjson" >/dev/null
jq -e 'select(.event == "final_outcome_success")' "$local_qa_dir/client.ndjson" >/dev/null
grep -F "Inference completada:" "$local_qa_dir/client.stdout" >/dev/null
grep -F "[Inference] Executed in" "$daemon_stdout" >/dev/null

if grep -Eqi 'illegal instruction|sigill' \
  "$local_qa_dir/remote/daemon.stdout" \
  "$local_qa_dir/remote/daemon.stderr" \
  "$local_qa_dir/remote/worker.stdout" \
  "$local_qa_dir/remote/worker.stderr"
then
  fail "SIGILL evidence found"
fi

log "CHECK 7: cleaning up and confirming side effects"
remote_cleanup

{
  printf 'result=PASS\n'
  printf 'local_branch=%s\n' "$local_branch"
  printf 'local_head=%s\n' "$local_head"
  printf 'remote_host=%s\n' "$remote_host"
  printf 'remote_qa_dir=%s\n' "$remote_qa_dir"
  printf 'model_id=%s\n' "$model_id"
  printf 'max_tokens=%s\n' "$max_tokens"
  printf 'client_status=%s\n' "$client_status"
  printf 'sigill=0\n'
} >"$local_qa_dir/result.txt"

log "PASS: Mac client -> legacy worker -> safe daemon -> real model result"
log "local evidence: $local_qa_dir"
log "remote evidence: $remote_host:$remote_qa_dir"
